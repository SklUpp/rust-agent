//! Termux wake-lock RAII guard for active turns.
//!
//! Android may suspend the CPU while a long provider request or tool call is
//! in flight. [`WakeLockGuard::acquire`] holds one process-wide reference and
//! the guard's drop releases it. `acquire`/`Drop` only update a coalesced
//! desired state and wake a worker, so neither ever blocks on Termux:API.
//!
//! A single worker thread reconciles `desired_locked` to the OS helper with a
//! `(Mutex, Condvar)` state machine. Coalescing means a slow `termux-wake-lock`
//! followed quickly by an unlock produces exactly one final unlock instead of
//! an unlock racing an older lock, and a queued unlock can never finish after a
//! newer lock. [`shutdown`] forces the terminal unlocked state and waits
//! (bounded) for the worker, so process exit cannot lose the final unlock.
//!
//! The helper is fail-open: a missing Termux:API binary or a hung helper
//! (killed and reaped after two seconds) never blocks a turn. Non-Android
//! builds compile a zero-field no-op state machine; unit tests compile the real
//! one on Linux through the injected runner.

use std::sync::LazyLock;

/// Set once, on the first acquire. Initialization is cheap even without a turn.
static STATE: LazyLock<imp::WakeState> = LazyLock::new(|| imp::WakeState::new(imp::run_helper));

/// Held for the lifetime of an active turn.
///
/// The first guard in the process establishes the OS lock; concurrent turns
/// share it, and only the last drop releases it.
#[must_use = "the wake lock is released as soon as the guard is dropped"]
pub struct WakeLockGuard {
	/// False when shutdown had already begun (no state was counted).
	acquired: bool,
}

impl WakeLockGuard {
	/// Acquire one reference, establishing the OS wake lock on the transition
	/// from zero. Never waits for the helper.
	pub fn acquire() -> Self {
		Self { acquired: STATE.acquire() }
	}
}

impl Drop for WakeLockGuard {
	fn drop(&mut self) {
		if self.acquired {
			STATE.release();
		}
	}
}

/// Drain the worker's final effect before the process exits.
///
/// On Android this forces the terminal unlocked state and waits up to
/// `SHUTDOWN_TIMEOUT` for the worker to finish; it returns immediately when no
/// wake lock was ever acquired (short CLI runs). A no-op on other platforms.
pub fn shutdown() {
	STATE.shutdown();
}

/// Real state machine: Android, and Linux tests that exercise it.
#[cfg(any(target_os = "android", test))]
mod imp {
	use std::{
		sync::{Arc, Condvar, Mutex},
		thread,
		time::Duration,
	};

	#[cfg(target_os = "android")]
	use std::{
		process::{Command, Stdio},
		time::Instant,
	};

	/// Enough for an in-flight lock (2s) plus a full unlock budget (2s).
	const SHUTDOWN_TIMEOUT: Duration = Duration::from_millis(4_500);
	#[cfg(target_os = "android")]
	const HELPER_TIMEOUT: Duration = Duration::from_secs(2);
	#[cfg(target_os = "android")]
	const HELPER_POLL: Duration = Duration::from_millis(10);

	/// Injected command runner: production runs the Termux helper, tests record.
	pub type Runner = fn(bool);

	struct Shared {
		active: usize,
		desired_locked: bool,
		shutting_down: bool,
		worker_started: bool,
		worker_done: bool,
	}

	struct Inner {
		shared: Mutex<Shared>,
		cv: Condvar,
		runner: Runner,
	}

	/// Refcount plus coalesced desired state, shared with the worker via `Arc`.
	pub struct WakeState {
		inner: Arc<Inner>,
	}

	impl WakeState {
		pub fn new(runner: Runner) -> Self {
			Self {
				inner: Arc::new(Inner {
					shared: Mutex::new(Shared {
						active:          0,
						desired_locked:  false,
						shutting_down:   false,
						worker_started:  false,
						worker_done:     false,
					}),
					cv: Condvar::new(),
					runner,
				}),
			}
		}

		/// Count one holder; starts the single worker on first use. Returns
		/// false when shutdown already began (the guard is then a no-op).
		pub fn acquire(&self) -> bool {
			let mut shared = self.lock();
			if shared.shutting_down {
				return false;
			}
			shared.active = shared.active.saturating_add(1);
			if shared.active == 1 {
				shared.desired_locked = true;
			}
			if !shared.worker_started {
				let inner = Arc::clone(&self.inner);
				let spawned = thread::Builder::new()
					.name("wake-lock-helper".to_owned())
					.spawn(move || worker_loop(inner));
				if spawned.is_ok() {
					shared.worker_started = true;
				}
			}
			self.inner.cv.notify_all();
			true
		}

		pub fn release(&self) {
			let mut shared = self.lock();
			if shared.active == 0 {
				return;
			}
			shared.active -= 1;
			if shared.active == 0 {
				shared.desired_locked = false;
				self.inner.cv.notify_all();
			}
		}

		/// Terminal drain: exit always ends unlocked, even with a leaked guard.
		pub fn shutdown(&self) {
			let mut shared = self.lock();
			if !shared.worker_started {
				return;
			}
			shared.shutting_down = true;
			self.inner.cv.notify_all();
			let _ = self
				.inner
				.cv
				.wait_timeout_while(shared, SHUTDOWN_TIMEOUT, |shared| !shared.worker_done)
				.unwrap_or_else(|poison| poison.into_inner());
		}

		fn lock(&self) -> std::sync::MutexGuard<'_, Shared> {
			self.inner.shared.lock().unwrap_or_else(|poison| poison.into_inner())
		}
	}

	fn worker_loop(inner: Arc<Inner>) {
		let mut shared = inner.shared.lock().unwrap_or_else(|poison| poison.into_inner());
		let mut applied_locked = false;
		loop {
			while !shared.shutting_down && shared.desired_locked == applied_locked {
				shared = inner.cv.wait(shared).unwrap_or_else(|poison| poison.into_inner());
			}
			if shared.shutting_down {
				if !applied_locked {
					shared.worker_done = true;
					inner.cv.notify_all();
					return;
				}
				drop(shared);
				(inner.runner)(false);
				shared = inner.shared.lock().unwrap_or_else(|poison| poison.into_inner());
				shared.worker_done = true;
				inner.cv.notify_all();
				return;
			}
			let target = shared.desired_locked;
			drop(shared);
			(inner.runner)(target);
			shared = inner.shared.lock().unwrap_or_else(|poison| poison.into_inner());
			applied_locked = target;
		}
	}

	/// Run one helper to completion with a bounded wait, then reap it.
	#[cfg(target_os = "android")]
	pub fn run_helper(locked: bool) {
		let command = if locked { "termux-wake-lock" } else { "termux-wake-unlock" };
		let Ok(mut child) = Command::new(command)
			.stdin(Stdio::null())
			.stdout(Stdio::null())
			.stderr(Stdio::null())
			.spawn()
		else {
			// Termux:API absent or not permitted: the turn still runs, just
			// without a CPU lock.
			return;
		};
		let deadline = Instant::now() + HELPER_TIMEOUT;
		loop {
			match child.try_wait() {
				Ok(Some(_)) => return,
				Ok(None) if Instant::now() >= deadline => {
					let _ = child.kill();
					let _ = child.wait();
					return;
				},
				Ok(None) => thread::sleep(HELPER_POLL),
				Err(_) => {
					let _ = child.kill();
					let _ = child.wait();
					return;
				},
			}
		}
	}

	#[cfg(not(target_os = "android"))]
	pub fn run_helper(_locked: bool) {}
}

/// Non-Android production: no helper exists, so the guard is a no-op and
/// `shutdown` cannot wait. Keeps Linux/Windows builds free of dead state.
#[cfg(not(any(target_os = "android", test)))]
mod imp {
	pub struct WakeState;

	impl WakeState {
		pub fn new(_runner: fn(bool)) -> Self {
			Self
		}

		pub fn acquire(&self) -> bool {
			false
		}

		pub fn release(&self) {}

		pub fn shutdown(&self) {}
	}

	pub fn run_helper(_locked: bool) {}
}

#[cfg(test)]
mod tests {
	use std::{sync::Mutex, time::Duration};

	use super::{WakeLockGuard, imp::WakeState};

	static TEST_LOCK: Mutex<()> = Mutex::new(());
	static SINK: Mutex<Vec<&'static str>> = Mutex::new(Vec::new());

	fn clear_sink() {
		SINK.lock().unwrap_or_else(|poison| poison.into_inner()).clear();
	}

	fn recorded() -> Vec<&'static str> {
		SINK.lock().unwrap_or_else(|poison| poison.into_inner()).clone()
	}

	fn push(entry: &'static str) {
		SINK.lock().unwrap_or_else(|poison| poison.into_inner()).push(entry);
	}

	fn push_to_sink(locked: bool) {
		push(if locked { "lock" } else { "unlock" });
	}

	/// A lock that stays in flight briefly, so shutdown has to wait for it.
	fn slow_runner(locked: bool) {
		if locked {
			push("lock");
			std::thread::sleep(Duration::from_millis(300));
		} else {
			push("unlock");
		}
	}

	fn wait_for_effects(expected: usize, timeout: Duration) -> Vec<&'static str> {
		let deadline = std::time::Instant::now() + timeout;
		loop {
			let effects = recorded();
			if effects.len() >= expected {
				return effects;
			}
			assert!(
				std::time::Instant::now() < deadline,
				"timed out waiting for {expected} helper effects; got {effects:?}"
			);
			std::thread::sleep(Duration::from_millis(5));
		}
	}

	#[test]
	fn refcount_locks_first_and_unlocks_last() {
		let _serial = TEST_LOCK.lock().unwrap_or_else(|poison| poison.into_inner());
		clear_sink();
		let state = WakeState::new(push_to_sink);

		assert!(state.acquire());
		wait_for_effects(1, Duration::from_secs(2));

		assert!(state.acquire());
		state.release();
		assert_eq!(recorded(), ["lock"], "one holder must not unlock");

		state.release();
		let effects = wait_for_effects(2, Duration::from_secs(2));
		assert_eq!(effects, ["lock", "unlock"], "the last drop releases the lock");

		// An unbalanced release must stay a no-op, with no third helper call.
		state.release();
		assert_eq!(recorded(), ["lock", "unlock"]);
		state.shutdown();
	}

	#[test]
	fn shutdown_waits_for_inflight_lock_then_unlocks() {
		let _serial = TEST_LOCK.lock().unwrap_or_else(|poison| poison.into_inner());
		clear_sink();
		let state = WakeState::new(slow_runner);

		assert!(state.acquire());
		wait_for_effects(1, Duration::from_secs(2));
		state.release();

		state.shutdown();

		assert_eq!(
			recorded(),
			["lock", "unlock"],
			"shutdown must wait out the in-flight lock and still run the final unlock"
		);
	}

	#[test]
	fn shutdown_before_any_acquire_returns_immediately() {
		let _serial = TEST_LOCK.lock().unwrap_or_else(|poison| poison.into_inner());
		clear_sink();
		let state = WakeState::new(push_to_sink);

		let started = std::time::Instant::now();
		state.shutdown();

		assert!(started.elapsed() < Duration::from_millis(100), "shutdown waited with no worker");
		assert!(recorded().is_empty());
	}

	#[test]
	fn shutdown_forces_unlock_even_with_a_leaked_holder() {
		let _serial = TEST_LOCK.lock().unwrap_or_else(|poison| poison.into_inner());
		clear_sink();
		let state = WakeState::new(push_to_sink);

		assert!(state.acquire());
		wait_for_effects(1, Duration::from_secs(2));
		state.shutdown(); // no release: exit must still unlock

		assert_eq!(recorded(), ["lock", "unlock"]);
	}

	#[test]
	fn guard_round_trip_uses_the_process_refcount() {
		// Production path on host CI: the default runner is a no-op, so this
		// only proves the guard balances the global refcount.
		drop(WakeLockGuard::acquire());
	}
}
