//! Process crash diagnostics for the headless `arag` runtime.
//!
//! [`install`] is called once from `main` and replaces the panic hook with one
//! that writes an actionable record (thread, pid, location, payload,
//! backtrace) to `<state>/logs/` before chaining to the default hook. On
//! non-Android nightly builds it also installs the unstable allocation-error
//! hook, which records the failing layout instead of letting the process abort
//! with only `memory allocation of N bytes failed`.
//!
//! Panics raised inside a recovery boundary (a shell/builtin body that is
//! about to be caught and surfaced as a failed command) are persisted to disk
//! only: no stderr dump and no chaining, so a recovered command does not look
//! like a crash. The boundary predicate is injected at install time so this
//! crate keeps no dependency on the shell crate; the caller passes
//! `pi_shell::panic_scope_active` when that re-export is available.
//!
//! - Backtraces use [`Backtrace::force_capture`], independent of
//!   `RUST_BACKTRACE`.
//! - Logs live under `crate::dirs::root_subdir("logs", State)`, the same
//!   sibling of `agent/` the JS logger rotates.
//! - Installation is idempotent ([`Once`]).

use std::{
	backtrace::Backtrace,
	cell::Cell,
	fmt::Write as _,
	fs::{self, OpenOptions},
	io::Write as _,
	path::{Path, PathBuf},
	process,
	sync::Once,
	thread,
	time::{SystemTime, UNIX_EPOCH},
};

#[cfg(not(target_os = "android"))]
use std::{
	alloc::Layout,
	sync::atomic::{AtomicBool, Ordering},
};

use crate::dirs::{AgentDirCategory, root_subdir};

/// Filename prefix; the crash kind, pid, and unix-ms timestamp follow.
const LOG_PREFIX: &str = "arag";

static INSTALL: Once = Once::new();

#[cfg(not(target_os = "android"))]
static ALLOC_HOOK_ACTIVE: AtomicBool = AtomicBool::new(false);

thread_local! {
	/// Set while the panic hook formats/persists a report. A panic raised by
	/// that work re-enters the hook; it must not recurse into formatting again.
	static IN_HOOK: Cell<bool> = const { Cell::new(false) };
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum PanicDisposition {
	/// No recovery boundary is active: persist, echo to stderr, and chain to
	/// the previous hook.
	Fatal,
	/// The panic will be caught and surfaced as a failed command: persist for
	/// diagnosis only, keep stderr quiet, and do not chain.
	LoggedRecoverable,
}

/// Install the hooks with no recoverable-panic boundary. Idempotent.
pub fn install() {
	install_with_probe(None);
}

/// Install with an optional `recoverable_panic` predicate.
///
/// `Some(probe)` marks a panic as recoverable when `probe()` is true on the
/// panicking thread (for example `pi_shell::panic_scope_active`). `None`
/// treats every panic as fatal. The first installation wins; later calls are
/// no-ops, so the probe must be supplied at process startup.
pub fn install_with_probe(recoverable_panic: Option<fn() -> bool>) {
	INSTALL.call_once(|| {
		let probe = recoverable_panic.unwrap_or(never_recoverable);
		let prev_panic = std::panic::take_hook();
		std::panic::set_hook(Box::new(move |info| {
			if IN_HOOK.try_with(Cell::get).unwrap_or(false) {
				// A panic while formatting the report: emit one fixed line and
				// stop. Delegating to the previous hook here could recurse if
				// that hook also panics.
				let _ = writeln!(std::io::stderr(), "arag: panic while formatting a crash report");
				return;
			}
			let _reentry = ReentryGuard::new();
			match panic_disposition(probe) {
				PanicDisposition::LoggedRecoverable => {
					persist(&format_panic_report(info), CrashKind::Panic, false);
				},
				PanicDisposition::Fatal => {
					persist(&format_panic_report(info), CrashKind::Panic, true);
					prev_panic(info);
				},
			}
		}));

		#[cfg(not(target_os = "android"))]
		std::alloc::set_alloc_error_hook(|layout| {
			// Canonical line before any allocation-prone work: under genuine
			// process-wide OOM the formatting below can re-enter this hook; the
			// secondary entry aborts immediately after repeating the line.
			write_alloc_failure_line(std::io::stderr(), layout.size());
			if ALLOC_HOOK_ACTIVE.swap(true, Ordering::AcqRel) {
				process::abort();
			}
			persist(&format_alloc_report(layout), CrashKind::Alloc, true);
			process::abort();
		});
	});
}

const fn never_recoverable() -> bool {
	false
}

/// Clears [`IN_HOOK`] on every exit path, including the fatal path that calls
/// the previous hook.
struct ReentryGuard;

impl Drop for ReentryGuard {
	fn drop(&mut self) {
		let _ = IN_HOOK.try_with(|flag| flag.set(false));
	}
}

// The guard's constructor is the only way in; keep the set/clear paired here.
impl ReentryGuard {
	fn new() -> Self {
		let _ = IN_HOOK.try_with(|flag| flag.set(true));
		Self
	}
}

fn panic_disposition(probe: fn() -> bool) -> PanicDisposition {
	if probe() { PanicDisposition::LoggedRecoverable } else { PanicDisposition::Fatal }
}

#[derive(Clone, Copy)]
enum CrashKind {
	Panic,
	#[cfg(not(target_os = "android"))]
	Alloc,
}

impl CrashKind {
	const fn as_str(self) -> &'static str {
		match self {
			Self::Panic => "panic",
			#[cfg(not(target_os = "android"))]
			Self::Alloc => "alloc",
		}
	}
}

fn format_panic_report(info: &std::panic::PanicHookInfo<'_>) -> String {
	let location = info.location().map_or_else(
		|| String::from("<unknown>"),
		|l| format!("{}:{}:{}", l.file(), l.line(), l.column()),
	);
	format_panic_report_parts(&location, &panic_payload(info.payload()))
}

/// Report body without a [`std::panic::PanicHookInfo`], so formatting is unit
/// testable (the hook info type cannot be constructed from a test).
fn format_panic_report_parts(location: &str, message: &str) -> String {
	let backtrace = Backtrace::force_capture();
	let mut out = report_header(CrashKind::Panic);
	let _ = writeln!(out, "location: {location}");
	let _ = writeln!(out, "message:  {message}");
	let _ = writeln!(out, "backtrace:\n{backtrace}");
	out
}

#[cfg(not(target_os = "android"))]
fn format_alloc_report(layout: Layout) -> String {
	// Capturing a backtrace allocates; `force_capture` swallows a secondary
	// failure and returns an empty trace, still better than nothing.
	let backtrace = Backtrace::force_capture();
	let mut out = report_header(CrashKind::Alloc);
	let _ = writeln!(out, "size:      {} bytes", layout.size());
	let _ = writeln!(out, "alignment: {} bytes", layout.align());
	let _ = writeln!(out, "backtrace:\n{backtrace}");
	out
}

fn report_header(kind: CrashKind) -> String {
	let thread_name = thread::current().name().unwrap_or("<unnamed>").to_owned();
	format!(
		"arag {kind} crash\npid:       {pid}\nthread:    {thread_name}\ntimestamp: {now_ms} \
		 (unix ms)\n",
		kind = kind.as_str(),
		pid = process::id(),
		now_ms = unix_millis(),
	)
}

#[cfg(not(target_os = "android"))]
fn write_alloc_failure_line(mut out: impl std::io::Write, size: usize) {
	let _ = out.write_all(b"memory allocation of ");
	let mut digits = [0u8; usize::MAX.ilog10() as usize + 1];
	let mut pos = digits.len();
	let mut value = size;
	if value == 0 {
		pos -= 1;
		digits[pos] = b'0';
	} else {
		while value > 0 {
			pos -= 1;
			digits[pos] = b'0' + (value % 10) as u8;
			value /= 10;
		}
	}
	let _ = out.write_all(&digits[pos..]);
	let _ = out.write_all(b" bytes failed\n");
}

/// Printable panic message: `panic!` produces `&'static str` or `String`;
/// `panic_any` payloads degrade to a sentinel.
fn panic_payload(payload: &(dyn std::any::Any + Send)) -> String {
	if let Some(s) = payload.downcast_ref::<&'static str>() {
		(*s).to_owned()
	} else if let Some(s) = payload.downcast_ref::<String>() {
		s.clone()
	} else {
		String::from("<non-string panic payload>")
	}
}

fn persist(report: &str, kind: CrashKind, echo_stderr: bool) {
	// Echo first so the user sees the report even when the file write fails
	// (read-only state dir). Recoverable panics stay quiet.
	if echo_stderr {
		let _ = writeln!(std::io::stderr(), "{report}");
	}

	let path = crash_log_path(kind);
	if let Some(parent) = path.parent() {
		let _ = fs::create_dir_all(parent);
	}
	if let Ok(mut file) = OpenOptions::new().create(true).append(true).open(&path) {
		let _ = file.write_all(report.as_bytes());
		let _ = file.flush();
		let _ = file.sync_data();
		if echo_stderr {
			let _ = writeln!(
				std::io::stderr(),
				"arag crash report written to {}",
				path.display()
			);
		}
	}
}

fn crash_log_path(kind: CrashKind) -> PathBuf {
	build_crash_log_path(
		&root_subdir("logs", AgentDirCategory::State),
		kind,
		process::id(),
		unix_millis(),
	)
}

fn build_crash_log_path(dir: &Path, kind: CrashKind, pid: u32, now_ms: u128) -> PathBuf {
	dir.join(format!("{LOG_PREFIX}-{}-{pid}-{now_ms}.log", kind.as_str()))
}

fn unix_millis() -> u128 {
	SystemTime::now()
		.duration_since(UNIX_EPOCH)
		.map_or(0, |d| d.as_millis())
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn panic_report_contains_header_location_message_and_backtrace() {
		let report = format_panic_report_parts("src/main.rs:10:5", "boom");
		assert!(report.contains("arag panic crash"), "missing header: {report}");
		assert!(report.contains("pid:       "), "missing pid: {report}");
		assert!(report.contains("location: src/main.rs:10:5"), "missing location: {report}");
		assert!(report.contains("message:  boom"), "missing message: {report}");
		assert!(report.contains("backtrace:"), "missing backtrace section: {report}");
	}

	#[test]
	fn recoverable_probe_skips_the_fatal_disposition() {
		fn recoverable() -> bool {
			true
		}
		fn fatal() -> bool {
			false
		}
		assert_eq!(panic_disposition(recoverable), PanicDisposition::LoggedRecoverable);
		assert_eq!(panic_disposition(fatal), PanicDisposition::Fatal);
	}

	#[test]
	fn crash_log_path_is_a_state_logs_file() {
		let path = build_crash_log_path(
			Path::new("/state/logs"),
			CrashKind::Panic,
			42,
			1_700_000_000_123,
		);
		assert_eq!(
			path,
			PathBuf::from("/state/logs/arag-panic-42-1700000000123.log")
		);
	}

	#[test]
	fn panic_payload_handles_str_string_and_other() {
		let static_str: Box<dyn std::any::Any + Send> = Box::new("static panic");
		assert_eq!(panic_payload(&*static_str), "static panic");

		let owned: Box<dyn std::any::Any + Send> = Box::new(String::from("owned panic"));
		assert_eq!(panic_payload(&*owned), "owned panic");

		let other: Box<dyn std::any::Any + Send> = Box::new(42u32);
		assert_eq!(panic_payload(&*other), "<non-string panic payload>");
	}

	#[cfg(not(target_os = "android"))]
	#[test]
	fn alloc_report_contains_size_alignment_and_backtrace() {
		let layout = Layout::from_size_align(7714, 8).expect("layout");
		let report = format_alloc_report(layout);
		assert!(report.contains("arag alloc crash"), "missing header: {report}");
		assert!(report.contains("size:      7714 bytes"), "missing size: {report}");
		assert!(report.contains("alignment: 8 bytes"), "missing alignment: {report}");
		assert!(report.contains("backtrace:"), "missing backtrace section: {report}");
	}

	#[cfg(not(target_os = "android"))]
	#[test]
	fn alloc_failure_line_matches_rust_default_text() {
		let mut buf = Vec::new();
		write_alloc_failure_line(&mut buf, 7714);
		assert_eq!(buf, b"memory allocation of 7714 bytes failed\n");
		buf.clear();
		write_alloc_failure_line(&mut buf, usize::MAX);
		assert_eq!(buf, format!("memory allocation of {} bytes failed\n", usize::MAX).as_bytes());
	}
}
