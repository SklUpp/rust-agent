//! Interactive terminal front-end for the default `arag` entry point.
//!
//! The TUI is a *view* over the one agent loop: a turn runs on a scoped worker
//! thread through `Session::turn` (journal, ack, goal and persistence untouched)
//! and streams the same [`Event`]s the headless reporter sees. The main thread
//! owns the terminal, keys, dialogs and rendering. No second runtime, no second
//! stdin reader: approvals and `ask` are answered through channel requests, and
//! the legacy stdin readers are bypassed while raw mode is active.
//!
//! Deliberate limits are documented where they matter: no mouse, no inline
//! image protocol, no config editing (the CLI/config layer exposes no writable
//! source path), transcript scrollback is bounded by the session history and
//! the subagent viewer shows one page of the child log.

#[cfg(unix)]
pub(crate) use imp::run;

#[cfg(not(unix))]
pub(crate) fn run(
	_session: &mut crate::Session<'_>,
	_runner: &std::sync::Arc<aragent::runtime::PiAgentRunner>,
) -> aragent::Result<()> {
	Err(aragent::Error::new(
		"interactive TUI requires a POSIX terminal; use `arag repl`",
	))
}

#[cfg(unix)]
mod imp {
	use std::collections::{BTreeMap, BTreeSet, VecDeque};
	use std::io::{self, Write};
	use std::panic::{AssertUnwindSafe, catch_unwind};
	use std::sync::atomic::{AtomicBool, AtomicPtr, Ordering};
	use std::sync::mpsc::{self, Receiver, Sender, TryRecvError};
	use std::sync::Arc;
	use std::time::{Duration, Instant};

	use async_trait::async_trait;
	use aragent::agent::Event;
	use aragent::approve::{
		Approvals, Decision, classify_tool, extract_tool_detail,
	};
	use aragent::output::{OutputMode, Reporter};
	use aragent::runtime::PiAgentRunner;
	use pi_agent::ContentPart;
	use pi_agent::supervisor::{
		SubagentEvent, SubagentLifecycleStatus, SubagentObserver,
	};
	use pi_agent::tools::ask::{
		AskAnswer, AskChannel, AskOutcome, AskQuestion,
	};
	use tokio_util::sync::CancellationToken;

	use aragent::Result;
	use crate::Session;

	/// One message from a worker turn, an observer, or an approval/ask request.
	///
	/// Only streaming/model events use [`Event`]; tool events arrive as narrow
	/// variants so no full tool result (potentially megabytes) is cloned per
	/// event on the turn thread, and `MessageEnd`/`TurnEnd`/`AgentEnd` are never
	/// copied at all.
	enum UiMsg {
		Event(Event),
		ToolStart {
			id: String,
			tool: String,
			args: serde_json::Value,
		},
		ToolUpdate {
			id: String,
			tool: String,
			partial: serde_json::Value,
		},
		ToolEnd {
			id: String,
			tool: String,
			error: Option<String>,
		},
		Advisor(String),
		Subagent(SubagentEvent),
		Approval {
			name: String,
			detail: Option<String>,
			reply: tokio::sync::oneshot::Sender<bool>,
		},
		Ask {
			questions: Vec<AskQuestion>,
			cancel: CancellationToken,
			reply: tokio::sync::oneshot::Sender<AskOutcome>,
		},
		TurnError(String),
		TurnDone,
	}

	// ── Session surfaces ───────────────────────────────────────────────────

	struct TuiApprover {
		policy: Approvals,
		tx: Sender<UiMsg>,
	}

	#[async_trait]
	impl pi_agent::ToolApprover for TuiApprover {
		fn ask_channel(&self) -> Option<Arc<dyn AskChannel>> {
			// The session's `ask` surface is installed separately.
			None
		}

		async fn approve(
			&self,
			name: &str,
			args: &serde_json::Value,
		) -> std::result::Result<(), String> {
			let tier = classify_tool(name, args);
			if self.policy.decide_tier(name, tier) == Decision::Auto {
				return Ok(());
			}
			let (reply, rx) = tokio::sync::oneshot::channel();
			self
				.tx
				.send(UiMsg::Approval {
					name: name.to_string(),
					detail: extract_tool_detail(name, args).or_else(|| {
						serde_json::to_string(args).ok().map(|text| clip(&text, 400))
					}),
					reply,
				})
				.map_err(|_| "approval denied: terminal UI closed".to_string())?;
			match rx.await {
				Ok(true) => Ok(()),
				Ok(false) => Err(format!("approval denied for {name}")),
				Err(_) => Err("approval denied: terminal UI closed".to_string()),
			}
		}
	}

	struct TuiAsk {
		tx: Sender<UiMsg>,
	}

	#[async_trait]
	impl AskChannel for TuiAsk {
		async fn ask(
			&self,
			questions: &[AskQuestion],
			cancel: CancellationToken,
		) -> AskOutcome {
			let (reply, rx) = tokio::sync::oneshot::channel();
			if self
				.tx
				.send(UiMsg::Ask {
					questions: questions.to_vec(),
					cancel: cancel.clone(),
					reply,
				})
				.is_err()
			{
				return AskOutcome::Aborted;
			}
			tokio::select! {
				() = cancel.cancelled() => AskOutcome::Aborted,
				outcome = rx => outcome.unwrap_or(AskOutcome::Aborted),
			}
		}
	}

	struct TuiObserver {
		tx: Sender<UiMsg>,
	}

	impl SubagentObserver for TuiObserver {
		fn observe(&self, event: &SubagentEvent) {
			// `Event` is ignored by the worker panel; filter it before the
			// clone so per-child event payloads never cross the channel.
			let message = match event {
				SubagentEvent::Lifecycle(lifecycle) => {
					Some(UiMsg::Subagent(SubagentEvent::Lifecycle(
						lifecycle.clone(),
					)))
				},
				SubagentEvent::Progress(progress) => {
					Some(UiMsg::Subagent(SubagentEvent::Progress(
						progress.clone(),
					)))
				},
				SubagentEvent::Event { .. } => None,
			};
			if let Some(message) = message {
				let _ = self.tx.send(message);
			}
		}
	}

	/// Install the UI-backed approval/ask surfaces and the worker observer.
	/// Children get a stripped approver (`without_ask`) and an empty injected
	/// input, so no child turn can read the terminal the TUI owns.
	fn install_surfaces(session: &mut Session<'_>, tx: &Sender<UiMsg>) {
		let policy = Approvals::new(
			session.approvals.mode,
			session.approvals.allow.iter().cloned(),
		);
		session.approvals.set_approver(Some(Arc::new(TuiApprover {
			policy,
			tx: tx.clone(),
		})));
		session.approvals.set_ask(Some(Arc::new(TuiAsk { tx: tx.clone() })));
		let empty: Box<dyn io::BufRead + Send> = Box::new(io::Cursor::new(Vec::<u8>::new()));
		session
			.approvals
			.set_input(Some(Arc::new(std::sync::Mutex::new(empty))));
		let observer: Arc<dyn SubagentObserver> = Arc::new(TuiObserver { tx: tx.clone() });
		session.set_subagent_observer(Some(aragent::agent::ObserverHandle(observer)));
	}

	// ── Terminal (RAII raw mode + alternate screen) ────────────────────────

	static SAVED_TTY: AtomicPtr<libc::termios> = AtomicPtr::new(std::ptr::null_mut());
	static TTY_ACTIVE: AtomicBool = AtomicBool::new(false);

	/// Async-signal-safe restore used by SIGTERM/SIGHUP/SIGINT handlers before
	/// the process re-raises the signal. It performs only lock-free atomic
	/// loads plus `tcsetattr(2)`, `write(2)`, `signal(2)` and `raise(2)` — all
	/// on POSIX's async-signal-safe list. No allocation, locks, formatting or
	/// Rust runtime calls; the `termios` storage is leaked and outlives the
	/// handler, and `TTY_ACTIVE` orders it against `Terminal::drop`.
	extern "C" fn restore_on_signal(sig: libc::c_int) {
		if TTY_ACTIVE.load(Ordering::SeqCst) {
			let saved = SAVED_TTY.load(Ordering::SeqCst);
			if !saved.is_null() {
				// SAFETY: the pointer is a leaked `termios` owned by the live
				// `Terminal`; it stays valid until after `TTY_ACTIVE` is cleared.
				unsafe {
					libc::tcsetattr(libc::STDIN_FILENO, libc::TCSANOW, saved);
				}
			}
			let reset = b"\x1b[0m\x1b[?25h\x1b[?1049l\r\n";
			// SAFETY: fixed buffer and the process's stdout descriptor.
			unsafe {
				libc::write(libc::STDOUT_FILENO, reset.as_ptr().cast(), reset.len());
			}
		}
		// SAFETY: restore the default disposition and re-raise so the exit
		// status still reflects the signal.
		unsafe {
			libc::signal(sig, libc::SIG_DFL);
			libc::raise(sig);
		}
	}

	struct Terminal {
		out: io::Stdout,
		saved: libc::termios,
		old_term: libc::sighandler_t,
		old_hup: libc::sighandler_t,
		old_int: libc::sighandler_t,
	}

	impl Terminal {
		fn enter() -> Result<Self> {
			// SAFETY: zeroed `termios` is valid for `tcgetattr` to fill.
			let mut saved: libc::termios = unsafe { std::mem::zeroed() };
			// SAFETY: stdin is the terminal the routing check required.
			if unsafe { libc::tcgetattr(libc::STDIN_FILENO, &mut saved) } != 0 {
				return Err(aragent::Error::new("cannot read terminal attributes"));
			}
			let mut raw = saved;
			raw.c_iflag &= !(libc::IXON
				| libc::ICRNL
				| libc::INLCR
				| libc::IGNCR
				| libc::BRKINT
				| libc::INPCK
				| libc::ISTRIP);
			raw.c_oflag &= !libc::OPOST;
			raw.c_lflag &= !(libc::ICANON
				| libc::ECHO
				| libc::ECHOE
				| libc::ECHOK
				| libc::ECHONL
				| libc::ISIG
				| libc::IEXTEN);
			raw.c_cflag |= libc::CS8;
			raw.c_cc[libc::VMIN] = 0;
			raw.c_cc[libc::VTIME] = 0;

			// Publish the saved terminal and install the restore handlers
			// *before* switching to raw mode: a signal in any later window then
			// restores valid attributes instead of leaving the tty raw.
			let leaked = Box::into_raw(Box::new(saved));
			SAVED_TTY.store(leaked, Ordering::SeqCst);
			TTY_ACTIVE.store(true, Ordering::SeqCst);
			let handler = restore_on_signal as *const () as libc::sighandler_t;
			// SAFETY: installing a handler with an async-signal-safe body.
			let old_term = unsafe { libc::signal(libc::SIGTERM, handler) };
			// SAFETY: same shape as SIGTERM.
			let old_hup = unsafe { libc::signal(libc::SIGHUP, handler) };
			// Raw mode delivers the keyboard Ctrl-C as a byte; an *external*
			// SIGINT must still restore the terminal before the process dies.
			// SAFETY: same shape as SIGTERM.
			let old_int = unsafe { libc::signal(libc::SIGINT, handler) };

			// SAFETY: descriptor is the terminal; `raw` is a full attrs struct.
			if unsafe { libc::tcsetattr(libc::STDIN_FILENO, libc::TCSAFLUSH, &raw) } != 0 {
				TTY_ACTIVE.store(false, Ordering::SeqCst);
				SAVED_TTY.store(std::ptr::null_mut(), Ordering::SeqCst);
				// SAFETY: handlers were installed above, nothing else owns them.
				unsafe {
					libc::signal(libc::SIGTERM, old_term);
					libc::signal(libc::SIGHUP, old_hup);
					libc::signal(libc::SIGINT, old_int);
				}
				return Err(aragent::Error::new("cannot enter raw terminal mode"));
			}

			let mut terminal = Self {
				out: io::stdout(),
				saved,
				old_term,
				old_hup,
				old_int,
			};
			write!(terminal.out, "\x1b[?1049h\x1b[H\x1b[2J\x1b[?25l")?;
			terminal.out.flush()?;
			Ok(terminal)
		}

		fn size(&self) -> (usize, usize) {
			let mut size = libc::winsize {
				ws_row: 0,
				ws_col: 0,
				ws_xpixel: 0,
				ws_ypixel: 0,
			};
			// SAFETY: writable out-parameter for the process's stdout.
			let ok = unsafe {
				libc::ioctl(libc::STDOUT_FILENO, libc::TIOCGWINSZ, &mut size)
			} == 0;
			if ok && size.ws_col > 0 && size.ws_row > 0 {
				(usize::from(size.ws_col), usize::from(size.ws_row))
			} else {
				(80, 24)
			}
		}

		fn draw(&mut self, app: &mut App) -> Result<()> {
			let (width, height) = self.size();
			app.width = width;
			app.height = height;
			let (rows, cursor_row, cursor_col) = app.render();
			let mut frame = String::with_capacity(rows.len() * (width + 8) + 32);
			frame.push_str("\x1b[?25l");
			for (index, row) in rows.iter().enumerate() {
				frame.push_str(&format!("\x1b[{};1H", index + 1));
				frame.push_str(row);
				frame.push_str("\x1b[K");
			}
			frame.push_str(&format!(
				"\x1b[{};{}H\x1b[?25h",
				cursor_row.max(1),
				cursor_col.max(1)
			));
			self.out.write_all(frame.as_bytes())?;
			self.out.flush()?;
			Ok(())
		}

	}

	impl Drop for Terminal {
		fn drop(&mut self) {
			let saved = self.saved;
			// Restore the visible state first, while `TTY_ACTIVE` is still true:
			// a signal arriving during this drop then performs the same
			// idempotent restore instead of leaving raw mode/alt-screen behind.
			let _ = write!(self.out, "\x1b[0m\x1b[?25h\x1b[?1049l");
			let _ = self.out.flush();
			// SAFETY: restoring the captured attributes on the terminal.
			unsafe {
				libc::tcsetattr(libc::STDIN_FILENO, libc::TCSANOW, &saved);
			}
			TTY_ACTIVE.store(false, Ordering::SeqCst);
			SAVED_TTY.store(std::ptr::null_mut(), Ordering::SeqCst);
			// SAFETY: restore the previous dispositions for this process.
			unsafe {
				libc::signal(libc::SIGTERM, self.old_term);
				libc::signal(libc::SIGHUP, self.old_hup);
				libc::signal(libc::SIGINT, self.old_int);
			}
		}
	}

	// ── Input ──────────────────────────────────────────────────────────────

	#[derive(Debug, Clone, Copy, PartialEq, Eq)]
	enum Key {
		Char(char),
		Enter,
		Backspace,
		Delete,
		Left,
		Right,
		Up,
		Down,
		Home,
		End,
		PageUp,
		PageDown,
		Tab,
		Esc,
		CtrlC,
		CtrlD,
		CtrlL,
		CtrlU,
	}

	/// Parse complete key sequences, retaining an incomplete UTF-8 suffix.
	fn parse_keys(buffer: &mut Vec<u8>) -> Vec<Key> {
		let mut keys = Vec::new();
		let mut index = 0;
		while index < buffer.len() {
			let byte = buffer[index];
			match byte {
				0x1b => {
					if index + 1 >= buffer.len() {
						keys.push(Key::Esc);
						index += 1;
					} else if buffer[index + 1] == b'[' {
						let mut end = index + 2;
						while end < buffer.len()
							&& !(0x40..=0x7e).contains(&buffer[end])
						{
							end += 1;
						}
						if end >= buffer.len() {
							break;
						}
						let params = &buffer[index + 2..end];
						let key = match (buffer[end], params) {
							(b'A', _) => Some(Key::Up),
							(b'B', _) => Some(Key::Down),
							(b'C', _) => Some(Key::Right),
							(b'D', _) => Some(Key::Left),
							(b'H', _) => Some(Key::Home),
							(b'F', _) => Some(Key::End),
							(b'~', b"1") => Some(Key::Home),
							(b'~', b"3") => Some(Key::Delete),
							(b'~', b"4") => Some(Key::End),
							(b'~', b"5") => Some(Key::PageUp),
							(b'~', b"6") => Some(Key::PageDown),
							(b'~', b"7") => Some(Key::Home),
							(b'~', b"8") => Some(Key::End),
							_ => None,
						};
						if let Some(key) = key {
							keys.push(key);
						}
						index = end + 1;
					} else if buffer[index + 1] == b'O' {
						if index + 2 >= buffer.len() {
							break;
						}
						match buffer[index + 2] {
							b'H' => keys.push(Key::Home),
							b'F' => keys.push(Key::End),
							b'A' => keys.push(Key::Up),
							b'B' => keys.push(Key::Down),
							b'C' => keys.push(Key::Right),
							b'D' => keys.push(Key::Left),
							_ => {},
						}
						index += 3;
					} else {
						keys.push(Key::Esc);
						index += 1;
					}
				},
				0x03 => {
					keys.push(Key::CtrlC);
					index += 1;
				},
				0x04 => {
					keys.push(Key::CtrlD);
					index += 1;
				},
				0x0c => {
					keys.push(Key::CtrlL);
					index += 1;
				},
				0x15 => {
					keys.push(Key::CtrlU);
					index += 1;
				},
				b'\r' | b'\n' => {
					keys.push(Key::Enter);
					index += 1;
				},
				0x7f | 0x08 => {
					keys.push(Key::Backspace);
					index += 1;
				},
				b'\t' => {
					keys.push(Key::Tab);
					index += 1;
				},
				b if b < 0x20 => index += 1,
				0x20..=0x7e => {
					keys.push(Key::Char(char::from(byte)));
					index += 1;
				},
				_ => {
					let length = utf8_len(byte);
					if length == 0 {
						index += 1;
					} else if index + length > buffer.len() {
						break;
					} else {
						if let Ok(text) = std::str::from_utf8(&buffer[index..index + length])
							&& let Some(ch) = text.chars().next()
						{
							keys.push(Key::Char(ch));
						}
						index += length;
					}
				},
			}
		}
		buffer.drain(..index);
		keys
	}

	fn utf8_len(first: u8) -> usize {
		match first {
			0xc2..=0xdf => 2,
			0xe0..=0xef => 3,
			0xf0..=0xf4 => 4,
			_ => 0,
		}
	}

	#[derive(Default)]
	struct LineInput {
		chars: Vec<char>,
		cursor: usize,
	}

	impl LineInput {
		fn text(&self) -> String {
			self.chars.iter().collect()
		}

		fn is_empty(&self) -> bool {
			self.chars.is_empty()
		}

		fn set_text(&mut self, text: &str) {
			self.chars = text.chars().collect();
			self.cursor = self.chars.len();
		}

		fn clear(&mut self) {
			self.chars.clear();
			self.cursor = 0;
		}

		fn insert(&mut self, ch: char) {
			self.chars.insert(self.cursor, ch);
			self.cursor += 1;
		}

		fn backspace(&mut self) {
			if self.cursor > 0 {
				self.cursor -= 1;
				self.chars.remove(self.cursor);
			}
		}

		fn delete(&mut self) {
			if self.cursor < self.chars.len() {
				self.chars.remove(self.cursor);
			}
		}

		fn prefix(&self) -> String {
			self.chars[..self.cursor].iter().collect()
		}
	}

	// ── Width / wrapping (Unicode-approximate, terminal-safe) ──────────────

	fn char_width(ch: char) -> usize {
		let code = ch as u32;
		if code == 0
			|| (0x0300..=0x036f).contains(&code)
			|| (0x1ab0..=0x1aff).contains(&code)
			|| (0x1dc0..=0x1dff).contains(&code)
			|| (0x20d0..=0x20ff).contains(&code)
			|| (0xfe20..=0xfe2f).contains(&code)
			|| (0x200b..=0x200f).contains(&code)
		{
			return 0;
		}
		if (0x1100..=0x115f).contains(&code)
			|| (0x2e80..=0xa4cf).contains(&code)
			|| (0xac00..=0xd7a3).contains(&code)
			|| (0xf900..=0xfaff).contains(&code)
			|| (0xfe30..=0xfe4f).contains(&code)
			|| (0xff00..=0xff60).contains(&code)
			|| (0xffe0..=0xffe6).contains(&code)
			|| (0x1f300..=0x1f64f).contains(&code)
			|| (0x1f900..=0x1f9ff).contains(&code)
			|| (0x20000..=0x3fffd).contains(&code)
		{
			return 2;
		}
		1
	}

	fn display_width(text: &str) -> usize {
		text.chars().map(char_width).sum()
	}

	/// Strip terminal control characters from model/tool output: a title or a
	/// tool result must never inject escapes into the operator's terminal.
	fn sanitize(text: &str) -> String {
		let mut out = String::with_capacity(text.len());
		for ch in text.chars() {
			match ch {
				'\n' => out.push('\n'),
				'\t' => out.push_str("    "),
				'\r' => {},
				ch if (ch as u32) < 0x20 => {},
				'\u{7f}' => {},
				ch if ('\u{80}'..='\u{9f}').contains(&ch) => {},
				ch => out.push(ch),
			}
		}
		out
	}

	fn clip(text: &str, max: usize) -> String {
		let mut out = String::new();
		let mut width = 0;
		for ch in text.chars() {
			let ch_width = char_width(ch);
			if width + ch_width > max {
				if width + 1 <= max {
					out.push('…');
				}
				break;
			}
			out.push(ch);
			width += ch_width;
		}
		out
	}

	fn wrap_line(line: &str, width: usize) -> Vec<String> {
		let width = width.max(1);
		let mut out = Vec::new();
		let mut current = String::new();
		let mut current_width = 0;
		for ch in line.chars() {
			let ch_width = char_width(ch);
			if current_width + ch_width > width && !current.is_empty() {
				if let Some(index) = current.rfind(' ') {
					let head = current[..index].trim_end().to_string();
					let tail = current[index + 1..].to_string();
					out.push(head);
					current = tail;
					current_width = display_width(&current);
				} else {
					out.push(std::mem::take(&mut current));
					current_width = 0;
				}
			}
			if ch == ' ' && current.is_empty() {
				continue;
			}
			current.push(ch);
			current_width += ch_width;
		}
		if !current.is_empty() || out.is_empty() {
			out.push(current);
		}
		out
	}

	fn wrap_text(text: &str, width: usize) -> Vec<String> {
		let mut out = Vec::new();
		for line in text.split('\n') {
			out.extend(wrap_line(line, width));
		}
		out
	}

	// ── App state ──────────────────────────────────────────────────────────

	#[derive(Clone, Copy, PartialEq, Eq)]
	enum Kind {
		User,
		Assistant,
		Thinking,
		Tool,
		ToolError,
		System,
		Advisor,
		Separator,
	}

	impl Kind {
		fn style(self) -> &'static str {
			match self {
				Self::User => "\x1b[1;36m",
				Self::Assistant => "",
				Self::Thinking => "\x1b[2;3m",
				Self::Tool => "\x1b[36m",
				Self::ToolError => "\x1b[31m",
				Self::System => "\x1b[2m",
				Self::Advisor => "\x1b[35m",
				Self::Separator => "\x1b[2m",
			}
		}
	}

	struct Entry {
		kind: Kind,
		text: String,
	}

	struct WorkerRow {
		index: usize,
		agent: String,
		status: String,
		task: String,
		session_file: Option<String>,
	}

	impl WorkerRow {
		fn running(&self) -> bool {
			!matches!(
				self.status.as_str(),
				"completed" | "failed" | "aborted"
			)
		}
	}

	/// Session facts mirrored for the header; the live `Session` is borrowed by
	/// the worker while a turn runs, so the idle state is snapshotted.
	struct Status {
		model: String,
		provider: String,
		cwd: String,
		session: String,
		approvals: String,
		tools_enabled: bool,
	}

	enum View {
		Chat,
		Text { title: String, lines: Vec<String> },
	}

	struct AskState {
		questions: Vec<AskQuestion>,
		index: usize,
		answers: Vec<AskAnswer>,
		selected: BTreeSet<usize>,
		custom: Option<String>,
		custom_mode: bool,
		custom_input: LineInput,
		reply: tokio::sync::oneshot::Sender<AskOutcome>,
		cancel: CancellationToken,
		deadline: Option<Instant>,
	}

	impl AskState {
		fn new(
			questions: Vec<AskQuestion>,
			reply: tokio::sync::oneshot::Sender<AskOutcome>,
			cancel: CancellationToken,
			timeout: Option<Duration>,
		) -> Self {
			Self {
				questions,
				index: 0,
				answers: Vec::new(),
				selected: BTreeSet::new(),
				custom: None,
				custom_mode: false,
				custom_input: LineInput::default(),
				reply,
				cancel,
				deadline: timeout.map(|timeout| Instant::now() + timeout),
			}
		}

		fn current(&self) -> Option<&AskQuestion> {
			self.questions.get(self.index)
		}

		fn expired(&self) -> bool {
			self.deadline.is_some_and(|deadline| Instant::now() >= deadline)
		}

		fn answer(&mut self, timed_out: bool) -> Option<AskOutcome> {
			let question = self.questions.get(self.index)?.clone();
			let selected: Vec<usize> = if question.multi {
				self.selected.iter().copied().collect()
			} else if self.selected.is_empty() {
				let recommended = question
					.recommended
					.and_then(|value| usize::try_from(value).ok())
					.filter(|value| *value < question.options.len())
					.unwrap_or(0);
				(!question.options.is_empty()).then_some(recommended).into_iter().collect()
			} else {
				self.selected.iter().copied().collect()
			};
			let keep_selection = self.custom.is_none() || question.multi;
			let selected_options = if keep_selection {
				selected
					.iter()
					.filter_map(|index| question.options.get(*index))
					.map(|option| option.label.clone())
					.collect()
			} else {
				Vec::new()
			};
			self.answers.push(AskAnswer {
				id: question.id.clone(),
				selected_options,
				custom_input: self.custom.clone(),
				note: None,
				timed_out,
			});
			self.index += 1;
			self.selected.clear();
			self.custom = None;
			self.custom_mode = false;
			self.custom_input.clear();
			if self.index >= self.questions.len() {
				Some(AskOutcome::Answered(std::mem::take(&mut self.answers)))
			} else {
				None
			}
		}

		fn timeout(&mut self) -> AskOutcome {
			while self.index < self.questions.len() {
				if let Some(outcome) = self.answer(true) {
					return outcome;
				}
			}
			AskOutcome::Answered(std::mem::take(&mut self.answers))
		}

		fn on_key(&mut self, key: Key) -> Option<AskOutcome> {
			if self.custom_mode {
				match key {
					Key::Enter => {
						let text = self.custom_input.text();
						self.custom = (!text.is_empty()).then_some(text);
						self.custom_mode = false;
						let question = self.current()?;
						if !question.multi {
							return self.answer(false);
						}
					},
					Key::Esc => {
						self.custom_mode = false;
						self.custom_input.clear();
					},
					Key::CtrlC => return Some(AskOutcome::Cancelled),
					Key::Backspace => self.custom_input.backspace(),
					Key::Left => {
						self.custom_input.cursor =
							self.custom_input.cursor.saturating_sub(1);
					},
					Key::Right => {
						self.custom_input.cursor = (self.custom_input.cursor + 1)
							.min(self.custom_input.chars.len());
					},
					Key::Char(ch) => self.custom_input.insert(ch),
					_ => {},
				}
				return None;
			}
			match key {
				Key::Esc => Some(AskOutcome::Cancelled),
				Key::CtrlC => Some(AskOutcome::Cancelled),
				Key::Char('q') | Key::Char('Q') => Some(AskOutcome::Cancelled),
				Key::Char('c') | Key::Char('C') => Some(AskOutcome::ChatRedirect),
				Key::Char('o') | Key::Char('O') => {
					self.custom_mode = true;
					self.custom_input.clear();
					None
				},
				Key::Char(digit) if digit.is_ascii_digit() => {
					let index = digit.to_digit(10).unwrap_or(0) as usize;
					if index >= 1 {
						let index = index - 1;
						let (multi, option_count) = self
							.current()
							.map(|question| (question.multi, question.options.len()))?;
						if index < option_count {
							if multi {
								if !self.selected.remove(&index) {
									self.selected.insert(index);
								}
							} else {
								self.selected.clear();
								self.selected.insert(index);
							}
						}
					}
					None
				},
				Key::Enter => {
					if self.custom.is_some() {
						return self.answer(false);
					}
					self.answer(false)
				},
				_ => None,
			}
		}
	}

	enum Dialog {
		Approval {
			name: String,
			detail: Option<String>,
			reply: tokio::sync::oneshot::Sender<bool>,
		},
		Ask(AskState),
	}

	enum Action {
		None,
		Exit,
		Submit(String),
		Steer(String),
		Cancel,
		Command(UiCommand),
	}

	#[derive(Debug, Clone, PartialEq, Eq)]
	enum UiCommand {
		Help,
		Status,
		History,
		Workers(Option<String>),
		Clear,
		Config,
		Chat,
		Model(Option<String>),
		Cwd(Option<String>),
		Memory(Vec<String>),
		Exit,
	}

	fn parse_ui_command(line: &str) -> Option<UiCommand> {
		let trimmed = line.trim();
		let (command, argument) = match trimmed.split_once(char::is_whitespace) {
			Some((command, argument)) => (command, argument.trim()),
			None => (trimmed, ""),
		};
		let argument = (!argument.is_empty()).then(|| argument.to_string());
		match command {
			"/exit" | "/quit" => Some(UiCommand::Exit),
			"/help" | "/?" => Some(UiCommand::Help),
			"/status" => Some(UiCommand::Status),
			"/history" => Some(UiCommand::History),
			"/workers" => Some(UiCommand::Workers(None)),
			"/worker" => Some(UiCommand::Workers(argument)),
			"/clear" => Some(UiCommand::Clear),
			"/config" => Some(UiCommand::Config),
			"/chat" => Some(UiCommand::Chat),
			"/model" => Some(UiCommand::Model(argument)),
			"/cwd" => Some(UiCommand::Cwd(argument)),
			"/memory" => Some(UiCommand::Memory(
				argument
					.map(|argument| {
						argument.split_whitespace().map(str::to_string).collect()
					})
					.unwrap_or_default(),
			)),
			_ => None,
		}
	}

	struct App {
		status: Status,
		entries: Vec<Entry>,
		live_text: Option<usize>,
		live_thinking: Option<usize>,
		tools: BTreeMap<String, usize>,
		input: LineInput,
		history: Vec<String>,
		history_pos: Option<usize>,
		scroll: usize,
		last_max_scroll: usize,
		view: View,
		workers: BTreeMap<String, WorkerRow>,
		busy: bool,
		quit: bool,
		dirty: bool,
		width: usize,
		height: usize,
		queue: VecDeque<UiMsg>,
		dialog: Option<Dialog>,
		ask_timeout: Option<Duration>,
		history_lines: Vec<String>,
	}

	impl App {
		fn new(status: Status, ask_timeout: Option<Duration>) -> Self {
			Self {
				status,
				entries: Vec::new(),
				live_text: None,
				live_thinking: None,
				tools: BTreeMap::new(),
				input: LineInput::default(),
				history: Vec::new(),
				history_pos: None,
				scroll: 0,
				last_max_scroll: 0,
				view: View::Chat,
				workers: BTreeMap::new(),
				busy: false,
				quit: false,
				dirty: true,
				width: 80,
				height: 24,
				queue: VecDeque::new(),
				dialog: None,
				ask_timeout,
				history_lines: Vec::new(),
			}
		}

		fn push(&mut self, kind: Kind, text: impl Into<String>) {
			self.entries.push(Entry { kind, text: text.into() });
			self.dirty = true;
		}

		fn push_system(&mut self, text: impl Into<String>) {
			self.push(Kind::System, text);
		}

		fn start_turn(&mut self, prompt: &str) {
			self.push(Kind::User, format!("› {prompt}"));
			self.busy = true;
			self.live_text = None;
			self.live_thinking = None;
			self.tools.clear();
			self.scroll = 0;
			self.dirty = true;
		}

		fn finish_turn(&mut self) {
			self.busy = false;
			self.live_text = None;
			self.live_thinking = None;
			self.tools.clear();
			self.entries.push(Entry {
				kind: Kind::Separator,
				text: String::new(),
			});
			self.dirty = true;
		}

		fn on_event(&mut self, event: Event) {
			match event {
				Event::MessageStart { role } => {
					if role == "assistant" {
						self.live_text = None;
						self.live_thinking = None;
					}
				},
				Event::MessageUpdate { delta } => {
					let index = match self.live_text {
						Some(index) => index,
						None => {
							self.entries.push(Entry {
								kind: Kind::Assistant,
								text: String::new(),
							});
							let index = self.entries.len() - 1;
							self.live_text = Some(index);
							index
						},
					};
					self.entries[index].text.push_str(&delta);
				},
				Event::ThinkingUpdate { delta } => {
					let index = match self.live_thinking {
						Some(index) => index,
						None => {
							self.entries.push(Entry {
								kind: Kind::Thinking,
								text: String::new(),
							});
							let index = self.entries.len() - 1;
							self.live_thinking = Some(index);
							index
						},
					};
					self.entries[index].text.push_str(&delta);
				},
				Event::MessageEnd { .. } => {
					self.live_text = None;
					self.live_thinking = None;
				},
				Event::ToolStart { .. } | Event::ToolUpdate { .. } | Event::ToolEnd { .. } => {},
				Event::UserSteer { parts } => {
					let text = parts
						.iter()
						.filter_map(|part| match part {
							ContentPart::Text { text } => Some(text.as_str()),
							_ => None,
						})
						.collect::<Vec<_>>()
						.join(" ");
					if !text.trim().is_empty() {
						self.entries.push(Entry {
							kind: Kind::User,
							text: format!("› {text}"),
						});
					}
				},
				Event::TurnEnd { .. } => {
					self.live_text = None;
					self.live_thinking = None;
				},
				Event::AgentEnd { .. } => {},
			}
			self.dirty = true;
		}

		fn on_tool_start(&mut self, id: String, tool: String, args: &serde_json::Value) {
			self.entries.push(Entry {
				kind: Kind::Tool,
				text: format!("→ {tool} {}", summarize(args)),
			});
			let index = self.entries.len() - 1;
			self.tools.insert(id, index);
			self.dirty = true;
		}

		fn on_tool_update(&mut self, id: &str, tool: &str, partial: &serde_json::Value) {
			if let Some(index) = self.tools.get(id).copied()
				&& let Some(entry) = self.entries.get_mut(index)
			{
				entry.text = format!("→ {tool} {}", summarize(partial));
				self.dirty = true;
			}
		}

		fn on_tool_end(&mut self, id: &str, tool: &str, error: Option<&str>) {
			let index = self.tools.remove(id);
			let (kind, text) = match error {
				Some(error) => (
					Kind::ToolError,
					format!("✗ {tool}: {}", first_line(error)),
				),
				None => (Kind::Tool, format!("✓ {tool}")),
			};
			match index.and_then(|index| self.entries.get_mut(index)) {
				Some(entry) => {
					entry.kind = kind;
					entry.text = text;
				},
				None => self.entries.push(Entry { kind, text }),
			}
			self.dirty = true;
		}

		fn on_subagent(&mut self, event: SubagentEvent) {
			match event {
				SubagentEvent::Lifecycle(lifecycle) => {
					let status = lifecycle.status.as_str().to_string();
					let row = WorkerRow {
						index: lifecycle.index,
						agent: lifecycle.agent.clone(),
						status: status.clone(),
						task: lifecycle.task.clone(),
						session_file: lifecycle.session_file.clone(),
					};
					if lifecycle.status == SubagentLifecycleStatus::Started {
						self.push(
							Kind::System,
							format!(
								"worker #{}{} started: {}",
								lifecycle.index,
								if lifecycle.agent.is_empty() {
									String::new()
								} else {
									format!(" ({})", lifecycle.agent)
								},
								first_line(&lifecycle.task)
							),
						);
					}
					self.workers.insert(lifecycle.id, row);
				},
				SubagentEvent::Progress(progress) => {
					let row = self
						.workers
						.entry(progress.id.clone())
						.or_insert(WorkerRow {
							index: progress.index,
							agent: progress.agent.clone(),
							status: progress.status.clone(),
							task: progress.task.clone(),
							session_file: progress.session_file.clone(),
						});
					row.status = progress.status.clone();
					row.agent = progress.agent.clone();
					row.task = progress.task.clone();
					if progress.session_file.is_some() {
						row.session_file = progress.session_file.clone();
					}
				},
				SubagentEvent::Event { .. } => {},
			}
			self.dirty = true;
		}

		fn running_workers(&self) -> usize {
			self.workers.values().filter(|row| row.running()).count()
		}

		fn handle_msg(&mut self, msg: UiMsg) -> bool {
			match msg {
				UiMsg::Event(event) => self.on_event(event),
				UiMsg::ToolStart { id, tool, args } => {
					self.on_tool_start(id, tool, &args);
				},
				UiMsg::ToolUpdate { id, tool, partial } => {
					self.on_tool_update(&id, &tool, &partial);
				},
				UiMsg::ToolEnd { id, tool, error } => {
					self.on_tool_end(&id, &tool, error.as_deref());
				},
				UiMsg::Advisor(text) => self.push(Kind::Advisor, format!("advisor: {text}")),
				UiMsg::Subagent(event) => self.on_subagent(event),
				UiMsg::Approval { name, detail, reply } => {
					self.queue.push_back(UiMsg::Approval { name, detail, reply });
					self.maybe_open_dialog();
				},
				UiMsg::Ask { questions, cancel, reply } => {
					self.queue.push_back(UiMsg::Ask { questions, cancel, reply });
					self.maybe_open_dialog();
				},
				UiMsg::TurnError(error) => self.push(Kind::ToolError, error),
				UiMsg::TurnDone => {
					self.busy = false;
					self.fail_dialogs();
					self.dirty = true;
					return true;
				},
			}
			false
		}

		/// Drain queued worker messages without blocking. Returns `true` when a
		/// turn finished.
		fn pump(&mut self, receiver: &Receiver<UiMsg>) -> bool {
			loop {
				match receiver.try_recv() {
					Ok(msg) => {
						if self.handle_msg(msg) {
							return true;
						}
					},
					Err(TryRecvError::Empty) => return false,
					Err(TryRecvError::Disconnected) => return true,
				}
			}
		}

		fn maybe_open_dialog(&mut self) {
			if self.dialog.is_some() {
				return;
			}
			while let Some(msg) = self.queue.pop_front() {
				match msg {
					UiMsg::Approval { name, detail, reply } => {
						if reply.is_closed() {
							continue;
						}
						self.dialog = Some(Dialog::Approval { name, detail, reply });
						self.dirty = true;
						return;
					},
					UiMsg::Ask { questions, cancel, reply } => {
						if reply.is_closed() {
							continue;
						}
						if cancel.is_cancelled() {
							let _ = reply.send(AskOutcome::Aborted);
							continue;
						}
						if questions.is_empty() {
							let _ = reply.send(AskOutcome::Answered(Vec::new()));
							continue;
						}
						self.dialog = Some(Dialog::Ask(AskState::new(
							questions,
							reply,
							cancel,
							self.ask_timeout,
						)));
						self.dirty = true;
						return;
					},
					_ => {},
				}
			}
		}

		fn fail_dialogs(&mut self) {
			if let Some(dialog) = self.dialog.take() {
				match dialog {
					Dialog::Approval { reply, .. } => {
						let _ = reply.send(false);
					},
					Dialog::Ask(state) => {
						let _ = state.reply.send(AskOutcome::Aborted);
					},
				}
			}
			while let Some(msg) = self.queue.pop_front() {
				match msg {
					UiMsg::Approval { reply, .. } => {
						let _ = reply.send(false);
					},
					UiMsg::Ask { reply, .. } => {
						let _ = reply.send(AskOutcome::Aborted);
					},
					_ => {},
				}
			}
			self.dirty = true;
		}

		fn tick(&mut self) {
			let outcome = match self.dialog.as_mut() {
				Some(Dialog::Ask(state)) => {
					if state.cancel.is_cancelled() {
						Some(AskOutcome::Aborted)
					} else if state.expired() {
						Some(state.timeout())
					} else {
						None
					}
				},
				_ => None,
			};
			if let Some(outcome) = outcome
				&& let Some(Dialog::Ask(state)) = self.dialog.take()
			{
				let _ = state.reply.send(outcome);
			}
			self.maybe_open_dialog();
		}

		fn on_key(&mut self, key: Key) -> Option<Action> {
			if let Some(dialog) = self.dialog.take() {
				match dialog {
					Dialog::Approval { name, detail, reply } => {
						let decision = match key {
							Key::Char('y') | Key::Char('Y') => Some(true),
							Key::Char('n')
							| Key::Char('N')
							| Key::Esc
							| Key::CtrlC => Some(false),
							_ => None,
						};
						match decision {
							Some(value) => {
								let _ = reply.send(value);
								self.dirty = true;
								return Some(Action::None);
							},
							None => {
								self.dialog = Some(Dialog::Approval {
									name,
									detail,
									reply,
								});
							},
						}
					},
					Dialog::Ask(mut state) => {
						if let Some(outcome) = state.on_key(key) {
							let _ = state.reply.send(outcome);
							self.dirty = true;
							return Some(Action::None);
						}
						self.dialog = Some(Dialog::Ask(state));
					},
				}
				self.dirty = true;
				return Some(Action::None);
			}

			if !matches!(&self.view, View::Chat) {
				match key {
					Key::Esc | Key::Char('q') | Key::Char('Q') => self.view = View::Chat,
					Key::CtrlC => {
						self.view = View::Chat;
						if self.busy {
							self.dirty = true;
							return Some(Action::Cancel);
						}
					},
					Key::PageUp => {
						self.scroll =
							(self.scroll + self.height / 2).min(self.last_max_scroll);
					},
					Key::PageDown => {
						self.scroll = self.scroll.saturating_sub(self.height / 2);
					},
					_ => {},
				}
				self.dirty = true;
				return None;
			}

			match key {
				Key::CtrlC => {
					if self.busy {
						return Some(Action::Cancel);
					}
					self.input.clear();
				},
				Key::CtrlD => {
					if self.input.is_empty() {
						return Some(Action::Exit);
					}
					self.input.delete();
				},
				Key::CtrlL => {
					self.scroll = 0;
				},
				Key::CtrlU => self.input.clear(),
				Key::Enter => {
					let text = self.input.text();
					if self.busy {
						if text.trim().is_empty() {
							return None;
						}
						self.input.clear();
						match text.trim() {
							"/cancel" | "/stop" => return Some(Action::Cancel),
							_ => {},
						}
						if text.trim_start().starts_with('/') {
							if let Some(command) = parse_ui_command(&text) {
								if matches!(
									&command,
									UiCommand::Help
										| UiCommand::Status
										| UiCommand::History
										| UiCommand::Workers(_)
								) {
									return Some(Action::Command(command));
								}
								self.push_system(format!(
									"{command:?} is unavailable while a turn runs \
									 (Ctrl-C cancels)"
								));
								return None;
							}
						}
						return Some(Action::Steer(text));
					}
					if text.trim().is_empty() {
						return None;
					}
					self.input.clear();
					self.history_pos = None;
					self.history.push(text.clone());
					self.scroll = 0;
					if let Some(command) = parse_ui_command(&text) {
						return Some(Action::Command(command));
					}
					if text.trim_start().starts_with('/') {
						// Unknown slash command keeps the line-REPL behavior:
						// it is a prompt for the model, not an error.
						return Some(Action::Submit(text));
					}
					return Some(Action::Submit(text));
				},
				Key::Backspace => self.input.backspace(),
				Key::Delete => self.input.delete(),
				Key::Left => {
					self.input.cursor = self.input.cursor.saturating_sub(1);
				},
				Key::Right => {
					self.input.cursor =
						(self.input.cursor + 1).min(self.input.chars.len());
				},
				Key::Home => self.input.cursor = 0,
				Key::End => self.input.cursor = self.input.chars.len(),
				Key::Tab => self.input.insert(' '),
				Key::PageUp => {
					self.scroll = (self.scroll + self.height / 2).min(self.last_max_scroll);
				},
				Key::PageDown => {
					self.scroll = self.scroll.saturating_sub(self.height / 2);
				},
				Key::Up => {
					if self.busy && self.input.is_empty() {
						self.scroll = (self.scroll + 1).min(self.last_max_scroll);
					} else if !self.history.is_empty() {
						let next = match self.history_pos {
							None => self.history.len() - 1,
							Some(0) => 0,
							Some(index) => index - 1,
						};
						self.history_pos = Some(next);
						let text = self.history[next].clone();
						self.input.set_text(&text);
					}
				},
				Key::Down => {
					if self.busy && self.input.is_empty() {
						self.scroll = self.scroll.saturating_sub(1);
					} else if let Some(index) = self.history_pos {
						if index + 1 < self.history.len() {
							self.history_pos = Some(index + 1);
							let text = self.history[index + 1].clone();
							self.input.set_text(&text);
						} else {
							self.history_pos = None;
							self.input.clear();
						}
					}
				},
				Key::Char(ch) => {
					self.input.insert(ch);
					self.history_pos = None;
				},
				Key::Esc => {},
			}
			self.dirty = true;
			None
		}

		fn body_total(&self, width: usize) -> usize {
			let width = width.max(1);
			match &self.view {
				View::Chat => self
					.entries
					.iter()
					.map(|entry| {
						wrap_text(&sanitize(&entry.text), width).len().max(1)
					})
					.sum(),
				View::Text { lines, .. } => lines
					.iter()
					.map(|line| wrap_text(&sanitize(line), width).len().max(1))
					.sum(),
			}
		}

		fn open_text_view(&mut self, title: impl Into<String>, lines: Vec<String>) {
			self.scroll = 0;
			self.view = View::Text { title: title.into(), lines };
			self.dirty = true;
		}

		/// Commands that need no `Session` access are legal while a turn runs.
		fn busy_command(&mut self, command: UiCommand) {
			match command {
				UiCommand::Help => self.open_text_view("Help", help_lines()),
				UiCommand::Status => {
					let lines = self.status_lines();
					self.open_text_view("Status", lines);
				},
				UiCommand::History => {
					let lines = self.history_lines.clone();
					self.open_text_view("History", lines);
				},
				UiCommand::Workers(None) => {
					let lines = self.worker_lines();
					self.open_text_view("Workers", lines);
				},
				UiCommand::Workers(Some(selector)) => {
					let lines = self.worker_transcript(&selector);
					self.open_text_view(format!("Worker {selector}"), lines);
				},
				command => self.push_system(format!(
					"{command:?} is unavailable while a turn runs"
				)),
			}
			self.dirty = true;
		}

		fn status_lines(&self) -> Vec<String> {
			vec![
				format!("model:     {}", self.status.model),
				format!(
					"provider:  {}",
					if self.status.provider.is_empty() {
						"(default)"
					} else {
						self.status.provider.as_str()
					}
				),
				format!("cwd:       {}", self.status.cwd),
				format!("session:   {}", self.status.session),
				format!("approvals: {}", self.status.approvals),
				format!(
					"tools:     {}",
					if self.status.tools_enabled {
						"enabled"
					} else {
						"disabled"
					}
				),
				format!("history:   {} lines", self.history_lines.len()),
				format!(
					"workers:   {} running / {} seen",
					self.running_workers(),
					self.workers.len()
				),
				format!("state:     {}", if self.busy { "busy" } else { "idle" }),
			]
		}

		fn worker_lines(&self) -> Vec<String> {
			if self.workers.is_empty() {
				return vec!["no subagents observed in this session".to_string()];
			}
			let mut rows: Vec<&WorkerRow> = self.workers.values().collect();
			rows.sort_by_key(|row| (row.index, row.agent.clone()));
			rows
				.into_iter()
				.map(|row| {
					format!(
						"#{:<3} {:<12} {:<10} {}",
						row.index,
						row.agent,
						row.status,
						first_line(&row.task)
					)
				})
				.collect()
		}

		fn worker_transcript(&self, selector: &str) -> Vec<String> {
			let mut rows: Vec<&WorkerRow> = self.workers.values().collect();
			rows.sort_by_key(|row| row.index);
			let selected = rows.into_iter().find(|row| {
				row.index.to_string() == selector || row.agent == selector
			});
			let Some(row) = selected else {
				return vec![format!("no worker matches '{selector}'")];
			};
			let Some(file) = row.session_file.as_deref() else {
				return vec![format!(
					"worker #{} has no persisted child session (detached or not yet written)",
					row.index
				)];
			};
			worker_transcript_lines(file)
		}

		fn dialog_lines(&self, width: usize) -> Vec<String> {
			match self.dialog.as_ref() {
				None => Vec::new(),
				Some(Dialog::Approval { name, detail, .. }) => {
					let mut lines = vec![format!("Approval required: {}", one_line(name))];
					if let Some(detail) = detail {
						for line in wrap_text(&sanitize(detail), width) {
							lines.push(format!("  {line}"));
						}
					}
					lines.push("[y] approve   [n] deny   Esc deny".to_string());
					lines
				},
				Some(Dialog::Ask(state)) => {
					let mut lines = Vec::new();
					let total = state.questions.len();
					if let Some(question) = state.current() {
						lines.push(format!(
							"Question {}/{}: {}",
							state.index + 1,
							total,
							one_line(&question.question)
						));
						for (index, option) in question.options.iter().enumerate() {
							let marked = state.selected.contains(&index);
							let recommended = question
								.recommended
								.and_then(|value| usize::try_from(value).ok())
								== Some(index);
							lines.push(format!(
								"  {}{} {}{}",
								if marked { "●" } else { "○" },
								index + 1,
								one_line(&option.label),
								if recommended { " (Recommended)" } else { "" }
							));
							if let Some(description) = option.description.as_deref() {
								lines.push(format!(
									"      {}",
									one_line(description)
								));
							}
						}
					}
					if state.custom_mode {
						lines.push(format!(
							"custom> {}",
							one_line(&state.custom_input.text())
						));
					} else if let Some(custom) = state.custom.as_deref() {
						lines.push(format!("custom: {}", one_line(custom)));
					}
					lines.push(
						"[1-9] select · [Enter] confirm · [o] other · [c] chat · [Esc] cancel"
							.to_string(),
					);
					lines
				},
			}
		}

		fn body_lines(&self, width: usize, height: usize) -> Vec<(Kind, String)> {
			let width = width.max(1);
			let mut lines: Vec<(Kind, String)> = Vec::new();
			match &self.view {
				View::Chat => {
					for entry in &self.entries {
						let text = sanitize(&entry.text);
						let wrapped = wrap_text(&text, width);
						if wrapped.is_empty() {
							lines.push((entry.kind, String::new()));
						}
						for line in wrapped {
							lines.push((entry.kind, line));
						}
					}
				},
				View::Text { lines: view_lines, .. } => {
					for line in view_lines {
						for wrapped in wrap_text(&sanitize(line), width) {
							lines.push((Kind::System, wrapped));
						}
					}
				},
			}
			let total = lines.len();
			let max_scroll = total.saturating_sub(height);
			let start = max_scroll.saturating_sub(self.scroll.min(max_scroll));
			lines.into_iter().skip(start).take(height).collect()
		}

		fn render(&mut self) -> (Vec<String>, usize, usize) {
			let width = self.width.max(20);
			let height = self.height.max(6);
			let content_width = width.saturating_sub(1).max(1);
			let view_title = match &self.view {
				View::Chat => None,
				View::Text { title, .. } => Some(title.clone()),
			};
			let mut dialog = self.dialog_lines(content_width);
			// A dialog never eats the whole screen; keep one transcript row.
			let fixed = 4usize;
			let available = height.saturating_sub(fixed);
			let dialog_cap = available.saturating_sub(1).min(height / 2);
			if dialog.len() > dialog_cap {
				dialog.truncate(dialog_cap);
				if let Some(last) = dialog.last_mut() {
					*last = "…".to_string();
				}
			}
			let dialog_height = dialog.len();
			let body_height = available.saturating_sub(dialog_height);

			let total_lines = self.body_total(content_width);
			self.last_max_scroll = total_lines.saturating_sub(body_height);
			if self.scroll > self.last_max_scroll {
				self.scroll = self.last_max_scroll;
			}
			let body = self.body_lines(content_width, body_height);

			let mut rows: Vec<String> = Vec::with_capacity(height);
			let busy = if self.busy { " · working" } else { "" };
			let workers = match self.running_workers() {
				0 => String::new(),
				count => format!(" · {count} worker(s)"),
			};
			let header = format!(
				"\x1b[1momp\x1b[0m {} · {}{}{}",
				clip(&one_line(&self.status.model), content_width.saturating_sub(24)),
				clip(&one_line(&self.status.cwd), content_width / 2),
				workers,
				busy
			);
			rows.push(header);
			rows.push(format!(
				"\x1b[2msession {} · approvals {} · /help for commands\x1b[0m",
				clip(&one_line(&self.status.session), 40),
				self.status.approvals
			));
			for (kind, line) in &body {
				let text = clip(line, content_width);
				if text.is_empty() {
					rows.push(String::new());
				} else {
					rows.push(format!("{}{}\x1b[0m", kind.style(), text));
				}
			}
			while rows.len() < 2 + body_height {
				rows.push(String::new());
			}
			for line in &dialog {
				rows.push(format!(
					"\x1b[1m{}\x1b[0m",
					clip(&sanitize(line), content_width)
				));
			}
			let viewing = view_title.is_some();
			let input_text = self.input.text();
			let input_line = if viewing {
				let title = view_title.as_deref().unwrap_or_default();
				format!(
					"\x1b[2m[{}] — Esc closes the view\x1b[0m",
					clip(&one_line(title), content_width.saturating_sub(16))
				)
			} else {
				let text = clip(
					&one_line(&input_text),
					content_width.saturating_sub(3),
				);
				format!("\x1b[1m> \x1b[0m{text}")
			};
			let cursor_prefix = self.input.prefix();
			let cursor_col = if viewing {
				1
			} else {
				(3 + display_width(&cursor_prefix)).min(content_width)
			};
			rows.push(input_line);
			rows.push(
				"\x1b[2mEnter send · Ctrl-C cancel/clear · PgUp/PgDn scroll · \
				 /status /history /workers /memory /model /cwd /config\x1b[0m"
					.to_string(),
			);
			while rows.len() < height {
				rows.push(String::new());
			}
			rows.truncate(height);

			let input_row = (2 + body_height + dialog_height + 1).min(height);
			(rows, input_row, cursor_col)
		}
	}

	// ── Formatting helpers ─────────────────────────────────────────────────

	fn first_line(text: &str) -> String {
		text.lines().next().unwrap_or_default().trim().to_string()
	}

	/// Untrusted single-line text for a frame row: strip terminal controls and
	/// collapse embedded newlines so one model-authored string cannot move the
	/// cursor or inject a second row.
	fn one_line(text: &str) -> String {
		sanitize(text).replace('\n', " ")
	}

	fn summarize(value: &serde_json::Value) -> String {
		let text = serde_json::to_string(value).unwrap_or_default();
		clip(&text, 160)
	}

	fn status_of(session: &Session<'_>) -> Status {
		Status {
			model: session
				.model
				.clone()
				.unwrap_or_else(|| "(default)".to_string()),
			provider: session.provider.clone().unwrap_or_default(),
			cwd: session.cwd.display().to_string(),
			session: if session.store.enabled() {
				session.store.id().to_string()
			} else {
				"(no session)".to_string()
			},
			approvals: session.approvals.mode.as_str().to_string(),
			tools_enabled: session.tools_enabled,
		}
	}

	fn history_lines(history: &[aragent::session::TurnRecord]) -> Vec<String> {
		let mut lines = Vec::new();
		for (index, turn) in history.iter().enumerate() {
			lines.push(format!("── turn {} ──", index + 1));
			for line in turn.prompt.lines() {
				lines.push(format!("you: {line}"));
			}
			if !turn.reasoning.is_empty() {
				lines.push("thinking:".to_string());
				for line in turn.reasoning.lines().take(40) {
					lines.push(format!("  {line}"));
				}
			}
			if !turn.assistant.is_empty() {
				lines.push("assistant:".to_string());
				for line in turn.assistant.lines().take(200) {
					lines.push(format!("  {line}"));
				}
			}
			for call in &turn.tool_calls {
				lines.push(format!(
					"tool {}: {} ({})",
					call.name,
					first_line(&call.result),
					if call.is_error { "error" } else { "ok" }
				));
			}
			if !turn.stop_reason.is_empty() {
				lines.push(format!("stop: {}", turn.stop_reason));
			}
		}
		if lines.is_empty() {
			lines.push("no turns in this session yet".to_string());
		}
		lines
	}

	fn worker_transcript_lines(file: &str) -> Vec<String> {
		let path = std::path::Path::new(file);
		if let Ok(metadata) = std::fs::metadata(path)
			&& metadata.len() > 2 * 1024 * 1024
		{
			return vec![
				"child transcript is larger than 2 MiB; open the file directly:"
					.to_string(),
				file.to_string(),
			];
		}
		match aragent::rpc::subagents::read_transcript(file, 0) {
			Err(error) => vec![error],
			Ok(slice) => {
				let mut lines = vec![format!(
					"transcript {} ({} messages, next byte {})",
					slice.session_file,
					slice.messages.len(),
					slice.next_byte
				)];
				let skip = slice.messages.len().saturating_sub(60);
				for message in slice.messages.iter().skip(skip) {
					let role = message
						.get("role")
						.and_then(serde_json::Value::as_str)
						.unwrap_or("?");
					let text = message
						.get("content")
						.map(content_text)
						.unwrap_or_default();
					lines.push(format!("{role}: {}", text.trim()));
				}
				if slice.messages.is_empty() {
					lines.push("(no messages yet)".to_string());
				}
				lines
			},
		}
	}

	fn content_text(content: &serde_json::Value) -> String {
		match content {
			serde_json::Value::String(text) => text.clone(),
			serde_json::Value::Array(parts) => parts
				.iter()
				.filter_map(|part| {
					part
						.get("text")
						.and_then(serde_json::Value::as_str)
						.map(str::to_string)
				})
				.collect::<Vec<_>>()
				.join("\n"),
			_ => String::new(),
		}
	}

	fn help_lines() -> Vec<String> {
		vec![
			"/status          session, model, cwd, workers".to_string(),
			"/history         turns persisted in this session".to_string(),
			"/workers         live child agents".to_string(),
			"/worker <n>      one child transcript page".to_string(),
			"/memory <args>   same grammar as `arag memory`".to_string(),
			"/model [id]      show or switch the model".to_string(),
			"/cwd [path]      show or change the working directory".to_string(),
			"/config          effective settings (read-only)".to_string(),
			"/clear           clear this transcript".to_string(),
			"/help            this list".to_string(),
			"/exit            leave".to_string(),
			String::new(),
			"Enter sends · while a turn runs Enter steers · Ctrl-C cancels".to_string(),
			"PgUp/PgDn scroll · Esc closes a view or denies an approval".to_string(),
		]
	}

	fn config_lines(session: &Session<'_>) -> Vec<String> {
		vec![
			"effective settings (read-only; edit config.yml outside the TUI)"
				.to_string(),
			format!(
				"model:          {}",
				session
					.model
					.as_deref()
					.or(session.config.default_model.as_deref())
					.unwrap_or("(default)")
			),
			format!(
				"approval mode:  {}",
				session
					.config
					.approval_mode
					.unwrap_or(session.approvals.mode)
					.as_str()
			),
			format!(
				"allow:          {}",
				if session.config.allow.is_empty() {
					"(none)".to_string()
				} else {
					session.config.allow.join(", ")
				}
			),
			format!(
				"tools:          {}",
				if session.tools_enabled { "enabled" } else { "disabled" }
			),
			format!(
				"memory backend: {:?}",
				session.config.memory_backend()
			),
			format!(
				"providers:      {} configured",
				session.catalog.providers.len()
			),
			format!("cwd:            {}", session.cwd.display()),
			format!("session dir:    {}", session
				.session_dir
				.as_ref()
				.map(|path| path.display().to_string())
				.unwrap_or_else(|| "(no session)".to_string())),
		]
	}

	fn memory_text(argv: &[String], session: &Session<'_>) -> Vec<String> {
		let agent_dir = session
			.skill_options
			.agent_dir
			.clone()
			.unwrap_or_else(aragent::dirs::agent_dir);
		let mut output: Vec<u8> = Vec::new();
		match aragent::memory_cmd::execute(
			argv,
			&agent_dir,
			&session.cwd,
			session.config.memory_backend(),
			&mut output,
		) {
			Ok(()) => String::from_utf8_lossy(&output)
				.lines()
				.map(str::to_string)
				.collect(),
			Err(error) => vec![format!("memory: {error}")],
		}
	}

	// ── Commands ───────────────────────────────────────────────────────────

	/// Execute an idle command. Returns `true` when the UI should exit.
	fn handle_command(
		session: &mut Session<'_>,
		app: &mut App,
		command: UiCommand,
	) -> Result<bool> {
		match command {
			UiCommand::Exit => return Ok(true),
			UiCommand::Help => app.open_text_view("Help", help_lines()),
			UiCommand::Status => {
				let lines = app.status_lines();
				app.open_text_view("Status", lines);
			},
			UiCommand::History => {
				app.history_lines = history_lines(&session.history);
				let lines = app.history_lines.clone();
				app.open_text_view("History", lines);
			},
			UiCommand::Workers(None) => {
				let lines = app.worker_lines();
				app.open_text_view("Workers", lines);
			},
			UiCommand::Workers(Some(selector)) => {
				let lines = app.worker_transcript(&selector);
				app.open_text_view(format!("Worker {selector}"), lines);
			},
			UiCommand::Clear => {
				app.entries.clear();
				app.live_text = None;
				app.live_thinking = None;
				app.tools.clear();
				app.scroll = 0;
				app.dirty = true;
			},
			UiCommand::Config => {
				let lines = config_lines(session);
				app.open_text_view("Config", lines);
			},
			UiCommand::Chat => app.view = View::Chat,
			UiCommand::Model(Some(id)) => {
				session.select_model(id.clone());
				app.push_system(format!("model: {id}"));
			},
			UiCommand::Model(None) => app.push_system(format!(
				"model: {}",
				session.model.as_deref().unwrap_or("(default)")
			)),
			UiCommand::Cwd(Some(path)) => {
				session.change_cwd(std::path::PathBuf::from(path))?;
				app.push_system(format!("cwd: {}", session.cwd.display()));
			},
			UiCommand::Cwd(None) => {
				app.push_system(format!("cwd: {}", session.cwd.display()));
			},
			UiCommand::Memory(argv) => {
				let lines = memory_text(&argv, session);
				if lines.is_empty() {
					app.push_system("memory: (no output)");
				}
				for line in lines {
					app.push(Kind::System, line);
				}
			},
		}
		app.status = status_of(session);
		app.history_lines = history_lines(&session.history);
		app.dirty = true;
		Ok(false)
	}

	// ── Worker turn ────────────────────────────────────────────────────────

	fn turn_worker(session: &mut Session<'_>, tx: &Sender<UiMsg>, prompt: String) {
		let result = catch_unwind(AssertUnwindSafe(|| -> Result<()> {
			let mut reporter = Reporter::new(OutputMode::Text, io::sink(), io::sink());
			let event_tx = tx.clone();
			reporter.set_event_hook(Some(Box::new(move |event: &Event| {
				// Copy only what the view consumes. Full messages and tool
				// results stay on the turn thread.
				let message = match event {
					Event::MessageStart { role } => {
						Some(UiMsg::Event(Event::MessageStart { role: role.clone() }))
					},
					Event::MessageUpdate { delta } => {
						Some(UiMsg::Event(Event::MessageUpdate { delta: delta.clone() }))
					},
					Event::ThinkingUpdate { delta } => {
						Some(UiMsg::Event(Event::ThinkingUpdate {
							delta: delta.clone(),
						}))
					},
					Event::UserSteer { parts } => {
						Some(UiMsg::Event(Event::UserSteer { parts: parts.clone() }))
					},
					Event::ToolStart { id, tool, args } => Some(UiMsg::ToolStart {
						id: id.clone(),
						tool: tool.clone(),
						args: args.clone(),
					}),
					Event::ToolUpdate { id, tool, partial, .. } => {
						Some(UiMsg::ToolUpdate {
							id: id.clone(),
							tool: tool.clone(),
							partial: partial.clone(),
						})
					},
					Event::ToolEnd { id, tool, error, .. } => Some(UiMsg::ToolEnd {
						id: id.clone(),
						tool: tool.clone(),
						error: error.clone(),
					}),
					// `MessageEnd`/`TurnEnd`/`AgentEnd` carry full messages and
					// tool-result vectors and are not needed: `MessageStart`
					// resets the live block for the next message.
					_ => None,
				};
				if let Some(message) = message {
					let _ = event_tx.send(message);
				}
			})));
			let advisor_tx = tx.clone();
			reporter.set_advisor_hook(Some(Box::new(
				move |content: &str, _details: &serde_json::Value| {
					let _ = advisor_tx.send(UiMsg::Advisor(content.to_string()));
				},
			)));
			let text = match session.expand_skill_command(&prompt) {
				Ok(text) => text,
				Err(error) => {
					let _ = tx.send(UiMsg::TurnError(format!("warning: {error}")));
					prompt.clone()
				},
			};
			session.turn(None, text, Vec::new(), OutputMode::Text, &mut reporter)?;
			session.drive_goal_continuation(OutputMode::Text, &mut reporter)?;
			session.drive_autolearn_capture(OutputMode::Text);
			reporter.finish()?;
			Ok(())
		}));
		match result {
			Ok(Ok(())) => {},
			Ok(Err(error)) => {
				let _ = tx.send(UiMsg::TurnError(error.to_string()));
			},
			Err(_) => {
				let _ = tx.send(UiMsg::TurnError("turn panicked".to_string()));
			},
		}
		let _ = tx.send(UiMsg::TurnDone);
	}

	fn cancel_turn(slot: &Arc<std::sync::Mutex<crate::TurnCancelState>>) {
		if let Ok(mut state) = slot.lock() {
			state.request();
		}
	}

	fn clear_cancel(slot: &Arc<std::sync::Mutex<crate::TurnCancelState>>) {
		if let Ok(mut state) = slot.lock() {
			state.clear();
		}
	}

	// ── Event loop ─────────────────────────────────────────────────────────

	/// Non-blocking readiness of stdin, distinguishing hangup.
	fn stdin_ready(timeout_ms: i32) -> (bool, bool) {
		let mut poll_fd = libc::pollfd {
			fd: libc::STDIN_FILENO,
			events: libc::POLLIN,
			revents: 0,
		};
		// SAFETY: one valid pollfd, bounded timeout.
		let result = unsafe { libc::poll(&mut poll_fd, 1, timeout_ms) };
		if result <= 0 {
			return (false, false);
		}
		let eof =
			poll_fd.revents & (libc::POLLHUP | libc::POLLERR | libc::POLLNVAL) != 0;
		(poll_fd.revents & libc::POLLIN != 0, eof)
	}

	fn read_stdin(buffer: &mut [u8]) -> Option<usize> {
		// SAFETY: writable buffer, stdin descriptor.
		let read = unsafe {
			libc::read(
				libc::STDIN_FILENO,
				buffer.as_mut_ptr().cast(),
				buffer.len(),
			)
		};
		if read < 0 {
			let error = io::Error::last_os_error();
			if error.kind() == io::ErrorKind::Interrupted
				|| error.kind() == io::ErrorKind::WouldBlock
			{
				return Some(0);
			}
			return None;
		}
		Some(read as usize)
	}

	struct LoopContext<'a> {
		terminal: &'a mut Terminal,
		app: &'a mut App,
		receiver: &'a Receiver<UiMsg>,
		runner: &'a Arc<PiAgentRunner>,
		cancel: &'a Arc<std::sync::Mutex<crate::TurnCancelState>>,
		busy: bool,
		buffer: Vec<u8>,
	}

	fn event_loop(context: &mut LoopContext<'_>) -> Result<Action> {
		loop {
			context.app.tick();
			let receiver = context.receiver;
			let finished = context.app.pump(receiver);
			if finished && context.busy {
				// A cancel that raced the last event must not poison the next
				// turn; the turn itself is already over.
				clear_cancel(context.cancel);
				return Ok(Action::None);
			}
			let timeout = if context.app.dirty { 0 } else { 50 };
			let (readable, eof) = stdin_ready(timeout);
			if eof {
				return Ok(Action::Exit);
			}
			if readable {
				let mut chunk = [0u8; 256];
				match read_stdin(&mut chunk) {
					None => return Ok(Action::Exit),
					Some(0) => {},
					Some(count) => {
						context.buffer.extend_from_slice(&chunk[..count]);
						for key in parse_keys(&mut context.buffer) {
							let Some(action) = context.app.on_key(key) else {
								continue;
							};
							match action {
								Action::None => {},
								Action::Cancel => {
									cancel_turn(context.cancel);
									context.app.fail_dialogs();
									context
										.app
										.push_system("cancelling turn (Ctrl-C)…");
								},
								Action::Steer(text) => {
									let accepted = context
										.runner
										.steer(vec![ContentPart::text(text.as_str())]);
									if !accepted {
										context.app.push_system(
											"steering unavailable: the turn is finishing",
										);
										context.app.input.set_text(&text);
									}
								},
								Action::Submit(prompt) => {
									if !context.busy {
										return Ok(Action::Submit(prompt));
									}
								},
								Action::Command(command) => {
									if context.busy {
										context.app.busy_command(command);
									} else {
										return Ok(Action::Command(command));
									}
								},
								Action::Exit => return Ok(Action::Exit),
							}
						}
						context.app.dirty = true;
					},
				}
			}
			if context.app.dirty {
				let terminal = &mut *context.terminal;
				let app = &mut *context.app;
				terminal.draw(app)?;
				context.app.dirty = false;
			} else {
				let (width, height) = context.terminal.size();
				if (width, height) != (context.app.width, context.app.height) {
					context.app.width = width;
					context.app.height = height;
					let terminal = &mut *context.terminal;
					let app = &mut *context.app;
					terminal.draw(app)?;
					context.app.dirty = false;
				}
			}
			if context.app.quit {
				return Ok(Action::Exit);
			}
		}
	}

	// ── Entry point ────────────────────────────────────────────────────────

	pub(crate) fn run(
		session: &mut Session<'_>,
		runner: &Arc<PiAgentRunner>,
	) -> Result<()> {
		let mut terminal = Terminal::enter()?;
		let (tx, receiver) = mpsc::channel::<UiMsg>();
		install_surfaces(session, &tx);
		let cancel = session.cancel_slot();
		let mut app = App::new(status_of(session), session.config.ask.timeout());
		app.history_lines = history_lines(&session.history);
		app.push_system(
			"arag interactive TUI — /help lists commands; Ctrl-C cancels a turn",
		);

		let outcome = (|| -> Result<()> {
			loop {
				let action = {
					let mut idle = LoopContext {
						terminal: &mut terminal,
						app: &mut app,
						receiver: &receiver,
						runner,
						cancel: &cancel,
						busy: false,
						buffer: Vec::new(),
					};
					event_loop(&mut idle)?
				};
				match action {
					Action::Exit => break,
					Action::Command(command) => {
						if handle_command(session, &mut app, command)? {
							break;
						}
					},
					Action::Submit(prompt) => {
						app.start_turn(&prompt);
						app.history_lines = history_lines(&session.history);
						// No cancel can meaningfully survive into a new turn;
						// one that arrives after this point is kept as pending
						// and applied when `turn_inner` publishes its token.
						clear_cancel(&cancel);
						let worker_tx = tx.clone();
						let worker_prompt = prompt.clone();
						let worker_session: &mut Session<'_> = &mut *session;
						std::thread::scope(|scope| -> Result<()> {
							let _worker = scope.spawn(move || {
								turn_worker(worker_session, &worker_tx, worker_prompt);
							});
							let action = {
								let mut busy = LoopContext {
									terminal: &mut terminal,
									app: &mut app,
									receiver: &receiver,
									runner,
									cancel: &cancel,
									busy: true,
									buffer: Vec::new(),
								};
								event_loop(&mut busy)
							};
							match action {
								Ok(Action::Exit) => {
									cancel_turn(&cancel);
									app.quit = true;
								},
								Ok(_) => {},
								Err(error) => {
									cancel_turn(&cancel);
									return Err(error);
								},
							}
							Ok(())
						})?;
						app.finish_turn();
						app.status = status_of(session);
						app.history_lines = history_lines(&session.history);
						app.dirty = true;
						if app.quit {
							break;
						}
					},
					Action::Steer(_) | Action::Cancel | Action::None => {},
				}
			}
			Ok(())
		})();

		// RAII restore happens here on every path (including the error above).
		drop(terminal);
		outcome
	}

	#[cfg(test)]
	mod tests {
		use super::*;

		#[test]
		fn wrapping_respects_wide_and_combining_chars() {
			assert_eq!(
				wrap_text("你好世界", 5),
				vec!["你好".to_string(), "世界".to_string()]
			);
			assert_eq!(
				wrap_text("hello world", 5),
				vec!["hello".to_string(), "world".to_string()]
			);
			// Combining mark adds no column.
			assert_eq!(display_width("e\u{301}"), 1);
			assert_eq!(clip("你好世界", 5), "你好…");
		}

		#[test]
		fn sanitize_strips_terminal_escapes() {
			assert_eq!(sanitize("a\x1b[31mb\u{9b}2Jc\r\td"), "a[31mb2Jc    d");
		}

		#[test]
		fn line_input_edits_unicode_by_char() {
			let mut input = LineInput::default();
			input.set_text("héllo");
			input.cursor = 1;
			input.insert('x');
			assert_eq!(input.text(), "hxéllo");
			input.backspace();
			assert_eq!(input.text(), "héllo");
			input.delete();
			assert_eq!(input.text(), "hllo");
		}

		#[test]
		fn key_parser_reads_arrows_and_partial_utf8() {
			let mut buffer = b"\x1b[5~\xe4\xbd".to_vec();
			let keys = parse_keys(&mut buffer);
			assert_eq!(keys, vec![Key::PageUp]);
			assert_eq!(buffer, b"\xe4\xbd".to_vec());
			buffer.extend_from_slice(b"\xa0");
			let keys = parse_keys(&mut buffer);
			assert_eq!(keys, vec![Key::Char('你')]);
			assert!(buffer.is_empty());
		}

		#[test]
		fn streaming_events_build_transcript() {
			let mut app = App::new(
				Status {
					model: "m".to_string(),
					provider: String::new(),
					cwd: "/".to_string(),
					session: "s".to_string(),
					approvals: "always-ask".to_string(),
					tools_enabled: true,
				},
				None,
			);
			app.on_event(Event::MessageStart { role: "assistant".into() });
			app.on_event(Event::MessageUpdate { delta: "hel".into() });
			app.on_event(Event::MessageUpdate { delta: "lo".into() });
			let assistant: Vec<&str> = app
				.entries
				.iter()
				.filter(|entry| entry.kind == Kind::Assistant)
				.map(|entry| entry.text.as_str())
				.collect();
			assert_eq!(assistant, vec!["hello"]);

			let _ = app.handle_msg(UiMsg::ToolStart {
				id: "t1".into(),
				tool: "read".into(),
				args: serde_json::json!({ "path": "a" }),
			});
			let _ = app.handle_msg(UiMsg::ToolEnd {
				id: "t1".into(),
				tool: "read".into(),
				error: None,
			});
			let tool = app.entries.last().expect("tool entry");
			assert!(tool.text.starts_with("✓ read"), "{}", tool.text);
			assert!(app.tools.is_empty());
		}

		#[test]
		fn ask_state_answers_recommended_question() {
			let (reply, _rx) = tokio::sync::oneshot::channel();
			let question = AskQuestion {
				id: "q".into(),
				question: "pick".into(),
				header: None,
				options: vec![
					pi_agent::tools::ask::AskOption {
						label: "one".into(),
						description: None,
						preview: None,
					},
					pi_agent::tools::ask::AskOption {
						label: "two".into(),
						description: None,
						preview: None,
					},
				],
				multi: false,
				recommended: Some(1),
			};
			let mut state = AskState::new(
				vec![question],
				reply,
				CancellationToken::new(),
				None,
			);
			let outcome = state.on_key(Key::Enter);
			match outcome {
				Some(AskOutcome::Answered(answers)) => {
					assert_eq!(answers[0].selected_options, vec!["two".to_string()]);
				},
				other => panic!("unexpected: {other:?}"),
			}
		}

		#[test]
		fn ui_commands_are_parsed_before_prompt_fallback() {
			assert_eq!(parse_ui_command("/status"), Some(UiCommand::Status));
			assert_eq!(
				parse_ui_command("/memory create keep terse"),
				Some(UiCommand::Memory(vec![
					"create".into(),
					"keep".into(),
					"terse".into()
				]))
			);
			assert_eq!(parse_ui_command("/unknown x"), None);
		}
	}
}
