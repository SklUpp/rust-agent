//! `pi_agent::AcpClientBridge` over [`AcpReverseClient`].
//!
//! The reverse client is synchronous: `ReverseCaller` waits on a reply channel
//! poll loop, so every call runs on `spawn_blocking` instead of occupying a
//! Tokio worker. Capability gates answer from the exact
//! `ClientCapabilities` the session advertised; a reverse call that was never
//! sent maps to [`AcpClientError::Unsupported`], and a cancellation maps to
//! [`AcpClientError::Cancelled`] so a native tool never retries a side effect.
//!
//! Mounted by [`super::session`] on `RpcSession` and copied into the
//! `RunRequest`, then onto the root `read`/`write`/`bash` tools and the
//! supervisor's child registries.

use async_trait::async_trait;
use pi_agent::{AcpClientBridge, AcpClientError, CancellationToken, state::PathMap};

use super::{protocol::ClientCapabilities, reverse::ReverseError, session::AcpReverseClient};

/// Reverse sender bound to one ACP session's capabilities.
pub struct AcpToolBridge {
	client:   AcpReverseClient,
	caps:     ClientCapabilities,
	path_map: Option<PathMap>,
}

impl AcpToolBridge {
	#[must_use]
	pub fn new(client: AcpReverseClient, caps: ClientCapabilities) -> Self {
		Self { client, caps, path_map: None }
	}

	/// Attach the agent-root → ACP-client-root map used by `read_text_file`,
	/// `write_text_file` and the `terminal_create` working directory.
	/// `None` keeps the historical identity behavior.
	#[must_use]
	pub fn with_path_map(mut self, path_map: Option<PathMap>) -> Self {
		self.path_map = path_map;
		self
	}

	/// Translate one agent-visible path before it reaches the wire. Fails
	/// closed outside the mapped root, so no reverse call is sent.
	fn map_path(&self, path: &std::path::Path) -> Result<String, AcpClientError> {
		match &self.path_map {
			Some(map) => map
				.to_client_path(path)
				.map(|mapped| mapped.to_string_lossy().into_owned()),
			// No map is the intentional identity mode: the session mounts one
			// only when the agent root differs from the client workspace root
			// (`session.rs:414`), matching `PathMap::is_identity`'s documented
			// backward-compatible passthrough for equal roots.
			None => Ok(path.to_string_lossy().into_owned()),
		}
	}
}

/// Run one blocking reverse call off the Tokio worker.
async fn off<T, F>(work: F) -> Result<T, AcpClientError>
where
	F: FnOnce() -> Result<T, ReverseError> + Send + 'static,
	T: Send + 'static,
{
	tokio::task::spawn_blocking(work)
		.await
		.map_err(|error| AcpClientError::Remote(format!("ACP reverse worker failed: {error}")))?
		.map_err(map_reverse)
}

fn map_reverse(error: ReverseError) -> AcpClientError {
	match error {
		ReverseError::Cancelled => AcpClientError::Cancelled,
		// `AcpReverseClient` reports an unadvertised capability through
		// `ReverseError::Send` with this exact prefix; nothing reached the wire.
		ReverseError::Send { ref reason, .. } if reason.starts_with("client did not advertise") => {
			AcpClientError::Unsupported
		},
		other => AcpClientError::Remote(other.to_string()),
	}
}

#[async_trait]
impl AcpClientBridge for AcpToolBridge {
	fn supports_fs_read(&self) -> bool {
		self.caps.fs.read_text_file
	}

	fn map_read_path(&self, path: &std::path::Path) -> Result<String, AcpClientError> {
		self.map_path(path)
	}

	fn supports_fs_write(&self) -> bool {
		self.caps.fs.write_text_file
	}

	fn supports_terminal(&self) -> bool {
		self.caps.terminal
	}

	async fn read_text_file(
		&self,
		path: &str,
		line: Option<u64>,
		limit: Option<u64>,
		cancel: &CancellationToken,
	) -> Result<String, AcpClientError> {
		let client = self.client.clone();
		let path = path.to_string();
		let cancel = cancel.clone();
		off(move || client.read_text_file(&path, line, limit, &cancel)).await
	}

	async fn write_text_file(
		&self,
		path: &str,
		content: &str,
		cancel: &CancellationToken,
	) -> Result<(), AcpClientError> {
		let path = self.map_path(std::path::Path::new(path))?;
		let client = self.client.clone();
		let content = content.to_string();
		let cancel = cancel.clone();
		off(move || client.write_text_file(&path, &content, &cancel)).await
	}

	async fn terminal_create(
		&self,
		command: &str,
		args: Vec<String>,
		env: Vec<(String, String)>,
		cwd: Option<&str>,
		output_byte_limit: Option<u64>,
		cancel: &CancellationToken,
	) -> Result<String, AcpClientError> {
		let cwd = match cwd {
			Some(cwd) => Some(self.map_path(std::path::Path::new(cwd))?),
			None => None,
		};
		let client = self.client.clone();
		let command = command.to_string();
		let cancel = cancel.clone();
		off(move || {
			client.terminal_create(
				&command,
				Some(args),
				Some(env),
				cwd.as_deref(),
				output_byte_limit,
				&cancel,
			)
		})
		.await
	}

	async fn terminal_output(
		&self,
		terminal_id: &str,
		cancel: &CancellationToken,
	) -> Result<serde_json::Value, AcpClientError> {
		let client = self.client.clone();
		let terminal_id = terminal_id.to_string();
		let cancel = cancel.clone();
		off(move || client.terminal_output(&terminal_id, &cancel)).await
	}

	async fn terminal_wait_for_exit(
		&self,
		terminal_id: &str,
		cancel: &CancellationToken,
	) -> Result<serde_json::Value, AcpClientError> {
		let client = self.client.clone();
		let terminal_id = terminal_id.to_string();
		let cancel = cancel.clone();
		off(move || client.terminal_wait_for_exit(&terminal_id, &cancel)).await
	}

	async fn terminal_kill(
		&self,
		terminal_id: &str,
		cancel: &CancellationToken,
	) -> Result<serde_json::Value, AcpClientError> {
		let client = self.client.clone();
		let terminal_id = terminal_id.to_string();
		let cancel = cancel.clone();
		off(move || client.terminal_kill(&terminal_id, &cancel)).await
	}

	async fn terminal_release(
		&self,
		terminal_id: &str,
		cancel: &CancellationToken,
	) -> Result<serde_json::Value, AcpClientError> {
		let client = self.client.clone();
		let terminal_id = terminal_id.to_string();
		let cancel = cancel.clone();
		off(move || client.terminal_release(&terminal_id, &cancel)).await
	}
}

#[cfg(test)]
mod tests {
	use std::{
		path::PathBuf,
		sync::{
			Arc,
			atomic::{AtomicU64, Ordering},
		},
		thread,
	};

	use serde_json::{Value, json};

	use crate::acp::{
		protocol::{ClientCapabilities, FileSystemCapabilities},
		reverse::ReverseCaller,
		session::AcpReverseClient,
		transport::{
			Transport,
			test_support::{Recorder, RecordingWriter, line, transport_with},
		},
	};

	use super::*;

	struct Fixture {
		bridge:   AcpToolBridge,
		writer:   RecordingWriter,
		transport: Transport,
		recorder:  Arc<Recorder>,
	}

	fn fixture(path_map: Option<PathMap>) -> Fixture {
		let writer = RecordingWriter::default();
		let (transport, recorder) = transport_with(&writer);
		let caps = ClientCapabilities {
			fs:       FileSystemCapabilities { write_text_file: true, ..Default::default() },
			terminal: true,
			..ClientCapabilities::default()
		};
		let client = AcpReverseClient::new(
			"session".to_string(),
			caps.clone(),
			ReverseCaller::new(transport.clone()),
			transport.clone(),
		);
		Fixture {
			bridge: AcpToolBridge::new(client, caps).with_path_map(path_map),
			writer,
			transport,
			recorder,
		}
	}

	/// Answer the next `method` frame with `result` from the fake client.
	fn answer(
		writer: &RecordingWriter,
		transport: &Transport,
		method: &'static str,
		result: Value,
	) -> thread::JoinHandle<()> {
		let writer = writer.clone();
		let transport = transport.clone();
		thread::spawn(move || {
			let frame = writer.wait_frame(|frame| frame["method"] == json!(method));
			transport.handle_line(&line(json!({
				"jsonrpc": "2.0",
				"id": frame["id"],
				"result": result,
			})));
		})
	}

	static SCRATCH_ID: AtomicU64 = AtomicU64::new(0);

	struct Roots {
		base:   PathBuf,
		agent:  PathBuf,
		client: PathBuf,
	}

	fn roots(label: &str) -> Roots {
		let base = loop {
			let unique = SCRATCH_ID.fetch_add(1, Ordering::Relaxed);
			let base = std::env::temp_dir().join(format!(
				"aragent-bridge-{}-{label}-{unique}",
				std::process::id()
			));
			match std::fs::create_dir(&base) {
				Ok(()) => break base,
				Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => continue,
				Err(error) => panic!("create scratch directory {}: {error}", base.display()),
			}
		};
		let agent = base.join("agent");
		let client = base.join("client");
		std::fs::create_dir(&agent).unwrap();
		std::fs::create_dir(&client).unwrap();
		Roots {
			base,
			agent: std::fs::canonicalize(agent).unwrap(),
			client: std::fs::canonicalize(client).unwrap(),
		}
	}

	impl Drop for Roots {
		fn drop(&mut self) {
			let _ = std::fs::remove_dir_all(&self.base);
		}
	}

	#[tokio::test]
	async fn no_path_map_preserves_write_path_and_terminal_cwd() {
		let fixture = fixture(None);
		let path = "/agent/root/../keep/new.txt";
		let cwd = "/work dir";

		let write = answer(&fixture.writer, &fixture.transport, "fs/write_text_file", json!(null));
		fixture
			.bridge
			.write_text_file(path, "bytes", &CancellationToken::new())
			.await
			.expect("write");
		write.join().unwrap();

		let terminal = answer(
			&fixture.writer,
			&fixture.transport,
			"terminal/create",
			json!({ "terminalId": "t-1" }),
		);
		let id = fixture
			.bridge
			.terminal_create("sh", vec![], vec![], Some(cwd), None, &CancellationToken::new())
			.await
			.expect("terminal");
		assert_eq!(id, "t-1");
		terminal.join().unwrap();

		let frames = fixture.writer.frames();
		let write_frame =
			frames.iter().find(|frame| frame["method"] == json!("fs/write_text_file")).unwrap();
		assert_eq!(write_frame["params"]["path"], json!(path));
		let terminal_frame =
			frames.iter().find(|frame| frame["method"] == json!("terminal/create")).unwrap();
		assert_eq!(terminal_frame["params"]["cwd"], json!(cwd));
	}

	#[tokio::test]
	async fn path_map_rewrites_write_path_and_terminal_cwd() {
		let roots = roots("mapped");
		std::fs::create_dir_all(roots.agent.join("work")).unwrap();
		let fixture =
			fixture(Some(PathMap::new(roots.agent.clone(), roots.client.clone())));

		let write_path = roots.agent.join("new.txt");
		let write =
			answer(&fixture.writer, &fixture.transport, "fs/write_text_file", json!(null));
		fixture
			.bridge
			.write_text_file(&write_path.to_string_lossy(), "bytes", &CancellationToken::new())
			.await
			.expect("write");
		write.join().unwrap();

		let work = roots.agent.join("work");
		let terminal = answer(
			&fixture.writer,
			&fixture.transport,
			"terminal/create",
			json!({ "terminalId": "t-2" }),
		);
		let id = fixture
			.bridge
			.terminal_create(
				"sh",
				vec![],
				vec![],
				Some(&work.to_string_lossy()),
				None,
				&CancellationToken::new(),
			)
			.await
			.expect("terminal");
		assert_eq!(id, "t-2");
		terminal.join().unwrap();

		let frames = fixture.writer.frames();
		let write_frame =
			frames.iter().find(|frame| frame["method"] == json!("fs/write_text_file")).unwrap();
		assert_eq!(
			write_frame["params"]["path"],
			json!(roots.client.join("new.txt").to_string_lossy())
		);
		let terminal_frame =
			frames.iter().find(|frame| frame["method"] == json!("terminal/create")).unwrap();
		assert_eq!(
			terminal_frame["params"]["cwd"],
			json!(roots.client.join("work").to_string_lossy())
		);
		assert_eq!(
			fixture.bridge.map_read_path(&write_path).unwrap(),
			roots.client.join("new.txt").to_string_lossy().into_owned()
		);
		assert!(fixture.recorder.seen().is_empty());
	}

	#[tokio::test]
	async fn out_of_root_write_and_terminal_cwd_fail_before_reverse_call() {
		let roots = roots("outside");
		let outside = roots.base.join("outside");
		std::fs::create_dir_all(&outside).unwrap();
		let fixture =
			fixture(Some(PathMap::new(roots.agent.clone(), roots.client.clone())));
		let outside_file = outside.join("x.txt");

		let write = fixture
			.bridge
			.write_text_file(&outside_file.to_string_lossy(), "bytes", &CancellationToken::new())
			.await;
		match write {
			Err(AcpClientError::Remote(message)) => {
				assert!(message.contains("outside"), "unexpected message: {message}");
			},
			other => panic!("expected Remote, got {other:?}"),
		}

		let terminal = fixture
			.bridge
			.terminal_create(
				"sh",
				vec![],
				vec![],
				Some(&outside.to_string_lossy()),
				None,
				&CancellationToken::new(),
			)
			.await;
		assert!(matches!(terminal, Err(AcpClientError::Remote(_))));

		assert!(fixture.recorder.seen().is_empty());
		assert!(fixture.writer.frames().is_empty());
	}
}
