//! Permanent transport contract for the C6 per-request credential callback.
//!
//! No mocks of provider behavior: a real localhost HTTP server observes the
//! exact request bytes/headers. Asserts (a) the callback runs before every HTTP
//! request with `force_refresh=false`, (b) 401/403 with a parsed `invalid_key`
//! body refreshes once and reissues the *same* body, (c) other 4xx bodies and
//! 200-with-nothing are never retried, (d) a second call starts with a fresh
//! (non-refresh) key.

use std::io::{Read, Write};
use std::net::TcpListener;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

use pi_agent::Message;
use pi_agent::provider::{
	CredentialCallback, ModelProvider, OpenAiCompatibleProvider, ProviderError, ToolChoice,
};

const OK_BODY: &str = "{\"choices\":[{\"message\":{\"role\":\"assistant\",\"content\":\"ok\"},\"finish_reason\":\"stop\"}]}";

struct Scripted {
	port: u16,
	requests: Arc<Mutex<Vec<String>>>,
	stop: std::sync::mpsc::Sender<()>,
	handle: Option<thread::JoinHandle<()>>,
}

impl Scripted {
	fn start(responses: Vec<(u16, String)>) -> Self {
		let listener = TcpListener::bind("127.0.0.1:0").expect("bind");
		let port = listener.local_addr().expect("addr").port();
		listener.set_nonblocking(true).expect("nonblocking");
		let (stop, stop_rx) = std::sync::mpsc::channel();
		let requests = Arc::new(Mutex::new(Vec::new()));
		let seen = Arc::clone(&requests);
		let responses = Arc::new(responses);
		let handle = thread::spawn(move || loop {
			if stop_rx.try_recv().is_ok() {
				break;
			}
			let Ok((mut stream, _)) = listener.accept() else {
				thread::sleep(Duration::from_millis(5));
				continue;
			};
			let mut buf = Vec::new();
			let mut chunk = [0u8; 4096];
			let mut head = None;
			while head.is_none() {
				match stream.read(&mut chunk) {
					Ok(0) => break,
					Ok(n) => {
						buf.extend_from_slice(&chunk[..n]);
						head = buf.windows(4).position(|w| w == b"\r\n\r\n");
					},
					Err(err) if err.kind() == std::io::ErrorKind::WouldBlock => {
						thread::sleep(Duration::from_millis(2));
					},
					Err(_) => break,
				}
			}
			let Some(position) = head else { continue };
			let text = String::from_utf8_lossy(&buf[..position]).to_string();
			let length: usize = text
				.lines()
				.find(|line| line.to_ascii_lowercase().starts_with("content-length:"))
				.and_then(|line| line.split(':').nth(1))
				.and_then(|value| value.trim().parse().ok())
				.unwrap_or(0);
			while buf.len() < position + 4 + length {
				match stream.read(&mut chunk) {
					Ok(0) => break,
					Ok(n) => buf.extend_from_slice(&chunk[..n]),
					Err(err) if err.kind() == std::io::ErrorKind::WouldBlock => {
						thread::sleep(Duration::from_millis(2));
					},
					Err(_) => break,
				}
			}
			let body_start = position + 4;
			let request = format!(
				"{}\n\n{}",
				text,
				String::from_utf8_lossy(&buf[body_start..])
			);
			let index = {
				let mut seen = seen.lock().expect("lock");
				seen.push(request);
				seen.len() - 1
			};
			let (status, body) = responses
				.get(index)
				.or_else(|| responses.last())
				.cloned()
				.unwrap_or((200, OK_BODY.to_string()));
			let response = format!(
				"HTTP/1.1 {status} X\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",
				body.len()
			);
			let _ = stream.write_all(response.as_bytes());
		});
		Self { port, requests, stop, handle: Some(handle) }
	}

	fn requests(&self) -> Vec<String> {
		self.requests.lock().expect("lock").clone()
	}

	fn stop(mut self) {
		let _ = self.stop.send(());
		if let Some(handle) = self.handle.take() {
			let _ = handle.join();
		}
	}
}

fn callback(flags: Arc<Mutex<Vec<bool>>>) -> CredentialCallback {
	Arc::new(move |force: bool| {
		let mut flags = flags.lock().expect("lock");
		flags.push(force);
		Ok(format!("k{}", flags.len() - 1))
	})
}

fn provider(port: u16, callback: CredentialCallback) -> OpenAiCompatibleProvider {
	OpenAiCompatibleProvider::new(
		format!("http://127.0.0.1:{port}/v1"),
		"static-stale",
		"test-model",
	)
	.expect("provider")
	.with_credential_callback(Some(callback))
}

fn body_of(request: &str) -> &str {
	request.split_once("\n\n").map(|(_, body)| body).unwrap_or("")
}

fn auth_of(request: &str) -> String {
	request
		.lines()
		.find(|line| line.to_ascii_lowercase().starts_with("authorization:"))
		.map(str::to_string)
		.unwrap_or_default()
}

#[tokio::test]
async fn invalid_key_refreshes_once_and_reissues_the_same_body() {
	let server = Scripted::start(vec![
		(401, "{\"error\":{\"code\":\"invalid_key\",\"message\":\"nope\"}}".to_string()),
		(200, OK_BODY.to_string()),
	]);
	let flags = Arc::new(Mutex::new(Vec::new()));
	let provider = provider(server.port, callback(Arc::clone(&flags)));
	let message = provider
		.complete(&[Message::user("hello")], &[], &ToolChoice::Auto)
		.await
		.expect("retried request succeeds");
	assert!(message.content.contains("ok"), "{}", message.content);

	let requests = server.requests();
	assert_eq!(requests.len(), 2, "exactly one retry");
	assert_eq!(flags.lock().expect("lock").clone(), vec![false, true]);
	assert_eq!(body_of(&requests[0]), body_of(&requests[1]), "same body");
	assert!(auth_of(&requests[0]).contains("k0"));
	assert!(auth_of(&requests[1]).contains("k1"));
	server.stop();
}

#[tokio::test]
async fn non_invalid_key_403_is_not_retried_and_new_call_starts_fresh() {
	let server = Scripted::start(vec![
		(403, "{\"error\":{\"code\":\"forbidden\",\"message\":\"no\"}}".to_string()),
		(200, OK_BODY.to_string()),
	]);
	let flags = Arc::new(Mutex::new(Vec::new()));
	let provider = provider(server.port, callback(Arc::clone(&flags)));
	let error = provider
		.complete(&[Message::user("hello")], &[], &ToolChoice::Auto)
		.await
		.expect_err("403 forbidden is surfaced");
	assert!(matches!(error, ProviderError::HttpStatus(403, _)), "got {error:?}");
	assert_eq!(server.requests().len(), 1, "no refresh for a non-key 403");
	assert_eq!(flags.lock().expect("lock").clone(), vec![false]);

	// The next HTTP request re-asks the resolver with `force_refresh=false`.
	provider
		.complete(&[Message::user("again")], &[], &ToolChoice::Auto)
		.await
		.expect("fresh key succeeds");
	assert_eq!(flags.lock().expect("lock").clone(), vec![false, false]);
	server.stop();
}

#[tokio::test]
async fn success_first_try_never_refreshes() {
	let server = Scripted::start(vec![(200, OK_BODY.to_string())]);
	let flags = Arc::new(Mutex::new(Vec::new()));
	let provider = provider(server.port, callback(Arc::clone(&flags)));
	provider
		.complete(&[Message::user("hello")], &[], &ToolChoice::Auto)
		.await
		.expect("succeeds");
	assert_eq!(server.requests().len(), 1);
	assert_eq!(flags.lock().expect("lock").clone(), vec![false]);
	server.stop();
}

#[tokio::test]
async fn anthropic_refreshes_once_per_request_and_redacts_used_tokens() {
	use pi_agent::provider_anthropic::AnthropicMessagesProvider;
	let server = Scripted::start(vec![
		(401, "{\"error\":{\"type\":\"invalid_key\",\"message\":\"bad sk-ant-secret-token-0\"}}".to_string()),
		(403, "{\"error\":{\"type\":\"invalid_key\",\"message\":\"bad sk-ant-secret-token-2\"}}".to_string()),
	]);
	let flags = Arc::new(Mutex::new(Vec::new()));
	// k0 on the first POST, k1 after the exact refresh, k2 for the reissued POST.
	let flags_for_callback = Arc::clone(&flags);
	let callback: CredentialCallback = Arc::new(move |force: bool| {
		let mut seen = flags_for_callback.lock().expect("lock");
		seen.push(force);
		let index = if force {
			1
		} else {
			if seen.iter().filter(|value| !**value).count() > 1 { 2 } else { 0 }
		};
		Ok(format!("sk-ant-secret-token-{index}"))
	});
	let provider = AnthropicMessagesProvider::new(
		format!("http://127.0.0.1:{}", server.port),
		"static-stale",
		"test-model",
	)
	.expect("provider")
	.with_credential_callback(Some(callback));
	let error = provider
		.complete(&[], &[], &ToolChoice::Auto)
		.await
		.expect_err("auth failure surfaces");
	let text = error.to_string();
	assert!(matches!(error, ProviderError::HttpStatus(403, _)), "got {error:?}");
	assert_eq!(server.requests().len(), 2, "one refresh, same body reissued");
	assert_eq!(flags.lock().expect("lock").clone(), vec![false, true, false]);
	assert!(
		!text.contains("sk-ant-secret-token-0") && !text.contains("sk-ant-secret-token-2"),
		"tokens redacted: {text}"
	);
	server.stop();
}

#[tokio::test]
async fn anthropic_non_auth_error_body_hides_the_token_actually_used() {
	use pi_agent::provider_anthropic::AnthropicMessagesProvider;
	let leaked = "sk-ant-callback-token-400";
	let server = Scripted::start(vec![(
		400,
		format!(
			"{{\"error\":{{\"type\":\"invalid_request_error\",\"message\":\"bad body for {leaked}\"}}}}"
		),
	)]);
	let callback: CredentialCallback = Arc::new(move |_force: bool| Ok(leaked.to_string()));
	let provider = AnthropicMessagesProvider::new(
		format!("http://127.0.0.1:{}", server.port),
		"unrelated-static-key",
		"test-model",
	)
	.expect("provider")
	.with_credential_callback(Some(callback));
	let error = provider
		.complete(&[], &[], &ToolChoice::Auto)
		.await
		.expect_err("400 surfaces");
	let text = error.to_string();
	assert!(matches!(error, ProviderError::HttpStatus(400, _)), "got {error:?}");
	assert!(!text.contains(leaked), "used callback token must never leak: {text}");
	server.stop();
}

#[tokio::test]
async fn anthropic_fast_fallback_error_uses_the_retry_token() {
	use pi_agent::provider_anthropic::AnthropicMessagesProvider;
	let first = "sk-ant-fast-original";
	let retry = "sk-ant-fast-retry";
	let server = Scripted::start(vec![
		(
			400,
			"{\"error\":{\"type\":\"invalid_request_error\",\"message\":\"speed not supported\"}}"
				.to_string(),
		),
		(
			400,
			format!("{{\"error\":{{\"type\":\"invalid_request_error\",\"message\":\"retry echo {retry}\"}}}}"),
		),
	]);
	let flags = Arc::new(Mutex::new(Vec::new()));
	let flags_for_callback = Arc::clone(&flags);
	let callback: CredentialCallback = Arc::new(move |force: bool| {
		let mut seen = flags_for_callback.lock().expect("lock");
		seen.push(force);
		Ok(if seen.len() == 1 { first.to_string() } else { retry.to_string() })
	});
	let provider = AnthropicMessagesProvider::new(
		format!("http://127.0.0.1:{}", server.port),
		"static-key",
		"test-model",
	)
	.expect("provider")
	.with_service_tier(Some("priority".to_string()))
	.with_credential_callback(Some(callback));
	let error = match provider.stream(&[], &[], &ToolChoice::Auto).await {
		Ok(_) => panic!("retry failure surfaces"),
		Err(error) => error,
	};
	let text = error.to_string();
	assert!(!text.contains(retry), "retry token must be redacted: {text}");
	assert_eq!(server.requests().len(), 2);
	server.stop();
}

#[tokio::test]
async fn anthropic_fast_fallback_sse_error_redacts_the_retry_token() {
	use futures::StreamExt;
	use pi_agent::provider_anthropic::AnthropicMessagesProvider;
	let retry = "sk-ant-sse-retry-token";
	let sse = format!(
		"event: error\ndata: {{\"type\":\"error\",\"error\":{{\"type\":\"api_error\",\"message\":\"echo {retry}\"}}}}\n\n"
	);
	let server = Scripted::start(vec![
		(
			400,
			"{\"error\":{\"type\":\"invalid_request_error\",\"message\":\"speed not supported\"}}"
				.to_string(),
		),
		(200, sse),
	]);
	let calls = Arc::new(Mutex::new(0u32));
	let calls_for_callback = Arc::clone(&calls);
	let callback: CredentialCallback = Arc::new(move |_force: bool| {
		let mut count = calls_for_callback.lock().expect("lock");
		*count += 1;
		Ok(if *count == 1 { "sk-ant-sse-original".to_string() } else { retry.to_string() })
	});
	let provider = AnthropicMessagesProvider::new(
		format!("http://127.0.0.1:{}", server.port),
		"static-key",
		"test-model",
	)
	.expect("provider")
	.with_service_tier(Some("priority".to_string()))
	.with_credential_callback(Some(callback));
	let mut stream = provider
		.stream(&[], &[], &ToolChoice::Auto)
		.await
		.expect("retry response opens a stream");
	let item = stream.next().await.expect("error event");
	let text = format!("{item:?}");
	assert!(!text.contains(retry), "SSE error must redact retry token: {text}");
	server.stop();
}
