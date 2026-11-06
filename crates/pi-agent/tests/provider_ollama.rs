//! Consumer boundaries for the Ollama `/api/chat` provider:
//! - request body shape: Ollama history conversion, vision guard, schema
//!   sanitization, cloud history stripping, no-user demotion, options and
//!   forced tool selection;
//! - NDJSON framing across transport chunks, UTF-8 sequences and line
//!   boundaries, EOF tails, malformed/truncated records and done-reason errors;
//! - bounded HTTP error bodies, credential-free diagnostics and drop
//!   cancellation closing the transport.
//!
//! The fixtures are local TCP servers: no Ollama daemon, model or account is
//! required.

use std::time::Duration;

use futures::StreamExt;
use pi_agent::provider_ollama::{
	DeepSeekInbandScanner, InbandScanEvent, KimiInbandScanner, MAX_SECTION_BYTES,
	MAX_STREAM_HOLD_BYTES, MarkupHealer, MarkupHealingEvent, OllamaPayloadCost,
	OllamaPayloadHook, OllamaPayloadModel, OllamaPayloadThinking, OllamaProvider,
	StreamMarkupHealingPattern, ThinkingInbandScanner, demote_thinking, extract_retry_hint,
	get_stream_markup_healing_pattern, is_llama_cpp_parse_error, is_malformed_tool_call,
	normalize_kimi_function_name, preferred_dialect, sanitize_malformed_tool_calls,
	should_retry_ollama_response, NON_VISION_IMAGE_PLACEHOLDER,
};
use pi_agent::state::{Attribution, Role, StopReason};
use pi_agent::{
	ChunkStream, ContentPart, Message, ModelProvider, ProviderError, StreamChunk, ToolCall,
	ToolChoice,
};
use serde_json::{Value, json};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

async fn read_request(socket: &mut TcpStream) -> Option<(String, Value)> {
	let mut buf = Vec::new();
	let mut header_end = None;
	let mut tmp = [0u8; 1024];
	let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
	while header_end.is_none() && tokio::time::Instant::now() < deadline {
		let n = tokio::time::timeout(Duration::from_secs(5), socket.read(&mut tmp))
			.await
			.ok()?
			.ok()?;
		if n == 0 {
			return None;
		}
		buf.extend_from_slice(&tmp[..n]);
		header_end = buf.windows(4).position(|w| w == b"\r\n\r\n");
	}
	let header_end = header_end?;
	let headers = String::from_utf8_lossy(&buf[..header_end]).into_owned();
	let content_length = headers
		.lines()
		.find_map(|line| {
			line.to_ascii_lowercase()
				.strip_prefix("content-length:")
				.map(|value| value.trim().parse::<usize>().unwrap_or(0))
		})
		.unwrap_or(0);
	let body_start = header_end + 4;
	let mut body = buf[body_start..].to_vec();
	while body.len() < content_length {
		let n = tokio::time::timeout(Duration::from_secs(5), socket.read(&mut tmp))
			.await
			.ok()?
			.ok()?;
		if n == 0 {
			break;
		}
		body.extend_from_slice(&tmp[..n]);
	}
	Some((headers, serde_json::from_slice(&body).unwrap_or(Value::Null)))
}

fn response(status: &str, content_type: &str, body: &str) -> Vec<u8> {
	format!(
		"HTTP/1.1 {status}\r\nContent-Type: {content_type}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
		body.len()
	)
	.into_bytes()
}

async fn serve_once(response: Vec<u8>) -> (String, tokio::sync::oneshot::Receiver<(String, Value)>) {
	let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
	let addr = listener.local_addr().expect("addr");
	let (request_tx, request_rx) = tokio::sync::oneshot::channel();
	tokio::spawn(async move {
		let (mut socket, _) = listener.accept().await.expect("accept");
		if let Some(captured) = read_request(&mut socket).await {
			let _ = request_tx.send(captured);
		}
		let _ = socket.write_all(&response).await;
		let _ = socket.flush().await;
	});
	(format!("http://{addr}"), request_rx)
}

async fn serve_chunked(pieces: Vec<Vec<u8>>) -> String {
	let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
	let addr = listener.local_addr().expect("addr");
	tokio::spawn(async move {
		let (mut socket, _) = listener.accept().await.expect("accept");
		let _ = read_request(&mut socket).await;
		let headers = "HTTP/1.1 200 OK\r\nContent-Type: application/x-ndjson\r\nTransfer-Encoding: chunked\r\nConnection: close\r\n\r\n";
		let _ = socket.write_all(headers.as_bytes()).await;
		for piece in pieces {
			let mut framed = format!("{:x}\r\n", piece.len()).into_bytes();
			framed.extend_from_slice(&piece);
			framed.extend_from_slice(b"\r\n");
			if socket.write_all(&framed).await.is_err() {
				return;
			}
			let _ = socket.flush().await;
			tokio::time::sleep(Duration::from_millis(2)).await;
		}
		let _ = socket.write_all(b"0\r\n\r\n").await;
		let _ = socket.flush().await;
	});
	format!("http://{addr}")
}

fn make_provider(base: &str, model: &str) -> OllamaProvider {
	OllamaProvider::new(base, "test-key", model).expect("provider")
}

async fn drain(mut stream: ChunkStream) -> Result<Vec<StreamChunk>, ProviderError> {
	let mut chunks = Vec::new();
	while let Some(item) = stream.next().await {
		chunks.push(item?);
	}
	Ok(chunks)
}

async fn drain_body(records: &str) -> Vec<StreamChunk> {
	let base = serve_chunked(vec![records.as_bytes().to_vec()]).await;
	let provider = make_provider(&base, "m");
	drain(
		provider
			.stream(&[Message::user("hi")], &[], &ToolChoice::Auto)
			.await
			.expect("stream"),
	)
	.await
	.expect("chunks")
}

fn tool(name: &str, description: &str) -> Value {
	json!({
		"type": "function",
		"function": {
			"name": name,
			"description": description,
			"parameters": {
				"type": "object",
				"properties": {"path": {"type": "string"}},
				"additionalProperties": false
			}
		}
	})
}

#[tokio::test]
async fn request_body_matches_ollama_history_and_forced_tool_selection() {
	let (base, request_rx) = serve_once(response(
		"200 OK",
		"application/x-ndjson",
		"{\"message\":{\"content\":\"ok\"},\"done\":true,\"prompt_eval_count\":2,\"eval_count\":1}\n",
	))
	.await;

	let mut assistant = Message::assistant("");
	assistant.parts = vec![
		ContentPart::thinking("plan"),
		ContentPart::Text {
			text: "answer".into(),
		},
	];
	assistant.tool_calls = vec![ToolCall {
		id: "call_1".into(),
		name: "read".into(),
		arguments: json!({"path": "a"}),
	}];
	// Same provider/API/model as the target so native thinking replays; the
	// cross-model demotion contract has its own fixture below.
	assistant.provider = Some("ollama".into());
	assistant.api = Some("ollama-chat".into());
	assistant.model = Some("llama3.2".into());

	let mut image_user = Message::user_with_parts(vec![
		ContentPart::text("look"),
		ContentPart::image("aGVsbG8=", "image/png").expect("image"),
	]);
	image_user.content = "look".into();

	let messages = vec![
		Message::system("sys"),
		Message::user("hello"),
		assistant,
		Message::tool_with_status("call_1", "read", "file body", false),
		image_user,
	];
	let tools = vec![tool("read", "Read a file"), tool("write", "Write a file")];
	let choice = ToolChoice::Tool {
		name: "read".into(),
	};

	let provider = make_provider(&base, "llama3.2")
		.with_model_reasoning(true)
		.with_reasoning(Some("minimal".into()))
		.with_max_tokens(Some(70_000))
		.with_temperature(Some(0.2))
		.with_top_p(Some(0.9));
	let message = provider
		.complete(&messages, &tools, &choice)
		.await
		.expect("complete");
	assert_eq!(message.content, "ok");
	let usage = message.usage.expect("usage");
	assert_eq!((usage.input, usage.output, usage.total_tokens), (2, 1, 3));

	let (headers, body) = request_rx.await.expect("request");
	assert!(
		headers
			.to_ascii_lowercase()
			.contains("authorization: bearer test-key"),
		"{headers}"
	);
	assert_eq!(body["model"], "llama3.2");
	assert_eq!(body["stream"], true);
	assert_eq!(body["think"], "low");
	assert_eq!(body["tool_choice"], "required");
	assert_eq!(body["options"]["num_predict"], 70_000);
	assert_eq!(body["options"]["temperature"], 0.2);
	assert_eq!(body["options"]["top_p"], 0.9);
	assert!(!body.to_string().contains("test-key"));

	let wire_tools = body["tools"].as_array().expect("tools");
	assert_eq!(wire_tools.len(), 1, "forced choice narrows the offer");
	assert_eq!(wire_tools[0]["function"]["name"], "read");
	assert!(
		wire_tools[0]["function"]["parameters"]
			.get("additionalProperties")
			.is_none()
	);

	let wire_messages = body["messages"].as_array().expect("messages");
	assert_eq!(wire_messages.len(), 5);
	assert_eq!(wire_messages[0]["role"], "system");
	assert_eq!(wire_messages[0]["content"], "sys");
	assert_eq!(wire_messages[1]["role"], "user");
	assert_eq!(wire_messages[2]["role"], "assistant");
	assert_eq!(wire_messages[2]["content"], "answer");
	assert_eq!(wire_messages[2]["thinking"], "plan");
	assert_eq!(wire_messages[2]["tool_calls"][0]["function"]["name"], "read");
	assert_eq!(wire_messages[3]["role"], "tool");
	assert_eq!(wire_messages[3]["tool_name"], "read");
	assert_eq!(wire_messages[3]["content"], "file body");
	assert_eq!(wire_messages[4]["role"], "user");
	assert_eq!(
		wire_messages[4]["content"],
		format!("look\n{NON_VISION_IMAGE_PLACEHOLDER}")
	);
	assert!(wire_messages[4].get("images").is_none());
}

#[tokio::test]
async fn foreign_history_thinking_demotes_to_target_dialect_text() {
	let (base, request_rx) = serve_once(response(
		"200 OK",
		"application/x-ndjson",
		"{\"message\":{\"content\":\"ok\"},\"done\":true}\n",
	))
	.await;

	let mut assistant = Message::assistant("");
	assistant.parts = vec![
		ContentPart::thinking("plan"),
		ContentPart::text("answer"),
	];
	assistant.provider = Some("anthropic".into());
	assistant.api = Some("anthropic-messages".into());
	assistant.model = Some("claude-3".into());

	let provider = make_provider(&base, "llama3.2");
	provider
		.complete(
			&[Message::user("hi"), assistant],
			&[],
			&ToolChoice::Auto,
		)
		.await
		.expect("complete");

	let (_, body) = request_rx.await.expect("request");
	let wire = body["messages"].as_array().expect("messages");
	let history = &wire[1];
	assert_eq!(history["role"], "assistant");
	assert!(
		history.get("thinking").is_none(),
		"foreign thinking must not replay natively: {history}"
	);
	assert_eq!(
		history["content"], "<thinking>\nplan\n</thinking>\nanswer",
		"demoted reasoning is consumer-visible content"
	);
}

#[tokio::test]
async fn foreign_history_for_hermes_target_demotes_as_think_block() {
	let (base, request_rx) = serve_once(response(
		"200 OK",
		"application/x-ndjson",
		"{\"message\":{\"content\":\"ok\"},\"done\":true}\n",
	))
	.await;

	let mut assistant = Message::assistant("");
	assistant.parts = vec![
		ContentPart::thinking("plan"),
		ContentPart::text("answer"),
	];
	assistant.provider = Some("anthropic".into());
	assistant.api = Some("anthropic-messages".into());
	assistant.model = Some("claude-3".into());

	let provider = make_provider(&base, "hermes-3");
	provider
		.complete(&[Message::user("hi"), assistant], &[], &ToolChoice::Auto)
		.await
		.expect("complete");

	let (_, body) = request_rx.await.expect("request");
	let history = &body["messages"][1];
	assert_eq!(history["role"], "assistant");
	assert!(history.get("thinking").is_none());
	assert_eq!(
		history["content"], "\x3cthink\x3e\nplan\n\x3c/think\x3e\nanswer",
		"Hermes target must receive a delimited think block, not <thinking>"
	);
}

#[tokio::test]
async fn hermes_wire_unwraps_nested_sibling_and_keeps_partial_wrapper() {
	let (base, request_rx) = serve_once(response(
		"200 OK",
		"application/x-ndjson",
		"{\"message\":{\"content\":\"ok\"},\"done\":true}\n",
	))
	.await;
	let foreign = |thinking: &str| {
		let mut message = Message::assistant("");
		message.parts = vec![
			ContentPart::thinking(thinking),
			ContentPart::text("answer"),
		];
		message.provider = Some("anthropic".into());
		message.api = Some("anthropic-messages".into());
		message.model = Some("claude-3".into());
		message
	};
	let messages = vec![
		Message::user("hi"),
		foreign("plan"),
		foreign("\x3cthink\x3e\n\x3cthink\x3e\nnested\n\x3c/think\x3e\n\x3c/think\x3e"),
		foreign("\x3cthink\x3ea\x3c/think\x3e\x3cthink\x3eb\x3c/think\x3e"),
		foreign("\x3cthink\x3epartial"),
	];
	let provider = make_provider(&base, "hermes-3");
	provider
		.complete(&messages, &[], &ToolChoice::Auto)
		.await
		.expect("complete");

	let (_, body) = request_rx.await.expect("request");
	let wire = body["messages"].as_array().expect("messages");
	assert_eq!(
		wire[1]["content"],
		"\x3cthink\x3e\nplan\n\x3c/think\x3e\nanswer"
	);
	assert_eq!(
		wire[2]["content"],
		"\x3cthink\x3e\nnested\n\x3c/think\x3e\nanswer"
	);
	assert_eq!(
		wire[3]["content"],
		"\x3cthink\x3e\na\nb\n\x3c/think\x3e\nanswer"
	);
	assert_eq!(
		wire[4]["content"],
		"\x3cthink\x3e\n\x3cthink\x3epartial\n\x3c/think\x3e\nanswer"
	);
	for message in &wire[1..] {
		assert!(message.get("thinking").is_none());
	}
}

#[tokio::test]
async fn hermes_deep_nesting_unwraps_without_stack_overflow() {
	const DEPTH: usize = 4_000;
	let open = "\x3cthink\x3e";
	let close = "\x3c/think\x3e";
	let mut thinking = String::new();
	for _ in 0..DEPTH {
		thinking.push_str(open);
	}
	thinking.push_str("\ndeep\n");
	for _ in 0..DEPTH {
		thinking.push_str(close);
	}
	let (base, request_rx) = serve_once(response(
		"200 OK",
		"application/x-ndjson",
		"{\"message\":{\"content\":\"ok\"},\"done\":true}\n",
	))
	.await;
	let mut assistant = Message::assistant("");
	assistant.parts = vec![
		ContentPart::thinking(thinking),
		ContentPart::text("answer"),
	];
	assistant.provider = Some("anthropic".into());
	assistant.api = Some("anthropic-messages".into());
	assistant.model = Some("claude-3".into());
	let provider = make_provider(&base, "hermes-3");
	provider
		.complete(&[Message::user("hi"), assistant], &[], &ToolChoice::Auto)
		.await
		.expect("complete");
	let (_, body) = request_rx.await.expect("request");
	let expected = format!("{open}\ndeep\n{close}\nanswer");
	assert_eq!(body["messages"][1]["content"], expected);
}

#[tokio::test]
async fn hermes_deep_sibling_nesting_outputs_exact_flat_text() {
	const DEPTH: usize = 3_000;
	let open = "\x3cthink\x3e";
	let close = "\x3c/think\x3e";
	let mut thinking = format!("{open}z{close}");
	for _ in 0..DEPTH {
		thinking = format!("{open}{thinking}{close}{open}x{close}");
	}
	thinking = format!("{open}{thinking}{close}");
	let (base, request_rx) = serve_once(response(
		"200 OK",
		"application/x-ndjson",
		"{\"message\":{\"content\":\"ok\"},\"done\":true}\n",
	))
	.await;
	let mut assistant = Message::assistant("");
	assistant.parts = vec![
		ContentPart::thinking(thinking),
		ContentPart::text("answer"),
	];
	assistant.provider = Some("anthropic".into());
	assistant.api = Some("anthropic-messages".into());
	assistant.model = Some("claude-3".into());
	let provider = make_provider(&base, "hermes-3");
	provider
		.complete(&[Message::user("hi"), assistant], &[], &ToolChoice::Auto)
		.await
		.expect("complete");
	let (_, body) = request_rx.await.expect("request");
	let expected = format!(
		"{open}\nz{}\n{close}\nanswer",
		"\nx".repeat(DEPTH)
	);
	assert_eq!(body["messages"][1]["content"], expected);
}

#[tokio::test]
async fn qwen3_history_double_wraps_existing_think_block_on_wire() {
	let (base, request_rx) = serve_once(response(
		"200 OK",
		"application/x-ndjson",
		"{\"message\":{\"content\":\"ok\"},\"done\":true}\n",
	))
	.await;
	let mut assistant = Message::assistant("");
	assistant.parts = vec![
		ContentPart::thinking("\x3cthink\x3e\nplan\n\x3c/think\x3e"),
		ContentPart::text("answer"),
	];
	assistant.provider = Some("anthropic".into());
	assistant.api = Some("anthropic-messages".into());
	assistant.model = Some("claude-3".into());
	let provider = make_provider(&base, "qwen-2.5-coder");
	provider
		.complete(&[Message::user("hi"), assistant], &[], &ToolChoice::Auto)
		.await
		.expect("complete");
	let (_, body) = request_rx.await.expect("request");
	assert_eq!(
		body["messages"][1]["content"],
		"\x3cthink\x3e\n\x3cthink\x3e\nplan\n\x3c/think\x3e\n\x3c/think\x3e\nanswer"
	);
}

#[tokio::test]
async fn cloud_dialect_caps_tokens_strips_history_thinking_and_demotes_system() {
	let (base, request_rx) = serve_once(response(
		"200 OK",
		"application/x-ndjson",
		"{\"message\":{\"content\":\"ok\"},\"done\":true}\n",
	))
	.await;

	let mut assistant = Message::assistant("");
	assistant.parts = vec![ContentPart::thinking("private plan")];
	let messages = vec![
		Message::system("prefix"),
		assistant,
		Message::developer("reminder"),
	];

	let provider = make_provider(&base, "deepseek-v4-pro")
		.with_cloud(true)
		.with_model_reasoning(true)
		.with_reasoning(Some("minimal".into()))
		.with_max_tokens(Some(70_000));
	provider
		.complete(&messages, &[], &ToolChoice::Auto)
		.await
		.expect("complete");

	let (_, body) = request_rx.await.expect("request");
	assert_eq!(body["options"]["num_predict"], 65_536);
	assert_eq!(body["think"], "low");
	let wire = body["messages"].as_array().expect("messages");
	assert_eq!(wire[1]["role"], "assistant");
	assert!(wire[1].get("thinking").is_none(), "cloud drops history thinking");
	assert_eq!(wire[2]["role"], "user", "no-user request demotes the last system turn");
}

#[tokio::test]
async fn omit_max_output_tokens_and_disable_reasoning_omit_wire_fields() {
	let (base, request_rx) = serve_once(response(
		"200 OK",
		"application/x-ndjson",
		"{\"message\":{\"content\":\"ok\"},\"done\":true,\"eval_count\":1}\n",
	))
	.await;
	let provider = make_provider(&base, "m")
		.with_model_reasoning(true)
		.with_disable_reasoning(true)
		.with_max_tokens(Some(100))
		.with_omit_max_output_tokens(true);
	provider
		.complete(&[Message::user("hi")], &[], &ToolChoice::Auto)
		.await
		.expect("complete");
	let (_, body) = request_rx.await.expect("request");
	assert_eq!(body["think"], false);
	assert!(body.get("options").is_none(), "{body}");
}

#[tokio::test]
async fn forced_tool_choice_is_validated_before_the_socket_opens() {
	let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
	let addr = listener.local_addr().expect("addr");
	let provider = make_provider(&format!("http://{addr}"), "m");
	let err = provider
		.stream(
			&[Message::user("hi")],
			&[tool("read", "Read")],
			&ToolChoice::Tool {
				name: "missing".into(),
			},
		)
		.await
		.err()
		.expect("unoffered tool must fail before the request");
	match err {
		ProviderError::InvalidToolChoice(message) => {
			assert!(message.contains("not offered"), "{message}");
		},
		other => panic!("expected InvalidToolChoice, got {other:?}"),
	}
	assert!(
		tokio::time::timeout(Duration::from_millis(300), listener.accept())
			.await
			.is_err(),
		"validation must not open a connection"
	);
}

#[tokio::test]
async fn stream_handles_fragmented_utf8_text_thinking_tools_and_usage() {
	let records = concat!(
		"{\"message\":{\"thinking\":\"plan \"},\"done\":false}\n",
		"{\"message\":{\"thinking\":\"\u{2713}\"},\"done\":false}\n",
		"{\"message\":{\"content\":\"Hello\"},\"done\":false}\n",
		"{\"message\":{\"content\":\", world\"},\"done\":false}\n",
		"{\"message\":{\"tool_calls\":[{\"type\":\"function\",\"function\":{\"name\":\"read\",\"arguments\":{\"path\":\"a\"}}}]},\"done\":false}\n",
		"{\"message\":{\"content\":\"after\"},\"done\":false}\n",
		"{\"done\":true,\"done_reason\":\"tool_calls\",\"prompt_eval_count\":12,\"eval_count\":7}\n",
	);
	let pieces: Vec<Vec<u8>> = records
		.as_bytes()
		.chunks(7)
		.map(|piece| piece.to_vec())
		.collect();
	let base = serve_chunked(pieces).await;

	let provider = make_provider(&base, "llama3.2");
	let chunks = drain(
		provider
			.stream(&[Message::user("hi")], &[], &ToolChoice::Auto)
			.await
			.expect("stream"),
	)
	.await
	.expect("chunks");

	let done = chunks.last().expect("done");
	assert!(done.is_done);
	assert_eq!(done.stop_reason, Some(StopReason::ToolUse));
	assert_eq!(done.error_message, None);
	let usage = done.usage.expect("usage");
	assert_eq!((usage.input, usage.output, usage.total_tokens), (12, 7, 19));

	let indices: Vec<Option<usize>> = chunks
		.iter()
		.filter(|chunk| !chunk.is_done)
		.map(|chunk| chunk.content_index)
		.collect();
	assert_eq!(
		indices,
		vec![Some(0), Some(0), Some(1), Some(1), Some(2), Some(3)],
		"thinking 0, text 1, tool 2, text after the tool 3"
	);

	let mut reasoning = String::new();
	let mut content = String::new();
	let mut deltas = Vec::new();
	for chunk in &chunks {
		if let Some(delta) = &chunk.reasoning {
			reasoning.push_str(delta);
		}
		if let Some(delta) = &chunk.content {
			content.push_str(delta);
		}
		deltas.extend(chunk.tool_call_deltas.iter().cloned());
	}
	assert_eq!(reasoning, "plan \u{2713}");
	assert_eq!(content, "Hello, worldafter");
	assert_eq!(deltas.len(), 1);
	assert_eq!(deltas[0].id.as_deref(), Some("ollama:2:read"));
	assert_eq!(deltas[0].name.as_deref(), Some("read"));
	assert_eq!(deltas[0].arguments_delta.as_deref(), Some("{\"path\":\"a\"}"));
	assert_eq!(deltas[0].content_index, Some(2));

	let mut assembler = pi_agent::StreamAssembler::new();
	for chunk in &chunks {
		assembler.push(chunk).expect("indexed chunk");
	}
	let message = assembler.finish().expect("assembled");
	assert_eq!(message.stop_reason, Some(StopReason::ToolUse));
	assert_eq!(message.content, "Hello, worldafter");
	assert_eq!(
		message.parts,
		vec![
			ContentPart::thinking("plan \u{2713}"),
			ContentPart::text("Hello, world"),
			ContentPart::ToolCall {
				id: "ollama:2:read".to_string(),
				name: "read".to_string(),
				arguments: json!({"path": "a"}),
			},
			ContentPart::text("after"),
		]
	);
	assert_eq!(message.tool_calls.len(), 1);
	assert_eq!(message.tool_calls[0].name, "read");
	assert_eq!(message.tool_calls[0].arguments, json!({"path": "a"}));
}

#[tokio::test]
async fn length_with_visible_content_keeps_text_and_reports_length() {
	let body = "{\"message\":{\"content\":\"partial\"},\"done\":true,\"done_reason\":\"length\",\"prompt_eval_count\":4,\"eval_count\":2}\n";
	let base = serve_chunked(vec![body.as_bytes().to_vec()]).await;
	let provider = make_provider(&base, "m");
	let chunks = drain(
		provider
			.stream(&[Message::user("hi")], &[], &ToolChoice::Auto)
			.await
			.expect("stream"),
	)
	.await
	.expect("chunks");
	assert_eq!(chunks[0].content.as_deref(), Some("partial"));
	assert_eq!(chunks[0].content_index, Some(0));
	let done = chunks.last().expect("done");
	assert!(done.is_done);
	assert_eq!(done.stop_reason, Some(StopReason::Length));
	assert_eq!(done.error_message, None, "visible output is not an error");
	assert_eq!(done.usage.expect("usage").total_tokens, 6);

	let mut assembler = pi_agent::StreamAssembler::new();
	for chunk in &chunks {
		assembler.push(chunk).expect("indexed chunk");
	}
	let message = assembler.finish().expect("assembled");
	assert_eq!(message.content, "partial");
	assert_eq!(message.stop_reason, Some(StopReason::Length));
	assert_eq!(message.error_message, None);
}

#[tokio::test]
async fn stream_flushes_an_eof_record_without_a_trailing_newline() {
	let body = "{\"message\":{\"content\":\"tail\"},\"done\":false}";
	let base = serve_chunked(vec![body.as_bytes().to_vec()]).await;
	let provider = make_provider(&base, "m");
	let chunks = drain(
		provider
			.stream(&[Message::user("hi")], &[], &ToolChoice::Auto)
			.await
			.expect("stream"),
	)
	.await
	.expect("chunks");
	assert_eq!(chunks.len(), 2);
	assert_eq!(chunks[0].content.as_deref(), Some("tail"));
	assert!(chunks[1].is_done);
	assert_eq!(chunks[1].stop_reason, Some(StopReason::Stop));
	assert_eq!(chunks[1].error_message, None);
	assert!(chunks[1].usage.is_none());
}

#[tokio::test]
async fn held_partial_tail_is_visible_so_length_stays_length() {
	// The entire content is a held markup prefix: it is not visible before the
	// terminal flush, so without flushing first `length` would become the empty
	// completion error.
	let body = "{\"message\":{\"content\":\"<\"},\"done\":false}\n{\"done\":true,\"done_reason\":\"length\",\"eval_count\":1}\n";
	let (base, _request_rx) =
		serve_once(response("200 OK", "application/x-ndjson", body)).await;
	let provider = make_provider(&base, "m");
	let message = provider
		.complete(&[Message::user("hi")], &[], &ToolChoice::Auto)
		.await
		.expect("complete");
	assert_eq!(message.stop_reason, Some(StopReason::Length));
	assert_eq!(message.content, "<");
}

#[tokio::test]
async fn done_record_does_not_end_stream_before_eof() {
	// A `done:true` record mid-stream must not stop reading; content before the
	// real EOF is still assembled and the terminal chunk waits for EOF.
	let body = concat!(
		"{\"message\":{\"content\":\"early\"},\"done\":false}\n",
		"{\"done\":true,\"done_reason\":\"stop\",\"eval_count\":1}\n",
		"{\"message\":{\"content\":\"late\"},\"done\":true}\n",
	);
	let (base, _request_rx) =
		serve_once(response("200 OK", "application/x-ndjson", body)).await;
	let provider = make_provider(&base, "m");
	let message = provider
		.complete(&[Message::user("hi")], &[], &ToolChoice::Auto)
		.await
		.expect("complete");
	assert_eq!(message.stop_reason, Some(StopReason::Stop));
	assert_eq!(message.content, "earlylate");
}

#[tokio::test]
async fn malformed_and_truncated_records_end_in_a_terminal_error_event() {
	let base = serve_chunked(vec![b"not-json\n".to_vec()]).await;
	let provider = make_provider(&base, "m");
	let chunks = drain(
		provider
			.stream(&[Message::user("hi")], &[], &ToolChoice::Auto)
			.await
			.expect("stream"),
	)
	.await
	.expect("stream ends with an error event");
	let terminal = chunks.last().expect("terminal");
	assert!(terminal.is_done);
	assert_eq!(terminal.stop_reason, Some(StopReason::Error));

	let base = serve_chunked(vec![
		b"{\"message\":{\"content\":\"x\"},\"done\":false}\n".to_vec(),
		b"{\"message\":".to_vec(),
	])
	.await;
	let provider = make_provider(&base, "m");
	let chunks = drain(
		provider
			.stream(&[Message::user("hi")], &[], &ToolChoice::Auto)
			.await
			.expect("stream"),
	)
	.await
	.expect("stream ends with an error event");
	assert_eq!(chunks.len(), 2, "partial output precedes the error event");
	assert_eq!(chunks[0].content.as_deref(), Some("x"));
	let terminal = chunks.last().expect("terminal");
	assert!(terminal.is_done);
	assert_eq!(terminal.stop_reason, Some(StopReason::Error));
}

#[tokio::test]
async fn load_done_reason_and_empty_length_emit_partial_terminal_errors() {
	const LOAD: &str = "Ollama loaded the model but generated nothing (done_reason: load): the request contained no user-role message.";
	const EMPTY_LENGTH: &str = "Model returned no content: prompt filled the context window; raise Ollama num_ctx or shorten the prompt.";

	let base = serve_chunked(vec![
		b"{\"message\":{\"content\":\"partial\"},\"done\":false}\n".to_vec(),
		b"{\"done\":true,\"done_reason\":\"load\",\"eval_count\":0}\n".to_vec(),
	])
	.await;
	let provider = make_provider(&base, "m");
	let chunks = drain(
		provider
			.stream(&[Message::user("hi")], &[], &ToolChoice::Auto)
			.await
			.expect("stream"),
	)
	.await
	.expect("load is a terminal error event, not a transport failure");
	assert_eq!(chunks.len(), 2, "the partial content event survives");
	assert_eq!(chunks[0].content.as_deref(), Some("partial"));
	let terminal = chunks.last().expect("terminal");
	assert!(terminal.is_done);
	assert_eq!(terminal.stop_reason, Some(StopReason::Error));
	assert_eq!(terminal.error_message.as_deref(), Some(LOAD));
	assert_eq!(terminal.usage.expect("usage").total_tokens, 0);

	let mut assembler = pi_agent::StreamAssembler::new();
	for chunk in &chunks {
		assembler.push(chunk).expect("indexed chunk");
	}
	let message = assembler.finish().expect("assembled");
	assert_eq!(message.content, "partial");
	assert_eq!(message.stop_reason, Some(StopReason::Error));
	assert_eq!(message.error_message.as_deref(), Some(LOAD));

	let base = serve_chunked(vec![
		b"{\"done\":true,\"done_reason\":\"length\",\"eval_count\":0}\n".to_vec(),
	])
	.await;
	let provider = make_provider(&base, "m");
	let chunks = drain(
		provider
			.stream(&[Message::user("hi")], &[], &ToolChoice::Auto)
			.await
			.expect("stream"),
	)
	.await
	.expect("empty length is a terminal error event");
	let terminal = chunks.last().expect("terminal");
	assert!(terminal.is_done);
	assert_eq!(terminal.stop_reason, Some(StopReason::Error));
	assert_eq!(terminal.error_message.as_deref(), Some(EMPTY_LENGTH));
}

#[tokio::test]
async fn complete_returns_the_partial_message_with_a_terminal_error() {
	let body = concat!(
		"{\"message\":{\"content\":\"partial\"},\"done\":false}\n",
		"{\"done\":true,\"done_reason\":\"load\",\"eval_count\":0}\n",
	);
	let (base, _request_rx) = serve_once(response("200 OK", "application/x-ndjson", body)).await;
	let provider = make_provider(&base, "m");
	let message = provider
		.complete(&[Message::user("hi")], &[], &ToolChoice::Auto)
		.await
		.expect("complete returns the partial message, like TS");
	assert_eq!(message.content, "partial");
	assert_eq!(message.stop_reason, Some(StopReason::Error));
	assert_eq!(
		message.error_message.as_deref(),
		Some(
			"Ollama loaded the model but generated nothing (done_reason: load): the request contained no user-role message."
		)
	);
	assert_eq!(message.api.as_deref(), Some("ollama-chat"));
	assert_eq!(message.provider.as_deref(), Some("ollama"));
	assert_eq!(message.model.as_deref(), Some("m"));
}

#[tokio::test]
async fn none_tool_choice_sends_the_wire_mode_and_keeps_offered_tools() {
	let (base, request_rx) = serve_once(response(
		"200 OK",
		"application/x-ndjson",
		"{\"message\":{\"content\":\"ok\"},\"done\":true}\n",
	))
	.await;
	let provider = make_provider(&base, "m");
	provider
		.complete(
			&[Message::user("hi")],
			&[tool("read", "Read")],
			&ToolChoice::None,
		)
		.await
		.expect("complete");
	let (_, body) = request_rx.await.expect("request");
	assert_eq!(body["tool_choice"], "none");
	assert_eq!(body["tools"].as_array().expect("tools").len(), 1);
}

#[tokio::test]
async fn developer_attribution_selects_system_or_user_role() {
	let (base, request_rx) = serve_once(response(
		"200 OK",
		"application/x-ndjson",
		"{\"message\":{\"content\":\"ok\"},\"done\":true}\n",
	))
	.await;
	let mut user_turn = Message::developer("captured user note");
	user_turn.attribution = Some(Attribution::User);
	let mut agent_turn = Message::developer("agent reminder");
	agent_turn.attribution = Some(Attribution::Agent);
	let plain_developer = Message::developer("no attribution");
	let messages = vec![
		Message::system("prefix"),
		user_turn,
		agent_turn,
		plain_developer,
		Message::user("hello"),
	];
	make_provider(&base, "m")
		.complete(&messages, &[], &ToolChoice::Auto)
		.await
		.expect("complete");
	let (_, body) = request_rx.await.expect("request");
	let roles: Vec<&str> = body["messages"]
		.as_array()
		.expect("messages")
		.iter()
		.map(|message| message["role"].as_str().expect("role"))
		.collect();
	assert_eq!(roles, vec!["system", "user", "system", "system", "user"]);
}

#[tokio::test]
async fn completed_message_carries_the_true_source_metadata() {
	let (base, _request_rx) = serve_once(response(
		"200 OK",
		"application/x-ndjson",
		"{\"message\":{\"content\":\"ok\"},\"done\":true}\n",
	))
	.await;
	let provider = make_provider(&base, "llama3.2");
	let message = provider
		.complete(&[Message::user("hi")], &[], &ToolChoice::Auto)
		.await
		.expect("complete");
	assert_eq!(message.api.as_deref(), Some("ollama-chat"));
	assert_eq!(message.provider.as_deref(), Some("ollama"));
	assert_eq!(message.model.as_deref(), Some("llama3.2"));
	assert_eq!(provider.api(), Some("ollama-chat"));
	assert_eq!(provider.provider(), Some("ollama"));
	assert_eq!(provider.model(), Some("llama3.2"));

	let cloud = make_provider(&base, "llama3.2").with_cloud(true);
	assert_eq!(cloud.provider(), Some("ollama-cloud"));
}

#[tokio::test]
async fn explicit_stop_with_tool_calls_promotes_to_tool_use() {
	let without_reason = drain_body(
		"{\"message\":{\"tool_calls\":[{\"function\":{\"name\":\"read\",\"arguments\":{\"path\":\"a\"}}}]},\"done\":false}\n{\"done\":true}\n",
	)
	.await;
	assert_eq!(
		without_reason.last().expect("terminal").stop_reason,
		Some(StopReason::ToolUse)
	);

	let explicit_stop = drain_body(
		"{\"message\":{\"tool_calls\":[{\"function\":{\"name\":\"read\",\"arguments\":{}}}]},\"done\":false}\n{\"done\":true,\"done_reason\":\"stop\"}\n",
	)
	.await;
	assert_eq!(
		explicit_stop.last().expect("terminal").stop_reason,
		Some(StopReason::ToolUse)
	);

	let length_with_calls = drain_body(
		"{\"message\":{\"tool_calls\":[{\"function\":{\"name\":\"read\",\"arguments\":{}}}]},\"done\":false}\n{\"done\":true,\"done_reason\":\"length\"}\n",
	)
	.await;
	assert_eq!(
		length_with_calls.last().expect("terminal").stop_reason,
		Some(StopReason::Length)
	);
}

#[tokio::test]
async fn http_error_body_is_bounded_and_redacts_the_effective_bearer_token() {
	let key = "sk-ollama-secret-token";
	let mut oversized = format!("gateway echoed {key} ");
	// The read ceiling lands inside this run; the trailing credential must not
	// survive as a prefix of the cut token.
	oversized.push_str(&"a".repeat(70 * 1024));
	oversized.push_str(key);
	// Non-retryable: retry behaviour is covered by its own fixture; this test
	// isolates bounded body reading and credential redaction.
	let (base, request_rx) = serve_once(response("400 Bad Request", "text/plain", &oversized)).await;
	let provider = OllamaProvider::new(base.as_str(), key, "m").expect("provider");
	let err = provider
		.stream(&[Message::user("hi")], &[], &ToolChoice::Auto)
		.await
		.err()
		.expect("HTTP failure");
	let (_, body) = request_rx.await.expect("request");
	assert_eq!(body["model"], "m");
	match &err {
		ProviderError::HttpStatus(status, body) => {
			assert_eq!(*status, 400);
			assert!(body.len() < 70 * 1024, "error body must be bounded: {}", body.len());
			assert!(body.contains("[redacted]"), "{body}");
			assert!(body.contains("[truncated]"), "{body}");
			assert!(!body.contains(key), "{body}");
			assert!(!body.contains("aaaaaaaaaa"), "cut token prefix leaked: {body}");
		},
		other => panic!("expected HttpStatus, got {other:?}"),
	}
	assert!(!err.to_string().contains(key));
}

#[tokio::test]
async fn malformed_record_diagnostics_redact_the_bearer_token() {
	let key = "sk-ollama-secret-token";
	let record = format!("not-json echoed {key}\n");
	let base = serve_chunked(vec![record.into_bytes()]).await;
	let provider = OllamaProvider::new(base.as_str(), key, "m").expect("provider");
	let chunks = drain(
		provider
			.stream(&[Message::user("hi")], &[], &ToolChoice::Auto)
			.await
			.expect("stream"),
	)
	.await
	.expect("chunks");
	let terminal = chunks.last().expect("terminal");
	assert_eq!(terminal.stop_reason, Some(StopReason::Error));
	let message = terminal.error_message.clone().expect("error message");
	assert!(message.contains("[redacted]"), "{message}");
	assert!(!message.contains(key), "{message}");
}

#[tokio::test]
async fn oversized_ndjson_record_fails_instead_of_buffering() {
	let huge = vec![b'a'; 8 * 1024 * 1024 + 16];
	let base = serve_chunked(vec![huge]).await;
	let provider = make_provider(&base, "m");
	let chunks = drain(
		provider
			.stream(&[Message::user("hi")], &[], &ToolChoice::Auto)
			.await
			.expect("stream"),
	)
	.await
	.expect("chunks");
	let terminal = chunks.last().expect("terminal");
	assert_eq!(terminal.stop_reason, Some(StopReason::Error));
	assert!(
		terminal
			.error_message
			.as_deref()
			.unwrap_or_default()
			.contains("exceeded"),
		"{terminal:?}"
	);
}

#[tokio::test]
async fn large_but_legitimate_record_is_not_truncated() {
	let text = "z".repeat(1024 * 1024);
	let records = format!(
		"{{\"message\":{{\"content\":\"{text}\"}},\"done\":false}}\n{{\"done\":true}}\n"
	);
	let chunks = drain_body(&records).await;
	let streamed: String = chunks
		.iter()
		.filter_map(|chunk| chunk.content.as_deref())
		.collect();
	assert_eq!(streamed.len(), text.len(), "large record must not be truncated");
	assert_eq!(streamed, text);
}

#[tokio::test]
async fn dropping_a_live_stream_closes_the_transport() {
	let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
	let addr = listener.local_addr().expect("addr");
	let (closed_tx, closed_rx) = tokio::sync::oneshot::channel();
	tokio::spawn(async move {
		let (mut socket, _) = listener.accept().await.expect("accept");
		let _ = read_request(&mut socket).await;
		let headers = "HTTP/1.1 200 OK\r\nContent-Type: application/x-ndjson\r\nTransfer-Encoding: chunked\r\n\r\n";
		let _ = socket.write_all(headers.as_bytes()).await;
		let record = b"{\"message\":{\"content\":\"first\"},\"done\":false}\n";
		let mut framed = format!("{:x}\r\n", record.len()).into_bytes();
		framed.extend_from_slice(record);
		framed.extend_from_slice(b"\r\n");
		let _ = socket.write_all(&framed).await;
		let _ = socket.flush().await;
		let mut tmp = [0u8; 64];
		let _ = socket.read(&mut tmp).await;
		let _ = closed_tx.send(());
	});

	let provider = make_provider(&format!("http://{addr}"), "m");
	let mut stream = provider
		.stream(&[Message::user("hi")], &[], &ToolChoice::Auto)
		.await
		.expect("stream");
	let first = stream.next().await.expect("chunk").expect("ok");
	assert_eq!(first.content.as_deref(), Some("first"));
	drop(stream);

	tokio::time::timeout(Duration::from_secs(2), closed_rx)
		.await
		.expect("server must observe the closed stream")
		.expect("closed signal");
}

#[tokio::test]
async fn completed_record_cap_rejects_a_newline_bearing_chunk() {
	let mut record = vec![b'a'; 8 * 1024 * 1024 + 16];
	record.push(b'\n');
	let base = serve_chunked(vec![record]).await;
	let provider = make_provider(&base, "m");
	let chunks = drain(
		provider
			.stream(&[Message::user("hi")], &[], &ToolChoice::Auto)
			.await
			.expect("stream"),
	)
	.await
	.expect("chunks");
	let terminal = chunks.last().expect("terminal");
	assert_eq!(terminal.stop_reason, Some(StopReason::Error));
	assert!(
		terminal
			.error_message
			.as_deref()
			.unwrap_or_default()
			.contains("exceeded"),
		"{terminal:?}"
	);
}

#[tokio::test]
async fn custom_provider_id_is_stamped_and_default_is_unchanged() {
	let records = "{\"message\":{\"content\":\"hi\"},\"done\":false}\n{\"done\":true}\n";
	let base = serve_chunked(vec![records.as_bytes().to_vec()]).await;
	let provider = make_provider(&base, "m").with_provider_id(Some("custom-gateway".to_string()));
	let message = provider
		.complete(&[Message::user("go")], &[], &ToolChoice::Auto)
		.await
		.expect("complete");
	assert_eq!(message.provider.as_deref(), Some("custom-gateway"));
	assert_eq!(message.api.as_deref(), Some("ollama-chat"));
	assert_eq!(message.model.as_deref(), Some("m"));

	let local = OllamaProvider::new("http://127.0.0.1:9", "", "m").expect("provider");
	assert_eq!(local.provider(), Some("ollama"));
	assert_eq!(
		local.with_provider_id(Some("   ".to_string())).provider(),
		Some("ollama"),
		"an empty override keeps the dialect default"
	);
	let cloud = OllamaProvider::new("http://127.0.0.1:9", "", "m").expect("provider");
	assert_eq!(cloud.with_cloud(true).provider(), Some("ollama-cloud"));
	let custom = OllamaProvider::new("http://127.0.0.1:9", "", "m").expect("provider");
	assert_eq!(
		custom
			.with_cloud(true)
			.with_provider_id(Some("gateway-7".to_string()))
			.provider(),
		Some("gateway-7")
	);
}

#[test]
fn test_thinking_scanner_tag_handling_and_streaming_deltas() {
	let mut scanner = ThinkingInbandScanner::default();
	// Feed text with <think> tag split across chunks
	let events1 = scanner.feed("Hello <th").expect("feed");
	assert_eq!(events1, vec![InbandScanEvent::Text("Hello ".to_string())]);

	let events2 = scanner.feed("ink>my thought</think> World").expect("feed");
	assert_eq!(
		events2,
		vec![
			InbandScanEvent::ThinkingStart,
			InbandScanEvent::ThinkingDelta("my thought".to_string()),
			InbandScanEvent::ThinkingEnd,
			InbandScanEvent::Text(" World".to_string()),
		]
	);

	// Test Gemini fenced thinking
	let mut fenced_scanner = ThinkingInbandScanner::default();
	let fevents = fenced_scanner
		.feed("```thinking\ninternal reasoning\n```final answer")
		.expect("feed");
	assert_eq!(
		fevents,
		vec![
			InbandScanEvent::ThinkingStart,
			InbandScanEvent::ThinkingDelta("internal reasoning\n".to_string()),
			InbandScanEvent::ThinkingEnd,
			InbandScanEvent::Text("final answer".to_string()),
		]
	);

	// Test Gemma reasoning channel
	let mut gemma_scanner = ThinkingInbandScanner::default();
	let gevents = gemma_scanner
		.feed("<|channel>thought\ngemma step<channel|>result")
		.expect("feed");
	assert_eq!(
		gevents,
		vec![
			InbandScanEvent::ThinkingStart,
			InbandScanEvent::ThinkingDelta("gemma step".to_string()),
			InbandScanEvent::ThinkingEnd,
			InbandScanEvent::Text("result".to_string()),
		]
	);
}

#[test]
fn test_thinking_scanner_code_fence_guard() {
	let mut scanner = ThinkingInbandScanner::default();
	// Inside a fenced code block, <think> is literal text and must NOT be parsed as thinking
	let code = "```python\n<think>not thinking</think>\n```\noutside";
	let events = scanner.feed(code).expect("feed");
	let flush_events = scanner.flush().expect("flush");
	let mut all_text = String::new();
	for ev in events.into_iter().chain(flush_events.into_iter()) {
		match ev {
			InbandScanEvent::Text(t) => all_text.push_str(&t),
			InbandScanEvent::ThinkingDelta(_) | InbandScanEvent::ThinkingStart => {
				panic!("Thinking detected inside code block: {all_text}");
			}
			_ => {}
		}
	}
	assert!(all_text.contains("<think>not thinking</think>"));

	// Inline code span
	let mut inline_scanner = ThinkingInbandScanner::default();
	let inline_events = inline_scanner.feed("Here is `<think>` inline").expect("feed");
	let mut inline_text = String::new();
	for ev in inline_events {
		if let InbandScanEvent::Text(t) = ev {
			inline_text.push_str(&t);
		}
	}
	assert!(inline_text.contains("`<think>`"));
}

#[test]
fn test_kimi_scanner_tool_calls_and_name_normalization() {
	assert_eq!(
		normalize_kimi_function_name("functions.web_search:0"),
		"web_search"
	);
	assert_eq!(
		normalize_kimi_function_name("my_module.nested.read_file"),
		"read_file"
	);

	let mut scanner = KimiInbandScanner::default();
	let payload = "<|tool_calls_section_begin|><|tool_call_begin|>functions.web_search:0<|tool_call_argument_begin|>{\"q\":\"rust\"}<|tool_call_end|><|tool_calls_section_end|>visible text";
	let events = scanner.feed(payload).expect("feed");
	assert_eq!(events.len(), 2);
	match &events[0] {
		InbandScanEvent::ToolCall { name, arguments, .. } => {
			assert_eq!(name, "web_search");
			assert_eq!(arguments, "{\"q\":\"rust\"}");
		}
		other => panic!("Expected ToolCall, got {other:?}"),
	}
	assert_eq!(events[1], InbandScanEvent::Text("visible text".to_string()));
}

#[test]
fn test_dsml_scanner_fullwidth_and_ascii() {
	let mut fullwidth_scanner = DeepSeekInbandScanner::default();
	let fw = format!(
		"<{p}DSML{p}tool_calls><{p}DSML{p}invoke name=\"bash\"><{p}DSML{p}parameter name=\"cmd\" string=\"true\">ls -la</{p}DSML{p}parameter></{p}DSML{p}invoke></{p}DSML{p}tool_calls>",
		p = "\u{FF5C}"
	);
	let events = fullwidth_scanner.feed(&fw).expect("feed");
	assert_eq!(events.len(), 1);
	match &events[0] {
		InbandScanEvent::ToolCall { name, arguments, .. } => {
			assert_eq!(name, "bash");
			let parsed: Value = serde_json::from_str(arguments).expect("json");
			assert_eq!(parsed["cmd"], "ls -la");
		}
		other => panic!("Expected ToolCall, got {other:?}"),
	}

	let mut ascii_scanner = DeepSeekInbandScanner::default();
	let ascii = "<|DSML|tool_calls><|DSML|invoke name=\"calc\"><|DSML|parameter name=\"expr\" string=\"false\">{\"val\": 42}</|DSML|parameter></|DSML|invoke></|DSML|tool_calls>";
	let a_events = ascii_scanner.feed(ascii).expect("feed");
	assert_eq!(a_events.len(), 1);
	match &a_events[0] {
		InbandScanEvent::ToolCall { name, arguments, .. } => {
			assert_eq!(name, "calc");
			let parsed: Value = serde_json::from_str(arguments).expect("json");
			assert_eq!(parsed["expr"]["val"], 42);
		}
		other => panic!("Expected ToolCall, got {other:?}"),
	}
}

#[test]
fn test_markup_healer_pattern_and_native_tool_call_protection() {
	assert_eq!(
		get_stream_markup_healing_pattern("moonshot", "kimi-k2"),
		StreamMarkupHealingPattern::Kimi
	);
	assert_eq!(
		get_stream_markup_healing_pattern("ollama", "deepseek-r1:7b"),
		StreamMarkupHealingPattern::Dsml
	);
	assert_eq!(
		get_stream_markup_healing_pattern("ollama", "llama3.3"),
		StreamMarkupHealingPattern::Thinking
	);

	// Verify feed_events_without_calls suppresses healed tool calls when native calls exist
	let mut healer = MarkupHealer::new(StreamMarkupHealingPattern::Kimi);
	let text_with_markup = "<|tool_calls_section_begin|><|tool_call_begin|>tool1<|tool_call_argument_begin|>{}<|tool_call_end|><|tool_calls_section_end|>extra text";
	let events = healer.feed_events_without_calls(text_with_markup).expect("feed");
	// ToolCall is suppressed, only clean text is emitted
	assert_eq!(events, vec![MarkupHealingEvent::Text("extra text".to_string())]);
}

#[test]
fn test_history_sanitize_malformed_tool_calls_and_pairing() {
	let bad_call = ToolCall {
		id: "  ".to_string(),
		name: "bash".to_string(),
		arguments: json!({}),
	};
	assert!(is_malformed_tool_call(&bad_call));

	let valid_call = ToolCall {
		id: "call_1".to_string(),
		name: "bash".to_string(),
		arguments: json!({}),
	};
	assert!(!is_malformed_tool_call(&valid_call));

	// The malformed call lives in both projections; its result must follow it out.
	let mut assistant = Message::assistant_with_tools("", vec![bad_call]);
	assistant.parts = vec![ContentPart::ToolCall {
		id: "  ".to_string(),
		name: "bash".to_string(),
		arguments: json!({}),
	}];
	let messages = vec![
		assistant,
		Message::tool_with_status("  ", "bash", "result", false),
		Message::user("follow up"),
	];
	let sanitized = sanitize_malformed_tool_calls(&messages);
	assert_eq!(sanitized.len(), 1);
	assert_eq!(sanitized[0].role, Role::User);
}

#[test]
fn test_history_thinking_demotion_target_dialects() {
	assert_eq!(preferred_dialect("claude-3-5-sonnet"), "anthropic");
	assert_eq!(preferred_dialect("gemini-2.0-flash"), "gemini");
	assert_eq!(preferred_dialect("deepseek-r1"), "deepseek");
	assert_eq!(preferred_dialect("qwen-2.5-coder"), "qwen3");

	assert_eq!(demote_thinking("claude-3-5-sonnet", "step 1"), "step 1");
	assert_eq!(
		demote_thinking("deepseek-r1", "step 1"),
		"<think>\nstep 1\n</think>"
	);
	assert_eq!(
		demote_thinking("gemini-2.0-flash", "step 1"),
		"```thinking\nstep 1\n```"
	);
	assert_eq!(
		demote_thinking("gemma-2-9b", "step 1"),
		"\x3cthink\x3e\nstep 1\n\x3c/think\x3e"
	);
	assert_eq!(
		demote_thinking("hermes-3", "step 1"),
		"\x3cthink\x3e\nstep 1\n\x3c/think\x3e"
	);
	assert_eq!(
		demote_thinking("hermes-3", "\x3cthink\x3e\nstep 1\n\x3c/think\x3e"),
		"\x3cthink\x3e\nstep 1\n\x3c/think\x3e"
	);
	assert_eq!(
		demote_thinking("qwen-2.5-coder", "\x3cthink\x3e\nstep 1\n\x3c/think\x3e"),
		"\x3cthink\x3e\n\x3cthink\x3e\nstep 1\n\x3c/think\x3e\n\x3c/think\x3e"
	);
	assert_eq!(
		demote_thinking(
			"hermes-3",
			"\x3cthink\x3e\n\x3cthink\x3e\nnested\n\x3c/think\x3e\n\x3c/think\x3e"
		),
		"\x3cthink\x3e\nnested\n\x3c/think\x3e"
	);
	assert_eq!(
		demote_thinking(
			"hermes-3",
			"\x3cthink\x3ea\x3c/think\x3e\x3cthink\x3eb\x3c/think\x3e"
		),
		"\x3cthink\x3e\na\nb\n\x3c/think\x3e"
	);
	assert_eq!(
		demote_thinking("hermes-3", "\x3cthink\x3epartial"),
		"\x3cthink\x3e\n\x3cthink\x3epartial\n\x3c/think\x3e"
	);
	// Inner mixed body returns its own slice unchanged, not the outer input.
	assert_eq!(
		demote_thinking(
			"hermes-3",
			"\x3cthink\x3e\x3cthink\x3ea\x3c/think\x3eplain\x3c/think\x3e"
		),
		"\x3cthink\x3e\n\x3cthink\x3ea\x3c/think\x3eplain\n\x3c/think\x3e"
	);
	assert_eq!(
		demote_thinking("random-model", "step 1"),
		"\x3cthinking\x3e\nstep 1\n\x3c/thinking\x3e"
	);
}

#[test]
fn test_retry_backoff_and_header_parsing() {
	let mut headers = reqwest::header::HeaderMap::new();
	headers.insert("retry-after", "7".parse().unwrap());
	let hint = extract_retry_hint(&headers, "");
	assert_eq!(hint, Some(Duration::from_secs(7)));

	let mut headers_ms = reqwest::header::HeaderMap::new();
	headers_ms.insert("retry-after-ms", "3500".parse().unwrap());
	let hint_ms = extract_retry_hint(&headers_ms, "");
	assert_eq!(hint_ms, Some(Duration::from_millis(3500)));

	let empty_headers = reqwest::header::HeaderMap::new();
	// TS `PLEASE_RETRY_PATTERN` requires `<digits><ms|s>` with no word between.
	let body_hint = extract_retry_hint(&empty_headers, "Please retry in 4s for rate limit");
	assert_eq!(body_hint, Some(Duration::from_secs(4)));

	let mut date_headers = reqwest::header::HeaderMap::new();
	date_headers.insert(
		"retry-after",
		"Sun, 06 Nov 2094 08:49:37 GMT".parse().unwrap(),
	);
	let date_hint = extract_retry_hint(&date_headers, "").expect("HTTP-date hint");
	assert!(
		date_hint >= Duration::from_secs(60 * 60 * 24 * 365 * 60),
		"future HTTP date must produce a long hint: {date_hint:?}"
	);

	let mut rfc850_headers = reqwest::header::HeaderMap::new();
	rfc850_headers.insert(
		"retry-after",
		"Sunday, 06-Nov-49 08:49:37 GMT".parse().unwrap(),
	);
	let rfc850_hint = extract_retry_hint(&rfc850_headers, "").expect("RFC850 hint");
	assert!(
		rfc850_hint >= Duration::from_secs(60 * 60 * 24 * 365 * 15),
		"RFC850 2049 date must be far future: {rfc850_hint:?}"
	);

	let mut asctime_headers = reqwest::header::HeaderMap::new();
	asctime_headers.insert(
		"retry-after",
		"Sun Nov  6 08:49:37 2094".parse().unwrap(),
	);
	let asctime_hint = extract_retry_hint(&asctime_headers, "").expect("asctime hint");
	assert!(
		asctime_hint >= Duration::from_secs(60 * 60 * 24 * 365 * 60),
		"asctime future date must produce a long hint: {asctime_hint:?}"
	);

	let mut invalid_headers = reqwest::header::HeaderMap::new();
	invalid_headers.insert(
		"retry-after",
		"Sun, 32 Nov 2094 08:49:37 GMT".parse().unwrap(),
	);
	assert_eq!(extract_retry_hint(&invalid_headers, ""), None);

	for malformed in [
		"Sun, 06 Nov 999999999999 08:49:37 GMT",
		"Sun, 06 Nov 2147483647 08:49:37 GMT",
		"Sunday, 06-Nov-2147483647 08:49:37 GMT",
		"Sunday, 06-Nov--1 08:49:37 GMT",
		"Sun Nov  6 08:49:37 999999999999",
		"Sun, 06 Nov 9999 24:00:00 GMT",
	] {
		let mut headers = reqwest::header::HeaderMap::new();
		headers.insert("retry-after", malformed.parse().unwrap());
		assert_eq!(extract_retry_hint(&headers, ""), None, "{malformed}");
	}

	// Test parse error exclusion
	assert!(is_llama_cpp_parse_error("Failed to parse tool call arguments as JSON: syntax error"));
	assert!(is_llama_cpp_parse_error("Exception [json.exception.parse_error.101] parse error"));
	assert!(!is_llama_cpp_parse_error("Internal server error"));

	assert!(should_retry_ollama_response(500, "Server crash"));
	assert!(!should_retry_ollama_response(500, "failed to parse tool call arguments as json"));
	assert!(should_retry_ollama_response(429, "Too many requests"));
	assert!(should_retry_ollama_response(408, "Request timeout"));
	assert!(!should_retry_ollama_response(400, "Bad request"));

	// A hint above the retry ceiling is surfaced, so the caller fails fast
	// instead of sleeping a clamped budget.
	let mut slow_headers = reqwest::header::HeaderMap::new();
	slow_headers.insert("retry-after", "120".parse().unwrap());
	let slow_hint = extract_retry_hint(&slow_headers, "").expect("hint");
	assert!(slow_hint > Duration::from_secs(60));
}

#[tokio::test]
async fn test_empty_completion_retry_retries_and_succeeds() {
	let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
	let addr = listener.local_addr().expect("addr");

	tokio::spawn(async move {
		// Attempt 1: empty completion (done: true, eval_count: 0, done_reason: stop)
		let (mut socket1, _) = listener.accept().await.expect("accept 1");
		let _ = read_request(&mut socket1).await;
		let headers = "HTTP/1.1 200 OK\r\nContent-Type: application/x-ndjson\r\nConnection: close\r\n\r\n";
		let empty_chunk = "{\"done\":true,\"done_reason\":\"stop\",\"eval_count\":0}\n";
		let _ = socket1.write_all(headers.as_bytes()).await;
		let _ = socket1.write_all(empty_chunk.as_bytes()).await;
		let _ = socket1.flush().await;
		// EOF terminates attempt 1 (TS waits for the final NDJSON `done` then
		// the server close), so the retry accept must not race an open socket.
		drop(socket1);

		// Attempt 2: successful completion
		let (mut socket2, _) = listener.accept().await.expect("accept 2");
		let _ = read_request(&mut socket2).await;
		let chunk2 = "{\"message\":{\"content\":\"retry success\"},\"done\":false}\n{\"done\":true,\"done_reason\":\"stop\",\"eval_count\":2}\n";
		let _ = socket2.write_all(headers.as_bytes()).await;
		let _ = socket2.write_all(chunk2.as_bytes()).await;
		let _ = socket2.flush().await;
	});

	let provider = make_provider(&format!("http://{addr}"), "test-model");
	let message = provider
		.complete(&[Message::user("ping")], &[], &ToolChoice::Auto)
		.await
		.expect("complete after retry");
	assert_eq!(message.content, "retry success");
}

#[tokio::test]
async fn test_on_payload_hook_modifies_request_payload() {
	let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
	let addr = listener.local_addr().expect("addr");
	let (req_tx, req_rx) = tokio::sync::oneshot::channel();

	tokio::spawn(async move {
		let (mut socket, _) = listener.accept().await.expect("accept");
		if let Some((_, json_body)) = read_request(&mut socket).await {
			let _ = req_tx.send(json_body);
		}
		let headers = "HTTP/1.1 200 OK\r\nContent-Type: application/x-ndjson\r\nConnection: close\r\n\r\n";
		let chunk = "{\"message\":{\"content\":\"hook ok\"},\"done\":false}\n{\"done\":true,\"done_reason\":\"stop\",\"eval_count\":3}\n";
		let _ = socket.write_all(headers.as_bytes()).await;
		let _ = socket.write_all(chunk.as_bytes()).await;
		let _ = socket.flush().await;
	});

	let expected_base = format!("http://{addr}");
	let hook: OllamaPayloadHook = std::sync::Arc::new(
		move |payload: &Value, model: &OllamaPayloadModel| {
			assert_eq!(model.id, "test-model");
			assert_eq!(model.name, "test-model");
			assert_eq!(model.api, "ollama-chat");
			assert_eq!(model.provider, "ollama");
			assert_eq!(model.base_url, expected_base.as_str());
			assert_eq!(model.input, vec!["text".to_string()]);
			assert!(model.cost.is_none());
			assert!(model.thinking.is_none());
			let mut modified = payload.clone();
			if let Some(map) = modified.as_object_mut() {
				map.insert("custom_injected_option".to_string(), json!(true));
			}
			Box::pin(async move { Ok(Some(modified)) })
		},
	);

	let provider = make_provider(&format!("http://{addr}"), "test-model").with_on_payload(hook);
	let _ = provider
		.complete(&[Message::user("hi")], &[], &ToolChoice::Auto)
		.await
		.expect("complete");

	let received_json = req_rx.await.expect("received request json");
	assert_eq!(received_json["custom_injected_option"], json!(true));
}

#[tokio::test]
async fn payload_model_context_carries_full_catalog_fields_and_pre_auth_headers() {
	let (base, request_rx) = serve_once(response(
		"200 OK",
		"application/x-ndjson",
		"{\"message\":{\"content\":\"ok\"},\"done\":true,\"eval_count\":1}\n",
	))
	.await;
	let context = OllamaPayloadModel {
		provider: "custom-route".to_string(),
		api: "ollama-chat".to_string(),
		id: "catalog-row".to_string(),
		request_model_id: Some("wire-model".to_string()),
		name: "Display Name".to_string(),
		base_url: base.clone(),
		input: vec!["text".to_string(), "image".to_string()],
		tokenizer: Some("qwen".to_string()),
		cost: Some(OllamaPayloadCost {
			input: 1.0,
			output: 2.0,
			cache_read: 0.1,
			cache_write: 0.2,
			long_context: Some(json!({"input": 3.0})),
		}),
		context_window: Some(131_072),
		max_tokens: Some(8_192),
		omit_max_output_tokens: true,
		reasoning: true,
		thinking: Some(OllamaPayloadThinking {
			mode: "effort".to_string(),
			efforts: vec!["low".to_string(), "high".to_string()],
			default_level: Some("low".to_string()),
			effort_map: Some(vec![("low".to_string(), "low".to_string())]),
			supports_display: Some(true),
			effort_routing: None,
			effort_budgets: Some(vec![("high".to_string(), 8192)]),
			suppress_when_off: Some(false),
			requires_effort: None,
		}),
		effort: Some("high".to_string()),
		headers: vec![("X-Model".to_string(), "model".to_string())],
	};
	let hook: OllamaPayloadHook = std::sync::Arc::new(
		move |_payload: &Value, model: &OllamaPayloadModel| {
			assert_eq!(model.provider, "custom-route");
			assert_eq!(model.id, "catalog-row");
			assert_eq!(model.request_model_id.as_deref(), Some("wire-model"));
			assert_eq!(model.name, "Display Name");
			assert_eq!(model.input, vec!["text".to_string(), "image".to_string()]);
			assert_eq!(model.tokenizer.as_deref(), Some("qwen"));
			let cost = model.cost.as_ref().expect("cost");
			assert_eq!((cost.input, cost.output), (1.0, 2.0));
			assert_eq!(cost.long_context.as_ref().unwrap()["input"], 3.0);
			assert_eq!(model.context_window, Some(131_072));
			assert_eq!(model.max_tokens, Some(8_192));
			assert!(model.omit_max_output_tokens);
			assert!(model.reasoning);
			let thinking = model.thinking.as_ref().expect("thinking");
			assert_eq!(thinking.mode, "effort");
			assert_eq!(thinking.efforts, vec!["low", "high"]);
			assert_eq!(thinking.default_level.as_deref(), Some("low"));
			assert_eq!(
				thinking.effort_map.as_deref(),
				Some(&[("low".to_string(), "low".to_string())][..])
			);
			assert_eq!(thinking.supports_display, Some(true));
			assert!(thinking.effort_routing.is_none());
			assert_eq!(
				thinking.effort_budgets.as_deref(),
				Some(&[("high".to_string(), 8_192u64)][..])
			);
			assert_eq!(thinking.suppress_when_off, Some(false));
			assert!(thinking.requires_effort.is_none());
			assert_eq!(model.effort.as_deref(), Some("high"));
			// Hook sees model-level headers only, never caller/config ones.
			assert_eq!(
				model.headers,
				vec![("X-Model".to_string(), "model".to_string())]
			);
			Box::pin(async move { Ok(None) })
		},
	);
	let provider = make_provider(&base, "row")
		.with_payload_model(context)
		.with_request_headers(vec![
			("X-Model".to_string(), "config-wins".to_string()),
			("X-Config".to_string(), "caller".to_string()),
			("Authorization".to_string(), "evil".to_string()),
			("Content-Type".to_string(), "text/plain".to_string()),
		])
		.with_on_payload(hook);
	provider
		.complete(&[Message::user("hi")], &[], &ToolChoice::Auto)
		.await
		.expect("complete");

	let (raw, body) = request_rx.await.expect("request");
	assert_eq!(body["model"], "wire-model");
	assert_eq!(provider.provider(), Some("custom-route"));
	let lower = raw.to_ascii_lowercase();
	assert!(lower.contains("x-model: config-wins"), "{raw}");
	assert!(!lower.contains("x-model: model\r\n"), "{raw}");
	assert!(lower.contains("x-config: caller"), "{raw}");
	assert!(lower.contains("authorization: bearer test-key"), "{raw}");
	assert!(!lower.contains("evil"), "{raw}");
	assert!(lower.contains("content-type: application/json"), "{raw}");
	assert!(!lower.contains("content-type: text/plain"), "{raw}");
}

#[tokio::test]
async fn default_client_rejects_redirects_without_contacting_second_origin() {
	let attacker = TcpListener::bind("127.0.0.1:0").await.expect("attacker bind");
	let attacker_addr = attacker.local_addr().expect("attacker addr");
	let first = TcpListener::bind("127.0.0.1:0").await.expect("first bind");
	let first_addr = first.local_addr().expect("first addr");
	let (head_tx, head_rx) = tokio::sync::oneshot::channel();
	tokio::spawn(async move {
		let (mut socket, _) = first.accept().await.expect("accept");
		if let Some((head, _)) = read_request(&mut socket).await {
			let _ = head_tx.send(head);
		}
		let redirect = format!(
			"HTTP/1.1 302 Found\r\nLocation: http://{attacker_addr}/steal\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
		);
		let _ = socket.write_all(redirect.as_bytes()).await;
	});
	let provider = make_provider(&format!("http://{first_addr}"), "m")
		.with_request_headers(vec![(
			"X-Api-Key".to_string(),
			"gateway-secret".to_string(),
		)]);
	let err = provider
		.stream(&[Message::user("hi")], &[], &ToolChoice::Auto)
		.await
		.err()
		.expect("redirect status");
	let head = head_rx.await.expect("first origin request");
	let lower_head = head.to_ascii_lowercase();
	assert!(lower_head.contains("authorization: bearer"));
	// The request that would have been forwarded carries a custom credential.
	assert!(lower_head.contains("x-api-key: gateway-secret"));
	match err {
		ProviderError::HttpStatus(status, _) => assert_eq!(status, 302),
		other => panic!("Expected HttpStatus(302), got {other:?}"),
	}
	assert!(
		tokio::time::timeout(Duration::from_millis(150), attacker.accept())
			.await
			.is_err(),
		"redirect target must never be contacted"
	);
}

#[tokio::test]
async fn diagnostic_redaction_covers_every_configuration_header_value() {
	let cookie = "session-token-abc";
	let custom = "gateway-token-xyz";
	let body = format!("denied cookie={cookie} custom={custom}");
	let (base, _request_rx) = serve_once(response("400 Bad Request", "text/plain", &body)).await;
	let provider = make_provider(&base, "m").with_request_headers(vec![
		("Cookie".to_string(), cookie.to_string()),
		("X-Gateway-Token".to_string(), custom.to_string()),
	]);
	let err = provider
		.stream(&[Message::user("hi")], &[], &ToolChoice::Auto)
		.await
		.err()
		.expect("HTTP failure");
	let ProviderError::HttpStatus(_, text) = err else {
		panic!("Expected HttpStatus, got {err:?}");
	};
	assert!(text.contains("[redacted]"), "{text}");
	assert!(!text.contains(cookie), "{text}");
	assert!(!text.contains(custom), "{text}");
}

#[test]
fn test_thinking_scanner_unicode_fragments_do_not_panic() {
	// Every emitted offset must land on a char boundary: `é` is 2 bytes, the
	// emoji 4, and fullwidth DSML tags 3 bytes per scalar.
	let mut scanner = ThinkingInbandScanner::default();
	let mut visible = String::new();
	for fragment in ["héllo 😀 <thi", "nk>réason 🧠</thi", "nk> après"] {
		for ev in scanner.feed(fragment).expect("feed") {
			if let InbandScanEvent::Text(text) = ev {
				visible.push_str(&text);
			}
		}
	}
	assert!(visible.contains("héllo 😀 "), "{visible}");

	let mut dsml = DeepSeekInbandScanner::default();
	for fragment in ["<｜DSML｜", "tool_calls>", "<｜DSML｜invoke name=\"x\">", "héllo 😀"] {
		let _ = dsml.feed(fragment).expect("feed");
	}
}

#[test]
fn test_scanner_retains_bounded_when_fence_never_closes() {
	// An unterminated fenced code line is held back for later fence detection;
	// without a cap it would grow with every record.
	let chunk = "a".repeat(64 * 1024);
	let mut fenced = ThinkingInbandScanner::default();
	fenced.feed("```python\n").expect("open fence");
	let mut hold_err = None;
	for _ in 0..(MAX_STREAM_HOLD_BYTES / chunk.len() + 2) {
		if let Err(err) = fenced.feed(&chunk) {
			hold_err = Some(err);
			break;
		}
	}
	assert!(hold_err.is_some(), "unterminated fence must stop retaining");
}

#[test]
fn test_tool_section_aggregate_cap_across_many_partial_records() {
	// Each record is a valid section prefix; only their accumulation is
	// unbounded, so the per-record cap cannot protect this path.
	let chunk = "a".repeat(64 * 1024);

	let mut kimi = KimiInbandScanner::default();
	kimi.feed("<|tool_calls_section_begin|><|tool_call_begin|>fn.x:0<|tool_call_argument_begin|>")
		.expect("open section");
	let mut kimi_err = None;
	let mut fed = 0usize;
	while fed < MAX_SECTION_BYTES + chunk.len() * 2 {
		if let Err(err) = kimi.feed(&chunk) {
			kimi_err = Some(err);
			break;
		}
		fed += chunk.len();
	}
	assert!(kimi_err.is_some(), "unclosed Kimi section must be capped");
	assert!(
		fed <= MAX_SECTION_BYTES + chunk.len(),
		"cap must fire near the limit, fed={fed}"
	);

	let mut dsml = DeepSeekInbandScanner::default();
	dsml.feed("<|DSML|tool_calls><|DSML|invoke name=\"t\">")
		.expect("open section");
	let mut dsml_err = None;
	let mut fed = 0usize;
	while fed < MAX_SECTION_BYTES + chunk.len() * 2 {
		if let Err(err) = dsml.feed(&chunk) {
			dsml_err = Some(err);
			break;
		}
		fed += chunk.len();
	}
	assert!(dsml_err.is_some(), "unclosed DSML section must be capped");
}

#[tokio::test]
async fn healed_text_and_thinking_get_distinct_block_indices() {
	let content = format!("visible <{}>plan</{}>", "think", "think");
	let payload = serde_json::json!({"message": {"content": content}, "done": false});
	let records = format!("{payload}\n{{\"done\":true,\"done_reason\":\"stop\",\"eval_count\":4}}\n");
	let base = serve_chunked(vec![records.as_bytes().to_vec()]).await;
	let provider = make_provider(&base, "llama3.3");
	let message = provider
		.complete(&[Message::user("go")], &[], &ToolChoice::Auto)
		.await
		.expect("complete");
	assert!(message.content.contains("visible "), "{}", message.content);
	assert!(
		message
			.parts
			.iter()
			.any(|part| matches!(part, ContentPart::Thinking { .. })),
		"{:?}",
		message.parts
	);
}

#[tokio::test]
async fn structured_tool_calls_suppress_healed_duplicates_in_same_record() {
	let leaked = "<|tool_calls_section_begin|><|tool_call_begin|>leak.x:0<|tool_call_argument_begin|>{}}<|tool_call_end|><|tool_calls_section_end|>";
	let record = format!(
		"{{\"message\":{{\"content\":\"{leaked}\",\"tool_calls\":[{{\"function\":{{\"name\":\"native_tool\",\"arguments\":{{}}}}}}]}},\"done\":false}}\n{{\"done\":true,\"done_reason\":\"tool_calls\"}}\n"
	);
	let base = serve_chunked(vec![record.into_bytes()]).await;
	let provider = make_provider(&base, "kimi-k2");
	let message = provider
		.complete(&[Message::user("go")], &[], &ToolChoice::Auto)
		.await
		.expect("complete");
	assert_eq!(message.tool_calls.len(), 1, "{:?}", message.tool_calls);
	assert_eq!(message.tool_calls[0].name, "native_tool");
	assert_eq!(message.stop_reason, Some(StopReason::ToolUse));
}

#[test]
fn test_dsml_string_mode_real_grammar() {
	let mut scanner = DeepSeekInbandScanner::default();
	let input = "<|DSML|tool_calls><|DSML|invoke name=\"run\"><|DSML|parameter name=\"cmd\" string=\"true\">ls -la</|DSML|parameter></|DSML|invoke></|DSML|tool_calls>";
	let events = scanner.feed(input).expect("feed");
	match &events[0] {
		InbandScanEvent::ToolCall { name, arguments, .. } => {
			assert_eq!(name, "run");
			let parsed: Value = serde_json::from_str(arguments).expect("json");
			assert_eq!(parsed["cmd"], Value::String("ls -la".to_string()));
		},
		other => panic!("Expected ToolCall, got {other:?}"),
	}

	// Absent flag defaults to string coercion, so JSON-looking bodies stay text.
	let mut default_scanner = DeepSeekInbandScanner::default();
	let default_input = "<|DSML|tool_calls><|DSML|invoke name=\"keep\"><|DSML|parameter name=\"raw\">{\"a\":1}</|DSML|parameter></|DSML|invoke></|DSML|tool_calls>";
	let default_events = default_scanner.feed(default_input).expect("feed");
	match &default_events[0] {
		InbandScanEvent::ToolCall { arguments, .. } => {
			let parsed: Value = serde_json::from_str(arguments).expect("json");
			assert_eq!(parsed["raw"], Value::String("{\"a\":1}".to_string()));
		},
		other => panic!("Expected ToolCall, got {other:?}"),
	}

	// Explicit `string="false"` coerces the body as JSON.
	let mut typed_scanner = DeepSeekInbandScanner::default();
	let typed_input = "<|DSML|tool_calls><|DSML|invoke name=\"calc\"><|DSML|parameter name=\"n\" string=\"false\">42</|DSML|parameter></|DSML|invoke></|DSML|tool_calls>";
	let typed_events = typed_scanner.feed(typed_input).expect("feed");
	match &typed_events[0] {
		InbandScanEvent::ToolCall { arguments, .. } => {
			let parsed: Value = serde_json::from_str(arguments).expect("json");
			assert_eq!(parsed["n"], Value::from(42));
		},
		other => panic!("Expected ToolCall, got {other:?}"),
	}
}
