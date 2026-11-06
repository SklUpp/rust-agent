//! Consumer-boundary fixtures for the Anthropic Messages adapter:
//! request shape/auth/headers, interleaved thinking/text/tool streams, usage,
//! HTTP failures, malformed/truncated streams, cancellation and credential
//! non-leakage.
//!
//! These tests use local loopback servers only; no external endpoint is called.

use std::time::Duration;

use futures::StreamExt;
use pi_agent::state::{CacheTtlUsage, ServerToolUsage, StopReason};
use pi_agent::{
	ContentPart, ImagePart, Message, ModelProvider, ProviderError, StreamAssembler, TokenUsage,
	ToolCall, ToolChoice,
};
use pi_agent::provider_anthropic::{
	AnthropicMessagesProvider, AnthropicThinkingConfig,
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
		header_end = buf.windows(4).position(|window| window == b"\r\n\r\n");
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
	response_with_headers(status, content_type, "", body)
}

fn response_with_headers(
	status: &str,
	content_type: &str,
	extra_headers: &str,
	body: &str,
) -> Vec<u8> {
	format!(
		"HTTP/1.1 {status}\r\nContent-Type: {content_type}\r\n{extra_headers}Content-Length: {}\r\nConnection: close\r\n\r\n{body}",
		body.len()
	)
	.into_bytes()
}

async fn serve_responses(responses: Vec<Vec<u8>>) -> String {
	let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
	let addr = listener.local_addr().expect("addr");
	tokio::spawn(async move {
		for response in responses {
			let Ok((mut socket, _)) = listener.accept().await else {
				return;
			};
			let _ = read_request(&mut socket).await;
			let _ = socket.write_all(&response).await;
			let _ = socket.flush().await;
		}
	});
	format!("http://{addr}")
}

async fn serve_capturing(response: Vec<u8>) -> (String, tokio::sync::oneshot::Receiver<(String, Value)>) {
	let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
	let addr = listener.local_addr().expect("addr");
	let (tx, rx) = tokio::sync::oneshot::channel();
	tokio::spawn(async move {
		let Ok((mut socket, _)) = listener.accept().await else {
			return;
		};
		let Some((headers, body)) = read_request(&mut socket).await else {
			return;
		};
		let _ = socket.write_all(&response).await;
		let _ = socket.flush().await;
		let _ = tx.send((headers, body));
	});
	(format!("http://{addr}"), rx)
}

fn read_tool() -> Value {
	json!({
		"type": "function",
		"function": {
			"name": "read",
			"description": "Read a file",
			"parameters": {
				"type": "object",
				"properties": { "path": { "type": "string" } },
				"required": ["path"]
			}
		}
	})
}

#[test]
fn official_api_key_request_shape_and_betas() {
	let provider = AnthropicMessagesProvider::new(
		"https://api.anthropic.com",
		"sk-api-key",
		"claude-sonnet",
	)
	.expect("provider")
	.with_max_tokens(8192)
	.with_request_max_tokens(Some(1024))
	.with_thinking(Some(AnthropicThinkingConfig::Enabled {
		budget_tokens: 2048,
		display: Some("summarized".to_string()),
	}))
	.with_output_effort(Some("high".to_string()))
	.with_extra_betas(vec!["custom-beta".to_string()]);

	let headers = provider
		.request_headers_for(true, true)
		.expect("headers");
	assert_eq!(headers.get("x-api-key").unwrap().to_str().unwrap(), "sk-api-key");
	assert!(headers.get("authorization").is_none());
	assert_eq!(headers.get("anthropic-version").unwrap().to_str().unwrap(), "2023-06-01");
	assert_eq!(headers.get("accept").unwrap().to_str().unwrap(), "text/event-stream");
	let beta = headers.get("anthropic-beta").unwrap().to_str().unwrap();
	assert!(beta.contains("context-management-2025-06-27"), "{beta}");
	assert!(beta.contains("interleaved-thinking-2025-05-14"), "{beta}");
	assert!(beta.contains("effort-2025-11-24"), "{beta}");
	assert!(beta.contains("custom-beta"), "{beta}");
	assert_eq!(provider.endpoint(), "https://api.anthropic.com/v1/messages");

	let messages = vec![
		Message::system("system prompt"),
		Message::user("hi"),
		Message::assistant_with_tools(
			"answer",
			vec![ToolCall {
				id: "toolu_1".to_string(),
				name: "read".to_string(),
				arguments: json!({ "path": "a.txt" }),
			}],
		),
		Message::tool("toolu_1", "file body"),
	];
	let tools = vec![read_tool()];
	let request = provider
		.build_request(
			&messages,
			&tools,
			&ToolChoice::Tool { name: "read".to_string() },
			true,
		)
		.expect("request");
	let body = serde_json::to_value(&request).expect("serialize");

	assert_eq!(body["model"], "claude-sonnet");
	assert_eq!(body["max_tokens"], 1024);
	assert_eq!(body["stream"], true);
	assert_eq!(
		body["system"],
		json!([{ "type": "text", "text": "system prompt" }])
	);
	assert_eq!(
		body["tool_choice"],
		json!({ "type": "tool", "name": "read" })
	);
	assert_eq!(body["tools"][0]["name"], "read");
	assert_eq!(body["tools"][0]["description"], "Read a file");
	assert_eq!(body["tools"][0]["input_schema"]["type"], "object");
	assert_eq!(body["tools"][0]["input_schema"]["required"], json!(["path"]));
	assert!(body.get("thinking").is_none(), "forced tool choice disables thinking");
	assert!(body.get("context_management").is_none());

	assert_eq!(body["messages"][0]["role"], "user");
	assert_eq!(body["messages"][0]["content"], "hi");
	assert_eq!(body["messages"][1]["role"], "assistant");
	assert_eq!(body["messages"][1]["content"][0]["type"], "text");
	assert_eq!(body["messages"][1]["content"][1]["type"], "tool_use");
	assert_eq!(body["messages"][1]["content"][1]["id"], "toolu_1");
	assert_eq!(body["messages"][2]["role"], "user");
	assert_eq!(body["messages"][2]["content"][0]["type"], "tool_result");
	assert_eq!(body["messages"][2]["content"][0]["tool_use_id"], "toolu_1");
	assert_eq!(body["messages"][2]["content"][0]["content"], "file body");

	// Auto mode keeps the thinking/context-management fields.
	let auto = provider
		.build_request(&[Message::user("hi")], &[], &ToolChoice::Auto, false)
		.expect("auto request");
	let auto_body = serde_json::to_value(&auto).expect("serialize");
	assert_eq!(auto_body["thinking"]["type"], "enabled");
	// Requested 1024 is raised to thinking budget + fallback headroom, still
	// under the 8192 model ceiling.
	assert_eq!(auto_body["max_tokens"], 6048);
	assert_eq!(auto_body["thinking"]["budget_tokens"], 2048);
	assert_eq!(auto_body["thinking"]["display"], "summarized");
	assert_eq!(
		auto_body["context_management"]["edits"][0]["type"],
		"clear_thinking_20251015"
	);
	assert_eq!(auto_body["output_config"]["effort"], "high");
}

#[test]
fn oauth_uses_bearer_beta_query_and_prefixed_tools() {
	let provider = AnthropicMessagesProvider::new(
		"https://api.anthropic.com",
		"sk-ant-oat-token",
		"claude-sonnet",
	)
	.expect("provider")
	.with_thinking(Some(AnthropicThinkingConfig::Adaptive {
		display: Some("summarized".to_string()),
	}));
	assert!(provider.is_oauth());
	let headers = provider
		.request_headers_for(true, true)
		.expect("headers");
	assert_eq!(
		headers.get("authorization").unwrap().to_str().unwrap(),
		"Bearer sk-ant-oat-token"
	);
	assert!(headers.get("x-api-key").is_none());
	assert_eq!(headers.get("accept").unwrap().to_str().unwrap(), "application/json");
	let beta = headers.get("anthropic-beta").unwrap().to_str().unwrap();
	assert!(beta.contains("oauth-2025-04-20"), "{beta}");
	assert!(beta.contains("claude-code-20250219"), "{beta}");
	assert_eq!(
		provider.endpoint(),
		"https://api.anthropic.com/v1/messages?beta=true"
	);

	let request = provider
		.build_request(
			&[Message::user("hi")],
			&[read_tool()],
			&ToolChoice::Tool { name: "read".to_string() },
			true,
		)
		.expect("request");
	let body = serde_json::to_value(&request).expect("serialize");
	assert_eq!(body["tools"][0]["name"], "_read");
	assert_eq!(body["tool_choice"], json!({ "type": "tool", "name": "_read" }));
}

#[test]
fn non_official_api_key_uses_bearer_not_x_api_key() {
	let provider = AnthropicMessagesProvider::new("http://127.0.0.1:9", "sk-local", "model")
		.expect("provider");
	let headers = provider.request_headers(false).expect("headers");
	assert_eq!(
		headers.get("authorization").unwrap().to_str().unwrap(),
		"Bearer sk-local"
	);
	assert!(headers.get("x-api-key").is_none());
	assert_eq!(headers.get("accept").unwrap().to_str().unwrap(), "application/json");
	assert_eq!(provider.endpoint(), "http://127.0.0.1:9/v1/messages");
}

#[tokio::test]
async fn invalid_forced_tool_fails_before_network() {
	let provider = AnthropicMessagesProvider::new("http://127.0.0.1:9", "sk-key", "model")
		.expect("provider");
	let err = provider
		.complete(
			&[Message::user("hi")],
			&[read_tool()],
			&ToolChoice::Tool { name: "write".to_string() },
		)
		.await
		.expect_err("unoffered tool");
	match err {
		ProviderError::InvalidToolChoice(message) => assert!(message.contains("not offered"), "{message}"),
		other => panic!("expected InvalidToolChoice, got {other:?}"),
	}

	let err = provider
		.complete(
			&[Message::user("hi")],
			&[],
			&ToolChoice::Tool { name: "  ".to_string() },
		)
		.await
		.expect_err("empty name");
	assert!(matches!(err, ProviderError::InvalidToolChoice(_)), "{err:?}");
}

#[tokio::test]
async fn complete_maps_interleaved_thinking_text_tool_and_usage() {
	let response_body = json!({
		"id": "msg_1",
		"type": "message",
		"role": "assistant",
		"content": [
			{ "type": "thinking", "thinking": "plan", "signature": "sig" },
			{ "type": "text", "text": "answer" },
			{ "type": "tool_use", "id": "toolu_9", "name": "read", "input": { "path": "a.txt" } }
		],
		"usage": {
			"input_tokens": 11,
			"output_tokens": 5,
			"cache_read_input_tokens": 3,
			"cache_creation_input_tokens": 2
		}
	})
	.to_string();
	let (url, captured) = serve_capturing(response("200 OK", "application/json", &response_body)).await;
	let provider = AnthropicMessagesProvider::new(url, "sk-wire", "claude-sonnet")
		.expect("provider")
		.with_max_tokens(256);

	let message = provider
		.complete(&[Message::user("hi")], &[read_tool()], &ToolChoice::Auto)
		.await
		.expect("completion");
	assert_eq!(message.content, "answer");
	assert_eq!(message.parts.len(), 3);
	assert_eq!(message.parts[0].as_thinking(), Some("plan"));
	assert!(matches!(
		&message.parts[0],
		ContentPart::Thinking { thinking_signature: Some(signature), .. } if signature.as_str() == "sig"
	));
	assert_eq!(message.parts[1], ContentPart::Text { text: "answer".to_string() });
	assert!(matches!(
		&message.parts[2],
		ContentPart::ToolCall { id, name, arguments }
			if id == "toolu_9" && name == "read" && arguments == &json!({ "path": "a.txt" })
	));
	assert_eq!(message.tool_calls.len(), 1);
	assert_eq!(message.tool_calls[0].id, "toolu_9");
	assert_eq!(message.tool_calls[0].name, "read");
	assert_eq!(message.tool_calls[0].arguments, json!({ "path": "a.txt" }));
	assert_eq!(
		message.usage,
		Some(TokenUsage {
			input: 11,
			output: 5,
			cache_read: 3,
			cache_write: 2,
			total_tokens: 21,
			..Default::default()
		})
	);

	let (headers, body) = captured.await.expect("captured request");
	assert!(
		headers
			.to_ascii_lowercase()
			.contains("authorization: bearer sk-wire"),
		"{headers}"
	);
	assert!(!headers.to_ascii_lowercase().contains("x-api-key"), "{headers}");
	assert_eq!(body["stream"], false);
	assert_eq!(body["max_tokens"], 256);
}

#[tokio::test]
async fn stream_maps_interleaved_events_and_usage() {
	let body = concat!(
		"event: message_start\n",
		"data: {\"type\":\"message_start\",\"message\":{\"id\":\"msg_2\",\"usage\":{\"input_tokens\":5,\"output_tokens\":0,\"cache_read_input_tokens\":7,\"cache_creation_input_tokens\":3,\"cache_creation\":{\"ephemeral_5m_input_tokens\":3,\"ephemeral_1h_input_tokens\":0},\"server_tool_use\":{\"web_search_requests\":2,\"web_fetch_requests\":1}}}}\n\n",
		"event: content_block_start\n",
		"data: {\"type\":\"content_block_start\",\"index\":0,\"content_block\":{\"type\":\"thinking\",\"thinking\":\"\"}}\n\n",
		"event: content_block_delta\n",
		"data: {\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"thinking_delta\",\"thinking\":\"think\"}}\n\n",
		"event: content_block_delta\n",
		"data: {\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"signature_delta\",\"signature\":\"sig\"}}\n\n",
		"event: content_block_stop\n",
		"data: {\"type\":\"content_block_stop\",\"index\":0}\n\n",
		"event: content_block_start\n",
		"data: {\"type\":\"content_block_start\",\"index\":1,\"content_block\":{\"type\":\"text\",\"text\":\"\"}}\n\n",
		"event: content_block_delta\n",
		"data: {\"type\":\"content_block_delta\",\"index\":1,\"delta\":{\"type\":\"text_delta\",\"text\":\"answer\"}}\n\n",
		"event: content_block_stop\n",
		"data: {\"type\":\"content_block_stop\",\"index\":1}\n\n",
		"event: content_block_start\n",
		"data: {\"type\":\"content_block_start\",\"index\":2,\"content_block\":{\"type\":\"tool_use\",\"id\":\"toolu_1\",\"name\":\"read\",\"input\":{}}}\n\n",
		"event: content_block_delta\n",
		"data: {\"type\":\"content_block_delta\",\"index\":2,\"delta\":{\"type\":\"input_json_delta\",\"partial_json\":\"{\\\"path\\\":\\\"a\"}}\n\n",
		"event: content_block_delta\n",
		"data: {\"type\":\"content_block_delta\",\"index\":2,\"delta\":{\"type\":\"input_json_delta\",\"partial_json\":\".txt\\\"}\"}}\n\n",
		"event: content_block_stop\n",
		"data: {\"type\":\"content_block_stop\",\"index\":2}\n\n",
		"event: message_delta\n",
		"data: {\"type\":\"message_delta\",\"delta\":{\"stop_reason\":\"tool_use\"},\"usage\":{\"output_tokens\":42}}\n\n",
		"event: message_stop\n",
		"data: {\"type\":\"message_stop\"}\n\n"
	);
	let url = serve_responses(vec![response("200 OK", "text/event-stream", body)]).await;
	let provider = AnthropicMessagesProvider::new(url, "sk-stream", "claude-sonnet")
		.expect("provider")
		.with_max_tokens(256);

	let mut stream = provider
		.stream(&[Message::user("hi")], &[read_tool()], &ToolChoice::Auto)
		.await
		.expect("stream");
	let mut assembler = StreamAssembler::new();
	let mut last_usage = None;
	let mut done = false;
	let mut done_reason = None;
	while let Some(item) = stream.next().await {
		let chunk = item.expect("chunk");
		if let Some(usage) = chunk.usage {
			last_usage = Some(usage);
		}
		if chunk.is_done {
			done = true;
			done_reason = chunk.stop_reason.clone();
		}
		assembler.push(&chunk).expect("assemble chunk");
	}
	assert!(done, "message_stop must terminate the stream");
	assert!(matches!(done_reason, Some(StopReason::ToolUse)));
	assert_eq!(
		last_usage,
		Some(TokenUsage {
			input: 5,
			output: 42,
			cache_read: 7,
			cache_write: 3,
			total_tokens: 57,
			cttl: Some(CacheTtlUsage {
				ephemeral_5m: Some(3),
				ephemeral_1h: None,
			}),
			server: Some(ServerToolUsage {
				web_search: Some(2),
				web_fetch: Some(1),
			}),
			..Default::default()
		})
	);

	let message = assembler.finish().expect("assembled turn");
	assert_eq!(message.content, "answer");
	assert_eq!(message.parts[0].as_thinking(), Some("think"));
	assert!(matches!(
		&message.parts[0],
		ContentPart::Thinking { thinking_signature: Some(signature), .. } if signature.as_str() == "sig"
	), "signature_delta must be attached to its indexed thinking block");
	assert_eq!(message.parts[1], ContentPart::Text { text: "answer".to_string() });
	assert_eq!(message.tool_calls.len(), 1);
	assert_eq!(message.tool_calls[0].name, "read");
	assert_eq!(message.tool_calls[0].arguments, json!({ "path": "a.txt" }));
	assert_eq!(message.usage, last_usage);
}

#[tokio::test]
async fn stream_ignores_unknown_events_without_fabricating_content() {
	let body = concat!(
		"event: future_event\n",
		"data: {\"type\":\"future_event\",\"payload\":1}\n\n",
		"event: ping\n",
		"data: {\"type\":\"ping\"}\n\n",
		"event: message_start\n",
		"data: {\"type\":\"message_start\",\"message\":{}}\n\n",
		"event: message_stop\n",
		"data: {\"type\":\"message_stop\"}\n\n"
	);
	let url = serve_responses(vec![response("200 OK", "text/event-stream", body)]).await;
	let provider = AnthropicMessagesProvider::new(url, "sk-key", "model").expect("provider");
	let mut stream = provider
		.stream(&[Message::user("hi")], &[], &ToolChoice::Auto)
		.await
		.expect("stream");
	let chunk = stream.next().await.expect("done").expect("ok");
	assert!(chunk.is_done);
	assert!(chunk.content.is_none());
	assert!(chunk.reasoning.is_none());
	assert!(chunk.tool_call_deltas.is_empty());
	assert!(stream.next().await.is_none());
}

#[tokio::test]
async fn stream_rejects_malformed_truncated_and_error_frames() {
	let malformed = concat!(
		"event: message_start\n",
		"data: {not json}\n\n"
	);
	let truncated = concat!(
		"event: message_start\n",
		"data: {\"type\":\"message_start\",\"message\":{}}\n\n",
		"event: content_block_start\n",
		"data: {\"type\":\"content_block_start\",\"index\":0,\"content_block\":{\"type\":\"text\",\"text\":\"\"}}\n\n",
		"event: content_block_delta\n",
		"data: {\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"text_delta\",\"text\":\"partial\"}}\n\n"
	);
	let stream_error = concat!(
		"event: error\n",
		"data: {\"type\":\"error\",\"error\":{\"type\":\"overloaded_error\",\"message\":\"Overloaded\"}}\n\n"
	);
	let url = serve_responses(vec![
		response("200 OK", "text/event-stream", malformed),
		response("200 OK", "text/event-stream", truncated),
		response("200 OK", "text/event-stream", stream_error),
	])
	.await;
	let provider = AnthropicMessagesProvider::new(url, "sk-key", "model").expect("provider");

	let mut stream = provider
		.stream(&[Message::user("hi")], &[], &ToolChoice::Auto)
		.await
		.expect("malformed stream");
	let err = stream.next().await.expect("error").expect_err("malformed");
	match err {
		ProviderError::Protocol(message) => assert!(message.contains("invalid Anthropic stream event JSON"), "{message}"),
		other => panic!("expected protocol error, got {other:?}"),
	}

	let mut stream = provider
		.stream(&[Message::user("hi")], &[], &ToolChoice::Auto)
		.await
		.expect("truncated stream");
	let mut saw_partial = false;
	let err = loop {
		match stream.next().await.expect("stream item") {
			Ok(chunk) => {
				if chunk.content.as_deref() == Some("partial") {
					saw_partial = true;
				}
			},
			Err(err) => break err,
		}
	};
	assert!(saw_partial, "content before truncation must stream");
	match err {
		ProviderError::Protocol(message) => assert!(message.contains("before message_stop"), "{message}"),
		other => panic!("expected truncated protocol error, got {other:?}"),
	}

	let mut stream = provider
		.stream(&[Message::user("hi")], &[], &ToolChoice::Auto)
		.await
		.expect("error stream");
	let err = stream.next().await.expect("error").expect_err("stream error");
	match err {
		ProviderError::Protocol(message) => {
			assert!(message.contains("overloaded_error"), "{message}");
			assert!(message.contains("Overloaded"), "{message}");
		},
		other => panic!("expected stream protocol error, got {other:?}"),
	}
}

#[tokio::test]
async fn http_status_errors_are_typed_and_bounded() {
	let long_body = "x".repeat(5_000);
	let url = serve_responses(vec![
		response("401 Unauthorized", "text/plain", "invalid credential"),
		response("429 Too Many Requests", "text/plain", &long_body),
	])
	.await;
	let provider = AnthropicMessagesProvider::new(url, "sk-key", "model")
		.expect("provider")
		.with_max_retries(0);

	let err = provider
		.complete(&[Message::user("hi")], &[], &ToolChoice::Auto)
		.await
		.expect_err("401");
	match err {
		ProviderError::HttpStatus(401, body) => assert_eq!(body, "invalid credential"),
		other => panic!("expected HttpStatus(401), got {other:?}"),
	}

	let err = match provider
		.stream(&[Message::user("hi")], &[], &ToolChoice::Auto)
		.await
	{
		Err(err) => err,
		Ok(_) => panic!("429 must fail before a stream is returned"),
	};
	match err {
		ProviderError::HttpStatus(429, body) => {
			assert_eq!(body.chars().count(), 2048, "error body must be capped");
		},
		other => panic!("expected HttpStatus(429), got {other:?}"),
	}
}

#[tokio::test]
async fn provider_errors_never_echo_the_api_key() {
	let secret = "sk-secret-boundary";
	let body = format!("upstream rejected {secret}");
	let url = serve_responses(vec![response("500 Internal Server Error", "text/plain", &body)]).await;
	let provider = AnthropicMessagesProvider::new(url, secret, "model")
		.expect("provider")
		.with_max_retries(0);
	let err = provider
		.complete(&[Message::user("hi")], &[], &ToolChoice::Auto)
		.await
		.expect_err("500");
	let rendered = format!("{err} {err:?}");
	assert!(
		!rendered.contains(secret),
		"API key must not appear in provider errors: {rendered}"
	);
	assert!(matches!(err, ProviderError::HttpStatus(500, _)), "{err:?}");
}

#[tokio::test]
async fn dropping_a_live_stream_closes_the_transport() {
	let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
	let addr = listener.local_addr().expect("addr");
	let (closed_tx, closed_rx) = tokio::sync::oneshot::channel();
	tokio::spawn(async move {
		let (mut socket, _) = listener.accept().await.expect("accept");
		let _ = read_request(&mut socket).await;
		let headers = "HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nTransfer-Encoding: chunked\r\n\r\n";
		let _ = socket.write_all(headers.as_bytes()).await;
		let event = concat!(
			"event: message_start\n",
			"data: {\"type\":\"message_start\",\"message\":{}}\n\n",
			"event: content_block_start\n",
			"data: {\"type\":\"content_block_start\",\"index\":0,\"content_block\":{\"type\":\"text\",\"text\":\"\"}}\n\n",
			"event: content_block_delta\n",
			"data: {\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"text_delta\",\"text\":\"first\"}}\n\n"
		);
		let chunk = format!("{:x}\r\n{event}\r\n", event.len());
		let _ = socket.write_all(chunk.as_bytes()).await;
		let _ = socket.flush().await;
		// The client must close this connection when it drops the stream.
		let mut tmp = [0u8; 64];
		let _ = socket.read(&mut tmp).await;
		let _ = closed_tx.send(());
	});

	let provider = AnthropicMessagesProvider::new(format!("http://{addr}"), "sk-drop", "model")
		.expect("provider");
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
async fn signed_and_redacted_thinking_round_trip() {
	let response_body = json!({
		"id": "msg_signed",
		"type": "message",
		"role": "assistant",
		"content": [
			{ "type": "thinking", "thinking": "plan", "signature": "sig-2" },
			{ "type": "redacted_thinking", "data": "opaque-data" },
			{ "type": "text", "text": "answer" }
		],
		"stop_reason": "end_turn",
		"usage": { "input_tokens": 4, "output_tokens": 2 }
	})
	.to_string();
	let url = serve_responses(vec![response("200 OK", "application/json", &response_body)]).await;
	let provider = AnthropicMessagesProvider::new(url, "sk-key", "claude-sonnet").expect("provider");
	let message = provider
		.complete(&[Message::user("hi")], &[], &ToolChoice::Auto)
		.await
		.expect("completion");

	assert_eq!(message.parts.len(), 3);
	assert_eq!(message.parts[0].as_thinking(), Some("plan"));
	assert!(matches!(
		&message.parts[0],
		ContentPart::Thinking { thinking_signature: Some(signature), .. } if signature.as_str() == "sig-2"
	));
	assert!(matches!(
		&message.parts[1],
		ContentPart::RedactedThinking { data } if data.as_str() == "opaque-data"
	));
	assert_eq!(message.parts[2], ContentPart::Text { text: "answer".to_string() });
	assert!(matches!(&message.stop_reason, Some(StopReason::Stop)));
	assert_eq!(message.api.as_deref(), Some("anthropic-messages"));
	assert_eq!(message.provider.as_deref(), Some("anthropic"));
	assert_eq!(message.model.as_deref(), Some("claude-sonnet"));

	let request = provider
		.build_request(&[message.clone()], &[], &ToolChoice::Auto, false)
		.expect("replay request");
	let body = serde_json::to_value(&request).expect("serialize");
	assert_eq!(body["messages"][0]["role"], "assistant");
	assert_eq!(body["messages"][0]["content"][0]["type"], "thinking");
	assert_eq!(body["messages"][0]["content"][0]["signature"], "sig-2");
	assert_eq!(body["messages"][0]["content"][1]["type"], "redacted_thinking");
	assert_eq!(body["messages"][0]["content"][1]["data"], "opaque-data");
	assert_eq!(body["messages"][0]["content"][2]["type"], "text");
	assert_eq!(body["messages"][0]["content"][2]["text"], "answer");
	// A trailing assistant prefill is repaired by the protocol adapter.
	assert_eq!(body["messages"][1]["role"], "user");
	assert_eq!(body["messages"][1]["content"], "Continue.");
}

#[tokio::test]
async fn refusal_stop_reason_survives_truncated_terminal_stream() {
	let body = concat!(
		"event: message_start\n",
		"data: {\"type\":\"message_start\",\"message\":{}}\n\n",
		"event: content_block_start\n",
		"data: {\"type\":\"content_block_start\",\"index\":0,\"content_block\":{\"type\":\"text\",\"text\":\"\"}}\n\n",
		"event: content_block_delta\n",
		"data: {\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"text_delta\",\"text\":\"partial\"}}\n\n",
		"event: message_delta\n",
		"data: {\"type\":\"message_delta\",\"delta\":{\"stop_reason\":\"refusal\",\"stop_details\":{\"type\":\"refusal\",\"category\":\"cyber\",\"explanation\":\"blocked\"}},\"usage\":{}}\n\n"
	);
	let url = serve_responses(vec![response("200 OK", "text/event-stream", body)]).await;
	let provider = AnthropicMessagesProvider::new(url, "sk-key", "model").expect("provider");
	let mut stream = provider
		.stream(&[Message::user("hi")], &[], &ToolChoice::Auto)
		.await
		.expect("stream");

	let mut terminal = None;
	let mut last = None;
	while let Some(item) = stream.next().await {
		let chunk = item.expect("chunk");
		if chunk.stop_reason.is_some() {
			terminal = Some(chunk.clone());
		}
		last = Some(chunk);
	}
	let terminal = terminal.expect("stop reason must reach the consumer");
	assert!(matches!(&terminal.stop_reason, Some(StopReason::Error)));
	assert_eq!(
		terminal.error_message.as_deref(),
		Some("Refusal (cyber): blocked")
	);
	let last = last.expect("terminal done chunk");
	assert!(last.is_done);
	assert!(matches!(&last.stop_reason, Some(StopReason::Error)));
	assert_eq!(
		last.error_message.as_deref(),
		Some("Refusal (cyber): blocked")
	);
}

const TINY_GIF: &str = "R0lGODlhAQABAIAAAAAAAP///ywAAAAAAQABAAACAUwAOw==";

#[tokio::test]
async fn retries_transient_429_once_then_succeeds() {
	let success = json!({
		"id": "msg_retry",
		"type": "message",
		"role": "assistant",
		"content": [{ "type": "text", "text": "ok" }],
		"usage": { "input_tokens": 1, "output_tokens": 1 }
	})
	.to_string();
	let url = serve_responses(vec![
		response_with_headers(
			"429 Too Many Requests",
			"text/plain",
			"Retry-After-Ms: 0\r\n",
			"slow down",
		),
		response("200 OK", "application/json", &success),
	])
	.await;
	let provider = AnthropicMessagesProvider::new(url, "sk-key", "model")
		.expect("provider")
		.with_max_retries(1);
	let message = provider
		.complete(&[Message::user("hi")], &[], &ToolChoice::Auto)
		.await
		.expect("retry succeeds");
	assert_eq!(message.content, "ok");
}

#[test]
fn tool_schema_is_normalized_to_anthropic_keywords() {
	let provider = AnthropicMessagesProvider::new("http://127.0.0.1:9", "sk-key", "model")
		.expect("provider");
	let tool = json!({
		"type": "function",
		"function": {
			"name": "read",
			"description": "Read",
			"parameters": {
				"type": "object",
				"properties": {
					"path": { "type": "string", "format": "uri", "pattern": "^a" },
					"tags": { "type": "array", "minItems": 1, "items": { "type": "string" } },
					"bad": { "type": "array", "minItems": 2, "items": { "type": "string" } },
					"meta": { "type": "object", "properties": { "x": { "type": "string" } } }
				},
				"required": ["path", 5],
				"additionalProperties": true,
				"anyOf": [{ "type": "object" }],
				"oneOf": [{ "type": "string" }]
			}
		}
	});
	let request = provider
		.build_request(&[Message::user("hi")], &[tool], &ToolChoice::Auto, false)
		.expect("request");
	let body = serde_json::to_value(&request).expect("serialize");
	let schema = &body["tools"][0]["input_schema"];
	assert_eq!(schema["type"], "object");
	assert_eq!(schema["required"], json!(["path"]));
	assert_eq!(schema["additionalProperties"], true);
	assert!(schema.get("anyOf").is_none());
	assert!(schema.get("oneOf").is_none());
	assert!(
		schema["description"].as_str().unwrap().contains("anyOf"),
		"{schema}"
	);
	assert_eq!(schema["properties"]["path"]["format"], "uri");
	assert!(schema["properties"]["path"].get("pattern").is_none());
	assert!(
		schema["properties"]["path"]["description"]
			.as_str()
			.unwrap()
			.contains("pattern")
	);
	assert_eq!(schema["properties"]["tags"]["minItems"], 1);
	assert!(schema["properties"]["bad"].get("minItems").is_none());
	assert!(
		schema["properties"]["bad"]["description"]
			.as_str()
			.unwrap()
			.contains("minItems")
	);
	assert_eq!(schema["properties"]["meta"]["additionalProperties"], false);
}

#[test]
fn sampling_and_thinking_compat_gate_request_fields() {
	let base = || {
		AnthropicMessagesProvider::new("http://127.0.0.1:9", "sk-key", "model").expect("provider")
	};

	let thinking = base()
		.with_max_tokens(8192)
		.with_request_max_tokens(Some(500))
		.with_thinking(Some(AnthropicThinkingConfig::Enabled {
			budget_tokens: 2000,
			display: None,
		}))
		.with_temperature(Some(0.2))
		.with_top_p(Some(0.9))
		.with_top_k(Some(5));
	let body = serde_json::to_value(
		thinking
			.build_request(&[Message::user("hi")], &[], &ToolChoice::Auto, false)
			.expect("thinking request"),
	)
	.expect("serialize");
	assert_eq!(body["max_tokens"], 6000);
	assert!(body.get("temperature").is_none());
	assert!(body.get("top_p").is_none());
	assert!(body.get("top_k").is_none());

	let sampling = base()
		.with_thinking(Some(AnthropicThinkingConfig::Disabled))
		.with_temperature(Some(0.2))
		.with_top_p(Some(0.9))
		.with_top_k(Some(5));
	let body = serde_json::to_value(
		sampling
			.build_request(&[Message::user("hi")], &[], &ToolChoice::Auto, false)
			.expect("sampling request"),
	)
	.expect("serialize");
	assert_eq!(body["temperature"], 0.2);
	assert_eq!(body["top_p"], 0.9);
	assert_eq!(body["top_k"], 5);

	let no_sampling = base()
		.with_sampling_params(false)
		.with_thinking(Some(AnthropicThinkingConfig::Disabled))
		.with_temperature(Some(0.2));
	let body = serde_json::to_value(
		no_sampling
			.build_request(&[Message::user("hi")], &[], &ToolChoice::Auto, false)
			.expect("no-sampling request"),
	)
	.expect("serialize");
	assert!(body.get("temperature").is_none());

	let downgraded = base()
		.with_forced_tool_choice(false)
		.with_thinking(Some(AnthropicThinkingConfig::Enabled {
			budget_tokens: 1024,
			display: None,
		}));
	let body = serde_json::to_value(
		downgraded
			.build_request(
				&[Message::user("hi")],
				&[read_tool()],
				&ToolChoice::Tool { name: "read".to_string() },
				false,
			)
			.expect("downgraded request"),
	)
	.expect("serialize");
	assert_eq!(body["tool_choice"], json!({ "type": "auto" }));
	assert_eq!(body["thinking"]["type"], "enabled");

	let adaptive = base()
		.with_adaptive_only_thinking(true)
		.with_thinking(Some(AnthropicThinkingConfig::Disabled));
	let body = serde_json::to_value(
		adaptive
			.build_request(&[Message::user("hi")], &[], &ToolChoice::Auto, false)
			.expect("adaptive request"),
	)
	.expect("serialize");
	assert!(body.get("thinking").is_none());
	assert_eq!(body["output_config"]["effort"], "low");
}

#[test]
fn cache_retention_adds_breakpoints_and_long_ttl_beta() {
	let provider = AnthropicMessagesProvider::new("http://127.0.0.1:9", "sk-key", "model")
		.expect("provider")
		.with_cache_retention(pi_agent::provider_anthropic::CacheRetention::Short, false);
	let request = provider
		.build_request(
			&[
				Message::user("one"),
				Message::assistant("two"),
				Message::user("three"),
			],
			&[],
			&ToolChoice::Auto,
			false,
		)
		.expect("request");
	let body = serde_json::to_value(&request).expect("serialize");
	assert_eq!(body["messages"][2]["content"][0]["type"], "text");
	assert_eq!(
		body["messages"][2]["content"][0]["cache_control"],
		json!({ "type": "ephemeral" })
	);
	assert_eq!(
		body["messages"][1]["content"][0]["cache_control"],
		json!({ "type": "ephemeral" })
	);

	let long = AnthropicMessagesProvider::new("http://127.0.0.1:9", "sk-key", "model")
		.expect("provider")
		.with_cache_retention(pi_agent::provider_anthropic::CacheRetention::Long, true);
	let request = long
		.build_request(&[Message::user("hi")], &[], &ToolChoice::Auto, false)
		.expect("long request");
	let body = serde_json::to_value(&request).expect("serialize");
	assert_eq!(body["messages"][0]["content"][0]["cache_control"]["ttl"], "1h");
	let beta = long
		.request_headers(false)
		.expect("headers")
		.get("anthropic-beta")
		.unwrap()
		.to_str()
		.unwrap()
		.to_string();
	assert!(beta.contains("extended-cache-ttl-2025-04-11"), "{beta}");
}

#[test]
fn image_placeholder_and_error_tool_result_hoisting() {
	let no_vision = AnthropicMessagesProvider::new("http://127.0.0.1:9", "sk-key", "model")
		.expect("provider")
		.with_images(false);
	let user = Message::user_with_images("look", vec![ImagePart::new("image/gif", TINY_GIF)]);
	let body = serde_json::to_value(
		no_vision
			.build_request(&[user], &[], &ToolChoice::Auto, false)
			.expect("placeholder request"),
	)
	.expect("serialize");
	let content = body["messages"][0]["content"].as_str().unwrap();
	assert!(content.contains("look"), "{content}");
	assert!(
		content.contains("[image omitted: model does not support vision]"),
		"{content}"
	);

	let vision = AnthropicMessagesProvider::new("http://127.0.0.1:9", "sk-key", "model")
		.expect("provider");
	let error_tool = Message::tool_with_status_and_parts(
		"toolu_1",
		"read",
		"",
		vec![ContentPart::image(TINY_GIF, "image/gif").expect("image")],
		true,
	);
	let body = serde_json::to_value(
		vision
			.build_request(&[error_tool], &[], &ToolChoice::Auto, false)
			.expect("hoist request"),
	)
	.expect("serialize");
	assert_eq!(body["messages"][0]["content"][0]["type"], "tool_result");
	assert_eq!(
		body["messages"][0]["content"][0]["content"],
		"Tool failed with no output."
	);
	assert_eq!(body["messages"][0]["content"][1]["type"], "text");
	assert_eq!(
		body["messages"][0]["content"][1]["text"],
		"Attached image(s) from the tool result(s) above:"
	);
	assert_eq!(body["messages"][0]["content"][2]["type"], "image");

	let ok_tool = Message::tool_with_status_and_parts(
		"toolu_2",
		"read",
		"",
		vec![ContentPart::image(TINY_GIF, "image/gif").expect("image")],
		false,
	);
	let body = serde_json::to_value(
		vision
			.build_request(&[ok_tool], &[], &ToolChoice::Auto, false)
			.expect("image tool request"),
	)
	.expect("serialize");
	assert_eq!(body["messages"][0]["content"][0]["type"], "tool_result");
	assert_eq!(
		body["messages"][0]["content"][0]["content"][0]["text"],
		"(see attached image)"
	);
	assert_eq!(
		body["messages"][0]["content"][0]["content"][1]["type"],
		"image"
	);
}

#[tokio::test]
async fn oauth_headers_system_blocks_and_cch_are_source_backed() {
	let success = json!({
		"id": "msg_oauth",
		"type": "message",
		"role": "assistant",
		"content": [{ "type": "text", "text": "ok" }]
	})
	.to_string();
	let (url, captured) = serve_capturing(response("200 OK", "application/json", &success)).await;
	let provider = AnthropicMessagesProvider::new(url, "sk-ant-oat-token", "claude-sonnet")
		.expect("provider")
		.with_claude_code_session_id(Some("session-1".to_string()));
	let message = provider
		.complete(
			&[Message::system("prompt"), Message::user("hello")],
			&[],
			&ToolChoice::Auto,
		)
		.await
		.expect("oauth completion");
	assert_eq!(message.content, "ok");

	let (headers, body) = captured.await.expect("captured");
	assert!(
		headers
			.to_ascii_lowercase()
			.contains("authorization: bearer sk-ant-oat-token"),
		"{headers}"
	);
	assert!(!headers.to_ascii_lowercase().contains("x-api-key"), "{headers}");
	assert!(
		headers
			.to_ascii_lowercase()
			.contains("user-agent: claude-cli/2.1.246"),
		"{headers}"
	);
	assert!(headers.to_ascii_lowercase().contains("x-stainless-arch:"), "{headers}");
	assert!(
		headers
			.to_ascii_lowercase()
			.contains("x-claude-code-session-id: session-1"),
		"{headers}"
	);
	let request_id = headers
		.lines()
		.find_map(|line| line.to_ascii_lowercase().strip_prefix("x-client-request-id:").map(str::to_string))
		.expect("request id");
	assert_eq!(request_id.trim().len(), 36, "{request_id}");

	let billing = body["system"][0]["text"].as_str().unwrap();
	assert!(billing.starts_with("x-anthropic-billing-header:"), "{billing}");
	assert!(billing.contains("cc_version=2.1.246."), "{billing}");
	assert!(!billing.contains("cch=00000"), "CCH must be attested: {billing}");
	assert!(billing.contains("cch="), "{billing}");
	assert_eq!(
		body["system"][1]["text"],
		"You are a Claude agent, built on Anthropic's Claude Agent SDK."
	);
	assert_eq!(body["system"][2]["text"], "prompt");
}

#[tokio::test]
async fn idle_watchdog_fails_a_stalled_stream() {
	let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
	let addr = listener.local_addr().expect("addr");
	tokio::spawn(async move {
		let (mut socket, _) = listener.accept().await.expect("accept");
		let _ = read_request(&mut socket).await;
		let headers = "HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nTransfer-Encoding: chunked\r\n\r\n";
		let _ = socket.write_all(headers.as_bytes()).await;
		let event = "event: message_start\ndata: {\"type\":\"message_start\",\"message\":{}}\n\n";
		let chunk = format!("{:x}\r\n{event}\r\n", event.len());
		let _ = socket.write_all(chunk.as_bytes()).await;
		let _ = socket.flush().await;
		tokio::time::sleep(Duration::from_secs(2)).await;
	});

	let provider = AnthropicMessagesProvider::new(format!("http://{addr}"), "sk-key", "model")
		.expect("provider")
		.with_stream_timeouts(None, Some(Duration::from_millis(100)));
	let mut stream = provider
		.stream(&[Message::user("hi")], &[], &ToolChoice::Auto)
		.await
		.expect("stream");
	let err = stream.next().await.expect("item").expect_err("watchdog");
	match err {
		ProviderError::Network(message) => assert!(message.contains("stalled"), "{message}"),
		other => panic!("expected watchdog network error, got {other:?}"),
	}
}

#[test]
fn gateway_flags_mid_system_strict_and_header_precedence() {
	let provider = AnthropicMessagesProvider::new("http://127.0.0.1:9", "sk-key", "model")
		.expect("provider")
		.with_requires_tool_result_id(true)
		.with_escape_builtin_tool_names(true)
		.with_mid_conversation_system(true)
		.with_strict_tools(true, false)
		.with_provider_id(Some("gateway-z".to_string()))
		.with_custom_headers(vec![
			("X-Api-Key".to_string(), "caller-key".to_string()),
			("Authorization".to_string(), "Bearer caller".to_string()),
			("X-Custom".to_string(), "kept".to_string()),
			("anthropic-beta".to_string(), "dropped".to_string()),
		]);
	assert_eq!(provider.provider(), Some("gateway-z"));

	let tool = json!({
		"type": "function",
		"function": {
			"name": "bash",
			"description": "Shell",
			"parameters": {
				"type": "object",
				"properties": {
					"command": { "type": "string" },
					"cwd": { "type": "string" }
				},
				"required": ["command"]
			}
		}
	});
	let messages = vec![
		Message::user("run"),
		Message::developer("developer note"),
		Message::assistant("ok"),
		Message::tool("toolu_1", "out"),
	];
	let body = serde_json::to_value(
		provider
			.build_request(&messages, &[tool], &ToolChoice::Auto, false)
			.expect("request"),
	)
	.expect("serialize");
	assert_eq!(body["tools"][0]["name"], "_bash");
	assert_eq!(body["tools"][0]["strict"], true);
	assert_eq!(body["tools"][0]["input_schema"]["additionalProperties"], false);
	assert_eq!(body["tools"][0]["input_schema"]["required"], json!(["command", "cwd"]));
	assert_eq!(
		body["tools"][0]["input_schema"]["properties"]["cwd"]["type"],
		json!(["string", "null"])
	);
	assert_eq!(body["messages"][1]["role"], "system");
	assert_eq!(body["messages"][3]["content"][0]["id"], "toolu_1");
	assert_eq!(body["messages"][3]["content"][0]["tool_use_id"], "toolu_1");

	let headers = provider.request_headers(false).expect("headers");
	assert_eq!(
		headers.get("authorization").unwrap().to_str().unwrap(),
		"Bearer caller"
	);
	assert_eq!(headers.get("x-api-key").unwrap().to_str().unwrap(), "caller-key");
	assert_eq!(headers.get("x-custom").unwrap().to_str().unwrap(), "kept");
	assert!(headers.get("anthropic-beta").is_none());
}

// ─── Security and wire-boundary regressions ────────────────────────────────

fn sse_events(events: &[(&str, Value)]) -> String {
	let mut body = String::new();
	for (name, data) in events {
		body.push_str(&format!("event: {name}\ndata: {data}\n\n"));
	}
	body
}

#[tokio::test]
async fn http_error_body_is_bounded_and_redacts_every_effective_credential() {
	let api_key = "sk-configured-secret";
	let custom = "custom-secret-token";
	let mut body = format!("gateway rejected: {api_key} {custom} ");
	// The read ceiling lands inside this run; the trailing credential must not
	// survive as a prefix of the cut token.
	body.push_str(&"a".repeat(70 * 1024));
	body.push_str(custom);
	let url = serve_responses(vec![response("500 Internal Server Error", "text/plain", &body)]).await;
	let provider = AnthropicMessagesProvider::new(url, api_key, "model")
		.expect("provider")
		.with_max_retries(0)
		.with_custom_headers(vec![
			("Authorization".to_string(), format!("Bearer {custom}")),
			("x-api-key".to_string(), custom.to_string()),
		]);
	let err = provider
		.complete(&[Message::user("hi")], &[], &ToolChoice::Auto)
		.await
		.expect_err("HTTP failure");
	match err {
		ProviderError::HttpStatus(status, snippet) => {
			assert_eq!(status, 500);
			assert!(snippet.len() < 70 * 1024, "error body must be bounded");
			assert!(snippet.contains("[redacted]"), "{snippet}");
			assert!(snippet.contains("[truncated]"), "{snippet}");
			assert!(!snippet.contains(api_key), "{snippet}");
			assert!(!snippet.contains(custom), "{snippet}");
			assert!(!snippet.contains("aaaaaaaaaa"), "cut token prefix leaked: {snippet}");
		},
		other => panic!("expected HttpStatus(500), got {other:?}"),
	}
}

#[tokio::test]
async fn refusal_details_and_sse_error_type_are_redacted() {
	let api_key = "sk-config-secret";
	let custom = "custom-refusal-token";
	let refusal = json!({
		"content": [{ "type": "text", "text": "refused" }],
		"stop_reason": "refusal",
		"stop_details": {
			"type": "refusal",
			"category": custom,
			"explanation": format!("gateway echoed {api_key}")
		}
	})
	.to_string();
	let stream_error = sse_events(&[(
		"error",
		json!({
			"type": "error",
			"error": { "type": format!("{custom}_violation"), "message": format!("denied {api_key}") }
		}),
	)]);
	let url = serve_responses(vec![
		response("200 OK", "application/json", &refusal),
		response("200 OK", "text/event-stream", &stream_error),
	])
	.await;
	let headers = vec![
		("Authorization".to_string(), format!("Bearer {custom}")),
		("x-api-key".to_string(), custom.to_string()),
	];
	let provider = AnthropicMessagesProvider::new(url.clone(), api_key, "model")
		.expect("provider")
		.with_max_retries(0)
		.with_custom_headers(headers.clone());
	let message = provider
		.complete(&[Message::user("hi")], &[], &ToolChoice::Auto)
		.await
		.expect("refusal completion");
	let rendered = message.error_message.expect("refusal error message");
	assert!(rendered.contains("[redacted]"), "{rendered}");
	assert!(!rendered.contains(api_key), "{rendered}");
	assert!(!rendered.contains(custom), "{rendered}");

	let provider = AnthropicMessagesProvider::new(url, api_key, "model")
		.expect("provider")
		.with_max_retries(0)
		.with_custom_headers(headers);
	let mut stream = provider
		.stream(&[Message::user("hi")], &[], &ToolChoice::Auto)
		.await
		.expect("error stream");
	let err = stream.next().await.expect("item").expect_err("SSE error");
	let rendered = format!("{err}");
	assert!(rendered.contains("[redacted]"), "{rendered}");
	assert!(!rendered.contains(api_key), "{rendered}");
	assert!(!rendered.contains(custom), "{rendered}");
}

#[tokio::test]
async fn split_signature_deltas_are_joined_and_emitted_once() {
	let body = sse_events(&[
		(
			"message_start",
			json!({ "type": "message_start", "message": { "usage": { "input_tokens": 3 } } }),
		),
		(
			"content_block_start",
			json!({
				"type": "content_block_start",
				"index": 0,
				"content_block": { "type": "thinking", "thinking": "plan" }
			}),
		),
		(
			"content_block_delta",
			json!({
				"type": "content_block_delta",
				"index": 0,
				"delta": { "type": "signature_delta", "signature": "sig-" }
			}),
		),
		(
			"content_block_delta",
			json!({
				"type": "content_block_delta",
				"index": 0,
				"delta": { "type": "signature_delta", "signature": "part-2" }
			}),
		),
		("content_block_stop", json!({ "type": "content_block_stop", "index": 0 })),
		("message_stop", json!({ "type": "message_stop" })),
	]);
	let url = serve_responses(vec![response("200 OK", "text/event-stream", &body)]).await;
	let provider = AnthropicMessagesProvider::new(url, "sk-key", "model")
		.expect("provider")
		.with_max_retries(0);
	let mut stream = provider
		.stream(&[Message::user("hi")], &[], &ToolChoice::Auto)
		.await
		.expect("stream");
	let mut signatures = Vec::new();
	while let Some(item) = stream.next().await {
		let chunk = item.expect("chunk");
		if let Some(signature) = chunk.signature {
			signatures.push(signature);
		}
	}
	assert_eq!(
		signatures,
		vec!["sig-part-2".to_string()],
		"split deltas must produce one finalized signature"
	);
}

#[tokio::test]
async fn usage_only_message_delta_does_not_terminate_the_stream() {
	let body = sse_events(&[
		(
			"message_start",
			json!({ "type": "message_start", "message": { "usage": { "input_tokens": 1 } } }),
		),
		(
			"message_delta",
			json!({ "type": "message_delta", "usage": { "output_tokens": 4 } }),
		),
	]);
	let url = serve_responses(vec![response("200 OK", "text/event-stream", &body)]).await;
	let provider = AnthropicMessagesProvider::new(url, "sk-key", "model")
		.expect("provider")
		.with_max_retries(0);
	let mut stream = provider
		.stream(&[Message::user("hi")], &[], &ToolChoice::Auto)
		.await
		.expect("stream");
	let mut error = None;
	while let Some(item) = stream.next().await {
		if let Err(err) = item {
			error = Some(err);
			break;
		}
	}
	match error.expect("usage-only delta must not end the stream") {
		ProviderError::Protocol(message) => {
			assert!(message.contains("before message_stop"), "{message}");
		},
		other => panic!("expected truncation protocol error, got {other:?}"),
	}
}

#[tokio::test]
async fn large_but_legitimate_event_and_completion_bodies_pass_intact() {
	let text = "y".repeat(200_000);
	let body = sse_events(&[
		("message_start", json!({ "type": "message_start", "message": {} })),
		(
			"content_block_start",
			json!({
				"type": "content_block_start",
				"index": 0,
				"content_block": { "type": "text", "text": "" }
			}),
		),
		(
			"content_block_delta",
			json!({
				"type": "content_block_delta",
				"index": 0,
				"delta": { "type": "text_delta", "text": text.clone() }
			}),
		),
		("content_block_stop", json!({ "type": "content_block_stop", "index": 0 })),
		("message_stop", json!({ "type": "message_stop" })),
	]);
	let completion_text = "z".repeat(256 * 1024);
	let completion = json!({
		"content": [{ "type": "text", "text": completion_text.clone() }],
		"stop_reason": "end_turn"
	})
	.to_string();
	let url = serve_responses(vec![
		response("200 OK", "text/event-stream", &body),
		response("200 OK", "application/json", &completion),
	])
	.await;
	let provider = AnthropicMessagesProvider::new(url, "sk-key", "model")
		.expect("provider")
		.with_max_retries(0);
	let mut stream = provider
		.stream(&[Message::user("hi")], &[], &ToolChoice::Auto)
		.await
		.expect("stream");
	let mut streamed = String::new();
	while let Some(item) = stream.next().await {
		let chunk = item.expect("chunk");
		if let Some(delta) = chunk.content {
			streamed.push_str(&delta);
		}
	}
	assert_eq!(streamed.len(), text.len(), "large delta must not be truncated");
	assert_eq!(streamed, text);

	let message = provider
		.complete(&[Message::user("hi")], &[], &ToolChoice::Auto)
		.await
		.expect("completion");
	assert_eq!(message.content, completion_text, "success body must not be truncated");
}

#[tokio::test]
async fn oversized_unterminated_sse_line_fails_instead_of_buffering() {
	let mut line = String::from("data: ");
	line.push_str(&"a".repeat(1024 * 1024 + 16));
	let url = serve_responses(vec![response("200 OK", "text/event-stream", &line)]).await;
	let provider = AnthropicMessagesProvider::new(url, "sk-key", "model")
		.expect("provider")
		.with_max_retries(0);
	let mut stream = provider
		.stream(&[Message::user("hi")], &[], &ToolChoice::Auto)
		.await
		.expect("stream");
	let err = stream.next().await.expect("item").expect_err("line cap");
	match err {
		ProviderError::Protocol(message) => {
			assert!(message.contains("line exceeded"), "{message}");
		},
		other => panic!("expected line-cap protocol error, got {other:?}"),
	}
}

#[tokio::test]
async fn first_event_deadline_ignores_a_slow_transport_drip() {
	let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
	let addr = listener.local_addr().expect("addr");
	tokio::spawn(async move {
		let (mut socket, _) = listener.accept().await.expect("accept");
		let _ = read_request(&mut socket).await;
		let headers = "HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nTransfer-Encoding: chunked\r\n\r\n";
		let _ = socket.write_all(headers.as_bytes()).await;
		for _ in 0..200 {
			// Bytes, never a complete event: the absolute deadline must still fire.
			let piece = b"data";
			let mut framed = format!("{:x}\r\n", piece.len()).into_bytes();
			framed.extend_from_slice(piece);
			framed.extend_from_slice(b"\r\n");
			if socket.write_all(&framed).await.is_err() {
				return;
			}
			let _ = socket.flush().await;
			tokio::time::sleep(Duration::from_millis(20)).await;
		}
	});
	let provider = AnthropicMessagesProvider::new(format!("http://{addr}"), "sk-key", "model")
		.expect("provider")
		.with_max_retries(0)
		.with_stream_timeouts(Some(Duration::from_millis(150)), None);
	let started = std::time::Instant::now();
	let mut stream = provider
		.stream(&[Message::user("hi")], &[], &ToolChoice::Auto)
		.await
		.expect("stream");
	let item = tokio::time::timeout(Duration::from_secs(3), stream.next())
		.await
		.expect("drip must not hang the watchdog")
		.expect("item");
	let err = item.expect_err("a drip must not extend the first-event budget");
	match err {
		ProviderError::Network(message) => {
			assert!(message.contains("first event"), "{message}");
		},
		other => panic!("expected first-event timeout, got {other:?}"),
	}
	assert!(
		started.elapsed() < Duration::from_secs(2),
		"the timeout must fire near its budget: {:?}",
		started.elapsed()
	);
}

#[test]
fn escape_builtin_tool_names_prefixes_builtins_but_oauth_alone_keeps_them() {
	let builtin = json!({
		"type": "function",
		"function": {
			"name": "web_search",
			"description": "Search",
			"parameters": { "type": "object", "properties": {} }
		}
	});
	let tools = vec![read_tool(), builtin];
	let escaping = AnthropicMessagesProvider::new("http://127.0.0.1:9", "sk-key", "model")
		.expect("provider")
		.with_escape_builtin_tool_names(true);
	let body = serde_json::to_value(
		escaping
			.build_request(&[Message::user("hi")], &tools, &ToolChoice::Auto, false)
			.expect("request"),
	)
	.expect("serialize");
	assert_eq!(body["tools"][0]["name"], "_read");
	assert_eq!(body["tools"][1]["name"], "_web_search");

	let oauth = AnthropicMessagesProvider::new("http://127.0.0.1:9", "sk-ant-oat-token", "model")
		.expect("provider");
	let body = serde_json::to_value(
		oauth
			.build_request(&[Message::user("hi")], &tools, &ToolChoice::Auto, false)
			.expect("request"),
	)
	.expect("serialize");
	assert_eq!(body["tools"][0]["name"], "_read");
	assert_eq!(body["tools"][1]["name"], "web_search");
}

#[test]
fn mixed_signed_and_unsigned_thinking_demotes_the_unsigned_half() {
	let provider = AnthropicMessagesProvider::new("http://127.0.0.1:9", "sk-key", "model")
		.expect("provider")
		.with_replay_unsigned_thinking(true);
	let mut message = Message::assistant("");
	message.parts = vec![
		ContentPart::thinking_with_signature("signed plan", "sig-1"),
		ContentPart::thinking("unsigned plan"),
	];
	let body = serde_json::to_value(
		provider
			.build_request(&[message], &[], &ToolChoice::Auto, false)
			.expect("request"),
	)
	.expect("serialize");
	let content = body["messages"][0]["content"].as_array().expect("blocks");
	assert_eq!(content.len(), 2, "unsigned half must not replay as thinking: {body}");
	assert_eq!(content[0]["type"], "thinking");
	assert_eq!(content[0]["signature"], "sig-1");
	assert_eq!(content[1]["type"], "text");
	assert_eq!(content[1]["text"], "unsigned plan");
}

#[tokio::test]
async fn uppercase_bearer_scheme_token_is_redacted() {
	let token = "Upper-Secret-Token";
	let body = format!("gateway echoed {token} and BEARER {token}");
	let url = serve_responses(vec![response("500 Internal Server Error", "text/plain", &body)]).await;
	let provider = AnthropicMessagesProvider::new(url, "sk-config-secret", "model")
		.expect("provider")
		.with_max_retries(0)
		.with_custom_headers(vec![(
			"Authorization".to_string(),
			format!("BEARER {token}"),
		)]);
	let err = provider
		.complete(&[Message::user("hi")], &[], &ToolChoice::Auto)
		.await
		.expect_err("HTTP failure");
	match err {
		ProviderError::HttpStatus(_, snippet) => {
			assert!(snippet.contains("[redacted]"), "{snippet}");
			assert!(!snippet.contains(token), "{snippet}");
		},
		other => panic!("expected HttpStatus, got {other:?}"),
	}
}

#[tokio::test]
async fn completed_line_cap_rejects_a_newline_bearing_chunk() {
	let mut body = String::from("data: ");
	body.push_str(&"a".repeat(1024 * 1024));
	body.push_str("\n\n");
	let url = serve_responses(vec![response("200 OK", "text/event-stream", &body)]).await;
	let provider = AnthropicMessagesProvider::new(url, "sk-key", "model")
		.expect("provider")
		.with_max_retries(0);
	let mut stream = provider
		.stream(&[Message::user("hi")], &[], &ToolChoice::Auto)
		.await
		.expect("stream");
	let err = stream.next().await.expect("item").expect_err("line cap");
	match err {
		ProviderError::Protocol(message) => {
			assert!(message.contains("line exceeded"), "{message}");
		},
		other => panic!("expected line-cap protocol error, got {other:?}"),
	}
}

#[tokio::test]
async fn event_capacity_is_checked_before_appending_a_data_line() {
	let mut body = String::new();
	for _ in 0..9 {
		body.push_str("data: ");
		body.push_str(&"b".repeat(1_000_000));
		body.push('\n');
	}
	body.push('\n');
	let url = serve_responses(vec![response("200 OK", "text/event-stream", &body)]).await;
	let provider = AnthropicMessagesProvider::new(url, "sk-key", "model")
		.expect("provider")
		.with_max_retries(0);
	let mut stream = provider
		.stream(&[Message::user("hi")], &[], &ToolChoice::Auto)
		.await
		.expect("stream");
	let err = stream.next().await.expect("item").expect_err("event cap");
	match err {
		ProviderError::Protocol(message) => {
			assert!(message.contains("event exceeded"), "{message}");
		},
		other => panic!("expected event-cap protocol error, got {other:?}"),
	}
}
