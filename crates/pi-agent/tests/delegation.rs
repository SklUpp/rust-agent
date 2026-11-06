use std::{
	path::PathBuf,
	sync::Arc,
	time::{Duration, SystemTime, UNIX_EPOCH},
};

use pi_agent::{
	AgentConfig, AgentContext, AgentLoop, AgentState,
	ChildStatus, DelegationLimits, HubTool, OpenAiCompatibleProvider,
	Role, Supervisor, TaskTool, Tool, ToolRegistry,
	tools::{InternalUrlRouter, UrlScope, task_catalog::{AgentCatalog, AgentDefinition, AgentSource, Spawns}},
};
use pi_agent::tools::hub::HubToolDetails;
use pi_agent::tools::task::TaskToolDetails;
use serde_json::{Value, json};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::sync::{Semaphore, mpsc, oneshot};

fn tool_call_response(tool_name: &str, tool_args: &Value) -> Value {
	json!({
		"choices": [
			{
				"message": {
					"role": "assistant",
					"content": null,
					"tool_calls": [
						{
							"id": "call_1",
							"type": "function",
							"function": {
								"name": tool_name,
								"arguments": serde_json::to_string(tool_args).unwrap()
							}
						}
					]
				}
			}
		]
	})
}

fn text_response(text: &str) -> Value {
	json!({
		"choices": [
			{
				"message": {
					"role": "assistant",
					"content": text,
					"tool_calls": null
				}
			}
		]
	})
}

struct MockServer {
	pub base_url: String,
	_shutdown: oneshot::Sender<()>,
	pub hang_closed_rx: Option<mpsc::Receiver<Result<usize, String>>>,
}

async fn spawn_mock_server(
	hang_marker: Option<&'static str>,
) -> (MockServer, Option<mpsc::Receiver<()>>) {
	let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind listener");
	let port = listener.local_addr().unwrap().port();
	let base_url = format!("http://127.0.0.1:{port}");

	let (shutdown_tx, mut shutdown_rx) = oneshot::channel();
	let (hang_signal_tx, hang_signal_rx) = if hang_marker.is_some() {
		let (tx, rx) = mpsc::channel(1);
		(Some(tx), Some(rx))
	} else {
		(None, None)
	};

	let (hang_closed_tx, hang_closed_rx) = if hang_marker.is_some() {
		let (tx, rx) = mpsc::channel(1);
		(Some(tx), Some(rx))
	} else {
		(None, None)
	};

	tokio::spawn(async move {
		loop {
			tokio::select! {
				_ = &mut shutdown_rx => break,
				accept_res = listener.accept() => {
					let (mut socket, _) = match accept_res {
						Ok(s) => s,
						Err(_) => break,
					};

					let hang_signal_tx = hang_signal_tx.clone();
					let hang_closed_tx = hang_closed_tx.clone();

					tokio::spawn(async move {
						let mut buf = Vec::new();
						let mut temp = [0u8; 1024];
						let mut header_len = None;
						let mut content_len = 0;

						while let Ok(n) = socket.read(&mut temp).await {
							if n == 0 {
								break;
							}
							buf.extend_from_slice(&temp[..n]);

							if header_len.is_none() {
								if let Some(pos) = buf.windows(4).position(|w| w == b"\r\n\r\n") {
									header_len = Some(pos + 4);
									let header_str = String::from_utf8_lossy(&buf[..pos]);
									for line in header_str.lines() {
										if line.to_ascii_lowercase().starts_with("content-length:") {
											if let Some(val) = line.split(':').nth(1) {
												content_len = val.trim().parse::<usize>().unwrap_or(0);
											}
										}
									}
								}
							}

							if let Some(h_len) = header_len {
								if buf.len() >= h_len + content_len {
									break;
								}
							}
						}

						let h_len = header_len.unwrap_or(0);
						let body = if buf.len() >= h_len + content_len {
							String::from_utf8_lossy(&buf[h_len..h_len + content_len]).to_string()
						} else {
							String::new()
						};

						// Check for hang marker in T3
						if let Some(marker) = hang_marker {
							if body.contains(marker) {
								if let Some(tx) = hang_signal_tx {
									let _ = tx.send(()).await;
								}
								// Do NOT respond: wait for connection close
								let mut wait_buf = [0u8; 100];
								let res = socket.read(&mut wait_buf).await;
								if let Some(ctx) = hang_closed_tx {
									let _ = ctx.send(res.map_err(|e| e.to_string())).await;
								}
								return;
							}
						}

						// Route on the latest message only: the request body carries the
						// whole history, so matching on it replays the first route forever.
						let resp_json = route_response(&last_message_text(&body));
						// Root turns run with `complete` (`stream: false`); child turns
						// forward events and therefore use `stream: true`. The mock must
						// answer both shapes or every child dies before its first tool call.
						let wants_stream = serde_json::from_str::<Value>(&body)
							.ok()
							.and_then(|request| request.get("stream").and_then(Value::as_bool))
							.unwrap_or(false);
						let (content_type, payload) = if wants_stream {
							("text/event-stream", stream_sse_body(&resp_json))
						} else {
							("application/json", serde_json::to_string(&resp_json).unwrap())
						};
						let response = format!(
							"HTTP/1.1 200 OK\r\nContent-Type: {content_type}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{payload}",
							payload.len(),
						);

						let _ = socket.write_all(response.as_bytes()).await;
						let _ = socket.flush().await;
					});
				}
			}
		}
	});

	(
		MockServer {
			base_url,
			_shutdown: shutdown_tx,
			hang_closed_rx,
		},
		hang_signal_rx,
	)
}

/// Text of the last message in an OpenAI-style chat request.
///
/// The request body carries the whole history, so scripted routes must key off
/// the message the model is actually answering instead of any history keyword.
fn last_message_text(body: &str) -> String {
	let Ok(request) = serde_json::from_str::<Value>(body) else {
		return body.to_string();
	};
	let Some(messages) = request.get("messages").and_then(Value::as_array) else {
		return body.to_string();
	};
	let Some(last) = messages.last() else {
		return String::new();
	};
	match last.get("content") {
		Some(Value::String(text)) => text.clone(),
		Some(Value::Array(parts)) => parts
			.iter()
			.filter_map(|part| part.get("text").and_then(Value::as_str))
			.collect::<Vec<_>>()
			.join(""),
		_ => String::new(),
	}
}

/// Scripted model reply for the latest message: most advanced conversation
/// stage first, because `child_finished` is a substring of `grandchild_finished`
/// and tool results echo the child task text they ran.
fn route_response(latest: &str) -> Value {
	// Nested output-id regression: grandchild -> child -> root files all live in
	// the producer artifacts root with dot-separated names.
	if latest.contains("nested_done") {
		return tool_call_response(
			"yield",
			&json!({ "result": { "data": "child_done" } }),
		);
	}
	if latest.contains("NESTED_GRAND") {
		return tool_call_response(
			"yield",
			&json!({ "result": { "data": "nested_done" } }),
		);
	}
	if latest.contains("NESTED_CHILD") {
		return tool_call_response(
			"task",
			&json!({ "name": "NestedGrand", "task": "NESTED_GRAND" }),
		);
	}
	if latest.contains("NESTED_PARENT") {
		return tool_call_response(
			"task",
			&json!({ "name": "NestedChild", "agent": "scout", "task": "NESTED_CHILD" }),
		);
	}

	// T9: child runs a long shell command that the parent future abandons.
	if latest.contains("BASH_DROP") {
		return tool_call_response(
			"bash",
			&json!({
				"command": "sh -c 'echo $$ > pid.txt; sleep 2; touch marker.txt'",
				"timeout": 0,
			}),
		);
	}

	// T8: a yield error is a task failure, not a cancellation.
	if latest.contains("YIELD_ERROR") {
		return tool_call_response("yield", &json!({ "result": { "error": "boom" } }));
	}

	// T2 recovery stages
	if latest.contains("Tool not found: task") {
		return tool_call_response(
			"yield",
			&json!({ "result": { "data": "grandchild_finished" } }),
		);
	}
	if latest.contains("grandchild_finished") {
		return tool_call_response(
			"yield",
			&json!({ "result": { "data": "child_finished" } }),
		);
	}
	if latest.contains("child_finished") {
		return text_response("Root completed after T2 depth.");
	}

	// T1 / T4 completion stages
	if latest.contains("result_val") {
		return text_response("Root completed after T1 child.");
	}
	if latest.contains("output_A") || latest.contains("output_B") {
		return text_response("Root completed after T4 batch.");
	}

	// T1 routes
	if latest.contains("T1_START") {
		return tool_call_response(
			"task",
			&json!({ "name": "sub-1", "task": "T1_CHILD_WORK" }),
		);
	}
	if latest.contains("T1_CHILD_WORK") {
		return tool_call_response(
			"yield",
			&json!({ "result": { "data": { "result_key": "result_val" } } }),
		);
	}

	// T2 routes
	if latest.contains("T2_START") {
		return tool_call_response(
			"task",
			&json!({ "name": "t2-child", "task": "T2_CHILD_WORK" }),
		);
	}
	if latest.contains("T2_CHILD_WORK") {
		return tool_call_response(
			"task",
			&json!({ "name": "t2-grandchild", "task": "T2_GRANDCHILD_WORK" }),
		);
	}
	if latest.contains("T2_GRANDCHILD_WORK") {
		return tool_call_response(
			"task",
			&json!({ "name": "t2-great", "task": "T2_GREAT_WORK" }),
		);
	}

	// T3 routes
	if latest.contains("T3_START") {
		return tool_call_response(
			"task",
			&json!({ "name": "hang-child", "task": "T3_HANG_CHILD" }),
		);
	}

	// T4 routes
	if latest.contains("T4_START") {
		return tool_call_response(
			"task",
			&json!({
				"context": "T4 shared context",
				"tasks": [
					{ "name": "task-A", "task": "T4_WORK_A" },
					{ "name": "task-B", "task": "T4_WORK_B" }
				]
			}),
		);
	}
	if latest.contains("T4_WORK_A") {
		return tool_call_response(
			"yield",
			&json!({ "result": { "data": "output_A" } }),
		);
	}
	if latest.contains("T4_WORK_B") {
		return tool_call_response(
			"yield",
			&json!({ "result": { "data": "output_B" } }),
		);
	}

	text_response("fallback")
}

/// Encode one scripted chat completion as OpenAI SSE events.
///
/// The non-streaming fixtures put content in `message.content` and calls in
/// `message.tool_calls`; streaming clients expect the same data as `delta`
/// frames, with tool arguments still carried as a JSON string.
fn stream_sse_body(response: &Value) -> String {
	let message = &response["choices"][0]["message"];
	let mut events = String::new();
	if let Some(content) = message.get("content").and_then(Value::as_str) {
		let delta = json!({ "choices": [{ "delta": { "content": content } }] });
		events.push_str(&format!("data: {delta}\n\n"));
	}
	if let Some(calls) = message.get("tool_calls").and_then(Value::as_array) {
		let deltas: Vec<Value> = calls
			.iter()
			.enumerate()
			.map(|(index, call)| {
				json!({
					"index": index,
					"id": call.get("id"),
					"function": {
						"name": call["function"]["name"],
						"arguments": call["function"]["arguments"],
					}
				})
			})
			.collect();
		let delta = json!({ "choices": [{ "delta": { "tool_calls": deltas } }] });
		events.push_str(&format!("data: {delta}\n\n"));
	}
	events.push_str("data: [DONE]\n\n");
	events
}

#[tokio::test]
async fn test_t1_happy_delegation() {
	let (server, _) = spawn_mock_server(None).await;
	let provider = Arc::new(
		OpenAiCompatibleProvider::new(&server.base_url, "fake-key", "test-model").unwrap(),
	);

	let supervisor = Supervisor::new(
		ToolRegistry::new(),
		provider.clone(),
		None,
		DelegationLimits::default(),
	);

	let root_registry = supervisor.root_registry();
	let config = AgentConfig {
		max_turns: 5,
		context_limit: 100_000,
		system_prompt: None,
		cwd: std::env::temp_dir(),
	};

	let agent_loop = AgentLoop::new(config, provider, root_registry);
	let mut state = AgentState::default();

	let turn_results = agent_loop
		.run_dialog(
			"T1_START",
			&mut state,
			None,
		)
		.await
		.expect("root dialog run");

	// 1. Root completed
	assert_eq!(turn_results.len(), 2);
	assert!(turn_results.last().unwrap().is_completed);

	// 2. First turn executed task tool
	let first_turn = &turn_results[0];
	assert_eq!(first_turn.tool_results.len(), 1);
	let tool_res = &first_turn.tool_results[0];
	assert_eq!(tool_res.tool_call_id, first_turn.message.tool_calls[0].id);

	let details: TaskToolDetails =
		serde_json::from_str(&tool_res.content).expect("valid TaskToolDetails JSON");
	assert_eq!(details.results.len(), 1);

	let sub_result = &details.results[0];
	assert_eq!(sub_result.index, 0);
	assert_eq!(sub_result.id, "task-1");
	assert_eq!(sub_result.name, "sub-1");
	assert_eq!(sub_result.agent, "task");
	assert_eq!(sub_result.exit_code, 0);

	// 3. extractedToolData has terminal yield payload
	let extracted = sub_result
		.extracted_tool_data
		.as_ref()
		.expect("extracted_tool_data");
	let yields = extracted.get("yield").expect("yield data in extractedToolData");
	assert_eq!(yields.len(), 1);
	assert_eq!(yields[0]["status"], "success");
	assert_eq!(yields[0]["data"]["result_key"], "result_val");

	// 4. Invariant: tool_call -> exactly one tool response message
	let tool_call_count = state
		.messages
		.iter()
		.filter(|m| m.role == Role::Assistant)
		.map(|m| m.tool_calls.len())
		.sum::<usize>();
	let tool_msg_count = state
		.messages
		.iter()
		.filter(|m| m.role == Role::Tool)
		.count();
	assert_eq!(tool_call_count, 1);
	assert_eq!(tool_msg_count, 1);

	// 5. Supervisor child record marked Completed
	let children = supervisor.children();
	assert_eq!(children.len(), 1);
	assert_eq!(children[0].id, "task-1");
	assert_eq!(children[0].name, "sub-1");
	assert_eq!(children[0].status, ChildStatus::Completed);
	assert!(children[0].duration_ms.is_some());
}

fn test_catalog_agent(name: &str) -> AgentDefinition {
	AgentDefinition {
		name: name.to_string(),
		description: format!("{name} test agent"),
		tools: None,
		spawns: Some(Spawns::All),
		model: Vec::new(),
		output: None,
		thinking_level: None,
		blocking: false,
		read_summarize: None,
		prewalk: None,
		advisor: None,
		autoload_skills: Vec::new(),
		system_prompt: String::new(),
		source: AgentSource::Bundled,
		file_path: format!("embedded:{name}.md"),
	}
}

fn saved_output_root(label: &str) -> PathBuf {
	let nanos = SystemTime::now()
		.duration_since(UNIX_EPOCH)
		.map(|duration| duration.as_nanos())
		.unwrap_or_default();
	let home = std::env::var_os("HOME")
		.map(PathBuf::from)
		.expect("HOME must be set for agent output tests");
	let root = home.join(format!(
		".omp-agent-output-it-{label}-{}-{nanos}",
		std::process::id()
	));
	std::fs::create_dir_all(&root).expect("output root");
	root
}

#[tokio::test]
async fn saved_child_output_survives_restart_and_resolves_via_agent_uri() {
	let (server, _) = spawn_mock_server(None).await;
	let provider = Arc::new(
		OpenAiCompatibleProvider::new(&server.base_url, "fake-key", "test-model").unwrap(),
	);
	let root = saved_output_root("restart");
	let subagents = root.join("subagents");
	std::fs::create_dir_all(&subagents).expect("subagents dir");

	let supervisor = Supervisor::new(
		ToolRegistry::new(),
		provider.clone(),
		None,
		DelegationLimits::default(),
	);
	supervisor.set_child_session_root(Some(subagents.clone()));
	let mut root_scope = UrlScope::default();
	root_scope.session_file = Some(root.join("main.jsonl"));
	root_scope.local_roots.insert("local".to_string(), root.clone());
	supervisor.set_url_scope(Arc::new(root_scope));
	let tool = TaskTool::new(supervisor.clone(), 0);
	let ctx = AgentContext::new(root.clone());
	let result = tool
		.execute(&json!({ "name": "Saved", "task": "T1_CHILD_WORK" }), &ctx)
		.await
		.expect("task result");
	let details: TaskToolDetails = serde_json::from_str(&result.content).expect("details");
	let saved = &details.results[0];
	let output_id = saved.output_id.clone().expect("saved output id");
	assert_eq!(output_id, "Saved");
	assert!(saved.id.starts_with("task-"), "lifecycle id stays unchanged");
	let path = root.join(format!("{output_id}.md"));
	let body = std::fs::read_to_string(&path).expect("saved markdown");
	assert!(body.contains("result_val"), "unexpected body: {body}");
	assert!(
		subagents.join(format!("{}.jsonl", saved.id)).is_file(),
		"child transcript stays under <producer>/subagents"
	);
	assert!(
		!subagents.join(format!("{}.init.json", saved.id)).exists(),
		"no lifecycle manager means no contract intent and no metadata failure"
	);
	assert!(
		!subagents.join(format!("{output_id}.md")).exists(),
		"saved output must not be written into the transcript directory"
	);

	let hub = HubTool::root(supervisor.clone());
	let jobs = hub.execute(&json!({ "op": "jobs" }), &ctx).await.expect("jobs");
	let jobs: HubToolDetails = serde_json::from_str(&jobs.content).expect("details");
	assert_eq!(jobs.jobs[0].output_id.as_deref(), Some("Saved"));

	let mut scope = UrlScope::default();
	scope.session_file = Some(root.join("main.jsonl"));
	scope
		.local_roots
		.insert("local".to_string(), root.clone());
	let router = InternalUrlRouter::new();
	let context = scope.resolve_context(std::path::Path::new("."), &pi_agent::CancellationToken::new());
	let resource = router
		.resolve(&format!("agent://{output_id}"), &context)
		.await
		.expect("agent uri");
	assert_eq!(resource.content, body);
	assert!(resource.immutable);
	assert!(!resource.writable);
	let extracted = router
		.resolve(&format!("agent://{output_id}?q=.result_key"), &context)
		.await
		.expect("extraction");
	assert_eq!(extracted.content, "\"result_val\"");

	// A restarted process seeds the allocator from disk: the same requested
	// name must not overwrite the already published output.
	let restarted = Supervisor::new(
		ToolRegistry::new(),
		provider,
		None,
		DelegationLimits::default(),
	);
	restarted.set_child_session_root(Some(subagents.clone()));
	let mut restarted_scope = UrlScope::default();
	restarted_scope.session_file = Some(root.join("main.jsonl"));
	restarted_scope.local_roots.insert("local".to_string(), root.clone());
	restarted.set_url_scope(Arc::new(restarted_scope));
	let restarted_tool = TaskTool::new(restarted, 0);
	let second = restarted_tool
		.execute(&json!({ "name": "Saved", "task": "T1_CHILD_WORK" }), &ctx)
		.await
		.expect("second task result");
	let second: TaskToolDetails = serde_json::from_str(&second.content).expect("details");
	let second_id = second.results[0].output_id.clone().expect("second id");
	assert_eq!(second_id, "Saved-2");
	assert_eq!(std::fs::read_to_string(&path).expect("first intact"), body);

	let aborted = restarted_tool
		.execute(&json!({ "name": "Aborted", "task": "YIELD_ERROR" }), &ctx)
		.await
		.expect("aborted result");
	let aborted: TaskToolDetails = serde_json::from_str(&aborted.content).expect("details");
	let aborted = &aborted.results[0];
	assert_eq!(aborted.aborted, Some(true));
	let aborted_id = aborted.output_id.clone().expect("aborted output id");
	let aborted_body =
		std::fs::read_to_string(root.join(format!("{aborted_id}.md"))).expect("aborted artifact");
	assert!(aborted_body.contains("\"aborted\": true"), "unexpected: {aborted_body}");
	assert!(aborted_body.contains("boom"), "unexpected: {aborted_body}");

	std::fs::remove_dir_all(&root).ok();
}

#[tokio::test]
async fn nested_spawn_writes_dotted_outputs_in_the_producer_root() {
	let (server, _) = spawn_mock_server(None).await;
	let provider = Arc::new(
		OpenAiCompatibleProvider::new(&server.base_url, "fake-key", "test-model").unwrap(),
	);
	let root = saved_output_root("nested");
	let subagents = root.join("subagents");
	std::fs::create_dir_all(&subagents).expect("subagents dir");
	let supervisor = Supervisor::new(
		ToolRegistry::new(),
		provider,
		None,
		// Explicit fixture depth: root -> NestedParent (1) -> NestedChild (2)
		// -> NestedGrand (3). The default limit intentionally stops before the
		// grandchild, so this test must opt into one extra level.
		DelegationLimits {
			max_depth:       3,
			max_concurrency: 32,
		},
	);
	supervisor.set_agent_catalog(Arc::new(AgentCatalog::new(
		vec![test_catalog_agent("task"), test_catalog_agent("scout")],
		None,
	)));
	supervisor.set_child_session_root(Some(subagents.clone()));
	let mut scope = UrlScope::default();
	scope.session_file = Some(root.join("main.jsonl"));
	scope.local_roots.insert("local".to_string(), root.clone());
	supervisor.set_url_scope(Arc::new(scope));
	let tool = TaskTool::new(supervisor, 0);
	let ctx = AgentContext::new(root.clone());
	let result = tool
		.execute(&json!({ "name": "NestedParent", "task": "NESTED_PARENT" }), &ctx)
		.await
		.expect("nested task result");
	let details: TaskToolDetails = serde_json::from_str(&result.content).expect("details");
	let parent_id = details.results[0]
		.output_id
		.clone()
		.expect("parent output id");
	assert_eq!(parent_id, "NestedParent");
	for id in [
		"NestedParent.NestedChild",
		"NestedParent.NestedChild.NestedGrand",
	] {
		let path = root.join(format!("{id}.md"));
		assert!(path.is_file(), "missing nested artifact {}", path.display());
		assert!(
			!subagents.join(format!("{id}.md")).exists(),
			"nested artifact must not live in subagents"
		);
	}
	let child_body = std::fs::read_to_string(root.join("NestedParent.NestedChild.md"))
		.expect("child artifact");
	assert!(child_body.contains("child_done"), "unexpected: {child_body}");

	let mut read_scope = UrlScope::default();
	read_scope.session_file = Some(root.join("main.jsonl"));
	read_scope
		.local_roots
		.insert("local".to_string(), root.clone());
	let router = InternalUrlRouter::new();
	let resource = router
		.resolve(
			"agent://NestedParent/NestedChild",
			&read_scope.resolve_context(
				std::path::Path::new("."),
				&pi_agent::CancellationToken::new(),
			),
		)
		.await
		.expect("nested resolver");
	assert!(resource.content.contains("child_done"), "{}", resource.content);
	std::fs::remove_dir_all(&root).ok();
}

#[tokio::test]
async fn test_t2_depth_limits() {
	let (server, _) = spawn_mock_server(None).await;
	let provider = Arc::new(
		OpenAiCompatibleProvider::new(&server.base_url, "fake-key", "test-model").unwrap(),
	);

	let supervisor = Supervisor::new(
		ToolRegistry::new(),
		provider.clone(),
		None,
		DelegationLimits {
			max_depth: 2,
			max_concurrency: 32,
		},
	);

	// Check registry contents at depths:
	// depth 0 (root): has task
	assert!(supervisor.root_registry().get("task").is_some());
	// depth 1 (child): has task and yield
	let reg1 = supervisor.child_registry(1, Some("child"));
	assert!(reg1.get("task").is_some());
	assert!(reg1.get("yield").is_some());
	// depth 2 (grandchild, depth == max_depth): has yield, but NO task
	let reg2 = supervisor.child_registry(2, Some("grandchild"));
	assert!(reg2.get("task").is_none());
	assert!(reg2.get("yield").is_some());

	// Separate unit check: invoking TaskTool directly at depth == max_depth returns exact TS message
	let unit_task = TaskTool::new(supervisor.clone(), 2);
	let ctx = AgentContext::new(std::env::temp_dir());
	let err = unit_task
		.execute(&json!({ "task": "test preflight" }), &ctx)
		.await
		.unwrap_err();
	let err_str = err.to_string();
	assert!(
		err_str.contains("Cannot spawn another agent at task depth 2; maximum depth is 2."),
		"Unexpected error message: {err_str}"
	);

	// End-to-end dialog test: root -> child (depth 1) -> grandchild (depth 2)
	// Grandchild tries to call task -> returns missing tool diagnostic -> recovers via yield -> finishes
	let root_registry = supervisor.root_registry();
	let config = AgentConfig {
		max_turns: 10,
		context_limit: 100_000,
		system_prompt: None,
		cwd: std::env::temp_dir(),
	};

	let agent_loop = AgentLoop::new(config, provider, root_registry);
	let mut state = AgentState::default();

	let turn_results = agent_loop
		.run_dialog("T2_START", &mut state, None)
		.await
		.expect("root dialog run");

	assert!(turn_results.last().unwrap().is_completed);

	let children = supervisor.children();
	assert!(
		children
			.iter()
			.any(|c| c.name == "t2-child" && c.status == ChildStatus::Completed)
	);
	assert!(
		children
			.iter()
			.any(|c| c.name == "t2-grandchild" && c.status == ChildStatus::Completed)
	);
}

#[tokio::test]
async fn test_t3_cancellation_cleans_up_and_records_aborted() {
	let (mut server, hang_rx) = spawn_mock_server(Some("T3_HANG_CHILD")).await;
	let mut hang_rx = hang_rx.expect("hang signal rx");

	let provider = Arc::new(
		OpenAiCompatibleProvider::new(&server.base_url, "fake-key", "test-model").unwrap(),
	);

	let supervisor = Supervisor::new(
		ToolRegistry::new(),
		provider.clone(),
		None,
		DelegationLimits::default(),
	);

	let root_registry = supervisor.root_registry();
	let config = AgentConfig {
		max_turns: 5,
		context_limit: 100_000,
		system_prompt: None,
		cwd: std::env::temp_dir(),
	};

	let agent_loop = AgentLoop::new(config, provider, root_registry);
	let mut state = AgentState::default();

	// Spawn root dialog as a background tokio task
	let root_task = tokio::spawn(async move {
		agent_loop.run_dialog("T3_START", &mut state, None).await
	});

	// Wait until child connects and reaches mock with T3_HANG_CHILD
	tokio::time::timeout(Duration::from_secs(5), hang_rx.recv())
		.await
		.expect("child reached hang mock")
		.expect("channel valid");

	// Now abort the root task
	root_task.abort();
	let _ = root_task.await;

	// Mock must observe socket disconnect / EOF because ChildGuard drops and aborts the child task
	let hang_closed_rx = server.hang_closed_rx.as_mut().expect("hang_closed_rx");
	let socket_res = tokio::time::timeout(Duration::from_secs(5), hang_closed_rx.recv())
		.await
		.expect("mock detected disconnect within 5s")
		.expect("disconnect payload");

	// Read result on closed connection is Ok(0) (EOF) or an IO error
	match socket_res {
		Ok(0) => {}, // clean EOF
		Ok(_) => {}, // read EOF
		Err(_) => {}, // connection reset
	}

	// Verify child record in supervisor is Aborted with abort_reason
	let children = supervisor.children();
	let child = children
		.iter()
		.find(|c| c.name == "hang-child")
		.expect("hang-child record in supervisor");
	assert_eq!(child.status, ChildStatus::Aborted);
	assert_eq!(child.aborted, Some(true));
	assert_eq!(child.abort_reason.as_deref(), Some("aborted by parent"));
}

#[tokio::test]
async fn test_t4_batch_execution_order() {
	let (server, _) = spawn_mock_server(None).await;
	let provider = Arc::new(
		OpenAiCompatibleProvider::new(&server.base_url, "fake-key", "test-model").unwrap(),
	);

	let supervisor = Supervisor::new(
		ToolRegistry::new(),
		provider.clone(),
		None,
		DelegationLimits::default(),
	);

	let root_registry = supervisor.root_registry();
	let config = AgentConfig {
		max_turns: 5,
		context_limit: 100_000,
		system_prompt: None,
		cwd: std::env::temp_dir(),
	};

	let agent_loop = AgentLoop::new(config, provider, root_registry);
	let mut state = AgentState::default();

	let turn_results = agent_loop
		.run_dialog("T4_START", &mut state, None)
		.await
		.expect("root dialog run");

	assert!(turn_results.last().unwrap().is_completed);

	let first_turn = &turn_results[0];
	let tool_res = &first_turn.tool_results[0];
	let details: TaskToolDetails = serde_json::from_str(&tool_res.content).unwrap();

	// Results must preserve exact input order (index 0, 1)
	assert_eq!(details.results.len(), 2);

	let r0 = &details.results[0];
	assert_eq!(r0.index, 0);
	assert_eq!(r0.id, "task-1");
	assert_eq!(r0.name, "task-A");
	assert_eq!(r0.exit_code, 0);
	assert_eq!(
		r0.extracted_tool_data.as_ref().unwrap()["yield"][0]["data"],
		"output_A"
	);

	let r1 = &details.results[1];
	assert_eq!(r1.index, 1);
	assert_eq!(r1.id, "task-2");
	assert_eq!(r1.name, "task-B");
	assert_eq!(r1.exit_code, 0);
	assert_eq!(
		r1.extracted_tool_data.as_ref().unwrap()["yield"][0]["data"],
		"output_B"
	);

	let children = supervisor.children();
	assert_eq!(children.len(), 2);
	assert!(children.iter().all(|c| c.status == ChildStatus::Completed));
}

#[tokio::test]
async fn test_t5_hub_jobs_snapshot_after_child_completion() {
	let (server, _) = spawn_mock_server(None).await;
	let provider = Arc::new(
		OpenAiCompatibleProvider::new(&server.base_url, "fake-key", "test-model").unwrap(),
	);

	let supervisor = Supervisor::new(
		ToolRegistry::new(),
		provider.clone(),
		None,
		DelegationLimits::default(),
	);

	// Root has no yield; children do.
	assert!(supervisor.root_registry().get("yield").is_none());
	assert!(supervisor.child_registry(1, Some("child")).get("yield").is_some());
	assert!(supervisor.root_registry().get("hub").is_some());

	let root_registry = supervisor.root_registry();
	let config = AgentConfig {
		max_turns: 5,
		context_limit: 100_000,
		system_prompt: None,
		cwd: std::env::temp_dir(),
	};
	let agent_loop = AgentLoop::new(config, provider, root_registry);
	let mut state = AgentState::default();
	let turn_results = agent_loop
		.run_dialog("T1_START", &mut state, None)
		.await
		.expect("root dialog run");
	assert!(turn_results.last().unwrap().is_completed);

	let hub = HubTool::root(supervisor.clone());
	let ctx = AgentContext::new(std::env::temp_dir());
	let snapshot = hub.execute(&json!({ "op": "jobs" }), &ctx).await.expect("jobs");
	assert!(!snapshot.is_error);
	let details: HubToolDetails = serde_json::from_str(&snapshot.content).expect("details");
	assert_eq!(details.op, "jobs");
	assert_eq!(details.jobs.len(), 1);
	assert_eq!(details.jobs[0].id, "task-1");
	assert_eq!(details.jobs[0].name, "sub-1");
	assert_eq!(details.jobs[0].kind, "task");
	assert_eq!(details.jobs[0].status, "completed");
	assert!(
		details.jobs[0]
			.result_text
			.as_deref()
			.is_some_and(|text| text.contains("result_val"))
	);
	assert!(details.jobs[0].label.contains("T1_CHILD_WORK"));

	// Unknown ids are reported per id, not fabricated.
	let unknown = hub
		.execute(&json!({ "op": "cancel", "ids": ["missing", "task-1"] }), &ctx)
		.await
		.expect("cancel");
	let cancel_details: HubToolDetails = serde_json::from_str(&unknown.content).expect("details");
	let cancelled = cancel_details.cancelled.expect("outcomes");
	assert_eq!(cancelled[0].id, "missing");
	assert_eq!(cancelled[0].status, "not_found");
	assert_eq!(cancelled[1].id, "task-1");
	assert_eq!(cancelled[1].status, "already_completed");
	assert_eq!(cancel_details.jobs.len(), 1);
	assert_eq!(cancel_details.jobs[0].status, "completed");

	// Process listing is a real op in this build; with no bound backend it
	// reports an empty list rather than erroring.
	let ps = hub.execute(&json!({ "op": "ps" }), &ctx).await.expect("ps");
	assert!(!ps.is_error, "{}", ps.content);

	// Unknown hub ops are explicitly rejected, not silently ignored.
	let unsupported = hub
		.execute(&json!({ "op": "frobnicate" }), &ctx)
		.await
		.expect("unsupported op result");
	assert!(unsupported.is_error);
	assert!(unsupported.content.contains("not supported"));
	assert!(unsupported.content.contains("jobs, cancel"));
}

#[tokio::test]
async fn test_t6_hub_cancel_aborts_running_child_and_scopes_owner() {
	let (server, hang_rx) = spawn_mock_server(Some("HUB_HANG")).await;
	let mut hang_rx = hang_rx.expect("hang signal rx");
	let provider = Arc::new(
		OpenAiCompatibleProvider::new(&server.base_url, "fake-key", "test-model").unwrap(),
	);

	let supervisor = Supervisor::new(
		ToolRegistry::new(),
		provider.clone(),
		None,
		DelegationLimits::default(),
	);

	let task = TaskTool::new(supervisor.clone(), 0);
	let ctx = AgentContext::new(std::env::temp_dir());
	let handle = tokio::spawn(async move {
		task
			.execute(&json!({ "name": "hang-cancel", "task": "HUB_HANG" }), &ctx)
			.await
	});

	tokio::time::timeout(Duration::from_secs(5), hang_rx.recv())
		.await
		.expect("child reached hang mock")
		.expect("hang channel");

	// A later turn rebinds the supervisor to its own registry/approval gate.
	// The in-flight child must keep its abort handle and record.
	supervisor.rebind(
		ToolRegistry::new(),
		provider,
		None,
		DelegationLimits::default(),
		true,
		true,
	);

	// A child-owned hub cannot cancel the root's child. Capture the generated
	// identity (names are only labels and cannot address a child).
	let child_id = supervisor
		.children()
		.into_iter()
		.find(|record| record.name == "hang-cancel")
		.expect("running child record")
		.id;
	let child_hub = HubTool::with_owner(supervisor.clone(), Some("not-the-parent".to_string()));
	let ctx = AgentContext::new(std::env::temp_dir());
	let foreign = child_hub
		.execute(&json!({ "op": "cancel", "ids": [child_id.clone()] }), &ctx)
		.await
		.expect("foreign cancel");
	let foreign_details: HubToolDetails = serde_json::from_str(&foreign.content).expect("details");
	assert_eq!(foreign_details.cancelled.unwrap()[0].status, "not_found");

	let hub = HubTool::root(supervisor.clone());
	let result = hub
		.execute(&json!({ "op": "cancel", "ids": [child_id] }), &ctx)
		.await
		.expect("cancel");
	let details: HubToolDetails = serde_json::from_str(&result.content).expect("details");
	assert_eq!(details.cancelled.as_ref().unwrap()[0].status, "cancelled");
	assert_eq!(details.jobs[0].status, "cancelled");
	assert_eq!(details.jobs[0].error_text.as_deref(), Some("cancelled by hub"));

	let task_result = tokio::time::timeout(Duration::from_secs(5), handle)
		.await
		.expect("task returned after cancel")
		.expect("task join")
		.expect("task execution");
	let task_details: TaskToolDetails =
		serde_json::from_str(&task_result.content).expect("task details");
	assert_eq!(task_details.results[0].exit_code, 1);
	assert_eq!(task_details.results[0].aborted, Some(true));

	let record = supervisor
		.children()
		.into_iter()
		.find(|record| record.name == "hang-cancel")
		.expect("child record");
	assert_eq!(record.status, ChildStatus::Aborted);
	assert_eq!(record.abort_reason.as_deref(), Some("cancelled by hub"));
}

#[tokio::test]
async fn test_t7_hub_cancel_queued_child_never_starts() {
	let (server, hang_rx) = spawn_mock_server(Some("HUB_QUEUE")).await;
	let mut hang_rx = hang_rx.expect("hang signal rx");
	let provider = Arc::new(
		OpenAiCompatibleProvider::new(&server.base_url, "fake-key", "test-model").unwrap(),
	);

	let supervisor = Supervisor::new(
		ToolRegistry::new(),
		provider.clone(),
		None,
		DelegationLimits::default(),
	);
	// One permit: the second reservation waits in the semaphore queue.
	let task = TaskTool::with_semaphore(supervisor.clone(), 0, Arc::new(Semaphore::new(1)));
	let ctx = AgentContext::new(std::env::temp_dir());
	let handle = tokio::spawn(async move {
		task
			.execute(
				&json!({ "tasks": [
					{ "name": "first", "task": "HUB_QUEUE" },
					{ "name": "queued", "task": "HUB_QUEUE" }
				] }),
				&ctx,
			)
			.await
	});

	tokio::time::timeout(Duration::from_secs(5), hang_rx.recv())
		.await
		.expect("first child reached hang mock")
		.expect("hang channel");

	let queued = supervisor
		.children()
		.into_iter()
		.find(|record| record.name == "queued")
		.expect("queued reservation");
	assert_eq!(queued.status, ChildStatus::Running);
	let queued_id = queued.id;
	let first_id = supervisor
		.children()
		.into_iter()
		.find(|record| record.name == "first")
		.expect("first child")
		.id;

	let hub = HubTool::root(supervisor.clone());
	let nav_ctx = AgentContext::new(std::env::temp_dir());
	let queued_cancel = hub
		.execute(&json!({ "op": "cancel", "ids": [queued_id.clone()] }), &nav_ctx)
		.await
		.expect("cancel queued");
	let queued_details: HubToolDetails =
		serde_json::from_str(&queued_cancel.content).expect("details");
	assert_eq!(queued_details.cancelled.as_ref().unwrap()[0].status, "cancelled");
	let record = supervisor
		.children()
		.into_iter()
		.find(|record| record.name == "queued")
		.expect("queued record");
	assert_eq!(record.status, ChildStatus::Aborted);
	assert_eq!(record.abort_reason.as_deref(), Some("cancelled by hub"));

	// Finishing the running child must not start the cancelled reservation.
	let first_cancel = hub
		.execute(&json!({ "op": "cancel", "ids": [first_id] }), &nav_ctx)
		.await
		.expect("cancel first");
	assert!(!first_cancel.is_error);

	let task_result = tokio::time::timeout(Duration::from_secs(10), handle)
		.await
		.expect("batch returned")
		.expect("task join")
		.expect("task execution");
	let details: TaskToolDetails = serde_json::from_str(&task_result.content).expect("details");
	let queued_result = details
		.results
		.iter()
		.find(|result| result.id == queued_id)
		.expect("queued result");
	assert_eq!(queued_result.name, "queued");
	assert_eq!(queued_result.aborted, Some(true));
	assert_eq!(queued_result.abort_reason.as_deref(), Some("cancelled by hub"));
	assert!(
		hang_rx.try_recv().is_err(),
		"cancelled reservation must never reach the model"
	);
}

#[tokio::test]
async fn test_t8_yield_error_reports_failed_not_cancelled() {
	let (server, _hang_rx) = spawn_mock_server(None).await;
	let provider = Arc::new(
		OpenAiCompatibleProvider::new(&server.base_url, "fake-key", "test-model").unwrap(),
	);
	let supervisor = Supervisor::new(
		ToolRegistry::new(),
		provider.clone(),
		None,
		DelegationLimits::default(),
	);
	let task = TaskTool::new(supervisor.clone(), 0);
	let ctx = AgentContext::new(std::env::temp_dir());
	let result = task
		.execute(&json!({ "name": "yield-fail", "task": "YIELD_ERROR" }), &ctx)
		.await
		.expect("task result");
	let details: TaskToolDetails = serde_json::from_str(&result.content).expect("details");
	assert_eq!(details.results[0].aborted, Some(true));
	assert_eq!(details.results[0].abort_reason.as_deref(), Some("boom"));

	let hub = HubTool::root(supervisor.clone());
	let jobs = hub
		.execute(&json!({ "op": "jobs" }), &ctx)
		.await
		.expect("jobs");
	let jobs: HubToolDetails = serde_json::from_str(&jobs.content).expect("details");
	assert_eq!(jobs.jobs[0].status, "failed");
	assert_eq!(jobs.jobs[0].error_text.as_deref(), Some("boom"));
}

#[tokio::test]
async fn test_t9_dropping_child_future_kills_its_shell_tree() {
	let (server, _hang_rx) = spawn_mock_server(None).await;
	let provider = Arc::new(
		OpenAiCompatibleProvider::new(&server.base_url, "fake-key", "test-model").unwrap(),
	);
	let dir = std::env::temp_dir().join(format!(
		"omp-delegation-drop-{}-{}",
		std::process::id(),
		std::time::SystemTime::now()
			.duration_since(std::time::UNIX_EPOCH)
			.map(|d| d.as_nanos())
			.unwrap_or_default()
	));
	std::fs::create_dir_all(&dir).expect("temp dir");
	let pid_file = dir.join("pid.txt");
	let marker = dir.join("marker.txt");

	let supervisor = Supervisor::new(
		ToolRegistry::with_builtins(),
		provider,
		None,
		DelegationLimits::default(),
	);
	let task = TaskTool::new(supervisor.clone(), 0);
	let ctx = AgentContext::new(dir.clone());
	let handle = tokio::spawn(async move {
		task
			.execute(&json!({ "name": "bash-drop", "task": "BASH_DROP" }), &ctx)
			.await
	});

	let mut pid = None;
	for _ in 0..60 {
		pid = std::fs::read_to_string(&pid_file)
			.ok()
			.and_then(|text| text.trim().parse::<u32>().ok());
		if pid.is_some() {
			break;
		}
		tokio::time::sleep(Duration::from_millis(50)).await;
	}
	let pid = pid.expect("child shell must start");

	// Drop the parent tool future without hub/cooperative cancellation.
	handle.abort();
	let _ = handle.await;

	let mut gone = false;
	for _ in 0..60 {
		let alive = std::fs::read_to_string(format!("/proc/{pid}/stat"))
			.ok()
			.map(|stat| {
				let state = stat.rsplit(')').next().and_then(|rest| rest.split_whitespace().next());
				state != Some("Z")
			})
			.unwrap_or_else(|| {
				std::process::Command::new("kill")
					.arg("-0")
					.arg(pid.to_string())
					.status()
					.is_ok_and(|status| status.success())
			});
		if !alive {
			gone = true;
			break;
		}
		tokio::time::sleep(Duration::from_millis(50)).await;
	}
	assert!(gone, "child shell {pid} survived parent drop");

	tokio::time::sleep(Duration::from_millis(1200)).await;
	assert!(!marker.exists(), "abandoned child shell executed its tail");
	assert_eq!(
		supervisor
			.children()
			.into_iter()
			.find(|record| record.name == "bash-drop")
			.expect("record")
			.status,
		ChildStatus::Aborted
	);
	std::fs::remove_dir_all(dir).ok();
}

#[tokio::test]
async fn test_t10_dropping_batch_settles_queued_reservations() {
	let (server, hang_rx) = spawn_mock_server(Some("DROP_QUEUE")).await;
	let mut hang_rx = hang_rx.expect("hang signal rx");
	let provider = Arc::new(
		OpenAiCompatibleProvider::new(&server.base_url, "fake-key", "test-model").unwrap(),
	);
	let supervisor = Supervisor::new(
		ToolRegistry::new(),
		provider,
		None,
		DelegationLimits::default(),
	);
	let task = TaskTool::with_semaphore(supervisor.clone(), 0, Arc::new(Semaphore::new(1)));
	let ctx = AgentContext::new(std::env::temp_dir());
	let handle = tokio::spawn(async move {
		task
			.execute(
				&json!({ "tasks": [
					{ "name": "first", "task": "DROP_QUEUE" },
					{ "name": "queued", "task": "DROP_QUEUE" }
				] }),
				&ctx,
			)
			.await
	});
	tokio::time::timeout(Duration::from_secs(5), hang_rx.recv())
		.await
		.expect("first child reached hang mock")
		.expect("hang channel");

	let queued_id = supervisor
		.children()
		.into_iter()
		.find(|record| record.name == "queued")
		.expect("queued reservation")
		.id;

	// Drop the whole batch while the second child is still queued.
	handle.abort();
	let _ = handle.await;

	let records = supervisor.children();
	assert!(
		records.iter().all(|record| record.status != ChildStatus::Running),
		"dropped batch left running records: {records:?}"
	);
	assert_eq!(
		records
			.iter()
			.find(|record| record.name == "queued")
			.expect("queued record")
			.status,
		ChildStatus::Aborted
	);

	// The reservation must already be settled: no five-second cancel wait.
	let hub = HubTool::root(supervisor);
	let ctx = AgentContext::new(std::env::temp_dir());
	let cancel = tokio::time::timeout(
		Duration::from_millis(500),
		hub.execute(&json!({ "op": "cancel", "ids": [queued_id] }), &ctx),
	)
	.await
	.expect("queued cancel must not wait for the settle timeout")
	.expect("cancel result");
	let details: HubToolDetails = serde_json::from_str(&cancel.content).expect("details");
	assert_eq!(details.cancelled.as_ref().unwrap()[0].status, "already_completed");
}
