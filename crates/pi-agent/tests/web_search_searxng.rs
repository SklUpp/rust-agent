//! Protocol contract for the SearXNG-backed `web_search` tool.
//!
//! A loopback HTTP server stands in for the configured instance, so every
//! assertion is on consumer-visible tool output, the wire request, or the
//! failure a caller sees — never on source shape or a mock echo.

use std::time::Duration;

use pi_agent::{
	AgentContext, Tool, ToolError,
	tools::{SearxngConfig, WebSearchTool},
};
use serde_json::json;
use tokio::{
	io::{AsyncReadExt, AsyncWriteExt},
	net::TcpListener,
	sync::mpsc,
};
use tokio_util::sync::CancellationToken;

const CANNED: &str = r#"{
	"query": "cats & dogs",
	"results": [
		{
			"title": "Async Rust",
			"url": "https://example.org/async",
			"content": "  Cancellation is cooperative.  ",
			"publishedDate": "2020-01-01T00:00:00Z"
		},
		{
			"title": "",
			"url": "https://example.org/untitled",
			"snippet": "Fallback title and snippet"
		},
		{
			"title": "Broken row without a url",
			"content": "dropped: no url field"
		}
	],
	"suggestions": ["rust cancellation patterns", "tokio select"],
	"answers": ["Cooperative cancellation via tokens."]
}"#;

fn http_json(body: &str) -> String {
	format!(
		"HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: \
		 close\r\n\r\n{body}",
		body.len()
	)
}

fn http_status(status: &str, extra_headers: &str, body: &str) -> String {
	format!(
		"HTTP/1.1 {status}\r\ncontent-type: text/html; charset=utf-8\r\n{extra_headers}content-length: \
		 {}\r\nconnection: close\r\n\r\n{body}",
		body.len()
	)
}

/// DuckDuckGo Lite fixture: a `uddg=` redirect with entities, a `javascript:`
/// row that must be dropped, and a protocol-relative row that gains `https:`.
const DDG_HTML: &str = r#"<html><body>
<a href="/l/?uddg=https%3A%2F%2Fexample.org%2Ffirst%3Fa%3D1%26b%3D2" class='result-link'>First &amp; Best</a>
<td class='result-snippet'>Snippet &lt;b&gt;one&lt;/b&gt; &amp; more</td>
<a href="javascript:alert(1)" class='result-link'>Danger</a>
<td class='result-snippet'>dropped</td>
<a href="//example.net/proto" class='result-link'>Proto</a>
<td class='result-snippet'>proto relative</td>
</body></html>"#;

/// First row has no snippet; the only snippet belongs to the second row.
const DDG_SPARSE_HTML: &str = r#"<html><body>
<a href='https://example.org/one' class='result-link'>One</a>
<a href='https://example.org/two' class='result-link'>Two</a>
<td class='result-snippet'>two snippet</td>
</body></html>"#;

/// Spawn a loopback server answering `responses` in order, one connection each.
/// Every received request head is streamed to the caller as soon as it lands, so
/// an assertion never waits for a deliberately stalled connection. `None` keeps
/// that connection open without answering, so a cancelled request never
/// completes.
async fn loopback(
	responses: Vec<Option<String>>,
) -> (String, mpsc::UnboundedReceiver<String>, tokio::task::JoinHandle<()>) {
	let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind loopback");
	let address = listener.local_addr().expect("loopback address");
	let (capture, captured) = mpsc::unbounded_channel();
	let handle = tokio::spawn(async move {
		for response in responses {
			let Ok((mut socket, _)) = listener.accept().await else {
				break;
			};
			let mut buffer = Vec::new();
			let mut chunk = [0u8; 1024];
			let mut expected = None;
			loop {
				match socket.read(&mut chunk).await {
					Ok(0) | Err(_) => break,
					Ok(read) => buffer.extend_from_slice(&chunk[..read]),
				}
				if expected.is_none()
					&& let Some(end) = buffer.windows(4).position(|window| window == b"\r\n\r\n")
				{
					let head = String::from_utf8_lossy(&buffer[..end]);
					let length = head
						.lines()
						.filter_map(|line| line.split_once(':'))
						.find(|(key, _)| key.trim().eq_ignore_ascii_case("content-length"))
						.and_then(|(_, value)| value.trim().parse::<usize>().ok())
						.unwrap_or(0);
					expected = Some(end + 4 + length);
				}
				if expected.is_some_and(|expected| buffer.len() >= expected) {
					break;
				}
			}
			let head = String::from_utf8_lossy(&buffer).into_owned();
			let _ = capture.send(head);
			match response {
				Some(response) => {
					let _ = socket.write_all(response.as_bytes()).await;
					let _ = socket.flush().await;
				},
				None => tokio::time::sleep(Duration::from_secs(5)).await,
			}
		}
	});
	(format!("http://{address}"), captured, handle)
}

fn request_target(head: &str) -> String {
	head.lines()
		.next()
		.expect("request line")
		.split_whitespace()
		.nth(1)
		.expect("request target")
		.to_string()
}

fn query_pairs(head: &str) -> Vec<(String, String)> {
	let url = reqwest::Url::parse(&format!("http://localhost{}", request_target(head)))
		.expect("request target parses");
	url.query_pairs().map(|(key, value)| (key.into_owned(), value.into_owned())).collect()
}

fn query_value(pairs: &[(String, String)], key: &str) -> Option<String> {
	pairs
		.iter()
		.find(|(name, _)| name == key)
		.map(|(_, value)| value.clone())
}

fn header(head: &str, name: &str) -> Option<String> {
	head.lines().find_map(|line| {
		let (key, value) = line.split_once(':')?;
		key.trim().eq_ignore_ascii_case(name).then(|| value.trim().to_string())
	})
}

fn configured(endpoint: String) -> SearxngConfig {
	SearxngConfig {
		endpoint: Some(endpoint),
		token: Some("bearer-secret".to_string()),
		basic_username: Some("user".to_string()),
		basic_password: Some("pass-secret".to_string()),
		..SearxngConfig::default()
	}
}

#[tokio::test]
async fn returns_filtered_sources_with_basic_auth_and_search_params() {
	let (endpoint, mut captured, server) = loopback(vec![Some(http_json(CANNED))]).await;
	let tool = WebSearchTool::new(configured(endpoint));
	let context = AgentContext::new(std::env::temp_dir());
	let result = tool
		.execute(
			&json!({ "query": "cats & dogs !!g", "recency": "week", "limit": 3 }),
			&context,
		)
		.await
		.expect("search succeeds");
	let head = captured.recv().await.expect("request captured");
	server.abort();
	assert!(request_target(&head).starts_with("/search?"), "{head}");
	let params = query_pairs(&head);
	assert_eq!(
		query_value(&params, "q").as_deref(),
		Some("cats & dogs"),
		"external bang stripped, query decoded: {params:?}"
	);
	assert_eq!(query_value(&params, "format").as_deref(), Some("json"));
	assert_eq!(query_value(&params, "pageno").as_deref(), Some("1"));
	assert_eq!(
		query_value(&params, "time_range").as_deref(),
		Some("month"),
		"upstream maps week to month"
	);
	assert_eq!(
		header(&head, "authorization").as_deref(),
		Some("Basic dXNlcjpwYXNzLXNlY3JldA=="),
		"Basic credentials take precedence over the bearer token"
	);

	let content = &result.content;
	assert!(content.starts_with("Cooperative cancellation via tokens."), "{content}");
	assert!(content.contains("\n## Sources\n2 sources\n"), "{content}");
	assert!(content.contains("[1] Async Rust ("), "{content}");
	assert!(
		content.contains(" ago)\n    https://example.org/async"),
		"published date renders as a human age: {content}"
	);
	assert!(content.contains("    Cancellation is cooperative."), "{content}");
	assert!(
		content.contains("[2] https://example.org/untitled\n    https://example.org/untitled"),
		"a titleless row falls back to its url: {content}"
	);
	assert!(content.contains("    Fallback title and snippet"), "{content}");
	assert!(!content.contains("Broken row"), "rows without a url are dropped: {content}");
	assert!(content.contains("2 questions"), "{content}");
	assert!(
		!content.contains("bearer-secret") && !content.contains("pass-secret"),
		"credentials never reach the model: {content}"
	);
	assert_eq!(result.details.as_ref().unwrap()["backend"], "searxng");
}

#[tokio::test]
async fn resolves_engine_shortcuts_through_the_config_endpoint() {
	let engines = r#"{"engines":[{"name":"Google","shortcut":"g"},{"name":"DuckDuckGo","shortcut":"ddg"}]}"#;
	let (endpoint, mut captured, server) =
		loopback(vec![Some(http_json(engines)), Some(http_json(CANNED))]).await;
	let tool = WebSearchTool::new(SearxngConfig {
		endpoint: Some(endpoint),
		engines: Some("g,ddg,unknown".to_string()),
		..SearxngConfig::default()
	});
	let context = AgentContext::new(std::env::temp_dir());
	tool.execute(&json!({ "query": "rust" }), &context)
		.await
		.expect("search succeeds after engine resolution");
	let config_head = captured.recv().await.expect("engine config requested");
	let search_head = captured.recv().await.expect("search requested");
	server.abort();
	assert!(
		request_target(&config_head).ends_with("/config"),
		"engine shortcuts resolve through /config: {config_head}"
	);
	assert!(request_target(&search_head).starts_with("/search?"), "{search_head}");
	let params = query_pairs(&search_head);
	assert_eq!(
		query_value(&params, "engines").as_deref(),
		Some("Google,DuckDuckGo,unknown"),
		"shortcuts resolve to canonical names, unknown entries pass through: {params:?}"
	);
}

#[tokio::test]
async fn cancellation_aborts_instead_of_returning_a_partial_answer() {
	let (endpoint, mut captured, server) = loopback(vec![None]).await;
	let tool = WebSearchTool::new(configured(endpoint));
	let token = CancellationToken::new();
	let context = AgentContext::with_cancellation(std::env::temp_dir(), token.clone());
	tokio::spawn(async move {
		tokio::time::sleep(Duration::from_millis(150)).await;
		token.cancel();
	});

	let error = tokio::time::timeout(
		Duration::from_secs(3),
		tool.execute(&json!({ "query": "slow" }), &context),
	)
	.await
	.expect("cancellation settles the request")
	.expect_err("a cancelled search fails");
	let arrived = captured.recv().await.is_some();
	server.abort();
	assert!(matches!(&error, ToolError::Aborted(_)), "{error}");
	assert!(arrived, "the request reached the configured endpoint");
}

#[tokio::test]
async fn unconfigured_searxng_falls_back_to_duckduckgo_lite() {
	let (endpoint, mut captured, server) = loopback(vec![Some(http_status("200 OK", "", DDG_HTML))]).await;
	let tool = WebSearchTool::new(SearxngConfig::default()).with_fallback_endpoint(endpoint);
	let context = AgentContext::new(std::env::temp_dir());
	let result = tool
		.execute(&json!({ "query": "cats & dogs", "limit": 2, "recency": "week" }), &context)
		.await
		.expect("unconfigured SearXNG uses the free backend");
	let head = captured.recv().await.expect("fallback request captured");
	server.abort();
	assert!(head.starts_with("POST "), "{head}");
	assert_eq!(header(&head, "accept").as_deref(), Some("text/html"));
	assert!(
		header(&head, "user-agent").is_some_and(|agent| agent.contains("Mozilla/5.0")),
		"browser UA is required by the lite endpoint: {head}"
	);
	assert_eq!(
		header(&head, "content-type").as_deref(),
		Some("application/x-www-form-urlencoded"),
		"{head}"
	);
	assert!(head.ends_with("q=cats+%26+dogs"), "form query on the wire: {head}");

	let content = &result.content;
	assert!(content.contains("SearXNG endpoint not configured"), "{content}");
	assert!(content.contains("does not filter by recency"), "{content}");
	assert!(
		content.contains("[1] First & Best\n    https://example.org/first?a=1&b=2"),
		"uddg unwrapped, entities decoded: {content}"
	);
	assert!(content.contains("Snippet one & more"), "snippet entities/tags normalized: {content}");
	assert!(
		content.contains("[2] Proto\n    https://example.net/proto"),
		"protocol-relative URL normalized: {content}"
	);
	assert!(!content.contains("javascript:"), "non-http URLs are dropped: {content}");
	assert!(!content.contains("Danger"), "dropped rows never render: {content}");
	assert_eq!(result.details.as_ref().unwrap()["backend"], "duckduckgo_free");
}

#[tokio::test]
async fn searxng_5xx_falls_back_to_duckduckgo_lite() {
	let (searxng, mut searxng_captured, searxng_server) =
		loopback(vec![Some(http_status("503 Service Unavailable", "", "down"))]).await;
	let (endpoint, mut captured, server) = loopback(vec![Some(http_status("200 OK", "", DDG_HTML))]).await;
	let tool = WebSearchTool::new(configured(searxng)).with_fallback_endpoint(endpoint);
	let context = AgentContext::new(std::env::temp_dir());
	let result = tool
		.execute(&json!({ "query": "rust" }), &context)
		.await
		.expect("5xx falls back");
	let searxng_head = searxng_captured.recv().await.expect("SearXNG was attempted");
	let fallback_head = captured.recv().await.expect("fallback requested");
	searxng_server.abort();
	server.abort();
	assert!(query_pairs(&searxng_head).iter().any(|(key, _)| key == "q"), "{searxng_head}");
	assert!(fallback_head.starts_with("POST "), "{fallback_head}");
	assert!(result.content.contains("SearXNG was unavailable"), "{}", result.content);
	assert!(result.content.contains("First & Best"), "{}", result.content);
	assert_eq!(result.details.as_ref().unwrap()["backend"], "duckduckgo_free");
}

#[tokio::test]
async fn searxng_unresponsive_engines_fall_back_to_duckduckgo_lite() {
	let failed = r#"{"results":[],"unresponsive_engines":[["google","timeout"]]}"#;
	let (searxng, _searxng_captured, searxng_server) = loopback(vec![Some(http_json(failed))]).await;
	let (endpoint, mut captured, server) = loopback(vec![Some(http_status("200 OK", "", DDG_HTML))]).await;
	let tool = WebSearchTool::new(configured(searxng)).with_fallback_endpoint(endpoint);
	let context = AgentContext::new(std::env::temp_dir());
	let result = tool
		.execute(&json!({ "query": "rust" }), &context)
		.await
		.expect("a locatable engine failure falls back");
	let fallback_head = captured.recv().await.expect("fallback requested");
	searxng_server.abort();
	server.abort();
	assert!(fallback_head.starts_with("POST "), "{fallback_head}");
	assert!(result.content.contains("SearXNG was unavailable"), "{}", result.content);
	assert_eq!(result.details.as_ref().unwrap()["backend"], "duckduckgo_free");
}

#[tokio::test]
async fn searxng_connection_refused_falls_back_to_duckduckgo_lite() {
	// Binding and dropping reserves an address that now refuses connections.
	let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
	let address = listener.local_addr().expect("address");
	drop(listener);
	let (endpoint, mut captured, server) = loopback(vec![Some(http_status("200 OK", "", DDG_HTML))]).await;
	let tool = WebSearchTool::new(configured(format!("http://{address}")))
		.with_fallback_endpoint(endpoint);
	let context = AgentContext::new(std::env::temp_dir());
	let result = tool
		.execute(&json!({ "query": "rust" }), &context)
		.await
		.expect("transport failure falls back");
	let fallback_head = captured.recv().await.expect("fallback requested");
	server.abort();
	assert!(fallback_head.starts_with("POST "), "{fallback_head}");
	assert!(result.content.contains("First & Best"), "{}", result.content);
	assert_eq!(result.details.as_ref().unwrap()["backend"], "duckduckgo_free");
}

#[tokio::test]
async fn invalid_searxng_config_never_falls_back() {
	let (endpoint, mut captured, server) = loopback(vec![Some(http_status("200 OK", "", DDG_HTML))]).await;
	let context = AgentContext::new(std::env::temp_dir());

	let bad_scheme = WebSearchTool::new(SearxngConfig {
		endpoint: Some("ftp://127.0.0.1:1".to_string()),
		..SearxngConfig::default()
	})
	.with_fallback_endpoint(endpoint.clone());
	let error = bad_scheme
		.execute(&json!({ "query": "rust" }), &context)
		.await
		.expect_err("invalid scheme is a configuration error");
	assert!(format!("{error}").contains("must use the http or https scheme"), "{error}");

	let half_basic = WebSearchTool::new(SearxngConfig {
		endpoint: Some("http://127.0.0.1:1".to_string()),
		basic_username: Some("user".to_string()),
		..SearxngConfig::default()
	})
	.with_fallback_endpoint(endpoint);
	let error = half_basic
		.execute(&json!({ "query": "rust" }), &context)
		.await
		.expect_err("half-configured Basic auth is a configuration error");
	assert!(format!("{error}").contains("requires both"), "{error}");

	server.abort();
	assert!(captured.try_recv().is_err(), "configuration errors must not reach the fallback");
}

#[tokio::test]
async fn searxng_4xx_and_invalid_bearer_fail_fast() {
	let (searxng, _searxng_captured, searxng_server) = loopback(vec![
		Some(http_status("401 Unauthorized", "", r#"{"error":"upstream engines failed"}"#)),
		Some(http_status("403 Forbidden", "", "forbidden")),
		Some(http_status("400 Bad Request", "", "upstream engines failed")),
	])
	.await;
	let (endpoint, mut captured, server) = loopback(vec![Some(http_status("200 OK", "", DDG_HTML))]).await;
	let context = AgentContext::new(std::env::temp_dir());

	let unauthorized = WebSearchTool::new(configured(searxng.clone()))
		.with_fallback_endpoint(endpoint.clone());
	let error = unauthorized
		.execute(&json!({ "query": "rust" }), &context)
		.await
		.expect_err("401 is fatal");
	assert!(format!("{error}").contains("401"), "{error}");

	let forbidden = WebSearchTool::new(configured(searxng.clone()))
		.with_fallback_endpoint(endpoint.clone());
	let error = forbidden
		.execute(&json!({ "query": "rust" }), &context)
		.await
		.expect_err("403 is fatal");
	assert!(format!("{error}").contains("403"), "{error}");

	let bad_request = WebSearchTool::new(configured(searxng)).with_fallback_endpoint(endpoint.clone());
	let error = bad_request
		.execute(&json!({ "query": "rust" }), &context)
		.await
		.expect_err("a 4xx body mentioning failed engines stays fatal");
	let message = format!("{error}");
	assert!(
		message.contains("400") && message.contains("upstream engines failed"),
		"4xx body is rendered as the API error, not a fallback trigger: {message}"
	);

	let invalid_bearer = WebSearchTool::new(SearxngConfig {
		endpoint: Some("http://127.0.0.1:1".to_string()),
		token: Some("bad\ntoken".to_string()),
		..SearxngConfig::default()
	})
	.with_fallback_endpoint(endpoint);
	let error = invalid_bearer
		.execute(&json!({ "query": "rust" }), &context)
		.await
		.expect_err("an invalid Bearer header is a configuration error");
	assert!(
		format!("{error}").contains("not a valid HTTP Authorization header value"),
		"{error}"
	);

	searxng_server.abort();
	server.abort();
	assert!(captured.try_recv().is_err(), "4xx and config errors must not contact DuckDuckGo");
}

#[tokio::test]
async fn searxng_4xx_oversized_body_fails_fast() {
	let body = "x".repeat(4 * 1024 * 1024 + 1);
	let (searxng, _searxng_captured, searxng_server) =
		loopback(vec![Some(http_status("400 Bad Request", "", &body))]).await;
	let (endpoint, mut captured, server) = loopback(vec![Some(http_status("200 OK", "", DDG_HTML))]).await;
	let tool = WebSearchTool::new(configured(searxng)).with_fallback_endpoint(endpoint);
	let context = AgentContext::new(std::env::temp_dir());
	let error = tool
		.execute(&json!({ "query": "rust" }), &context)
		.await
		.expect_err("an oversized 4xx body is fatal");
	searxng_server.abort();
	server.abort();
	assert!(format!("{error}").contains("exceeded 4194304 bytes"), "{error}");
	assert!(captured.try_recv().is_err(), "an oversized 4xx body must not fall back");
}

#[tokio::test]
async fn control_only_endpoint_is_not_treated_as_unconfigured() {
	let (endpoint, mut captured, server) = loopback(vec![Some(http_status("200 OK", "", DDG_HTML))]).await;
	let tool = WebSearchTool::new(SearxngConfig {
		endpoint: Some("\n".to_string()),
		..SearxngConfig::default()
	})
	.with_fallback_endpoint(endpoint);
	let context = AgentContext::new(std::env::temp_dir());
	let error = tool
		.execute(&json!({ "query": "rust" }), &context)
		.await
		.expect_err("control-only endpoints are configuration errors");
	server.abort();
	assert!(format!("{error}").contains("control characters"), "{error}");
	assert!(captured.try_recv().is_err(), "a control-only endpoint must not reach the fallback");
}

#[tokio::test]
async fn duckduckgo_keeps_snippets_with_their_own_rows() {
	let (endpoint, _captured, server) =
		loopback(vec![Some(http_status("200 OK", "", DDG_SPARSE_HTML))]).await;
	let tool = WebSearchTool::new(SearxngConfig::default()).with_fallback_endpoint(endpoint);
	let context = AgentContext::new(std::env::temp_dir());
	let result = tool
		.execute(&json!({ "query": "rust" }), &context)
		.await
		.expect("sparse snippets parse");
	server.abort();
	let content = &result.content;
	assert!(
		content.contains(
			"[1] One\n    https://example.org/one\n[2] Two\n    https://example.org/two\n    two snippet"
		),
		"the second row keeps its own snippet: {content}"
	);
	assert!(
		!content.contains("[1] One\n    https://example.org/one\n    two snippet"),
		"a missing first snippet must not steal the second row's snippet: {content}"
	);
}

#[tokio::test]
async fn duckduckgo_applies_lenient_constraints_with_a_note() {
	let (endpoint, _captured, server) = loopback(vec![Some(http_status("200 OK", "", DDG_HTML))]).await;
	let tool = WebSearchTool::new(SearxngConfig::default()).with_fallback_endpoint(endpoint);
	let context = AgentContext::new(std::env::temp_dir());
	let result = tool
		.execute(&json!({ "query": "site:example.com/docs rust" }), &context)
		.await
		.expect("an unmatchable constraint is relaxed, not fatal");
	server.abort();
	let content = &result.content;
	assert!(
		content.contains("no results matched `site:example.com/docs`; the constraint was relaxed"),
		"{content}"
	);
	assert!(content.contains("First & Best") && content.contains("Proto"), "{content}");
}

#[tokio::test]
async fn duckduckgo_body_is_capped_at_256_kib() {
	let mut html = String::from("<a href='https://example.org/early' class='result-link'>Early</a>");
	html.push_str(&"x".repeat(300 * 1024));
	html.push_str("<a href='https://example.org/late' class='result-link'>Late</a>");
	let (endpoint, _captured, server) = loopback(vec![Some(http_status("200 OK", "", &html))]).await;
	let tool = WebSearchTool::new(SearxngConfig::default()).with_fallback_endpoint(endpoint);
	let context = AgentContext::new(std::env::temp_dir());
	let result = tool
		.execute(&json!({ "query": "rust" }), &context)
		.await
		.expect("the prefix before the cap still parses");
	server.abort();
	assert!(result.content.contains("Early"), "{}", result.content);
	assert!(!result.content.contains("Late"), "bytes past 256 KiB are ignored: {}", result.content);
}

#[tokio::test]
async fn duckduckgo_does_not_follow_redirects() {
	let redirect = http_status("302 Found", "location: https://example.org/\r\n", "");
	let (endpoint, _captured, server) = loopback(vec![Some(redirect)]).await;
	let tool = WebSearchTool::new(SearxngConfig::default()).with_fallback_endpoint(endpoint);
	let context = AgentContext::new(std::env::temp_dir());
	let error = tool
		.execute(&json!({ "query": "rust" }), &context)
		.await
		.expect_err("redirects are surfaced, not followed");
	server.abort();
	assert!(format!("{error}").contains("HTTP 302"), "{error}");
}
