//! Built-in `mcp-prompt://` read handler.
//!
//! URL forms:
//! - `mcp-prompt://<server>` — `prompts/list` for that server.
//! - `mcp-prompt://<server>/<name>?<arg>=<value>` — `prompts/get`.
//!
//! Prompts live in their own namespace instead of a subpath of `mcp://`: an
//! MCP resource URI is opaque, so reserving any `mcp://<server>/...` prefix for
//! prompts would steal resource URIs a server may legitimately define.
//!
//! Arguments are percent-decoded query pairs (`%20` is the space escape; `+` is
//! a literal plus because prompt arguments are opaque strings, not form data).
//! A repeated key and an empty key are explicit errors instead of silent
//! last-wins.
//!
//! Every content block the server returns is kept: text verbatim, images and
//! audio as explicit base64 markers, embedded resources with their URI, MIME
//! type and payload. The rendered result is untrusted server content — the same
//! trust level as a resource read — and is never interpreted as runtime policy.
//! Nothing runs automatically: a prompt is fetched only when this URL is read.

use std::collections::BTreeMap;

use async_trait::async_trait;

use super::{
	internal_urls::{InternalResource, InternalUrl, ProtocolHandler, ResolveContext},
	internal_urls_github::percent_decode,
};
use crate::mcp::{MCPContent, MCPPrompt, get_active_session, list_active_sessions};

/// Scheme for MCP prompt listing and rendering.
pub const MCP_PROMPT_SCHEME: &str = "mcp-prompt";

pub struct McpPromptUrlHandler;

#[async_trait]
impl ProtocolHandler for McpPromptUrlHandler {
	fn scheme(&self) -> &str {
		MCP_PROMPT_SCHEME
	}

	async fn resolve(
		&self,
		url: &InternalUrl,
		context: &ResolveContext,
	) -> Result<InternalResource, String> {
		if context.signal.is_cancelled() {
			return Err("aborted".to_string());
		}
		if url.host.is_empty() {
			let servers = list_active_sessions();
			let available = if servers.is_empty() {
				"none".to_string()
			} else {
				servers.join(", ")
			};
			return Err(format!(
				"mcp-prompt:// requires a server: mcp-prompt://<server>/<prompt>\nActive MCP servers: {available}"
			));
		}
		let Some(connection) = get_active_session(&url.host) else {
			let servers = list_active_sessions();
			return Err(format!(
				"Unknown MCP server \"{}\".\nAvailable servers: {}",
				url.host,
				servers.join(", ")
			));
		};

		let target = parse_target(url)?;
		match target {
			PromptTarget::List => {
				let prompts = connection.list_prompts().await.map_err(|error| {
					format!("mcp-prompt://{} prompts/list failed: {error}", url.host)
				})?;
				let content = render_prompt_list(&url.host, &prompts);
				let mut resource = InternalResource::text(&url.href, content);
				resource.content_type = "text/markdown".to_string();
				resource.immutable = true;
				resource.is_directory = true;
				resource.notes = Some(vec![format!("MCP server: {}", url.host)]);
				Ok(resource)
			},
			PromptTarget::Get { name, arguments } => {
				if context.signal.is_cancelled() {
					return Err("aborted".to_string());
				}
				let result = connection
					.get_prompt(&name, arguments)
					.await
					.map_err(|error| {
						format!("MCP prompt \"{name}\" on \"{}\" failed: {error}", url.host)
					})?;
				if result.messages.is_empty() {
					return Err(format!(
						"Server \"{}\" returned no messages for prompt \"{name}\".",
						url.host
					));
				}
				let mut content = format!("# Prompt {name}\n");
				if let Some(description) = &result.description {
					content.push_str(&format!("\n{description}\n"));
				}
				for message in &result.messages {
					content.push_str(&format!("\n## {}\n", message.role));
					let mut parts = Vec::new();
					for block in message.content.clone().into_vec() {
						parts.push(render_content(block));
					}
					if parts.is_empty() {
						parts.push("[No content]".to_string());
					}
					content.push_str(&parts.join("\n\n"));
					content.push('\n');
				}
				let mut resource = InternalResource::text(&url.href, content);
				resource.immutable = true;
				resource.notes = Some(vec![format!("MCP server: {}", url.host)]);
				Ok(resource)
			},
		}
	}
}

#[derive(Debug)]
enum PromptTarget {
	List,
	Get { name: String, arguments: BTreeMap<String, String> },
}

fn parse_target(url: &InternalUrl) -> Result<PromptTarget, String> {
	let relative = url.path.trim_start_matches('/');
	if relative.is_empty() {
		return Ok(PromptTarget::List);
	}
	let (raw_name, raw_query) = relative.split_once('?').unwrap_or((relative, ""));
	let name = percent_decode(raw_name).unwrap_or_else(|| raw_name.to_string());
	if name.is_empty() {
		return Err("mcp-prompt://<server>/<prompt> requires a prompt name".to_string());
	}
	let mut arguments = BTreeMap::new();
	for pair in raw_query.split('&') {
		if pair.is_empty() {
			continue;
		}
		let (raw_key, raw_value) = pair.split_once('=').unwrap_or((pair, ""));
		let key = percent_decode(raw_key).unwrap_or_else(|| raw_key.to_string());
		let value = percent_decode(raw_value).unwrap_or_else(|| raw_value.to_string());
		if key.is_empty() {
			return Err(format!("mcp-prompt://{}: empty argument name in \"?{raw_query}\"", url.host));
		}
		if arguments.insert(key.clone(), value).is_some() {
			return Err(format!(
				"mcp-prompt://{}: argument \"{key}\" was supplied more than once",
				url.host
			));
		}
	}
	Ok(PromptTarget::Get { name, arguments })
}

fn render_prompt_list(server: &str, prompts: &[MCPPrompt]) -> String {
	let mut lines = vec![format!("# MCP prompts on {server}"), String::new()];
	if prompts.is_empty() {
		lines.push("(none)".to_string());
		return lines.join("\n");
	}
	for prompt in prompts {
		let mut line = format!("- {}", prompt.name);
		if let Some(title) = prompt.title.as_deref().filter(|title| !title.is_empty()) {
			line.push_str(&format!(" ({title})"));
		}
		if let Some(description) = prompt
			.description
			.as_deref()
			.filter(|description| !description.is_empty())
		{
			line.push_str(&format!(" — {description}"));
		}
		lines.push(line);
		let arguments: Vec<String> = prompt
			.arguments
			.iter()
			.map(|argument| {
				if argument.required {
					format!("{} (required)", argument.name)
				} else {
					argument.name.clone()
				}
			})
			.collect();
		if !arguments.is_empty() {
			lines.push(format!("  arguments: {}", arguments.join(", ")));
		}
	}
	lines.join("\n")
}

fn render_content(content: MCPContent) -> String {
	match content {
		MCPContent::Text { text } => text,
		MCPContent::Image { data, mime_type } => {
			format!("[Image: {mime_type}; base64={data}]")
		},
		MCPContent::Audio { data, mime_type } => {
			format!("[Audio: {mime_type}; base64={data}]")
		},
		MCPContent::Resource { resource } => {
			let uri = &resource.uri;
			let mime = resource
				.mime_type
				.as_deref()
				.map(|mime| format!("; {mime}"))
				.unwrap_or_default();
			if let Some(text) = &resource.text {
				format!("[Resource: {uri}{mime}]\n{text}")
			} else if let Some(blob) = &resource.blob {
				format!("[Resource: {uri}{mime}; base64={blob}]")
			} else {
				format!("[Resource: {uri}{mime}]")
			}
		},
	}
}

#[cfg(test)]
mod tests {
	use std::sync::{Arc, Mutex};
	use std::time::Duration;

	use serde_json::{json, Value};
	use tokio::sync::RwLock;

	use super::*;
	use crate::mcp::McpConnection;
	use crate::mcp::error::McpError;
	use crate::mcp::transports::McpTransport;
	use crate::mcp::types::{
		MCPPromptArgument, MCPPromptsCapability, MCPServerCapabilities, MCPServerConfig,
		MCPServerInfo, MCPStdioServerConfig,
	};

	struct FakeTransport {
		error: Option<(i64, &'static str)>,
		requested: Mutex<Vec<String>>,
	}

	#[async_trait]
	impl McpTransport for FakeTransport {
		async fn request(
			&self,
			method: &str,
			_params: Option<Value>,
			_timeout: Option<Duration>,
		) -> Result<Value, McpError> {
			self
				.requested
				.lock()
				.expect("requested")
				.push(method.to_string());
			if let Some((code, message)) = self.error {
				return Err(McpError::JsonRpc { code, message: message.to_string(), data: None });
			}
			Err(McpError::Protocol(format!("unexpected method {method}")))
		}

		async fn notify(&self, _method: &str, _params: Option<Value>) -> Result<(), McpError> {
			Ok(())
		}

		async fn close(&self) -> Result<(), McpError> {
			Ok(())
		}
	}

	fn connection(
		prompts: bool,
		error: Option<(i64, &'static str)>,
	) -> (McpConnection, Arc<FakeTransport>) {
		let transport = Arc::new(FakeTransport { error, requested: Mutex::new(Vec::new()) });
		let connection = McpConnection {
			name: "demo".to_string(),
			config: MCPServerConfig::Stdio(MCPStdioServerConfig {
				command: "true".to_string(),
				args: Vec::new(),
				env: Default::default(),
				cwd: None,
				timeout: None,
				request_id_format: None,
			}),
			transport: Arc::new(RwLock::new(Arc::clone(&transport) as Arc<dyn McpTransport>)),
			server_info: MCPServerInfo { name: "demo".to_string(), version: None },
			capabilities: MCPServerCapabilities {
				prompts: prompts.then_some(MCPPromptsCapability { list_changed: false }),
				..Default::default()
			},
			instructions: None,
		};
		(connection, transport)
	}

	fn url(raw: &str) -> InternalUrl {
		InternalUrl::parse(raw).expect("parse")
	}

	#[test]
	fn parses_prompt_targets_and_rejects_ambiguous_arguments() {
		assert!(matches!(parse_target(&url("mcp-prompt://demo")), Ok(PromptTarget::List)));

		match parse_target(&url("mcp-prompt://demo/review?path=src%2Flib.rs&mode=a%20b")) {
			Ok(PromptTarget::Get { name, arguments }) => {
				assert_eq!(name, "review");
				assert_eq!(arguments.get("path").map(String::as_str), Some("src/lib.rs"));
				assert_eq!(arguments.get("mode").map(String::as_str), Some("a b"));
			},
			other => panic!("expected a get target, got {other:?}"),
		}

		// A literal `+` is not a space: prompt arguments are opaque strings.
		match parse_target(&url("mcp-prompt://demo/review?q=a+b")) {
			Ok(PromptTarget::Get { arguments, .. }) => {
				assert_eq!(arguments.get("q").map(String::as_str), Some("a+b"));
			},
			other => panic!("expected a get target, got {other:?}"),
		}

		let error = parse_target(&url("mcp-prompt://demo//?x=1")).expect_err("empty name");
		assert!(error.contains("requires a prompt name"), "{error}");
		let error = parse_target(&url("mcp-prompt://demo/review?a=1&a=2")).expect_err("repeated");
		assert!(error.contains("supplied more than once"), "{error}");
		let error = parse_target(&url("mcp-prompt://demo/review?=1")).expect_err("empty key");
		assert!(error.contains("empty argument name"), "{error}");
	}

	#[test]
	fn renders_catalog_and_every_content_block() {
		let prompts = vec![
			MCPPrompt {
				name: "review".to_string(),
				title: Some("Review".to_string()),
				description: Some("Review a change".to_string()),
				arguments: vec![
					MCPPromptArgument {
						name: "path".to_string(),
						description: Some("File".to_string()),
						required: true,
					},
					MCPPromptArgument { name: "mode".to_string(), ..Default::default() },
				],
			},
			MCPPrompt {
				name: "second".to_string(),
				title: None,
				description: None,
				arguments: Vec::new(),
			},
		];
		let rendered = render_prompt_list("demo", &prompts);
		assert!(rendered.contains("- review (Review) — Review a change"), "{rendered}");
		assert!(rendered.contains("arguments: path (required), mode"), "{rendered}");
		assert!(rendered.contains("- second"), "{rendered}");
		assert_eq!(render_prompt_list("demo", &[]), "# MCP prompts on demo\n\n(none)");

		let blocks = vec![
			MCPContent::Text { text: "hello".to_string() },
			MCPContent::Image { data: "AAECAwQ=".to_string(), mime_type: "image/png".to_string() },
			MCPContent::Audio { data: "BAUGBw==".to_string(), mime_type: "audio/wav".to_string() },
			MCPContent::Resource {
				resource: crate::mcp::MCPResourceContentItem {
					uri: "mem://note".to_string(),
					mime_type: Some("text/plain".to_string()),
					text: Some("embedded".to_string()),
					blob: None,
				},
			},
			MCPContent::Resource {
				resource: crate::mcp::MCPResourceContentItem {
					uri: "mem://bin".to_string(),
					mime_type: Some("application/octet-stream".to_string()),
					text: None,
					blob: Some("AAECAwQ=".to_string()),
				},
			},
		];
		let rendered: Vec<String> = blocks.into_iter().map(render_content).collect();
		assert_eq!(rendered[0], "hello");
		assert_eq!(rendered[1], "[Image: image/png; base64=AAECAwQ=]");
		assert_eq!(rendered[2], "[Audio: audio/wav; base64=BAUGBw==]");
		assert_eq!(rendered[3], "[Resource: mem://note; text/plain]\nembedded");
		assert_eq!(rendered[4], "[Resource: mem://bin; application/octet-stream; base64=AAECAwQ=]");
	}

	#[tokio::test]
	async fn capability_gate_and_server_errors_are_explicit() {
		let (gated, transport) = connection(false, None);
		assert!(!gated.supports_prompts());
		match gated.list_prompts().await {
			Err(McpError::UnsupportedCapability(message)) => {
				assert!(message.contains("prompts"), "{message}");
			},
			other => panic!("expected UnsupportedCapability, got {other:?}"),
		}
		match gated.get_prompt("review", BTreeMap::new()).await {
			Err(McpError::UnsupportedCapability(message)) => {
				assert!(message.contains("prompts"), "{message}");
			},
			other => panic!("expected UnsupportedCapability, got {other:?}"),
		}
		assert!(
			transport.requested.lock().expect("requested").is_empty(),
			"the capability check must send no request"
		);

		let (connection, _) = connection(true, Some((-32603, "render failed")));
		match connection.get_prompt("review", BTreeMap::new()).await {
			Err(McpError::JsonRpc { code, message, .. }) => {
				assert_eq!(code, -32603);
				assert_eq!(message, "render failed");
			},
			other => panic!("expected JsonRpc error, got {other:?}"),
		}
	}

	#[tokio::test]
	async fn handler_requires_a_server_and_lists_unknown_ones() {
		let context = ResolveContext::default();
		let error = McpPromptUrlHandler
			.resolve(&url("mcp-prompt://"), &context)
			.await
			.expect_err("empty host");
		assert!(error.contains("requires a server"), "{error}");

		let error = McpPromptUrlHandler
			.resolve(&url("mcp-prompt://no-such-server-for-prompt-tests"), &context)
			.await
			.expect_err("unknown server");
		assert!(error.contains("Unknown MCP server"), "{error}");
	}
}
