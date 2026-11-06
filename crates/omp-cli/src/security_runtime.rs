//! C6 runtime-owned glue: `SecurityHost` / `SecurityScanSession` over the
//! existing agent runner and exact OAuth resolver.
//!
//! The coordinator supplies the built `SecurityScanRequest` (plan, pinned
//! model, publication tool, cancel token, resolver). This module never builds a
//! second provider engine and never falls back to another key:
//!
//! - `execution_root` is the worker cwd; it is **never** used for the journal.
//! - The scan journal lives under the security store's `sessions` directory
//!   (`SecurityStore::open(plan.repository_root, host.state_root())`).
//! - Keys come only through the exact resolver adapter: plan resolution asks
//!   `api_key_for(..., false, ...)` once, and the transport's per-request
//!   callback owns `force_refresh` after a `401/403 invalid_key`. The worker
//!   never passes a pinned token as an explicit `api_key`, because that would
//!   disable the transport refresh.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use crate::agent::{AgentRunner, Event, PlanModeHooks, ResolverHandle, RunRequest, ToolCeiling};
use crate::approve::Approvals;
use crate::auth::error::AuthError;
use crate::auth::http::Cancel;
use crate::catalog::Catalog;
use crate::config::Config;
use crate::output::OutputMode;
use crate::session::{
	ContentPart, MessageContent, MessageEntry, SessionMessage, SessionOptions, SessionStore,
};
use crate::security::{
	SecurityHost, SecurityModelRef, SecurityScanRequest, SecurityScanSession, SecurityScanStats,
};

/// TS `securityCoordinatorPrompt` (`coordinator.ts:12,258`), hashed into the
/// workflow fingerprint and appended after the normal harness prompt.
const COORDINATOR_PROMPT: &str =
	include_str!("../../../packages/coding-agent/src/prompts/security/scan-coordinator.md");

/// `SecurityHost` backed by the live CLI session.
pub struct RuntimeSecurityHost {
	pub config:       Config,
	pub catalog:      Arc<Catalog>,
	pub cwd:          PathBuf,
	pub session_id:   Option<String>,
	pub agent_id:     Option<String>,
	pub active_model: Option<String>,
	pub state_root:   Option<PathBuf>,
	pub auth_db:      PathBuf,
	pub approvals:    Approvals,
	pub service_tier: std::collections::BTreeMap<String, String>,
}

impl SecurityHost for RuntimeSecurityHost {
	fn security_enabled(&self) -> bool {
		self.config.security.enabled()
	}

	fn cwd(&self) -> PathBuf {
		self.cwd.clone()
	}

	fn session_id(&self) -> Option<String> {
		self.session_id.clone()
	}

	fn agent_id(&self) -> Option<String> {
		self.agent_id.clone()
	}

	fn active_model(&self) -> Option<SecurityModelRef> {
		let active = self.active_model.as_deref()?;
		let (provider, model_id) = active.split_once('/')?;
		Some(SecurityModelRef {
			provider:       provider.to_string(),
			model_id:       model_id.to_string(),
			thinking_level: None,
		})
	}

	fn config_snapshot(&self) -> serde_json::Value {
		self.config.security.snapshot()
	}

	fn state_root(&self) -> Option<PathBuf> {
		self.state_root.clone()
	}

	fn auth_db(&self) -> PathBuf {
		self.auth_db.clone()
	}

	/// Exact pinned availability only: the active model when it matches, else
	/// the exact `provider/model` pair resolves or errors. Never a chain.
	fn ensure_model(&self, model: &SecurityModelRef) -> Result<(), String> {
		let pair = format!("{}/{}", model.provider, model.model_id);
		if self.active_model.as_deref() == Some(pair.as_str()) {
			return Ok(());
		}
		let mut config = self.config.clone();
		config.model_roles.clear();
		match crate::runtime::resolve_model_switch(&self.catalog, &config, &pair, None) {
			Ok(switch)
				if switch.provider == model.provider && switch.model == model.model_id =>
			{
				Ok(())
			},
			_ => Err(format!("Security scan model is unavailable: {pair}")),
		}
	}

	fn create_scan_session(
		&self,
		request: SecurityScanRequest,
	) -> Result<Arc<dyn SecurityScanSession>, String> {
		let runner =
			crate::agent::runner().ok_or_else(|| "agent runner unavailable".to_string())?;
		Ok(Arc::new(SecurityRunSession::new(
			request,
			runner,
			self.config.clone(),
			Arc::clone(&self.catalog),
			self.approvals.clone(),
			self.state_root.clone(),
			self.service_tier.clone(),
		)?))
	}
}

/// One restricted scan worker. `run` is synchronous by trait contract; the
/// coordinator calls it on its own operation thread, so `block_on` never runs
/// inside the reactor and no std lock is held across it.
pub struct SecurityRunSession {
	request:      SecurityScanRequest,
	runner:       &'static dyn AgentRunner,
	config:       Config,
	catalog:      Arc<Catalog>,
	approvals:    Approvals,
	state_root:   Option<PathBuf>,
	service_tier: std::collections::BTreeMap<String, String>,
	journal:      std::sync::Mutex<SessionStore>,
	session_file: PathBuf,
}

impl SecurityRunSession {
	fn new(
		request: SecurityScanRequest,
		runner: &'static dyn AgentRunner,
		mut config: Config,
		catalog: Arc<Catalog>,
		_approvals: Approvals,
		state_root: Option<PathBuf>,
		service_tier: std::collections::BTreeMap<String, String>,
	) -> Result<Self, String> {
		let store = crate::security::SecurityStore::open(
			Path::new(&request.plan.repository_root),
			state_root.as_deref(),
		)
		.map_err(|error| error.to_string())?;
		let journal = SessionStore::open(&SessionOptions {
			enabled: true,
			cwd: request.execution_root.clone(),
			model: Some(request.model.model_id.clone()),
			provider: Some(request.model.provider.clone()),
			dir: Some(store.sessions_directory()),
			id: Some(request.scan_id.clone()),
			continue_latest: false,
			no_title: true,
			parent_session: None,
		});
		let session_file = journal
			.file()
			.ok_or_else(|| "security scan journal has no session file".to_string())?
			.to_path_buf();
		// Exact restricted surface from TS `SECURITY_SESSION_TOOLS`; the
		// publication tool is the only extra and exists only in this session.
		config.tools = Some(
			crate::security::SECURITY_SESSION_TOOLS
				.iter()
				.map(|name| (*name).to_string())
				.filter(|name| name != "security_publish")
				.collect(),
		);
		config.extensions = Some(Vec::new());
		// TS `enableLsp: true` for this session even when the parent disabled it;
		// the read-only action ceiling still applies to the worker and children.
		config.lsp = crate::config::LspConfig {
			enabled: Some(true),
			shared:  config.lsp.shared,
		};
		// TS `hasUI:false, autoApprove:true`: the restricted tool set + read-only
		// LSP ceiling are the boundary, so no approval surface can stall a scan.
		let approvals = Approvals::new(
			crate::approve::ApprovalMode::Yolo,
			std::iter::empty::<String>(),
		);
		Ok(Self {
			request,
			runner,
			config,
			catalog,
			approvals,
			state_root,
			service_tier,
			journal: std::sync::Mutex::new(journal),
			session_file,
		})
	}

	/// The worker never hands the pinned OAuth token to the explicit-key
	/// channel: `api_key: None` keeps the exact resolver in charge of both the
	/// initial key (plan lookup) and the transport's forced refresh after a
	/// 401/403 `invalid_key`. An explicit key here would disable that refresh.
	fn request(&self, text: &str) -> Result<RunRequest, String> {
		let prompts = crate::prompts::resolve(
			None,
			None,
			Path::new(&self.request.execution_root),
		)
		.map_err(|error| error.to_string())?;
		let mut append = prompts.append.unwrap_or_default();
		if !append.trim().is_empty() {
			append.push_str("\n\n");
		}
		append.push_str(COORDINATOR_PROMPT.trim());
		let provider = self.request.model.provider.clone();
		let model_id = self.request.model.model_id.clone();
		let router = pi_agent::tools::internal_urls::InternalUrlRouter::new();
		pi_agent::tools::internal_urls::register_security(
			&router,
			crate::security::security_url_handler(true, self.state_root.clone()),
		)?;
		let mut local_roots = std::collections::BTreeMap::new();
		local_roots.insert(
			"local".to_string(),
			self.session_file
				.parent()
				.map(Path::to_path_buf)
				.unwrap_or_else(|| self.session_file.clone()),
		);
		let url_scope = Arc::new(pi_agent::tools::internal_urls::UrlScope {
			session_file: Some(self.session_file.clone()),
			local_roots,
		});
		Ok(RunRequest {
			prompt: text.to_string(),
			parts: Vec::<ContentPart>::new(),
			cwd: self.request.execution_root.clone(),
			output_mode: OutputMode::Text,
			model: Some(model_id),
			provider: Some(provider),
			// Resolver-owned: never the explicit-key channel (see `request`).
			api_key: None,
			provider_session_id: Some(format!("security:{}", self.request.scan_id)),
			prompt_cache_key: None,
			system_prompt: prompts.system,
			append_system_prompt: Some(append),
			context_files: prompts.files,
			skills: Vec::new(),
			history: Vec::new(),
			history_messages: None,
			checkpoint_controller: None,
			turn_ack: None,
			todo_phases: Vec::new(),
			session_persisted: true,
			session_file: Some(self.session_file.clone()),
			session_id: Some(format!("security:{}", self.request.scan_id)),
			config: self.config.clone(),
			catalog: Arc::clone(&self.catalog),
			approvals: self.approvals.clone(),
			max_time: None,
			max_turns: 120,
			tools_enabled: true,
			cancellation_token: Some(self.request.cancel.clone()),
			extra_tools: vec![self.request.publication_tool.clone().into()],
			acp_bridge: None,
			extra_mcp_servers: Vec::new(),
			internal_urls: Some(Arc::new(router)),
			url_scope: Some(url_scope),
			subagent_observer: None,
			child_session_root: None,
			goal: None,
			goal_context: None,
			plan_context: None,
			service_tier_by_family: self.service_tier.clone(),
			credential_resolver: Some(ResolverHandle(Arc::new(ExactResolverAdapter {
				resolver: self.request.resolver.clone(),
				cancel:   self.request.cancel.clone(),
			}))),
			plan_mode: PlanModeHooks::default(),
			// The tool-name allowlist is not enough: a same-name MCP server and
			// the extension/eval bridge would spawn outside it.
			tool_ceiling: Some(ToolCeiling {
				exclude_mcp:             true,
				exclude_extension_tools: true,
			}),
			skill_autoload: Default::default(),
			advisor_card_sink: None,
			continuation: false,
			security_host: None,
			security_worker: true,
		})
	}

	/// Drive the runner's blocking event stream to `AgentEnd` and aggregate the
	/// reported usage. The worker never replays itself: a scan may already have
	/// published results, so auth retry belongs to the transport's per-request
	/// callback. Stats come from `messages[*].usage`; cost/premium stay `None`.
	fn run_blocking(&self, text: &str) -> Result<Option<SecurityScanStats>, String> {
		// No worker-level replay: a scan may have already published or mutated
		// artifacts before a late auth failure. The transport owns auth retry so
		// only the failed HTTP request is reissued.
		self.run_once(text)
	}

	fn run_once(&self, text: &str) -> Result<Option<SecurityScanStats>, String> {
		let request = self.request(text)?;
		let stream = self.runner.run(&request).map_err(|error| error.to_string())?;
		let mut journal = self
			.journal
			.lock()
			.unwrap_or_else(|poison| poison.into_inner());
		let mut parent = journal.leaf_id().map(str::to_string);
		{
			let now = std::time::SystemTime::now();
			let now_ms = now
				.duration_since(std::time::UNIX_EPOCH)
				.unwrap_or_default()
				.as_millis() as u64;
			let id = format!("msg-{}", crate::output::session_id());
			let entry = MessageEntry::new(
				id.clone(),
				parent.clone(),
				crate::output::iso8601(now),
				SessionMessage::user(MessageContent::Text(text.to_string()), now_ms),
			);
			if entry.validate().is_ok() {
				if let Err(error) = journal.append_message(&entry) {
					eprintln!("warning: security journal: {error}");
				} else {
					parent = Some(id);
				}
			}
		}
		let mut usage = pi_agent::TokenUsage::default();
		let mut reported = false;
		for event in stream {
			match event {
				Ok(Event::TurnEnd {
					message,
					tool_results,
					stop_reason,
					api,
					provider,
					model,
				}) => {
					parent = self.persist_turn(
						&mut journal,
						parent,
						&message,
						&tool_results,
						&stop_reason,
						api.as_deref(),
						provider.as_deref(),
						model.as_deref(),
					);
				},
				Ok(Event::AgentEnd { messages, .. }) => {
					for message in messages {
						if let Some(message_usage) = message.usage {
							usage.merge(message_usage);
							reported = true;
						}
					}
					break;
				},
				Ok(_) => {},
				Err(error) => return Err(error.to_string()),
			}
		}
		if !reported {
			let (cost, premium_requests) = self.journal_metrics(&journal);
			if cost.is_none() && premium_requests.is_none() {
				return Ok(None);
			}
			return Ok(Some(SecurityScanStats {
				token_usage: None,
				cost,
				premium_requests,
			}));
		}
		let (cost, premium_requests) = self.journal_metrics(&journal);
		Ok(Some(SecurityScanStats {
			token_usage: Some(crate::security::types::SecurityTokenUsage {
				input:       usage.input,
				output:      usage.output,
				reasoning:   usage.reasoning_tokens.unwrap_or(0),
				cache_read:  usage.cache_read,
				cache_write: usage.cache_write,
				total:       usage.total_tokens,
			}),
			cost,
			premium_requests,
		}))
	}

	#[allow(clippy::too_many_arguments)]
	fn persist_turn(
		&self,
		store: &mut SessionStore,
		parent: Option<String>,
		message: &pi_agent::Message,
		tool_results: &[pi_agent::ToolResult],
		stop_reason: &str,
		api: Option<&str>,
		provider: Option<&str>,
		model: Option<&str>,
	) -> Option<String> {
		let now = std::time::SystemTime::now();
		let iso = crate::output::iso8601(now);
		let now_ms = now
			.duration_since(std::time::UNIX_EPOCH)
			.unwrap_or_default()
			.as_millis() as u64;
		let mut parts = Vec::new();
		let mut has_tool_part = false;
		for part in &message.parts {
			if matches!(part, pi_agent::ContentPart::Image { .. }) {
				continue;
			}
			if let Some(part) = ContentPart::from_agent_part(part) {
				has_tool_part |= part.kind == "toolCall";
				parts.push(part);
			}
		}
		if parts.is_empty() && !message.content.is_empty() {
			parts.push(ContentPart::text(message.content.clone()));
		}
		if !has_tool_part {
			for call in &message.tool_calls {
				parts.push(ContentPart::tool_call(
					call.id.clone(),
					call.name.clone(),
					call.arguments.clone(),
				));
			}
		}
		let content = if parts.is_empty() {
			MessageContent::Text(String::new())
		} else {
			MessageContent::Parts(parts)
		};
		let stop = message
			.stop_reason
			.map(pi_agent::StopReason::as_str)
			.unwrap_or_else(|| crate::session::canonical_stop_reason(stop_reason));
		let mut assistant = SessionMessage::assistant(
			content,
			api.unwrap_or_default().to_string(),
			provider.unwrap_or_default().to_string(),
			model.unwrap_or_default().to_string(),
			crate::session::reported_usage(message),
			stop,
			now_ms,
		);
		assistant.error_message = message.error_message.clone();
		let mut entries = Vec::new();
		let assistant_id = format!("msg-{}", crate::output::session_id());
		entries.push(MessageEntry::new(
			assistant_id.clone(),
			parent,
			&iso,
			assistant,
		));
		let mut previous = assistant_id;
		let names: std::collections::HashMap<&str, &str> = message
			.tool_calls
			.iter()
			.map(|call| (call.id.as_str(), call.name.as_str()))
			.collect();
		for result in tool_results {
			let id = format!("msg-{}", crate::output::session_id());
			let name = names.get(result.tool_call_id.as_str()).copied().unwrap_or_default();
			let tool = SessionMessage::tool_result(
				result.tool_call_id.clone(),
				name.to_string(),
				MessageContent::Text(result.content.clone()),
				result.is_error,
				now_ms,
			);
			entries.push(MessageEntry::new(id.clone(), Some(previous), &iso, tool));
			previous = id;
		}
		let valid: Vec<MessageEntry> = entries
			.into_iter()
			.filter(|entry| {
				entry.validate().is_ok_and(|()| true)
			})
			.collect();
		if let Err(error) = store.append_messages(&valid) {
			eprintln!("warning: security journal: {error}");
			return None;
		}
		Some(previous)
	}

	/// Persisted usage objects carry what the live event cannot: an actual
	/// vendor/catalog `cost` and `premiumRequests`. Read the one session store
	/// that also wrote the dialog; report `None` for counters no entry carried.
	fn journal_metrics(&self, store: &SessionStore) -> (Option<f64>, Option<u64>) {
		let mut cost: Option<f64> = None;
		let mut premium: Option<u64> = None;
		for entry in store.messages() {
			if entry.message.role != "assistant" {
				continue;
			}
			let Some(usage) = &entry.message.usage else {
				continue;
			};
			if let Some(entry_cost) = &usage.cost {
				*cost.get_or_insert(0.0) += entry_cost.total;
			}
			if let Some(requests) = usage
				.extra
				.get("premiumRequests")
				.and_then(serde_json::Value::as_u64)
			{
				*premium.get_or_insert(0) = premium.unwrap_or(0).saturating_add(requests);
			}
		}
		(cost, premium)
	}
}

impl SecurityScanSession for SecurityRunSession {
	fn run(&self, request_text: &str) -> Result<Option<SecurityScanStats>, String> {
		self.run_blocking(request_text)
	}

	fn session_file(&self) -> Option<PathBuf> {
		Some(self.session_file.clone())
	}

	fn abort(&self) {
		self.request.cancel.cancel();
	}
}

/// Exact OAuth resolver exposed to `RunRequest` key precedence: no fallback
/// chain; errors surface unchanged (identity/provider mismatch, pinned gone).
struct ExactResolverAdapter {
	resolver: Arc<crate::auth::security::ExactSecurityOAuthResolver>,
	cancel:   pi_agent::CancellationToken,
}

impl crate::rpc::auth::CredentialResolver for ExactResolverAdapter {
	fn api_key(
		&self,
		provider: &str,
		_session: Option<&str>,
		_cancel: &Cancel,
	) -> Result<Option<String>, AuthError> {
		let resolver = Arc::clone(&self.resolver);
		let provider = provider.to_string();
		let cancel = self.cancel.clone();
		crate::rpc::auth::off_thread(move || {
			resolver.api_key_for(
				&provider,
				false,
				&crate::security::host::auth_cancel(&cancel),
			)
		})
	}

	fn peek_api_key(&self, provider: &str) -> Result<Option<String>, AuthError> {
		let resolver = Arc::clone(&self.resolver);
		let provider = provider.to_string();
		crate::rpc::auth::off_thread(move || {
			resolver.api_key_for(&provider, false, &Cancel::new())
		})
	}

	fn api_key_force(
		&self,
		provider: &str,
		_session: Option<&str>,
		force_refresh: bool,
		_cancel: &Cancel,
	) -> Result<Option<String>, AuthError> {
		let resolver = Arc::clone(&self.resolver);
		let provider = provider.to_string();
		let cancel = self.cancel.clone();
		crate::rpc::auth::off_thread(move || {
			resolver.api_key_for(
				&provider,
				force_refresh,
				&crate::security::host::auth_cancel(&cancel),
			)
		})
	}
}

#[cfg(test)]
mod tests {
	use crate::agent::EventStream;

	use super::*;

	/// Runner fixture for worker-request construction: the run path is never
	/// exercised, only `SecurityRunSession::request`.
	struct NoopRunner;

	impl AgentRunner for NoopRunner {
		fn run(&self, _request: &RunRequest) -> crate::Result<EventStream> {
			Err(crate::Error::new("noop runner"))
		}
	}

	static NOOP_RUNNER: NoopRunner = NoopRunner;

	fn host(active: Option<&str>) -> RuntimeSecurityHost {
		RuntimeSecurityHost {
			config:       Config::default(),
			catalog:      Arc::new(Catalog::default()),
			cwd:          PathBuf::from("."),
			session_id:   None,
			agent_id:     None,
			active_model: active.map(str::to_string),
			state_root:   None,
			auth_db:      PathBuf::from("agent.db"),
			approvals:    Approvals::new(
				crate::approve::ApprovalMode::Yolo,
				std::iter::empty::<String>(),
			),
			service_tier: std::collections::BTreeMap::new(),
		}
	}

	/// Exact pinned selection: the live model matches or the exact catalog pair
	/// resolves; anything else is the contract error, never a fallback chain.
	#[test]
	fn ensure_model_is_exact_and_never_falls_back() {
		let live = host(Some("mock/primary"));
		let exact = SecurityModelRef {
			provider:       "mock".to_string(),
			model_id:       "primary".to_string(),
			thinking_level: None,
		};
		assert!(live.ensure_model(&exact).is_ok());

		let missing = SecurityModelRef {
			provider:       "mock".to_string(),
			model_id:       "gone".to_string(),
			thinking_level: None,
		};
		assert_eq!(
			live.ensure_model(&missing).expect_err("unknown model must fail"),
			"Security scan model is unavailable: mock/gone"
		);
	}

	#[test]
	fn mounted_active_model_is_the_resolved_provider_model() {
		let mounted = host(Some("provider/model-id"));
		assert_eq!(
			mounted.active_model(),
			Some(SecurityModelRef {
				provider:       "provider".to_string(),
				model_id:       "model-id".to_string(),
				thinking_level: None,
			})
		);
		assert_eq!(host(None).active_model(), None);
	}

	#[test]
	fn security_mount_defaults_off_and_reports_its_snapshot() {
		let off = RuntimeSecurityHost { config: Config::default(), ..host(None) };
		assert!(!off.security_enabled());
		assert_eq!(off.config_snapshot(), serde_json::json!({"securityEnabled": false}));
		let mut config = Config::default();
		config.security.enabled = Some(true);
		let on = RuntimeSecurityHost { config, ..off };
		assert!(on.security_enabled());
		assert_eq!(on.config_snapshot(), serde_json::json!({"securityEnabled": true}));
	}

	/// The worker must let the exact resolver own the pinned token. Putting it
	/// in `RunRequest.api_key` marks it explicit and the runtime then installs
	/// no credential callback, which disables the transport's forced refresh.
	#[test]
	fn worker_request_keeps_the_exact_resolver_in_charge_of_the_key() {
		use std::sync::Arc;

		use crate::auth::AuthBackend;
		use crate::auth::security::{ExactSecurityOAuthResolver, SecurityOAuthAccount};
		use crate::security::types::{
			SecurityKnowledgeBaseRef, SecurityOutputPlan, SecurityTarget,
		};
		use crate::security::SecurityScanPlan;

		let root = std::env::temp_dir().join(format!(
			"omp-security-worker-{}-{}",
			std::process::id(),
			crate::output::session_id()
		));
		std::fs::create_dir_all(&root).expect("temp root");
		let root_text = root.to_string_lossy().to_string();
		let model = SecurityModelRef {
			provider:       "mock".to_string(),
			model_id:       "primary".to_string(),
			thinking_level: None,
		};
		let plan = SecurityScanPlan {
			document_type:   "security-scan-plan".to_string(),
			schema_version:  "1".to_string(),
			id:              "secplan_test1".to_string(),
			created_at:      "2026-01-01T00:00:00.000Z".to_string(),
			repository_root: root_text.clone(),
			target:          SecurityTarget {
				kind:            "repository".to_string(),
				repository_root: root_text,
				display_name:    "test".to_string(),
				revision:        None,
				base_revision:   None,
				head_revision:   None,
				include_paths:   Vec::new(),
				exclude_paths:   Vec::new(),
				tree_digest:     String::new(),
			},
			knowledge_bases: Vec::<SecurityKnowledgeBaseRef>::new(),
			output:          SecurityOutputPlan {
				root:             root.join("out").to_string_lossy().to_string(),
				archive_existing: false,
				existing_state:   "absent".to_string(),
			},
			model:               model.clone(),
			account:             crate::security::SecurityAccountRef {
				provider:          "mock".to_string(),
				credential_id:     1,
				account_id:        None,
				email:             None,
				organization_id:   None,
				organization_name: None,
			},
			config_fingerprint:   "config".to_string(),
			workflow_fingerprint: "workflow".to_string(),
			fingerprint:          "fingerprint".to_string(),
		};
		let resolver = Arc::new(ExactSecurityOAuthResolver::new(
			Arc::new(std::sync::Mutex::new(
				AuthBackend::in_memory().expect("in-memory auth store"),
			)),
			SecurityOAuthAccount {
				provider:      "mock".to_string(),
				credential_id: 1,
				account_id:    None,
				email:         None,
				org_id:        None,
				org_name:      None,
			},
		));
		let runner: &'static dyn AgentRunner = &NOOP_RUNNER;
		let session = SecurityRunSession::new(
			SecurityScanRequest {
				plan,
				scan_id: "secscan_test1".to_string(),
				execution_root: root.clone(),
				model,
				publication_tool: Arc::new(pi_agent::ReadTool::new()),
				cancel: pi_agent::CancellationToken::new(),
				resolver,
			},
			runner,
			Config::default(),
			Arc::new(Catalog::default()),
			Approvals::new(crate::approve::ApprovalMode::Yolo, std::iter::empty::<String>()),
			Some(root.join("state")),
			std::collections::BTreeMap::new(),
		)
		.expect("worker session");

		let built = session.request("scan the repository").expect("worker request");
		assert!(
			built.api_key.is_none(),
			"the pinned OAuth token belongs to the resolver, not the explicit-key channel"
		);
		assert!(
			built.credential_resolver.is_some(),
			"the transport needs the exact-resolver callback for forced refresh"
		);
		assert_eq!(built.provider.as_deref(), Some("mock"));
		assert!(built.security_worker);

		drop(session);
		let _ = std::fs::remove_dir_all(&root);
	}

	/// Offline edge of the exact refresh contract: the adapter forwards
	/// `force_refresh` to the pinned resolver, which never crosses providers and
	/// returns a typed error (not a fallback key) when the pinned row is absent.
	#[test]
	fn exact_adapter_keeps_force_refresh_pinned_and_fail_closed() {
		use crate::auth::AuthBackend;
		use crate::auth::security::{ExactSecurityOAuthResolver, SecurityOAuthAccount};
		use crate::rpc::auth::CredentialResolver;

		let resolver = ExactSecurityOAuthResolver::new(
			Arc::new(std::sync::Mutex::new(
				AuthBackend::in_memory().expect("in-memory auth store"),
			)),
			SecurityOAuthAccount {
				provider:      "openai-codex".to_string(),
				credential_id: 7,
				account_id:    None,
				email:         None,
				org_id:        None,
				org_name:      None,
			},
		);
		let adapter = ExactResolverAdapter {
			resolver: Arc::new(resolver),
			cancel:   pi_agent::CancellationToken::new(),
		};
		let cancel = Cancel::new();
		let mismatch = adapter
			.api_key("other", None, &cancel)
			.expect_err("provider mismatch");
		assert!(mismatch.to_string().contains("provider mismatch"), "{mismatch}");
		let forced = adapter
			.api_key_force("other", None, true, &cancel)
			.expect_err("forced provider mismatch");
		assert!(forced.to_string().contains("provider mismatch"), "{forced}");
		// The pinned row does not exist in this store: the forced-refresh path
		// fails closed instead of returning an unaligned credential.
		let missing = adapter
			.api_key_force("openai-codex", None, true, &cancel)
			.expect_err("absent pinned row");
		assert!(missing.to_string().contains("unavailable"), "{missing}");
	}
}
