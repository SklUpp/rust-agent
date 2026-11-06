//! Host implementation of the per-child advisor seam.
//!
//! The trigger contract is the TypeScript one: `task.rs` calls these methods at
//! the same boundaries as `setOnTurnEnd` / `onPrimaryTurnEnd`
//! (`session-advisors.ts:337-354`): once at spawn with the initial user message,
//! once per settled turn after tool results (never for an aborted turn), and
//! once when the child session ends. Advice is delivered through the root host's
//! card sink, so a child card is persisted/rendered exactly like a root card and
//! never starts a hidden child turn; per-child transcripts are written beside the
//! child session log by [`AdvisorManager`].

use std::collections::{BTreeMap, HashMap};
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use pi_agent::Message;
use pi_agent::subagent_advisor::{ChildAdvisorContext, ChildAdvisorHook};
use pi_agent::tools::task_catalog::HandoffTarget;

use crate::advisor_child::{apply_to_config, resolve_agent_advisor};
use crate::advisor_identity::AdvisorIdentities;
use crate::advisor_runtime::{
	AdvisorExecutor, AdvisorManager, AdvisorManagerOptions, AdvisorRoute, CardSink, RouteResolver,
};
use crate::agent::RunRequest;
use crate::catalog::Catalog;
use crate::config::Config;
use crate::runtime::CliAdvisorExecutor;

/// One advisor runtime per live child session, owned by the root run.
pub struct ChildAdvisorHost {
	config:      Mutex<Config>,
	catalog:     Mutex<Arc<Catalog>>,
	approvals:   Mutex<crate::approve::Approvals>,
	identities:  Arc<AdvisorIdentities>,
	cwd:         PathBuf,
	agent_dir:   PathBuf,
	card_sink:   Mutex<Option<CardSink>>,
	service_tier: Mutex<BTreeMap<String, String>>,
	handle:      tokio::runtime::Handle,
	children:    Mutex<HashMap<String, Arc<AdvisorManager>>>,
}

impl ChildAdvisorHost {
	pub fn new(
		request: &RunRequest,
		handle: tokio::runtime::Handle,
		identities: Arc<AdvisorIdentities>,
	) -> Self {
		Self {
			config: Mutex::new(request.config.clone()),
			catalog: Mutex::new(Arc::clone(&request.catalog)),
			approvals: Mutex::new(request.approvals.clone()),
			identities,
			cwd: request.cwd.clone(),
			agent_dir: crate::dirs::agent_dir(),
			card_sink: Mutex::new(
				request.advisor_card_sink.as_ref().map(|sink| Arc::clone(&sink.0)),
			),
			service_tier: Mutex::new(crate::advisor_identity::service_tier_map(
				Some(request.config.advisor.tier_setting()),
				&request.service_tier_by_family,
			)),
			handle,
			children: Mutex::new(HashMap::new()),
		}
	}

	/// Refresh host-owned state for a new root turn without replacing the host,
	/// so children spawned by earlier turns keep their managers and sinks.
	pub fn refresh(&self, request: &RunRequest) {
		*self.config.lock().unwrap_or_else(|poison| poison.into_inner()) =
			request.config.clone();
		*self.catalog.lock().unwrap_or_else(|poison| poison.into_inner()) =
			Arc::clone(&request.catalog);
		*self.approvals.lock().unwrap_or_else(|poison| poison.into_inner()) =
			request.approvals.clone();
		*self.card_sink.lock().unwrap_or_else(|poison| poison.into_inner()) =
			request.advisor_card_sink.as_ref().map(|sink| Arc::clone(&sink.0));
		*self.service_tier.lock().unwrap_or_else(|poison| poison.into_inner()) =
			crate::advisor_identity::service_tier_map(
				Some(request.config.advisor.tier_setting()),
				&request.service_tier_by_family,
			);
	}

	/// Build the child roster on first use. Absent opt-in returns `None`; no
	/// provider is contacted and no hidden work starts.
	fn manager_for(&self, child: &ChildAdvisorContext<'_>) -> Option<Arc<AdvisorManager>> {
		// TS `createSubagentSettings` builds isolated child settings: a child
		// selection turns `advisor.enabled` on for that child regardless of the
		// root setting, so the root gate must not be consulted here.
		let mut children = self
			.children
			.lock()
			.unwrap_or_else(|poison| poison.into_inner());
		if let Some(manager) = children.get(child.child_id) {
			return Some(Arc::clone(manager));
		}
		let definition = if child.definition_advisor {
			Some(
				child
					.definition_pattern
					.map(|pattern| HandoffTarget::Pattern(pattern.to_string()))
					.unwrap_or(HandoffTarget::Default),
			)
		} else {
			None
		};
		let selection = resolve_agent_advisor(
			self
				.config
				.lock()
				.unwrap_or_else(|poison| poison.into_inner())
				.advisor
				.agent_advisor_for(child.agent.unwrap_or("")),
			definition.as_ref(),
		)?;
		let mut config = self
			.config
			.lock()
			.unwrap_or_else(|poison| poison.into_inner())
			.clone();
		apply_to_config(&mut config, &selection);
		let pattern_override = selection.model.clone();
		let catalog = Arc::clone(
			&self.catalog.lock().unwrap_or_else(|poison| poison.into_inner()),
		);
		let resolver_config = config.clone();
		let resolver: RouteResolver = Arc::new(move |pattern| {
			let pattern = if pattern == "@advisor" {
				pattern_override.clone().unwrap_or_else(|| "@advisor".to_string())
			} else {
				pattern.to_string()
			};
			let switch =
				crate::runtime::resolve_model_switch(&catalog, &resolver_config, &pattern, None)?;
			let api_key = resolver_config.resolve_api_key(&switch.provider);
			Ok(AdvisorRoute {
				provider: switch.provider,
				model:    switch.model,
				effort:   switch.effort,
				api_key,
			})
		});
		let executor: Arc<dyn AdvisorExecutor> = Arc::new(CliAdvisorExecutor);
		let discovered =
			crate::advisor_config::discover(&self.cwd, Some(self.agent_dir.as_path()));
		let roster = if discovered.advisors.is_empty() {
			vec![crate::advisor_config::AdvisorEntry {
				name:         "default".to_string(),
				model:        None,
				tools:        None,
				instructions: None,
				enabled:      None,
			}]
		} else {
			discovered.advisors
		};
		let watchdog_docs = crate::advisor_config::discover_watchdog_docs(
			&self.cwd,
			Some(self.agent_dir.as_path()),
		);
		let session_file = child.session_file.map(std::path::Path::to_path_buf);
		let manager = AdvisorManager::new(AdvisorManagerOptions {
			cwd: self.cwd.clone(),
			agent_dir: Some(self.agent_dir.clone()),
			config: config.clone(),
			catalog: Arc::clone(
				&self.catalog.lock().unwrap_or_else(|poison| poison.into_inner()),
			),
			approvals: self
				.approvals
				.lock()
				.unwrap_or_else(|poison| poison.into_inner())
				.clone(),
			primary_session_id: session_file
				.as_ref()
				.map(|path| path.display().to_string())
				.or_else(|| Some(child.child_id.to_string())),
			primary_cache_key: None,
			session_file,
			service_tier: self
				.service_tier
				.lock()
				.unwrap_or_else(|poison| poison.into_inner())
				.clone(),
			context_prompt: None,
			watchdog_prompt: (!watchdog_docs.is_empty()).then(|| watchdog_docs.join("\n\n")),
			shared_instructions: discovered.shared_instructions,
			roster,
			executor,
			resolver,
			identities: Arc::clone(&self.identities),
			handle: self.handle.clone(),
		});
		manager.configure_run(
			Some(child.child_id.to_string()),
			None,
			child
				.session_file
				.map(std::path::Path::to_path_buf),
			self
				.card_sink
				.lock()
				.unwrap_or_else(|poison| poison.into_inner())
				.clone(),
			None,
		);
		children.insert(child.child_id.to_string(), Arc::clone(&manager));
		Some(manager)
	}
}

impl Drop for ChildAdvisorHost {
	fn drop(&mut self) {
		let managers: Vec<_> = self
			.children
			.lock()
			.unwrap_or_else(|poison| poison.into_inner())
			.drain()
			.map(|(_, manager)| manager)
			.collect();
		for manager in managers {
			manager.dispose();
		}
	}
}

fn child_steer_sink(child: &ChildAdvisorContext<'_>) -> Option<crate::advisor_runtime::SteerSink> {
	child.steer.as_ref().map(|sender| {
		let sender = sender.clone();
		Arc::new(move |text: &str| {
			sender
				.try_send(pi_agent::SteeringEvent::InjectMessage(Message::user(text).parts))
				.is_ok()
		}) as crate::advisor_runtime::SteerSink
	})
}

#[async_trait::async_trait]
impl ChildAdvisorHook for ChildAdvisorHost {
	async fn on_child_start(&self, child: &ChildAdvisorContext<'_>, initial: &Message) {
		if let Some(manager) = self.manager_for(child) {
			manager.configure_run(
				Some(child.child_id.to_string()),
				None,
				child.session_file.map(std::path::Path::to_path_buf),
				self
					.card_sink
					.lock()
					.unwrap_or_else(|poison| poison.into_inner())
					.clone(),
				child_steer_sink(child),
			);
			manager.begin_primary_run(&[]);
			manager.push_user(initial);
		}
	}

	async fn on_child_turn(
		&self,
		child: &ChildAdvisorContext<'_>,
		message: &Message,
		tool_results: &[pi_agent::ToolResult],
		_will_continue: bool,
	) {
		let Some(manager) = self
			.children
			.lock()
			.unwrap_or_else(|poison| poison.into_inner())
			.get(child.child_id)
			.cloned()
		else {
			return;
		};
		manager.push_turn(message, tool_results);
		// TS `onPrimaryTurnEnd`: feed the turn, then wait for the backlog before
		// delivering. Waiting on the blocking pool keeps the async child loop
		// responsive instead of blocking its Tokio worker.
		let threshold = {
			let config = self
				.config
				.lock()
				.unwrap_or_else(|poison| poison.into_inner());
			config.advisor.sync_backlog()
		};
		if let Some(threshold) = threshold
			&& manager.backlog() >= threshold
		{
			let waiting = Arc::clone(&manager);
			let _ = tokio::task::spawn_blocking(move || {
				waiting.sync_backlog(threshold, std::time::Duration::from_secs(30));
				waiting.wait_for_catchup(std::time::Duration::from_secs(30));
			})
			.await;
		}
		// Announce after the review settled, then hand the same text to the
		// child's next request. A failed `try_send` leaves the emitted card as
		// the delivery, so no note is silently dropped.
		manager.flush_asides();
		if let Some(content) = manager.take_pending_aside_content()
			&& let Some(steer) = child_steer_sink(child)
		{
			steer(&content);
		}
	}

	async fn on_child_end(&self, child: &ChildAdvisorContext<'_>, aborted: bool) {
		let Some(manager) = self
			.children
			.lock()
			.unwrap_or_else(|poison| poison.into_inner())
			.remove(child.child_id)
		else {
			return;
		};
		if !aborted {
			// Final review drain: preserve cards, wait bounded for the last
			// review, emit everything, then dispose. TS does not re-prompt the
			// child after it settled.
			manager.prepare_headless_drain();
			let waiting = Arc::clone(&manager);
			let _ = tokio::task::spawn_blocking(move || {
				waiting.wait_for_catchup(std::time::Duration::from_secs(30));
			})
			.await;
			manager.flush_asides();
		}
		manager.dispose();
	}
}
