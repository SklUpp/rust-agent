//! `web_search` backed by a host-configured SearXNG instance.
//!
//! An unconfigured instance, a transport/timeout failure, an HTTP 5xx, or a
//! search whose engines all failed falls back to DuckDuckGo Lite HTML (no
//! credentials, same rendered output); configuration errors and caller
//! cancellation never fall back.
//!
//! Upstream resolves a provider chain (`web/search/provider.ts`); this slice
//! ships SearXNG plus the free fallback, so there is one concrete tool and no
//! provider abstraction. The endpoint and every credential arrive through
//! [`SearxngConfig`] from trusted host settings/env, never from tool arguments,
//! so the model cannot redirect the request.
//!
//! Request/response mapping mirrors `web/search/providers/searxng.ts`:
//! `GET {endpoint}/search?format=json`, query canonicalized with
//! [`format_scraper_query`] then external bangs stripped, Basic
//! auth before Bearer, `week` recency mapped to SearXNG's `month`, configured
//! engine shortcuts resolved through `/config`, rows without a `url` dropped,
//! lenient constraint relaxation, empty results with unresponsive engines
//! treated as unavailable, and every model-facing error scrubbed of the
//! resolved authorization material.

use std::{
	collections::{BTreeMap, HashMap},
	sync::{Arc, LazyLock, Mutex},
	time::{Duration, SystemTime, UNIX_EPOCH},
};

use async_trait::async_trait;
use regex::Regex;
use reqwest::{StatusCode, Url, header};
use serde_json::{Value, json};
use tokio_util::sync::CancellationToken;

use super::search_query::{
	ConstraintSource, StructuredQuery, filter_source_indices, format_scraper_query,
	parse_search_query, parse_timestamp_ms,
};
use crate::{
	state::{AgentContext, ToolResult},
	tool::{Tool, ToolError},
};

/// Upstream `DEFAULT_NUM_RESULTS`.
pub const DEFAULT_NUM_RESULTS: usize = 10;
/// Upstream `MAX_NUM_RESULTS`; the schema-facing clamp ceiling.
pub const MAX_NUM_RESULTS: usize = 20;
/// Upstream `DEFAULT_WEB_SEARCH_TIMEOUT_SECONDS` per transport.
const REQUEST_TIMEOUT: Duration = Duration::from_secs(60);
/// A SearXNG JSON page is far smaller; refuse larger bodies instead of
/// buffering unbounded bytes from a user-configured host.
const MAX_RESPONSE_BYTES: usize = 4 * 1024 * 1024;
const SNIPPET_MAX_CHARS: usize = 240;
const ERROR_BODY_MAX_CHARS: usize = 2_000;
const NOT_CONFIGURED: &str =
	"SearXNG endpoint not configured. Set searxng.endpoint in settings or SEARXNG_ENDPOINT in environment.";

/// Credential-free fallback used when SearXNG is unconfigured or unavailable.
const DUCKDUCKGO_LITE_URL: &str = "https://lite.duckduckgo.com/lite/";
const DUCKDUCKGO_TIMEOUT: Duration = Duration::from_secs(10);
/// The lite page is ~50 KB; stop buffering here so a hostile or runaway
/// response cannot grow memory without bound.
const DUCKDUCKGO_MAX_HTML_BYTES: usize = 256 * 1024;
/// The lite endpoint rejects obvious bot agents.
const DUCKDUCKGO_USER_AGENT: &str =
	"Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/149.0 Safari/537.36";

/// Per-endpoint cache of shortcut/name → canonical engine names, mirroring
/// upstream's process-lifetime `engineNameMapCache`. Failures are not cached,
/// so a transient error retries on the next search.
static ENGINE_NAMES: Mutex<BTreeMap<String, Arc<HashMap<String, String>>>> = Mutex::new(BTreeMap::new());

/// SearXNG settings after host resolution.
///
/// `endpoint`/`token`/`engines` follow upstream's truthiness rule (an empty
/// settings value falls back to the environment); `basic_username` and
/// `basic_password` follow its presence rule (an empty string still counts as
/// configured, so a half-configured pair is an error rather than a silent
/// downgrade to Bearer). `categories`/`language`/`safesearch` have no
/// environment fallback upstream.
#[derive(Clone, Default, PartialEq, Eq)]
pub struct SearxngConfig {
	pub endpoint:       Option<String>,
	pub token:          Option<String>,
	pub basic_username: Option<String>,
	pub basic_password: Option<String>,
	pub categories:     Option<String>,
	pub engines:        Option<String>,
	pub language:       Option<String>,
	pub safesearch:     Option<i32>,
}

/// Credentials are masked (presence only); endpoint/engine selection remains
/// visible for diagnostics.
impl std::fmt::Debug for SearxngConfig {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		f.debug_struct("SearxngConfig")
			.field("endpoint", &self.endpoint)
			.field("token", &self.token.as_ref().map(|_| "[REDACTED]"))
			.field("basic_username", &self.basic_username)
			.field(
				"basic_password",
				&self.basic_password.as_ref().map(|_| "[REDACTED]"),
			)
			.field("categories", &self.categories)
			.field("engines", &self.engines)
			.field("language", &self.language)
			.field("safesearch", &self.safesearch)
			.finish()
	}
}

impl SearxngConfig {
	/// Layer the `SEARXNG_*` environment fallbacks under the settings values.
	///
	/// Callers pass the process (or host-supplied) environment; the tool only
	/// ever reads this resolved struct, so no tool invocation can see the
	/// environment directly.
	#[must_use]
	pub fn with_env_overrides(mut self, env: &HashMap<String, String>) -> Self {
		if !self.endpoint.as_deref().is_some_and(|value| !value.is_empty()) {
			self.endpoint = env.get("SEARXNG_ENDPOINT").filter(|value| !value.is_empty()).cloned();
		}
		if !self.token.as_deref().is_some_and(|value| !value.is_empty()) {
			self.token = env.get("SEARXNG_TOKEN").filter(|value| !value.is_empty()).cloned();
		}
		if self.basic_username.is_none() {
			self.basic_username = env.get("SEARXNG_BASIC_USERNAME").cloned();
		}
		if self.basic_password.is_none() {
			self.basic_password = env.get("SEARXNG_BASIC_PASSWORD").cloned();
		}
		self
	}
}

/// Everything that must never reach the model in transport or mapping errors:
/// the resolved `Authorization` value, its Basic payload, and the raw secrets.
struct Redactor {
	secrets: Vec<String>,
}

impl Redactor {
	fn new(config: &SearxngConfig, authorization: Option<&str>) -> Self {
		let mut secrets: Vec<String> = Vec::new();
		if let Some(authorization) = authorization {
			secrets.push(authorization.to_string());
			if let Some(encoded) = authorization.strip_prefix("Basic ") {
				secrets.push(encoded.to_string());
			}
		}
		for secret in [config.token.as_deref(), config.basic_password.as_deref()] {
			if let Some(secret) = secret.filter(|secret| !secret.is_empty()) {
				secrets.push(secret.to_string());
			}
		}
		// Longest first so an encoded header cannot survive a partial replace.
		secrets.sort_by_key(|secret| std::cmp::Reverse(secret.len()));
		Self { secrets }
	}

	fn redact(&self, text: &str) -> String {
		let mut redacted = text.to_string();
		for secret in &self.secrets {
			redacted = redacted.replace(secret, "[redacted]");
		}
		redacted
	}

	fn redact_error(&self, error: ToolError) -> ToolError {
		match error {
			ToolError::ExecutionFailed(message) => ToolError::ExecutionFailed(self.redact(&message)),
			other => other,
		}
	}
}

/// SearXNG-backed `web_search`, with a credential-free DuckDuckGo Lite
/// fallback for an unconfigured or unavailable instance.
pub struct WebSearchTool {
	config: SearxngConfig,
	client: reqwest::Client,
	timeout: Duration,
	fallback_endpoint: String,
}

impl WebSearchTool {
	pub fn new(config: SearxngConfig) -> Self {
		Self {
			config,
			// Endpoint is fixed by host config; a redirect could both break the
			// JSON parse (external bangs) and carry `Authorization` to another
			// origin, so redirects are surfaced as errors instead of followed.
			client: crate::tls::http_client_builder()
				.redirect(reqwest::redirect::Policy::none())
				.build()
				.expect("failed to construct web search HTTP client with preconfigured TLS"),
			timeout: REQUEST_TIMEOUT,
			fallback_endpoint: DUCKDUCKGO_LITE_URL.to_string(),
		}
	}

	/// Override the DuckDuckGo Lite endpoint. Host/test plumbing only: the tool
	/// schema never exposes it, so the model cannot redirect the fallback.
	#[must_use]
	pub fn with_fallback_endpoint(mut self, endpoint: impl Into<String>) -> Self {
		self.fallback_endpoint = endpoint.into();
		self
	}

	fn parse_args(&self, args: &Value) -> Result<SearchParams, ToolError> {
		let object = args
			.as_object()
			.ok_or_else(|| ToolError::InvalidArguments("Arguments must be an object".to_string()))?;
		for key in object.keys() {
			match key.as_str() {
				"query" | "recency" | "limit" | "num_search_results" | "max_tokens"
				| "temperature" => {},
				"endpoint" | "base_url" | "url" => {
					return Err(ToolError::InvalidArguments(format!(
						"'{key}' is not a tool parameter: the SearXNG endpoint comes from host \
						 settings (searxng.endpoint / SEARXNG_ENDPOINT)"
					)));
				},
				other => {
					return Err(ToolError::InvalidArguments(format!(
						"unknown parameter '{other}'; web_search accepts query, recency, limit, \
						 num_search_results, max_tokens, temperature"
					)));
				},
			}
		}
		let query = object
			.get("query")
			.and_then(Value::as_str)
			.ok_or_else(|| ToolError::InvalidArguments("Missing required parameter 'query'".to_string()))?;
		let recency = match object.get("recency") {
			None | Some(Value::Null) => None,
			Some(Value::String(value)) => Some(Recency::parse(value).ok_or_else(|| {
				ToolError::InvalidArguments(format!(
					"recency must be one of day, week, month, year; got '{value}'"
				))
			})?),
			Some(_) => {
				return Err(ToolError::InvalidArguments("recency must be a string".to_string()));
			},
		};
		// Upstream prefers `num_search_results` over `limit`; `max_tokens` and
		// `temperature` only reach LLM-mediated providers, so SearXNG ignores them.
		let requested = match object.get("num_search_results").or_else(|| object.get("limit")) {
			None | Some(Value::Null) => None,
			Some(value) => Some(value.as_f64().ok_or_else(|| {
				ToolError::InvalidArguments("num_search_results/limit must be a number".to_string())
			})?),
		};
		Ok(SearchParams {
			parsed: parse_search_query(query),
			query: query.to_string(),
			recency,
			num_results: clamp_num_results(requested),
		})
	}

	/// Validate the configured endpoint and return it without the trailing `/`,
	/// so both `/search` and `/config` resolve against the same canonical base.
	fn endpoint_base(&self) -> Result<String, ToolError> {
		let endpoint = self
			.config
			.endpoint
			.as_deref()
			.filter(|endpoint| !endpoint.trim().is_empty())
			.ok_or_else(|| ToolError::ExecutionFailed(NOT_CONFIGURED.to_string()))?;
		if endpoint.chars().any(char::is_control) {
			return Err(ToolError::ExecutionFailed(
				"searxng.endpoint must not contain control characters".to_string(),
			));
		}
		let base = endpoint.trim().trim_end_matches('/').to_string();
		let url = Url::parse(&base)
			.map_err(|error| ToolError::ExecutionFailed(format!("searxng.endpoint is not a valid URL: {error}")))?;
		if !matches!(url.scheme(), "http" | "https") {
			return Err(ToolError::ExecutionFailed(
				"searxng.endpoint must use the http or https scheme".to_string(),
			));
		}
		if !url.username().is_empty() || url.password().is_some() {
			return Err(ToolError::ExecutionFailed(
				"searxng.endpoint must not embed credentials; use searxng.basicUsername/\
				 searxng.basicPassword or searxng.token"
					.to_string(),
			));
		}
		if url.query().is_some() || url.fragment().is_some() {
			return Err(ToolError::ExecutionFailed(
				"searxng.endpoint must not carry a query string or fragment".to_string(),
			));
		}
		Ok(base)
	}

	/// Resolve `Authorization` following upstream precedence: Basic credentials
	/// (both-or-error) win over a Bearer token.
	fn resolve_auth(&self) -> Result<Option<String>, ToolError> {
		let username = self.config.basic_username.as_deref();
		let password = self.config.basic_password.as_deref();
		if username.is_some() || password.is_some() {
			let (Some(username), Some(password)) = (username, password) else {
				return Err(ToolError::ExecutionFailed(
					"SearXNG Basic auth requires both searxng.basicUsername and \
					 searxng.basicPassword, or SEARXNG_BASIC_USERNAME and SEARXNG_BASIC_PASSWORD."
						.to_string(),
				));
			};
			if username.contains(':') {
				return Err(ToolError::ExecutionFailed(
					"SearXNG Basic auth username cannot contain ':' because RFC 7617 uses it as \
					 the separator."
						.to_string(),
				));
			}
			if has_control_characters(username) || has_control_characters(password) {
				return Err(ToolError::ExecutionFailed(
					"SearXNG Basic auth credentials must not contain RFC 7617 control characters."
						.to_string(),
				));
			}
			let encoded = crate::eval::util::base64_encode(format!("{username}:{password}").as_bytes());
			return Ok(Some(format!("Basic {encoded}")));
		}
		let Some(token) = self.config.token.as_deref().filter(|token| !token.is_empty()) else {
			return Ok(None);
		};
		let authorization = format!("Bearer {token}");
		if header::HeaderValue::from_str(&authorization).is_err() {
			return Err(ToolError::ExecutionFailed(
				"SearXNG token is not a valid HTTP Authorization header value.".to_string(),
			));
		}
		Ok(Some(authorization))
	}

	fn validate_safesearch(&self) -> Result<(), ToolError> {
		if let Some(safesearch) = self.config.safesearch
			&& !(0..=2).contains(&safesearch)
		{
			return Err(ToolError::ExecutionFailed(
				"searxng.safesearch must be 0 (off), 1 (moderate), or 2 (strict).".to_string(),
			));
		}
		Ok(())
	}

	/// Resolve configured engine entries (canonical names or shortcuts like
	/// `ddg`) to canonical names; SearXNG's `engines=` accepts names only.
	/// Unknown entries pass through verbatim. Failures fall back to the raw
	/// entries, exactly like upstream.
	async fn resolve_engines(
		&self,
		raw: &str,
		base: &str,
		authorization: Option<&str>,
		redactor: &Redactor,
		token: &CancellationToken,
	) -> Option<String> {
		let entries: Vec<&str> = raw.split(',').map(str::trim).filter(|entry| !entry.is_empty()).collect();
		if entries.is_empty() {
			return None;
		}
		let cached = ENGINE_NAMES
			.lock()
			.ok()
			.and_then(|cache| cache.get(base).cloned());
		let names = match cached {
			Some(names) => Some(names),
			None => self.fetch_engine_names(base, authorization, redactor, token).await,
		};
		let Some(names) = names else {
			return Some(entries.join(","));
		};
		Some(
			entries
				.iter()
				.map(|entry| {
					names
						.get(&entry.to_ascii_lowercase())
						.cloned()
						.unwrap_or_else(|| (*entry).to_string())
				})
				.collect::<Vec<_>>()
				.join(","),
		)
	}

	/// Fetch and cache `{base}/config`'s engine names through the same bounded,
	/// cancellable, redacting transport as the search itself.
	async fn fetch_engine_names(
		&self,
		base: &str,
		authorization: Option<&str>,
		redactor: &Redactor,
		token: &CancellationToken,
	) -> Option<Arc<HashMap<String, String>>> {
		let url = Url::parse(&format!("{base}/config")).ok()?;
		let mut request = self.client.get(url).header(header::ACCEPT, "application/json");
		if let Some(authorization) = authorization {
			request = request.header(header::AUTHORIZATION, authorization.to_string());
		}
		let (status, body) = self.send(request, token, redactor).await.ok()?;
		if !status.is_success() {
			return None;
		}
		let payload: Value = serde_json::from_str(&body).ok()?;
		let mut names: HashMap<String, String> = HashMap::new();
		for engine in payload.get("engines").and_then(Value::as_array).into_iter().flatten() {
			let Some(name) = engine.get("name").and_then(Value::as_str) else {
				continue;
			};
			names.insert(name.to_ascii_lowercase(), name.to_string());
			if let Some(shortcut) = engine.get("shortcut").and_then(Value::as_str) {
				names.insert(shortcut.to_ascii_lowercase(), name.to_string());
			}
		}
		if names.is_empty() {
			return None;
		}
		let names = Arc::new(names);
		if let Ok(mut cache) = ENGINE_NAMES.lock() {
			cache.insert(base.to_string(), Arc::clone(&names));
		}
		Some(names)
	}

	fn request_url(
		&self,
		base: &str,
		params: &SearchParams,
		engines: Option<&str>,
	) -> Result<Url, ToolError> {
		let mut url = Url::parse(&format!("{base}/search")).map_err(|error| {
			ToolError::ExecutionFailed(format!("searxng.endpoint is not a valid URL: {error}"))
		})?;
		{
			let mut query = url.query_pairs_mut();
			let engine_query = strip_external_bangs(&format_scraper_query(&params.query));
			query.append_pair("q", &engine_query);
			query.append_pair("format", "json");
			query.append_pair("pageno", "1");
			if let Some(range) = params.recency.map(Recency::time_range) {
				query.append_pair("time_range", range);
			}
			for (key, value) in [("categories", self.config.categories.as_deref()), ("engines", engines)] {
				if let Some(value) = value.filter(|value| !value.trim().is_empty()) {
					query.append_pair(key, value);
				}
			}
			// `lang:` maps onto the native `language` param and overrides the
			// configured default.
			let language = params
				.parsed
				.lang
				.as_deref()
				.or(self.config.language.as_deref())
				.filter(|value| !value.trim().is_empty());
			if let Some(language) = language {
				query.append_pair("language", language);
			}
			if let Some(safesearch) = self.config.safesearch {
				query.append_pair("safesearch", &safesearch.to_string());
			}
		}
		Ok(url)
	}

	/// One bounded request. HTTP 5xx is classified before the body is read, so a
	/// large or broken 5xx body cannot change the outcome. A non-2xx body read
	/// failure (including a 4xx body over 4 MiB) stays fatal instead of becoming
	/// a fallback-eligible outage.
	async fn send(
		&self,
		request: reqwest::RequestBuilder,
		token: &CancellationToken,
		redactor: &Redactor,
	) -> Result<(StatusCode, String), SearXngFailure> {
		let response = tokio::select! {
			() = token.cancelled() => return Err(SearXngFailure::Fatal(ToolError::Aborted(
				"web search cancelled".to_string(),
			))),
			result = tokio::time::timeout(self.timeout, request.send()) => match result {
				Ok(Ok(response)) => response,
				Ok(Err(_)) | Err(_) => return Err(SearXngFailure::Unavailable),
			},
		};
		let status = response.status();
		if status.is_server_error() {
			return Err(SearXngFailure::Unavailable);
		}
		let body = tokio::select! {
			() = token.cancelled() => return Err(SearXngFailure::Fatal(ToolError::Aborted(
				"web search cancelled".to_string(),
			))),
			result = tokio::time::timeout(self.timeout, read_bounded_body(response)) => match result {
				Ok(Ok(body)) => body,
				Ok(Err(error)) => {
					let message = format!(
						"SearXNG request failed while reading the response: {}",
						redactor.redact(&error)
					);
					return Err(if status.is_success() {
						SearXngFailure::Unavailable
					} else {
						SearXngFailure::Fatal(ToolError::ExecutionFailed(message))
					});
				},
				Err(_) => {
					let message = format!("SearXNG request timed out after {}s", self.timeout.as_secs());
					return Err(if status.is_success() {
						SearXngFailure::Unavailable
					} else {
						SearXngFailure::Fatal(ToolError::ExecutionFailed(message))
					});
				},
			},
		};
		Ok((status, body))
	}

	/// One DuckDuckGo Lite search. HTML-only: no credentials are sent and
	/// redirects stay disabled, so the fixed lite endpoint cannot bounce the
	/// client to another origin.
	async fn duckduckgo_search(
		&self,
		params: &SearchParams,
		token: &CancellationToken,
	) -> Result<SearchResponse, ToolError> {
		let url = Url::parse(&self.fallback_endpoint).map_err(|error| {
			ToolError::ExecutionFailed(format!(
				"duckduckgo lite endpoint is not a valid URL: {error}"
			))
		})?;
		if !matches!(url.scheme(), "http" | "https") {
			return Err(ToolError::ExecutionFailed(
				"duckduckgo lite endpoint must use the http or https scheme".to_string(),
			));
		}
		let query = strip_external_bangs(&format_scraper_query(&params.query));
		let request = self
			.client
			.post(url)
			.header(header::ACCEPT, "text/html")
			.header(header::USER_AGENT, DUCKDUCKGO_USER_AGENT)
			.form(&[("q", query.as_str())]);
		let exchange = async {
			let response = request.send().await.map_err(|error| {
				ToolError::ExecutionFailed(format!("DuckDuckGo lite request failed: {error}"))
			})?;
			let status = response.status();
			if !status.is_success() {
				return Err(ToolError::ExecutionFailed(format!(
					"DuckDuckGo lite search returned HTTP {}",
					status.as_u16()
				)));
			}
			let body = read_capped_body(response, DUCKDUCKGO_MAX_HTML_BYTES)
				.await
				.map_err(ToolError::ExecutionFailed)?;
			Ok::<_, ToolError>(parse_duckduckgo_lite(&body))
		};
		let mut response = tokio::select! {
			() = token.cancelled() => Err(ToolError::Aborted("web search cancelled".to_string())),
			result = tokio::time::timeout(DUCKDUCKGO_TIMEOUT, exchange) => match result {
				Ok(result) => result,
				Err(_) => Err(ToolError::ExecutionFailed(format!(
					"DuckDuckGo lite request timed out after {}s",
					DUCKDUCKGO_TIMEOUT.as_secs()
				))),
			},
		}?;
		apply_constraints(&mut response, &params.parsed);
		response.sources.truncate(params.num_results);
		if response.sources.is_empty() {
			return Err(ToolError::ExecutionFailed(
				"DuckDuckGo lite returned no usable results.".to_string(),
			));
		}
		Ok(response)
	}

	async fn fallback_result(
		&self,
		params: &SearchParams,
		token: &CancellationToken,
		fallback_note: Option<String>,
	) -> Result<ToolResult, ToolError> {
		let mut response = self.duckduckgo_search(params, token).await?;
		if let Some(note) = fallback_note {
			response.notes.push(note);
		}
		if params.recency.is_some() {
			response.notes.push(
				"duckduckgo-free does not filter by recency; results may be older than requested."
					.to_string(),
			);
		}
		Ok(search_result(&response, "duckduckgo_free"))
	}

	fn parse_response(
		&self,
		status: StatusCode,
		body: String,
		limit: usize,
		parsed: &StructuredQuery,
		redactor: &Redactor,
	) -> Result<SearchResponse, SearXngFailure> {
		if !status.is_success() {
			return Err(SearXngFailure::Fatal(ToolError::ExecutionFailed(http_error_message(
				status,
				&redactor.redact(&body),
			))));
		}
		let payload: Value = serde_json::from_str(&body).map_err(|error| {
			SearXngFailure::Fatal(ToolError::ExecutionFailed(format!(
				"SearXNG response was not valid JSON: {error}"
			)))
		})?;
		let mut response = map_response(&payload, limit).map_err(|failure| match failure {
			SearXngFailure::Unavailable => SearXngFailure::Unavailable,
			SearXngFailure::Fatal(error) => SearXngFailure::Fatal(redactor.redact_error(error)),
		})?;

		// Lenient constraint pass: enforce site:/inurl:/intitle:/filetype:/date
		// directives the engine could not honor natively, relaxing any dimension
		// that would wipe out every result instead of returning nothing.
		apply_constraints(&mut response, parsed);

		if response.answer.is_none() && response.sources.is_empty() && response.related_questions.is_empty() {
			return Err(SearXngFailure::Fatal(ToolError::ExecutionFailed(
				"SearXNG returned no renderable search content.".to_string(),
			)));
		}
		Ok(response)
	}
}

#[async_trait]
impl Tool for WebSearchTool {
	fn name(&self) -> &str {
		"web_search"
	}

	fn description(&self) -> &str {
		// Verbatim `prompts/tools/web-search.md`.
		"Web search: current information beyond knowledge cutoff.\n\n<instruction>\n- SHOULD prefer \
		 primary sources (papers, official docs); corroborate key claims with multiple sources.\n- \
		 MUST link cited sources in final response.\n- NEVER use for programmatically accessible \
		 content or known URLs (GitHub repos/issues, known arXiv papers, Wikipedia pages, official \
		 docs) — `read` URL directly.\n- `query`: every provider supports Google-style \
		 `site:`/`-site:`, `after:`/`before:` (`YYYY-MM-DD`), `inurl:`, `intitle:`, `filetype:`, \
		 `\"exact phrase\"`, `-term`, `OR`. Map constraints to native filters when available; \
		 otherwise filter results leniently. If a constraint matches nothing, relax and report it; \
		 do not return zero results.\n</instruction>"
	}

	fn schema(&self) -> Value {
		json!({
			"type": "object",
			"properties": {
				"query": {
					"type": "string",
					"description": "Search query; Google-style site:/after:/inurl:/filetype: operators \
					 are forwarded to SearXNG."
				},
				"recency": {
					"type": "string",
					"enum": ["day", "week", "month", "year"],
					"description": "Restrict results by publication window (SearXNG maps week to month)."
				},
				"limit": {
					"type": "number",
					"description": "Maximum number of sources to return (clamped to 1-20, default 10)."
				},
				"num_search_results": {
					"type": "number",
					"description": "Provider-specific result count; takes precedence over limit."
				},
				"max_tokens": {
					"type": "number",
					"description": "LLM-mediated providers only; ignored by SearXNG."
				},
				"temperature": {
					"type": "number",
					"description": "LLM-mediated providers only; ignored by SearXNG."
				}
			},
			"required": ["query"],
			"additionalProperties": false
		})
	}

	async fn execute(&self, args: &Value, ctx: &AgentContext) -> Result<ToolResult, ToolError> {
		let params = self.parse_args(args)?;
		let token = ctx.cancellation_token.clone();
		if token.is_cancelled() {
			return Err(ToolError::Aborted("web search cancelled".to_string()));
		}
		// Control-only endpoints are configuration errors, not blank/unconfigured.
		if self.config.endpoint.as_deref().is_some_and(has_control_characters) {
			return Err(ToolError::ExecutionFailed(
				"searxng.endpoint must not contain control characters".to_string(),
			));
		}
		// A blank endpoint is the normal unconfigured case: no SearXNG syntax or
		// credentials to validate, so go straight to the free fallback.
		if !self.config.endpoint.as_deref().is_some_and(|endpoint| !endpoint.trim().is_empty()) {
			return self
				.fallback_result(
					&params,
					&token,
					Some("SearXNG endpoint not configured; used duckduckgo-free.".to_string()),
				)
				.await;
		}
		let base = self.endpoint_base()?;
		self.validate_safesearch()?;
		let authorization = self.resolve_auth()?;
		let redactor = Redactor::new(&self.config, authorization.as_deref());
		let engines = match self.config.engines.as_deref().filter(|raw| !raw.trim().is_empty()) {
			Some(raw) => self.resolve_engines(raw, &base, authorization.as_deref(), &redactor, &token).await,
			None => None,
		};
		let url = self.request_url(&base, &params, engines.as_deref())?;
		let mut request = self.client.get(url).header(header::ACCEPT, "application/json");
		if let Some(authorization) = &authorization {
			request = request.header(header::AUTHORIZATION, authorization.clone());
		}
		let (status, body) = match self.send(request, &token, &redactor).await {
			Ok(exchange) => exchange,
			Err(SearXngFailure::Unavailable) => {
				return self
					.fallback_result(
						&params,
						&token,
						Some("SearXNG was unavailable; fell back to duckduckgo-free.".to_string()),
					)
					.await;
			},
			Err(SearXngFailure::Fatal(error)) => return Err(error),
		};
		let response = match self.parse_response(status, body, params.num_results, &params.parsed, &redactor) {
			Ok(response) => response,
			Err(SearXngFailure::Unavailable) => {
				return self
					.fallback_result(
						&params,
						&token,
						Some("SearXNG was unavailable; fell back to duckduckgo-free.".to_string()),
					)
					.await;
			},
			Err(SearXngFailure::Fatal(error)) => return Err(error),
		};
		Ok(search_result(&response, "searxng"))
	}
}

/// Outcome of a SearXNG lookup. Only [`SearXngFailure::Unavailable`] is safe to
/// retry on the credential-free fallback; everything else is surfaced.
enum SearXngFailure {
	/// Transport/connect/timeout, HTTP 5xx (detected before the body read), or
	/// every routed engine failed with no usable result.
	Unavailable,
	/// Configuration-independent wire/payload failure, 3xx/4xx, or cancellation.
	Fatal(ToolError),
}

struct SearchParams {
	query:       String,
	parsed:      StructuredQuery,
	recency:     Option<Recency>,
	num_results: usize,
}

#[derive(Clone, Copy)]
enum Recency {
	Day,
	Week,
	Month,
	Year,
}

impl Recency {
	fn parse(value: &str) -> Option<Self> {
		match value {
			"day" => Some(Self::Day),
			"week" => Some(Self::Week),
			"month" => Some(Self::Month),
			"year" => Some(Self::Year),
			_ => None,
		}
	}

	/// SearXNG only accepts day/month/year; `week` maps to `month`.
	fn time_range(self) -> &'static str {
		match self {
			Self::Day => "day",
			Self::Week | Self::Month => "month",
			Self::Year => "year",
		}
	}
}

#[derive(Debug, Clone)]
struct SearchSource {
	title:          String,
	url:            String,
	snippet:        Option<String>,
	published_date: Option<String>,
	age_seconds:    Option<i64>,
}

impl ConstraintSource for SearchSource {
	fn constraint_url(&self) -> &str {
		&self.url
	}

	fn constraint_title(&self) -> &str {
		&self.title
	}

	fn constraint_time_ms(&self) -> Option<i64> {
		self.published_date.as_deref().and_then(parse_timestamp_ms)
	}
}

#[derive(Debug)]
struct SearchResponse {
	answer:            Option<String>,
	sources:           Vec<SearchSource>,
	related_questions: Vec<String>,
	notes:             Vec<String>,
}

/// Upstream `clampNumResults`: absent, zero, or NaN yields the default.
fn clamp_num_results(value: Option<f64>) -> usize {
	let Some(value) = value.filter(|value| *value != 0.0 && !value.is_nan()) else {
		return DEFAULT_NUM_RESULTS;
	};
	value.clamp(1.0, MAX_NUM_RESULTS as f64) as usize
}

fn has_control_characters(value: &str) -> bool {
	value
		.chars()
		.any(|character| character.is_control() || ('\u{7f}'..='\u{9f}').contains(&character))
}

/// Drop external bangs (`!!g`): SearXNG answers them with an HTTP redirect even
/// for JSON requests. Single-bang engine/category selectors stay, because the
/// instance resolves and strips them server-side.
fn strip_external_bangs(query: &str) -> String {
	query
		.split_whitespace()
		.filter(|part| !part.starts_with("!!"))
		.collect::<Vec<_>>()
		.join(" ")
}

async fn read_bounded_body(mut response: reqwest::Response) -> Result<String, String> {
	let mut buf: Vec<u8> = Vec::new();
	while let Some(chunk) = response.chunk().await.map_err(|error| error.to_string())? {
		if buf.len() + chunk.len() > MAX_RESPONSE_BYTES {
			return Err(format!("SearXNG response exceeded {MAX_RESPONSE_BYTES} bytes"));
		}
		buf.extend_from_slice(&chunk);
	}
	Ok(String::from_utf8_lossy(&buf).into_owned())
}

/// Read at most `cap` body bytes, then stop; the DDG lite page is parsed from
/// that bounded prefix, mirroring upstream's `MAX_HTML_BYTES` slice.
async fn read_capped_body(mut response: reqwest::Response, cap: usize) -> Result<String, String> {
	let mut buf: Vec<u8> = Vec::new();
	while buf.len() < cap {
		let Some(chunk) = response.chunk().await.map_err(|error| error.to_string())? else {
			break;
		};
		let take = chunk.len().min(cap - buf.len());
		buf.extend_from_slice(&chunk[..take]);
		if take < chunk.len() {
			break;
		}
	}
	Ok(String::from_utf8_lossy(&buf).into_owned())
}

fn search_result(response: &SearchResponse, backend: &str) -> ToolResult {
	ToolResult {
		tool_call_id: String::new(),
		content: format_for_llm(response),
		is_error: false,
		terminate: false,
		details: Some(json!({ "backend": backend, "sources": response.sources.len() })),
	}
}

/// Lenient constraint pass shared by both backends: relax any dimension that
/// would eliminate every result and report it instead of returning nothing.
fn apply_constraints(response: &mut SearchResponse, parsed: &StructuredQuery) {
	if !parsed.has_constraints || response.sources.is_empty() {
		return;
	}
	let (kept, dropped) = filter_source_indices(&response.sources, parsed);
	if kept.len() != response.sources.len() {
		response.sources = kept.iter().map(|index| response.sources[*index].clone()).collect();
	}
	for label in dropped {
		response
			.notes
			.push(format!("no results matched `{label}`; the constraint was relaxed"));
	}
}

// Upstream `freeWebSearch.ts` patterns. Captures are hard-bounded so malformed
// or adversarial HTML cannot force catastrophic backtracking.
static DDG_ANCHOR: LazyLock<Regex> = LazyLock::new(|| {
	Regex::new(r#"(?is)<a\b([^>]*?class=['"][^'"]*result-link[^'"]*['"][^>]*)>(.{0,512}?)</a>"#)
		.expect("DuckDuckGo anchor pattern is valid")
});
static DDG_HREF: LazyLock<Regex> =
	LazyLock::new(|| Regex::new(r#"(?i)href=['"]([^'"]+)['"]"#).expect("href pattern is valid"));
static DDG_SNIPPET: LazyLock<Regex> = LazyLock::new(|| {
	Regex::new(
		r#"(?is)<td\b[^>]*?class=['"][^'"]*result-snippet[^'"]*['"][^>]*>(.{0,2048}?)</td>"#,
	)
	.expect("DuckDuckGo snippet pattern is valid")
});
static HTML_TAG: LazyLock<Regex> =
	LazyLock::new(|| Regex::new(r"(?s)<[^>]*>").expect("HTML tag pattern is valid"));

/// One `<a class=result-link>` match with its byte span in the page.
struct DuckDuckGoRow {
	start: usize,
	end:   usize,
	title: String,
	url:   Option<String>,
}

/// Parse DuckDuckGo Lite HTML into sources. Each result's snippet is searched
/// only between its anchor and the next anchor, so a missing snippet cannot
/// shift later snippets onto earlier rows.
fn parse_duckduckgo_lite(html: &str) -> SearchResponse {
	// Keep every anchor, including ones later dropped, so their spans still
	// delimit the preceding row's snippet window.
	let mut rows = Vec::new();
	for captures in DDG_ANCHOR.captures_iter(html) {
		let Some(whole) = captures.get(0) else {
			continue;
		};
		rows.push(DuckDuckGoRow {
			start: whole.start(),
			end:   whole.end(),
			title: strip_html_tags(&captures[2]),
			url:   DDG_HREF
				.captures(&captures[1])
				.and_then(|href| resolve_duckduckgo_url(&href[1])),
		});
	}
	let mut sources = Vec::new();
	for (index, row) in rows.iter().enumerate() {
		let window_end = rows.get(index + 1).map_or(html.len(), |next| next.start).max(row.end);
		let snippet = DDG_SNIPPET
			.captures(&html[row.end..window_end])
			.map(|captures| strip_html_tags(&captures[1]))
			.map(|snippet| snippet.trim().to_string())
			.filter(|snippet| !snippet.is_empty());
		let Some(url) = row.url.as_ref() else {
			continue;
		};
		if row.title.is_empty() {
			continue;
		}
		sources.push(SearchSource {
			title: row.title.clone(),
			url: url.clone(),
			snippet,
			published_date: None,
			age_seconds: None,
		});
	}
	SearchResponse { answer: None, sources, related_questions: Vec::new(), notes: Vec::new() }
}

/// Strip tags after decoding entities, exactly like upstream `stripTags`: the
/// loop runs to a fixpoint and a trailing unclosed `<…` is dropped.
fn strip_html_tags(html: &str) -> String {
	let mut text = decode_html_entities(html);
	loop {
		if !HTML_TAG.is_match(&text) {
			break;
		}
		text = HTML_TAG.replace_all(&text, "").into_owned();
	}
	if let Some(index) = text.rfind('<')
		&& !text[index..].contains('>')
	{
		text.truncate(index);
	}
	text.split_whitespace().collect::<Vec<_>>().join(" ")
}

fn decode_html_entities(text: &str) -> String {
	let mut decoded = String::with_capacity(text.len());
	let mut rest = text;
	while let Some(index) = rest.find('&') {
		decoded.push_str(&rest[..index]);
		let tail = &rest[index + 1..];
		let end = tail
			.char_indices()
			.take(32)
			.find(|(_, character)| *character == ';')
			.map(|(position, _)| position);
		let Some(end) = end else {
			decoded.push('&');
			rest = tail;
			continue;
		};
		if let Some(character) = decode_entity(&tail[..end]) {
			decoded.push(character);
			rest = &tail[end + 1..];
		} else {
			decoded.push('&');
			rest = tail;
		}
	}
	decoded.push_str(rest);
	decoded
}

fn decode_entity(entity: &str) -> Option<char> {
	match entity.to_ascii_lowercase().as_str() {
		"amp" => Some('&'),
		"lt" => Some('<'),
		"gt" => Some('>'),
		"quot" => Some('"'),
		"apos" | "#39" => Some('\''),
		"nbsp" => Some(' '),
		_ => {
			let number = entity.strip_prefix('#')?;
			let code = match number.strip_prefix('x').or_else(|| number.strip_prefix('X')) {
				Some(hex) => u32::from_str_radix(hex, 16).ok()?,
				None => number.parse().ok()?,
			};
			char::from_u32(code)
		},
	}
}

/// Upstream `resolveResultUrl`: unwrap the `uddg=` redirect, normalize a
/// protocol-relative href, and keep only absolute http(s) URLs.
fn resolve_duckduckgo_url(href: &str) -> Option<String> {
	let candidate = match duckduckgo_redirect_target(href) {
		Some(encoded) => percent_decode(encoded).unwrap_or_else(|| href.to_string()),
		None if href.starts_with("//") => format!("https:{href}"),
		None => href.to_string(),
	};
	let url = Url::parse(&candidate).ok()?;
	matches!(url.scheme(), "http" | "https").then(|| url.to_string())
}

fn duckduckgo_redirect_target(href: &str) -> Option<&str> {
	let start = href.find("uddg=")?;
	if !matches!(href[..start].chars().next_back(), Some('?' | '&')) {
		return None;
	}
	let tail = &href[start + "uddg=".len()..];
	Some(tail.split(|character| matches!(character, '&' | '#')).next().unwrap_or(tail))
}

fn percent_decode(input: &str) -> Option<String> {
	let bytes = input.as_bytes();
	let mut decoded = Vec::with_capacity(bytes.len());
	let mut index = 0;
	while index < bytes.len() {
		if bytes[index] == b'%' {
			let hex = std::str::from_utf8(bytes.get(index + 1..index + 3)?).ok()?;
			decoded.push(u8::from_str_radix(hex, 16).ok()?);
			index += 3;
		} else {
			decoded.push(bytes[index]);
			index += 1;
		}
	}
	String::from_utf8(decoded).ok()
}

fn http_error_message(status: StatusCode, body: &str) -> String {
	if body_is_quota_exhausted(body) {
		return "searxng: credits exhausted".to_string();
	}
	match status.as_u16() {
		401 => "searxng: 401 unauthorized".to_string(),
		402 => "searxng: 402 credits exhausted".to_string(),
		403 => "searxng: 403 forbidden".to_string(),
		code => format!(
			"SearXNG API error ({code}): {}",
			truncate_text(body.trim(), ERROR_BODY_MAX_CHARS)
		),
	}
}

/// Upstream `CREDIT_BODY_PATTERN`: `credits?\s*(exhausted|exceeded)|quota|insufficient`.
fn body_is_quota_exhausted(body: &str) -> bool {
	let lower = body.to_ascii_lowercase();
	if lower.contains("quota") || lower.contains("insufficient") {
		return true;
	}
	let mut rest = lower.as_str();
	while let Some(index) = rest.find("credit") {
		let tail = rest[index + "credit".len()..].trim_start_matches('s');
		let tail = tail.trim_start();
		if tail.starts_with("exhausted") || tail.starts_with("exceeded") {
			return true;
		}
		rest = &rest[index + "credit".len()..];
	}
	false
}

fn map_response(payload: &Value, limit: usize) -> Result<SearchResponse, SearXngFailure> {
	let results: &[Value] = match payload.get("results") {
		None | Some(Value::Null) => &[],
		Some(Value::Array(items)) => items.as_slice(),
		Some(_) => {
			return Err(SearXngFailure::Fatal(ToolError::ExecutionFailed(
				"SearXNG response field `results` is not an array".to_string(),
			)));
		},
	};
	let now_ms = now_millis();
	let mut sources = Vec::new();
	for result in results {
		let Some(url) = result.get("url").and_then(Value::as_str).filter(|url| !url.is_empty()) else {
			continue;
		};
		let title = result
			.get("title")
			.and_then(Value::as_str)
			.and_then(non_empty)
			.unwrap_or(url);
		// Upstream reads `content ?? snippet`: a present-but-blank content wins
		// over the legacy `snippet` field instead of falling through to it.
		let snippet = result
			.get("content")
			.or_else(|| result.get("snippet"))
			.and_then(Value::as_str)
			.and_then(non_empty);
		let published_date = ["publishedDate", "published_date"]
			.iter()
			.find_map(|key| result.get(*key).and_then(Value::as_str).and_then(non_empty));
		let age_seconds = published_date
			.and_then(parse_timestamp_ms)
			.map(|published_ms| (now_ms - published_ms) / 1_000);
		sources.push(SearchSource {
			title: title.to_string(),
			url: url.to_string(),
			snippet: snippet.map(str::to_string),
			published_date: published_date.map(str::to_string),
			age_seconds,
		});
	}
	let sources: Vec<SearchSource> = sources.into_iter().take(limit).collect();
	if sources.is_empty() && unresponsive_failures(payload).is_some() {
		return Err(SearXngFailure::Unavailable);
	}
	Ok(SearchResponse {
		answer: format_answers(payload.get("answers")),
		sources,
		related_questions: string_list(payload.get("suggestions")),
		notes: Vec::new(),
	})
}

fn now_millis() -> i64 {
	SystemTime::now()
		.duration_since(UNIX_EPOCH)
		.map_or(0, |duration| i64::try_from(duration.as_millis()).unwrap_or(i64::MAX))
}

fn non_empty(text: &str) -> Option<&str> {
	let trimmed = text.trim();
	(!trimmed.is_empty()).then_some(trimmed)
}

fn string_list(value: Option<&Value>) -> Vec<String> {
	value
		.and_then(Value::as_array)
		.map(|items| {
			items
				.iter()
				.filter_map(Value::as_str)
				.filter(|text| !text.trim().is_empty())
				.map(str::to_string)
				.collect()
		})
		.unwrap_or_default()
}

fn unresponsive_failures(payload: &Value) -> Option<String> {
	let engines = payload.get("unresponsive_engines")?.as_array()?;
	let parts: Vec<String> = engines
		.iter()
		.filter_map(|entry| {
			let pair = entry.as_array()?;
			let engine = pair.first()?.as_str()?;
			let reason = pair.get(1).and_then(Value::as_str).unwrap_or_default();
			Some(format!("{engine}: {reason}"))
		})
		.collect();
	(!parts.is_empty()).then(|| parts.join("; "))
}

fn format_answers(answers: Option<&Value>) -> Option<String> {
	let answers = answers?.as_array()?;
	let mut texts = Vec::new();
	for answer in answers {
		if let Some(text) = extract_answer_text(answer) {
			texts.push(text);
		}
		if texts.len() == 3 {
			break;
		}
	}
	(!texts.is_empty()).then(|| texts.join("\n\n"))
}

/// Upstream `extractAnswerText`: legacy strings, structured `answer`, up to
/// three translation rows, and the weather plugin's `current` block.
fn extract_answer_text(answer: &Value) -> Option<String> {
	if let Some(text) = answer.as_str() {
		return non_empty(text).map(str::to_string);
	}
	let record = answer.as_object()?;
	if let Some(text) = record.get("answer").and_then(Value::as_str).and_then(non_empty) {
		return Some(text.to_string());
	}
	if let Some(translations) = record.get("translations").and_then(Value::as_array) {
		let texts: Vec<&str> = translations
			.iter()
			.filter_map(|item| item.get("text").and_then(Value::as_str).and_then(non_empty))
			.take(3)
			.collect();
		if !texts.is_empty() {
			return Some(texts.join("\n"));
		}
	}
	let current = record.get("current")?.as_object()?;
	if let Some(summary) = current.get("summary").and_then(Value::as_str).and_then(non_empty) {
		return Some(summary.to_string());
	}
	let location = current
		.get("location")
		.and_then(|location| location.get("name"))
		.and_then(Value::as_str)
		.and_then(non_empty)
		.map(str::to_string);
	let temperature = current.get("temperature").and_then(|temperature| {
		let value = match temperature.get("val")? {
			Value::String(value) => value.trim().to_string(),
			other => other.as_f64()?.to_string(),
		};
		if value.is_empty() {
			return None;
		}
		let unit = temperature.get("unit").and_then(Value::as_str).unwrap_or_default();
		Some(format!("{value}{unit}"))
	});
	let condition = current
		.get("condition")
		.and_then(Value::as_str)
		.and_then(non_empty)
		.map(str::to_string);
	let parts: Vec<String> = [location, temperature, condition].into_iter().flatten().collect();
	(!parts.is_empty()).then(|| parts.join(": "))
}

/// Upstream `formatForLLM`: relaxation notes lead, then answers, sources with
/// age (or the raw date), and related questions.
fn format_for_llm(response: &SearchResponse) -> String {
	let mut parts: Vec<String> = Vec::new();
	for note in &response.notes {
		parts.push(format!("Note: {note}"));
	}
	if let Some(answer) = &response.answer {
		parts.push(answer.clone());
		if !response.sources.is_empty() {
			parts.push("\n## Sources".to_string());
			parts.push(format_count("source", response.sources.len()));
		}
	}
	for (index, source) in response.sources.iter().enumerate() {
		let age = source
			.age_seconds
			.and_then(format_age)
			.or_else(|| source.published_date.clone());
		let age_part = age.map(|age| format!(" ({age})")).unwrap_or_default();
		parts.push(format!(
			"[{}] {}{age_part}\n    {}",
			index + 1,
			source.title,
			source.url
		));
		if let Some(snippet) = &source.snippet {
			parts.push(format!("    {}", truncate_text(snippet, SNIPPET_MAX_CHARS)));
		}
	}
	if !response.related_questions.is_empty() {
		parts.push("\n## Related".to_string());
		parts.push(format_count("question", response.related_questions.len()));
		for question in &response.related_questions {
			parts.push(format!("- {question}"));
		}
	}
	parts.join("\n")
}

/// Upstream `formatAge`; returns `None` for non-positive ages so the raw
/// publish date is shown instead.
fn format_age(age_seconds: i64) -> Option<String> {
	if age_seconds <= 0 {
		return None;
	}
	let minutes = age_seconds / 60;
	let hours = minutes / 60;
	let days = hours / 24;
	let weeks = days / 7;
	let months = days / 30;
	Some(if months > 0 {
		format!("{months}mo ago")
	} else if weeks > 0 {
		format!("{weeks}w ago")
	} else if days > 0 {
		format!("{days}d ago")
	} else if hours > 0 {
		format!("{hours}h ago")
	} else if minutes > 0 {
		format!("{minutes}m ago")
	} else {
		"just now".to_string()
	})
}

fn format_count(label: &str, count: usize) -> String {
	let suffix = if count == 1 { "" } else { "s" };
	format!("{count} {label}{suffix}")
}

fn truncate_text(text: &str, max_chars: usize) -> String {
	if text.chars().count() <= max_chars {
		return text.to_string();
	}
	let mut truncated: String = text.chars().take(max_chars.saturating_sub(1)).collect();
	truncated.push('…');
	truncated
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn clamps_result_count_like_upstream() {
		assert_eq!(clamp_num_results(None), DEFAULT_NUM_RESULTS);
		assert_eq!(clamp_num_results(Some(0.0)), DEFAULT_NUM_RESULTS);
		assert_eq!(clamp_num_results(Some(f64::NAN)), DEFAULT_NUM_RESULTS);
		assert_eq!(clamp_num_results(Some(-4.0)), 1);
		assert_eq!(clamp_num_results(Some(3.7)), 3);
		assert_eq!(clamp_num_results(Some(99.0)), MAX_NUM_RESULTS);
	}

	#[test]
	fn strips_only_external_bangs() {
		assert_eq!(strip_external_bangs("!!g rust async"), "rust async");
		assert_eq!(strip_external_bangs("!ddg rust !!ddg async"), "!ddg rust async");
	}

	#[test]
	fn basic_auth_wins_over_bearer() {
		let tool = WebSearchTool::new(SearxngConfig {
			token: Some("bearer".to_string()),
			basic_username: Some("user".to_string()),
			basic_password: Some("pass".to_string()),
			..SearxngConfig::default()
		});
		assert_eq!(tool.resolve_auth().unwrap().as_deref(), Some("Basic dXNlcjpwYXNz"));
		let half = WebSearchTool::new(SearxngConfig {
			basic_username: Some("user".to_string()),
			..SearxngConfig::default()
		});
		assert!(half.resolve_auth().is_err());
		let invalid_bearer = WebSearchTool::new(SearxngConfig {
			token: Some("bad\ntoken".to_string()),
			..SearxngConfig::default()
		});
		assert!(invalid_bearer.resolve_auth().is_err(), "a control character cannot form a header");
	}

	#[test]
	fn redacts_resolved_authorization_material() {
		let config = SearxngConfig {
			token: Some("bearer-secret".to_string()),
			basic_username: Some("user".to_string()),
			basic_password: Some("pass-secret".to_string()),
			..SearxngConfig::default()
		};
		let authorization = "Basic dXNlcjpwYXNzLXNlY3JldA==";
		let redactor = Redactor::new(&config, Some(authorization));
		let echoed = format!(
			"proxy echoed Authorization: {authorization} and payload dXNlcjpwYXNzLXNlY3JldA== token \
			 bearer-secret password pass-secret"
		);
		let redacted = redactor.redact(&echoed);
		assert!(!redacted.contains(authorization), "{redacted}");
		assert!(!redacted.contains("dXNlcjpwYXNzLXNlY3JldA=="), "{redacted}");
		assert!(!redacted.contains("bearer-secret") && !redacted.contains("pass-secret"), "{redacted}");
		assert_eq!(redacted.matches("[redacted]").count(), 4, "{redacted}");
	}

	#[test]
	fn maps_unresponsive_engines_to_unavailable() {
		let payload = json!({
			"results": [],
			"unresponsive_engines": [["google", "proxy said pass-secret is wrong"]]
		});
		let failure = map_response(&payload, DEFAULT_NUM_RESULTS).expect_err("no usable results");
		assert!(matches!(failure, SearXngFailure::Unavailable));
	}

	#[test]
	fn request_url_canonicalizes_query_language_and_engines() {
		let tool = WebSearchTool::new(SearxngConfig {
			endpoint: Some("http://127.0.0.1:9/searxng/".to_string()),
			language: Some("en".to_string()),
			..SearxngConfig::default()
		});
		let args = json!({ "query": "site:github.com/anthropics rust lang:de", "recency": "day" });
		let params = tool.parse_args(&args).expect("arguments parse");
		let base = tool.endpoint_base().expect("endpoint validates");
		assert_eq!(base, "http://127.0.0.1:9/searxng");
		let url = tool.request_url(&base, &params, Some("Google")).expect("url builds");
		assert_eq!(url.path(), "/searxng/search");
		let pairs: HashMap<String, String> =
			url.query_pairs().map(|(key, value)| (key.into_owned(), value.into_owned())).collect();
		assert_eq!(pairs.get("q").map(String::as_str), Some("rust github.com/anthropics"));
		assert_eq!(pairs.get("format").map(String::as_str), Some("json"));
		assert_eq!(pairs.get("time_range").map(String::as_str), Some("day"));
		assert_eq!(pairs.get("engines").map(String::as_str), Some("Google"));
		assert_eq!(pairs.get("language").map(String::as_str), Some("de"), "lang: overrides the configured language");
	}

	#[test]
	fn renders_age_and_notes_ahead_of_sources() {
		let response = SearchResponse {
			answer: None,
			sources: vec![SearchSource {
				title: "Title".to_string(),
				url: "https://example.org/a".to_string(),
				snippet: Some("snippet".to_string()),
				published_date: Some("2020-01-01T00:00:00Z".to_string()),
				age_seconds: Some(3 * 86_400),
			}],
			related_questions: Vec::new(),
			notes: vec!["no results matched `site:example.com`; the constraint was relaxed".to_string()],
		};
		let content = format_for_llm(&response);
		assert!(
			content.starts_with("Note: no results matched `site:example.com`; the constraint was relaxed\n[1] Title (3d ago)"),
			"{content}"
		);
		assert!(content.contains("\n    https://example.org/a\n    snippet"), "{content}");
	}
}
