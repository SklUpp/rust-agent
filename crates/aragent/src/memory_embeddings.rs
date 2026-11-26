//! Optional embedding providers for memory recall.
//!
//! Two providers share one [`EmbeddingProvider`] contract:
//! - [`HttpEmbeddingProvider`] posts to an OpenAI-compatible
//!   `POST <base>/v1/embeddings` endpoint (9Router, OpenAI, local gateways).
//!   It is async and built on the workspace's only HTTP stack (`reqwest` +
//!   rustls via [`pi_agent::http_client_builder`]).
//! - `FastEmbedProvider` runs `fastembed` locally behind the
//!   `local-embeddings` feature. Model files download on first use and ONNX
//!   Runtime loads dynamically, so a missing runtime or offline host is a
//!   provider error, never a process error.
//!
//! Embeddings are an *optional accelerator*. Call [`embed_best_effort`] from
//! recall paths: a disabled provider, an offline endpoint, a rejected request
//! or a malformed payload all collapse to `None`, and lexical (FTS5/LIKE)
//! recall must still answer. Credentials are never printed: `Debug` redacts
//! the API key and error text drops request URLs, truncates bodies and masks
//! registered secrets.
//!
//! `ponytail:` one request carries the whole batch; chunking belongs to the
//! caller because the right batch size is bounded by the provider's request
//! limit, not by this module. Add `dimensions` only when a provider needs
//! shorter vectors; it is otherwise omitted from the request.

use std::{
	fmt,
	time::Duration,
};

use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use crate::auth::known_secrets;

/// Default per-request timeout for remote embedding calls.
pub const DEFAULT_HTTP_TIMEOUT: Duration = Duration::from_secs(30);

/// Maximum characters of a provider error body echoed in an error message.
const ERROR_BODY_LIMIT: usize = 300;

pub type EmbeddingResult<T> = Result<T, EmbeddingError>;

/// Recoverable embedding failure.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EmbeddingError {
	/// Not configured, offline, rejected by the endpoint, or the local model is
	/// unavailable. Callers must fall back to lexical recall.
	Unavailable(String),
	/// The endpoint answered, but the payload was not usable embeddings.
	InvalidResponse(String),
}

impl fmt::Display for EmbeddingError {
	fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
		match self {
			Self::Unavailable(message) | Self::InvalidResponse(message) => {
				formatter.write_str(message)
			},
		}
	}
}

impl std::error::Error for EmbeddingError {}

/// Async embedding contract shared by remote and local providers.
#[async_trait]
pub trait EmbeddingProvider: Send + Sync {
	/// Model identifier as it would be stored alongside the vectors.
	fn model(&self) -> &str;

	/// Width of returned vectors when known up front.
	fn dimensions(&self) -> Option<usize> {
		None
	}

	/// Embed every text, preserving input order.
	async fn embed(&self, texts: &[String]) -> EmbeddingResult<Vec<Vec<f32>>>;

	/// Like [`Self::embed`], but any failure becomes `None` so recall can stay
	/// lexical. Prefer this on the recall path.
	async fn embed_optional(&self, texts: &[String]) -> Option<Vec<Vec<f32>>> {
		self.embed(texts).await.ok()
	}
}

/// Run an optional provider without letting embeddings fail recall.
///
/// `None` provider, empty input is an empty success (not an error), and any
/// provider error is swallowed. Callers that need the reason should call
/// [`EmbeddingProvider::embed`] directly and log the error there.
pub async fn embed_best_effort(
	provider: Option<&dyn EmbeddingProvider>,
	texts: &[String],
) -> Option<Vec<Vec<f32>>> {
	let provider = provider?;
	provider.embed_optional(texts).await
}

/// Blocking variant for synchronous SQLite/CLI paths. Runs the provider on a
/// scoped thread with its own current-thread runtime, so callers already inside
/// an async runtime do not panic and any provider/thread failure is just `None`.
#[must_use]
pub fn embed_best_effort_blocking(
	provider: Option<&dyn EmbeddingProvider>,
	texts: &[String],
) -> Option<Vec<Vec<f32>>> {
	let provider = provider?;
	if texts.is_empty() {
		return Some(Vec::new());
	}
	std::thread::scope(|scope| {
		scope
			.spawn(|| -> Option<Vec<Vec<f32>>> {
				let runtime = tokio::runtime::Builder::new_current_thread()
					.enable_all()
					.build()
					.ok()?;
				runtime.block_on(provider.embed_optional(texts))
			})
			.join()
			.ok()
			.flatten()
	})
}

/// Configuration for an OpenAI-compatible embeddings endpoint.
#[derive(Clone)]
pub struct HttpEmbeddingConfig {
	/// Base URL, with or without `/v1`. A full `/embeddings` URL is used as-is;
	/// otherwise `/v1/embeddings` is appended. Query strings are not supported.
	pub base_url: String,
	/// Bearer credential. Resolved by the caller (config/env); never logged.
	pub api_key: Option<String>,
	/// Served model name, e.g. `text-embedding-3-small`.
	pub model: String,
	/// Requested vector width, forwarded only when set.
	pub dimensions: Option<usize>,
	pub timeout: Duration,
}

impl HttpEmbeddingConfig {
	#[must_use]
	pub fn new(base_url: impl Into<String>, model: impl Into<String>) -> Self {
		Self {
			base_url:  base_url.into(),
			api_key:   None,
			model:     model.into(),
			dimensions: None,
			timeout:   DEFAULT_HTTP_TIMEOUT,
		}
	}

	#[must_use]
	pub fn with_api_key(mut self, api_key: impl Into<String>) -> Self {
		self.api_key = Some(api_key.into());
		self
	}

	#[must_use]
	pub fn with_dimensions(mut self, dimensions: usize) -> Self {
		self.dimensions = Some(dimensions);
		self
	}

	#[must_use]
	pub fn with_timeout(mut self, timeout: Duration) -> Self {
		self.timeout = timeout;
		self
	}
}

impl fmt::Debug for HttpEmbeddingConfig {
	fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
		formatter
			.debug_struct("HttpEmbeddingConfig")
			.field("base_url", &redact_url(&self.base_url))
			.field("api_key", &self.api_key.as_ref().map(|_| "<redacted>"))
			.field("model", &self.model)
			.field("dimensions", &self.dimensions)
			.field("timeout", &self.timeout)
			.finish()
	}
}

/// OpenAI-compatible `/v1/embeddings` client.
pub struct HttpEmbeddingProvider {
	client: reqwest::Client,
	endpoint: String,
	api_key: Option<String>,
	model: String,
	dimensions: Option<usize>,
}

impl fmt::Debug for HttpEmbeddingProvider {
	fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
		formatter
			.debug_struct("HttpEmbeddingProvider")
			.field("endpoint", &redact_url(&self.endpoint))
			.field("api_key", &self.api_key.as_ref().map(|_| "<redacted>"))
			.field("model", &self.model)
			.field("dimensions", &self.dimensions)
			.finish()
	}
}

impl HttpEmbeddingProvider {
	/// Build the client. Configuration problems are reported as
	/// [`EmbeddingError::Unavailable`] so callers can disable embeddings and
	/// keep lexical recall.
	pub fn new(config: HttpEmbeddingConfig) -> EmbeddingResult<Self> {
		let endpoint = normalize_endpoint(&config.base_url)?;
		let model = config.model.trim().to_owned();
		if model.is_empty() {
			return Err(EmbeddingError::Unavailable("embedding model is not configured".into()));
		}
		let api_key = config.api_key.filter(|key| !key.trim().is_empty());
		if let Some(key) = &api_key {
			known_secrets::register(key);
		}
		let client = pi_agent::http_client_builder()
			.user_agent(concat!("aragent/", env!("CARGO_PKG_VERSION")))
			.timeout(config.timeout)
			.build()
			.map_err(|error| {
				EmbeddingError::Unavailable(format!("embedding HTTP client unavailable: {error}"))
			})?;
		Ok(Self { client, endpoint, api_key, model, dimensions: config.dimensions })
	}

	/// Endpoint after `/v1/embeddings` normalization.
	#[must_use]
	pub fn endpoint(&self) -> &str {
		&self.endpoint
	}
}

#[async_trait]
impl EmbeddingProvider for HttpEmbeddingProvider {
	fn model(&self) -> &str {
		&self.model
	}

	fn dimensions(&self) -> Option<usize> {
		self.dimensions
	}

	async fn embed(&self, texts: &[String]) -> EmbeddingResult<Vec<Vec<f32>>> {
		if texts.is_empty() {
			return Ok(Vec::new());
		}
		let body =
			EmbeddingRequest { model: &self.model, input: texts, dimensions: self.dimensions };
		let mut request = self.client.post(&self.endpoint).json(&body);
		if let Some(key) = &self.api_key {
			request = request.bearer_auth(key);
		}
		let response = request.send().await.map_err(|error| {
			// `without_url` keeps credentials that a caller put in the URL out of
			// the message; registered secrets are masked as well.
			let message = error.without_url().to_string();
			EmbeddingError::Unavailable(format!(
				"embedding request failed: {}",
				redact_text(&message, self.api_key.as_deref())
			))
		})?;
		let status = response.status();
		if !status.is_success() {
			let body = response.text().await.unwrap_or_default();
			return Err(EmbeddingError::Unavailable(format!(
				"embedding endpoint returned HTTP {status}: {}",
				excerpt(&body, self.api_key.as_deref())
			)));
		}
		let payload: EmbeddingsResponse = response.json().await.map_err(|error| {
			let message = error.without_url().to_string();
			EmbeddingError::InvalidResponse(format!(
				"invalid embedding response: {}",
				redact_text(&message, self.api_key.as_deref())
			))
		})?;
		assemble_embeddings(payload, texts.len(), self.dimensions)
	}
}

#[derive(Serialize)]
struct EmbeddingRequest<'a> {
	model:      &'a str,
	input:      &'a [String],
	#[serde(skip_serializing_if = "Option::is_none")]
	dimensions: Option<usize>,
}

#[derive(Deserialize)]
struct EmbeddingsResponse {
	data: Vec<EmbeddingObject>,
}

#[derive(Deserialize)]
struct EmbeddingObject {
	embedding: Vec<f32>,
	#[serde(default)]
	index: Option<usize>,
}

/// Normalize an OpenAI-compatible base URL to the full embeddings endpoint.
fn normalize_endpoint(raw: &str) -> EmbeddingResult<String> {
	let base = raw.trim().trim_end_matches('/');
	if base.is_empty() {
		return Err(EmbeddingError::Unavailable("embedding endpoint is not configured".into()));
	}
	if !(base.starts_with("http://") || base.starts_with("https://")) {
		return Err(EmbeddingError::Unavailable(
			"embedding endpoint must be an http(s) URL".into(),
		));
	}
	if base.ends_with("/embeddings") {
		return Ok(base.to_owned());
	}
	if base.ends_with("/v1") {
		return Ok(format!("{base}/embeddings"));
	}
	Ok(format!("{base}/v1/embeddings"))
}

/// Reorder a provider payload by its `index` field and validate every vector.
fn assemble_embeddings(
	response: EmbeddingsResponse,
	expected: usize,
	dimensions: Option<usize>,
) -> EmbeddingResult<Vec<Vec<f32>>> {
	if response.data.len() != expected {
		return Err(EmbeddingError::InvalidResponse(format!(
			"expected {expected} embeddings, received {}",
			response.data.len()
		)));
	}
	let mut slots: Vec<Option<Vec<f32>>> = vec![None; expected];
	for (position, object) in response.data.into_iter().enumerate() {
		let index = object.index.unwrap_or(position);
		if index >= expected {
			return Err(EmbeddingError::InvalidResponse(format!(
				"embedding index {index} is out of range for {expected} vectors"
			)));
		}
		if slots[index].is_some() {
			return Err(EmbeddingError::InvalidResponse(format!(
				"embedding index {index} appears more than once"
			)));
		}
		slots[index] = Some(object.embedding);
	}

	let mut vectors = Vec::with_capacity(expected);
	let mut width = None;
	for (index, slot) in slots.into_iter().enumerate() {
		let vector = slot.ok_or_else(|| {
			EmbeddingError::InvalidResponse(format!("embedding index {index} is missing"))
		})?;
		if vector.is_empty() {
			return Err(EmbeddingError::InvalidResponse(format!(
				"embedding index {index} is empty"
			)));
		}
		if let Some(declared) = dimensions
			&& vector.len() != declared
		{
			return Err(EmbeddingError::InvalidResponse(format!(
				"embedding index {index} has {} dimensions, expected {declared}",
				vector.len()
			)));
		}
		match width {
			Some(width) if vector.len() != width => {
				return Err(EmbeddingError::InvalidResponse(format!(
					"embedding index {index} has {} dimensions, earlier vectors have {width}",
					vector.len()
				)));
			},
			None => width = Some(vector.len()),
			_ => {},
		}
		if vector.iter().any(|value| !value.is_finite()) {
			return Err(EmbeddingError::InvalidResponse(format!(
				"embedding index {index} contains a non-finite value"
			)));
		}
		vectors.push(vector);
	}
	Ok(vectors)
}

/// Mask registered secrets plus an explicitly supplied API key, including keys
/// shorter than the global redactor's minimum pattern length.
fn redact_text(text: &str, api_key: Option<&str>) -> String {
	let explicit = api_key.filter(|key| !key.trim().is_empty());
	let masked = match explicit {
		Some(key) => text.replace(key, "<redacted>"),
		None => text.to_string(),
	};
	known_secrets::redact(&masked).into_owned()
}

/// Keep scheme/host/port/path and mask URL userinfo and query parameters.
fn redact_url(raw: &str) -> String {
	let Ok(mut url) = reqwest::Url::parse(raw) else {
		let without_suffix = raw.split(|ch| ch == '?' || ch == '#').next().unwrap_or(raw);
		return match without_suffix.split_once("://") {
			Some((scheme, rest)) if rest.contains('@') => {
				let host = rest.rsplit_once('@').map_or(rest, |(_, host)| host);
				format!("{scheme}://<redacted>@{host}")
			},
			_ => without_suffix.to_string(),
		};
	};
	if !url.username().is_empty() {
		let _ = url.set_username("redacted");
	}
	if url.password().is_some() {
		let _ = url.set_password(Some("redacted"));
	}
	if url.query().is_some() {
		url.set_query(Some("redacted"));
	}
	url.set_fragment(None);
	url.to_string()
}

/// Human-sized, secret-masked copy of an error body.
fn excerpt(body: &str, api_key: Option<&str>) -> String {
	let redacted = redact_text(body, api_key);
	let trimmed = redacted.trim();
	let mut out: String = trimmed.chars().take(ERROR_BODY_LIMIT).collect();
	if trimmed.chars().count() > ERROR_BODY_LIMIT {
		out.push_str("...");
	}
	if out.is_empty() {
		out.push_str("<empty response>");
	}
	out
}

#[cfg(feature = "local-embeddings")]
mod local {
	use std::{
		path::PathBuf,
		sync::{Arc, Mutex},
	};

	use async_trait::async_trait;
	use fastembed::{EmbeddingModel, TextEmbedding, TextInitOptions};

	use super::{EmbeddingError, EmbeddingProvider, EmbeddingResult};

	/// Local `fastembed` configuration. `model` accepts the `EmbeddingModel`
	/// variant name, e.g. `BGESmallENV15` or `AllMiniLML6V2`.
	#[derive(Debug, Clone)]
	pub struct FastEmbedConfig {
		pub model:     String,
		pub cache_dir: Option<PathBuf>,
	}

	impl Default for FastEmbedConfig {
		fn default() -> Self {
			Self { model: "BGESmallENV15".to_owned(), cache_dir: None }
		}
	}

	impl FastEmbedConfig {
		#[must_use]
		pub fn new(model: impl Into<String>) -> Self {
			Self { model: model.into(), cache_dir: None }
		}

		#[must_use]
		pub fn with_cache_dir(mut self, cache_dir: impl Into<PathBuf>) -> Self {
			self.cache_dir = Some(cache_dir.into());
			self
		}
	}

	/// `fastembed` text embedder running on ONNX Runtime.
	pub struct FastEmbedProvider {
		model_name: String,
		dimensions: Option<usize>,
		embedding: Arc<Mutex<TextEmbedding>>,
	}

	impl std::fmt::Debug for FastEmbedProvider {
		fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
			formatter
				.debug_struct("FastEmbedProvider")
				.field("model_name", &self.model_name)
				.field("dimensions", &self.dimensions)
				.finish_non_exhaustive()
		}
	}

	impl FastEmbedProvider {
		/// Load a model. Missing files trigger the fastembed download; offline
		/// hosts, missing ONNX Runtime and unknown model names all surface as
		/// [`EmbeddingError::Unavailable`].
		pub fn new(config: FastEmbedConfig) -> EmbeddingResult<Self> {
			let model_name = config.model.trim().to_owned();
			if model_name.is_empty() {
				return Err(EmbeddingError::Unavailable(
					"local embedding model is not configured".into(),
				));
			}
			let model: EmbeddingModel = model_name.parse().map_err(|_| {
				EmbeddingError::Unavailable(format!(
					"unknown local embedding model: {model_name}"
				))
			})?;
			let dimensions = TextEmbedding::get_model_info(&model).ok().map(|info| info.dim);
			let mut options =
				TextInitOptions::new(model).with_show_download_progress(false);
			if let Some(cache_dir) = config.cache_dir {
				options = options.with_cache_dir(cache_dir);
			}
			let embedding = TextEmbedding::try_new(options).map_err(|error| {
				EmbeddingError::Unavailable(format!("local embedding model unavailable: {error}"))
			})?;
			Ok(Self {
				model_name,
				dimensions,
				embedding: Arc::new(Mutex::new(embedding)),
			})
		}

		/// Blocking embedding for callers already on a worker thread (SQLite
		/// recall paths). The async trait impl wraps this in `spawn_blocking`.
		pub fn embed_blocking(&self, texts: &[String]) -> EmbeddingResult<Vec<Vec<f32>>> {
			if texts.is_empty() {
				return Ok(Vec::new());
			}
			let mut embedding = self.embedding.lock().map_err(|_| {
				EmbeddingError::Unavailable("local embedding model lock is poisoned".into())
			})?;
			embedding.embed(texts, None).map_err(|error| {
				EmbeddingError::Unavailable(format!("local embedding inference failed: {error}"))
			})
		}
	}

	#[async_trait]
	impl EmbeddingProvider for FastEmbedProvider {
		fn model(&self) -> &str {
			&self.model_name
		}

		fn dimensions(&self) -> Option<usize> {
			self.dimensions
		}

		async fn embed(&self, texts: &[String]) -> EmbeddingResult<Vec<Vec<f32>>> {
			if texts.is_empty() {
				return Ok(Vec::new());
			}
			let embedding = Arc::clone(&self.embedding);
			let texts = texts.to_vec();
			tokio::task::spawn_blocking(move || {
				let mut model = embedding.lock().map_err(|_| {
					EmbeddingError::Unavailable("local embedding model lock is poisoned".into())
				})?;
				model.embed(texts, None).map_err(|error| {
					EmbeddingError::Unavailable(format!(
						"local embedding inference failed: {error}"
					))
				})
			})
			.await
			.map_err(|error| {
				EmbeddingError::Unavailable(format!("local embedding task failed: {error}"))
			})?
		}
	}
}

#[cfg(feature = "local-embeddings")]
pub use local::{FastEmbedConfig, FastEmbedProvider};

#[cfg(test)]
mod tests {
	use std::time::Duration;

	use async_trait::async_trait;
	use serde_json::json;

	use super::{
		EmbeddingError, EmbeddingObject, EmbeddingProvider, EmbeddingRequest, EmbeddingsResponse,
		HttpEmbeddingConfig, assemble_embeddings, embed_best_effort, excerpt,
		normalize_endpoint, redact_text, redact_url,
	};

	fn response(value: serde_json::Value) -> EmbeddingsResponse {
		serde_json::from_value(value).expect("test payload")
	}

	#[test]
	fn endpoint_normalization_accepts_base_or_v1_or_full() {
		assert_eq!(
			normalize_endpoint("http://127.0.0.1:20128").unwrap(),
			"http://127.0.0.1:20128/v1/embeddings"
		);
		assert_eq!(
			normalize_endpoint("https://api.example.com/v1/").unwrap(),
			"https://api.example.com/v1/embeddings"
		);
		assert_eq!(
			normalize_endpoint("https://gateway.example.com/openai/v1/embeddings").unwrap(),
			"https://gateway.example.com/openai/v1/embeddings"
		);
		assert!(normalize_endpoint("").is_err());
		assert!(normalize_endpoint("ftp://example.com").is_err());
	}

	#[test]
	fn config_debug_redacts_api_key() {
		let config = HttpEmbeddingConfig::new("http://localhost:20128", "text-embedding-3-small")
			.with_api_key("sk-secret-value")
			.with_dimensions(4)
			.with_timeout(Duration::from_secs(2));
		let debug = format!("{config:?}");
		assert!(!debug.contains("sk-secret-value"));
		assert!(debug.contains("<redacted>"));
	}

	#[test]
	fn url_redaction_masks_userinfo_query_and_fragment() {
		let redacted = redact_url(
			"https://user:pass@example.com:8443/openai/v1/embeddings?api_key=secret#frag",
		);
		assert!(redacted.contains("example.com:8443/openai/v1/embeddings"), "{redacted}");
		assert!(!redacted.contains("user"), "{redacted}");
		assert!(!redacted.contains("pass"), "{redacted}");
		assert!(!redacted.contains("secret"), "{redacted}");
		assert!(!redacted.contains("frag"), "{redacted}");
		assert!(redacted.contains("redacted"), "{redacted}");
	}

	#[test]
	fn short_api_key_is_masked_in_error_excerpts() {
		let text = redact_text("request rejected for k12345", Some("k12345"));
		assert!(!text.contains("k12345"), "{text}");
		let excerpt = excerpt("provider says: k12345", Some("k12345"));
		assert!(!excerpt.contains("k12345"), "{excerpt}");
		assert!(excerpt.contains("<redacted>"));
	}

	#[test]
	fn request_body_uses_model_and_input_and_optional_dimensions() {
		let texts = vec!["one".to_owned(), "two".to_owned()];
		let body = EmbeddingRequest { model: "m", input: &texts, dimensions: Some(8) };
		let value = serde_json::to_value(body).unwrap();
		assert_eq!(value, json!({"model": "m", "input": ["one", "two"], "dimensions": 8}));

		let without = EmbeddingRequest { model: "m", input: &texts, dimensions: None };
		let value = serde_json::to_value(without).unwrap();
		assert_eq!(value, json!({"model": "m", "input": ["one", "two"]}));
	}

	#[test]
	fn payload_reorders_by_index() {
		let payload = response(json!({
			"data": [
				{"embedding": [3.0, 4.0], "index": 1},
				{"embedding": [1.0, 2.0], "index": 0}
			]
		}));
		assert_eq!(
			assemble_embeddings(payload, 2, Some(2)).unwrap(),
			vec![vec![1.0, 2.0], vec![3.0, 4.0]]
		);
	}

	#[test]
	fn payload_without_index_keeps_order() {
		let payload = response(json!({
			"data": [{"embedding": [1.0]}, {"embedding": [2.0]}]
		}));
		assert_eq!(assemble_embeddings(payload, 2, None).unwrap(), vec![vec![1.0], vec![2.0]]);
	}

	#[test]
	fn payload_rejects_bad_shapes_and_values() {
		let short = response(json!({"data": [{"embedding": [1.0]}]}));
		assert!(assemble_embeddings(short, 2, None).is_err());

		let duplicate = response(json!({
			"data": [
				{"embedding": [1.0], "index": 0},
				{"embedding": [2.0], "index": 0}
			]
		}));
		assert!(assemble_embeddings(duplicate, 2, None).is_err());

		let out_of_range = response(json!({
			"data": [{"embedding": [1.0], "index": 3}]
		}));
		assert!(assemble_embeddings(out_of_range, 1, None).is_err());

		let wrong_width = response(json!({"data": [{"embedding": [1.0, 2.0]}]}));
		assert!(assemble_embeddings(wrong_width, 1, Some(3)).is_err());

		let empty = response(json!({"data": [{"embedding": []}]}));
		assert!(assemble_embeddings(empty, 1, None).is_err());

		let inconsistent =
			EmbeddingsResponse { data: vec![
				EmbeddingObject { embedding: vec![1.0], index: Some(0) },
				EmbeddingObject { embedding: vec![1.0, 2.0], index: Some(1) },
			] };
		assert!(assemble_embeddings(inconsistent, 2, None).is_err());

		let non_finite =
			EmbeddingsResponse { data: vec![EmbeddingObject {
				embedding: vec![f32::NAN],
				index:   Some(0),
			}] };
		assert!(assemble_embeddings(non_finite, 1, None).is_err());
	}

	struct Offline;

	#[async_trait]
	impl EmbeddingProvider for Offline {
		fn model(&self) -> &'static str {
			"offline"
		}

		async fn embed(&self, _texts: &[String]) -> Result<Vec<Vec<f32>>, EmbeddingError> {
			Err(EmbeddingError::Unavailable("offline".into()))
		}
	}

	#[tokio::test]
	async fn best_effort_swallows_provider_failure() {
		let texts = vec!["memory".to_owned()];
		assert!(embed_best_effort(Some(&Offline), &texts).await.is_none());
		assert!(embed_best_effort(None, &texts).await.is_none());
	}
}
