//! Per-model-request timeout with bounded retry after timeout.
//!
//! Single owner for request-level retry: adapters keep their own transport
//! retries, but only this wrapper bounds a whole model request by wall-clock
//! time and only it reissues a timed-out request. Retries happen after a
//! timeout and nothing else; any other failure surfaces immediately.
//!
//! Safety contract:
//! - Tools never replay: tools run in the agent loop after the provider
//!   returns a message, so reissuing the model request itself cannot rerun a
//!   tool. No tool state lives here.
//! - Partial streams never combine: a stream that already yielded a chunk may
//!   already be visible or persisted by the caller, so its timeout is terminal.
//!   Only a stream with zero yielded chunks is re-requested, and then the
//!   stalled stream is dropped wholesale — never interleaved with the fresh one.

use std::{sync::Arc, time::Duration};

use async_trait::async_trait;
use futures::StreamExt;

use super::{ChunkStream, ModelProvider, ProviderError, ToolChoice};
use crate::state::Message;

/// Per-model-request timeout configuration.
///
/// `timeout` is the single absolute deadline per attempt: one `complete()`
/// call, or `stream()` establishment plus the whole stream. It never resets
/// per chunk. `None` (or zero) disables the wrapper. `max_retries` bounds
/// how many times a timed-out attempt is reissued; `0` means the timeout
/// surfaces immediately.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RequestTimeoutConfig {
	pub timeout:     Option<Duration>,
	pub max_retries: u32,
}

impl Default for RequestTimeoutConfig {
	fn default() -> Self {
		Self::disabled()
	}
}

impl RequestTimeoutConfig {
	/// Passthrough: no per-request timeout, no retry.
	#[must_use]
	pub const fn disabled() -> Self {
		Self { timeout: None, max_retries: 0 }
	}

	/// `true` when at least one attempt is bounded by a nonzero timeout.
	#[must_use]
	pub fn is_enabled(&self) -> bool {
		self.timeout.is_some_and(|timeout| !timeout.is_zero())
	}
}

/// Classify a provider failure as a timeout for retry decisions.
#[must_use]
pub fn is_timeout_error(error: &ProviderError) -> bool {
	matches!(error, ProviderError::Timeout(_))
}

/// [`ModelProvider`] wrapper applying [`RequestTimeoutConfig`].
///
/// Cheap to clone (one `Arc`); with [`RequestTimeoutConfig::disabled`] every
/// call goes straight through without cloning the request or arming a timer.
#[derive(Clone)]
pub struct TimeoutProvider {
	inner:       Arc<dyn ModelProvider>,
	timeout:     Option<Duration>,
	max_retries: u32,
}

impl TimeoutProvider {
	/// Wrap `inner` with `config`. A disabled config still wraps (uniform
	/// assembly in one place) but never alters behavior.
	#[must_use]
	pub fn new(inner: Arc<dyn ModelProvider>, config: RequestTimeoutConfig) -> Self {
		let timeout = config.timeout.filter(|timeout| !timeout.is_zero());
		Self { inner, timeout, max_retries: config.max_retries }
	}

	/// [`Self::new`] boxed for provider assembly.
	#[must_use]
	pub fn wrap(inner: Arc<dyn ModelProvider>, config: RequestTimeoutConfig) -> Arc<Self> {
		Arc::new(Self::new(inner, config))
	}

	fn timeout_message(budget: Duration, yielded: bool) -> String {
		if yielded {
			format!("model stream timed out after {budget:?}")
		} else {
			format!("model request timed out after {budget:?}")
		}
	}
}

#[async_trait]
impl ModelProvider for TimeoutProvider {
	async fn complete(
		&self,
		messages: &[Message],
		tools: &[serde_json::Value],
		tool_choice: &ToolChoice,
	) -> Result<Message, ProviderError> {
		let Some(budget) = self.timeout else {
			return self.inner.complete(messages, tools, tool_choice).await;
		};
		// Owned clones: each retry reissues the same request body.
		let messages = messages.to_vec();
		let tools = tools.to_vec();
		let tool_choice = tool_choice.clone();
		let mut retries_left = self.max_retries;
		loop {
			match tokio::time::timeout(
				budget,
				self.inner.complete(&messages, &tools, &tool_choice),
			)
			.await
			{
				Ok(result) => return result,
				Err(_) if retries_left > 0 => {
					retries_left -= 1;
				},
				Err(_) => {
					return Err(ProviderError::Timeout(Self::timeout_message(budget, false)));
				},
			}
		}
	}

	async fn stream(
		&self,
		messages: &[Message],
		tools: &[serde_json::Value],
		tool_choice: &ToolChoice,
	) -> Result<ChunkStream, ProviderError> {
		let Some(budget) = self.timeout else {
			return self.inner.stream(messages, tools, tool_choice).await;
		};
		let messages = messages.to_vec();
		let tools = tools.to_vec();
		let tool_choice = tool_choice.clone();
		// One absolute deadline per attempt: establishment and the whole stream
		// share it, never resetting per chunk. Establishment retries are safe
		// (no chunk exists yet).
		let mut retries_left = self.max_retries;
		loop {
			let deadline = tokio::time::Instant::now() + budget;
			match tokio::time::timeout_at(
				deadline,
				self.inner.stream(&messages, &tools, &tool_choice),
			)
			.await
			{
				Ok(Ok(stream)) => {
					return Ok(watch_stream(
						self.inner.clone(),
						messages,
						tools,
						tool_choice,
						stream,
						deadline,
						budget,
						retries_left,
					));
				},
				Ok(Err(error)) => return Err(error),
				Err(_) if retries_left > 0 => {
					retries_left -= 1;
				},
				Err(_) => {
					return Err(ProviderError::Timeout(Self::timeout_message(budget, false)));
				},
			}
		}
	}
	fn api(&self) -> Option<&str> {
		self.inner.api()
	}

	fn provider(&self) -> Option<&str> {
		self.inner.provider()
	}

	fn model(&self) -> Option<&str> {
		self.inner.model()
	}

	fn supports_images(&self) -> bool {
		self.inner.supports_images()
	}

	fn check_support(&self, messages: &[Message]) -> Result<(), ProviderError> {
		self.inner.check_support(messages)
	}
}

/// Absolute-deadline watchdog over an established stream.
///
/// `deadline` is the single per-attempt deadline covering establishment and
/// the whole stream: it never resets per chunk. A timeout before the first
/// chunk drops the stalled stream and reissues the whole request (nothing
/// visible yet, remaining retries permitting). After any chunk the timeout
/// is terminal — the stream is dropped and the timeout surfaces, never
/// retried or merged.
fn watch_stream(
	inner: Arc<dyn ModelProvider>,
	messages: Vec<Message>,
	tools: Vec<serde_json::Value>,
	tool_choice: ToolChoice,
	stream: ChunkStream,
	deadline: tokio::time::Instant,
	budget: Duration,
	retries_left: u32,
) -> ChunkStream {
	struct WatchState {
		inner: Arc<dyn ModelProvider>,
		messages: Vec<Message>,
		tools: Vec<serde_json::Value>,
		tool_choice: ToolChoice,
		stream: ChunkStream,
		yielded: bool,
		retries_left: u32,
		deadline: tokio::time::Instant,
		budget: Duration,
	}

	let state = WatchState {
		inner,
		messages,
		tools,
		tool_choice,
		stream,
		yielded: false,
		retries_left,
		deadline,
		budget,
	};
	futures::stream::unfold(Some(state), |mut state: Option<WatchState>| async move {
		let mut current = state.take()?;
		loop {
			match tokio::time::timeout_at(current.deadline, current.stream.next()).await {
				Ok(None) => return None,
				Ok(Some(Err(error))) => {
					return Some((Err(error), None));
				},
				Ok(Some(Ok(chunk))) => {
					current.yielded = true;
					return Some((Ok(chunk), Some(current)));
				},
				Err(_) => {
					if current.yielded || current.retries_left == 0 {
						let error = ProviderError::Timeout(TimeoutProvider::timeout_message(
							current.budget,
							current.yielded,
						));
						return Some((Err(error), None));
					}
					// Drop the stalled prior stream before reissuing: its
					// items must never interleave with the fresh attempt.
					current.stream = futures::stream::empty().boxed();
					let mut fresh = None;
					while current.retries_left > 0 {
						current.retries_left -= 1;
						let deadline = tokio::time::Instant::now() + current.budget;
						match tokio::time::timeout_at(
							deadline,
							current.inner.stream(
								&current.messages,
								&current.tools,
								&current.tool_choice,
							),
						)
						.await
						{
							Ok(Ok(stream)) => {
								fresh = Some((stream, deadline));
								break;
							},
							// Retry-after-timeout only: a concrete failure
							// surfaces without spending further attempts.
							Ok(Err(error)) => return Some((Err(error), None)),
							Err(_) => {},
						}
					}
					match fresh {
						Some((stream, deadline)) => {
							current.stream = stream;
							current.deadline = deadline;
						},
						None => {
							let error = ProviderError::Timeout(
								TimeoutProvider::timeout_message(current.budget, false),
							);
							return Some((Err(error), None));
						},
					}
				},
			}
		}
	})
	.boxed()
}

#[cfg(test)]
mod tests {
	use std::sync::atomic::{AtomicUsize, Ordering};

	use super::*;

	fn config(timeout_ms: u64, max_retries: u32) -> RequestTimeoutConfig {
		RequestTimeoutConfig {
			timeout: Some(Duration::from_millis(timeout_ms)),
			max_retries,
		}
	}

	fn done_chunk(text: &str, is_done: bool) -> StreamChunk {
		StreamChunk {
			content: Some(text.to_string()),
			reasoning: None,
			tool_call_deltas: Vec::new(),
			usage: None,
			is_done,
			content_index: None,
			signature: None,
			redacted: None,
			stop_reason: None,
			error_message: None,
			disabled_features: None,
		}
	}

	struct ScriptedComplete {
		calls: AtomicUsize,
		hangs: usize,
	}

	#[async_trait]
	impl ModelProvider for ScriptedComplete {
		async fn complete(
			&self,
			_messages: &[Message],
			_tools: &[serde_json::Value],
			_tool_choice: &ToolChoice,
		) -> Result<Message, ProviderError> {
			let call = self.calls.fetch_add(1, Ordering::SeqCst);
			if call < self.hangs {
				futures::future::pending::<()>().await;
				unreachable!("pending never resolves");
			}
			Ok(Message::assistant("ok"))
		}

		async fn stream(
			&self,
			_messages: &[Message],
			_tools: &[serde_json::Value],
			_tool_choice: &ToolChoice,
		) -> Result<ChunkStream, ProviderError> {
			Err(ProviderError::Protocol("unused".to_string()))
		}
	}

	#[tokio::test]
	async fn complete_retries_timed_out_attempts_then_succeeds() {
		let inner = Arc::new(ScriptedComplete { calls: AtomicUsize::new(0), hangs: 2 });
		let provider = TimeoutProvider::new(inner.clone(), config(20, 2));
		let message = provider
			.complete(&[Message::user("hi")], &[], &ToolChoice::Auto)
			.await
			.expect("retry succeeds");
		assert_eq!(message.content, "ok");
		assert_eq!(inner.calls.load(Ordering::SeqCst), 3);
	}

	#[tokio::test]
	async fn complete_timeout_exhaustion_is_a_timeout_error() {
		let inner = Arc::new(ScriptedComplete { calls: AtomicUsize::new(0), hangs: 5 });
		let provider = TimeoutProvider::new(inner.clone(), config(20, 1));
		let error = provider
			.complete(&[Message::user("hi")], &[], &ToolChoice::Auto)
			.await
			.expect_err("budget exhausted");
		assert!(is_timeout_error(&error), "{error:?}");
		assert_eq!(inner.calls.load(Ordering::SeqCst), 2);
	}

	#[tokio::test]
	async fn complete_surfaces_non_timeout_without_retry() {
		struct Failing;
		#[async_trait]
		impl ModelProvider for Failing {
			async fn complete(
				&self,
				_messages: &[Message],
				_tools: &[serde_json::Value],
				_tool_choice: &ToolChoice,
			) -> Result<Message, ProviderError> {
				Err(ProviderError::HttpStatus(500, "boom".to_string()))
			}

			async fn stream(
				&self,
				_messages: &[Message],
				_tools: &[serde_json::Value],
				_tool_choice: &ToolChoice,
			) -> Result<ChunkStream, ProviderError> {
				Err(ProviderError::Protocol("unused".to_string()))
			}
		}
		let provider = TimeoutProvider::new(Arc::new(Failing), config(20, 3));
		let error = provider
			.complete(&[Message::user("hi")], &[], &ToolChoice::Auto)
			.await
			.expect_err("concrete failure");
		assert!(matches!(error, ProviderError::HttpStatus(500, _)), "{error:?}");
	}

	#[test]
	fn disabled_config_never_arms_a_timer() {
		assert!(!RequestTimeoutConfig::default().is_enabled());
		assert!(
			!RequestTimeoutConfig { timeout: Some(Duration::ZERO), max_retries: 3 }.is_enabled()
		);
		assert!(config(1, 0).is_enabled());
	}

	#[derive(Clone, Copy)]
	enum StreamMode {
		/// First establishment hangs; later ones emit one done chunk.
		EstablishHangOnceThenDone,
		/// First stream stalls before any chunk; the next emits one done chunk.
		StallBeforeFirstChunk,
		/// First stream yields a chunk, then stalls.
		StallAfterFirstChunk,
	}

	struct ScriptedStream {
		calls: AtomicUsize,
		mode:  StreamMode,
	}

	#[async_trait]
	impl ModelProvider for ScriptedStream {
		async fn complete(
			&self,
			_messages: &[Message],
			_tools: &[serde_json::Value],
			_tool_choice: &ToolChoice,
		) -> Result<Message, ProviderError> {
			Err(ProviderError::Protocol("unused".to_string()))
		}

		async fn stream(
			&self,
			_messages: &[Message],
			_tools: &[serde_json::Value],
			_tool_choice: &ToolChoice,
		) -> Result<ChunkStream, ProviderError> {
			let call = self.calls.fetch_add(1, Ordering::SeqCst);
			match self.mode {
				StreamMode::EstablishHangOnceThenDone if call == 0 => {
					futures::future::pending::<()>().await;
					unreachable!("pending never resolves");
				},
				StreamMode::EstablishHangOnceThenDone => {
					Ok(futures::stream::iter(vec![Ok(done_chunk("hi", true))]).boxed())
				},
				StreamMode::StallBeforeFirstChunk if call == 0 => {
					Ok(futures::stream::pending::<
						Result<StreamChunk, ProviderError>,
					>()
					.boxed())
				},
				StreamMode::StallBeforeFirstChunk => {
					Ok(futures::stream::iter(vec![Ok(done_chunk("hi", true))]).boxed())
				},
				StreamMode::StallAfterFirstChunk => {
					let head = futures::stream::iter(vec![Ok(done_chunk("part", false))]);
					let tail = futures::stream::pending::<Result<StreamChunk, ProviderError>>();
					Ok(head.chain(tail).boxed())
				},
			}
		}
	}

	#[tokio::test]
	async fn stream_retries_hung_establishment() {
		let inner = Arc::new(ScriptedStream {
			calls: AtomicUsize::new(0),
			mode:  StreamMode::EstablishHangOnceThenDone,
		});
		let provider = TimeoutProvider::new(inner.clone(), config(20, 1));
		let mut stream = provider
			.stream(&[Message::user("hi")], &[], &ToolChoice::Auto)
			.await
			.expect("establishment retry succeeds");
		let chunk = stream.next().await.expect("item").expect("chunk");
		assert_eq!(chunk.content.as_deref(), Some("hi"));
		assert_eq!(inner.calls.load(Ordering::SeqCst), 2);
	}

	#[tokio::test]
	async fn stream_reissues_before_the_first_chunk_only() {
		let inner = Arc::new(ScriptedStream {
			calls: AtomicUsize::new(0),
			mode:  StreamMode::StallBeforeFirstChunk,
		});
		let provider = TimeoutProvider::new(inner.clone(), config(20, 1));
		let mut stream = provider
			.stream(&[Message::user("hi")], &[], &ToolChoice::Auto)
			.await
			.expect("established");
		let chunk = stream.next().await.expect("item").expect("chunk");
		assert_eq!(chunk.content.as_deref(), Some("hi"));
		assert_eq!(inner.calls.load(Ordering::SeqCst), 2, "one silent reissue");
	}

	#[tokio::test]
	async fn stream_timeout_after_a_chunk_is_terminal_without_reissue() {
		let inner = Arc::new(ScriptedStream {
			calls: AtomicUsize::new(0),
			mode:  StreamMode::StallAfterFirstChunk,
		});
		let provider = TimeoutProvider::new(inner.clone(), config(20, 2));
		let mut stream = provider
			.stream(&[Message::user("hi")], &[], &ToolChoice::Auto)
			.await
			.expect("established");
		let chunk = stream.next().await.expect("item").expect("chunk");
		assert_eq!(chunk.content.as_deref(), Some("part"));
		let error = stream.next().await.expect("item").expect_err("stall is terminal");
		assert!(is_timeout_error(&error), "{error:?}");
		assert_eq!(inner.calls.load(Ordering::SeqCst), 1, "no reissue after output");
	}
}
