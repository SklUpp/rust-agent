//! Per-model-request timeout with bounded retry after timeout.
//!
//! Single owner for request-level retry: adapters keep their own transport
//! retries, but only this wrapper bounds a whole model request by wall-clock
//! time and only it reissues a timed-out request. It also owns the replay-safe
//! empty-completion retry: pre-output chunks are buffered until visible
//! content commits the attempt, and a clean stop with no visible output is
//! reissued up to [`MAX_EMPTY_COMPLETION_RETRIES`] times. There is exactly one
//! attempt loop here, so timeout retries and empty-completion retries cannot
//! stack or reset each other's budgets.
//!
//! Safety contract:
//! - Tools never replay: tools run in the agent loop after the provider
//!   returns a message, so reissuing the model request itself cannot rerun a
//!   tool. No tool state lives here.
//! - Partial streams never combine: a stream that already yielded a chunk may
//!   already be visible or persisted by the caller, so its timeout is terminal.
//!   Only a stream with zero yielded chunks is re-requested, and then the
//!   stalled stream is dropped wholesale — never interleaved with the fresh one.
//! - Empty-completion retries discard only pre-output chunks that never
//!   escaped this wrapper; once visible text, thinking or a tool event is
//!   committed, a later empty stop is terminal.

use std::{
	collections::VecDeque,
	sync::Arc,
	time::Duration,
};

use async_trait::async_trait;
use futures::StreamExt;

use super::{
	ChunkStream, ModelProvider, ProviderError, StreamChunk, ToolChoice,
	empty_retry::{
		MAX_EMPTY_COMPLETION_RETRIES, empty_retry_delay_ms, is_empty_completion_terminal,
		is_meaningful_chunk,
	},
};
use crate::state::{Message, StopReason, TokenUsage};

/// Per-model-request timeout configuration.
///
/// `timeout` is the single absolute deadline per attempt: one `complete()`
/// call, or `stream()` establishment plus the whole stream. It never resets
/// per chunk. `None` (or zero) disables the wall-clock timeout; `stream()`
/// still applies the replay-safe empty-completion retry. `max_retries` bounds
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
	/// No wall-clock timeout and no timeout retry; `stream()` still applies the
	/// replay-safe empty-completion retry.
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
/// Cheap to clone (one `Arc`); with [`RequestTimeoutConfig::disabled`]
/// `complete()` passes straight through and `stream()` arms no timer, while
/// still watching for replay-safe empty completions.
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
		let messages = messages.to_vec();
		let tools = tools.to_vec();
		let tool_choice = tool_choice.clone();
		let mut retries_left = self.max_retries;
		let (stream, deadline) = establish_stream(
			self.inner.as_ref(),
			&messages,
			&tools,
			&tool_choice,
			self.timeout,
			&mut retries_left,
		)
		.await?;
		Ok(watch_stream(
			self.inner.clone(),
			messages,
			tools,
			tool_choice,
			stream,
			deadline,
			self.timeout,
			retries_left,
		))
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

/// Establish one streaming attempt, reissuing after an establishment timeout
/// while `retries_left` permits. A concrete failure surfaces without spending
/// further attempts. With no `budget`, establishment is unbounded and returns
/// no deadline.
async fn establish_stream(
	inner: &dyn ModelProvider,
	messages: &[Message],
	tools: &[serde_json::Value],
	tool_choice: &ToolChoice,
	budget: Option<Duration>,
	retries_left: &mut u32,
) -> Result<(ChunkStream, Option<tokio::time::Instant>), ProviderError> {
	loop {
		let Some(budget) = budget else {
			let stream = inner.stream(messages, tools, tool_choice).await?;
			return Ok((stream, None));
		};
		let deadline = tokio::time::Instant::now() + budget;
		match tokio::time::timeout_at(deadline, inner.stream(messages, tools, tool_choice)).await {
			Ok(Ok(stream)) => return Ok((stream, Some(deadline))),
			Ok(Err(error)) => return Err(error),
			Err(_) if *retries_left > 0 => *retries_left -= 1,
			Err(_) => {
				return Err(ProviderError::Timeout(TimeoutProvider::timeout_message(
					budget, false,
				)));
			},
		}
	}
}

/// Replay-safe watchdog over an established stream.
///
/// `deadline` is the single per-attempt deadline covering establishment and
/// the whole stream: it never resets per chunk. It is `None` when no timeout is
/// configured, so the stream is only watched for empty completions.
/// Pre-output chunks are buffered until visible content commits the attempt. A
/// timeout before any chunk drops the stalled stream and reissues the whole
/// request (remaining timeout retries permitting); after any chunk the timeout
/// is terminal. A clean empty stop before any visible output is reissued from
/// the same loop, with its own fixed budget, so timeout and empty-completion
/// retries never stack.
fn watch_stream(
	inner: Arc<dyn ModelProvider>,
	messages: Vec<Message>,
	tools: Vec<serde_json::Value>,
	tool_choice: ToolChoice,
	stream: ChunkStream,
	deadline: Option<tokio::time::Instant>,
	budget: Option<Duration>,
	retries_left: u32,
) -> ChunkStream {
	struct WatchState {
		inner:              Arc<dyn ModelProvider>,
		messages:           Vec<Message>,
		tools:              Vec<serde_json::Value>,
		tool_choice:        ToolChoice,
		stream:             ChunkStream,
		yielded:            bool,
		retries_left:       u32,
		empty_retries_left: u32,
		deadline:           Option<tokio::time::Instant>,
		budget:             Option<Duration>,
		/// Chunks held back until the attempt commits or ends.
		buffered:           Vec<StreamChunk>,
		/// Ready-to-yield chunks; drained one per stream item.
		out:                VecDeque<StreamChunk>,
		committed:          bool,
		usage:              Option<TokenUsage>,
		done_seen:          bool,
		done_stop:          Option<StopReason>,
		done_error:         bool,
		finished:           bool,
		pending_error:      Option<ProviderError>,
	}

	impl WatchState {
		fn reset_attempt(&mut self) {
			self.yielded = false;
			self.committed = false;
			self.buffered.clear();
			self.usage = None;
			self.done_seen = false;
			self.done_stop = None;
			self.done_error = false;
		}

		fn empty_retries_used(&self) -> u32 {
			MAX_EMPTY_COMPLETION_RETRIES.saturating_sub(self.empty_retries_left)
		}
	}

	let state = WatchState {
		inner,
		messages,
		tools,
		tool_choice,
		stream,
		yielded: false,
		retries_left,
		empty_retries_left: MAX_EMPTY_COMPLETION_RETRIES,
		deadline,
		budget,
		buffered: Vec::new(),
		out: VecDeque::new(),
		committed: false,
		usage: None,
		done_seen: false,
		done_stop: None,
		done_error: false,
		finished: false,
		pending_error: None,
	};
	futures::stream::unfold(Some(state), |mut state: Option<WatchState>| async move {
		let mut current = state.take()?;
		loop {
			if let Some(chunk) = current.out.pop_front() {
				return Some((Ok(chunk), Some(current)));
			}
			if let Some(error) = current.pending_error.take() {
				return Some((Err(error), None));
			}
			if current.finished {
				return None;
			}

			let deadline = current.deadline;
			let next = current.stream.next();
			let item = match deadline {
				Some(deadline) => tokio::time::timeout_at(deadline, next).await.map_err(|_| ()),
				None => Ok::<_, ()>(next.await),
			};
			match item {
				Ok(None) => {
					let retry_empty = !current.committed
						&& current.empty_retries_left > 0
						&& is_empty_completion_terminal(
							current.done_seen,
							current.done_stop,
							current.done_error,
							current.usage,
						);
					if retry_empty {
						let delay = empty_retry_delay_ms(current.empty_retries_used());
						current.empty_retries_left -= 1;
						current.reset_attempt();
						tokio::time::sleep(Duration::from_millis(delay)).await;
						match establish_stream(
							current.inner.as_ref(),
							&current.messages,
							&current.tools,
							&current.tool_choice,
							current.budget,
							&mut current.retries_left,
						)
						.await
						{
							Ok((stream, deadline)) => {
								current.stream = stream;
								current.deadline = deadline;
							},
							Err(error) => return Some((Err(error), None)),
						}
						continue;
					}
					current.out.extend(current.buffered.drain(..));
					if current.out.is_empty() {
						return None;
					}
					current.finished = true;
				},
				Ok(Some(Err(error))) => {
					current.out.extend(current.buffered.drain(..));
					if current.out.is_empty() {
						return Some((Err(error), None));
					}
					current.pending_error = Some(error);
				},
				Ok(Some(Ok(chunk))) => {
					current.yielded = true;
					if chunk.usage.is_some() {
						current.usage = chunk.usage;
					}
					if current.committed {
						current.out.push_back(chunk);
						continue;
					}
					if is_meaningful_chunk(&chunk) {
						current.committed = true;
						current.out.extend(current.buffered.drain(..));
						current.out.push_back(chunk);
						continue;
					}
					if chunk.stop_reason.is_some() {
						current.done_stop = chunk.stop_reason;
					}
					if chunk.error_message.is_some() {
						current.done_error = true;
					}
					if chunk.is_done {
						current.done_seen = true;
					}
					current.buffered.push(chunk);
				},
				Err(_) => {
					if current.yielded || current.retries_left == 0 {
						let budget = current.budget.unwrap_or_default();
						let error = ProviderError::Timeout(TimeoutProvider::timeout_message(
							budget,
							current.yielded,
						));
						current.out.extend(current.buffered.drain(..));
						if current.out.is_empty() {
							return Some((Err(error), None));
						}
						current.pending_error = Some(error);
						continue;
					}
					// Preserve the existing budget shape: a pre-output stall
					// consumes one timeout retry before establishment.
					current.retries_left -= 1;
					match establish_stream(
						current.inner.as_ref(),
						&current.messages,
						&current.tools,
						&current.tool_choice,
						current.budget,
						&mut current.retries_left,
					)
					.await
					{
						Ok((stream, deadline)) => {
							current.stream = stream;
							current.deadline = deadline;
							current.reset_attempt();
						},
						Err(error) => return Some((Err(error), None)),
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
	use crate::StreamChunk;

	fn config(timeout_ms: u64, max_retries: u32) -> RequestTimeoutConfig {
		RequestTimeoutConfig {
			timeout: Some(Duration::from_millis(timeout_ms)),
			max_retries,
		}
	}

	fn done_chunk(text: &str, is_done: bool) -> StreamChunk {
		StreamChunk {
			refusal: None,
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

	fn empty_done_chunk() -> StreamChunk {
		StreamChunk {
			refusal:     None,
			is_done:     true,
			stop_reason: Some(StopReason::Stop),
			..done_chunk("", true)
		}
	}

	#[derive(Clone, Copy)]
	enum StreamMode {
		/// First establishment hangs; later ones emit one done chunk.
		EstablishHangOnceThenDone,
		/// First stream stalls before any chunk; the next emits one done chunk.
		StallBeforeFirstChunk,
		/// First stream yields a chunk, then stalls.
		StallAfterFirstChunk,
		/// First stream stops empty; the next emits one done chunk.
		EmptyThenDone,
		/// One stream emits visible text followed by an empty stop.
		VisibleThenEmptyStop,
		/// One stream emits a refusal chunk then a low-usage empty stop.
		RefusalThenEmptyStop,
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
				StreamMode::EmptyThenDone if call == 0 => {
					Ok(futures::stream::iter(vec![Ok(empty_done_chunk())]).boxed())
				},
				StreamMode::EmptyThenDone => {
					Ok(futures::stream::iter(vec![Ok(done_chunk("hi", true))]).boxed())
				},
				StreamMode::VisibleThenEmptyStop => Ok(futures::stream::iter(vec![
					Ok(done_chunk("part", false)),
					Ok(empty_done_chunk()),
				])
				.boxed()),
				StreamMode::RefusalThenEmptyStop => Ok(futures::stream::iter(vec![
					Ok(StreamChunk {
						content: None,
						refusal: Some("blocked by safety".to_string()),
						..done_chunk("", false)
					}),
					Ok(StreamChunk {
						usage: Some(crate::state::TokenUsage {
							output: 1,
							..Default::default()
						}),
						..empty_done_chunk()
					}),
				])
				.boxed()),
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
	async fn stream_retries_empty_completion_before_output() {
		let inner = Arc::new(ScriptedStream {
			calls: AtomicUsize::new(0),
			mode:  StreamMode::EmptyThenDone,
		});
		let provider = TimeoutProvider::new(inner.clone(), config(1_000, 0));
		let mut stream = provider
			.stream(&[Message::user("hi")], &[], &ToolChoice::Auto)
			.await
			.expect("established");
		let chunk = stream.next().await.expect("item").expect("chunk");
		assert_eq!(chunk.content.as_deref(), Some("hi"));
		assert_eq!(inner.calls.load(Ordering::SeqCst), 2, "one empty reissue");
	}

	#[tokio::test]
	async fn stream_retries_empty_completion_without_timeout_budget() {
		let inner = Arc::new(ScriptedStream {
			calls: AtomicUsize::new(0),
			mode:  StreamMode::EmptyThenDone,
		});
		let provider = TimeoutProvider::new(inner.clone(), RequestTimeoutConfig::disabled());
		let mut stream = provider
			.stream(&[Message::user("hi")], &[], &ToolChoice::Auto)
			.await
			.expect("established");
		let chunk = stream.next().await.expect("item").expect("chunk");
		assert_eq!(chunk.content.as_deref(), Some("hi"));
		assert_eq!(
			inner.calls.load(Ordering::SeqCst),
			2,
			"empty completion retries without arming a wall-clock timeout"
		);
	}

	#[tokio::test]
	async fn stream_does_not_retry_empty_stop_after_visible_output() {
		let inner = Arc::new(ScriptedStream {
			calls: AtomicUsize::new(0),
			mode:  StreamMode::VisibleThenEmptyStop,
		});
		let provider = TimeoutProvider::new(inner.clone(), config(1_000, 0));
		let mut stream = provider
			.stream(&[Message::user("hi")], &[], &ToolChoice::Auto)
			.await
			.expect("established");
		let chunk = stream.next().await.expect("item").expect("chunk");
		assert_eq!(chunk.content.as_deref(), Some("part"));
		assert!(
			stream.next().await.expect("item").expect("chunk").is_done,
			"empty stop after visible output is terminal"
		);
		assert_eq!(inner.calls.load(Ordering::SeqCst), 1, "no reissue after output");
	}

	#[tokio::test]
	async fn stream_preserves_refusal_without_empty_completion_retry() {
		let inner = Arc::new(ScriptedStream {
			calls: AtomicUsize::new(0),
			mode:  StreamMode::RefusalThenEmptyStop,
		});
		let provider = TimeoutProvider::new(inner.clone(), config(1_000, 0));
		let mut stream = provider
			.stream(&[Message::user("hi")], &[], &ToolChoice::Auto)
			.await
			.expect("established");
		let mut refusal = None;
		while let Some(item) = stream.next().await {
			let chunk = item.expect("chunk");
			if chunk.refusal.is_some() {
				refusal = chunk.refusal;
			}
		}
		assert_eq!(refusal.as_deref(), Some("blocked by safety"));
		assert_eq!(
			inner.calls.load(Ordering::SeqCst),
			1,
			"a refusal chunk commits the attempt; no empty-completion retry"
		);
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
