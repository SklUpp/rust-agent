//! Replay-safe retries for provider streams.
//!
//! A provider attempt may be discarded only until meaningful assistant output
//! is emitted. Pre-output chunks are buffered so a transient transport failure
//! or a benign empty completion can re-issue a fresh request without
//! duplicating content; the first text, thinking, redacted, or tool event
//! commits the attempt and restores live streaming.
//!
//! Empty-completion retries stay opt-in through
//! [`ReplaySafeStreamRetryPolicy::retry_empty_completion`] because a normal
//! empty stop can be a valid provider result. Transient provider-error retries
//! reuse the shared [`super::error_flags`] classifier and are separately
//! bounded by the caller's policy.

use std::future::Future;

use futures::{StreamExt, stream::unfold};

use super::{
	ChunkStream, ProviderError, StreamChunk,
	oneshot_retry::{is_retryable_flags, is_retryable_provider_error},
};
use crate::state::{ContentPart, Message, StopReason, TokenUsage};

/// Fixed budget for empty-completion retries, matching the donor
/// (`MAX_EMPTY_COMPLETION_RETRIES`).
pub const MAX_EMPTY_COMPLETION_RETRIES: u32 = 2;

/// First backoff step for an empty completion; doubles per retry.
pub const EMPTY_COMPLETION_BASE_DELAY_MS: u64 = 500;

/// Whether a completed assistant message carries content worth delivering: an
/// image, tool call, or any non-whitespace text. An empty/whitespace-only
/// message — or one that only ever produced thinking — is the "empty
/// response" failure.
#[must_use]
pub fn has_visible_assistant_content(message: &Message) -> bool {
	message.content.chars().any(|c| !c.is_whitespace())
		|| !message.tool_calls.is_empty()
		|| message.parts.iter().any(|part| match part {
			ContentPart::Text { text } => text.chars().any(|c| !c.is_whitespace()),
			ContentPart::Image { .. } | ContentPart::ToolCall { .. } => true,
			ContentPart::Thinking { .. } | ContentPart::RedactedThinking { .. } => false,
		})
}

/// Which replay-safe provider results may issue a fresh request.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReplaySafeStreamRetryPolicy {
	/// Retry benign terminal stops that contain no visible output.
	pub retry_empty_completion: bool,
	/// Retry transient provider errors before any output is committed.
	pub retry_provider_errors: bool,
	/// Maximum transient provider-error retries. Empty completions keep their
	/// shared fixed budget ([`MAX_EMPTY_COMPLETION_RETRIES`]).
	pub max_provider_error_retries: u32,
}

impl Default for ReplaySafeStreamRetryPolicy {
	/// Empty-completion retries on (their budget is fixed); provider-error
	/// retries off until a caller opts in.
	fn default() -> Self {
		Self {
			retry_empty_completion: true,
			retry_provider_errors: false,
			max_provider_error_retries: 0,
		}
	}
}

impl ReplaySafeStreamRetryPolicy {
	/// No retries at all: the wrapper only buffers until the first meaningful
	/// chunk and otherwise passes the stream through unchanged.
	#[must_use]
	pub const fn disabled() -> Self {
		Self {
			retry_empty_completion: false,
			retry_provider_errors: false,
			max_provider_error_retries: 0,
		}
	}
}

/// A streamed chunk that delivers content worth committing the attempt for.
///
/// Mirrors the donor `isMeaningfulCompletionEvent`: non-empty text/thinking/
/// redacted deltas, any tool-call delta, or a thinking signature commit the
/// attempt. Empty openers and usage-only chunks stay buffered.
pub(crate) fn is_meaningful_chunk(chunk: &StreamChunk) -> bool {
	chunk.content.as_deref().is_some_and(|text| !text.is_empty())
		|| chunk.reasoning.as_deref().is_some_and(|text| !text.is_empty())
		|| chunk.redacted.as_deref().is_some_and(|text| !text.is_empty())
		|| !chunk.tool_call_deltas.is_empty()
		|| chunk.signature.is_some()
}

/// Terminal bookkeeping of one replay-safe attempt: a clean stop with no
/// visible output and no reported error is the empty-completion failure.
#[must_use]
pub(crate) fn is_empty_completion_terminal(
	done_seen: bool,
	done_stop: Option<StopReason>,
	done_error: bool,
	usage: Option<TokenUsage>,
) -> bool {
	done_seen
		&& done_stop == Some(StopReason::Stop)
		&& !done_error
		&& usage.is_none_or(|usage| usage.output <= 1)
}

struct RetryState<F> {
	attempt:         F,
	policy:          ReplaySafeStreamRetryPolicy,
	stream:          Option<ChunkStream>,
	/// Chunks held back until the attempt commits or ends.
	buffered:        Vec<StreamChunk>,
	/// Ready-to-yield chunks; drained one per stream item.
	out:             std::collections::VecDeque<StreamChunk>,
	committed:       bool,
	empty_retries:   u32,
	error_retries:   u32,
	usage:           Option<TokenUsage>,
	done_seen:       bool,
	done_stop:       Option<StopReason>,
	done_error:      bool,
	done_error_message: Option<String>,
	/// A terminal error that must surface only after pre-output chunks.
	pending_error:   Option<ProviderError>,
	/// The terminal attempt was flushed; no further attempt may start.
	finished:        bool,
}

impl<F> RetryState<F> {
	/// The attempt stopped cleanly with no visible output and no error: a
	/// benign empty completion the provider may simply reproduce.
	fn is_empty_completion(&self) -> bool {
		is_empty_completion_terminal(
			self.done_seen,
			self.done_stop,
			self.done_error,
			self.usage,
		)
	}

	fn drop_attempt(&mut self) {
		self.stream = None;
		self.buffered.clear();
		self.usage = None;
		self.done_seen = false;
		self.done_stop = None;
		self.done_error = false;
		self.done_error_message = None;
	}
}

pub(crate) fn empty_retry_delay_ms(retries: u32) -> u64 {
	EMPTY_COMPLETION_BASE_DELAY_MS.saturating_mul(1u64 << retries.min(16))
}

/// Re-issues a fresh provider request only while the current attempt remains
/// replay-safe.
///
/// `attempt` is the fresh-request factory: it is called once per attempt and
/// must reissue the same request. Buffered pre-output chunks from discarded
/// attempts never reach the consumer, and once any meaningful chunk escapes
/// no further attempt is made — a stream error or empty stop after that is
/// terminal.
#[must_use]
pub fn with_replay_safe_stream_retry<F, Fut>(
	policy: ReplaySafeStreamRetryPolicy,
	attempt: F,
) -> ChunkStream
where
	F: FnMut() -> Fut + Send + 'static,
	Fut: Future<Output = Result<ChunkStream, ProviderError>> + Send + 'static,
{
	let state = RetryState {
		attempt,
		policy,
		stream: None,
		buffered: Vec::new(),
		out: std::collections::VecDeque::new(),
		committed: false,
		empty_retries: 0,
		error_retries: 0,
		usage: None,
		done_seen: false,
		done_stop: None,
		done_error: false,
		done_error_message: None,
		pending_error: None,
		finished: false,
	};
	unfold(Some(state), |state| async move {
		let mut state = state?;
		loop {
			if let Some(chunk) = state.out.pop_front() {
				return Some((Ok(chunk), Some(state)));
			}
			if let Some(error) = state.pending_error.take() {
				return Some((Err(error), None));
			}
			if state.finished {
				return None;
			}

			let Some(stream) = state.stream.as_mut() else {
				// No attempt in flight: start a fresh one.
				match (state.attempt)().await {
					Ok(stream) => {
						state.stream = Some(stream);
						state.committed = false;
					},
					Err(error) => {
						if state.policy.retry_provider_errors
							&& state.error_retries < state.policy.max_provider_error_retries
							&& is_retryable_provider_error(&error)
						{
							let delay = empty_retry_delay_ms(state.error_retries);
							state.error_retries += 1;
							tokio::time::sleep(std::time::Duration::from_millis(delay)).await;
							continue;
						}
						return Some((Err(error), None));
					},
				}
				continue;
			};

			match stream.next().await {
				Some(Ok(chunk)) => {
					if chunk.usage.is_some() {
						state.usage = chunk.usage;
					}
					if state.committed {
						state.out.push_back(chunk);
						continue;
					}
					if is_meaningful_chunk(&chunk) {
						state.committed = true;
						state.out.extend(state.buffered.drain(..));
						state.out.push_back(chunk);
						continue;
					}
					if chunk.stop_reason.is_some() {
						state.done_stop = chunk.stop_reason;
					}
					if chunk.error_message.is_some() {
						state.done_error = true;
						state.done_error_message = chunk.error_message.clone();
					}
					if chunk.is_done {
						state.done_seen = true;
					}
					state.buffered.push(chunk);
				},
				Some(Err(error)) => {
					state.stream = None;
					let retry = !state.committed
						&& state.policy.retry_provider_errors
						&& state.error_retries < state.policy.max_provider_error_retries
						&& is_retryable_provider_error(&error);
					if retry {
						let delay = empty_retry_delay_ms(state.error_retries);
						state.error_retries += 1;
						state.drop_attempt();
						tokio::time::sleep(std::time::Duration::from_millis(delay)).await;
						continue;
					}
					state.out.extend(state.buffered.drain(..));
					if state.out.is_empty() {
						return Some((Err(error), None));
					}
					state.pending_error = Some(error);
				},
				None => {
					state.stream = None;
					let retry_empty = state.policy.retry_empty_completion
						&& !state.committed
						&& state.empty_retries < MAX_EMPTY_COMPLETION_RETRIES
						&& state.is_empty_completion();
					let retry_reported_error = !state.committed
						&& state.done_stop == Some(StopReason::Error)
						&& state.done_error
						&& state.policy.retry_provider_errors
						&& state.error_retries < state.policy.max_provider_error_retries
						&& state.done_error_message.as_deref().is_some_and(|message| {
							is_retryable_flags(super::error_flags::classify_message(message))
						});
					if retry_empty {
						let delay = empty_retry_delay_ms(state.empty_retries);
						state.empty_retries += 1;
						state.drop_attempt();
						tokio::time::sleep(std::time::Duration::from_millis(delay)).await;
						continue;
					}
					if retry_reported_error {
						let delay = empty_retry_delay_ms(state.error_retries);
						state.error_retries += 1;
						state.drop_attempt();
						tokio::time::sleep(std::time::Duration::from_millis(delay)).await;
						continue;
					}
					state.out.extend(state.buffered.drain(..));
					if state.out.is_empty() {
						return None;
					}
					state.finished = true;
				},
			}
		}
	})
	.boxed()
}

#[cfg(test)]
mod tests {
	use std::sync::{
		Arc,
		atomic::{AtomicUsize, Ordering},
	};

	use super::*;
	use crate::state::{TokenUsage, ToolCall};

	fn chunk() -> StreamChunk {
		StreamChunk {
			content:           None,
			reasoning:         None,
			tool_call_deltas:  Vec::new(),
			usage:             None,
			is_done:           false,
			content_index:     None,
			signature:         None,
			redacted:          None,
			stop_reason:       None,
			error_message:     None,
			disabled_features: None,
		}
	}

	fn text(text: &str) -> StreamChunk {
		StreamChunk { content: Some(text.to_string()), ..chunk() }
	}

	fn empty_done() -> StreamChunk {
		StreamChunk {
			is_done:     true,
			stop_reason: Some(StopReason::Stop),
			..chunk()
		}
	}

	fn scripted(
		calls: Arc<AtomicUsize>,
		scripts: Vec<Vec<Result<StreamChunk, ProviderError>>>,
	) -> impl FnMut() -> std::future::Ready<Result<ChunkStream, ProviderError>> + Send + 'static
	{
		let scripts = Arc::new(std::sync::Mutex::new(std::collections::VecDeque::from(
			scripts,
		)));
		move || {
			calls.fetch_add(1, Ordering::SeqCst);
			let items = scripts
				.lock()
				.expect("scripted attempts")
				.pop_front()
				.unwrap_or_else(|| vec![Ok(empty_done())]);
			std::future::ready(Ok(futures::stream::iter(items).boxed()))
		}
	}

	#[test]
	fn visible_content_requires_text_tool_or_image() {
		let thinking_only = Message {
			parts: vec![ContentPart::Thinking { thinking: "hmm".to_string(), thinking_signature: None }],
			..Message::assistant("")
		};
		assert!(!has_visible_assistant_content(&thinking_only));
		assert!(!has_visible_assistant_content(&Message::assistant("  \n\t")));
		assert!(has_visible_assistant_content(&Message::assistant("hi")));
		assert!(has_visible_assistant_content(&Message {
			parts: vec![ContentPart::Image {
				data:      "eA==".to_string(),
				mime_type: "image/png".to_string(),
			}],
			..Message::assistant("")
		}));
		assert!(has_visible_assistant_content(&Message::assistant_with_tools(
			"",
			vec![ToolCall { id: "c".to_string(), name: "t".to_string(), arguments: serde_json::json!({}) }],
		)));
	}

	#[tokio::test]
	async fn retries_empty_completion_and_discards_prior_chunks() {
		let calls = Arc::new(AtomicUsize::new(0));
		let attempt = scripted(calls.clone(), vec![
			vec![Ok(empty_done())],
			vec![Ok(text("answer")), Ok(empty_done())],
		]);
		let chunks: Vec<_> = with_replay_safe_stream_retry(
			ReplaySafeStreamRetryPolicy { retry_empty_completion: true, ..Default::default() },
			attempt,
		)
		.collect()
		.await;
		assert_eq!(calls.load(Ordering::SeqCst), 2);
		assert_eq!(chunks.len(), 2);
		assert_eq!(chunks[0].as_ref().unwrap().content.as_deref(), Some("answer"));
	}

	#[tokio::test]
	async fn partial_visible_output_is_never_replayed() {
		let calls = Arc::new(AtomicUsize::new(0));
		let attempt = scripted(calls.clone(), vec![vec![
			Ok(text("partial")),
			Err(ProviderError::Network("reset".to_string())),
		]]);
		let chunks: Vec<_> = with_replay_safe_stream_retry(
			ReplaySafeStreamRetryPolicy {
				retry_empty_completion:    true,
				retry_provider_errors:     true,
				max_provider_error_retries: 5,
			},
			attempt,
		)
		.collect()
		.await;
		assert_eq!(calls.load(Ordering::SeqCst), 1);
		assert_eq!(chunks.len(), 2);
		assert!(chunks[0].is_ok());
		assert!(chunks[1].is_err());
	}

	#[tokio::test]
	async fn empty_retries_are_bounded() {
		let calls = Arc::new(AtomicUsize::new(0));
		let attempt = scripted(calls.clone(), vec![
			vec![Ok(empty_done())],
			vec![Ok(empty_done())],
			vec![Ok(empty_done())],
		]);
		let chunks: Vec<_> = with_replay_safe_stream_retry(
			ReplaySafeStreamRetryPolicy { retry_empty_completion: true, ..Default::default() },
			attempt,
		)
		.collect()
		.await;
		assert_eq!(calls.load(Ordering::SeqCst), 3);
		assert_eq!(chunks.len(), 1);
		assert!(chunks[0].as_ref().unwrap().is_done);
	}

	#[tokio::test]
	async fn thinking_delta_commits_the_attempt() {
		let calls = Arc::new(AtomicUsize::new(0));
		let attempt = scripted(calls.clone(), vec![vec![
			Ok(StreamChunk { reasoning: Some("think".to_string()), ..chunk() }),
			Err(ProviderError::Network("reset".to_string())),
		]]);
		let chunks: Vec<_> = with_replay_safe_stream_retry(
			ReplaySafeStreamRetryPolicy {
				retry_empty_completion:    true,
				retry_provider_errors:     true,
				max_provider_error_retries: 5,
			},
			attempt,
		)
		.collect()
		.await;
		assert_eq!(calls.load(Ordering::SeqCst), 1);
		assert_eq!(chunks.len(), 2);
	}

	#[tokio::test]
	async fn pre_output_provider_error_is_retried() {
		let calls = Arc::new(AtomicUsize::new(0));
		let attempt = scripted(calls.clone(), vec![
			vec![Err(ProviderError::Network("connection reset".to_string()))],
			vec![Ok(text("ok")), Ok(empty_done())],
		]);
		let chunks: Vec<_> = with_replay_safe_stream_retry(
			ReplaySafeStreamRetryPolicy {
				retry_empty_completion:    false,
				retry_provider_errors:     true,
				max_provider_error_retries: 1,
			},
			attempt,
		)
		.collect()
		.await;
		assert_eq!(calls.load(Ordering::SeqCst), 2);
		assert_eq!(chunks.len(), 2);
		assert_eq!(chunks[0].as_ref().unwrap().content.as_deref(), Some("ok"));
	}

	#[test]
	fn empty_completion_ignores_reported_usage_above_one_token() {
		let state = RetryState {
			attempt:         || {
				std::future::ready(Ok::<ChunkStream, ProviderError>(
					futures::stream::empty::<Result<StreamChunk, ProviderError>>().boxed(),
				))
			},
			policy:          ReplaySafeStreamRetryPolicy::default(),
			stream:          None,
			buffered:        Vec::new(),
			out:             std::collections::VecDeque::new(),
			committed:       false,
			empty_retries:   0,
			error_retries:   0,
			usage:           Some(TokenUsage { output: 2, ..TokenUsage::default() }),
			done_seen:       true,
			done_stop:       Some(StopReason::Stop),
			done_error:      false,
			done_error_message: None,
			pending_error:   None,
			finished:        false,
		};
		assert!(!state.is_empty_completion());
	}
}
