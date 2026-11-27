//! Transient-failure retry for **oneshot** (non-agent-loop) completions.
//!
//! `complete()` has no streaming side effects: a summary, title, handoff, or
//! image description can be re-requested wholesale, so a transient provider
//! failure need not surface on the first blip. The set of retryable failures
//! stays defined by [`super::error_flags`]: classification and `retriable`
//! are shared with the streaming paths, while this module adds the oneshot
//! policy (no replay after a deterministic input rejection, bounded backoff).
//!
//! A resolved [`Message`] with `stop_reason == StopReason::Error` is the
//! failure shape produced by the OpenAI-compatible adapter; a raw transport
//! failure arrives as `Err(ProviderError)`. Both are handled; anything
//! non-retryable is returned unchanged so existing caller handling keeps
//! working.

use std::time::Duration;

use super::{
	ModelProvider, ProviderError, ToolChoice,
	error_flags::{self, ErrorFlags},
	retry,
};
use crate::state::{Message, StopReason};

/// Retry policy for one non-mutating completion.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OneshotRetryOptions {
	/// Total attempts, including the first. Default 3; `0` is treated as `1`.
	pub max_attempts: u32,
	/// First backoff step in ms; doubles per attempt. Default 500.
	pub base_delay_ms: u64,
	/// Upper bound for a single backoff wait. Default 30_000.
	pub max_delay_ms: u64,
}

impl Default for OneshotRetryOptions {
	fn default() -> Self {
		Self { max_attempts: 3, base_delay_ms: 500, max_delay_ms: 30_000 }
	}
}

/// Cap on pure backoff growth; `max_delay_ms` still bounds the final wait.
const BACKOFF_CEILING_MS: u64 = 8_000;

/// `true` when the classified failure is safe to replay as a fresh oneshot.
///
/// `retriable` already rejects content-blocked and payload-rejected failures
/// and requires a replay-safe kind. Context overflow is excluded here because
/// a oneshot replays a fixed prompt: an input the model cannot fit fails on
/// every attempt, so the caller's fallback must own it instead.
#[must_use]
pub(crate) fn is_retryable_flags(flags: ErrorFlags) -> bool {
	if error_flags::is(flags, ErrorFlags::CONTEXT_OVERFLOW)
		|| error_flags::is(flags, ErrorFlags::ABORT)
		|| error_flags::is(flags, ErrorFlags::USER_INTERRUPT)
	{
		return false;
	}
	error_flags::retriable(flags, false)
}

/// Classify a transport/provider failure and decide whether a fresh request
/// may succeed.
#[must_use]
pub(crate) fn is_retryable_provider_error(error: &ProviderError) -> bool {
	let flags = match error {
		ProviderError::HttpStatus(status, body) => {
			// Seed the classifier with the retryable-status bit instead of OR-ing
			// it afterwards: `classify_message_with` clears `TRANSIENT` for
			// deterministic failures (e.g. tool-call JSON parse errors), so the
			// exclusion must survive while an opaque 5xx body still retries.
			let existing = if retry::should_retry_status(*status) {
				ErrorFlags::create(ErrorFlags::TRANSIENT)
			} else {
				ErrorFlags::NONE
			};
			error_flags::classify_message_with(body, existing, Some(*status), None, None, None)
		},
		// Transport failures have no body to classify; the shared transient
		// set covers both network resets and request timeouts.
		ProviderError::Network(_) | ProviderError::Timeout(_) => error_flags::classify_message(
			&error.to_string(),
		)
		.union(ErrorFlags::create(ErrorFlags::TRANSIENT)),
		other => error_flags::classify_message(&other.to_string()),
	};
	is_retryable_flags(flags)
}

/// Exponential backoff with 75-100% jitter, so concurrent oneshots do not
/// re-converge on the same instant.
///
/// ponytail: provider `Retry-After` hints are not read (a resolved `Message`
/// carries no headers); add when oneshots must honor long server-directed
/// waits instead of the capped exponential schedule.
fn backoff_delay_ms(attempt: u32, base_delay_ms: u64, max_delay_ms: u64) -> u64 {
	let growth = base_delay_ms
		.saturating_mul(1u64 << (attempt.saturating_sub(1)).min(16))
		.min(BACKOFF_CEILING_MS);
	let nanos = std::time::SystemTime::now()
		.duration_since(std::time::UNIX_EPOCH)
		.map_or(0, |elapsed| elapsed.subsec_nanos());
	let jitter = 750 + u64::from(nanos % 250);
	(growth.saturating_mul(jitter) / 1000).min(max_delay_ms)
}

/// Run a oneshot completion, retrying transient provider failures.
///
/// `provider.complete` is non-mutating, so replaying the whole request is
/// safe; the function never touches messages, tools, or tool choice. A
/// non-retryable failure is returned or rethrown unchanged.
///
/// # Errors
///
/// Returns the provider error when the final attempt fails or the failure is
/// not retryable.
#[must_use]
pub async fn retry_transient_completion(
	provider: &dyn ModelProvider,
	messages: &[Message],
	tools: &[serde_json::Value],
	tool_choice: &ToolChoice,
	options: OneshotRetryOptions,
) -> Result<Message, ProviderError> {
	let max_attempts = options.max_attempts.max(1);
	for attempt in 1..=max_attempts {
		let last_attempt = attempt == max_attempts;
		match provider.complete(messages, tools, tool_choice).await {
			Ok(message) => {
				if message.stop_reason != Some(StopReason::Error) {
					return Ok(message);
				}
				let flags = error_flags::classify_message(
					message.error_message.as_deref().unwrap_or("unknown error"),
				);
				// A provider-reported error is a resolved message: returning it
				// preserves the exact failure the caller already handles.
				if last_attempt || !is_retryable_flags(flags) {
					return Ok(message);
				}
			},
			Err(error) => {
				if last_attempt || !is_retryable_provider_error(&error) {
					return Err(error);
				}
			},
		}
		let delay = backoff_delay_ms(attempt, options.base_delay_ms, options.max_delay_ms);
		if delay > 0 {
			tokio::time::sleep(Duration::from_millis(delay)).await;
		}
	}
	unreachable!("the attempt loop always returns on the final iteration")
}

#[cfg(test)]
mod tests {
	use std::sync::{
		Arc,
		atomic::{AtomicUsize, Ordering},
	};

	use super::*;
	use crate::state::Message;

	#[derive(Clone)]
	struct ScriptedProvider {
		calls:     Arc<AtomicUsize>,
		failures:  usize,
		error:     fn() -> ProviderError,
	}

	#[async_trait::async_trait]
	impl ModelProvider for ScriptedProvider {
		async fn complete(
			&self,
			_messages: &[Message],
			_tools: &[serde_json::Value],
			_tool_choice: &ToolChoice,
		) -> Result<Message, ProviderError> {
			let call = self.calls.fetch_add(1, Ordering::SeqCst);
			if call < self.failures {
				return Err((self.error)());
			}
			Ok(Message::assistant("answer"))
		}

		async fn stream(
			&self,
			_messages: &[Message],
			_tools: &[serde_json::Value],
			_tool_choice: &ToolChoice,
		) -> Result<super::super::ChunkStream, ProviderError> {
			unreachable!("oneshot tests never stream")
		}
	}

	fn options() -> OneshotRetryOptions {
		OneshotRetryOptions { max_attempts: 3, base_delay_ms: 0, max_delay_ms: 0 }
	}

	#[tokio::test]
	async fn retries_transient_transport_failure() {
		let calls = Arc::new(AtomicUsize::new(0));
		let provider = ScriptedProvider {
			calls:    calls.clone(),
			failures: 1,
			error:    || ProviderError::Network("connection reset".to_string()),
		};
		let message = retry_transient_completion(
			&provider,
			&[],
			&[],
			&ToolChoice::Auto,
			options(),
		)
		.await
		.unwrap();
		assert_eq!(message.content, "answer");
		assert_eq!(calls.load(Ordering::SeqCst), 2);
	}

	#[tokio::test]
	async fn retries_retryable_http_status() {
		let calls = Arc::new(AtomicUsize::new(0));
		let provider = ScriptedProvider {
			calls:    calls.clone(),
			failures: 2,
			error:    || ProviderError::HttpStatus(503, String::new()),
		};
		let message = retry_transient_completion(
			&provider,
			&[],
			&[],
			&ToolChoice::Auto,
			options(),
		)
		.await
		.unwrap();
		assert_eq!(message.content, "answer");
		assert_eq!(calls.load(Ordering::SeqCst), 3);
	}

	#[tokio::test]
	async fn non_retryable_failure_surfaces_immediately() {
		let calls = Arc::new(AtomicUsize::new(0));
		let provider = ScriptedProvider {
			calls:    calls.clone(),
			failures: 3,
			error:    || ProviderError::HttpStatus(400, "invalid request".to_string()),
		};
		let error = retry_transient_completion(
			&provider,
			&[],
			&[],
			&ToolChoice::Auto,
			options(),
		)
		.await
		.unwrap_err();
		assert!(matches!(error, ProviderError::HttpStatus(400, _)));
		assert_eq!(calls.load(Ordering::SeqCst), 1);
	}

	#[tokio::test]
	async fn deterministic_tool_call_parse_failure_is_not_retried() {
		let calls = Arc::new(AtomicUsize::new(0));
		let provider = ScriptedProvider {
			calls:    calls.clone(),
			failures: 3,
			error:    || {
				ProviderError::HttpStatus(
					500,
					"failed to parse tool call arguments as json".to_string(),
				)
			},
		};
		let error = retry_transient_completion(
			&provider,
			&[],
			&[],
			&ToolChoice::Auto,
			options(),
		)
		.await
		.unwrap_err();
		assert!(matches!(error, ProviderError::HttpStatus(500, _)));
		assert_eq!(calls.load(Ordering::SeqCst), 1, "deterministic failure must not replay");
	}

	#[tokio::test]
	async fn returned_error_stop_is_retried_then_returned() {
		struct ErrorStop {
			calls: Arc<AtomicUsize>,
		}

		#[async_trait::async_trait]
		impl ModelProvider for ErrorStop {
			async fn complete(
				&self,
				_messages: &[Message],
				_tools: &[serde_json::Value],
				_tool_choice: &ToolChoice,
			) -> Result<Message, ProviderError> {
				let call = self.calls.fetch_add(1, Ordering::SeqCst);
				let mut message = Message::assistant("");
				if call < 2 {
					message.stop_reason = Some(StopReason::Error);
					message.error_message = Some("server overloaded, try again".to_string());
				} else {
					message.content = "ok".to_string();
				}
				Ok(message)
			}

			async fn stream(
				&self,
				_messages: &[Message],
				_tools: &[serde_json::Value],
				_tool_choice: &ToolChoice,
			) -> Result<super::super::ChunkStream, ProviderError> {
				unreachable!("oneshot tests never stream")
			}
		}

		let calls = Arc::new(AtomicUsize::new(0));
		let message = retry_transient_completion(
			&ErrorStop { calls: calls.clone() },
			&[],
			&[],
			&ToolChoice::Auto,
			options(),
		)
		.await
		.unwrap();
		assert_eq!(message.content, "ok");
		assert_eq!(calls.load(Ordering::SeqCst), 3);
	}

	#[test]
	fn backoff_is_bounded_and_jittered() {
		let base = backoff_delay_ms(1, 500, 30_000);
		assert!((375..=500).contains(&base), "unexpected jitter: {base}");
		let capped = backoff_delay_ms(10, 500, 30_000);
		assert!((6_000..=8_000).contains(&capped), "unexpected cap: {capped}");
		assert_eq!(backoff_delay_ms(10, 500, 100), 100);
	}
}
