//! Bounded and redacted HTTP response-body handling for the
//! OpenAI-compatible transport.
//!
//! Always compiled: that transport is available without `direct-providers`.

use futures::StreamExt;

use crate::provider::{ProviderError, redact_with, snippet};

/// Body text capped at a byte limit, plus whether bytes were dropped.
pub(crate) struct BoundedBody {
	pub(crate) text:      String,
	pub(crate) truncated: bool,
}

/// Read at most `limit` body bytes, then stop; never buffers past the ceiling
/// even for an endless response.
pub(crate) async fn read_body_bounded(
	response: reqwest::Response,
	limit: usize,
) -> Result<BoundedBody, ProviderError> {
	let mut stream = response.bytes_stream();
	let mut bytes: Vec<u8> = Vec::new();
	let mut truncated = false;
	while let Some(item) = stream.next().await {
		let chunk = item.map_err(|err| ProviderError::Network(err.to_string()))?;
		if bytes.len() + chunk.len() > limit {
			let room = limit.saturating_sub(bytes.len());
			bytes.extend_from_slice(&chunk[..room]);
			truncated = true;
			break;
		}
		bytes.extend_from_slice(&chunk);
	}
	let mut text = String::from_utf8_lossy(&bytes).into_owned();
	if truncated {
		trim_partial_token(&mut text);
	}
	Ok(BoundedBody { text, truncated })
}

/// Best effort for a byte-capped read: drop the incomplete trailing token so a
/// credential sliced across the cap is unlikely to survive as a prefix.
pub(crate) fn trim_partial_token(text: &mut String) {
	let boundary = text
		.char_indices()
		.rev()
		.find(|(_, ch)| {
			ch.is_whitespace() || matches!(ch, '"' | '\'' | ',' | ':' | '{' | '}' | '[' | ']')
		})
		.map(|(index, ch)| index + ch.len_utf8());
	if let Some(boundary) = boundary
		&& boundary < text.len()
	{
		text.truncate(boundary);
	}
}

/// Redact before bounding so a credential cut by the character limit cannot
/// survive as a prefix; `truncated` marks a byte-capped read.
pub(crate) fn bounded_redaction(text: &str, secrets: &[String], truncated: bool) -> String {
	let mut bounded = snippet(&redact_with(text, secrets));
	if truncated {
		bounded.push_str("\n[truncated]");
	}
	bounded
}

#[cfg(test)]
mod tests {
	use tokio::io::{AsyncReadExt, AsyncWriteExt};
	use tokio::net::TcpListener;

	use super::*;
	use crate::provider::ERROR_BODY_CHARS;

	#[test]
	fn trim_partial_token_drops_the_cut_tail() {
		let mut text = "error: sk-secret".to_string();
		trim_partial_token(&mut text);
		assert_eq!(text, "error: ");
	}

	#[test]
	fn bounded_redaction_redacts_before_truncating() {
		// The secret straddles the character cap, so truncating first would keep
		// a credential prefix; redaction runs before the cap.
		let text = format!("{}sk-secret", "x".repeat(ERROR_BODY_CHARS - 5));
		let bounded = bounded_redaction(&text, &["sk-secret".to_string()], true);
		assert!(!bounded.contains("sk-secret"), "leaked secret: {bounded}");
		assert!(bounded.ends_with("[truncated]"));
		assert!(bounded.chars().count() <= ERROR_BODY_CHARS + "\n[truncated]".chars().count());
	}

	#[tokio::test]
	async fn read_body_bounded_stops_at_the_ceiling_and_trims_the_tail() {
		let listener = TcpListener::bind(("127.0.0.1", 0)).await.expect("bind");
		let addr = listener.local_addr().expect("addr");
		let body = format!("{} sk-secret", "y".repeat(4_000));
		tokio::spawn(async move {
			let (mut socket, _) = listener.accept().await.expect("accept");
			let mut scratch = [0u8; 1024];
			// Drain the request headers; the body length is irrelevant here.
			let _ = socket.read(&mut scratch).await;
			let head = format!(
				"HTTP/1.1 200 OK\r\ncontent-type: text/plain\r\ncontent-length: {}\r\nconnection: close\r\n\r\n",
				body.len()
			);
			let _ = socket.write_all(head.as_bytes()).await;
			let _ = socket.write_all(body.as_bytes()).await;
			let _ = socket.shutdown().await;
		});
		let response = reqwest::get(format!("http://{addr}/")).await.expect("response");
		let bounded = read_body_bounded(response, 1_000).await.expect("bounded");
		assert!(bounded.truncated);
		assert_eq!(bounded.text.len(), 1_000);
		assert!(!bounded.text.ends_with("sk-secret"), "partial token kept: {}", bounded.text);
	}
}
