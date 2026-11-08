//! Background result delivery contract.
//!
//! The loop consumes finished child results through this module; the supervisor
//! (product-side) is one implementation behind the trait, not a dependency of
//! the loop.

use crate::state::TokenUsage;

/// Pull-based delivery of finished background results into an agent loop.
///
/// The loop calls [`take_background_results`] at a turn boundary; removal and
/// return happen under the supervisor lock, so a result is delivered exactly
/// once even when several callers race.
pub trait BackgroundResultSink: Send + Sync {
	fn take_background_results(&self) -> Vec<String>;

	/// Rich view for hosts that account delivered child usage at the boundary.
	///
	/// Shares one consumption with [`take_background_results`]: whichever view
	/// drains an id first removes it, so a host wrapper may call this while the
	/// loop calls the text view and still see every delivery once. The default
	/// carries no identity or usage (a sink with no supervisor behind it);
	/// supervisor-backed sinks override it.
	fn take_background_deliveries(&self) -> Vec<BackgroundDelivery> {
		self
			.take_background_results()
			.into_iter()
			.map(|text| BackgroundDelivery {
				id: String::new(),
				result_key: None,
				delivery_id: None,
				text,
				usage: None,
			})
			.collect()
	}

	/// Acknowledge that a delivery was already handled (e.g. through durable
	/// recovery or store consumption). Removes it from pending and marks it
	/// consumed so it is not returned by subsequent takes.
	fn acknowledge_delivery(&self, _delivery_id: &str) {}
}

/// A delivered background result with the identity and reported usage that
/// make root accounting exact.
///
/// `id` is the exactly-once key: the supervisor marks a result consumed under
/// the same lock that removes it, so an id reaches a consumer through the
/// auto-delivery path or the hub snapshot path, never both. `usage` is absent
/// when nothing was reported — never a zero-filled stand-in.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BackgroundDelivery {
	pub id: String,
	/// Canonical accounting key. `None` only for a sink that has no supervisor
	/// behind it (the trait default); consumers then fall back to `id`.
	pub result_key: Option<String>,
	/// Durable delivery identity when the host persisted this notification.
	/// `None` for ephemeral/eval results with no delivery row.
	pub delivery_id: Option<String>,
	pub text: String,
	pub usage: Option<TokenUsage>,
}
