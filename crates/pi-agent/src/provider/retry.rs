//! Shared HTTP retry policy for the OpenAI-compatible transport.
//!
//! Always compiled: that transport is available without `direct-providers`,
//! so the status classification and `Retry-After` parsing live here rather
//! than inside a feature-gated adapter.
//!
//! Ports:
//! - `packages/utils/src/fetch-retry.ts` (`extractRetryHint`,
//!   `resolveDefaultDelay`; the TS donor for both Rust adapters),
//! - `packages/ai/src/utils/retry-after.ts` (header hint order and HTTP-date
//!   handling),
//! - `packages/ai/src/error/retryable.ts` (transient status set),
//! - `packages/ai/src/utils/openai-http.ts` (attempt budget and the
//!   concurrency-admission 429 bypass).

use std::{
	sync::LazyLock,
	time::Duration,
};

use reqwest::header::HeaderMap;

/// Status codes the OpenAI-compatible transport retries: 408, 409, 429 and
/// every 5xx (`retryable.ts` transient set).
pub(crate) const fn should_retry_status(status: u16) -> bool {
	matches!(status, 408 | 409 | 429 | 500..=599)
}

/// `x-should-retry` override: an explicit `true`/`false` beats the status.
pub(crate) fn should_retry_header(headers: &HeaderMap) -> Option<bool> {
	headers.get("x-should-retry").and_then(|value| match value.as_bytes() {
		b"true" => Some(true),
		b"false" => Some(false),
		_ => None,
	})
}

/// Retry decision: an explicit `x-should-retry` wins, then the status.
pub(crate) fn should_retry_response(headers: &HeaderMap, status: u16) -> bool {
	should_retry_header(headers).unwrap_or_else(|| should_retry_status(status))
}

/// Retry budget and delay schedule for the transport.
#[derive(Debug, Clone, Copy)]
pub(crate) struct RetryPolicy {
	/// Retries after the first attempt (`attempts = max_retries + 1`).
	pub(crate) max_retries:            u32,
	/// Exponential fallback base delay.
	pub(crate) base_delay_ms:          u64,
	/// Ceiling for the exponential fallback (excludes server hints).
	pub(crate) max_backoff_ms:         u64,
	/// Early jitter applied to the fallback, as a percentage (`0` = none).
	pub(crate) jitter_percent:         u8,
	/// Fixed per-attempt schedule; when set it overrides the exponential ramp.
	pub(crate) fixed_delays_ms:        Option<&'static [u64]>,
	/// Longest server-directed wait honored; a longer hint stops retrying.
	pub(crate) max_server_delay_ms:    u64,
	/// Parse retry hints out of an error body (quota / "try again" phrasing).
	pub(crate) parse_body_hints:       bool,
	/// Honor `x-ratelimit-reset*` headers in addition to `Retry-After`.
	pub(crate) parse_rate_limit_reset: bool,
}

impl Default for RetryPolicy {
	/// OpenAI-compatible transport: five retries, `500ms * 2^attempt` capped at
	/// the 60s server-hint ceiling, no jitter (`openai-http.ts` +
	/// `fetch-retry.ts` defaults).
	fn default() -> Self {
		Self {
			max_retries:            5,
			base_delay_ms:          500,
			max_backoff_ms:         60_000,
			jitter_percent:         0,
			fixed_delays_ms:        None,
			max_server_delay_ms:    60_000,
			parse_body_hints:       true,
			parse_rate_limit_reset: true,
		}
	}
}

impl RetryPolicy {
	/// Wait before retrying `attempt` (0-based), or `None` when the server asked
	/// for more than [`Self::max_server_delay_ms`]; the caller then surfaces the
	/// response instead of sleeping a clamped budget.
	pub(crate) fn delay(&self, headers: &HeaderMap, body: &str, attempt: u32) -> Option<Duration> {
		match self.hint(headers, body) {
			Some(hint) if hint > Duration::from_millis(self.max_server_delay_ms) => None,
			Some(hint) => Some(hint),
			None => Some(self.backoff(attempt)),
		}
	}

	fn hint(&self, headers: &HeaderMap, body: &str) -> Option<Duration> {
		if self.parse_rate_limit_reset {
			let body = if self.parse_body_hints { body } else { "" };
			return extract_retry_hint(headers, body);
		}
		parse_retry_after(headers)
	}

	/// Fallback wait for `attempt` (0-based): the fixed schedule when configured,
	/// else exponential with optional early jitter.
	pub(crate) fn backoff(&self, attempt: u32) -> Duration {
		if let Some(schedule) = self.fixed_delays_ms {
			return schedule
				.get(attempt as usize)
				.copied()
				.or_else(|| schedule.last().copied())
				.map_or(Duration::ZERO, Duration::from_millis);
		}
		let factor = 1u64.checked_shl(attempt).unwrap_or(u64::MAX);
		let base = self
			.base_delay_ms
			.saturating_mul(factor)
			.min(self.max_backoff_ms);
		let percent = u32::from(self.jitter_percent).min(100);
		if percent == 0 {
			return Duration::from_millis(base);
		}
		let nanos = std::time::SystemTime::now()
			.duration_since(std::time::UNIX_EPOCH)
			.map_or(0, |duration| duration.subsec_nanos());
		let fraction = f64::from(nanos % 1000) / 1000.0;
		let factor = 1.0 - fraction * (f64::from(percent) / 100.0);
		Duration::from_millis((base as f64 * factor) as u64)
	}
}

/// Sleep before a retry. Cancellation-aware: the timer is dropped with the
/// future, so a caller abort or watchdog cancels the wait.
pub(crate) async fn sleep_before_retry(delay: Duration) {
	if delay.is_zero() {
		tokio::task::yield_now().await;
		return;
	}
	tokio::time::sleep(delay).await;
}

/// `true` for a proxy concurrency-admission 429 that must bypass transport
/// retry (`openai-http.ts` `isConcurrencyAdmissionRejection`): LiteLLM sheds
/// `max_parallel_requests` immediately and the session layer owns its backoff.
pub(crate) fn is_concurrency_admission_rejection(headers: &HeaderMap, body: &str) -> bool {
	static BODY_MARKER: LazyLock<regex::Regex> = LazyLock::new(|| {
		regex::Regex::new(r#""rate_limit_type"\s*:\s*"max_parallel_requests""#)
			.expect("concurrency admission pattern")
	});
	let header_marker = headers
		.get("rate_limit_type")
		.and_then(|value| value.to_str().ok())
		.map(str::trim);
	header_marker == Some("max_parallel_requests") || BODY_MARKER.is_match(body)
}

/// Server-directed hint across every donor header source and body phrasing.
///
/// Header order follows `fetch-retry.ts`; the first usable source wins.
pub(crate) fn extract_retry_hint(headers: &HeaderMap, body: &str) -> Option<Duration> {
	parse_retry_after(headers)
		.or_else(|| parse_rate_limit_reset(headers))
		.or_else(|| body_retry_hint(body))
}

/// `retry-after-ms` (delta ms) then `Retry-After` (seconds or HTTP-date).
fn parse_retry_after(headers: &HeaderMap) -> Option<Duration> {
	if let Some(value) = headers.get("retry-after-ms").and_then(|value| value.to_str().ok())
		&& let Ok(ms) = value.trim().parse::<u64>()
	{
		return Some(Duration::from_millis(ms));
	}
	let value = headers.get("retry-after")?.to_str().ok()?;
	let trimmed = value.trim();
	if let Ok(seconds) = trimmed.parse::<u64>() {
		return Some(Duration::from_secs(seconds));
	}
	let target = parse_http_date_epoch(trimmed)?;
	let target_ms = u128::from(target).saturating_mul(1_000);
	Some(Duration::from_millis(
		target_ms.saturating_sub(now_unix_ms()) as u64,
	))
}

/// `x-ratelimit-reset-ms` (epoch ms/s or delta ms), `x-ratelimit-reset`
/// (epoch seconds) and `x-ratelimit-reset-after` (delta seconds).
fn parse_rate_limit_reset(headers: &HeaderMap) -> Option<Duration> {
	if let Some(value) = headers
		.get("x-ratelimit-reset-ms")
		.and_then(|value| value.to_str().ok())
		&& let Ok(raw) = value.trim().parse::<u64>()
	{
		let now = now_unix_ms();
		let target = if raw > 1_000_000_000_000 {
			u128::from(raw)
		} else if raw > 1_000_000_000 {
			u128::from(raw) * 1_000
		} else {
			return Some(Duration::from_millis(raw));
		};
		if target > now {
			return Some(Duration::from_millis((target - now) as u64));
		}
	}
	if let Some(value) = headers
		.get("x-ratelimit-reset")
		.and_then(|value| value.to_str().ok())
		&& let Ok(seconds) = value.trim().parse::<u64>()
	{
		let target = u128::from(seconds) * 1_000;
		if target > now_unix_ms() {
			return Some(Duration::from_millis((target - now_unix_ms()) as u64));
		}
	}
	if let Some(value) = headers
		.get("x-ratelimit-reset-after")
		.and_then(|value| value.to_str().ok())
		&& let Ok(seconds) = value.trim().parse::<u64>()
		&& seconds > 0
	{
		return Some(Duration::from_secs(seconds));
	}
	None
}

fn now_unix_ms() -> u128 {
	std::time::SystemTime::now()
		.duration_since(std::time::UNIX_EPOCH)
		.map_or(0, |elapsed| elapsed.as_millis())
}

/// Quota and "try again" phrasings from gateway error bodies.
fn body_retry_hint(body: &str) -> Option<Duration> {
	if body.is_empty() {
		return None;
	}
	let patterns = retry_body_patterns();
	if let Some(captures) = patterns.quota.captures(body) {
		let hours: f64 = captures
			.get(1)
			.and_then(|value| value.as_str().parse().ok())
			.unwrap_or(0.0);
		let minutes: f64 = captures
			.get(2)
			.and_then(|value| value.as_str().parse().ok())
			.unwrap_or(0.0);
		let seconds: f64 = captures
			.get(3)
			.and_then(|value| value.as_str().parse().ok())
			.unwrap_or(f64::NAN);
		if seconds.is_finite() {
			let total_ms = ((hours * 60.0 + minutes) * 60.0 + seconds) * 1000.0;
			if total_ms > 0.0 {
				return Some(Duration::from_millis(total_ms as u64));
			}
		}
	}
	// Account-reset hints precede short retry hints: a body carrying both must
	// honor the longer window (`fetch-retry.ts`).
	for pattern in [
		&patterns.will_reset,
		&patterns.please_retry,
		&patterns.retry_delay,
		&patterns.try_again,
	] {
		let Some(captures) = pattern.captures(body) else {
			continue;
		};
		let value: f64 = captures
			.get(1)
			.and_then(|value| value.as_str().parse().ok())
			.unwrap_or(f64::NAN);
		let unit = captures.get(2).map_or("", |value| value.as_str());
		if value.is_finite() && value > 0.0 {
			if let Some(unit_ms) = retry_unit_to_ms(unit) {
				return Some(Duration::from_millis((value * unit_ms as f64) as u64));
			}
		}
	}
	None
}

struct RetryBodyPatterns {
	quota:        regex::Regex,
	will_reset:   regex::Regex,
	please_retry: regex::Regex,
	retry_delay:  regex::Regex,
	try_again:    regex::Regex,
}

static RETRY_BODY_PATTERNS: LazyLock<RetryBodyPatterns> = LazyLock::new(|| RetryBodyPatterns {
	quota:        regex::Regex::new(r"(?i)reset after (?:(\d+)h)?(?:(\d+)m)?(\d+(?:\.\d+)?)s")
		.expect("quota reset pattern"),
	will_reset:   regex::Regex::new(
		r"(?i)(?:will\s+)?reset in\s+~?\s*([0-9.]+)\s*(ms|sec|s|minutes?|mins?|m|hours?|hrs?|h)\b",
	)
	.expect("will reset pattern"),
	please_retry: regex::Regex::new(r"(?i)Please retry in ([0-9.]+)(ms|s)")
		.expect("please retry pattern"),
	retry_delay:  regex::Regex::new(r#"(?i)"retryDelay":\s*"([0-9.]+)(ms|s)""#)
		.expect("retryDelay pattern"),
	try_again:    regex::Regex::new(
		r"(?i)try again in\s+~?\s*([0-9.]+)\s*(ms|sec|s|minutes?|mins?|m|hours?|hrs?|h)\b",
	)
	.expect("try again pattern"),
});

fn retry_body_patterns() -> &'static RetryBodyPatterns {
	&RETRY_BODY_PATTERNS
}

fn retry_unit_to_ms(unit: &str) -> Option<u64> {
	match unit.to_ascii_lowercase().as_str() {
		"ms" => Some(1),
		"s" | "sec" => Some(1_000),
		"m" | "min" | "mins" | "minute" | "minutes" => Some(60_000),
		"h" | "hr" | "hrs" | "hour" | "hours" => Some(3_600_000),
		_ => None,
	}
}

/// HTTP-date to Unix epoch seconds. Accepts the three `Date.parse` forms:
/// IMF-fixdate, RFC 850 and asctime.
fn parse_http_date_epoch(value: &str) -> Option<u64> {
	let value = value.trim();
	if let Some(comma) = value.find(',') {
		let rest = value[comma + 1..].trim();
		if let Some(epoch) = parse_imf_date(rest) {
			return Some(epoch);
		}
		return parse_rfc850_date(rest);
	}
	parse_asctime_date(value)
}

fn parse_imf_date(rest: &str) -> Option<u64> {
	let mut parts = rest.split_whitespace();
	let day = parse_http_number(parts.next()?, 1, 2)?;
	let month = http_month(parts.next()?)?;
	let year = parse_http_number(parts.next()?, 4, 4)?;
	let (hour, minute, second) = parse_http_time(parts.next()?)?;
	if !parts.next()?.eq_ignore_ascii_case("GMT") || parts.next().is_some() {
		return None;
	}
	assemble_http_epoch(i32::try_from(year).ok()?, month, day, hour, minute, second)
}

fn parse_rfc850_date(rest: &str) -> Option<u64> {
	let mut parts = rest.split_whitespace();
	let date = parts.next()?;
	let (day_text, month_text, year_text) = {
		let mut date_parts = date.split('-');
		let day = date_parts.next()?;
		let month = date_parts.next()?;
		let year = date_parts.next()?;
		if date_parts.next().is_some() {
			return None;
		}
		(day, month, year)
	};
	let day = parse_http_number(day_text, 1, 2)?;
	let month = http_month(month_text)?;
	// RFC 850 year is exactly two digits; no wider parse can reach here.
	let short_year = parse_http_number(year_text, 2, 2)?;
	let year = if short_year <= 49 {
		2000 + short_year
	} else {
		1900 + short_year
	};
	let (hour, minute, second) = parse_http_time(parts.next()?)?;
	if !parts.next()?.eq_ignore_ascii_case("GMT") || parts.next().is_some() {
		return None;
	}
	assemble_http_epoch(i32::try_from(year).ok()?, month, day, hour, minute, second)
}

fn parse_asctime_date(value: &str) -> Option<u64> {
	let mut parts = value.split_whitespace();
	let _weekday = parts.next()?;
	let month = http_month(parts.next()?)?;
	let day = parse_http_number(parts.next()?, 1, 2)?;
	let (hour, minute, second) = parse_http_time(parts.next()?)?;
	let year = parse_http_number(parts.next()?, 4, 4)?;
	if parts.next().is_some() {
		return None;
	}
	assemble_http_epoch(i32::try_from(year).ok()?, month, day, hour, minute, second)
}

/// ASCII digits only, bounded length, so server input cannot overflow a wider
/// numeric parse or smuggle signs/whitespace.
fn parse_http_number(value: &str, min_len: usize, max_len: usize) -> Option<u32> {
	let bytes = value.as_bytes();
	if bytes.len() < min_len || bytes.len() > max_len || !bytes.iter().all(u8::is_ascii_digit) {
		return None;
	}
	value.parse().ok()
}

fn parse_http_time(value: &str) -> Option<(u64, u64, u64)> {
	let mut parts = value.split(':');
	let hour = u64::from(parse_http_number(parts.next()?, 1, 2)?);
	let minute = u64::from(parse_http_number(parts.next()?, 1, 2)?);
	let second = u64::from(parse_http_number(parts.next()?, 1, 2)?);
	if parts.next().is_some() || hour > 23 || minute > 59 || second > 59 {
		return None;
	}
	Some((hour, minute, second))
}

fn http_month(value: &str) -> Option<u32> {
	Some(match value.to_ascii_lowercase().as_str() {
		"jan" => 1,
		"feb" => 2,
		"mar" => 3,
		"apr" => 4,
		"may" => 5,
		"jun" => 6,
		"jul" => 7,
		"aug" => 8,
		"sep" => 9,
		"oct" => 10,
		"nov" => 11,
		"dec" => 12,
		_ => return None,
	})
}

fn assemble_http_epoch(
	year: i32,
	month: u32,
	day: u32,
	hour: u64,
	minute: u64,
	second: u64,
) -> Option<u64> {
	if !(1..=31).contains(&day) {
		return None;
	}
	let days = days_from_civil(year, month, day);
	let seconds = days
		.checked_mul(86_400)?
		.checked_add((hour * 3_600 + minute * 60 + second) as i64)?;
	u64::try_from(seconds).ok()
}

fn days_from_civil(year: i32, month: u32, day: u32) -> i64 {
	let year = i64::from(year) - i64::from(month <= 2);
	let era = if year >= 0 { year } else { year - 399 } / 400;
	let year_of_era = year - era * 400;
	let month_prime = if month > 2 { month - 3 } else { month + 9 };
	let day_of_year = i64::from((153 * month_prime + 2) / 5 + day - 1);
	let day_of_era = year_of_era * 365 + year_of_era / 4 - year_of_era / 100 + day_of_year;
	era * 146_097 + day_of_era - 719_468
}

#[cfg(test)]
mod tests {
	use super::*;

	fn headers(pairs: &[(&str, &str)]) -> HeaderMap {
		let mut map = HeaderMap::new();
		for (name, value) in pairs {
			map.insert(
				reqwest::header::HeaderName::from_bytes(name.as_bytes()).expect("header name"),
				value.parse().expect("header value"),
			);
		}
		map
	}

	#[test]
	fn parses_all_three_http_date_forms() {
		let imf = parse_http_date_epoch("Sun, 06 Nov 1994 08:49:37 GMT").expect("IMF-fixdate");
		assert_eq!(imf, 784_111_777);
		let rfc850 = parse_http_date_epoch("Sunday, 06-Nov-94 08:49:37 GMT").expect("RFC 850");
		assert_eq!(rfc850, 784_111_777);
		let asctime = parse_http_date_epoch("Sun Nov  6 08:49:37 1994").expect("asctime");
		assert_eq!(asctime, 784_111_777);
		assert_eq!(parse_http_date_epoch("Sun, 32 Nov 1994 08:49:37 GMT"), None);
	}

	#[test]
	fn retry_after_ms_and_seconds_are_honored() {
		let hint = extract_retry_hint(&headers(&[("retry-after-ms", "1500")]), "");
		assert_eq!(hint, Some(Duration::from_millis(1_500)));
		let hint = extract_retry_hint(&headers(&[("retry-after", "7")]), "");
		assert_eq!(hint, Some(Duration::from_secs(7)));
	}

	#[test]
	fn http_date_retry_after_produces_a_future_delay() {
		let hint = extract_retry_hint(
			&headers(&[("retry-after", "Sun, 06 Nov 2094 08:49:37 GMT")]),
			"",
		)
		.expect("future date");
		assert!(hint >= Duration::from_secs(60 * 60 * 24 * 365 * 60), "{hint:?}");
	}

	#[test]
	fn rate_limit_reset_headers_are_honored() {
		let hint = extract_retry_hint(&headers(&[("x-ratelimit-reset-after", "30")]), "");
		assert_eq!(hint, Some(Duration::from_secs(30)));
		// Small values are a delta in milliseconds.
		let hint = extract_retry_hint(&headers(&[("x-ratelimit-reset-ms", "2500")]), "");
		assert_eq!(hint, Some(Duration::from_millis(2_500)));
	}

	#[test]
	fn exponential_backoff_is_capped_and_jitter_only_shortens() {
		let openai = RetryPolicy::default();
		assert_eq!(openai.backoff(0), Duration::from_millis(500));
		assert_eq!(openai.backoff(3), Duration::from_millis(4_000));
		assert_eq!(openai.backoff(5), Duration::from_millis(16_000));
		assert_eq!(openai.backoff(20), Duration::from_millis(60_000), "cap");

		let anthropic = RetryPolicy {
			max_retries: 2,
			base_delay_ms: 500,
			max_backoff_ms: 8_000,
			jitter_percent: 25,
			fixed_delays_ms: None,
			max_server_delay_ms: 60_000,
			parse_body_hints: false,
			parse_rate_limit_reset: false,
		};
		for attempt in 0..6 {
			let base = anthropic.backoff(attempt);
			let capped = (500u64 << attempt.min(4)).min(8_000);
			assert!(base <= Duration::from_millis(capped), "{base:?}");
			assert!(base >= Duration::from_millis(capped * 3 / 4), "{base:?}");
		}
	}

	#[test]
	fn fixed_schedule_clamps_to_the_last_entry() {
		static SCHEDULE: [u64; 3] = [2_000, 5_000, 10_000];
		let ollama = RetryPolicy {
			max_retries: 4,
			base_delay_ms: 0,
			max_backoff_ms: 0,
			jitter_percent: 0,
			fixed_delays_ms: Some(&SCHEDULE),
			max_server_delay_ms: 60_000,
			parse_body_hints: true,
			parse_rate_limit_reset: true,
		};
		assert_eq!(ollama.backoff(0), Duration::from_millis(2_000));
		assert_eq!(ollama.backoff(1), Duration::from_millis(5_000));
		assert_eq!(ollama.backoff(2), Duration::from_millis(10_000));
		assert_eq!(ollama.backoff(3), Duration::from_millis(10_000));
	}

	#[test]
	fn hints_above_the_cap_stop_retrying() {
		let policy = RetryPolicy::default();
		let hint = headers(&[("retry-after", "120")]);
		assert_eq!(policy.delay(&hint, "", 0), None, "over-cap hint must fail fast");
		let hint = headers(&[("retry-after", "5")]);
		assert_eq!(policy.delay(&hint, "", 0), Some(Duration::from_secs(5)));
		assert_eq!(policy.delay(&HeaderMap::new(), "", 1), Some(Duration::from_millis(1_000)));
	}

	#[test]
	fn status_classification_matches_the_donors() {
		for status in [408, 409, 429, 500, 503, 599] {
			assert!(should_retry_status(status), "{status}");
		}
		for status in [400, 401, 403, 404, 422] {
			assert!(!should_retry_status(status), "{status}");
		}
		let retry = headers(&[("x-should-retry", "true")]);
		assert!(should_retry_response(&retry, 400));
		let no_retry = headers(&[("x-should-retry", "false")]);
		assert!(!should_retry_response(&no_retry, 503));
		assert!(should_retry_response(&HeaderMap::new(), 429));
		assert!(!should_retry_response(&HeaderMap::new(), 401));
	}

	#[test]
	fn concurrency_admission_429_is_classified() {
		let header = headers(&[("rate_limit_type", "max_parallel_requests")]);
		assert!(is_concurrency_admission_rejection(&header, ""));
		assert!(is_concurrency_admission_rejection(
			&HeaderMap::new(),
			r#"{"error":{"rate_limit_type":"max_parallel_requests"}}"#
		));
		assert!(!is_concurrency_admission_rejection(&HeaderMap::new(), "slow down"));
	}
}
