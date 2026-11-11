//! TS `ServiceTierByFamily` resolution (`packages/ai/src/types.ts`): one
//! implementation shared by the RPC owner, the root runtime and the child
//! factory. Values are `ServiceTier` strings; an empty map means "no
//! selection" and never re-enables a configured default.

use std::collections::BTreeMap;

fn is_openai_service_tier_api(api: Option<&str>) -> bool {
	matches!(
		api,
		Some("openai-completions" | "openai-responses" | "openai-codex-responses")
	)
}

/// Port of TS `isOpenAIModelId` (`packages/catalog/src/identity/family.ts`).
fn is_openai_model_id(id: &str) -> bool {
	let lower = id.to_ascii_lowercase();
	if lower.contains("openai/") {
		return true;
	}
	for segment in lower.split('/') {
		for prefix in ["gpt-", "gpt.", "chatgpt-", "chatgpt.", "codex-", "codex."] {
			if segment.starts_with(prefix) {
				return true;
			}
		}
		for sku in ["o1", "o3", "o4"] {
			if segment == sku
				|| segment.starts_with(&format!("{sku}-"))
				|| segment.starts_with(&format!("{sku}."))
			{
				return true;
			}
		}
	}
	false
}

fn excludes_inferred_openai_service_tier(provider: &str) -> bool {
	matches!(provider, "fireworks" | "github-copilot")
}

fn is_openai_service_tier_model(provider: &str, api: Option<&str>, id: &str) -> bool {
	!excludes_inferred_openai_service_tier(provider)
		&& is_openai_service_tier_api(api)
		&& is_openai_model_id(id)
}

/// Port of TS `serviceTierFamily`.
pub(crate) fn family(provider: &str, api: Option<&str>, id: &str) -> Option<&'static str> {
	if provider == "openrouter" {
		let lower = id.to_ascii_lowercase();
		if lower.starts_with("anthropic/") {
			return Some("anthropic");
		}
		if lower.starts_with("google/") {
			return Some("google");
		}
		if lower.starts_with("openai/") {
			return Some("openai");
		}
		return None;
	}
	if matches!(provider, "openai" | "openai-codex") {
		return Some("openai");
	}
	if api == Some("anthropic-messages") {
		return Some("anthropic");
	}
	if matches!(provider, "google" | "google-vertex") {
		return Some("google");
	}
	if is_openai_service_tier_model(provider, api, id) {
		return Some("openai");
	}
	None
}

/// Port of TS `shouldSendServiceTier`.
pub(crate) fn should_send_service_tier(provider: &str, api: Option<&str>, id: &str) -> bool {
	if matches!(provider, "openai" | "openai-codex") {
		return true;
	}
	if provider == "openrouter" {
		return true;
	}
	if is_openai_service_tier_model(provider, api, id) {
		return true;
	}
	matches!(provider, "google" | "google-vertex" | "fireworks")
}

/// Port of TS `realizesPriorityServiceTier`: whether `priority` reaches the
/// provider wire for this model. Bedrock/Vertex Claude and OpenRouter Anthropic
/// models silently drop it, so they report inactive.
pub(crate) fn realizes_priority(provider: &str, api: Option<&str>, id: &str) -> bool {
	if provider == "anthropic" {
		return true;
	}
	if provider == "openrouter" {
		let family = family(provider, api, id);
		return matches!(family, Some("openai" | "google"));
	}
	if api == Some("anthropic-messages") {
		return false;
	}
	should_send_service_tier(provider, api, id)
}

/// TS `resolveModelServiceTier`: the active model's family entry, or `None`.
/// An absent family or an empty value stays `None`, so an explicit reset never
/// falls back to the configured default.
pub(crate) fn resolve<'a>(
	tiers: &'a BTreeMap<String, String>,
	provider: &str,
	api: Option<&str>,
	id: &str,
) -> Option<&'a str> {
	let family = family(provider, api, id)?;
	tiers
		.get(family)
		.map(String::as_str)
		.filter(|tier| !tier.is_empty())
}

/// TS `--service-tier` override (`cli/args.ts:48` -> `sdk.ts:3408`): it only
/// mutates the `openai` family entry of the effective map, preserving the
/// other families. `none`/empty removes only that entry.
pub fn overlay_openai(
	explicit: Option<&str>,
	base: &BTreeMap<String, String>,
) -> BTreeMap<String, String> {
	let mut out = base.clone();
	let value = explicit.filter(|value| !matches!(*value, "" | "inherit"));
	match value {
		Some("none") => {
			out.remove("openai");
		},
		Some(value) => {
			if family_value("openai", value).is_some() {
				out.insert("openai".to_string(), value.to_string());
			}
		},
		None => {},
	}
	out
}

/// TS `isServiceTierForFamily`: the value a family can actually realize.
/// `none`/empty/unknown values yield `None`.
pub(crate) fn family_value(family: &str, value: &str) -> Option<&'static str> {
	let values: &[&'static str] = match family {
		"openai" => &["auto", "default", "flex", "scale", "priority"],
		"anthropic" => &["priority"],
		"google" => &["flex", "priority"],
		_ => return None,
	};
	values.iter().copied().find(|tier| *tier == value)
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn explicit_override_only_mutates_the_openai_family() {
		let base = BTreeMap::from([
			("anthropic".to_string(), "priority".to_string()),
			("openai".to_string(), "flex".to_string()),
		]);
		let kept = overlay_openai(Some("none"), &base);
		assert_eq!(kept.get("openai"), None);
		assert_eq!(kept.get("anthropic").map(String::as_str), Some("priority"));
		let replaced = overlay_openai(Some("scale"), &base);
		assert_eq!(replaced.get("openai").map(String::as_str), Some("scale"));
		assert_eq!(replaced.get("anthropic").map(String::as_str), Some("priority"));
		let inherited = overlay_openai(Some("inherit"), &base);
		assert_eq!(inherited, base);
		assert!(overlay_openai(Some("priority"), &BTreeMap::new())
			.get("anthropic")
			.is_none());
	}
}
