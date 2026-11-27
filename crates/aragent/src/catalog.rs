//! Model catalog: `models.json` (bundled snapshot) or `models.yml` overrides.
//!
//! Search order (`models.json`): `<cwd>/models.json`, `~/.omp/agent/models.json`,
//! `<share>/omp/models.json`. The bundled file is ~10 MB, so it is decoded
//! straight from a buffered reader into borrowed-free provider/model maps and
//! never materialised as one `String`.

use std::collections::BTreeMap;
use std::fs;
use std::io::BufReader;
use std::path::{Path, PathBuf};

use serde::Deserialize;

use crate::config::{self, ProviderConfig};
use crate::{Error, Result, dirs, yaml};

#[derive(Debug, Default, Clone, PartialEq)]
pub struct Catalog {
	/// provider id -> model id -> definition subset the runtime consumes.
	pub providers: BTreeMap<String, BTreeMap<String, ModelInfo>>,
	/// Connection blocks from `models.yml` (`baseUrl`, `apiKey`, `api`, `auth`,
	/// `headers`) — the same provider fields `docs/models.md` documents.
	pub connections: BTreeMap<String, ProviderConfig>,
	pub source: PathBuf,
}

/// Fields of one model definition the headless runtime and `arag models` consume.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct ModelInfo {
	/// Provider reference: `def.provider`, else the enclosing provider key.
	pub provider:        String,
	/// `contextWindow` when the definition declares one.
	pub context_window:  Option<usize>,
	/// Input modalities (`["text"]`, `["text", "image"]`) when declared by catalog.
	pub input:           Option<Vec<String>>,
	/// Display name; `arag models` falls back to the model id.
	pub name:            Option<String>,
	/// `maxTokens` output cap when declared.
	pub max_tokens:      Option<u64>,
	/// TS `Model.omitMaxOutputTokens`: `Some(true)` suppresses the wire output
	/// cap while `maxTokens` still budgets locally; `Some(false)` is an
	/// authored re-enable and `None` means the definition never mentioned it
	/// (`models.json`/`models.yml`). Kept tri-state so a patch can override a
	/// bundled `true` with an explicit `false`.
	pub omit_max_output_tokens: Option<bool>,
	/// `reasoning` flag (defaults to false, as the catalog schema does).
	pub reasoning:       bool,
	/// Supported thinking efforts (`thinking.efforts`); `None` when the
	/// definition carries no thinking config.
	pub efforts:         Option<Vec<String>>,
	/// Declared `cost` block normalized to the four upstream rates
	/// (`input`/`output`/`cacheRead`/`cacheWrite`, missing ones `0`); extra
	/// keys such as `longContext` are preserved for `--json`.
	pub cost:            Option<serde_json::Value>,
	/// Per-model wire dialect (`api`), as authored. `None` inherits the
	/// connection's `api`; a provider-level `api` never masks a declared
	/// per-model one (TS `modelDef.api ?? providerApi`).
	pub api:             Option<String>,
	/// Per-model `baseUrl`: authored for `models.yml` models, and replaced by
	/// the provider `baseUrl` for bundled `models.json` rows (TS transport
	/// override). `None` leaves the connection's `baseUrl` to the consumer.
	pub base_url:        Option<String>,
	/// Sparse `compat` block resolved for this model: the provider-level
	/// block is the base and the model's own keys win (`models.yml` models,
	/// TS `mergeCompat(providerCompat, modelDef.compat)`). Bundled
	/// `models.json` rows are materialized, so a `models.yml` provider block
	/// overrides them key by key instead (TS `#loadBuiltInModels`). Kept as
	/// `serde_json::Value` so dialect keys this binary does not model survive.
	pub compat:          Option<serde_json::Value>,
	/// Authored `thinking` block preserved verbatim (`mode`, `efforts`,
	/// `maxTokens`, `effortMap`, ...); `efforts` above is its listing subset.
	/// No provider-level fallback: TS inherits thinking from the model only.
	pub thinking:        Option<serde_json::Value>,
	/// TS `Model.headers`: per-model request headers as authored by the row,
	/// layered before connection headers by the runtime. Never carries a
	/// credential; the adapter appends `Authorization` last.
	pub headers:          Option<BTreeMap<String, String>>,
	/// TS `Model.requestModelId`: wire model id when it differs from `id`.
	pub request_model_id: Option<String>,
	/// TS `Model.tokenizer`: local tokenizer family when the row declares one.
	pub tokenizer:        Option<String>,
	/// TS `compat.supportsUsageInStreaming` override for this model
	/// (`models.json`/`models.yml`). `None` means the model declares nothing,
	/// so the provider-level default applies (`!isCerebras`, as
	/// `compat/openai.ts` resolves it).
	pub supports_usage_in_streaming: Option<bool>,
}

/// Wire field carrying the output-token cap for an OpenAI-compatible model.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MaxTokensField {
	/// Legacy `max_tokens` (Mistral, native Kimi/Moonshot, Z.ai/Zhipu, Chutes,
	/// Fireworks, and DeepSeek's first-party API).
	MaxTokens,
	/// `max_completion_tokens`, used by OpenAI and every host that does not
	/// require the legacy field.
	MaxCompletionTokens,
}

impl MaxTokensField {
	fn parse(value: &str) -> Option<Self> {
		match value {
			"max_tokens" => Some(Self::MaxTokens),
			"max_completion_tokens" => Some(Self::MaxCompletionTokens),
			_ => None,
		}
	}
}

/// Reasoning/thinking request dialect for an OpenAI-compatible endpoint.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ThinkingFormat {
	/// OpenAI-style `reasoning_effort` (the fallback).
	OpenAi,
	/// OpenRouter's reasoning control.
	OpenRouter,
	/// Z.ai/Moonshot binary `thinking: { type }` block.
	Zai,
	/// Kimi-specific `thinking: { type, effort }` block.
	Kimi,
	/// Qwen's top-level `enable_thinking` dialect.
	Qwen,
	/// Qwen served with vLLM's `chat_template_kwargs.enable_thinking`.
	QwenChatTemplate,
	/// Generic chat-template thinking switch.
	ChatTemplate,
}

impl ThinkingFormat {
	fn parse(value: &str) -> Option<Self> {
		match value {
			"openai" => Some(Self::OpenAi),
			"openrouter" => Some(Self::OpenRouter),
			"zai" => Some(Self::Zai),
			"kimi" => Some(Self::Kimi),
			"qwen" => Some(Self::Qwen),
			"qwen-chat-template" => Some(Self::QwenChatTemplate),
			"chat-template" => Some(Self::ChatTemplate),
			_ => None,
		}
	}
}

/// Resolved chat-completions compat record for one model: the subset of TS
/// `ResolvedOpenAICompat` the headless runtime consumes, with `buildOpenAICompat`
/// provider/base-URL/model-id defaults already applied and sparse catalog
/// `compat` keys layered on top. Resolution happens once per lookup, never per
/// request field.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OpenAiCompat {
	/// Accept the `developer` role instead of folding it into `system`.
	pub supports_developer_role:     bool,
	/// Accept the OpenAI `store` request field.
	pub supports_store:              bool,
	/// Accept `reasoning_effort`.
	pub supports_reasoning_effort:   bool,
	/// Wire field carrying the output cap.
	pub max_tokens_field:            MaxTokensField,
	/// Reasoning/thinking request dialect.
	pub thinking_format:             ThinkingFormat,
	/// Effort remap for this model (detected Kimi K3 values first, catalog
	/// `reasoningEffortMap` keys win).
	pub reasoning_effort_map:        BTreeMap<String, String>,
	/// Stream-watchdog idle floor, when the host needs a widened one.
	pub stream_idle_timeout_ms:      Option<u64>,
	/// Accept OpenAI strict-mode tool schemas.
	pub supports_strict_mode:        bool,
}

impl OpenAiCompat {
	/// Resolve the chat-completions compat record for one model: TS
	/// `buildOpenAICompat` defaults from `provider`, `base_url`, `model_id`
	/// and `reasoning`, then the merged sparse `compat` block overrides keys it
	/// names. Unknown enum values keep the detected default.
	#[must_use]
	pub fn resolve(
		provider: &str,
		base_url: Option<&str>,
		model_id: &str,
		reasoning: bool,
		sparse: Option<&serde_json::Value>,
	) -> Self {
		let url = base_url.unwrap_or_default().to_ascii_lowercase();
		let model = model_id.to_ascii_lowercase();

		let is_openai = provider == "openai" || url.contains("api.openai.com");
		let is_azure = provider == "azure"
			|| url.contains(".openai.azure.com")
			|| url.contains("azure.com/openai")
			|| url.contains("models.inference.ai.azure.com");
		let is_openrouter = provider == "openrouter" || url.contains("openrouter.ai");
		let is_cerebras = provider == "cerebras" || url.contains("cerebras.ai");
		let is_together = provider == "together" || url.contains("api.together.xyz");
		let is_copilot = provider == "github-copilot";
		let is_zenmux = provider == "zenmux";
		let is_zai = provider == "zai" || url.contains("api.z.ai");
		let is_zhipu = provider == "zhipu-coding-plan" || url.contains("open.bigmodel.cn");
		let is_kilo = provider == "kilo" || url.contains("api.kilo.ai");
		let is_alibaba = provider == "alibaba-coding-plan"
			|| provider == "alibaba-token-plan"
			|| url.contains("dashscope")
			|| url.contains("token-plan.");
		let is_xai = provider == "xai" || provider == "xai-oauth" || url.contains("api.x.ai");
		let is_mistral = provider == "mistral" || url.contains("mistral.ai");
		let is_chutes = url.contains("chutes.ai");
		let is_fireworks = url.contains("fireworks.ai");
		let is_google_aistudio = url.contains("generativelanguage.googleapis.com");
		let is_xiaomi = provider == "xiaomi"
			|| provider.starts_with("xiaomi-token-plan-")
			|| url.contains("xiaomimimo.com");
		let is_moonshot_native = provider == "moonshot"
			|| provider == "kimi-code"
			|| url.contains("api.moonshot.ai")
			|| url.contains("api.kimi.com");
		let is_opencode = provider == "opencode-go"
			|| provider == "opencode-zen"
			|| url.contains("opencode.ai");
		let is_deepseek_family = provider == "deepseek" || url.contains("deepseek.com");
		let is_deepseek_direct = provider == "deepseek" || url.contains("api.deepseek.com");
		let is_nvidia = provider == "nvidia" || url.contains("integrate.api.nvidia.com");
		let is_venice = provider == "venice" || url.contains("api.venice.ai");
		let is_qwen = model.contains("qwen");
		let is_mimo = model.contains("mimo");
		let is_moonshot_kimi = is_moonshot_native && is_kimi_model_id(&model);
		let is_kimi_k3 = is_kimi_k3_id(&model);
		let is_local = matches!(provider, "llama.cpp" | "lm-studio" | "vllm" | "ollama")
			|| has_local_http_host(&url);

		// TS `isNonStandard`: hosts that reject OpenAI-only fields such as `store`.
		let is_non_standard = is_cerebras
			|| is_xai
			|| is_mistral
			|| is_google_aistudio
			|| is_chutes
			|| is_deepseek_family
			|| is_fireworks
			|| is_alibaba
			|| is_zai
			|| is_zhipu
			|| is_kilo
			|| is_qwen
			|| is_xiaomi
			|| is_moonshot_native
			|| is_opencode;

		let supports_developer_role = is_openai || is_azure;
		let supports_store = !is_non_standard;
		let supports_reasoning_effort = !is_xai
			&& !(is_xiaomi && is_mimo)
			&& (!(is_zai || is_zhipu) || is_glm52_reasoning_effort_id(&model));
		let use_max_tokens = is_mistral
			|| is_moonshot_native
			|| is_zai
			|| is_zhipu
			|| is_chutes
			|| is_fireworks
			|| is_deepseek_direct;
		let max_tokens_field = if use_max_tokens {
			MaxTokensField::MaxTokens
		} else {
			MaxTokensField::MaxCompletionTokens
		};

		// TS `thinkingFormat` ladder (chat-completions dialect).
		let thinking_format = if (is_moonshot_kimi && !is_kimi_k3)
			|| is_zai
			|| is_zhipu
			|| (is_xiaomi && is_mimo)
		{
			ThinkingFormat::Zai
		} else if is_openrouter {
			ThinkingFormat::OpenRouter
		} else if is_qwen && (is_nvidia || provider == "vllm") {
			ThinkingFormat::QwenChatTemplate
		} else if is_qwen && (is_fireworks || is_venice) {
			ThinkingFormat::OpenAi
		} else if is_alibaba || is_qwen {
			ThinkingFormat::Qwen
		} else {
			ThinkingFormat::OpenAi
		};

		// TS `mergeModelReasoningEffortMap`: detected Kimi K3 map first, then
		// the catalog's sparse map wins key by key.
		let mut reasoning_effort_map = BTreeMap::new();
		if is_kimi_k3 {
			for (from, to) in [("minimal", "low"), ("medium", "high"), ("xhigh", "max"), ("max", "max")] {
				reasoning_effort_map.insert(from.to_string(), to.to_string());
			}
		}

		// TS stream-watchdog floors: slow reasoning/cold-load hosts idle for
		// minutes before the first SSE event.
		let glm_coding_plan = is_glm_coding_plan_id(&model) && (is_zai || is_zhipu || is_opencode);
		let long_coding_plan_idle = glm_coding_plan || provider == "alibaba-coding-plan";
		let stream_idle_timeout_ms = if long_coding_plan_idle {
			Some(600_000)
		} else if (is_xiaomi && is_mimo)
			|| is_local
			|| (reasoning
				&& (is_kimi_k26_id(&model)
					|| (is_moonshot_kimi && is_kimi_k3)
					|| (is_moonshot_kimi && is_kimi_k27_code_id(&model))
					|| is_deepseek_direct))
		{
			Some(300_000)
		} else {
			None
		};

		let supports_strict_mode = is_openai
			|| is_azure
			|| is_openrouter
			|| is_cerebras
			|| is_together
			|| is_copilot
			|| is_zenmux
			|| is_deepseek_family;

		let mut compat = Self {
			supports_developer_role,
			supports_store,
			supports_reasoning_effort,
			max_tokens_field,
			thinking_format,
			reasoning_effort_map,
			stream_idle_timeout_ms,
			supports_strict_mode,
		};

		let Some(sparse) = sparse.filter(|value| value.is_object()) else {
			return compat;
		};
		if let Some(value) = sparse.get("supportsDeveloperRole").and_then(serde_json::Value::as_bool) {
			compat.supports_developer_role = value;
		}
		if let Some(value) = sparse.get("supportsStore").and_then(serde_json::Value::as_bool) {
			compat.supports_store = value;
		}
		if let Some(value) = sparse
			.get("supportsReasoningEffort")
			.and_then(serde_json::Value::as_bool)
		{
			compat.supports_reasoning_effort = value;
		}
		if let Some(value) = sparse
			.get("maxTokensField")
			.and_then(serde_json::Value::as_str)
			.and_then(MaxTokensField::parse)
		{
			compat.max_tokens_field = value;
		}
		if let Some(value) = sparse
			.get("thinkingFormat")
			.and_then(serde_json::Value::as_str)
			.and_then(ThinkingFormat::parse)
		{
			compat.thinking_format = value;
		}
		if let Some(map) = sparse
			.get("reasoningEffortMap")
			.and_then(serde_json::Value::as_object)
		{
			for (from, to) in map {
				if let Some(to) = to.as_str() {
					compat.reasoning_effort_map.insert(from.clone(), to.to_string());
				}
			}
		}
		if let Some(value) = sparse
			.get("streamIdleTimeoutMs")
			.and_then(serde_json::Value::as_u64)
		{
			compat.stream_idle_timeout_ms = Some(value);
		}
		if let Some(value) = sparse.get("supportsStrictMode").and_then(serde_json::Value::as_bool) {
			compat.supports_strict_mode = value;
		}
		compat
	}
}

/// Kimi-family ids in namespace or bare form (`moonshotai/kimi-*`,
/// `kimi-k2.6`, `kimi.k3`). Input is already lowercase.
fn is_kimi_model_id(model: &str) -> bool {
	model.contains("moonshotai/kimi")
		|| model.starts_with("kimi-")
		|| model.starts_with("kimi.")
		|| model.contains("/kimi-")
		|| model.contains("/kimi.")
}

/// Suffix after a segment-anchored `needle` (`needle` at the start or right
/// after `/`), matching TS `(^|\/)needle`.
fn segment_suffix<'a>(model: &'a str, needle: &str) -> Option<&'a str> {
	model
		.match_indices(needle)
		.find(|(index, _)| {
			let index = *index;
			index == 0 || model.as_bytes()[index - 1] == b'/'
		})
		.map(|(index, _)| &model[index + needle.len()..])
}

/// Separator-or-end boundary used by the Kimi/GLM id patterns.
fn is_separator_boundary(rest: &str) -> bool {
	rest.is_empty()
		|| rest.starts_with('-')
		|| rest.starts_with('.')
		|| rest.starts_with(':')
		|| rest.starts_with('_')
}

/// TS `isKimiK3ModelId`: `kimi-k3`, `kimi-k3.1`, `kimi-k3-turbo`.
fn is_kimi_k3_id(model: &str) -> bool {
	let Some(rest) = segment_suffix(model, "kimi-k3") else {
		return false;
	};
	let rest = match rest.strip_prefix('.') {
		Some(after) => {
			let digits = after.bytes().take_while(u8::is_ascii_digit).count();
			if digits == 0 {
				return false;
			}
			&after[digits..]
		},
		None => rest,
	};
	is_separator_boundary(rest)
}

/// TS `isKimiK26ModelId`: `kimi-k2.6`, router form `kimi-k2p6`.
fn is_kimi_k26_id(model: &str) -> bool {
	let Some(rest) = segment_suffix(model, "kimi-k2") else {
		return false;
	};
	let rest = if let Some(after) = rest.strip_prefix(".6") {
		after
	} else if let Some(after) = rest.strip_prefix("p6") {
		after
	} else {
		return false;
	};
	rest.is_empty() || rest.starts_with('-') || rest.starts_with(':')
}

/// TS `KIMI_K27_CODE_MODEL_PATTERN`: `kimi-k2.7-code`, `kimi-k27-code`,
/// `kimi-k2p7-code`, with the optional `-highspeed` suffix.
fn is_kimi_k27_code_id(model: &str) -> bool {
	let Some(rest) = segment_suffix(model, "kimi-k2") else {
		return false;
	};
	let rest = rest.strip_prefix(['.', '-', '_']).unwrap_or(rest);
	let rest = rest.strip_prefix('p').unwrap_or(rest);
	let Some(rest) = rest.strip_prefix('7') else {
		return false;
	};
	let rest = rest.strip_prefix(['-', '.', '_']).unwrap_or(rest);
	let Some(rest) = rest.strip_prefix("code") else {
		return false;
	};
	let rest = rest.strip_prefix(['-', '.', '_']).unwrap_or(rest);
	rest.is_empty() || rest == "highspeed"
}

/// TS `GLM_CODING_PLAN_MODEL_PATTERN`: `glm-5`, `glm-5.2`, `glm-5-turbo`.
fn is_glm_coding_plan_id(model: &str) -> bool {
	let Some(rest) = segment_suffix(model, "glm-5") else {
		return false;
	};
	rest.is_empty() || rest.starts_with('.') || rest.starts_with('-')
}

/// TS `isGlm52ReasoningEffortModelId`, without the shared GLM semver parser:
/// GLM-5.2+ base/air/turbo (and 5.3+ flash) SKUs accept `reasoning_effort`.
fn is_glm52_reasoning_effort_id(model: &str) -> bool {
	let Some(position) = model.find("glm-") else {
		return false;
	};
	let rest = &model[position + 4..];
	let version_len = rest
		.bytes()
		.take_while(|byte| byte.is_ascii_digit() || *byte == b'.')
		.count();
	if version_len == 0 {
		return false;
	}
	let version = &rest[..version_len];
	let mut parts = version.split('.');
	let Some(major) = parts.next().and_then(|part| part.parse::<u32>().ok()) else {
		return false;
	};
	let minor = parts.next().and_then(|part| part.parse::<u32>().ok()).unwrap_or(0);
	if (major, minor) < (5, 2) {
		return false;
	}
	let suffix = &rest[version_len..];
	if suffix.starts_with('v') {
		return false;
	}
	let variant = suffix.strip_prefix('-').unwrap_or(suffix);
	let variant = variant.split(['-', '.', '_']).next().unwrap_or(variant);
	if matches!(variant, "flashx" | "preview") {
		return false;
	}
	!(variant == "flash" && (major, minor) < (5, 3))
}

/// TS `hasLocalLoopbackBaseUrl` for the stream-timeout floor: loopback,
/// RFC1918, and `.local` hosts. `url` is already lowercase.
fn has_local_http_host(url: &str) -> bool {
	let authority = url.split_once("://").map_or(url, |(_, rest)| rest);
	let authority = authority.split(['/', '?', '#']).next().unwrap_or_default();
	let authority = authority
		.rsplit_once('@')
		.map_or(authority, |(_, host)| host);
	let host = if let Some(rest) = authority.strip_prefix('[') {
		rest.split_once(']').map_or(rest, |(host, _)| host)
	} else {
		authority.split(':').next().unwrap_or(authority)
	};
	if matches!(host, "localhost" | "127.0.0.1" | "0.0.0.0" | "::1") {
		return true;
	}
	if host.starts_with("10.") || host.starts_with("192.168.") || host.ends_with(".local") {
		return true;
	}
	if let Some(rest) = host.strip_prefix("172.") {
		if let Some(second) = rest.split('.').next().and_then(|octet| octet.parse::<u8>().ok()) {
			return (16..=31).contains(&second);
		}
	}
	false
}

#[derive(Deserialize)]
struct ModelEntry {
	#[serde(default)]
	id: Option<String>,
	#[serde(default)]
	provider: Option<String>,
	#[serde(default, rename = "contextWindow")]
	context_window: Option<u64>,
	#[serde(default)]
	input: Option<Vec<String>>,
	#[serde(default)]
	name: Option<String>,
	#[serde(default, rename = "maxTokens")]
	max_tokens: Option<u64>,
	#[serde(default, rename = "omitMaxOutputTokens")]
	omit_max_output_tokens: Option<bool>,
	#[serde(default)]
	reasoning: bool,
	#[serde(default)]
	thinking: Option<serde_json::Value>,
	#[serde(default)]
	cost: Option<serde_json::Value>,
	#[serde(default)]
	api: Option<String>,
	#[serde(default, rename = "baseUrl")]
	base_url: Option<String>,
	#[serde(default)]
	compat: Option<serde_json::Value>,
	#[serde(default)]
	headers: Option<BTreeMap<String, String>>,
	#[serde(default, rename = "requestModelId")]
	request_model_id: Option<String>,
	#[serde(default)]
	tokenizer: Option<String>,
}

/// One authored `models.yml` model plus the authored-ness a same-id patch
/// needs: TS `applyModelPatch` leaves the bundled row's fields alone when the
/// definition omits them, while `false`/`0` are authored values.
struct YamlModel {
	info:      ModelInfo,
	/// `None` when the definition has no `reasoning` key.
	reasoning: Option<bool>,
}

impl YamlModel {
	/// Model inserted when the provider/id has no bundled row yet.
	fn finish(self) -> ModelInfo {
		let mut info = self.info;
		info.cost = normalize_cost(info.cost);
		info
	}

	/// TS `applyModelPatch(..., "replace")` for a same-id `models.yml`
	/// definition: authored fields win, omitted ones keep the bundled row's
	/// value, `api`/`baseUrl` come from the definition (the provider fallback
	/// is the consumer's connection lookup), and the `compat` block is the
	/// definition's own provider+model merge — bundled transport metadata is
	/// deliberately not re-merged (model-patch transport policy `replace`).
	fn patch(self, base: ModelInfo) -> ModelInfo {
		let YamlModel { info, reasoning } = self;
		let compat = info.compat;
		let thinking_declared = info.thinking.is_some();
		let supports_usage_in_streaming =
			compat_bool(compat.as_ref(), "supportsUsageInStreaming");
		ModelInfo {
			provider:        info.provider,
			context_window:  info.context_window.or(base.context_window),
			input:           info.input.or(base.input),
			name:            info.name.or(base.name),
			max_tokens:      info.max_tokens.or(base.max_tokens),
			omit_max_output_tokens: info
				.omit_max_output_tokens
				.or(base.omit_max_output_tokens),
			reasoning:       reasoning.unwrap_or(base.reasoning),
			efforts:         if thinking_declared { info.efforts } else { base.efforts },
			cost:            patch_cost(base.cost.as_ref(), info.cost.as_ref()),
			api:             info.api,
			base_url:        info.base_url,
			compat,
			thinking:        info.thinking.or(base.thinking),
			// Same-id patch: an authored request field replaces the row's value,
			// an omitted one keeps it. No cross-field merge is invented.
			headers:         info.headers.or(base.headers),
			request_model_id: info.request_model_id.or(base.request_model_id),
			tokenizer:       info.tokenizer.or(base.tokenizer),
			supports_usage_in_streaming,
		}
	}
}

/// TS `applyModelPatch` cost merge: authored rates win per rate, the bundled
/// row keeps the rates the definition omits. The result stays normalized.
fn patch_cost(
	base: Option<&serde_json::Value>,
	patch: Option<&serde_json::Value>,
) -> Option<serde_json::Value> {
	let Some(patch) = patch.and_then(serde_json::Value::as_object) else {
		return base.cloned();
	};
	let mut merged = base
		.and_then(serde_json::Value::as_object)
		.cloned()
		.unwrap_or_default();
	for (key, value) in patch {
		merged.insert(key.clone(), value.clone());
	}
	normalize_cost(Some(serde_json::Value::Object(merged)))
}

/// Boolean key from a sparse `compat` block; non-object or non-bool → `None`.
fn compat_bool(compat: Option<&serde_json::Value>, key: &str) -> Option<bool> {
	compat?.get(key)?.as_bool()
}

/// TS `mergeCompat`: objects merge key by key (recursively for nested
/// records), the override wins everywhere else. A non-object block keeps only
/// the override, matching the TS spread of a non-record base.
fn merge_compat(
	base: Option<&serde_json::Value>,
	over: Option<&serde_json::Value>,
) -> Option<serde_json::Value> {
	// TS `mergeCompat` treats a falsy side as absent: a bare `compat:` key in
	// `models.yml` parses as null and must neither erase nor surface as a block.
	let base = base.filter(|value| !value.is_null());
	let over = over.filter(|value| !value.is_null());
	match (base, over) {
		(None, None) => None,
		(None, over) => over.cloned(),
		(base, None) => base.cloned(),
		(Some(base), Some(over)) => Some(merge_compat_value(base, over)),
	}
}

fn merge_compat_value(base: &serde_json::Value, over: &serde_json::Value) -> serde_json::Value {
	let (serde_json::Value::Object(base), serde_json::Value::Object(over)) = (base, over) else {
		return over.clone();
	};
	let mut merged = base.clone();
	for (key, value) in over {
		let next = match merged.get(key) {
			Some(existing) if existing.is_object() && value.is_object() => {
				merge_compat_value(existing, value)
			},
			_ => value.clone(),
		};
		merged.insert(key.clone(), next);
	}
	serde_json::Value::Object(merged)
}

/// Bundled `models.json` rows are materialized, so a `models.yml` provider
/// block acts as a user override, exactly like TS `#loadBuiltInModels`:
/// `baseUrl` replaces the row endpoint (`#applyProviderTransportOverride`,
/// `override.baseUrl ?? entry.baseUrl`) and `compat` keys win over the row's
/// own block (`mergeCompat(model.compatConfig, providerOverride.compat)`).
/// The provider `api` never replaces a row's dialect, and this runs before
/// the `models.yml` model merge so an authored model's own `baseUrl` wins.
fn apply_provider_overrides(
	providers: &mut BTreeMap<String, BTreeMap<String, ModelInfo>>,
	connections: &BTreeMap<String, ProviderConfig>,
	provider_compat: &BTreeMap<String, serde_json::Value>,
) {
	for (provider_id, models) in providers {
		let base_url = connections
			.get(provider_id)
			.and_then(|connection| connection.base_url.as_deref());
		let over = provider_compat.get(provider_id);
		if base_url.is_none() && over.is_none() {
			continue;
		}
		for info in models.values_mut() {
			if let Some(base_url) = base_url {
				info.base_url = Some(base_url.to_string());
			}
			if let Some(over) = over {
				info.compat = merge_compat(info.compat.as_ref(), Some(over));
				info.supports_usage_in_streaming =
					compat_bool(info.compat.as_ref(), "supportsUsageInStreaming");
			}
		}
	}
}

/// `thinking.efforts` from a catalog `thinking` block; `true`-only or absent
/// thinking metadata yields `None` (no controllable effort surface).
/// String map of an authored per-model `headers` block (`models.yml`).
///
/// Non-string values are dropped: the TS wire type is `Record<string, string>`
/// and a header can never be a credential carrier here.
fn yml_headers(value: Option<serde_json::Value>) -> Option<BTreeMap<String, String>> {
	let map = value?.as_object()?.clone();
	let headers: BTreeMap<String, String> = map
		.into_iter()
		.filter_map(|(name, value)| value.as_str().map(|value| (name, value.to_string())))
		.collect();
	(!headers.is_empty()).then_some(headers)
}

fn thinking_efforts(thinking: Option<serde_json::Value>) -> Option<Vec<String>> {
	let thinking = thinking?;
	let efforts = thinking.get("efforts")?.as_array()?;
	Some(efforts.iter().filter_map(serde_json::Value::as_str).map(str::to_string).collect())
}

impl Catalog {
	pub fn load(cwd: &Path) -> Result<Self> {
		let (json_paths, yml_path) = Self::paths(cwd);
		Self::load_from(&json_paths, &yml_path)
	}

	/// Listing loader for `arag models`: a malformed `models.yml` disables custom
	/// providers but must not erase a valid `models.json` (upstream
	/// `models-cli` `configError`). The warning is returned for the caller to
	/// print; [`Self::load`] stays strict so a bad catalog never runs.
	pub fn load_listing(cwd: &Path) -> Result<(Self, Option<String>)> {
		let (json_paths, yml_path) = Self::paths(cwd);
		Self::load_listing_at(&json_paths, &yml_path)
	}

	/// Testable listing loader with explicit candidate paths.
	pub fn load_listing_at(json_paths: &[PathBuf], yml_path: &Path) -> Result<(Self, Option<String>)> {
		match Self::load_from(json_paths, yml_path) {
			Ok(catalog) => Ok((catalog, None)),
			Err(err) if yml_path.is_file() => {
				// Only a valid `models.json` can stand in for the broken
				// catalog; without one the strict error is the truth.
				let Some(json) = json_paths.iter().find(|path| path.is_file()) else {
					return Err(err);
				};
				Ok((Self::from_json(json)?, Some(err.to_string())))
			},
			Err(err) => Err(err),
		}
	}

	fn paths(cwd: &Path) -> (Vec<PathBuf>, PathBuf) {
		let agent = dirs::agent_dir();
		let yml_path = ["models.yml", "models.yaml"]
			.iter()
			.map(|name| agent.join(name))
			.find(|path| path.is_file())
			.unwrap_or_else(|| agent.join("models.yml"));
		(json_candidates(cwd), yml_path)
	}

	/// Testable loader: first existing `models.json` wins for model lists, while
	/// `models.yml` still contributes provider connection blocks; without
	/// `models.json`, `models.yml` providers are the model source.
	pub fn load_from(json_paths: &[PathBuf], yml_path: &Path) -> Result<Self> {
		let mut connections = BTreeMap::new();
		let mut provider_compat = BTreeMap::new();
		let mut yml_providers = None;
		if yml_path.is_file() {
			let (providers, parsed, compat) = yml_sections(yml_path)?;
			connections = parsed;
			provider_compat = compat;
			yml_providers = Some((providers, yml_path.to_path_buf()));
		}
		if let Some(path) = json_paths.iter().find(|path| path.is_file()) {
			let mut catalog = Self::from_json(path)?;
			catalog.connections = connections;
			apply_provider_overrides(&mut catalog.providers, &catalog.connections, &provider_compat);
			if let Some((providers, _)) = yml_providers {
				// TS `#mergeCustomModels` + `applyModelPatch`: a `models.yml`
				// definition for the same `provider + id` patches the row (its
				// authored fields win, omitted ones keep the row's value) and
				// every other definition is appended.
				for (provider_id, models) in providers {
					let entries = catalog.providers.entry(provider_id).or_default();
					for (id, model) in models {
						if let Some(base) = entries.remove(&id) {
							entries.insert(id, model.patch(base));
						} else {
							entries.insert(id, model.finish());
						}
					}
				}
			}
			return Ok(catalog);
		}
		if let Some((providers, path)) = yml_providers
			&& !providers.is_empty()
		{
			let providers: BTreeMap<String, BTreeMap<String, ModelInfo>> = providers
				.into_iter()
				.map(|(provider_id, models)| {
					let models: BTreeMap<String, ModelInfo> = models
						.into_iter()
						.map(|(id, model)| (id, model.finish()))
						.collect();
					(provider_id, models)
				})
				.collect();
			return Ok(Self { providers, connections, source: path });
		}
		Err(Error::new(
			"no models: expected models.json in the workspace, ~/.omp/agent, or share/omp \
			 (or providers in ~/.omp/agent/models.yml)",
		))
	}

	fn from_json(path: &Path) -> Result<Self> {
		let file = fs::File::open(path)
			.map_err(|err| Error::with_source(format!("cannot read {}", path.display()), err))?;
		let parsed: BTreeMap<String, BTreeMap<String, ModelEntry>> =
			serde_json::from_reader(BufReader::new(file))
				.map_err(|err| Error::new(format!("invalid models.json {}: {err}", path.display())))?;
		let mut providers = BTreeMap::new();
		for (provider_id, models) in parsed {
			let mut entries = BTreeMap::new();
			for (key, entry) in models {
				let id = entry.id.unwrap_or_else(|| key.clone());
				let provider = entry.provider.unwrap_or_else(|| provider_id.clone());
				let context_window = entry
					.context_window
					.and_then(|window| usize::try_from(window).ok())
					.filter(|window| *window > 0);
				let thinking = entry.thinking;
				let supports_usage_in_streaming =
					compat_bool(entry.compat.as_ref(), "supportsUsageInStreaming");
				entries.insert(
					id,
					ModelInfo {
						provider,
						context_window,
						input: entry.input,
						name: entry.name,
						max_tokens: entry.max_tokens,
						omit_max_output_tokens: entry.omit_max_output_tokens,
						reasoning: entry.reasoning,
						efforts: thinking_efforts(thinking.clone()),
						cost: normalize_cost(entry.cost),
						api: entry.api,
						base_url: entry.base_url,
						compat: entry.compat,
						thinking,
						headers: entry.headers,
						request_model_id: entry.request_model_id,
						tokenizer: entry.tokenizer,
						supports_usage_in_streaming,
					},
				);
			}
			providers.insert(provider_id, entries);
		}
		if providers.is_empty() {
			return Err(Error::new(format!("no models in {}", path.display())));
		}
		Ok(Self { providers, connections: BTreeMap::new(), source: path.to_path_buf() })
	}
}

/// `models.json` `cost` as the four-rate block `--json` always emits: missing
/// rates default to `0` (`buildModel`), extra keys survive. A non-object cost
/// is unknown, not a rate block.
fn normalize_cost(cost: Option<serde_json::Value>) -> Option<serde_json::Value> {
	let Some(serde_json::Value::Object(mut rates)) = cost else {
		return None;
	};
	for key in ["input", "output", "cacheRead", "cacheWrite"] {
		rates.entry(key).or_insert_with(|| serde_json::json!(0));
	}
	Some(serde_json::Value::Object(rates))
}

/// Parse `models.yml`: provider model lists plus connection blocks.
fn yml_sections(
	path: &Path,
) -> Result<(
	BTreeMap<String, BTreeMap<String, YamlModel>>,
	BTreeMap<String, ProviderConfig>,
	BTreeMap<String, serde_json::Value>,
)> {
	let text = fs::read_to_string(path)
		.map_err(|err| Error::with_source(format!("cannot read {}", path.display()), err))?;
	let value =
		yaml::parse(&text).map_err(|err| Error::new(format!("{}: {err}", path.display())))?;
	let mut providers = BTreeMap::new();
	let mut connections = BTreeMap::new();
	let mut provider_compat_by_id = BTreeMap::new();
	if let Some(section) = value.get("providers") {
		let Some(pairs) = section.as_map() else {
			return Err(Error::new(format!("{}: `providers` must be a mapping", path.display())));
		};
		for (id, provider) in pairs {
			connections.insert(id.clone(), config::parse_provider(provider, id, path)?);
			// Provider-level `compat` is a default for every model below it;
			// a model-level key wins (TS `mergeCompat(providerCompat,
			// modelDef.compat)`), and never masks a per-model `api`/`baseUrl`.
			let provider_compat = provider
				.get("compat")
				.map(yaml_json)
				.filter(|value| !value.is_null());
			if let Some(compat) = &provider_compat {
				provider_compat_by_id.insert(id.clone(), compat.clone());
			}
			let mut models = BTreeMap::new();
			if let Some(list) = provider.get("models") {
				let Some(items) = list.as_list() else {
					return Err(Error::new(format!(
						"{}: providers.{id}.models must be a sequence",
						path.display()
					)));
				};
				for item in items {
					if let Some(model_id) = item.get("id").and_then(yaml::Value::as_str) {
						let context_window = item
							.get("contextWindow")
							.and_then(yaml::Value::to_text)
							.and_then(|window| window.parse::<usize>().ok())
							.filter(|window| *window > 0);
						let input = item
							.get("input")
							.and_then(yaml::Value::as_list)
							.map(|list| {
								list.iter()
									.filter_map(yaml::Value::to_text)
									.collect()
							});
						let thinking = item.get("thinking").map(yaml_json);
						let model_compat = item
							.get("compat")
							.map(yaml_json)
							.filter(|value| !value.is_null());
						let compat = merge_compat(provider_compat.as_ref(), model_compat.as_ref());
						let reasoning =
							item.get("reasoning").and_then(yaml::Value::as_bool);
						models.insert(model_id.to_string(), YamlModel {
							info: ModelInfo {
								provider: id.clone(),
								context_window,
								input,
								name: item.get("name").and_then(yaml::Value::to_text),
								max_tokens: item
									.get("maxTokens")
									.and_then(yaml::Value::to_text)
									.and_then(|cap| cap.parse::<u64>().ok()),
								reasoning: reasoning.unwrap_or(false),
								efforts: thinking_efforts(thinking.clone()),
								cost: yml_cost(item),
								api: item.get("api").and_then(yaml::Value::to_text),
								base_url: item.get("baseUrl").and_then(yaml::Value::to_text),
								omit_max_output_tokens: item
									.get("omitMaxOutputTokens")
									.and_then(yaml::Value::as_bool),
								supports_usage_in_streaming: compat_bool(
									compat.as_ref(),
									"supportsUsageInStreaming",
								),
								compat,
								thinking,
								headers: yml_headers(item.get("headers").map(yaml_json)),
								request_model_id: item
									.get("requestModelId")
									.and_then(yaml::Value::to_text),
								tokenizer: item.get("tokenizer").and_then(yaml::Value::to_text),
							},
							reasoning,
						});
					}
				}
			}
			providers.insert(id.clone(), models);
		}
	}
	Ok((providers, connections, provider_compat_by_id))
}

/// `models.yml` value as JSON, so authored metadata shares one
/// representation with the bundled `models.json` rows.
fn yaml_json(value: &yaml::Value) -> serde_json::Value {
	match value {
		yaml::Value::Null => serde_json::Value::Null,
		yaml::Value::Bool(value) => serde_json::Value::Bool(*value),
		yaml::Value::Int(value) => serde_json::Value::from(*value),
		yaml::Value::Float(value) => serde_json::Value::from(*value),
		yaml::Value::Str(value) => serde_json::Value::String(value.clone()),
		yaml::Value::List(items) => serde_json::Value::Array(items.iter().map(yaml_json).collect()),
		yaml::Value::Map(pairs) => serde_json::Value::Object(
			pairs
				.iter()
				.map(|(key, value)| (key.clone(), yaml_json(value)))
				.collect(),
		),
	}
}

/// Numeric YAML scalar as `f64`.
fn yaml_rate(value: &yaml::Value) -> Option<f64> {
	match value {
		yaml::Value::Int(int) => Some(*int as f64),
		yaml::Value::Float(float) => Some(*float),
		_ => value.to_text().and_then(|text| text.parse().ok()),
	}
}

/// Integral rates stay JSON integers (`0`, not `0.0`), like `models.json`.
fn json_rate(value: f64) -> serde_json::Value {
	if value.fract() == 0.0 && value.abs() < 9_007_199_254_740_992.0 {
		serde_json::json!(value as i64)
	} else {
		serde_json::json!(value)
	}
}

/// `models.yml` `cost` block with only the authored rates, so a same-id patch
/// leaves the bundled rates it omits alone (TS `applyModelPatch`).
/// [`normalize_cost`] fills zeros for standalone models.
fn yml_cost(item: &yaml::Value) -> Option<serde_json::Value> {
	let cost = item.get("cost")?;
	let pairs = cost.as_map()?;
	let mut rates = serde_json::Map::new();
	for (key, value) in pairs {
		if matches!(key.as_str(), "input" | "output" | "cacheRead" | "cacheWrite")
			&& let Some(rate) = yaml_rate(value)
		{
			rates.insert(key.clone(), json_rate(rate));
		}
	}
	Some(serde_json::Value::Object(rates))
}

impl Catalog {
	/// Provider that declares `model`, either `provider/model` or a bare id.
	#[must_use]
	pub fn provider_of(&self, model: &str) -> Option<&str> {
		if let Some((provider, id)) = model.split_once('/')
			&& self
				.providers
				.get(provider)
				.is_some_and(|models| models.contains_key(id))
		{
			return self
				.providers
				.get_key_value(provider)
				.map(|(key, _)| key.as_str());
		}
		self
			.providers
			.iter()
			.find(|(_, models)| models.contains_key(model))
			.map(|(provider, _)| provider.as_str())
	}

	/// Declared context window for a model, when the catalog knows it.
	#[must_use]
	pub fn context_window(&self, provider: &str, model: &str) -> Option<usize> {
		self.providers.get(provider)?.get(model)?.context_window
	}

	/// Declared input modalities for a model, if known in catalog.
	#[must_use]
	pub fn model_input(&self, provider: &str, model: &str) -> Option<&[String]> {
		self.providers.get(provider)?.get(model)?.input.as_deref()
	}

	/// Image support status from catalog:
	/// - `Some(true)`: model explicitly declares `"image"` input modality.
	/// - `Some(false)`: model explicitly declares inputs, but `"image"` is absent (fail-closed explicit text-only).
	/// - `None`: capabilities unknown (model unlisted or omitted `input` field in catalog).
	#[must_use]
	pub fn supports_images(&self, provider: &str, model: &str) -> Option<bool> {
		let input = self.model_input(provider, model)?;
		Some(input.iter().any(|m| m == "image"))
	}

	/// Per-model `compat.supportsUsageInStreaming` override, when the catalog
	/// declares one. `None` leaves the provider default in charge.
	#[must_use]
	pub fn supports_usage_in_streaming(&self, provider: &str, model: &str) -> Option<bool> {
		self
			.providers
			.get(provider)?
			.get(model)?
			.supports_usage_in_streaming
	}

	/// Declared per-model wire dialect (`api`), without the connection
	/// fallback: a provider-level `api` never masks a model that names one.
	#[must_use]
	pub fn model_api(&self, provider: &str, model: &str) -> Option<&str> {
		self.providers.get(provider)?.get(model)?.api.as_deref()
	}

	/// Per-model `baseUrl`: bundled rows already carry the `models.yml`
	/// provider override, `models.yml` models keep their own, and `None`
	/// falls back to the connection's `baseUrl`.
	#[must_use]
	pub fn model_base_url(&self, provider: &str, model: &str) -> Option<&str> {
		self
			.providers
			.get(provider)?
			.get(model)?
			.base_url
			.as_deref()
	}

	/// Resolved OpenAI-compatible wire compat for this model (TS
	/// `buildOpenAICompat` defaults plus the model's merged sparse `compat`
	/// block). `None` when the provider/model row is absent.
	#[must_use]
	pub fn openai_compat(&self, provider: &str, model: &str) -> Option<OpenAiCompat> {
		let info = self.providers.get(provider)?.get(model)?;
		let base_url = info
			.base_url
			.as_deref()
			.or_else(|| self.connections.get(provider)?.base_url.as_deref());
		Some(OpenAiCompat::resolve(
			provider,
			base_url,
			model,
			info.reasoning,
			info.compat.as_ref(),
		))
	}

	/// Sparse `compat` block resolved for this model, provider default and
	/// model keys already merged (precedence in [`ModelInfo::compat`]).
	#[must_use]
	pub fn model_compat(&self, provider: &str, model: &str) -> Option<&serde_json::Value> {
		self.providers.get(provider)?.get(model)?.compat.as_ref()
	}

	/// Authored `thinking` block, verbatim.
	#[must_use]
	pub fn model_thinking(&self, provider: &str, model: &str) -> Option<&serde_json::Value> {
		self
			.providers
			.get(provider)?
			.get(model)?
			.thinking
			.as_ref()
	}

	/// TS `Model.omitMaxOutputTokens` for this model: `Some(true)` means the
	/// runtime must omit the wire output cap (Ollama `num_predict`), while
	/// `Some(false)` is an authored re-enable and `None` means the catalog
	/// never declared it, so the builder keeps its default.
	#[must_use]
	pub fn omit_max_output_tokens(&self, provider: &str, model: &str) -> Option<bool> {
		self
			.providers
			.get(provider)?
			.get(model)?
			.omit_max_output_tokens
	}

	pub fn model_count(&self) -> usize {
		self.providers.values().map(BTreeMap::len).sum()
	}
}

fn json_candidates(cwd: &Path) -> Vec<PathBuf> {
	let mut paths = vec![cwd.join("models.json"), dirs::agent_dir().join("models.json")];
	paths.extend(
		dirs::share_dirs()
			.into_iter()
			.map(|dir| dir.join("models.json")),
	);
	paths
}

#[cfg(test)]
mod tests {
	use super::*;
	use std::env;
	use std::io::Write as _;
	use std::sync::atomic::{AtomicU32, Ordering};

	static COUNTER: AtomicU32 = AtomicU32::new(0);

	fn temp_dir() -> PathBuf {
		let unique = format!(
			"aragent-catalog-{}-{}",
			std::process::id(),
			COUNTER.fetch_add(1, Ordering::Relaxed)
		);
		let dir = env::temp_dir().join(unique);
		fs::create_dir_all(&dir).expect("temp dir");
		dir
	}

	fn write(path: &Path, text: &str) {
		let mut file = fs::File::create(path).expect("create");
		file.write_all(text.as_bytes()).expect("write");
	}

	#[test]
	fn parses_json_and_prefers_workspace_file() {
		let dir = temp_dir();
		let workspace = dir.join("models.json");
		let user = dir.join("user-models.json");
		write(&user, r#"{"ignored":{"m1":{"id":"m1","provider":"ignored"}}}"#);
		write(
			&workspace,
			r#"{"openai":{"gpt-5":{"id":"gpt-5","provider":"openai","cost":{"input":1}}},"local":{"qwen":{"provider":"local"}}}"#,
		);
		let catalog =
			Catalog::load_from(&[workspace.clone(), user], &dir.join("models.yml")).expect("catalog");
		assert_eq!(catalog.source, workspace);
		assert_eq!(catalog.provider_of("gpt-5"), Some("openai"));
		assert_eq!(catalog.provider_of("openai/gpt-5"), Some("openai"));
		assert_eq!(catalog.provider_of("qwen"), Some("local"));
		assert_eq!(catalog.model_count(), 2);
		fs::remove_dir_all(dir).ok();
	}

	#[test]
	fn falls_back_to_models_yml_providers() {
		let dir = temp_dir();
		let yml = dir.join("models.yml");
		write(
			&yml,
			"providers:\n  ollama:\n    baseUrl: http://localhost:11434\n    models:\n      - id: llama3\n",
		);
		let catalog = Catalog::load_from(&[dir.join("missing.json")], &yml).expect("catalog");
		assert_eq!(catalog.provider_of("llama3"), Some("ollama"));
		let connection = catalog.connections.get("ollama").expect("connection block");
		assert_eq!(connection.base_url.as_deref(), Some("http://localhost:11434"));
		fs::remove_dir_all(dir).ok();
	}

	#[test]
	fn yml_connections_load_even_when_json_provides_models() {
		let dir = temp_dir();
		let json = dir.join("models.json");
		write(&json, r#"{"local":{"m":{"id":"m","provider":"local"}}}"#);
		let yml = dir.join("models.yml");
		write(
			&yml,
			"providers:\n  local:\n    baseUrl: http://127.0.0.1:20128/v1\n    api: openai-completions\n    auth: none\n",
		);
		let catalog = Catalog::load_from(&[json.clone()], &yml).expect("catalog");
		assert_eq!(catalog.source, json);
		let connection = catalog.connections.get("local").expect("connection");
		assert_eq!(connection.base_url.as_deref(), Some("http://127.0.0.1:20128/v1"));
		assert_eq!(connection.api.as_deref(), Some("openai-completions"));
		fs::remove_dir_all(dir).ok();
	}

	#[test]
	fn missing_catalog_reports_no_models() {
		let dir = temp_dir();
		let err = Catalog::load_from(&[dir.join("missing.json")], &dir.join("missing.yml"))
			.expect_err("must fail");
		assert!(err.to_string().starts_with("no models"));
		fs::remove_dir_all(dir).ok();
	}

	#[test]
	fn parses_json_display_fields_for_models_listing() {
		let dir = temp_dir();
		let json = dir.join("models.json");
		write(
			&json,
			r#"{"openai":{"gpt-5":{"id":"gpt-5","name":"GPT-5","reasoning":true,"maxTokens":393216,"contextWindow":1048576,"input":["text","image"],"thinking":{"mode":"effort","efforts":["low","high"]},"cost":{"input":1.5,"output":2,"cacheRead":0,"cacheWrite":0}}}}"#,
		);
		let catalog = Catalog::load_from(&[json], &dir.join("missing.yml")).expect("catalog");
		let info = &catalog.providers["openai"]["gpt-5"];
		assert_eq!(info.name.as_deref(), Some("GPT-5"));
		assert_eq!(info.max_tokens, Some(393_216));
		assert!(info.reasoning);
		assert_eq!(info.efforts.as_deref(), Some(&["low".to_string(), "high".to_string()][..]));
		assert_eq!(info.input.as_deref(), Some(&["text".to_string(), "image".to_string()][..]));
		let cost = info.cost.as_ref().expect("cost block");
		assert_eq!(cost.get("input").and_then(serde_json::Value::as_f64), Some(1.5));
		fs::remove_dir_all(dir).ok();
	}

	#[test]
	fn parses_models_yml_display_fields_for_models_listing() {
		let dir = temp_dir();
		let yml = dir.join("models.yml");
		write(
			&yml,
			"providers:\n  local:\n    baseUrl: http://127.0.0.1:1/v1\n    models:\n      - id: vision\n        name: Vision\n        reasoning: true\n        maxTokens: 8192\n        contextWindow: 128000\n        input:\n          - text\n          - image\n        thinking:\n          efforts:\n            - low\n            - high\n        cost:\n          input: 1\n          output: 2.5\n          cacheRead: 0\n          cacheWrite: 0\n",
		);
		let catalog = Catalog::load_from(&[dir.join("missing.json")], &yml).expect("catalog");
		let info = &catalog.providers["local"]["vision"];
		assert_eq!(info.name.as_deref(), Some("Vision"));
		assert_eq!(info.max_tokens, Some(8_192));
		assert!(info.reasoning);
		assert_eq!(info.efforts.as_deref(), Some(&["low".to_string(), "high".to_string()][..]));
		let cost = info.cost.as_ref().expect("cost block");
		assert_eq!(cost.get("input"), Some(&serde_json::json!(1)));
		assert_eq!(cost.get("output"), Some(&serde_json::json!(2.5)));
		fs::remove_dir_all(dir).ok();
	}

	#[test]
	fn json_cost_normalizes_missing_rates_and_keeps_extras() {
		let dir = temp_dir();
		let json = dir.join("models.json");
		write(
			&json,
			r#"{"openai":{"gpt-5":{"id":"gpt-5","cost":{"input":1.5,"longContext":{"input":3}}}}}"#,
		);
		let catalog = Catalog::load_from(&[json], &dir.join("missing.yml")).expect("catalog");
		let cost = catalog.providers["openai"]["gpt-5"].cost.as_ref().expect("cost");
		assert_eq!(cost["input"], serde_json::json!(1.5));
		assert_eq!(cost["output"], serde_json::json!(0));
		assert_eq!(cost["cacheRead"], serde_json::json!(0));
		assert_eq!(cost["cacheWrite"], serde_json::json!(0));
		assert_eq!(cost["longContext"], serde_json::json!({ "input": 3 }));
		fs::remove_dir_all(dir).ok();
	}

	#[test]
	fn parses_per_model_usage_in_streaming_compat() {
		let dir = temp_dir();
		let json = dir.join("models.json");
		write(
			&json,
			r#"{"cerebras":{"llama":{"id":"llama","compat":{"supportsUsageInStreaming":true}},"other":{"id":"other"}}}"#,
		);
		let catalog = Catalog::load_from(&[json], &dir.join("missing.yml")).expect("catalog");
		assert_eq!(catalog.supports_usage_in_streaming("cerebras", "llama"), Some(true));
		assert_eq!(catalog.supports_usage_in_streaming("cerebras", "other"), None);
		assert_eq!(catalog.supports_usage_in_streaming("absent", "llama"), None);

		let yml = dir.join("models.yml");
		write(
			&yml,
			"providers:\n  venice:\n    baseUrl: http://127.0.0.1:1/v1\n    compat:\n      supportsUsageInStreaming: false\n    models:\n      - id: v1\n      - id: v2\n        compat:\n          supportsUsageInStreaming: true\n",
		);
		let catalog = Catalog::load_from(&[dir.join("missing.json")], &yml).expect("catalog");
		assert_eq!(
			catalog.supports_usage_in_streaming("venice", "v1"),
			Some(false),
			"provider-level compat is the default for its models"
		);
		assert_eq!(
			catalog.supports_usage_in_streaming("venice", "v2"),
			Some(true),
			"model-level compat overrides the provider default"
		);
		fs::remove_dir_all(dir).ok();
	}

	#[test]
	fn malformed_yml_warns_and_keeps_valid_json_catalog() {
		let dir = temp_dir();
		let json = dir.join("models.json");
		write(&json, r#"{"openai":{"gpt-5":{"id":"gpt-5","provider":"openai"}}}"#);
		let yml = dir.join("models.yml");
		write(&yml, "providers: broken\n");
		let (catalog, warning) =
			Catalog::load_listing_at(&[json.clone()], &yml).expect("valid json survives");
		assert_eq!(catalog.provider_of("gpt-5"), Some("openai"));
		assert_eq!(catalog.source, json);
		assert!(warning.expect("warning").contains("`providers` must be a mapping"));

		// Without a stand-in catalog the strict error stands, and the runtime
		// loader never accepts the malformed file.
		let missing = dir.join("missing.json");
		let err = Catalog::load_listing_at(&[missing.clone()], &yml).expect_err("no fallback");
		assert!(err.to_string().contains("`providers` must be a mapping"));
		assert!(Catalog::load_from(&[json, missing], &yml).is_err());
		fs::remove_dir_all(dir).ok();
	}

	#[test]
	fn openai_compat_defaults_cover_cerebras_kimi_glm_deepseek_and_local() {
		let openai = OpenAiCompat::resolve("openai", None, "gpt-5", true, None);
		assert!(openai.supports_developer_role);
		assert!(openai.supports_store);
		assert_eq!(openai.max_tokens_field, MaxTokensField::MaxCompletionTokens);
		assert_eq!(openai.thinking_format, ThinkingFormat::OpenAi);
		assert!(openai.supports_strict_mode);

		let cerebras = OpenAiCompat::resolve("cerebras", None, "llama-3.3-70b", false, None);
		assert!(!cerebras.supports_developer_role);
		assert!(!cerebras.supports_store);
		assert!(cerebras.supports_reasoning_effort);
		assert_eq!(cerebras.max_tokens_field, MaxTokensField::MaxCompletionTokens);
		assert!(cerebras.supports_strict_mode);

		let kimi = OpenAiCompat::resolve("moonshot", None, "kimi-k2.6", true, None);
		assert!(!kimi.supports_developer_role);
		assert!(!kimi.supports_store);
		assert_eq!(kimi.max_tokens_field, MaxTokensField::MaxTokens);
		assert_eq!(kimi.thinking_format, ThinkingFormat::Zai);
		assert_eq!(kimi.stream_idle_timeout_ms, Some(300_000));

		let glm = OpenAiCompat::resolve("zai", None, "glm-5.2", true, None);
		assert!(!glm.supports_developer_role);
		assert!(!glm.supports_store);
		assert!(glm.supports_reasoning_effort);
		assert_eq!(glm.max_tokens_field, MaxTokensField::MaxTokens);
		assert_eq!(glm.thinking_format, ThinkingFormat::Zai);
		assert_eq!(glm.stream_idle_timeout_ms, Some(600_000));

		let deepseek = OpenAiCompat::resolve(
			"deepseek",
			Some("https://api.deepseek.com/v1"),
			"deepseek-reasoner",
			true,
			None,
		);
		assert!(!deepseek.supports_developer_role);
		assert!(!deepseek.supports_store);
		assert_eq!(deepseek.max_tokens_field, MaxTokensField::MaxTokens);
		assert_eq!(deepseek.stream_idle_timeout_ms, Some(300_000));
		assert!(deepseek.supports_strict_mode);

		let local = OpenAiCompat::resolve(
			"llama.cpp",
			Some("http://127.0.0.1:8080/v1"),
			"llama3",
			true,
			None,
		);
		assert!(!local.supports_developer_role);
		assert!(local.supports_store);
		assert_eq!(local.max_tokens_field, MaxTokensField::MaxCompletionTokens);
		assert_eq!(local.stream_idle_timeout_ms, Some(300_000));
		assert!(!local.supports_strict_mode);
	}

	#[test]
	fn openai_compat_applies_sparse_catalog_overrides() {
		let dir = temp_dir();
		let yml = dir.join("models.yml");
		write(
			&yml,
			"providers:\n  local:\n    baseUrl: http://127.0.0.1:9999/v1\n    models:\n      - id: m1\n        reasoning: true\n        compat:\n          supportsDeveloperRole: true\n          supportsStore: false\n          supportsReasoningEffort: false\n          maxTokensField: max_tokens\n          thinkingFormat: qwen\n          reasoningEffortMap:\n            high: medium\n          streamIdleTimeoutMs: 1234\n          supportsStrictMode: true\n",
		);
		let catalog = Catalog::load_from(&[dir.join("missing.json")], &yml).expect("catalog");
		let compat = catalog.openai_compat("local", "m1").expect("compat");
		assert!(compat.supports_developer_role);
		assert!(!compat.supports_store);
		assert!(!compat.supports_reasoning_effort);
		assert_eq!(compat.max_tokens_field, MaxTokensField::MaxTokens);
		assert_eq!(compat.thinking_format, ThinkingFormat::Qwen);
		assert_eq!(
			compat.reasoning_effort_map.get("high").map(String::as_str),
			Some("medium")
		);
		assert_eq!(compat.stream_idle_timeout_ms, Some(1_234));
		assert!(compat.supports_strict_mode);
		fs::remove_dir_all(dir).ok();
	}

	#[test]
	fn openai_compat_kimi_k3_remaps_efforts_and_catalog_map_wins() {
		let sparse = serde_json::json!({"reasoningEffortMap": {"medium": "low"}});
		let compat = OpenAiCompat::resolve(
			"openrouter",
			None,
			"moonshotai/kimi-k3",
			true,
			Some(&sparse),
		);
		assert_eq!(compat.thinking_format, ThinkingFormat::OpenRouter);
		assert_eq!(
			compat.reasoning_effort_map.get("minimal").map(String::as_str),
			Some("low")
		);
		assert_eq!(
			compat.reasoning_effort_map.get("medium").map(String::as_str),
			Some("low"),
			"catalog override replaces the detected K3 remap"
		);
		assert_eq!(
			compat.reasoning_effort_map.get("xhigh").map(String::as_str),
			Some("max")
		);
	}
}
