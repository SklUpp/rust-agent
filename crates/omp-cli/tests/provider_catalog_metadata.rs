//! Per-model catalog metadata consumed by real runtime dispatch: the wire
//! dialect (`api`), endpoint (`baseUrl`), sparse `compat` block and authored
//! `thinking` config must survive `models.json` + `models.yml` loading with
//! the TS precedence:
//!
//! * a `models.yml` model for an existing `provider + id` patches the bundled
//!   row (TS `applyModelPatch`): authored fields win, omitted non-transport
//!   ones keep the row's value, `api`/`baseUrl` come from the definition with
//!   the provider connection as fallback, and the patch's own provider+model
//!   `compat` replaces the row block (transport policy `replace`, so bundled
//!   row-only keys are dropped); definitions without a row are appended;
//! * a `models.yml` provider block is a user override for remaining bundled
//!   rows: its `baseUrl` replaces the row endpoint and its `compat` keys win
//!   (TS `#loadBuiltInModels` / `#applyProviderTransportOverride`);
//! * a bare `compat:` (null) is absent and never erases an existing block.

use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU32, Ordering};

use omp_cli::catalog::Catalog;

static COUNTER: AtomicU32 = AtomicU32::new(0);

fn temp_dir() -> PathBuf {
	let dir = std::env::temp_dir().join(format!(
		"omp-catalog-metadata-{}-{}",
		std::process::id(),
		COUNTER.fetch_add(1, Ordering::Relaxed)
	));
	fs::create_dir_all(&dir).expect("temp dir");
	dir
}

fn write(path: &Path, text: &str) {
	fs::write(path, text).expect("write catalog fixture");
}

/// Bundled rows plus a `models.yml` provider block and model list: a same-id
/// definition patches the row without losing the fields it omits, remaining
/// rows take the provider endpoint, and new definitions are appended.
#[test]
fn json_rows_and_yml_models_resolve_ts_precedence() {
	let dir = temp_dir();
	let json = dir.join("models.json");
	write(
		&json,
		r#"{
			"ollama": {
				"qwen3": {
					"id": "qwen3",
					"name": "Qwen3 bundled",
					"api": "ollama-chat",
					"baseUrl": "http://127.0.0.1:11434",
					"maxTokens": 8192,
					"omitMaxOutputTokens": true,
					"contextWindow": 131072,
					"input": ["text", "image"],
					"reasoning": true,
					"thinking": {"mode": "effort", "efforts": ["low", "high"], "maxTokens": 4096},
					"compat": {"keepAlive": true, "rowOnly": 7, "supportsUsageInStreaming": true, "nested": {"a": 1, "b": 2}},
					"cost": {"input": 1, "output": 2}
				},
				"llama3": {
					"id": "llama3",
					"name": "Llama 3",
					"api": "anthropic-messages",
					"baseUrl": "https://proxy.example/v1",
					"maxTokens": 4096,
					"omitMaxOutputTokens": true,
					"contextWindow": 200000,
					"input": ["text"],
					"compat": {"keepAlive": true, "rowOnly": 7},
					"cost": {"input": 3}
				},
				"phi": {
					"id": "phi",
					"name": "Phi bundled",
					"api": "openai-completions",
					"baseUrl": "https://phi.example/v1",
					"maxTokens": 1000,
					"contextWindow": 4096,
					"reasoning": true,
					"cost": {"input": 2, "output": 4, "cacheRead": 5}
				}
			}
		}"#,
	);
	let yml = dir.join("models.yml");
	write(
		&yml,
		"providers:\n  ollama:\n    baseUrl: http://gateway.example:8080\n    api: openai-completions\n    compat:\n      keepAlive: false\n      supportsUsageInStreaming: false\n      nested:\n        b: 9\n        c: 3\n    models:\n      - id: qwen3\n        api: anthropic-messages\n        baseUrl: http://127.0.0.1:9000\n        maxTokens: 2048\n        omitMaxOutputTokens: false\n        thinking:\n          mode: budget\n          efforts:\n            - low\n            - medium\n          maxTokens: 1024\n        compat:\n          keepAlive: true\n          nested:\n            y: 2\n      - id: phi\n        reasoning: false\n        cost:\n          cacheRead: 0\n      - id: mistral\n        api: ollama-chat\n",
	);
	let catalog = Catalog::load_from(&[json], &yml).expect("catalog");

	// `qwen3` patched by the same-id definition: authored dialect, endpoint,
	// output cap and thinking win, while the bundled name/context/input/
	// reasoning/cost survive. The transport policy is `replace`, so the row's
	// own compat block (`rowOnly`, nested `a`) is not re-merged.
	assert_eq!(catalog.model_api("ollama", "qwen3"), Some("anthropic-messages"));
	assert_eq!(
		catalog.model_base_url("ollama", "qwen3"),
		Some("http://127.0.0.1:9000")
	);
	assert_eq!(
		catalog.model_thinking("ollama", "qwen3"),
		Some(&serde_json::json!({"mode": "budget", "efforts": ["low", "medium"], "maxTokens": 1024}))
	);
	assert_eq!(
		catalog.model_compat("ollama", "qwen3"),
		Some(&serde_json::json!({
			"keepAlive": true,
			"supportsUsageInStreaming": false,
			"nested": {"b": 9, "c": 3, "y": 2},
		}))
	);
	assert_eq!(catalog.supports_usage_in_streaming("ollama", "qwen3"), Some(false));
	let qwen3 = &catalog.providers["ollama"]["qwen3"];
	assert_eq!(qwen3.name.as_deref(), Some("Qwen3 bundled"));
	assert_eq!(qwen3.max_tokens, Some(2048));
	assert_eq!(catalog.context_window("ollama", "qwen3"), Some(131072));
	assert!(qwen3.reasoning, "omitted reasoning keeps the bundled flag");
	assert_eq!(
		catalog.model_input("ollama", "qwen3"),
		Some(&["text".to_string(), "image".to_string()][..])
	);
	assert_eq!(
		qwen3.efforts.as_deref(),
		Some(&["low".to_string(), "medium".to_string()][..])
	);
	let qwen3_cost = qwen3.cost.as_ref().expect("bundled cost");
	assert_eq!(qwen3_cost["input"], serde_json::json!(1));
	assert_eq!(qwen3_cost["output"], serde_json::json!(2));
	// The bundled row sets the native omission flag; the authored `false` must
	// override it while every other patched field above stays intact.
	assert_eq!(catalog.omit_max_output_tokens("ollama", "qwen3"), Some(false));

	// `phi` authored `reasoning: false` and an explicit zero rate: both beat
	// the bundled values, while omitted name/cost rates keep the row's.
	assert!(!catalog.providers["ollama"]["phi"].reasoning);
	assert_eq!(catalog.providers["ollama"]["phi"].name.as_deref(), Some("Phi bundled"));
	assert_eq!(catalog.providers["ollama"]["phi"].max_tokens, Some(1000));
	let phi_cost = catalog.providers["ollama"]["phi"].cost.as_ref().expect("cost");
	assert_eq!(phi_cost["input"], serde_json::json!(2));
	assert_eq!(phi_cost["output"], serde_json::json!(4));
	assert_eq!(phi_cost["cacheRead"], serde_json::json!(0));
	// No authored dialect/endpoint: consumers fall back to the connection.
	assert_eq!(catalog.model_api("ollama", "phi"), None);
	assert_eq!(catalog.model_base_url("ollama", "phi"), None);
	// Never declared: distinct from an authored `false`.
	assert_eq!(catalog.omit_max_output_tokens("ollama", "phi"), None);

	// `llama3`: no `models.yml` definition, so the provider block overrides the
	// bundled endpoint and merges into the row compat without touching `api`.
	assert_eq!(catalog.model_api("ollama", "llama3"), Some("anthropic-messages"));
	assert_eq!(
		catalog.model_base_url("ollama", "llama3"),
		Some("http://gateway.example:8080")
	);
	assert_eq!(
		catalog.model_compat("ollama", "llama3"),
		Some(&serde_json::json!({
			"keepAlive": false,
			"rowOnly": 7,
			"supportsUsageInStreaming": false,
			"nested": {"b": 9, "c": 3},
		}))
	);
	assert_eq!(catalog.supports_usage_in_streaming("ollama", "llama3"), Some(false));
	assert_eq!(catalog.supports_usage_in_streaming("ollama", "qwen3"), Some(false));
	assert_eq!(catalog.model_thinking("ollama", "llama3"), None);
	// No same-id definition touches this row, so the bundled flag survives.
	assert_eq!(catalog.omit_max_output_tokens("ollama", "llama3"), Some(true));

	// `mistral`: a `models.yml`-only definition is appended, not dropped.
	assert_eq!(catalog.provider_of("mistral"), Some("ollama"));
	assert_eq!(catalog.model_api("ollama", "mistral"), Some("ollama-chat"));
	assert_eq!(catalog.model_base_url("ollama", "mistral"), None);
	assert_eq!(catalog.omit_max_output_tokens("ollama", "mistral"), None);
	assert_eq!(
		catalog.model_compat("ollama", "mistral"),
		Some(&serde_json::json!({
			"keepAlive": false,
			"supportsUsageInStreaming": false,
			"nested": {"b": 9, "c": 3},
		}))
	);

	// The connection block itself keeps the provider values for consumers.
	let connection = catalog.connections.get("ollama").expect("connection");
	assert_eq!(connection.api.as_deref(), Some("openai-completions"));
	assert_eq!(connection.base_url.as_deref(), Some("http://gateway.example:8080"));

	// Existing listing behavior on the untouched row is unchanged.
	let info = &catalog.providers["ollama"]["llama3"];
	assert_eq!(info.name.as_deref(), Some("Llama 3"));
	assert_eq!(info.max_tokens, Some(4096));
	assert_eq!(catalog.context_window("ollama", "llama3"), Some(200000));
	assert_eq!(
		catalog.model_input("ollama", "llama3"),
		Some(&["text".to_string()][..])
	);
	assert_eq!(catalog.supports_images("ollama", "llama3"), Some(false));
	let cost = info.cost.as_ref().expect("cost");
	assert_eq!(cost["input"], serde_json::json!(3));
	assert_eq!(cost["cacheRead"], serde_json::json!(0));
	fs::remove_dir_all(dir).ok();
}

/// `models.yml`-authored models use the TS custom-provider precedence: the
/// provider block is the base, the model's own keys win, and a bare `compat:`
/// is absent on either side.
#[test]
fn yml_models_keep_model_metadata_above_provider_defaults() {
	let dir = temp_dir();
	let yml = dir.join("models.yml");
	write(
		&yml,
		"providers:\n  local:\n    baseUrl: http://127.0.0.1:8080\n    api: openai-completions\n    compat:\n      supportsUsageInStreaming: false\n      disableStrictTools: false\n      nested:\n        x: 1\n    models:\n      - id: alpha\n        api: anthropic-messages\n        baseUrl: http://127.0.0.1:9000\n        thinking:\n          mode: budget\n          efforts:\n            - low\n            - medium\n          maxTokens: 2048\n        compat:\n          supportsUsageInStreaming: true\n          nested:\n            y: 2\n      - id: beta\n      - id: gamma\n        compat:\n  sparse:\n    baseUrl: http://127.0.0.1:7000\n    api: openai-completions\n    compat:\n    models:\n      - id: delta\n",
	);
	let catalog = Catalog::load_from(&[dir.join("missing.json")], &yml).expect("catalog");

	assert_eq!(catalog.model_api("local", "alpha"), Some("anthropic-messages"));
	assert_eq!(
		catalog.model_base_url("local", "alpha"),
		Some("http://127.0.0.1:9000")
	);
	// No declared per-model value: consumers fall back to the connection.
	assert_eq!(catalog.model_api("local", "beta"), None);
	assert_eq!(catalog.model_base_url("local", "beta"), None);
	assert_eq!(
		catalog.connections["local"].api.as_deref(),
		Some("openai-completions")
	);

	let alpha = catalog.model_compat("local", "alpha").expect("alpha compat");
	assert_eq!(alpha["supportsUsageInStreaming"], serde_json::json!(true));
	assert_eq!(alpha["disableStrictTools"], serde_json::json!(false));
	assert_eq!(alpha["nested"], serde_json::json!({"x": 1, "y": 2}));
	let beta = catalog.model_compat("local", "beta").expect("beta compat");
	assert_eq!(beta["supportsUsageInStreaming"], serde_json::json!(false));
	assert_eq!(beta["nested"], serde_json::json!({"x": 1}));
	assert_eq!(catalog.supports_usage_in_streaming("local", "alpha"), Some(true));
	assert_eq!(catalog.supports_usage_in_streaming("local", "beta"), Some(false));

	// A bare model `compat:` inherits the provider block instead of erasing it.
	assert_eq!(catalog.model_compat("local", "gamma"), Some(beta));

	// A bare provider `compat:` is absent: nothing is surfaced or erased.
	assert_eq!(catalog.model_compat("sparse", "delta"), None);
	assert_eq!(catalog.supports_usage_in_streaming("sparse", "delta"), None);

	assert_eq!(
		catalog.model_thinking("local", "alpha"),
		Some(&serde_json::json!({"mode": "budget", "efforts": ["low", "medium"], "maxTokens": 2048}))
	);
	assert_eq!(
		catalog.providers["local"]["alpha"].efforts.as_deref(),
		Some(&["low".to_string(), "medium".to_string()][..])
	);
	fs::remove_dir_all(dir).ok();
}

/// Per-model request metadata the Ollama payload context needs: `headers`,
/// `requestModelId` and `tokenizer` survive loading with the same patch
/// precedence as the other authored fields. A provider block is a connection
/// override and never becomes a model-level header.
#[test]
fn json_rows_and_yml_models_carry_request_metadata() {
	let dir = temp_dir();
	let json = dir.join("models.json");
	write(&json, r#"{
		"ollama": {
			"qwen3": {
				"id": "qwen3",
				"name": "Qwen3 bundled",
				"api": "ollama-chat",
				"baseUrl": "http://127.0.0.1:11434",
				"headers": {"User-Agent": "row-agent"},
				"requestModelId": "qwen3-row",
				"tokenizer": "qwen3"
			},
			"llama3": {
				"id": "llama3",
				"name": "Llama bundled",
				"api": "ollama-chat",
				"baseUrl": "http://127.0.0.1:11434",
				"headers": {"User-Agent": "row-agent"}
			}
		}
	}"#);
	let yml = dir.join("models.yml");
	write(&yml, r#"
providers:
  ollama:
    baseUrl: http://127.0.0.1:11434
    headers:
      X-Connection: provider
    models:
      - id: qwen3
        headers:
          X-Model: patch
        requestModelId: qwen3-patch
        tokenizer: qwen3-v2
"#);
	let catalog = Catalog::load_from(&[json], &yml).expect("load catalog");
	let qwen = &catalog.providers["ollama"]["qwen3"];
	assert_eq!(qwen.name.as_deref(), Some("Qwen3 bundled"), "an omitted field keeps the row value");
	assert_eq!(qwen.request_model_id.as_deref(), Some("qwen3-patch"), "the patch wins");
	assert_eq!(qwen.tokenizer.as_deref(), Some("qwen3-v2"), "the patch wins");
	assert_eq!(
		qwen.headers.as_ref(),
		Some(&BTreeMap::from([("X-Model".to_string(), "patch".to_string())])),
		"the patch replaces the row headers; connection headers stay out of the model"
	);
	let llama = &catalog.providers["ollama"]["llama3"];
	assert_eq!(
		llama.headers.as_ref(),
		Some(&BTreeMap::from([("User-Agent".to_string(), "row-agent".to_string())])),
		"a row the provider block does not patch keeps its own headers"
	);
	assert_eq!(llama.request_model_id, None);
}
