use async_trait::async_trait;
use serde_json::{Value, json};
use std::path::{Path, PathBuf};
use std::time::Duration;

use crate::mcp::error::McpError;

pub mod header_policy;
pub mod http;
pub mod sse;
pub mod stdio;
pub mod rmcp_adapter;

#[async_trait]
pub trait McpTransport: Send + Sync {
    async fn request(
        &self,
        method: &str,
        params: Option<Value>,
        timeout: Option<Duration>,
    ) -> Result<Value, McpError>;

    async fn notify(&self, method: &str, params: Option<Value>) -> Result<(), McpError>;

    async fn close(&self) -> Result<(), McpError>;

    fn set_protocol_version(&self, _version: &str) {}
}

/// Percent-encoded `file://` URI for a root directory.
///
/// Building the URI by hand (`format!("file://{path}")`) leaves spaces and `#`
/// unescaped, which strict servers reject or truncate as a fragment. There is no
/// malformed fallback: an unconvertible path is an error.
pub(crate) fn root_uri(path: &Path) -> Result<String, String> {
    let absolute = if path.is_absolute() {
        path.to_path_buf()
    } else {
        std::env::current_dir()
            .map_err(|e| format!("cannot resolve workspace root {path:?}: {e}"))?
            .join(path)
    };
    // `from_file_path` keeps the root in the spec's example shape
    // (`file:///home/user/project`) and still percent-encodes every unsafe byte.
    reqwest::Url::from_file_path(&absolute)
        .map(|url| url.to_string())
        .map_err(|_| format!("cannot convert workspace root {absolute:?} to a file URI"))
}

/// Workspace root path: the configured server `cwd` when the transport has one,
/// otherwise the process cwd.
fn workspace_root(configured: Option<&Path>) -> Result<PathBuf, String> {
    match configured {
        Some(path) => Ok(path.to_path_buf()),
        None => std::env::current_dir()
            .map_err(|e| format!("cannot determine workspace root: {e}")),
    }
}

/// JSON-RPC reply to a server-initiated `roots/list`. Conversion failures become
/// an explicit `-32603` error rather than a malformed root.
pub(crate) fn roots_list_reply(id: &Value, configured: Option<&Path>) -> Value {
    match workspace_root(configured).and_then(|path| root_uri(&path)) {
        Ok(uri) => json!({
            "jsonrpc": "2.0",
            "id": id,
            "result": {
                "roots": [{ "uri": uri, "name": "workspace" }]
            }
        }),
        Err(message) => json!({
            "jsonrpc": "2.0",
            "id": id,
            "error": {
                "code": -32603,
                "message": message
            }
        }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn root_uri_percent_encodes_absolute_paths() {
        let uri = root_uri(Path::new("/data/My Project#1/тест")).expect("uri");
        assert!(uri.starts_with("file:///"), "uri={uri}");
        assert!(!uri.ends_with('/'), "root URI must not invent a trailing slash: {uri}");
        assert!(!uri.contains(' '), "raw space is not a valid URI: {uri}");
        assert!(uri.contains("My%20Project"), "uri={uri}");
        assert!(uri.contains("%23"), "fragment delimiter must be encoded: {uri}");
    }

    #[test]
    fn root_uri_resolves_relative_paths_against_process_cwd() {
        let uri = root_uri(Path::new("rel dir")).expect("uri");
        assert!(uri.starts_with("file:///"), "uri={uri}");
        assert!(uri.ends_with("rel%20dir"), "uri={uri}");
    }

    #[test]
    fn roots_list_reply_uses_configured_cwd() {
        let reply = roots_list_reply(&json!(7), Some(Path::new("/tmp/w s")));
        assert_eq!(reply["jsonrpc"], json!("2.0"));
        assert_eq!(reply["id"], json!(7));
        assert_eq!(reply["result"]["roots"][0]["name"], json!("workspace"));
        let uri = reply["result"]["roots"][0]["uri"].as_str().expect("uri");
        assert!(uri.ends_with("w%20s"), "uri={uri}");
        assert!(reply.get("error").is_none());
    }
}
