use async_trait::async_trait;
use serde_json::{json, Value};
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::io::AsyncWriteExt;
use tokio::process::{Child, Command};
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;

use crate::mcp::error::McpError;
use crate::mcp::transports::{McpTransport, roots_list_reply};
use crate::mcp::types::{MCPStdioServerConfig, RequestIdFormat};

struct PendingGuard<'a> {
    map: &'a Mutex<HashMap<String, oneshot::Sender<Result<Value, McpError>>>>,
    id_key: &'a str,
    active: bool,
}

/// Fire-and-forget JSON-RPC cancel. try_send never blocks the timeout/abort path.
fn write_cancelled(stdin_tx: &mpsc::Sender<String>, request_id: &Value) {
    let notification = json!({
        "jsonrpc": "2.0",
        "method": "notifications/cancelled",
        "params": { "requestId": request_id }
    });
    let _ = stdin_tx.try_send(format!("{}\n", notification));
}

struct CancelNotifyGuard {
    stdin_tx: mpsc::Sender<String>,
    request_id: Value,
    active: bool,
}

impl Drop for CancelNotifyGuard {
    fn drop(&mut self) {
        if self.active {
            write_cancelled(&self.stdin_tx, &self.request_id);
        }
    }
}

impl<'a> Drop for PendingGuard<'a> {
    fn drop(&mut self) {
        if self.active {
            let mut guard = self.map.lock().unwrap();
            guard.remove(self.id_key);
        }
    }
}

/// Parent variables a stdio server needs to execute at all: command lookup,
/// home, Termux prefix/tmp, terminal, locale, and CA-bundle paths (paths, not
/// secrets). Everything else the host holds (provider keys, tokens, ambient
/// credentials, proxy URLs that may embed credentials) is withheld: those must
/// come from explicit `config.env`, which is applied afterwards and always wins.
const INHERITED_ENV: &[&str] = &[
    "PATH",
    "HOME",
    "PREFIX",
    "TMPDIR",
    "TMP",
    "TEMP",
    "LANG",
    "LC_ALL",
    "LC_CTYPE",
    "LC_MESSAGES",
    "TZ",
    "TERM",
    "SSL_CERT_FILE",
    "NODE_EXTRA_CA_CERTS",
];

fn apply_isolated_env(cmd: &mut Command, overrides: &HashMap<String, String>) {
    cmd.env_clear();
    for key in INHERITED_ENV {
        if let Some(value) = std::env::var_os(key) {
            cmd.env(key, value);
        }
    }
    for (key, value) in overrides {
        cmd.env(key, value);
    }
}

pub struct StdioTransport {
    child: Arc<Mutex<Option<Child>>>,
    stdin_tx: mpsc::Sender<String>,
    pending_requests: Arc<Mutex<HashMap<String, oneshot::Sender<Result<Value, McpError>>>>>,
    next_id: AtomicU64,
    request_id_format: RequestIdFormat,
    read_handle: Mutex<Option<JoinHandle<()>>>,
    write_handle: Mutex<Option<JoinHandle<()>>>,
    closed: Arc<AtomicBool>,
}

impl StdioTransport {
    pub async fn spawn(config: &MCPStdioServerConfig) -> Result<Self, McpError> {
        let mut cmd = Command::new(&config.command);
        cmd.args(&config.args);

        if let Some(cwd) = &config.cwd {
            cmd.current_dir(cwd);
        }

        apply_isolated_env(&mut cmd, &config.env);

        // Advertised root: the configured server cwd (the project root set by
        // the host) when present, never a blind assumption of the process cwd.
        let configured_root = config.cwd.clone().map(std::path::PathBuf::from);

        cmd.stdin(std::process::Stdio::piped());
        cmd.stdout(std::process::Stdio::piped());
        cmd.stderr(std::process::Stdio::null());

        let mut child = cmd.spawn().map_err(|e| {
            McpError::Transport(format!("Failed to spawn stdio process '{}': {}", config.command, e))
        })?;

        let stdout = child.stdout.take().ok_or_else(|| {
            McpError::Transport("Failed to capture stdio stdout".to_string())
        })?;
        let mut stdin = child.stdin.take().ok_or_else(|| {
            McpError::Transport("Failed to capture stdio stdin".to_string())
        })?;

        let (stdin_tx, mut stdin_rx) = mpsc::channel::<String>(128);
        let pending_requests: Arc<Mutex<HashMap<String, oneshot::Sender<Result<Value, McpError>>>>> =
            Arc::new(Mutex::new(HashMap::new()));
        let closed = Arc::new(AtomicBool::new(false));

        let stdin_tx_reply = stdin_tx.clone();
        let write_handle = tokio::spawn(async move {
            while let Some(line) = stdin_rx.recv().await {
                if stdin.write_all(line.as_bytes()).await.is_err() {
                    break;
                }
                if stdin.flush().await.is_err() {
                    break;
                }
            }
        });

        let pending_clone = Arc::clone(&pending_requests);
        let closed_reader = Arc::clone(&closed);
        let read_handle = tokio::spawn(async move {
            const MAX_MCP_FRAME_SIZE: usize = 16 * 1024 * 1024;
            use tokio::io::AsyncReadExt;
            let mut reader = stdout;
            let mut line_bytes = Vec::new();
            let mut chunk = [0u8; 8192];

            'outer: loop {
                let mut hit_newline = false;
                let mut full_line = Vec::new();
                let mut scanned_len = 0;

                while !hit_newline {
                    if let Some(pos) = line_bytes[scanned_len..].iter().position(|&b| b == b'\n') {
                        let pos = scanned_len + pos;
                        full_line = line_bytes[..pos].to_vec();
                        line_bytes.drain(..=pos);
                        hit_newline = true;
                        break;
                    }
                    scanned_len = line_bytes.len();

                    match reader.read(&mut chunk).await {
                        Ok(0) => {
                            if !line_bytes.is_empty() {
                                full_line = std::mem::take(&mut line_bytes);
                                hit_newline = true;
                                break;
                            }
                            break 'outer;
                        }
                        Ok(n) => {
                            if line_bytes.len() + n > MAX_MCP_FRAME_SIZE {
                                // Strictly drop connection before memory can grow
                                break 'outer;
                            }
                            line_bytes.extend_from_slice(&chunk[..n]);
                        }
                        Err(_) => break 'outer,
                    }
                }

                if !hit_newline {
                    break 'outer;
                }

                let line_str = String::from_utf8_lossy(&full_line);
                let trimmed = line_str.trim();
                if trimmed.is_empty() {
                    continue;
                }
                let parsed: Value = match serde_json::from_str(trimmed) {
                    Ok(val) => val,
                    Err(_) => continue,
                };
                let id_val = parsed.get("id");
                let method_val = parsed.get("method").and_then(|v| v.as_str());

                if let Some(method) = method_val {
                    if let Some(id) = id_val {
                        let resp = match method {
                            "ping" => json!({
                                "jsonrpc": "2.0",
                                "id": id,
                                "result": {}
                            }),
                            "roots/list" => roots_list_reply(id, configured_root.as_deref()),
                            _ => json!({
                                "jsonrpc": "2.0",
                                "id": id,
                                "error": {
                                    "code": -32601,
                                    "message": format!("Method not found: {}", method)
                                }
                            }),
                        };
                        let _ = stdin_tx_reply.send(format!("{}\n", resp)).await;
                    }
                    continue;
                }

                if let Some(id) = id_val {
                    let id_key = serde_json::to_string(id).unwrap_or_default();

                    let sender = {
                        let mut map = pending_clone.lock().unwrap();
                        map.remove(&id_key)
                    };

                    if let Some(tx) = sender {
                        if let Some(err) = parsed.get("error") {
                            let code = err.get("code").and_then(|c| c.as_i64()).unwrap_or(-1);
                            let message = err
                                .get("message")
                                .and_then(|m| m.as_str())
                                .unwrap_or("Unknown JSON-RPC error")
                                .to_string();
                            let data = err.get("data").cloned();
                            let _ = tx.send(Err(McpError::JsonRpc { code, message, data }));
                        } else if let Some(result) = parsed.get("result") {
                            let _ = tx.send(Ok(result.clone()));
                        } else {
                            let _ = tx.send(Ok(Value::Null));
                        }
                    }
                }
            }

            closed_reader.store(true, Ordering::Release);
            let mut map = pending_clone.lock().unwrap();
            // These requests were already dispatched when the stream died, so
            // their delivery is unverified. `McpError::Closed` is reserved for
            // requests that never left the client (see `request`/`close`).
            for (_, tx) in map.drain() {
                let _ = tx.send(Err(McpError::Transport(
                    "Connection lost while awaiting response".to_string(),
                )));
            }
        });

        Ok(Self {
            child: Arc::new(Mutex::new(Some(child))),
            stdin_tx,
            pending_requests,
            next_id: AtomicU64::new(1),
            request_id_format: config.request_id_format.unwrap_or(RequestIdFormat::Number),
            read_handle: Mutex::new(Some(read_handle)),
            write_handle: Mutex::new(Some(write_handle)),
            closed,
        })
    }
}

#[async_trait]
impl McpTransport for StdioTransport {
    async fn request(
        &self,
        method: &str,
        params: Option<Value>,
        timeout: Option<Duration>,
    ) -> Result<Value, McpError> {
        if self.closed.load(Ordering::Acquire) {
            return Err(McpError::Closed);
        }
        let id_num = self.next_id.fetch_add(1, Ordering::SeqCst);
        let id_val = match self.request_id_format {
            RequestIdFormat::Number => json!(id_num),
            RequestIdFormat::String => json!(format!("req_{}", id_num)),
        };
        let id_key = serde_json::to_string(&id_val).unwrap_or_else(|_| id_num.to_string());

        let req = json!({
            "jsonrpc": "2.0",
            "id": id_val,
            "method": method,
            "params": params.unwrap_or_else(|| json!({}))
        });

        let (tx, rx) = oneshot::channel();
        {
            let mut map = self.pending_requests.lock().unwrap();
            map.insert(id_key.clone(), tx);
            if self.closed.load(Ordering::Acquire) {
                map.remove(&id_key);
                return Err(McpError::Closed);
            }
        }

        let mut guard = PendingGuard {
            map: &self.pending_requests,
            id_key: &id_key,
            active: true,
        };

        // Declared after PendingGuard so cancel is sent before the pending entry is removed.
        let mut cancel_guard = CancelNotifyGuard {
            stdin_tx: self.stdin_tx.clone(),
            request_id: id_val.clone(),
            active: true,
        };

        let stdin_tx = self.stdin_tx.clone();
        let send_and_recv = async move {
            let line = format!("{}\n", req);
            stdin_tx
                .send(line)
                .await
                .map_err(|e| McpError::Transport(format!("Failed to write to stdin: {}", e)))?;
            rx.await.map_err(|_| McpError::Transport("Connection lost while awaiting response".to_string()))?
        };

        let res = if let Some(dur) = timeout {
            // On timeout: notify server before select drops the oneshot receiver.
            // On abort: CancelNotifyGuard Drop still fires (active left true).
            tokio::select! {
                biased;
                res = send_and_recv => {
                    cancel_guard.active = false;
                    res
                }
                _ = tokio::time::sleep(dur) => {
                    cancel_guard.active = false;
                    write_cancelled(&self.stdin_tx, &id_val);
                    Err(McpError::Timeout(format!("Request '{}' timed out after {:?}", method, dur)))
                }
            }
        } else {
            let res = send_and_recv.await;
            cancel_guard.active = false;
            res
        };

        if res.is_ok() {
            guard.active = false;
        }
        res
    }

    async fn notify(&self, method: &str, params: Option<Value>) -> Result<(), McpError> {
        if self.closed.load(Ordering::Acquire) {
            return Err(McpError::Closed);
        }
        let notification = json!({
            "jsonrpc": "2.0",
            "method": method,
            "params": params.unwrap_or_else(|| json!({}))
        });
        let line = format!("{}\n", notification);
        self.stdin_tx
            .send(line)
            .await
            .map_err(|e| McpError::Transport(format!("Failed to send notification: {}", e)))?;
        Ok(())
    }

    async fn close(&self) -> Result<(), McpError> {
        self.closed.store(true, Ordering::Release);
        if let Some(handle) = self.read_handle.lock().unwrap().take() {
            handle.abort();
        }
        if let Some(handle) = self.write_handle.lock().unwrap().take() {
            handle.abort();
        }

        let child_opt = {
            let mut child_guard = self.child.lock().unwrap();
            child_guard.take()
        };
        if let Some(mut child) = child_opt {
            let _ = child.kill().await;
            // Reap in the background so close() never blocks on a stuck wait.
            tokio::spawn(async move {
                let _ = child.wait().await;
            });
        }
        let mut map = self.pending_requests.lock().unwrap();
        for (_, tx) in map.drain() {
            let _ = tx.send(Err(McpError::Closed));
        }

        Ok(())
    }
}

impl Drop for StdioTransport {
    fn drop(&mut self) {
        self.closed.store(true, Ordering::Release);
        if let Some(handle) = self.read_handle.lock().unwrap().take() {
            handle.abort();
        }
        if let Some(handle) = self.write_handle.lock().unwrap().take() {
            handle.abort();
        }
        let mut child_guard = self.child.lock().unwrap();
        if let Some(mut child) = child_guard.take() {
            let _ = child.start_kill();
            if let Ok(handle) = tokio::runtime::Handle::try_current() {
                handle.spawn(async move {
                    let _ = child.wait().await;
                });
            } else if let Ok(rt) = tokio::runtime::Builder::new_current_thread().enable_all().build() {
                std::thread::spawn(move || {
                    let _ = rt.block_on(child.wait());
                });
            }
        }
        let mut map = self.pending_requests.lock().unwrap();
        for (_, tx) in map.drain() {
            let _ = tx.send(Err(McpError::Closed));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::mcp::types::MCPStdioServerConfig;
    use std::collections::HashMap;

    /// Ambient parent variables must not reach the child; explicit config env
    /// must, and the documented platform baseline must survive. Mutates only
    /// the `Command` under test, never the process environment.
    #[cfg(unix)]
    #[tokio::test]
    async fn spawn_env_is_explicit_not_ambient() {
        let mut overrides = HashMap::new();
        overrides.insert("MCP_TEST_EXPLICIT".to_string(), "from-config".to_string());
        // A proxy may embed credentials, so it is only ever accepted from
        // explicit server config (D27), never from the host environment.
        overrides.insert("HTTPS_PROXY".to_string(), "http://explicit.invalid:8080".to_string());

        let mut cmd = Command::new("env");
        apply_isolated_env(&mut cmd, &overrides);

        let out = cmd.output().await.expect("spawn env");
        assert!(out.status.success(), "env failed: {}", String::from_utf8_lossy(&out.stderr));
        let child: HashMap<String, String> = String::from_utf8_lossy(&out.stdout)
            .lines()
            .filter_map(|line| line.split_once('='))
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect();

        assert_eq!(
            child.get("MCP_TEST_EXPLICIT").map(String::as_str),
            Some("from-config"),
            "explicit config env must be passed"
        );
        assert_eq!(
            child.get("HTTPS_PROXY").map(String::as_str),
            Some("http://explicit.invalid:8080"),
            "explicit server config must be able to set a proxy"
        );

        // Anything outside the baseline (plus explicit overrides) means the
        // parent environment leaked through.
        for key in child.keys() {
            assert!(
                INHERITED_ENV.contains(&key.as_str()) || overrides.contains_key(key.as_str()),
                "ambient parent variable {key} leaked into the MCP child"
            );
        }

        // Baseline values present in the parent are preserved verbatim.
        for key in INHERITED_ENV {
            let parent = std::env::var(key).ok();
            let got = child.get(*key).cloned();
            assert_eq!(got, parent, "baseline variable {key} must match the parent");
        }

        // Other proxy URLs are not inherited; HTTPS_PROXY is only present above
        // because server config set it explicitly.
        for key in [
            "HTTP_PROXY",
            "NO_PROXY",
            "ALL_PROXY",
            "http_proxy",
            "https_proxy",
            "no_proxy",
            "all_proxy",
        ] {
            if std::env::var_os(key).is_some() {
                assert!(
                    !child.contains_key(key),
                    "proxy variable {key} must come from explicit server env, not the host"
                );
            }
        }
    }

    /// `roots/list` is reachable now that the capability is declared. The root is
    /// the configured server cwd, and its URI must be valid (percent-encoded).
    #[cfg(unix)]
    #[tokio::test]
    async fn roots_list_returns_encoded_configured_cwd() {
        let dir = std::env::temp_dir().join(format!("arag mcp roots #{}", std::process::id()));
        std::fs::create_dir_all(&dir).expect("create root dir");
        let script_path = dir.join("roots_server.py");
        std::fs::write(
            &script_path,
            r#"
import sys, json
init_id = None
def send(obj):
    sys.stdout.write(json.dumps(obj) + "\n")
    sys.stdout.flush()
for line in sys.stdin:
    msg = json.loads(line)
    if msg.get("method") == "initialize":
        init_id = msg["id"]
        send({"jsonrpc": "2.0", "id": 9001, "method": "roots/list"})
    elif msg.get("id") == 9001 and "result" in msg:
        uri = msg["result"]["roots"][0]["uri"]
        send({"jsonrpc": "2.0", "id": init_id, "result": {
            "protocolVersion": "2025-11-25",
            "capabilities": {},
            "serverInfo": {"name": "roots", "version": "1"},
            "instructions": uri,
        }})
"#,
        )
        .expect("write roots fixture");

        let config = MCPStdioServerConfig {
            command: "python3".to_string(),
            args: vec![script_path.to_string_lossy().into_owned()],
            env: HashMap::new(),
            cwd: Some(dir.to_string_lossy().into_owned()),
            timeout: Some(10_000),
            request_id_format: Some(RequestIdFormat::Number),
        };
        let transport = StdioTransport::spawn(&config).await.expect("spawn roots fixture");
        let init = transport
            .request(
                "initialize",
                Some(serde_json::json!({
                    "protocolVersion": "2025-11-25",
                    "capabilities": {"roots": {"listChanged": false}},
                    "clientInfo": {"name": "test", "version": "1"},
                })),
                Some(Duration::from_secs(10)),
            )
            .await
            .expect("initialize");
        transport.close().await.expect("close");

        let uri = init["instructions"].as_str().expect("server echoed the root uri");
        reqwest::Url::parse(uri).expect("advertised root must be a valid URI");
        assert!(uri.starts_with("file:///"), "uri={uri}");
        assert!(!uri.contains(' '), "space must be encoded: {uri}");
        assert!(!uri.contains('#'), "fragment delimiter must be encoded: {uri}");
        assert!(uri.contains("arag%20mcp%20roots%20%23"), "configured cwd must be the root: {uri}");

        let _ = std::fs::remove_dir_all(&dir);
    }

    /// Stub: after close(), child pid must leave /proc (reaped, not zombie).
    #[cfg(unix)]
    #[tokio::test]
    async fn close_reaps_child_process() {
        let config = MCPStdioServerConfig {
            command: "sleep".to_string(),
            args: vec!["30".to_string()],
            env: HashMap::new(),
            cwd: None,
            timeout: Some(5_000),
            request_id_format: Some(RequestIdFormat::Number),
        };
        let transport = StdioTransport::spawn(&config)
            .await
            .expect("spawn sleep");
        let pid = {
            let guard = transport.child.lock().unwrap();
            guard.as_ref().and_then(|c| c.id()).expect("child pid")
        };
        transport.close().await.expect("close");

        let status_path = format!("/proc/{pid}/status");
        let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
        loop {
            if !std::path::Path::new(&status_path).exists() {
                return;
            }
            if tokio::time::Instant::now() >= deadline {
                panic!("pid {pid} still in /proc after close — unreaped zombie");
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }
}
