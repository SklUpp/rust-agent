use std::{future::Future, sync::atomic::{AtomicBool, Ordering}, time::Duration};
use serde_json::{json, Value};
use tokio::process::Command;
use crate::mcp::{error::McpError, transports::{McpTransport, root_uri}, types::MCPStdioServerConfig};

pub(crate) struct StdioService { service: tokio::sync::Mutex<Option<rmcp::service::RunningService<rmcp::RoleClient, Handler>>>, closed: AtomicBool }

struct Handler { config: rmcp::model::ClientConfig, root: Result<String, String> }
impl rmcp::ClientHandler for Handler {
    fn get_info(&self) -> rmcp::model::ClientConfig { self.config.clone() }
    fn list_roots(&self, _: rmcp::service::RequestContext<rmcp::RoleClient>) -> impl Future<Output=Result<rmcp::model::ListRootsResult,rmcp::model::ErrorData>> + Send + '_ {
        std::future::ready(self.root.as_ref().map(|uri| rmcp::model::ListRootsResult::new(vec![rmcp::model::Root::new(uri.clone()).with_name("workspace")])).map_err(|e| rmcp::model::ErrorData::internal_error(e.clone(), None)))
    }
}
impl StdioService {
    pub(crate) async fn connect(config: &MCPStdioServerConfig) -> Result<Self,McpError> {
        let timeout_ms = crate::mcp::timeout::resolve_mcp_timeout_ms(config.timeout);
        use rmcp::{ClientLifecycleMode,ClientServiceExt};
        use rmcp::transport::child_process::TokioChildProcess;
        let mut command=Command::new(&config.command); command.args(&config.args);
        if let Some(cwd)=&config.cwd { command.current_dir(cwd); }
        super::stdio::apply_isolated_env(&mut command,&config.env);
        let (transport,_)=TokioChildProcess::builder(command).stderr(std::process::Stdio::null()).spawn().map_err(|e|McpError::Transport(format!("Failed to spawn stdio process '{}': {e}",config.command)))?;
        let capabilities=serde_json::from_value(json!({"roots":{"listChanged":false}})).map_err(McpError::Json)?;
        let client=rmcp::model::InitializeRequestParams::new(capabilities,rmcp::model::Implementation::new("arag","1.0.0")).with_protocol_version(rmcp::model::ProtocolVersion::LATEST_WITH_INITIALIZE);
        let root=match config.cwd.as_deref(){Some(p)=>root_uri(std::path::Path::new(p)),None=>std::env::current_dir().map_err(|e|e.to_string()).and_then(|p|root_uri(&p))};
        let handshake=Handler{config:client,root}.serve_with_lifecycle(transport,ClientLifecycleMode::Initialize);
        let service=if timeout_ms > 0 { tokio::time::timeout(Duration::from_millis(timeout_ms),handshake).await.map_err(|_|McpError::Timeout("MCP initialization timed out; child transport dropped".into()))? } else { handshake.await } .map_err(map_initialize_error)?;
        Ok(Self{service:tokio::sync::Mutex::new(Some(service)), closed: AtomicBool::new(false)})
    }
    pub(crate) async fn initialized_result(&self)->Result<crate::mcp::types::MCPInitializeResult,McpError>{
        let service=self.service.lock().await; let info=service.as_ref().ok_or(McpError::Closed)?.peer().peer_info().ok_or_else(||McpError::Protocol("RMCP initialization omitted peer info".into()))?;
        let implementation=info.server_info.as_ref().ok_or_else(||McpError::Protocol("Server initialize response omitted serverInfo".into()))?;
        let mut caps=serde_json::to_value(&info.capabilities).map_err(McpError::Json)?; if let Value::Object(m)=&mut caps {m.remove("completions");}
        serde_json::from_value(json!({"protocolVersion":info.protocol_version.as_str(),"capabilities":caps,"serverInfo":{"name":implementation.name,"version":implementation.version},"instructions":info.instructions})).map_err(McpError::Json)
    }
}
#[async_trait::async_trait]
impl McpTransport for StdioService {
    async fn request(&self,method:&str,params:Option<Value>,timeout:Option<Duration>)->Result<Value,McpError>{
        let peer={let service=self.service.lock().await; service.as_ref().ok_or(McpError::Closed)?.peer().clone()};
        let run=async { let p=params.unwrap_or_else(||json!({})); match method {
            "tools/list"=>encode(peer.list_tools(parse_optional(p)?).await,false),
            "tools/call"=>encode(peer.call_tool(parse_params(p)?).await,true),
            "resources/list"=>encode(peer.list_resources(parse_optional(p)?).await,false),
            "resources/templates/list"=>encode(peer.list_resource_templates(parse_optional(p)?).await,false),
            "resources/read"=>encode(peer.read_resource(parse_params(p)?).await,false),
            "prompts/list"=>encode(peer.list_prompts(parse_optional(p)?).await,false),
            "prompts/get"=>encode(peer.send_request(rmcp::model::ClientRequest::CustomRequest(rmcp::model::CustomRequest::new("prompts/get",Some(p)))).await,false),
            "ping"=>encode(peer.send_request(rmcp::model::ClientRequest::PingRequest(rmcp::model::PingRequest::default())).await,false),
            _=>Err(McpError::Protocol(format!("Unsupported MCP method: {method}"))),}};
        let result=match timeout {Some(d)=>tokio::time::timeout(d,run).await.map_err(|_|if method=="tools/call"{McpError::UnknownOutcome(format!("{method} timed out; verify server state before retry"))}else{McpError::Timeout(format!("{method} timed out"))})?,None=>run.await};
        if self.closed.load(Ordering::Acquire) {
            match result {
                Ok(value) => Ok(value),
                Err(error) if method == "tools/call" => match error {
                    McpError::UnknownOutcome(_) => Err(error),
                    other => Err(McpError::UnknownOutcome(format!("tools/call interrupted by close; remote outcome unknown: {other}"))),
                },
                Err(_) => Err(McpError::Closed),
            }
        } else { result }
    }
    async fn notify(&self,method:&str,params:Option<Value>)->Result<(),McpError>{
        let peer={let service=self.service.lock().await; service.as_ref().ok_or(McpError::Closed)?.peer().clone()};
        peer.send_notification(rmcp::model::ClientNotification::CustomNotification(rmcp::model::CustomNotification::new(method,params))).await.map_err(|e|McpError::Transport(e.to_string()))
    }
    async fn close(&self)->Result<(),McpError>{self.closed.store(true,Ordering::Release);let service={let mut guard=self.service.lock().await;guard.take()};let Some(mut service)=service else{return Ok(())};match service.close_with_timeout(Duration::from_secs(8)).await{Ok(Some(_))=>Ok(()),Ok(None)=>Err(McpError::Transport("RMCP shutdown timed out; child cleanup incomplete".into())),Err(e)=>Err(McpError::Transport(e.to_string()))}}

}

fn parse_params<T: serde::de::DeserializeOwned>(value: Value) -> Result<T, McpError> {
    serde_json::from_value(value).map_err(|e| McpError::Protocol(format!("Invalid MCP request params: {e}")))
}
fn parse_optional<T: serde::de::DeserializeOwned>(value: Value) -> Result<Option<T>, McpError> {
    if value.as_object().is_some_and(serde_json::Map::is_empty) { Ok(None) } else { parse_params(value).map(Some) }
}
fn encode<T: serde::Serialize>(result: Result<T, rmcp::ServiceError>, mutating: bool) -> Result<Value, McpError> {
    serde_json::to_value(result.map_err(|error| map_service_error(error, mutating))?).map_err(McpError::Json)
}
fn map_service_error(e:rmcp::ServiceError, mutating: bool)->McpError{match e{rmcp::ServiceError::McpError(e)=>McpError::JsonRpc{code:i64::from(e.code.0),message:e.message.into_owned(),data:e.data},rmcp::ServiceError::Timeout{timeout}=>if mutating{McpError::UnknownOutcome(format!("RMCP request timed out after {timeout:?}"))}else{McpError::Timeout(format!("RMCP request timed out after {timeout:?}"))},other=>McpError::Transport(other.to_string())}}
fn map_initialize_error(error: rmcp::service::ClientInitializeError) -> McpError {
    match error {
        rmcp::service::ClientInitializeError::JsonRpcError(error) => McpError::JsonRpc {
            code: i64::from(error.code.0), message: error.message.into_owned(), data: error.data,
        },
        other => McpError::Protocol(format!("MCP initialization failed: {other}")),
    }
}
