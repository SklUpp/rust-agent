pub mod compaction;
pub mod dap;
pub mod delivery;
pub mod durable;
pub mod eval;
pub mod lsp;
pub mod mailbox;
pub mod mcp;
#[path = "loop.rs"]
pub mod loop_module;
pub mod process_broker;
pub mod provider;
pub mod provider_anthropic;
pub mod provider_ollama;
pub mod state;
pub mod subagent_advisor;
pub mod subagent_lifecycle;
pub mod supervisor;
pub mod task_persisted_revive;
pub mod tokenizer;
pub mod tool;
pub mod tools;
pub mod workspace_writer;
pub mod tls;

pub use compaction::{CompactionReport, Compactor};
// Aragent consumes these without a direct `pi-shell` dependency (its
// `Cargo.toml` stays pinned): the shell policy used by the approval gate and
// the recoverable-panic probe used by the crash reporter.
pub use pi_shell::panic_scope_active;
pub use pi_shell::policy as shell_policy;
pub use tls::{ensure_crypto_provider, http_client_builder, shared_rustls_config};
pub use durable::{
	ChildDurability, ChildDurabilityFactory, DURABILITY_ADMISSION_FAILURE_CODE,
	DURABILITY_FAILURE_CODE, DURABILITY_UNKNOWN_OUTCOME_CODE, DeliveredPeerMessage,
	DeliveredWorkerResult, DurableBoundary, DurableError, WorkerCompletionSink, WorkerIdentity,
};
pub use loop_module::{
	AgentError, AgentEvent, AgentLoop, AssistantGuard, InMemoryRewind, SteeringEvent, TurnAck,
	TurnAckDecision, TurnAckRequest, TurnRewrite, TurnToolSelection, INTERRUPTED_TOOL_RESULT,
	repair_tool_batch_boundary, tool_batch_end, image_parts_from_details, tool_image_parts_of,
};
pub use tokio_util::sync::CancellationToken;
pub use provider::{
	ChunkStream, ModelProvider, OpenAiChatRequest, OpenAiContent,
	OpenAiContentPart, OpenAiImageUrl, OpenAiMessage, OpenAiStreamOptions, OpenAiToolChoice,
	OpenAiToolChoiceFunction, OpenAiUsage, ProviderError, StreamAssembler, StreamBlockCursor,
	StreamBlockKind, StreamChunk, ToolCallDelta, ToolChoice, normalize_usage, parse_sse_line,
	sse_stream, to_openai_messages,
};
pub use provider::timeout::{RequestTimeoutConfig, TimeoutProvider, is_timeout_error};
pub use state::{
	AcpClientBridge, AcpClientError, AgentConfig, AgentContext, AgentShell, AgentState,
	Attribution,
	CacheTtlUsage, ContentPart, ImagePart, Message, Role, ServerToolUsage, StopReason, TokenUsage,
	UsagePresence,
	StreamPartialConfig, ToolApprover, ToolCall, ToolResult, TurnResult, TurnStatus,
};
pub use tokenizer::Tokenizer;
pub use delivery::{BackgroundDelivery, BackgroundResultSink};
pub use supervisor::{
	CancelAttempt, ChildGuard, ChildRecord, ChildStatus, ChildToolSettings, DelegationLimits,
	Supervisor,
};
pub use tool::{Tool, ToolError};
pub use mailbox::{
	DEFAULT_IRC_TIMEOUT_MS, IrcMessage, MAIN_AGENT_ID, MAILBOX_CAP, Mailbox, MailboxWaiter,
	format_incoming,
};
pub use tools::{
	AskTool, AstGrepTool, BashTool, CheckpointTool, EditTool, GlobTool, GrepTool, HubTool,
	PendingAction, PendingStore, REJECT_DEVICE_PATH, RESOLVE_DEVICE_PATH, ReadTool, RewindTool,
	TaskTool, ThinkTool, TodoTool, WriteTool, YieldTool, registry::ToolRegistry,
	scheduler::ToolScheduler,
};
