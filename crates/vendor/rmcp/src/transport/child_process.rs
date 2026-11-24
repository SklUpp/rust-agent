use std::process::Stdio;

use futures::future::Future;
use process_wrap::tokio::{ChildWrapper, CommandWrap};
use tokio::process::{ChildStderr, ChildStdin, ChildStdout};

use super::{RxJsonRpcMessage, Transport, TxJsonRpcMessage, async_rw::AsyncRwTransport};
use crate::RoleClient;

const MAX_WAIT_ON_DROP_SECS: u64 = 3;
/// The parts of a child process.
type ChildProcessParts = (
    Box<dyn ChildWrapper>,
    ChildStdout,
    ChildStdin,
    Option<ChildStderr>,
);

/// Extract the stdio handles from a spawned child.
/// Returns `(child, stdout, stdin, stderr)` where `stderr` is `Some` only
/// if the process was spawned with `Stdio::piped()`.
#[inline]
fn child_process(mut child: Box<dyn ChildWrapper>) -> std::io::Result<ChildProcessParts> {
    let child_stdin = match child.inner_mut().stdin().take() {
        Some(stdin) => stdin,
        None => return Err(std::io::Error::other("stdin was already taken")),
    };
    let child_stdout = match child.inner_mut().stdout().take() {
        Some(stdout) => stdout,
        None => return Err(std::io::Error::other("stdout was already taken")),
    };
    let child_stderr = child.inner_mut().stderr().take();
    Ok((child, child_stdout, child_stdin, child_stderr))
}

pub struct TokioChildProcess {
    child: ChildWithCleanup,
    transport: AsyncRwTransport<RoleClient, ChildStdout, ChildStdin>,
    #[cfg(feature = "test-support")]
    kill_rx: Option<tokio::sync::mpsc::Receiver<KillCommand>>,
    #[cfg(feature = "test-support")]
    pending_kill: Option<PendingKill>,
    #[cfg(feature = "test-support")]
    exit_tx: Option<tokio::sync::watch::Sender<Option<std::io::Result<std::process::ExitStatus>>>>,
}

#[cfg(feature = "test-support")]
pub struct KillCommand {
    pub signal: i32,
    pub reply: tokio::sync::oneshot::Sender<std::io::Result<std::process::ExitStatus>>,
}

#[cfg(feature = "test-support")]
struct PendingKill {
    signal: i32,
    signaled: bool,
    reply: tokio::sync::oneshot::Sender<std::io::Result<std::process::ExitStatus>>,
}

#[cfg(feature = "test-support")]
#[derive(Debug)]
pub enum ReapError {
    PendingTimeout,
    Terminal(std::io::Error),
}

#[cfg(feature = "test-support")]
impl std::fmt::Display for ReapError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::PendingTimeout => write!(f, "timed out waiting for child reap (child still pending)"),
            Self::Terminal(e) => write!(f, "terminal reap failure: {e}"),
        }
    }
}

#[cfg(feature = "test-support")]
impl std::error::Error for ReapError {}

#[cfg(feature = "test-support")]
#[derive(Clone)]
pub struct ChildKillController {
    tx: tokio::sync::mpsc::Sender<KillCommand>,
    exit_rx: tokio::sync::watch::Receiver<Option<std::io::Result<std::process::ExitStatus>>>,
}

#[cfg(feature = "test-support")]
impl ChildKillController {
    pub async fn kill(&self, signal: i32) -> std::io::Result<std::process::ExitStatus> {
        let (reply_tx, reply_rx) = tokio::sync::oneshot::channel();
        self.tx
            .send(KillCommand { signal, reply: reply_tx })
            .await
            .map_err(|_| std::io::Error::new(std::io::ErrorKind::BrokenPipe, "child already closed"))?;
        reply_rx
            .await
            .map_err(|_| std::io::Error::new(std::io::ErrorKind::BrokenPipe, "kill confirmation dropped"))?
    }

    pub async fn wait_reaped(&mut self, timeout: std::time::Duration) -> Result<std::process::ExitStatus, ReapError> {
        tokio::time::timeout(timeout, async {
            loop {
                if let Some(res) = (*self.exit_rx.borrow()).as_ref() {
                    return match res {
                        Ok(status) => Ok(*status),
                        Err(e) => Err(ReapError::Terminal(std::io::Error::new(e.kind(), e.to_string()))),
                    };
                }
                self.exit_rx.changed().await.map_err(|_| {
                    ReapError::Terminal(std::io::Error::new(std::io::ErrorKind::BrokenPipe, "exit watcher closed"))
                })?;
            }
        })
        .await
        .map_err(|_| ReapError::PendingTimeout)?
    }
}

#[cfg(not(feature = "test-support"))]
pub struct ChildWithCleanup {
    inner: Option<Box<dyn ChildWrapper>>,
}

#[cfg(not(feature = "test-support"))]
impl Drop for ChildWithCleanup {
    fn drop(&mut self) {
        // We should not use start_kill(), instead we should use kill() to avoid zombies
        if let Some(mut inner) = self.inner.take() {
            // We don't care about the result, just try to kill it
            tokio::spawn(async move {
                if let Err(e) = Box::into_pin(inner.kill()).await {
                    tracing::warn!("Error killing child process: {}", e);
                }
            });
        }
    }
}

#[cfg(feature = "test-support")]
pub struct ChildWithCleanup {
    inner: Option<Box<dyn ChildWrapper>>,
    exit_tx: Option<tokio::sync::watch::Sender<Option<std::io::Result<std::process::ExitStatus>>>>,
}

#[cfg(feature = "test-support")]
impl Drop for ChildWithCleanup {
    fn drop(&mut self) {
        if let Some(mut inner) = self.inner.take() {
            let exit_tx = self.exit_tx.take();
            tokio::spawn(async move {
                if let Err(e) = inner.start_kill() {
                    tracing::warn!("start_kill failed: {e}");
                    if let Some(tx) = exit_tx {
                        let _ = tx.send(Some(Err(std::io::Error::new(
                            e.kind(),
                            format!("start_kill failed during drop: {e}"),
                        ))));
                    }
                    return;
                }
                let wait_res = inner.wait().await;
                if let Some(tx) = exit_tx {
                    let _ = tx.send(Some(wait_res));
                }
            });
        }
    }
}

/// Wait up to `MAX_WAIT_ON_DROP_SECS` for the child to exit.
/// Returns `Ok(None)` if it is still running when the timeout expires.
async fn wait_for_exit(
    child: &mut Box<dyn ChildWrapper>,
) -> std::io::Result<Option<std::process::ExitStatus>> {
    let timeout = std::time::Duration::from_secs(MAX_WAIT_ON_DROP_SECS);
    match tokio::time::timeout(timeout, child.wait()).await {
        Ok(Ok(status)) => {
            tracing::info!("Child exited gracefully {}", status);
            Ok(Some(status))
        }
        Ok(Err(e)) => {
            tracing::warn!("Error waiting for child: {e}");
            Err(e)
        }
        Err(_) => Ok(None),
    }
}

impl TokioChildProcess {
    /// Convenience: spawn with default `piped` stdio
    pub fn new(command: impl Into<CommandWrap>) -> std::io::Result<Self> {
        let (proc, _ignored) = TokioChildProcessBuilder::new(command).spawn()?;
        Ok(proc)
    }

    /// Builder entry-point allowing fine-grained stdio control.
    pub fn builder(command: impl Into<CommandWrap>) -> TokioChildProcessBuilder {
        TokioChildProcessBuilder::new(command)
    }

    /// Get the process ID of the child process.
    pub fn id(&self) -> Option<u32> {
        self.child.inner.as_ref()?.id()
    }

    /// Gracefully shutdown the child process
    ///
    /// This follows the stdio shutdown sequence from the MCP specification:
    /// close the transport to the child process (the server) and wait for it to
    /// exit; if it doesn't exit within the timeout, send `SIGTERM` (Unix only) and
    /// wait again; if it still doesn't exit, kill it.
    pub async fn graceful_shutdown(&mut self) -> std::io::Result<()> {
        // The child stays in its cleanup guard until it has exited or been killed, so if this
        // future is cancelled partway through, dropping the transport still kills the child.
        let Some(child) = self.child.inner.as_mut() else {
            return Ok(());
        };
        self.transport.close().await?;

        if let Some(status) = wait_for_exit(child).await? {
            self.child.inner = None;
            #[cfg(feature = "test-support")]
            if let Some(exit_tx) = self.exit_tx.as_ref() {
                let _ = exit_tx.send(Some(Ok(status)));
            }
            return Ok(());
        }

        #[cfg(unix)]
        match child.signal(libc::SIGTERM) {
            Ok(()) => {
                if let Some(status) = wait_for_exit(child).await? {
                    self.child.inner = None;
                    #[cfg(feature = "test-support")]
                    if let Some(exit_tx) = self.exit_tx.as_ref() {
                        let _ = exit_tx.send(Some(Ok(status)));
                    }
                    return Ok(());
                }
            }
            Err(e) => tracing::warn!("Error sending SIGTERM to child: {e}"),
        }

        if let Err(e) = Box::into_pin(child.kill()).await {
            tracing::warn!("Error killing child: {e}");
            #[cfg(feature = "test-support")]
            if let Some(exit_tx) = self.exit_tx.as_ref() {
                let _ = exit_tx.send(Some(Err(std::io::Error::new(e.kind(), e.to_string()))));
            }
            return Err(e);
        }
        #[cfg(feature = "test-support")]
        if let Some(exit_tx) = self.exit_tx.as_ref() {
            let status = child.wait().await;
            let _ = exit_tx.send(Some(status));
        }
        self.child.inner = None;
        Ok(())
    }
    /// Take ownership of the inner child process
    pub fn into_inner(mut self) -> Option<Box<dyn ChildWrapper>> {
        self.child.inner.take()
    }
}

/// Builder for `TokioChildProcess` allowing custom `Stdio` configuration.
pub struct TokioChildProcessBuilder {
    cmd: CommandWrap,
    stdin: Stdio,
    stdout: Stdio,
    stderr: Stdio,
    #[cfg(feature = "test-support")]
    kill_hook: Option<Box<dyn FnOnce(ChildKillController) + Send>>,
}

impl TokioChildProcessBuilder {
    fn new(cmd: impl Into<CommandWrap>) -> Self {
        Self {
            cmd: cmd.into(),
            stdin: Stdio::piped(),
            stdout: Stdio::piped(),
            stderr: Stdio::inherit(),
            #[cfg(feature = "test-support")]
            kill_hook: None,
        }
    }

    #[cfg(feature = "test-support")]
    pub fn on_kill_controller(mut self, hook: impl FnOnce(ChildKillController) + Send + 'static) -> Self {
        self.kill_hook = Some(Box::new(hook));
        self
    }

    /// Override the child stdin configuration.
    pub fn stdin(mut self, io: impl Into<Stdio>) -> Self {
        self.stdin = io.into();
        self
    }
    /// Override the child stdout configuration.
    pub fn stdout(mut self, io: impl Into<Stdio>) -> Self {
        self.stdout = io.into();
        self
    }
    /// Override the child stderr configuration.
    pub fn stderr(mut self, io: impl Into<Stdio>) -> Self {
        self.stderr = io.into();
        self
    }

    /// Spawn the child process. Returns the transport plus an optional captured stderr handle.
    pub fn spawn(mut self) -> std::io::Result<(TokioChildProcess, Option<ChildStderr>)> {
        self.cmd
            .command_mut()
            .stdin(self.stdin)
            .stdout(self.stdout)
            .stderr(self.stderr);

        let raw_child = self.cmd.spawn()?;

        #[cfg(feature = "test-support")]
        let (exit_tx, exit_rx) = tokio::sync::watch::channel(None);

        let mut child_guard = ChildWithCleanup {
            inner: Some(raw_child),
            #[cfg(feature = "test-support")]
            exit_tx: Some(exit_tx.clone()),
        };

        #[cfg(feature = "test-support")]
        let (kill_tx, kill_rx) = if self.kill_hook.is_some() {
            let (tx, rx) = tokio::sync::mpsc::channel(1);
            (Some(tx), Some(rx))
        } else {
            (None, None)
        };

        #[cfg(feature = "test-support")]
        if let (Some(hook), Some(tx)) = (self.kill_hook.take(), kill_tx) {
            hook(ChildKillController { tx, exit_rx });
        }

        let inner_ref = child_guard.inner.as_mut().unwrap();
        let stdin = match inner_ref.inner_mut().stdin().take() {
            Some(stdin) => stdin,
            None => return Err(std::io::Error::other("stdin was already taken")),
        };
        let stdout = match inner_ref.inner_mut().stdout().take() {
            Some(stdout) => stdout,
            None => return Err(std::io::Error::other("stdout was already taken")),
        };
        let stderr_opt = inner_ref.inner_mut().stderr().take();

        let transport = AsyncRwTransport::new(stdout, stdin);
        let proc = TokioChildProcess {
            child: child_guard,
            transport,
            #[cfg(feature = "test-support")]
            kill_rx,
            #[cfg(feature = "test-support")]
            pending_kill: None,
            #[cfg(feature = "test-support")]
            exit_tx: Some(exit_tx),
        };
        Ok((proc, stderr_opt))
    }
}
#[cfg(feature = "test-support")]
impl TokioChildProcess {
    async fn drive_pending_kill(&mut self) -> Option<RxJsonRpcMessage<RoleClient>> {
        let Some(pending) = self.pending_kill.as_mut() else {
            return None;
        };
        let Some(child) = self.child.inner.as_mut() else {
            let pending = self.pending_kill.take().unwrap();
            let _ = pending.reply.send(Err(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                "child already exited",
            )));
            let _ = self.transport.close().await;
            return None;
        };

        if !pending.signaled {
            #[cfg(unix)]
            if let Err(e) = child.signal(pending.signal) {
                let pending = self.pending_kill.take().unwrap();
                let _ = pending.reply.send(Err(e));
                let _ = self.transport.close().await;
                return None;
            }

            #[cfg(not(unix))]
            if let Err(e) = Box::into_pin(child.kill()).await {
                let pending = self.pending_kill.take().unwrap();
                let _ = pending.reply.send(Err(e));
                let _ = self.transport.close().await;
                return None;
            }

            self.pending_kill.as_mut().unwrap().signaled = true;
        }

        let wait_result = self.child.inner.as_mut().unwrap().wait().await;

        let pending = self.pending_kill.take().unwrap();
        match wait_result {
            Ok(status) => {
                self.child.inner = None;
                if let Some(exit_tx) = self.exit_tx.as_ref() {
                    let _ = exit_tx.send(Some(Ok(status)));
                }
                let _ = pending.reply.send(Ok(status));
            }
            Err(e) => {
                if let Some(exit_tx) = self.exit_tx.as_ref() {
                    let _ = exit_tx.send(Some(Err(std::io::Error::new(e.kind(), e.to_string()))));
                }
                let _ = pending.reply.send(Err(e));
            }
        }
        let _ = self.transport.close().await;
        None
    }
}

impl Transport<RoleClient> for TokioChildProcess {
    type Error = std::io::Error;

    fn send(
        &mut self,
        item: TxJsonRpcMessage<RoleClient>,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send + 'static {
        self.transport.send(item)
    }

    fn receive(&mut self) -> impl Future<Output = Option<RxJsonRpcMessage<RoleClient>>> + Send {
        async move {
            #[cfg(feature = "test-support")]
            loop {
                if self.pending_kill.is_some() {
                    return self.drive_pending_kill().await;
                }

                if let Some(kill_rx) = self.kill_rx.as_mut() {
                    tokio::select! {
                        biased;
                        Some(cmd) = kill_rx.recv() => {
                            self.pending_kill = Some(PendingKill {
                                signal: cmd.signal,
                                signaled: false,
                                reply: cmd.reply,
                            });
                            continue;
                        }
                        msg = self.transport.receive() => {
                            return msg;
                        }
                    }
                } else {
                    return self.transport.receive().await;
                }
            }

            #[cfg(not(feature = "test-support"))]
            self.transport.receive().await
        }
    }

    fn close(&mut self) -> impl Future<Output = Result<(), Self::Error>> + Send {
        async move {
            #[cfg(feature = "test-support")]
            if self.pending_kill.is_some() {
                let _ = self.drive_pending_kill().await;
                let reaped = self.child.inner.is_none();
                if !reaped {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::Other,
                        "failed to reap child during pending kill shutdown",
                    ));
                }
                return Ok(());
            }

            self.graceful_shutdown().await
        }
    }
}

pub trait ConfigureCommandExt {
    fn configure(self, f: impl FnOnce(&mut Self)) -> Self;
}

impl ConfigureCommandExt for tokio::process::Command {
    fn configure(mut self, f: impl FnOnce(&mut Self)) -> Self {
        f(&mut self);
        self
    }
}

/// Resolve the absolute path to an executable using the system `PATH`,
/// then return a [`tokio::process::Command`] pointing at it.
///
/// This is especially useful on Windows where `.cmd` / `.exe` shim scripts
/// (e.g. `npx.cmd`) are not reliably found by [`tokio::process::Command`]
/// without a fully-qualified path.
///
/// # Example
/// ```rust,no_run
/// use rmcp::transport::{which_command, ConfigureCommandExt};
///
/// # fn example() -> std::io::Result<()> {
/// let cmd = which_command("npx")?
///     .configure(|cmd| {
///         cmd.arg("-y").arg("@modelcontextprotocol/server-everything");
///     });
/// # Ok(())
/// # }
/// ```
#[cfg(feature = "which-command")]
pub fn which_command(
    name: impl AsRef<std::ffi::OsStr>,
) -> std::io::Result<tokio::process::Command> {
    let resolved = which::which(name.as_ref())
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::NotFound, e))?;
    Ok(tokio::process::Command::new(resolved))
}

#[cfg(feature = "which-command")]
#[cfg(test)]
mod tests_which {
    #[test]
    fn which_command_resolves_known_binary() {
        // `ls` exists on every Unix system, `cmd` on Windows
        #[cfg(unix)]
        let result = super::which_command("ls");
        #[cfg(windows)]
        let result = super::which_command("cmd");

        assert!(result.is_ok());
    }

    #[test]
    fn which_command_fails_for_nonexistent() {
        let result = super::which_command("this_binary_definitely_does_not_exist_12345");
        assert!(result.is_err());
        assert_eq!(result.unwrap_err().kind(), std::io::ErrorKind::NotFound);
    }
}

#[cfg(unix)]
#[cfg(test)]
mod tests {
    use tokio::process::Command;

    use super::*;

    #[tokio::test]
    async fn test_tokio_child_process_drop() {
        let r = TokioChildProcess::new(Command::new("sleep").configure(|cmd| {
            cmd.arg("30");
        }));
        assert!(r.is_ok());
        let child_process = r.unwrap();
        let id = child_process.id();
        assert!(id.is_some());
        let id = id.unwrap();
        // Drop the child process
        drop(child_process);
        // Wait a moment to allow the cleanup task to run
        tokio::time::sleep(std::time::Duration::from_secs(MAX_WAIT_ON_DROP_SECS + 1)).await;
        // Check if the process is still running
        let status = Command::new("ps")
            .arg("-p")
            .arg(id.to_string())
            .status()
            .await;
        match status {
            Ok(status) => {
                assert!(
                    !status.success(),
                    "Process with PID {} is still running",
                    id
                );
            }
            Err(e) => {
                panic!("Failed to check process status: {}", e);
            }
        }
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn test_tokio_child_process_graceful_shutdown_sends_sigterm_before_kill() {
        // A server that ignores stdin EOF but exits cleanly on SIGTERM, recording that it did.
        let marker = std::env::temp_dir().join(format!(
            "rmcp-sigterm-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let script = format!(
            "trap 'echo term > \"{}\"; exit 0' TERM; while :; do sleep 0.1; done",
            marker.display()
        );
        let mut child_process = TokioChildProcess::new(Command::new("sh").configure(|cmd| {
            cmd.arg("-c").arg(&script);
        }))
        .unwrap();

        let started = std::time::Instant::now();
        child_process.graceful_shutdown().await.unwrap();

        assert!(
            marker.exists(),
            "child was killed without receiving SIGTERM first"
        );
        assert!(
            started.elapsed() < std::time::Duration::from_secs(2 * MAX_WAIT_ON_DROP_SECS),
            "child exited on SIGTERM, so the kill timeout should not be reached"
        );
        let _ = std::fs::remove_file(&marker);
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn test_tokio_child_process_cancelled_graceful_shutdown_still_cleans_up() {
        // A server that ignores both stdin EOF and SIGTERM.
        let mut child_process = TokioChildProcess::new(Command::new("sh").configure(|cmd| {
            cmd.arg("-c")
                .arg("trap '' TERM; while :; do sleep 0.1; done");
        }))
        .unwrap();
        let id = child_process.id().unwrap();

        // Cancel the shutdown during the second wait, after SIGTERM and before the kill.
        let shutdown = tokio::time::timeout(
            std::time::Duration::from_secs(MAX_WAIT_ON_DROP_SECS + 1),
            child_process.graceful_shutdown(),
        )
        .await;
        assert!(
            shutdown.is_err(),
            "shutdown should still be waiting when the timeout fires"
        );

        // Dropping the transport must still clean the child up.
        drop(child_process);
        tokio::time::sleep(std::time::Duration::from_secs(1)).await;
        let status = Command::new("ps")
            .arg("-p")
            .arg(id.to_string())
            .status()
            .await
            .unwrap();
        assert!(
            !status.success(),
            "Process with PID {} survived a cancelled graceful_shutdown",
            id
        );
    }

    #[tokio::test]
    async fn test_tokio_child_process_graceful_shutdown() {
        let r = TokioChildProcess::new(Command::new("sleep").configure(|cmd| {
            cmd.arg("30");
        }));
        assert!(r.is_ok());
        let mut child_process = r.unwrap();
        let id = child_process.id();
        assert!(id.is_some());
        let id = id.unwrap();
        child_process.graceful_shutdown().await.unwrap();
        // Wait a moment to allow the cleanup task to run
        tokio::time::sleep(std::time::Duration::from_secs(MAX_WAIT_ON_DROP_SECS + 1)).await;
        // Check if the process is still running
        let status = Command::new("ps")
            .arg("-p")
            .arg(id.to_string())
            .status()
            .await;
        match status {
            Ok(status) => {
                assert!(
                    !status.success(),
                    "Process with PID {} is still running",
                    id
                );
            }
            Err(e) => {
                panic!("Failed to check process status: {}", e);
            }
        }
    }

    #[cfg(all(feature = "test-support", unix))]
    #[tokio::test]
    async fn test_pending_kill_persists_across_receive_cancellation() {
        let (kill_tx, mut kill_rx) = tokio::sync::mpsc::unbounded_channel();
        let builder = TokioChildProcess::builder(Command::new("sleep").configure(|cmd| {
            cmd.arg("30");
        }))
        .on_kill_controller(move |ctrl| {
            let _ = kill_tx.send(ctrl);
        });

        let (mut proc, _) = builder.spawn().unwrap();
        let ctrl = kill_rx.recv().await.unwrap();

        let (reply_tx, reply_rx) = tokio::sync::oneshot::channel();
        // Signal SIGCONT: supported non-terminating signal, so sleep 30 remains pending in wait()
        ctrl.tx
            .send(KillCommand {
                signal: libc::SIGCONT,
                reply: reply_tx,
            })
            .await
            .unwrap();

        // Drive receive() into drive_pending_kill: delivers signal 0, sets signaled=true, then awaits child.wait()
        {
            let mut recv_fut = std::pin::pin!(proc.receive());
            let _ = tokio::time::timeout(std::time::Duration::from_millis(50), &mut recv_fut).await;
        }

        // sleep 30 is still running, so pending_kill must be preserved across cancellation!
        let pending = proc.pending_kill.as_ref().expect("pending_kill must survive receive future cancellation");
        assert!(pending.signaled, "signal must have been delivered before cancellation");

        // Now trigger process termination and drive pending kill via close()
        let _ = proc.child.inner.as_mut().unwrap().start_kill();
        let close_res = proc.close().await;
        assert!(close_res.is_ok(), "close must succeed driving pending kill");

        let status = reply_rx.await.unwrap().unwrap();
        assert!(!status.success());
    }
}
