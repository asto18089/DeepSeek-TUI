//! Low-level tool execution helpers for the engine turn loop.
//!
//! This module keeps the mechanics of MCP dispatch, execution locking, and
//! parallel-tool fanout out of `engine.rs`; the turn loop still owns planning,
//! approval, and how tool results are written back into session state.

use std::{collections::VecDeque, fs::OpenOptions, io::Write, sync::Arc, time::Duration};

use super::*;
use crate::tools::spec::{ToolOutputSink, ToolOutputStream};

/// RAII guard that pauses the TUI's terminal-state ownership for the duration
/// of an interactive tool, then restores it on drop.
///
/// Background: interactive tools (anything that needs the raw TTY — external
/// editor, `exec_shell` with stdin, etc.) need the TUI to leave alt-screen,
/// disable raw mode, and release mouse capture so the child sees a normal
/// terminal. The TUI listens for `Event::PauseEvents` / `Event::ResumeEvents`
/// and runs `pause_terminal` / `resume_terminal` in response.
///
/// Earlier code sent `PauseEvents` before tool execution and `ResumeEvents`
/// after. That worked on the happy path, but if the tool's future was dropped
/// — Ctrl+C cancellation, sub-agent abort, parent task cancelled while the
/// tool was awaiting — the second `await` never reached and `ResumeEvents`
/// was never sent. It also let interactive children start before the UI had
/// actually left alt-screen/raw mode. Both failures strand the TUI in a
/// regular shell scrollback: the parent shell scrollbar takes over, mouse
/// wheel scrolls the host terminal instead of the transcript, and the TUI
/// renders at the bottom of cooked-mode output.
///
/// `Drop` runs synchronously and can't await, so we first use `try_send` on a
/// **clone of the event channel** to push `ResumeEvents` non-blockingly. If the
/// channel is full we enqueue the resume on the active Tokio runtime instead of
/// dropping it; otherwise a burst of engine events can strand the UI in the
/// paused terminal state.
pub(super) struct InteractiveTerminalGuard {
    tx: Option<mpsc::Sender<Event>>,
}

impl InteractiveTerminalGuard {
    /// Send `PauseEvents` and arm the guard. If `interactive` is false the
    /// guard is a no-op — `Drop` will skip the resume.
    pub(super) async fn engage(tx: mpsc::Sender<Event>, interactive: bool) -> Self {
        if !interactive {
            return Self { tx: None };
        }
        // Best-effort: if the receiver is gone the TUI has already shut down
        // and there's nothing to restore. If the event is delivered, wait for
        // the UI to actually release the terminal before starting the child.
        let ack = Arc::new(tokio::sync::Notify::new());
        match tx
            .send(Event::PauseEvents {
                ack: Some(ack.clone()),
            })
            .await
        {
            Ok(()) => {
                if tokio::time::timeout(Duration::from_millis(750), ack.notified())
                    .await
                    .is_err()
                {
                    tracing::warn!(
                        target: "engine.tool_execution",
                        "InteractiveTerminalGuard: timed out waiting for terminal pause ack; \
                         continuing with interactive tool"
                    );
                }
            }
            Err(err) => {
                tracing::debug!(
                    target: "engine.tool_execution",
                    ?err,
                    "InteractiveTerminalGuard: event channel closed before PauseEvents"
                );
            }
        }
        Self { tx: Some(tx) }
    }
}

const TOOL_OUTPUT_FLUSH_INTERVAL: Duration = Duration::from_millis(25);

enum ToolOutputForwarderMessage {
    Chunk {
        stream: ToolOutputStream,
        content: String,
    },
    Flush(tokio::sync::oneshot::Sender<()>),
}

struct ToolOutputEventForwarder {
    tx: mpsc::UnboundedSender<ToolOutputForwarderMessage>,
}

impl ToolOutputEventForwarder {
    fn spawn(event_tx: mpsc::Sender<Event>, tool_call_id: String) -> (Self, ToolOutputSink) {
        let (tx, rx) = mpsc::unbounded_channel();
        tokio::spawn(run_tool_output_forwarder(rx, event_tx, tool_call_id));

        let sink_tx = tx.clone();
        let sink: ToolOutputSink = Arc::new(move |stream, content| {
            // Reader threads only enqueue into an unbounded in-process channel;
            // the async worker owns event-channel backpressure and batching.
            let _ = sink_tx.send(ToolOutputForwarderMessage::Chunk { stream, content });
        });
        (Self { tx }, sink)
    }

    async fn flush(&self) {
        let (ack_tx, ack_rx) = tokio::sync::oneshot::channel();
        if self
            .tx
            .send(ToolOutputForwarderMessage::Flush(ack_tx))
            .is_ok()
        {
            let _ = ack_rx.await;
        }
    }
}

fn append_tool_output_batch(
    batches: &mut VecDeque<(ToolOutputStream, String)>,
    stream: ToolOutputStream,
    content: String,
) {
    if content.is_empty() {
        return;
    }
    if let Some((last_stream, last_content)) = batches.back_mut()
        && *last_stream == stream
    {
        last_content.push_str(&content);
    } else {
        batches.push_back((stream, content));
    }
}

async fn flush_tool_output_batches(
    batches: &mut VecDeque<(ToolOutputStream, String)>,
    event_tx: &mpsc::Sender<Event>,
    tool_call_id: &str,
) -> bool {
    while let Some((stream, content)) = batches.pop_front() {
        if event_tx
            .send(Event::ToolCallOutput {
                id: tool_call_id.to_string(),
                stream,
                content,
            })
            .await
            .is_err()
        {
            return false;
        }
    }
    true
}

async fn run_tool_output_forwarder(
    mut rx: mpsc::UnboundedReceiver<ToolOutputForwarderMessage>,
    event_tx: mpsc::Sender<Event>,
    tool_call_id: String,
) {
    let mut batches = VecDeque::new();
    let first_tick = tokio::time::Instant::now() + TOOL_OUTPUT_FLUSH_INTERVAL;
    let mut ticker = tokio::time::interval_at(first_tick, TOOL_OUTPUT_FLUSH_INTERVAL);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    loop {
        tokio::select! {
            message = rx.recv() => {
                match message {
                    Some(ToolOutputForwarderMessage::Chunk { stream, content }) => {
                        append_tool_output_batch(&mut batches, stream, content);
                    }
                    Some(ToolOutputForwarderMessage::Flush(ack)) => {
                        let delivered =
                            flush_tool_output_batches(&mut batches, &event_tx, &tool_call_id).await;
                        let _ = ack.send(());
                        if !delivered {
                            return;
                        }
                    }
                    None => {
                        let _ =
                            flush_tool_output_batches(&mut batches, &event_tx, &tool_call_id).await;
                        return;
                    }
                }
            }
            _ = ticker.tick(), if !batches.is_empty() => {
                if !flush_tool_output_batches(&mut batches, &event_tx, &tool_call_id).await {
                    return;
                }
            }
        }
    }
}

impl Drop for InteractiveTerminalGuard {
    fn drop(&mut self) {
        if let Some(tx) = self.tx.take() {
            match tx.try_send(Event::ResumeEvents) {
                Ok(()) => {}
                Err(tokio::sync::mpsc::error::TrySendError::Full(event)) => {
                    match tokio::runtime::Handle::try_current() {
                        Ok(handle) => {
                            handle.spawn(async move {
                                if let Err(err) = tx.send(event).await {
                                    tracing::warn!(
                                        target: "engine.tool_execution",
                                        ?err,
                                        "InteractiveTerminalGuard: async send(ResumeEvents) failed; \
                                         terminal may stay in paused state until the next \
                                         pause/resume cycle"
                                    );
                                }
                            });
                        }
                        Err(err) => {
                            tracing::warn!(
                                target: "engine.tool_execution",
                                ?err,
                                "InteractiveTerminalGuard: event channel full and no Tokio runtime \
                                 available to queue ResumeEvents; terminal may stay paused until \
                                 the next pause/resume cycle"
                            );
                        }
                    }
                }
                Err(tokio::sync::mpsc::error::TrySendError::Closed(_)) => {
                    tracing::debug!(
                        target: "engine.tool_execution",
                        "InteractiveTerminalGuard: event channel closed before ResumeEvents"
                    );
                }
            }
        }
    }
}

pub(super) fn emit_tool_audit(event: serde_json::Value) {
    let Some(path) = std::env::var_os("DEEPSEEK_TOOL_AUDIT_LOG") else {
        return;
    };
    let line = match serde_json::to_string(&event) {
        Ok(line) => line,
        Err(e) => {
            tracing::error!("Failed to serialize tool audit event: {e}");
            return;
        }
    };
    let path = PathBuf::from(path);
    if let Some(parent) = path.parent()
        && let Err(e) = std::fs::create_dir_all(parent)
    {
        tracing::error!(
            "Failed to create audit log directory {}: {e}",
            parent.display()
        );
        return;
    }
    match OpenOptions::new().create(true).append(true).open(&path) {
        Ok(mut file) => {
            if let Err(e) = writeln!(file, "{line}") {
                tracing::error!("Failed to write to audit log {}: {e}", path.display());
            }
        }
        Err(e) => {
            tracing::error!("Failed to open audit log {}: {e}", path.display());
        }
    }
}

impl Engine {
    pub(super) async fn execute_mcp_tool_with_pool(
        pool: Arc<AsyncMutex<McpPool>>,
        name: &str,
        input: serde_json::Value,
    ) -> Result<ToolResult, ToolError> {
        let mut pool = pool.lock().await;
        let result = pool
            .call_tool(name, input)
            .await
            .map_err(|e| ToolError::execution_failed(format!("MCP tool failed: {e}")))?;
        let content = serde_json::to_string_pretty(&result).unwrap_or_else(|_| result.to_string());
        Ok(ToolResult::success(content))
    }

    pub(super) async fn execute_parallel_tool(
        &mut self,
        input: serde_json::Value,
        tool_registry: Option<&crate::tools::ToolRegistry>,
        tool_exec_lock: Arc<RwLock<()>>,
    ) -> Result<ToolResult, ToolError> {
        let calls = parse_parallel_tool_calls(&input)?;
        let mcp_pool = if calls.iter().any(|(tool, _)| McpPool::is_mcp_tool(tool)) {
            Some(self.ensure_mcp_pool().await?)
        } else {
            None
        };
        let Some(registry) = tool_registry else {
            return Err(ToolError::not_available(
                "tool registry unavailable for multi_tool_use.parallel",
            ));
        };

        let result_count = calls.len();
        let mut tasks = FuturesUnordered::new();
        let shell_permits = Arc::new(tokio::sync::Semaphore::new(MAX_PARALLEL_SHELL_EXEC));
        for (index, (tool_name, tool_input)) in calls.into_iter().enumerate() {
            if tool_name == MULTI_TOOL_PARALLEL_NAME {
                return Err(ToolError::invalid_input(
                    "multi_tool_use.parallel cannot call itself",
                ));
            }
            if McpPool::is_mcp_tool(&tool_name) {
                if !mcp_tool_is_parallel_safe(&tool_name) {
                    return Err(ToolError::invalid_input(format!(
                        "Tool '{tool_name}' is an MCP tool and cannot run in parallel. \
                         Allowed MCP tools: list_mcp_resources, list_mcp_resource_templates, \
                         mcp_read_resource, read_mcp_resource, mcp_get_prompt."
                    )));
                }
            } else {
                let Some(spec) = registry.get(&tool_name) else {
                    return Err(ToolError::not_available(format!(
                        "tool '{tool_name}' is not registered"
                    )));
                };
                if !spec.is_read_only_for(&tool_input) {
                    return Err(ToolError::invalid_input(format!(
                        "Tool '{tool_name}' is not read-only and cannot run in parallel"
                    )));
                }
                if spec.approval_requirement_for(&tool_input) != ApprovalRequirement::Auto {
                    return Err(ToolError::invalid_input(format!(
                        "Tool '{tool_name}' requires approval and cannot run in parallel"
                    )));
                }
                if !spec.supports_parallel_for(&tool_input) {
                    return Err(ToolError::invalid_input(format!(
                        "Tool '{tool_name}' does not support parallel execution"
                    )));
                }
            }

            let registry_ref = registry;
            let lock = tool_exec_lock.clone();
            let tx_event = self.tx_event.clone();
            let mcp_pool = mcp_pool.clone();
            let shell_permits = shell_permits.clone();
            let workspace = self.session.workspace.clone();
            tasks.push(async move {
                let _shell_permit = if tool_name == "exec_shell" {
                    shell_permits.acquire_owned().await.ok()
                } else {
                    None
                };
                let result = Engine::execute_tool_with_lock(
                    lock,
                    true,
                    false,
                    tx_event,
                    tool_name.clone(),
                    tool_input.clone(),
                    workspace,
                    Some(registry_ref),
                    mcp_pool,
                    None,
                    None,
                )
                .await;
                (index, tool_name, result)
            });
        }

        let mut results: Vec<Option<ParallelToolResultEntry>> = Vec::with_capacity(result_count);
        results.resize_with(result_count, || None);
        while let Some((index, tool_name, result)) = tasks.next().await {
            let entry = match result {
                Ok(output) => {
                    let mut error = None;
                    if !output.success {
                        error = Some(output.content.clone());
                    }
                    ParallelToolResultEntry {
                        tool_name,
                        success: output.success,
                        content: output.content,
                        error,
                    }
                }
                Err(err) => {
                    let message = format!("{err}");
                    ParallelToolResultEntry {
                        tool_name,
                        success: false,
                        content: format!("Error: {message}"),
                        error: Some(message),
                    }
                }
            };
            results[index] = Some(entry);
        }
        let results = results.into_iter().flatten().collect();

        ToolResult::json(&ParallelToolResult { results })
            .map_err(|e| ToolError::execution_failed(e.to_string()))
    }

    #[allow(clippy::too_many_arguments)]
    pub(super) async fn execute_tool_with_lock(
        lock: Arc<RwLock<()>>,
        supports_parallel: bool,
        interactive: bool,
        tx_event: mpsc::Sender<Event>,
        tool_name: String,
        tool_input: serde_json::Value,
        workspace: PathBuf,
        registry: Option<&crate::tools::ToolRegistry>,
        mcp_pool: Option<Arc<AsyncMutex<McpPool>>>,
        context_override: Option<crate::tools::ToolContext>,
        tool_call_id: Option<String>,
    ) -> Result<ToolResult, ToolError> {
        let started_at = std::time::Instant::now();
        let dispatch = if McpPool::is_mcp_tool(&tool_name) {
            "mcp"
        } else if matches!(
            tool_name.as_str(),
            CODE_EXECUTION_TOOL_NAME | JS_EXECUTION_TOOL_NAME
        ) {
            "interpreter"
        } else if registry.is_some() {
            "registry"
        } else {
            "missing"
        };
        let input_bytes = serde_json::to_string(&tool_input)
            .map(|s| s.len())
            .unwrap_or(0);
        tracing::debug!(
            target: "engine.tool_execution",
            tool = %tool_name,
            dispatch,
            interactive,
            supports_parallel,
            input_bytes,
            "tool.exec.start",
        );

        let mut context_override = context_override;
        let mut output_forwarder = None;
        if matches!(
            tool_name.as_str(),
            "exec_shell" | "exec_shell_wait" | "exec_wait" | "task_shell_wait"
        )
            && let (Some(registry), Some(tool_call_id)) = (registry, tool_call_id)
        {
            let mut context = context_override
                .take()
                .unwrap_or_else(|| registry.context().clone());
            let (forwarder, sink) = ToolOutputEventForwarder::spawn(tx_event.clone(), tool_call_id);
            context.tool_output_sink = Some(sink);
            output_forwarder = Some(forwarder);
            context_override = Some(context);
        }

        let _guard = if supports_parallel {
            ToolExecGuard::Read(lock.read().await)
        } else {
            ToolExecGuard::Write(lock.write().await)
        };

        // RAII pause/resume: ensures `Event::ResumeEvents` always fires on
        // drop, even if the tool future is cancelled mid-await. See
        // `InteractiveTerminalGuard` doc-comment for the regression this
        // closes (parent terminal scrollback hijacking the TUI after a
        // cancelled interactive tool).
        let _terminal = InteractiveTerminalGuard::engage(tx_event, interactive).await;

        let outcome = if McpPool::is_mcp_tool(&tool_name) {
            if let Some(pool) = mcp_pool {
                Engine::execute_mcp_tool_with_pool(pool, &tool_name, tool_input).await
            } else {
                Err(ToolError::not_available(format!(
                    "tool '{tool_name}' is not registered"
                )))
            }
        } else if tool_name == CODE_EXECUTION_TOOL_NAME {
            execute_code_execution_tool(&tool_input, &workspace).await
        } else if tool_name == JS_EXECUTION_TOOL_NAME {
            execute_js_execution_tool(&tool_input, &workspace).await
        } else if let Some(registry) = registry {
            registry
                .execute_full_with_context(&tool_name, tool_input, context_override.as_ref())
                .await
        } else {
            Err(ToolError::not_available(format!(
                "tool '{tool_name}' is not registered"
            )))
        };
        if let Some(forwarder) = output_forwarder.as_ref() {
            // Preserve ordering with the subsequent ToolCallComplete event.
            // Detached readers keep their own sink clone and continue using
            // the same worker after this initial flush.
            forwarder.flush().await;
        }

        let duration_ms = started_at.elapsed().as_millis() as u64;
        match &outcome {
            Ok(result) => {
                tracing::debug!(
                    target: "engine.tool_execution",
                    tool = %tool_name,
                    dispatch,
                    duration_ms,
                    success = result.success,
                    output_bytes = result.content.len(),
                    "tool.exec.end",
                );
            }
            Err(err) => {
                let kind = match err {
                    ToolError::InvalidInput { .. } => "invalid_input",
                    ToolError::MissingField { .. } => "missing_field",
                    ToolError::PathEscape { .. } => "path_escape",
                    ToolError::ExecutionFailed { .. } => "execution_failed",
                    ToolError::Timeout { .. } => "timeout",
                    ToolError::NotAvailable { .. } => "not_available",
                    ToolError::PermissionDenied { .. } => "permission_denied",
                };
                tracing::warn!(
                    target: "engine.tool_execution",
                    tool = %tool_name,
                    dispatch,
                    duration_ms,
                    error_kind = kind,
                    error = %err,
                    "tool.exec.end",
                );
            }
        }
        outcome
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use std::{ffi::OsString, path::Path, sync::Mutex, time::Duration};

    /// Tests in this module mutate `DEEPSEEK_TOOL_AUDIT_LOG` which is
    /// process-global; serialise through this guard so the parallel
    /// runner doesn't observe interleaved env mutations.
    static AUDIT_TEST_GUARD: Mutex<()> = Mutex::new(());

    fn audit_test_guard() -> std::sync::MutexGuard<'static, ()> {
        AUDIT_TEST_GUARD.lock().unwrap_or_else(|e| e.into_inner())
    }

    struct AuditEnvGuard {
        previous: Option<OsString>,
    }

    impl AuditEnvGuard {
        fn set(path: &Path) -> Self {
            let previous = std::env::var_os("DEEPSEEK_TOOL_AUDIT_LOG");
            // SAFETY: serialised by the guard above.
            unsafe {
                std::env::set_var("DEEPSEEK_TOOL_AUDIT_LOG", path);
            }
            Self { previous }
        }

        fn unset() -> Self {
            let previous = std::env::var_os("DEEPSEEK_TOOL_AUDIT_LOG");
            // SAFETY: serialised by the guard above.
            unsafe {
                std::env::remove_var("DEEPSEEK_TOOL_AUDIT_LOG");
            }
            Self { previous }
        }
    }

    impl Drop for AuditEnvGuard {
        fn drop(&mut self) {
            // SAFETY: callers hold AUDIT_TEST_GUARD for this guard's lifetime.
            unsafe {
                if let Some(previous) = self.previous.take() {
                    std::env::set_var("DEEPSEEK_TOOL_AUDIT_LOG", previous);
                } else {
                    std::env::remove_var("DEEPSEEK_TOOL_AUDIT_LOG");
                }
            }
        }
    }

    #[tokio::test]
    async fn terminal_guard_queues_resume_when_event_channel_is_full() {
        let (tx, mut rx) = mpsc::channel(1);
        tx.try_send(Event::status("filler")).expect("fill channel");

        drop(InteractiveTerminalGuard { tx: Some(tx) });

        assert!(matches!(rx.recv().await, Some(Event::Status { .. })));
        let resumed = tokio::time::timeout(Duration::from_secs(1), rx.recv())
            .await
            .expect("queued resume event")
            .expect("event channel still open");
        assert!(matches!(resumed, Event::ResumeEvents));
    }

    #[tokio::test]
    async fn terminal_guard_waits_for_pause_ack_before_returning() {
        let (tx, mut rx) = mpsc::channel(4);
        let task = tokio::spawn(InteractiveTerminalGuard::engage(tx, true));

        let event = tokio::time::timeout(Duration::from_secs(1), rx.recv())
            .await
            .expect("pause event")
            .expect("event channel still open");
        let ack = match event {
            Event::PauseEvents { ack: Some(ack) } => ack,
            other => panic!("expected PauseEvents with ack, got {other:?}"),
        };

        tokio::task::yield_now().await;
        assert!(!task.is_finished(), "guard returned before pause ack");

        ack.notify_one();
        let guard = tokio::time::timeout(Duration::from_secs(1), task)
            .await
            .expect("guard returned after ack")
            .expect("guard task joined");

        drop(guard);
        let resumed = tokio::time::timeout(Duration::from_secs(1), rx.recv())
            .await
            .expect("resume event")
            .expect("event channel still open");
        assert!(matches!(resumed, Event::ResumeEvents));
    }

    #[tokio::test]
    async fn forkguard_tool_output_forwarder_coalesces_without_dropping_on_backpressure() {
        let (tx, mut rx) = mpsc::channel(1);
        tx.send(Event::status("filler"))
            .await
            .expect("fill channel");
        let expected_stdout = (0..100).map(|i| format!("out-{i};")).collect::<String>();
        let expected_stderr = (0..50).map(|i| format!("err-{i};")).collect::<String>();
        let mut batches = VecDeque::new();
        for i in 0..100 {
            append_tool_output_batch(
                &mut batches,
                ToolOutputStream::Stdout,
                format!("out-{i};"),
            );
        }
        for i in 0..50 {
            append_tool_output_batch(
                &mut batches,
                ToolOutputStream::Stderr,
                format!("err-{i};"),
            );
        }
        assert_eq!(batches.len(), 2, "adjacent chunks should be coalesced");

        let mut flush = Box::pin(flush_tool_output_batches(
            &mut batches,
            &tx,
            "congested-tool",
        ));
        assert!(
            matches!(
                futures_util::poll!(&mut flush),
                std::task::Poll::Pending
            ),
            "a full event channel must apply backpressure to the async worker"
        );
        assert!(matches!(rx.recv().await, Some(Event::Status { .. })));

        let collect = async {
            let mut stdout = String::new();
            let mut stderr = String::new();
            for _ in 0..2 {
                match rx.recv().await.expect("forwarded tool output") {
                    Event::ToolCallOutput {
                        id,
                        stream,
                        content,
                    } => {
                        assert_eq!(id, "congested-tool");
                        match stream {
                            ToolOutputStream::Stdout => stdout.push_str(&content),
                            ToolOutputStream::Stderr => stderr.push_str(&content),
                        }
                    }
                    other => panic!("unexpected event: {other:?}"),
                }
            }
            (stdout, stderr)
        };
        let (delivered, (stdout, stderr)) = tokio::join!(flush, collect);

        assert!(delivered);
        assert_eq!(stdout, expected_stdout);
        assert_eq!(stderr, expected_stderr);
    }

    #[test]
    fn emit_tool_audit_writes_jsonl_line_when_env_var_set() {
        let _g = audit_test_guard();
        let tmp = tempfile::tempdir().expect("tempdir");
        let path = tmp.path().join("audit.log");
        let _env = AuditEnvGuard::set(&path);
        let marker = path.display().to_string();

        emit_tool_audit(json!({
            "event": "tool.spillover",
            "test_marker": marker,
            "tool_id": "call-abc",
            "tool_name": "exec_shell",
            "path": "/tmp/foo.txt",
        }));
        emit_tool_audit(json!({
            "event": "tool.result",
            "test_marker": marker,
            "tool_id": "call-xyz",
            "success": true,
        }));

        let body = std::fs::read_to_string(&path).expect("audit log written");
        let entries: Vec<serde_json::Value> = body
            .lines()
            .map(|line| serde_json::from_str(line).expect("audit line is JSON"))
            .filter(|entry: &serde_json::Value| {
                entry.get("test_marker").and_then(|v| v.as_str()) == Some(marker.as_str())
            })
            .collect();
        assert_eq!(entries.len(), 2, "two marked emits -> two lines");

        // Each line round-trips as JSON, has the expected event key.
        let first = &entries[0];
        assert_eq!(
            first.get("event").and_then(|v| v.as_str()),
            Some("tool.spillover")
        );
        assert_eq!(
            first.get("tool_id").and_then(|v| v.as_str()),
            Some("call-abc")
        );

        let second = &entries[1];
        assert_eq!(
            second.get("event").and_then(|v| v.as_str()),
            Some("tool.result")
        );
    }

    #[test]
    fn emit_tool_audit_is_noop_when_env_var_unset() {
        let _g = audit_test_guard();
        let _env = AuditEnvGuard::unset();
        // Should not panic and should not create any file. We can't
        // assert "no file written" without knowing where one might be
        // written, but the contract is "do nothing", which we verify
        // by ensuring the call returns without error.
        emit_tool_audit(json!({"event": "noop", "x": 1}));
        // Successful return is the assertion.
    }

    #[test]
    fn emit_tool_audit_creates_parent_directory() {
        let _g = audit_test_guard();
        let tmp = tempfile::tempdir().expect("tempdir");
        // Path with a parent that doesn't exist yet — the writer
        // should create it.
        let nested = tmp.path().join("nested").join("dir").join("audit.log");
        let _env = AuditEnvGuard::set(&nested);
        emit_tool_audit(json!({"event": "test"}));
        assert!(nested.exists(), "writer should mkdir -p the parent chain");
    }
}
