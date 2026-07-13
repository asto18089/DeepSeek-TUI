//! Persistent background task manager for DeepSeek agent work.
//!
//! Tasks are durable across restarts and execute with a bounded worker pool.
//! Execution stays DeepSeek-only and now links every task to runtime
//! thread/turn records for unified timelines.

use std::collections::{HashMap, HashSet, VecDeque};
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;
#[cfg(test)]
use std::time::Duration as StdDuration;

use anyhow::{Context, Result, anyhow, bail};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use thiserror::Error;
use tokio::sync::{Mutex, Notify, mpsc, oneshot};
use tokio::time::sleep;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use crate::config::{Config, DEFAULT_TEXT_MODEL, MAX_SUBAGENTS};
use crate::runtime_threads::{
    CreateThreadRequest, RuntimeThreadManager, RuntimeThreadManagerConfig, RuntimeTurnStatus,
    SharedRuntimeThreadManager, StartTurnRequest,
};
use crate::utils::spawn_supervised;

const DEFAULT_WORKERS: usize = 2;
const MAX_WORKERS: usize = 8;
const TIMELINE_SUMMARY_LIMIT: usize = 240;
const ARTIFACT_THRESHOLD: usize = 1200;
const CURRENT_TASK_SCHEMA_VERSION: u32 = 3;
const MAX_TASK_ID_GENERATION_ATTEMPTS: usize = 32;
const TASK_PERSIST_RETRY_BACKOFF: Duration = Duration::from_millis(50);

type TaskIdGenerator = Arc<dyn Fn() -> String + Send + Sync>;

#[cfg(test)]
#[derive(Default)]
struct TestPersistenceProbe {
    task_writes: std::sync::atomic::AtomicUsize,
    queue_writes: std::sync::atomic::AtomicUsize,
    task_writes_by_id: std::sync::Mutex<HashMap<String, usize>>,
    task_writes_by_status: std::sync::Mutex<HashMap<(String, String), usize>>,
    artifact_writes_by_id: std::sync::Mutex<HashMap<String, usize>>,
    fail_next_task_write: std::sync::atomic::AtomicBool,
    fail_next_queue_write: std::sync::atomic::AtomicBool,
    block_artifact_writes: std::sync::atomic::AtomicBool,
    blocked_status: std::sync::Mutex<Option<TaskStatus>>,
    blocked_timeline_summary: std::sync::Mutex<Option<String>>,
}

#[cfg(test)]
impl TestPersistenceProbe {
    fn before_task_write(&self, task: &TaskRecord) -> Result<()> {
        self.task_writes
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        *self
            .task_writes_by_id
            .lock()
            .expect("test task write count lock")
            .entry(task.id.clone())
            .or_default() += 1;
        *self
            .task_writes_by_status
            .lock()
            .expect("test task status write count lock")
            .entry((task.id.clone(), format!("{:?}", task.status)))
            .or_default() += 1;
        if self
            .fail_next_task_write
            .swap(false, std::sync::atomic::Ordering::SeqCst)
        {
            bail!("injected task persistence failure");
        }
        if self
            .blocked_status
            .lock()
            .expect("test blocked task status lock")
            .is_some_and(|status| status == task.status)
        {
            bail!("injected {:?} task persistence failure", task.status);
        }
        if let Some(summary) = self
            .blocked_timeline_summary
            .lock()
            .expect("test blocked timeline summary lock")
            .as_ref()
            && task.timeline.iter().any(|entry| &entry.summary == summary)
        {
            bail!("injected task event persistence failure");
        }
        Ok(())
    }

    fn before_queue_write(&self) -> Result<()> {
        self.queue_writes
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        if self
            .fail_next_queue_write
            .swap(false, std::sync::atomic::Ordering::SeqCst)
        {
            bail!("injected queue persistence failure");
        }
        Ok(())
    }

    fn before_artifact_write(&self, task_id: &str) -> Result<()> {
        *self
            .artifact_writes_by_id
            .lock()
            .expect("test artifact write count lock")
            .entry(task_id.to_string())
            .or_default() += 1;
        if self
            .block_artifact_writes
            .load(std::sync::atomic::Ordering::SeqCst)
        {
            bail!("injected artifact persistence failure");
        }
        Ok(())
    }

    fn block_status(&self, status: TaskStatus) {
        *self
            .blocked_status
            .lock()
            .expect("test blocked task status lock") = Some(status);
    }

    fn unblock_status(&self) {
        *self
            .blocked_status
            .lock()
            .expect("test blocked task status lock") = None;
    }

    fn block_timeline_summary(&self, summary: impl Into<String>) {
        *self
            .blocked_timeline_summary
            .lock()
            .expect("test blocked timeline summary lock") = Some(summary.into());
    }

    fn unblock_timeline_summary(&self) {
        *self
            .blocked_timeline_summary
            .lock()
            .expect("test blocked timeline summary lock") = None;
    }

    fn task_write_count(&self, task_id: &str) -> usize {
        self.task_writes_by_id
            .lock()
            .expect("test task write count lock")
            .get(task_id)
            .copied()
            .unwrap_or_default()
    }

    fn task_status_write_count(&self, task_id: &str, status: TaskStatus) -> usize {
        self.task_writes_by_status
            .lock()
            .expect("test task status write count lock")
            .get(&(task_id.to_string(), format!("{status:?}")))
            .copied()
            .unwrap_or_default()
    }

    fn artifact_write_count(&self, task_id: &str) -> usize {
        self.artifact_writes_by_id
            .lock()
            .expect("test artifact write count lock")
            .get(task_id)
            .copied()
            .unwrap_or_default()
    }

    fn reset_write_counts(&self) {
        self.task_writes
            .store(0, std::sync::atomic::Ordering::SeqCst);
        self.queue_writes
            .store(0, std::sync::atomic::Ordering::SeqCst);
        self.task_writes_by_id
            .lock()
            .expect("test task write count lock")
            .clear();
        self.task_writes_by_status
            .lock()
            .expect("test task status write count lock")
            .clear();
        self.artifact_writes_by_id
            .lock()
            .expect("test artifact write count lock")
            .clear();
    }
}

const fn default_task_schema_version() -> u32 {
    CURRENT_TASK_SCHEMA_VERSION
}

/// Durable task status.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum TaskStatus {
    Queued,
    Running,
    Completed,
    Failed,
    Canceled,
}

impl TaskStatus {
    #[must_use]
    pub fn is_terminal(self) -> bool {
        matches!(self, Self::Completed | Self::Failed | Self::Canceled)
    }
}

/// Durable tool-call status within a task timeline.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum TaskToolStatus {
    Running,
    Success,
    Failed,
    Canceled,
}

/// Timeline entry for a task execution.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskTimelineEntry {
    pub timestamp: DateTime<Utc>,
    pub kind: String,
    pub summary: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub detail_path: Option<PathBuf>,
}

/// Tool call summary for a task.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskToolCallSummary {
    pub id: String,
    pub name: String,
    pub status: TaskToolStatus,
    pub started_at: DateTime<Utc>,
    pub ended_at: Option<DateTime<Utc>>,
    pub duration_ms: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub input_summary: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub output_summary: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub detail_path: Option<PathBuf>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub patch_ref: Option<PathBuf>,
}

/// Checklist item stored on durable tasks. This is the durable form behind the
/// model-visible checklist/todo compatibility tools.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskChecklistItem {
    pub id: u32,
    pub content: String,
    pub status: String,
}

/// Checklist state associated with a task.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct TaskChecklistState {
    pub items: Vec<TaskChecklistItem>,
    pub completion_pct: u8,
    pub in_progress_id: Option<u32>,
    pub updated_at: Option<DateTime<Utc>>,
}

/// Structured verification evidence attached to a task.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskGateRecord {
    pub id: String,
    pub gate: String,
    pub command: String,
    pub cwd: PathBuf,
    pub exit_code: Option<i32>,
    pub status: String,
    pub classification: String,
    pub duration_ms: u64,
    pub summary: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub log_path: Option<PathBuf>,
    pub recorded_at: DateTime<Utc>,
}

/// PR-attempt metadata and artifacts attached to a task.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskAttemptRecord {
    pub id: String,
    pub attempt_group_id: String,
    pub attempt_index: u32,
    pub attempt_count: u32,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub base_ref: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub base_sha: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub head_ref: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub head_sha: Option<String>,
    pub summary: String,
    pub changed_files: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub patch_path: Option<PathBuf>,
    pub verification: Vec<String>,
    pub selected: bool,
    pub recorded_at: DateTime<Utc>,
}

/// Durable artifact reference produced by task-aware tools.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskArtifactRef {
    pub label: String,
    pub path: PathBuf,
    pub summary: String,
    pub created_at: DateTime<Utc>,
}

/// GitHub write/read evidence attached to a task timeline.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskGithubEvent {
    pub id: String,
    pub action: String,
    pub target: String,
    pub number: u64,
    pub summary: String,
    pub url: Option<String>,
    pub recorded_at: DateTime<Utc>,
}

/// Durable task record.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskRecord {
    #[serde(default = "default_task_schema_version")]
    pub schema_version: u32,
    pub id: String,
    pub prompt: String,
    pub model: String,
    pub workspace: PathBuf,
    pub mode: String,
    pub allow_shell: bool,
    pub trust_mode: bool,
    #[serde(default = "default_auto_approve")]
    pub auto_approve: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub idempotency_key: Option<String>,
    pub status: TaskStatus,
    pub created_at: DateTime<Utc>,
    pub started_at: Option<DateTime<Utc>>,
    pub ended_at: Option<DateTime<Utc>>,
    pub duration_ms: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub result_summary: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub result_detail_path: Option<PathBuf>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub thread_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub turn_id: Option<String>,
    #[serde(default)]
    pub runtime_event_count: usize,
    #[serde(default)]
    pub checklist: TaskChecklistState,
    #[serde(default)]
    pub gates: Vec<TaskGateRecord>,
    #[serde(default)]
    pub attempts: Vec<TaskAttemptRecord>,
    #[serde(default)]
    pub artifacts: Vec<TaskArtifactRef>,
    #[serde(default)]
    pub github_events: Vec<TaskGithubEvent>,
    pub tool_calls: Vec<TaskToolCallSummary>,
    pub timeline: Vec<TaskTimelineEntry>,
}

/// Lightweight task view.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskSummary {
    pub id: String,
    pub status: TaskStatus,
    pub prompt_summary: String,
    pub model: String,
    pub mode: String,
    pub created_at: DateTime<Utc>,
    pub started_at: Option<DateTime<Utc>>,
    pub ended_at: Option<DateTime<Utc>>,
    pub duration_ms: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub thread_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub turn_id: Option<String>,
}

impl From<&TaskRecord> for TaskSummary {
    fn from(value: &TaskRecord) -> Self {
        Self {
            id: value.id.clone(),
            status: value.status,
            prompt_summary: summarize_text(&value.prompt, TIMELINE_SUMMARY_LIMIT),
            model: value.model.clone(),
            mode: value.mode.clone(),
            created_at: value.created_at,
            started_at: value.started_at,
            ended_at: value.ended_at,
            duration_ms: value.duration_ms,
            error: value.error.clone(),
            thread_id: value.thread_id.clone(),
            turn_id: value.turn_id.clone(),
        }
    }
}

/// Count totals by status for task dashboards.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, Default)]
pub struct TaskCounts {
    pub queued: usize,
    pub running: usize,
    pub completed: usize,
    pub failed: usize,
    pub canceled: usize,
}

/// Request to enqueue a new task.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NewTaskRequest {
    pub prompt: String,
    pub model: Option<String>,
    pub workspace: Option<PathBuf>,
    pub mode: Option<String>,
    pub allow_shell: Option<bool>,
    pub trust_mode: Option<bool>,
    pub auto_approve: Option<bool>,
}

impl NewTaskRequest {
    #[cfg(test)]
    #[must_use]
    pub fn from_prompt(prompt: impl Into<String>) -> Self {
        Self {
            prompt: prompt.into(),
            model: None,
            workspace: None,
            mode: None,
            allow_shell: None,
            trust_mode: None,
            auto_approve: Some(true),
        }
    }
}

/// Task manager startup options.
#[derive(Debug, Clone)]
pub struct TaskManagerConfig {
    pub data_dir: PathBuf,
    pub worker_count: usize,
    pub default_workspace: PathBuf,
    pub default_model: String,
    pub default_mode: String,
    pub allow_shell: bool,
    pub trust_mode: bool,
    #[allow(dead_code)]
    pub max_subagents: usize,
}

impl TaskManagerConfig {
    #[must_use]
    pub fn from_runtime(
        config: &Config,
        workspace: PathBuf,
        default_model: Option<String>,
        worker_count: Option<usize>,
    ) -> Self {
        Self {
            data_dir: default_tasks_dir(),
            worker_count: worker_count.unwrap_or(DEFAULT_WORKERS),
            default_workspace: workspace,
            default_model: default_model.unwrap_or_else(|| {
                config
                    .default_text_model
                    .clone()
                    .unwrap_or_else(|| DEFAULT_TEXT_MODEL.to_string())
            }),
            default_mode: "agent".to_string(),
            allow_shell: config.allow_shell(),
            trust_mode: false,
            max_subagents: config
                .max_subagents_for_provider(config.api_provider())
                .clamp(1, MAX_SUBAGENTS),
        }
    }
}

#[derive(Debug, Clone)]
pub struct ExecutionTask {
    id: String,
    prompt: String,
    model: String,
    workspace: PathBuf,
    mode_label: String,
    allow_shell: bool,
    trust_mode: bool,
    auto_approve: bool,
}

impl ExecutionTask {
    #[must_use]
    pub fn id(&self) -> &str {
        &self.id
    }

    #[must_use]
    pub fn prompt(&self) -> &str {
        &self.prompt
    }

    #[must_use]
    pub fn model(&self) -> &str {
        &self.model
    }

    #[must_use]
    pub fn workspace(&self) -> &Path {
        &self.workspace
    }

    #[must_use]
    pub fn mode_label(&self) -> &str {
        &self.mode_label
    }

    #[must_use]
    pub const fn allow_shell(&self) -> bool {
        self.allow_shell
    }

    #[must_use]
    pub const fn trust_mode(&self) -> bool {
        self.trust_mode
    }

    #[must_use]
    pub const fn auto_approve(&self) -> bool {
        self.auto_approve
    }
}

/// Event stream produced by an executor while a task runs.
#[derive(Debug, Clone)]
pub enum TaskExecutionEvent {
    ThreadCreated {
        thread_id: String,
    },
    ThreadLinked {
        thread_id: String,
        turn_id: String,
    },
    Status {
        message: String,
    },
    MessageDelta {
        content: String,
    },
    ToolStarted {
        id: String,
        name: String,
        input: Value,
    },
    ToolProgress {
        id: String,
        output: String,
    },
    ToolCompleted {
        id: String,
        name: String,
        success: bool,
        output: String,
        metadata: Option<Value>,
    },
    Error {
        message: String,
    },
    RuntimeEvent {
        seq: u64,
        event: String,
        summary: String,
    },
}

#[derive(Debug)]
struct PendingTaskExecutionEvent {
    event: TaskExecutionEvent,
    ack: oneshot::Sender<std::result::Result<(), String>>,
}

/// Durable event reporter passed to task executors.
///
/// Every report completes only after the task manager has applied and persisted
/// the event. Executors can therefore order critical runtime work after the
/// durable acknowledgement instead of relying on fire-and-forget delivery.
#[derive(Clone)]
pub struct TaskExecutionReporter {
    tx: mpsc::UnboundedSender<PendingTaskExecutionEvent>,
    cancel: CancellationToken,
}

impl std::fmt::Debug for TaskExecutionReporter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TaskExecutionReporter")
            .finish_non_exhaustive()
    }
}

impl TaskExecutionReporter {
    fn new(
        tx: mpsc::UnboundedSender<PendingTaskExecutionEvent>,
        cancel: CancellationToken,
    ) -> Self {
        Self { tx, cancel }
    }

    /// Report an executor event and wait for its durable TaskManager ack.
    pub async fn report(
        &self,
        event: TaskExecutionEvent,
    ) -> std::result::Result<(), TaskExecutionReportError> {
        let (ack_tx, ack_rx) = oneshot::channel();
        let outcome = match self
            .tx
            .send(PendingTaskExecutionEvent { event, ack: ack_tx })
        {
            Ok(()) => match ack_rx.await {
                Ok(Ok(())) => Ok(()),
                Ok(Err(message)) => Err(TaskExecutionReportError::Rejected(message)),
                Err(_) => Err(TaskExecutionReportError::AcknowledgementDropped),
            },
            Err(_) => Err(TaskExecutionReportError::Closed),
        };
        if outcome.is_err() {
            self.cancel.cancel();
        }
        outcome
    }
}

/// Failure to durably report an executor event.
#[derive(Debug, Clone, Error, PartialEq, Eq)]
pub enum TaskExecutionReportError {
    #[error("task execution reporter is closed")]
    Closed,
    #[error("task execution event acknowledgement was dropped")]
    AcknowledgementDropped,
    #[error("task execution event persistence failed: {0}")]
    Rejected(String),
}

/// Final executor result.
#[derive(Debug, Clone)]
pub struct TaskExecutionResult {
    pub status: TaskStatus,
    pub result_text: Option<String>,
    pub error: Option<String>,
}

impl TaskExecutionResult {
    fn reporting_failed(error: TaskExecutionReportError, captured_text: &str) -> Self {
        Self {
            status: TaskStatus::Failed,
            result_text: (!captured_text.trim().is_empty()).then(|| captured_text.to_string()),
            error: Some(format!("Failed to persist task execution event: {error}")),
        }
    }
}

/// Abstraction for task execution.
#[async_trait]
pub trait TaskExecutor: Send + Sync {
    async fn execute(
        &self,
        task: ExecutionTask,
        reporter: TaskExecutionReporter,
        cancel: CancellationToken,
    ) -> TaskExecutionResult;
}

#[async_trait]
trait TaskTurnRuntime: Send + Sync {
    fn events_since(
        &self,
        thread_id: &str,
        since_seq: Option<u64>,
    ) -> Result<Vec<crate::runtime_threads::RuntimeEventRecord>>;

    async fn interrupt_turn(&self, thread_id: &str, turn_id: &str) -> Result<()>;
}

#[async_trait]
impl TaskTurnRuntime for RuntimeThreadManager {
    fn events_since(
        &self,
        thread_id: &str,
        since_seq: Option<u64>,
    ) -> Result<Vec<crate::runtime_threads::RuntimeEventRecord>> {
        RuntimeThreadManager::events_since(self, thread_id, since_seq)
    }

    async fn interrupt_turn(&self, thread_id: &str, turn_id: &str) -> Result<()> {
        RuntimeThreadManager::interrupt_turn(self, thread_id, turn_id)
            .await
            .map(|_| ())
    }
}

enum ActiveTurnExit {
    Terminal(TaskExecutionResult),
    Abort(TaskExecutionResult),
}

async fn interrupt_active_turn_once(
    runtime: &dyn TaskTurnRuntime,
    thread_id: &str,
    turn_id: &str,
    interrupt_requested: &mut bool,
) {
    if *interrupt_requested {
        return;
    }
    *interrupt_requested = true;
    if let Err(err) = runtime.interrupt_turn(thread_id, turn_id).await {
        tracing::warn!("Failed to interrupt runtime thread {thread_id} turn {turn_id}: {err}");
    }
}

async fn drive_active_turn(
    runtime: &dyn TaskTurnRuntime,
    task_id: &str,
    thread_id: &str,
    turn_id: &str,
    reporter: &TaskExecutionReporter,
    cancel: &CancellationToken,
) -> TaskExecutionResult {
    let mut interrupt_requested = false;
    let exit = drive_active_turn_inner(
        runtime,
        task_id,
        thread_id,
        turn_id,
        reporter,
        cancel,
        &mut interrupt_requested,
    )
    .await;
    match exit {
        ActiveTurnExit::Terminal(result) => result,
        ActiveTurnExit::Abort(result) => {
            interrupt_active_turn_once(runtime, thread_id, turn_id, &mut interrupt_requested).await;
            result
        }
    }
}

async fn drive_active_turn_inner(
    runtime: &dyn TaskTurnRuntime,
    task_id: &str,
    thread_id: &str,
    turn_id: &str,
    reporter: &TaskExecutionReporter,
    cancel: &CancellationToken,
    interrupt_requested: &mut bool,
) -> ActiveTurnExit {
    if let Err(err) = reporter
        .report(TaskExecutionEvent::ThreadLinked {
            thread_id: thread_id.to_string(),
            turn_id: turn_id.to_string(),
        })
        .await
    {
        return ActiveTurnExit::Abort(TaskExecutionResult::reporting_failed(err, ""));
    }
    if let Err(err) = reporter
        .report(TaskExecutionEvent::Status {
            message: format!("Task {task_id} started"),
        })
        .await
    {
        return ActiveTurnExit::Abort(TaskExecutionResult::reporting_failed(err, ""));
    }

    let mut final_text = String::new();
    let mut seen_seq = 0u64;
    let mut terminal_status: Option<RuntimeTurnStatus> = None;
    let mut terminal_error: Option<String> = None;

    loop {
        if cancel.is_cancelled() && !*interrupt_requested {
            interrupt_active_turn_once(runtime, thread_id, turn_id, interrupt_requested).await;
            if let Err(err) = reporter
                .report(TaskExecutionEvent::Status {
                    message: "Cancellation requested".to_string(),
                })
                .await
            {
                return ActiveTurnExit::Abort(TaskExecutionResult::reporting_failed(
                    err,
                    &final_text,
                ));
            }
        }

        let batch = match runtime.events_since(thread_id, Some(seen_seq)) {
            Ok(batch) => batch,
            Err(err) => {
                return ActiveTurnExit::Abort(TaskExecutionResult {
                    status: TaskStatus::Failed,
                    result_text: if final_text.trim().is_empty() {
                        None
                    } else {
                        Some(final_text)
                    },
                    error: Some(format!("Failed to read runtime events: {err}")),
                });
            }
        };

        for event in batch {
            seen_seq = seen_seq.max(event.seq);
            if let Err(err) = reporter
                .report(TaskExecutionEvent::RuntimeEvent {
                    seq: event.seq,
                    event: event.event.clone(),
                    summary: summarize_text(&event.payload.to_string(), TIMELINE_SUMMARY_LIMIT),
                })
                .await
            {
                return ActiveTurnExit::Abort(TaskExecutionResult::reporting_failed(
                    err,
                    &final_text,
                ));
            }

            match event.event.as_str() {
                "item.delta" => {
                    let kind = event
                        .payload
                        .get("kind")
                        .and_then(Value::as_str)
                        .unwrap_or_default();
                    if kind == "agent_message" {
                        if let Some(content) = event.payload.get("delta").and_then(Value::as_str) {
                            final_text.push_str(content);
                            if let Err(err) = reporter
                                .report(TaskExecutionEvent::MessageDelta {
                                    content: content.to_string(),
                                })
                                .await
                            {
                                return ActiveTurnExit::Abort(
                                    TaskExecutionResult::reporting_failed(err, &final_text),
                                );
                            }
                        }
                    } else if kind == "tool_call" {
                        let output = event
                            .payload
                            .get("delta")
                            .and_then(Value::as_str)
                            .unwrap_or_default()
                            .to_string();
                        if let Err(err) = reporter
                            .report(TaskExecutionEvent::ToolProgress {
                                id: event.item_id.clone().unwrap_or_default(),
                                output,
                            })
                            .await
                        {
                            return ActiveTurnExit::Abort(TaskExecutionResult::reporting_failed(
                                err,
                                &final_text,
                            ));
                        }
                    }
                }
                "item.started" => {
                    if let Some(tool) = event.payload.get("tool") {
                        let id = tool
                            .get("id")
                            .and_then(Value::as_str)
                            .unwrap_or_default()
                            .to_string();
                        let name = tool
                            .get("name")
                            .and_then(Value::as_str)
                            .unwrap_or_default()
                            .to_string();
                        let input = tool.get("input").cloned().unwrap_or_else(|| json!({}));
                        if let Err(err) = reporter
                            .report(TaskExecutionEvent::ToolStarted { id, name, input })
                            .await
                        {
                            return ActiveTurnExit::Abort(TaskExecutionResult::reporting_failed(
                                err,
                                &final_text,
                            ));
                        }
                    }
                }
                "item.completed" | "item.failed" => {
                    if let Some(item) = event.payload.get("item") {
                        let kind = item.get("kind").and_then(Value::as_str).unwrap_or_default();
                        if kind == "tool_call"
                            || kind == "file_change"
                            || kind == "command_execution"
                        {
                            let id = item
                                .get("id")
                                .and_then(Value::as_str)
                                .unwrap_or_default()
                                .to_string();
                            let name = item
                                .get("summary")
                                .and_then(Value::as_str)
                                .unwrap_or("tool")
                                .split(':')
                                .next()
                                .unwrap_or("tool")
                                .trim()
                                .to_string();
                            let output = item
                                .get("detail")
                                .and_then(Value::as_str)
                                .unwrap_or_default()
                                .to_string();
                            let metadata = item.get("metadata").cloned();
                            if let Err(err) = reporter
                                .report(TaskExecutionEvent::ToolCompleted {
                                    id,
                                    name,
                                    success: event.event == "item.completed",
                                    output,
                                    metadata,
                                })
                                .await
                            {
                                return ActiveTurnExit::Abort(
                                    TaskExecutionResult::reporting_failed(err, &final_text),
                                );
                            }
                        } else if kind == "status" {
                            let message = item
                                .get("detail")
                                .and_then(Value::as_str)
                                .or_else(|| item.get("summary").and_then(Value::as_str))
                                .unwrap_or_default()
                                .to_string();
                            if let Err(err) = reporter
                                .report(TaskExecutionEvent::Status { message })
                                .await
                            {
                                return ActiveTurnExit::Abort(
                                    TaskExecutionResult::reporting_failed(err, &final_text),
                                );
                            }
                        } else if kind == "error" {
                            let message = item
                                .get("detail")
                                .and_then(Value::as_str)
                                .or_else(|| item.get("summary").and_then(Value::as_str))
                                .unwrap_or_default()
                                .to_string();
                            if let Err(err) =
                                reporter.report(TaskExecutionEvent::Error { message }).await
                            {
                                return ActiveTurnExit::Abort(
                                    TaskExecutionResult::reporting_failed(err, &final_text),
                                );
                            }
                        }
                    }
                }
                "turn.completed" => {
                    if let Some(turn_payload) = event.payload.get("turn") {
                        let status = turn_payload
                            .get("status")
                            .and_then(Value::as_str)
                            .unwrap_or("failed");
                        terminal_status = Some(match status {
                            "completed" => RuntimeTurnStatus::Completed,
                            "interrupted" => RuntimeTurnStatus::Interrupted,
                            "canceled" => RuntimeTurnStatus::Canceled,
                            _ => RuntimeTurnStatus::Failed,
                        });
                        terminal_error = turn_payload
                            .get("error")
                            .and_then(Value::as_str)
                            .map(ToString::to_string);
                    } else {
                        terminal_status = Some(RuntimeTurnStatus::Completed);
                    }
                }
                _ => {}
            }
        }

        if terminal_status.is_some() {
            break;
        }

        sleep(Duration::from_millis(40)).await;
    }

    let result = match terminal_status.unwrap_or(RuntimeTurnStatus::Failed) {
        RuntimeTurnStatus::Completed => TaskExecutionResult {
            status: TaskStatus::Completed,
            result_text: if final_text.trim().is_empty() {
                None
            } else {
                Some(final_text)
            },
            error: None,
        },
        RuntimeTurnStatus::Interrupted | RuntimeTurnStatus::Canceled => TaskExecutionResult {
            status: TaskStatus::Canceled,
            result_text: if final_text.trim().is_empty() {
                None
            } else {
                Some(final_text)
            },
            error: None,
        },
        RuntimeTurnStatus::Queued | RuntimeTurnStatus::InProgress | RuntimeTurnStatus::Failed => {
            TaskExecutionResult {
                status: TaskStatus::Failed,
                result_text: if final_text.trim().is_empty() {
                    None
                } else {
                    Some(final_text)
                },
                error: terminal_error.or_else(|| Some("Task ended unexpectedly".to_string())),
            }
        }
    };
    ActiveTurnExit::Terminal(result)
}

/// Engine-backed executor (DeepSeek-only).
pub struct EngineTaskExecutor {
    runtime_threads: SharedRuntimeThreadManager,
}

impl EngineTaskExecutor {
    #[must_use]
    pub fn new(runtime_threads: SharedRuntimeThreadManager) -> Self {
        Self { runtime_threads }
    }
}

#[async_trait]
impl TaskExecutor for EngineTaskExecutor {
    async fn execute(
        &self,
        task: ExecutionTask,
        reporter: TaskExecutionReporter,
        cancel: CancellationToken,
    ) -> TaskExecutionResult {
        let thread = match self
            .runtime_threads
            .create_thread(CreateThreadRequest {
                model: Some(task.model.clone()),
                workspace: Some(task.workspace.clone()),
                mode: Some(task.mode_label.clone()),
                allow_shell: Some(task.allow_shell),
                trust_mode: Some(task.trust_mode),
                auto_approve: Some(task.auto_approve),
                archived: false,
                system_prompt: None,
                task_id: Some(task.id.clone()),
                ..Default::default()
            })
            .await
        {
            Ok(thread) => thread,
            Err(err) => {
                return TaskExecutionResult {
                    status: TaskStatus::Failed,
                    result_text: None,
                    error: Some(format!("Failed to create runtime thread: {err}")),
                };
            }
        };

        if let Err(err) = reporter
            .report(TaskExecutionEvent::ThreadCreated {
                thread_id: thread.id.clone(),
            })
            .await
        {
            return TaskExecutionResult::reporting_failed(err, "");
        }

        let turn = match self
            .runtime_threads
            .start_turn(
                &thread.id,
                StartTurnRequest {
                    prompt: task.prompt.clone(),
                    input_summary: Some(summarize_text(&task.prompt, TIMELINE_SUMMARY_LIMIT)),
                    model: Some(task.model.clone()),
                    mode: Some(task.mode_label.clone()),
                    allow_shell: Some(task.allow_shell),
                    trust_mode: Some(task.trust_mode),
                    auto_approve: Some(task.auto_approve),
                    ..Default::default()
                },
            )
            .await
        {
            Ok(turn) => turn,
            Err(err) => {
                return TaskExecutionResult {
                    status: TaskStatus::Failed,
                    result_text: None,
                    error: Some(format!("Failed to start task: {err}")),
                };
            }
        };

        drive_active_turn(
            self.runtime_threads.as_ref(),
            &task.id,
            &thread.id,
            &turn.id,
            &reporter,
            &cancel,
        )
        .await
    }
}

/// Thread-safe task manager.
pub type SharedTaskManager = Arc<TaskManager>;

pub struct TaskManager {
    cfg: TaskManagerConfig,
    default_workspace: Mutex<PathBuf>,
    executor: Arc<dyn TaskExecutor>,
    task_id_generator: TaskIdGenerator,
    tasks_dir: PathBuf,
    artifacts_dir: PathBuf,
    queue_path: PathBuf,
    state: Mutex<ManagerState>,
    #[cfg(test)]
    persistence_probe: std::sync::Mutex<Option<Arc<TestPersistenceProbe>>>,
    notify: Notify,
    cancel_token: CancellationToken,
}

struct ManagerState {
    tasks: HashMap<String, TaskRecord>,
    queue: VecDeque<String>,
    idempotency_index: HashMap<String, String>,
    running_cancel: HashMap<String, CancellationToken>,
}

enum WorkerStep {
    Wait,
    Retry,
    Execute(String, ExecutionTask, CancellationToken),
}

#[derive(Debug, Serialize, Deserialize, Default)]
struct QueueFile {
    queue: Vec<String>,
}

impl TaskManager {
    /// Start the manager with the default DeepSeek executor.
    pub async fn start(cfg: TaskManagerConfig, api_config: Config) -> Result<SharedTaskManager> {
        let runtime_threads = Arc::new(RuntimeThreadManager::open(
            api_config.clone(),
            cfg.default_workspace.clone(),
            RuntimeThreadManagerConfig::from_task_data_dir(cfg.data_dir.clone()),
        )?);
        Self::start_with_runtime_manager(cfg, api_config, runtime_threads).await
    }

    /// Start the manager with an injected runtime thread manager.
    pub async fn start_with_runtime_manager(
        cfg: TaskManagerConfig,
        _api_config: Config,
        runtime_threads: SharedRuntimeThreadManager,
    ) -> Result<SharedTaskManager> {
        let executor: Arc<dyn TaskExecutor> =
            Arc::new(EngineTaskExecutor::new(runtime_threads.clone()));
        let manager = Self::start_with_executor(cfg, executor).await?;
        runtime_threads.attach_task_manager(manager.clone());
        Ok(manager)
    }

    /// Start the manager with a custom executor (used for tests).
    pub async fn start_with_executor(
        cfg: TaskManagerConfig,
        executor: Arc<dyn TaskExecutor>,
    ) -> Result<SharedTaskManager> {
        Self::start_with_executor_inner(
            cfg,
            executor,
            Arc::new(|| format!("task_{}", Uuid::new_v4())),
        )
        .await
    }

    #[cfg(test)]
    async fn start_with_executor_and_id_generator(
        cfg: TaskManagerConfig,
        executor: Arc<dyn TaskExecutor>,
        task_id_generator: TaskIdGenerator,
    ) -> Result<SharedTaskManager> {
        Self::start_with_executor_inner(cfg, executor, task_id_generator).await
    }

    async fn start_with_executor_inner(
        cfg: TaskManagerConfig,
        executor: Arc<dyn TaskExecutor>,
        task_id_generator: TaskIdGenerator,
    ) -> Result<SharedTaskManager> {
        let workers = cfg.worker_count.clamp(1, MAX_WORKERS);
        let tasks_dir = cfg.data_dir.join("tasks");
        let artifacts_dir = cfg.data_dir.join("artifacts");
        let queue_path = cfg.data_dir.join("queue.json");
        fs::create_dir_all(&tasks_dir)
            .with_context(|| format!("Failed to create tasks dir {}", tasks_dir.display()))?;
        fs::create_dir_all(&artifacts_dir).with_context(|| {
            format!(
                "Failed to create task artifacts dir {}",
                artifacts_dir.display()
            )
        })?;

        recover_pending_task_prunes(&tasks_dir, &artifacts_dir)?;

        let (tasks, queue) = load_state(&tasks_dir, &queue_path)?;
        let idempotency_index = build_idempotency_index(&tasks)?;

        let cancel_token = CancellationToken::new();
        let default_workspace = cfg.default_workspace.clone();
        let manager = Arc::new(Self {
            cfg,
            default_workspace: Mutex::new(default_workspace),
            executor,
            task_id_generator,
            tasks_dir,
            artifacts_dir,
            queue_path,
            state: Mutex::new(ManagerState {
                tasks,
                queue,
                idempotency_index,
                running_cancel: HashMap::new(),
            }),
            #[cfg(test)]
            persistence_probe: std::sync::Mutex::new(None),
            notify: Notify::new(),
            cancel_token: cancel_token.clone(),
        });

        {
            let state = manager.state.lock().await;
            if let Err(err) = manager.persist_queue_locked(&state.queue) {
                tracing::warn!("Failed to refresh task queue cache during startup: {err}");
            }
        }

        for _ in 0..workers {
            let manager_clone = Arc::clone(&manager);
            spawn_supervised(
                "task-manager-worker",
                std::panic::Location::caller(),
                async move {
                    manager_clone.worker_loop().await;
                },
            );
        }

        Ok(manager)
    }

    #[allow(dead_code)] // Public API for external callers (runtime API)
    pub fn shutdown(&self) {
        self.cancel_token.cancel();
    }

    #[cfg(test)]
    fn install_persistence_probe(&self, probe: Arc<TestPersistenceProbe>) {
        *self
            .persistence_probe
            .lock()
            .expect("test persistence probe lock") = Some(probe);
    }

    #[allow(dead_code)] // Public API for external callers
    pub fn is_shutdown(&self) -> bool {
        self.cancel_token.is_cancelled()
    }

    pub async fn set_default_workspace(&self, workspace: PathBuf) {
        let mut default_workspace = self.default_workspace.lock().await;
        *default_workspace = workspace;
    }

    pub async fn default_workspace(&self) -> PathBuf {
        self.default_workspace.lock().await.clone()
    }

    /// Enqueue a new task.
    pub async fn add_task(&self, req: NewTaskRequest) -> Result<TaskRecord> {
        self.add_task_inner(req, None).await
    }

    /// Enqueue a task once for a caller-supplied durable idempotency key.
    ///
    /// A repeated key returns the existing durable task, including after the
    /// manager has reopened its on-disk state.
    pub async fn add_task_with_idempotency_key(
        &self,
        req: NewTaskRequest,
        idempotency_key: impl Into<String>,
    ) -> Result<TaskRecord> {
        let idempotency_key = normalize_idempotency_key(idempotency_key.into())?;
        self.add_task_inner(req, Some(idempotency_key)).await
    }

    async fn add_task_inner(
        &self,
        req: NewTaskRequest,
        idempotency_key: Option<String>,
    ) -> Result<TaskRecord> {
        let prompt = req.prompt.trim().to_string();
        if prompt.is_empty() {
            bail!("Task prompt cannot be empty");
        }
        let mode = normalize_task_mode(req.mode.unwrap_or_else(|| self.cfg.default_mode.clone()))?;
        let model = req.model.unwrap_or_else(|| self.cfg.default_model.clone());
        let workspace = match req.workspace {
            Some(workspace) => workspace,
            None => self.default_workspace().await,
        };
        let allow_shell = req.allow_shell.unwrap_or(self.cfg.allow_shell);
        let trust_mode = req.trust_mode.unwrap_or(self.cfg.trust_mode);
        // Auto-approval must be opted into explicitly
        // (GHSA-72w5-pf8h-xfp4).
        let auto_approve = req.auto_approve.unwrap_or(false);

        let mut state = self.state.lock().await;
        if let Some(key) = idempotency_key.as_deref()
            && let Some(existing_id) = state.idempotency_index.get(key)
        {
            let existing = state
                .tasks
                .get(existing_id)
                .ok_or_else(|| {
                    anyhow!("Task idempotency index points to missing task {existing_id}")
                })?
                .clone();
            let should_notify = existing.status == TaskStatus::Queued;
            drop(state);
            if should_notify {
                self.notify.notify_one();
            }
            return Ok(existing);
        }

        let id = (0..MAX_TASK_ID_GENERATION_ATTEMPTS)
            .find_map(|_| {
                let candidate = (self.task_id_generator)();
                (!state.tasks.contains_key(&candidate)).then_some(candidate)
            })
            .ok_or_else(|| anyhow!("Failed to generate a unique task id"))?;
        ensure_safe_storage_id("task id", &id)?;
        let now = Utc::now();
        let task = TaskRecord {
            schema_version: CURRENT_TASK_SCHEMA_VERSION,
            id,
            prompt,
            model,
            workspace,
            mode,
            allow_shell,
            trust_mode,
            auto_approve,
            idempotency_key: idempotency_key.clone(),
            status: TaskStatus::Queued,
            created_at: now,
            started_at: None,
            ended_at: None,
            duration_ms: None,
            result_summary: None,
            result_detail_path: None,
            error: None,
            thread_id: None,
            turn_id: None,
            runtime_event_count: 0,
            checklist: TaskChecklistState::default(),
            gates: Vec::new(),
            attempts: Vec::new(),
            artifacts: Vec::new(),
            github_events: Vec::new(),
            tool_calls: Vec::new(),
            timeline: vec![TaskTimelineEntry {
                timestamp: now,
                kind: "queued".to_string(),
                summary: "Task queued".to_string(),
                detail_path: None,
            }],
        };

        self.persist_task_locked(&task)?;
        state.tasks.insert(task.id.clone(), task.clone());
        state.queue.push_back(task.id.clone());
        if let Some(key) = idempotency_key {
            state.idempotency_index.insert(key, task.id.clone());
        }
        if let Err(err) = self.persist_queue_locked(&state.queue) {
            tracing::warn!(
                "Failed to persist task queue cache after adding {}: {err}",
                task.id
            );
        }
        drop(state);
        self.notify.notify_one();
        Ok(task)
    }

    /// List tasks, newest first.
    pub async fn list_tasks(&self, limit: Option<usize>) -> Vec<TaskSummary> {
        let state = self.state.lock().await;
        let mut items = state
            .tasks
            .values()
            .map(TaskSummary::from)
            .collect::<Vec<_>>();
        items.sort_by_key(|i| std::cmp::Reverse(i.created_at));
        if let Some(limit) = limit {
            items.truncate(limit);
        }
        items
    }

    /// Return the in-memory task count without cloning or sorting records.
    pub async fn task_count(&self) -> usize {
        self.state.lock().await.tasks.len()
    }

    /// Permanently remove unprotected terminal tasks and their artifacts.
    ///
    /// Queued and running tasks are never candidates. Callers must include
    /// task ids referenced by any external durable journal (for example an
    /// automation pending-enqueue record) in `protected_task_ids`.
    pub async fn prune_terminal_tasks(
        &self,
        protected_task_ids: &HashSet<String>,
    ) -> Result<Vec<String>> {
        let mut state = self.state.lock().await;
        let mut candidates = state
            .tasks
            .values()
            .filter(|task| {
                task.status.is_terminal() && !protected_task_ids.contains(task.id.as_str())
            })
            .map(|task| task.id.clone())
            .collect::<Vec<_>>();
        candidates.sort();

        let mut pruned = Vec::with_capacity(candidates.len());
        for task_id in candidates {
            ensure_safe_storage_id("task id", &task_id)?;
            let marker = persist_task_prune_marker(&self.tasks_dir, &task_id)?;
            let artifact_dir = self.artifacts_dir.join(&task_id);
            remove_dir_all_if_exists(&artifact_dir).with_context(|| {
                format!("Failed to prune task artifacts {}", artifact_dir.display())
            })?;
            let task_path = self.tasks_dir.join(format!("{task_id}.json"));
            remove_file_if_exists(&task_path)
                .with_context(|| format!("Failed to prune task file {}", task_path.display()))?;

            state.tasks.remove(&task_id);
            state.queue.retain(|queued_id| queued_id != &task_id);
            state.running_cancel.remove(&task_id);
            state
                .idempotency_index
                .retain(|_, indexed_task_id| indexed_task_id != &task_id);
            pruned.push(task_id);

            if let Err(err) = remove_file_if_exists(&marker) {
                tracing::warn!(path = %marker.display(), error = %err, "failed to clear completed task prune marker");
            }
        }

        if !pruned.is_empty()
            && let Err(err) = self.persist_queue_locked(&state.queue)
        {
            tracing::warn!(error = %err, "failed to refresh queue cache after task pruning");
        }
        Ok(pruned)
    }

    /// Retrieve a task by full id or prefix.
    pub async fn get_task(&self, id_or_prefix: &str) -> Result<TaskRecord> {
        let state = self.state.lock().await;
        let id = resolve_task_id(&state.tasks, id_or_prefix)?;
        state
            .tasks
            .get(&id)
            .cloned()
            .ok_or_else(|| anyhow!("Task not found: {id_or_prefix}"))
    }

    /// Cancel a queued or running task by id/prefix.
    pub async fn cancel_task(&self, id_or_prefix: &str) -> Result<TaskRecord> {
        let mut state = self.state.lock().await;
        let id = resolve_task_id(&state.tasks, id_or_prefix)?;
        let mut candidate = state
            .tasks
            .get(&id)
            .cloned()
            .ok_or_else(|| anyhow!("Task not found: {id}"))?;
        let now = Utc::now();

        match candidate.status {
            TaskStatus::Queued => {
                candidate.status = TaskStatus::Canceled;
                candidate.ended_at = Some(now);
                candidate.duration_ms = Some(0);
                candidate.timeline.push(TaskTimelineEntry {
                    timestamp: now,
                    kind: "canceled".to_string(),
                    summary: "Task canceled before execution".to_string(),
                    detail_path: None,
                });
                let mut next_queue = state.queue.clone();
                next_queue.retain(|queued_id| queued_id != &id);
                self.persist_task_locked(&candidate)?;
                state.tasks.insert(id.clone(), candidate.clone());
                state.queue = next_queue;
                if let Err(err) = self.persist_queue_locked(&state.queue) {
                    tracing::warn!(
                        "Failed to persist task queue cache after canceling {id}: {err}"
                    );
                }
            }
            TaskStatus::Running => {
                candidate.timeline.push(TaskTimelineEntry {
                    timestamp: now,
                    kind: "cancel_requested".to_string(),
                    summary: "Cancellation requested".to_string(),
                    detail_path: None,
                });
                self.persist_task_locked(&candidate)?;
                state.tasks.insert(id.clone(), candidate.clone());
                if let Some(token) = state.running_cancel.get(&id) {
                    token.cancel();
                }
            }
            TaskStatus::Completed | TaskStatus::Failed | TaskStatus::Canceled => {}
        }

        Ok(candidate)
    }

    /// Return aggregate status counters.
    pub async fn counts(&self) -> TaskCounts {
        let state = self.state.lock().await;
        let mut counts = TaskCounts::default();
        for task in state.tasks.values() {
            match task.status {
                TaskStatus::Queued => counts.queued += 1,
                TaskStatus::Running => counts.running += 1,
                TaskStatus::Completed => counts.completed += 1,
                TaskStatus::Failed => counts.failed += 1,
                TaskStatus::Canceled => counts.canceled += 1,
            }
        }
        counts
    }

    /// Root directory for durable task state.
    #[must_use]
    pub fn data_dir(&self) -> PathBuf {
        self.cfg.data_dir.clone()
    }

    /// Resolve a task artifact reference to an absolute path.
    #[must_use]
    pub fn artifact_absolute_path(&self, path: &Path) -> PathBuf {
        if path.is_absolute() {
            path.to_path_buf()
        } else {
            self.cfg.data_dir.join(path)
        }
    }

    /// Write a durable task artifact and return the persisted path reference.
    pub fn write_task_artifact(
        &self,
        task_id: &str,
        label: &str,
        content: &str,
    ) -> Result<PathBuf> {
        self.write_artifact(task_id, label, content)
    }

    /// Apply model-visible tool metadata to a task and persist it.
    pub async fn record_tool_metadata(
        &self,
        id_or_prefix: &str,
        metadata: &Value,
    ) -> Result<TaskRecord> {
        let mut state = self.state.lock().await;
        let id = resolve_task_id(&state.tasks, id_or_prefix)?;
        let mut updated = state
            .tasks
            .get(&id)
            .cloned()
            .ok_or_else(|| anyhow!("Task not found: {id}"))?;
        self.apply_task_update_metadata(&mut updated, Some(metadata))?;
        self.persist_task_locked(&updated)?;
        state.tasks.insert(id, updated.clone());
        Ok(updated)
    }

    async fn worker_loop(self: Arc<Self>) {
        loop {
            if self.cancel_token.is_cancelled() {
                tracing::debug!("Worker exiting due to shutdown");
                break;
            }
            let step = {
                let mut state = self.state.lock().await;
                loop {
                    let Some(task_id) = state.queue.front().cloned() else {
                        break WorkerStep::Wait;
                    };
                    let Some(mut candidate) = state.tasks.get(&task_id).cloned() else {
                        state.queue.pop_front();
                        if let Err(err) = self.persist_queue_locked(&state.queue) {
                            tracing::warn!("Failed to refresh task queue cache: {err}");
                        }
                        continue;
                    };
                    if candidate.status != TaskStatus::Queued {
                        state.queue.pop_front();
                        if let Err(err) = self.persist_queue_locked(&state.queue) {
                            tracing::warn!("Failed to refresh task queue cache: {err}");
                        }
                        continue;
                    }

                    let now = Utc::now();
                    candidate.status = TaskStatus::Running;
                    candidate.started_at = Some(now);
                    candidate.ended_at = None;
                    candidate.duration_ms = None;
                    candidate.error = None;
                    candidate.timeline.push(TaskTimelineEntry {
                        timestamp: now,
                        kind: "running".to_string(),
                        summary: "Task started".to_string(),
                        detail_path: None,
                    });
                    let request = ExecutionTask {
                        id: candidate.id.clone(),
                        prompt: candidate.prompt.clone(),
                        model: candidate.model.clone(),
                        workspace: candidate.workspace.clone(),
                        mode_label: candidate.mode.clone(),
                        allow_shell: candidate.allow_shell,
                        trust_mode: candidate.trust_mode,
                        auto_approve: candidate.auto_approve,
                    };
                    if let Err(err) = self.persist_task_locked(&candidate) {
                        tracing::error!("Failed to persist task start for {task_id}: {err}");
                        break WorkerStep::Retry;
                    }

                    let cancel = self.cancel_token.child_token();
                    state.tasks.insert(task_id.clone(), candidate);
                    let popped = state.queue.pop_front();
                    debug_assert_eq!(popped.as_deref(), Some(task_id.as_str()));
                    state.running_cancel.insert(task_id.clone(), cancel.clone());
                    if let Err(err) = self.persist_queue_locked(&state.queue) {
                        tracing::warn!(
                            "Failed to persist task queue cache after starting {task_id}: {err}"
                        );
                    }
                    break WorkerStep::Execute(task_id, request, cancel);
                }
            };

            match step {
                WorkerStep::Execute(task_id, request, cancel) => {
                    self.run_task(task_id, request, cancel).await;
                }
                WorkerStep::Retry => {
                    tokio::select! {
                        _ = self.cancel_token.cancelled() => {
                            tracing::debug!("Worker exiting during persistence backoff");
                            break;
                        }
                        _ = sleep(TASK_PERSIST_RETRY_BACKOFF) => {}
                    }
                }
                WorkerStep::Wait => {
                    tokio::select! {
                        _ = self.cancel_token.cancelled() => {
                            tracing::debug!("Worker exiting during wait");
                            break;
                        }
                        _ = self.notify.notified() => {}
                    }
                }
            }
        }
    }

    async fn run_task(&self, task_id: String, request: ExecutionTask, cancel: CancellationToken) {
        let (event_tx, mut event_rx) = mpsc::unbounded_channel();
        let reporter = TaskExecutionReporter::new(event_tx, cancel.clone());
        let exec_fut = self
            .executor
            .execute(request.clone(), reporter, cancel.clone());
        tokio::pin!(exec_fut);

        let result = loop {
            tokio::select! {
                maybe_report = event_rx.recv() => {
                    if let Some(report) = maybe_report {
                        self.apply_reported_event(&task_id, report).await;
                    }
                }
                exec_result = &mut exec_fut => {
                    break exec_result;
                }
            }
        };

        while let Ok(report) = event_rx.try_recv() {
            self.apply_reported_event(&task_id, report).await;
        }

        if let Err(err) = self
            .finish_task(&task_id, result, cancel, &request.mode_label)
            .await
        {
            tracing::error!("Failed to finalize task {task_id}: {err}");
        }
    }

    async fn apply_reported_event(&self, task_id: &str, report: PendingTaskExecutionEvent) {
        let outcome = self
            .apply_execution_event(task_id, report.event)
            .await
            .map_err(|err| err.to_string());
        if let Err(err) = &outcome {
            tracing::error!("Failed to durably apply task event for {task_id}: {err}");
        }
        if report.ack.send(outcome).is_err() {
            tracing::debug!("Task executor dropped event ack receiver for {task_id}");
        }
    }

    async fn apply_execution_event(&self, task_id: &str, event: TaskExecutionEvent) -> Result<()> {
        let mut state = self.state.lock().await;
        let Some(mut task) = state.tasks.get(task_id).cloned() else {
            bail!("Task not found while applying executor event: {task_id}");
        };

        match event {
            TaskExecutionEvent::ThreadCreated { thread_id } => {
                task.thread_id = Some(thread_id.clone());
                task.timeline.push(TaskTimelineEntry {
                    timestamp: Utc::now(),
                    kind: "runtime_thread".to_string(),
                    summary: format!("Linked runtime thread {thread_id}"),
                    detail_path: None,
                });
            }
            TaskExecutionEvent::ThreadLinked { thread_id, turn_id } => {
                task.thread_id = Some(thread_id.clone());
                task.turn_id = Some(turn_id.clone());
                task.timeline.push(TaskTimelineEntry {
                    timestamp: Utc::now(),
                    kind: "runtime_link".to_string(),
                    summary: format!("Linked runtime thread {thread_id} turn {turn_id}"),
                    detail_path: None,
                });
            }
            TaskExecutionEvent::Status { message } => {
                task.timeline.push(TaskTimelineEntry {
                    timestamp: Utc::now(),
                    kind: "status".to_string(),
                    summary: summarize_text(&message, TIMELINE_SUMMARY_LIMIT),
                    detail_path: None,
                });
            }
            TaskExecutionEvent::MessageDelta { content } => {
                if !content.trim().is_empty() {
                    task.timeline.push(TaskTimelineEntry {
                        timestamp: Utc::now(),
                        kind: "message".to_string(),
                        summary: summarize_text(&content, TIMELINE_SUMMARY_LIMIT),
                        detail_path: None,
                    });
                }
            }
            TaskExecutionEvent::ToolStarted { id, name, input } => {
                let input_summary = summarize_json(&input);
                task.tool_calls.push(TaskToolCallSummary {
                    id: id.clone(),
                    name: name.clone(),
                    status: TaskToolStatus::Running,
                    started_at: Utc::now(),
                    ended_at: None,
                    duration_ms: None,
                    input_summary: input_summary.clone(),
                    output_summary: None,
                    detail_path: None,
                    patch_ref: None,
                });
                let summary = input_summary
                    .map(|s| format!("{name} started ({s})"))
                    .unwrap_or_else(|| format!("{name} started"));
                task.timeline.push(TaskTimelineEntry {
                    timestamp: Utc::now(),
                    kind: "tool_started".to_string(),
                    summary,
                    detail_path: None,
                });
            }
            TaskExecutionEvent::ToolProgress { id, output } => {
                task.timeline.push(TaskTimelineEntry {
                    timestamp: Utc::now(),
                    kind: "tool_progress".to_string(),
                    summary: format!(
                        "{id}: {}",
                        summarize_text(&output, TIMELINE_SUMMARY_LIMIT.saturating_sub(8))
                    ),
                    detail_path: None,
                });
            }
            TaskExecutionEvent::ToolCompleted {
                id,
                name,
                success,
                output,
                metadata,
            } => {
                let now = Utc::now();
                let detail_path = self.artifact_if_large(task_id, &name, &output)?;
                let output_summary = summarize_text(&output, TIMELINE_SUMMARY_LIMIT);
                let patch_ref = if name == "apply_patch" {
                    detail_path.clone()
                } else {
                    None
                };

                if let Some(call) = task.tool_calls.iter_mut().find(|call| call.id == id) {
                    call.status = if success {
                        TaskToolStatus::Success
                    } else {
                        TaskToolStatus::Failed
                    };
                    call.ended_at = Some(now);
                    call.duration_ms = Some(duration_ms(call.started_at, now));
                    call.output_summary = Some(output_summary.clone());
                    call.detail_path = detail_path.clone();
                    call.patch_ref = patch_ref.clone();

                    if call.duration_ms.is_none()
                        && let Some(duration) = metadata
                            .as_ref()
                            .and_then(|m| m.get("duration_ms"))
                            .and_then(Value::as_u64)
                    {
                        call.duration_ms = Some(duration);
                    }
                }

                let status = if success { "success" } else { "failed" };
                task.timeline.push(TaskTimelineEntry {
                    timestamp: now,
                    kind: "tool_completed".to_string(),
                    summary: format!("{name} {status}: {output_summary}"),
                    detail_path: detail_path.clone(),
                });
                if let Some(patch_ref) = patch_ref {
                    task.timeline.push(TaskTimelineEntry {
                        timestamp: now,
                        kind: "patch_ref".to_string(),
                        summary: format!("Patch artifact: {}", patch_ref.display()),
                        detail_path: Some(patch_ref),
                    });
                }

                self.apply_task_update_metadata(&mut task, metadata.as_ref())?;
            }
            TaskExecutionEvent::Error { message } => {
                task.timeline.push(TaskTimelineEntry {
                    timestamp: Utc::now(),
                    kind: "error".to_string(),
                    summary: summarize_text(&message, TIMELINE_SUMMARY_LIMIT),
                    detail_path: None,
                });
            }
            TaskExecutionEvent::RuntimeEvent {
                seq,
                event,
                summary,
            } => {
                task.runtime_event_count = task.runtime_event_count.saturating_add(1);
                task.timeline.push(TaskTimelineEntry {
                    timestamp: Utc::now(),
                    kind: "runtime_event".to_string(),
                    summary: format!("#{seq} {event}: {summary}"),
                    detail_path: None,
                });
            }
        }

        self.persist_task_locked(&task)?;
        state.tasks.insert(task_id.to_string(), task);
        Ok(())
    }

    async fn finish_task(
        &self,
        task_id: &str,
        mut result: TaskExecutionResult,
        cancel: CancellationToken,
        mode_label: &str,
    ) -> Result<()> {
        let now = Utc::now();
        if cancel.is_cancelled() && result.status == TaskStatus::Completed {
            result.status = TaskStatus::Canceled;
            result.result_text = None;
            result.error = None;
        }
        if matches!(result.status, TaskStatus::Queued | TaskStatus::Running) {
            let returned_status = result.status;
            result.status = TaskStatus::Failed;
            result.error = Some(format!(
                "Executor returned non-terminal task status: {returned_status:?}"
            ));
        }

        let terminal_status = result.status;
        let terminal_error = result.error;
        let mode_label = mode_label.to_string();
        let finished_summary = match terminal_status {
            TaskStatus::Completed => "Task completed".to_string(),
            TaskStatus::Failed => format!(
                "Task failed: {}",
                terminal_error
                    .as_deref()
                    .map(|error| summarize_text(error, TIMELINE_SUMMARY_LIMIT))
                    .unwrap_or_else(|| "unknown error".to_string())
            ),
            TaskStatus::Canceled => "Task canceled".to_string(),
            TaskStatus::Queued | TaskStatus::Running => unreachable!(
                "executor non-terminal statuses are normalized to Failed before persistence"
            ),
        };
        let result_summary = result
            .result_text
            .as_deref()
            .map(|text| summarize_text(text, TIMELINE_SUMMARY_LIMIT))
            .or_else(|| {
                (terminal_status == TaskStatus::Completed)
                    .then(|| "(no textual output)".to_string())
            });
        let result_detail_path = match result.result_text.as_deref() {
            Some(text) => loop {
                match self.artifact_if_large(task_id, "result", text) {
                    Ok(detail_path) => break detail_path,
                    Err(err) => {
                        tracing::error!("Failed to persist terminal artifact for {task_id}: {err}");
                        tokio::select! {
                            _ = self.cancel_token.cancelled() => {
                                bail!(
                                    "Task manager shut down before terminal artifact for {task_id} was durable: {err}"
                                );
                            }
                            _ = sleep(TASK_PERSIST_RETRY_BACKOFF) => {}
                        }
                    }
                }
            },
            None => None,
        };

        loop {
            let persistence_error = {
                let mut state = self.state.lock().await;
                let Some(mut candidate) = state.tasks.get(task_id).cloned() else {
                    return Ok(());
                };
                candidate.status = terminal_status;
                candidate.mode = mode_label.clone();
                candidate.ended_at = Some(now);
                candidate.duration_ms = candidate.started_at.map(|start| duration_ms(start, now));
                candidate.error = terminal_error.clone();
                candidate.timeline.push(TaskTimelineEntry {
                    timestamp: now,
                    kind: "finished".to_string(),
                    summary: finished_summary.clone(),
                    detail_path: None,
                });
                candidate.result_summary = result_summary.clone();
                candidate.result_detail_path = result_detail_path.clone();
                if let Some(detail_path) = result_detail_path.as_ref() {
                    candidate.timeline.push(TaskTimelineEntry {
                        timestamp: now,
                        kind: "result_ref".to_string(),
                        summary: format!("Result artifact: {}", detail_path.display()),
                        detail_path: Some(detail_path.clone()),
                    });
                }

                match self.persist_task_locked(&candidate) {
                    Ok(()) => {
                        state.tasks.insert(task_id.to_string(), candidate);
                        state.running_cancel.remove(task_id);
                        return Ok(());
                    }
                    Err(err) => err,
                }
            };
            tracing::error!("Failed to persist terminal task {task_id}: {persistence_error}");
            tokio::select! {
                _ = self.cancel_token.cancelled() => {
                    bail!(
                        "Task manager shut down before terminal state for {task_id} was durable: {persistence_error}"
                    );
                }
                _ = sleep(TASK_PERSIST_RETRY_BACKOFF) => {}
            }
        }
    }

    fn artifact_if_large(
        &self,
        task_id: &str,
        label: &str,
        content: &str,
    ) -> Result<Option<PathBuf>> {
        if content.len() < ARTIFACT_THRESHOLD {
            return Ok(None);
        }
        self.write_artifact(task_id, label, content).map(Some)
    }

    fn write_artifact(&self, task_id: &str, label: &str, content: &str) -> Result<PathBuf> {
        ensure_safe_storage_id("task id", task_id)?;
        #[cfg(test)]
        if let Some(probe) = self
            .persistence_probe
            .lock()
            .expect("test persistence probe lock")
            .clone()
        {
            probe.before_artifact_write(task_id)?;
        }
        let artifact_dir = self.artifacts_dir.join(task_id);
        fs::create_dir_all(&artifact_dir)
            .with_context(|| format!("Failed to create artifact dir {}", artifact_dir.display()))?;
        let stamp = Utc::now().format("%Y%m%dT%H%M%S%.3fZ");
        let filename = format!("{stamp}_{}.txt", sanitize_filename(label));
        let absolute = artifact_dir.join(filename);
        fs::write(&absolute, content)
            .with_context(|| format!("Failed to write artifact {}", absolute.display()))?;
        let relative = absolute
            .strip_prefix(&self.cfg.data_dir)
            .map(PathBuf::from)
            .unwrap_or(absolute);
        Ok(relative)
    }

    fn apply_task_update_metadata(
        &self,
        task: &mut TaskRecord,
        metadata: Option<&Value>,
    ) -> Result<()> {
        let Some(updates) = metadata.and_then(|m| m.get("task_updates")) else {
            return Ok(());
        };
        let now = Utc::now();

        if let Some(value) = updates.get("checklist") {
            let mut checklist: TaskChecklistState = serde_json::from_value(value.clone())
                .context("Failed to parse checklist task update")?;
            checklist.updated_at = checklist.updated_at.or(Some(now));
            task.checklist = checklist;
            task.timeline.push(TaskTimelineEntry {
                timestamp: now,
                kind: "checklist".to_string(),
                summary: format!(
                    "Checklist updated: {} item(s), {}% complete",
                    task.checklist.items.len(),
                    task.checklist.completion_pct
                ),
                detail_path: None,
            });
        }

        if let Some(value) = updates.get("gate") {
            let gate: TaskGateRecord = serde_json::from_value(value.clone())
                .context("Failed to parse gate task update")?;
            let summary = format!("Gate {} {}: {}", gate.gate, gate.status, gate.summary);
            task.gates.retain(|existing| existing.id != gate.id);
            task.gates.push(gate.clone());
            task.timeline.push(TaskTimelineEntry {
                timestamp: now,
                kind: "gate".to_string(),
                summary: summarize_text(&summary, TIMELINE_SUMMARY_LIMIT),
                detail_path: gate.log_path,
            });
        }

        if let Some(value) = updates.get("attempt") {
            let attempt: TaskAttemptRecord = serde_json::from_value(value.clone())
                .context("Failed to parse attempt task update")?;
            task.attempts.retain(|existing| existing.id != attempt.id);
            task.attempts.push(attempt.clone());
            task.timeline.push(TaskTimelineEntry {
                timestamp: now,
                kind: "pr_attempt".to_string(),
                summary: format!(
                    "Attempt {}/{} recorded for {}",
                    attempt.attempt_index, attempt.attempt_count, attempt.attempt_group_id
                ),
                detail_path: attempt.patch_path,
            });
        }

        if let Some(value) = updates.get("artifacts")
            && let Some(items) = value.as_array()
        {
            for item in items {
                let artifact: TaskArtifactRef = serde_json::from_value(item.clone())
                    .context("Failed to parse artifact task update")?;
                task.timeline.push(TaskTimelineEntry {
                    timestamp: now,
                    kind: "artifact".to_string(),
                    summary: format!("{}: {}", artifact.label, artifact.summary),
                    detail_path: Some(artifact.path.clone()),
                });
                task.artifacts.push(artifact);
            }
        }

        if let Some(value) = updates.get("github_event") {
            let event: TaskGithubEvent = serde_json::from_value(value.clone())
                .context("Failed to parse GitHub task update")?;
            task.timeline.push(TaskTimelineEntry {
                timestamp: now,
                kind: "github".to_string(),
                summary: format!(
                    "{} {}#{}: {}",
                    event.action, event.target, event.number, event.summary
                ),
                detail_path: None,
            });
            task.github_events.push(event);
        }

        Ok(())
    }

    fn persist_queue_locked(&self, queue: &VecDeque<String>) -> Result<()> {
        #[cfg(test)]
        if let Some(probe) = self
            .persistence_probe
            .lock()
            .expect("test persistence probe lock")
            .clone()
        {
            probe.before_queue_write()?;
        }
        write_json_atomic(
            &self.queue_path,
            &QueueFile {
                queue: queue.iter().cloned().collect(),
            },
        )
    }

    fn persist_task_locked(&self, task: &TaskRecord) -> Result<()> {
        #[cfg(test)]
        if let Some(probe) = self
            .persistence_probe
            .lock()
            .expect("test persistence probe lock")
            .clone()
        {
            probe.before_task_write(task)?;
        }
        let path = self.tasks_dir.join(format!("{}.json", task.id));
        write_json_atomic(&path, task)
    }
}

fn load_state(
    tasks_dir: &Path,
    queue_path: &Path,
) -> Result<(HashMap<String, TaskRecord>, VecDeque<String>)> {
    let mut tasks = HashMap::new();
    if tasks_dir.exists() {
        for entry in fs::read_dir(tasks_dir)
            .with_context(|| format!("Failed to read tasks dir {}", tasks_dir.display()))?
        {
            let entry = entry?;
            let path = entry.path();
            if path.extension().is_none_or(|ext| ext != "json") {
                continue;
            }
            let content = fs::read_to_string(&path)
                .with_context(|| format!("Failed to read task file {}", path.display()))?;
            let mut task: TaskRecord = serde_json::from_str(&content)
                .with_context(|| format!("Failed to parse task file {}", path.display()))?;
            ensure_safe_storage_id("persisted task id", &task.id)
                .with_context(|| format!("Invalid task identity in {}", path.display()))?;
            let file_stem = path
                .file_stem()
                .and_then(|stem| stem.to_str())
                .with_context(|| {
                    format!("Task filename must be valid UTF-8: {}", path.display())
                })?;
            if file_stem != task.id {
                bail!(
                    "Task filename stem '{}' does not match persisted task id '{}' in {}",
                    file_stem,
                    task.id,
                    path.display()
                );
            }
            if task.schema_version > CURRENT_TASK_SCHEMA_VERSION {
                bail!(
                    "Task schema v{} is newer than supported v{}",
                    task.schema_version,
                    CURRENT_TASK_SCHEMA_VERSION
                );
            }
            let mut task_needs_persist = false;
            let persisted_mode = task.mode.clone();
            match normalize_persisted_task_mode(&persisted_mode) {
                Ok(canonical) => {
                    if task.mode != canonical {
                        task.mode = canonical;
                        task_needs_persist = true;
                    }
                }
                Err(err) => {
                    let now = Utc::now();
                    let duration_ms = task.started_at.and_then(|started| {
                        u64::try_from(now.signed_duration_since(started).num_milliseconds()).ok()
                    });
                    let error = err.to_string();
                    task.mode = "agent".to_string();
                    task.status = TaskStatus::Failed;
                    task.ended_at = Some(now);
                    task.duration_ms = duration_ms;
                    task.error = Some(error.clone());
                    for tool in &mut task.tool_calls {
                        if tool.status == TaskToolStatus::Running {
                            tool.status = TaskToolStatus::Failed;
                            tool.ended_at = Some(now);
                            tool.duration_ms = duration_ms.or_else(|| {
                                u64::try_from(
                                    now.signed_duration_since(tool.started_at)
                                        .num_milliseconds(),
                                )
                                .ok()
                            });
                        }
                    }
                    task.timeline.push(TaskTimelineEntry {
                        timestamp: now,
                        kind: "recovered_invalid_mode".to_string(),
                        summary: error,
                        detail_path: None,
                    });
                    task_needs_persist = true;
                }
            }
            if task.status == TaskStatus::Running {
                let now = Utc::now();
                let duration_ms = task.started_at.and_then(|started| {
                    u64::try_from(now.signed_duration_since(started).num_milliseconds()).ok()
                });
                task.status = TaskStatus::Failed;
                task.ended_at = Some(now);
                task.duration_ms = duration_ms;
                task.error = Some(
                    "Interrupted by process restart; prior process is not attached".to_string(),
                );
                for tool in &mut task.tool_calls {
                    if tool.status == TaskToolStatus::Running {
                        tool.status = TaskToolStatus::Failed;
                        tool.ended_at = Some(now);
                        tool.duration_ms = duration_ms.or_else(|| {
                            u64::try_from(
                                now.signed_duration_since(tool.started_at)
                                    .num_milliseconds(),
                            )
                            .ok()
                        });
                    }
                }
                task.timeline.push(TaskTimelineEntry {
                    timestamp: now,
                    kind: "recovered".to_string(),
                    summary: "Interrupted by process restart; prior process is not attached"
                        .to_string(),
                    detail_path: None,
                });
                task_needs_persist = true;
            }
            if task_needs_persist {
                write_json_atomic(&path, &task).with_context(|| {
                    format!("Failed to persist recovered task file {}", path.display())
                })?;
            }
            insert_loaded_task(&mut tasks, task, &path)?;
        }
    }

    let mut queue = if queue_path.exists() {
        let content = fs::read_to_string(queue_path)
            .with_context(|| format!("Failed to read queue file {}", queue_path.display()))?;
        let parsed: QueueFile = serde_json::from_str(&content)
            .with_context(|| format!("Failed to parse queue file {}", queue_path.display()))?;
        VecDeque::from(parsed.queue)
    } else {
        VecDeque::new()
    };

    queue.retain(|id| {
        tasks
            .get(id)
            .is_some_and(|task| task.status == TaskStatus::Queued)
    });

    let known = queue.iter().cloned().collect::<HashSet<_>>();
    let mut missing = tasks
        .values()
        .filter(|task| task.status == TaskStatus::Queued && !known.contains(&task.id))
        .map(|task| task.id.clone())
        .collect::<Vec<_>>();
    missing.sort();
    for id in missing {
        queue.push_back(id);
    }

    Ok((tasks, queue))
}

fn insert_loaded_task(
    tasks: &mut HashMap<String, TaskRecord>,
    task: TaskRecord,
    path: &Path,
) -> Result<()> {
    if tasks.contains_key(&task.id) {
        bail!(
            "Duplicate persisted task id '{}' while loading {}",
            task.id,
            path.display()
        );
    }
    tasks.insert(task.id.clone(), task);
    Ok(())
}

#[derive(Debug, Serialize, Deserialize)]
struct TaskPruneMarker {
    task_id: String,
}

fn task_prune_dir(tasks_dir: &Path) -> PathBuf {
    tasks_dir.join(".pruning")
}

fn persist_task_prune_marker(tasks_dir: &Path, task_id: &str) -> Result<PathBuf> {
    ensure_safe_storage_id("task id", task_id)?;
    let path = task_prune_dir(tasks_dir).join(format!("{task_id}.json"));
    write_json_atomic(
        &path,
        &TaskPruneMarker {
            task_id: task_id.to_string(),
        },
    )?;
    Ok(path)
}

fn recover_pending_task_prunes(tasks_dir: &Path, artifacts_dir: &Path) -> Result<()> {
    let prune_dir = task_prune_dir(tasks_dir);
    if !prune_dir.exists() {
        return Ok(());
    }
    for entry in fs::read_dir(&prune_dir)
        .with_context(|| format!("Failed to read task prune dir {}", prune_dir.display()))?
    {
        let path = entry?.path();
        if path.extension().is_none_or(|extension| extension != "json") {
            continue;
        }
        let file_stem = path
            .file_stem()
            .and_then(|stem| stem.to_str())
            .with_context(|| {
                format!("Task prune marker must be valid UTF-8: {}", path.display())
            })?;
        let marker: TaskPruneMarker = serde_json::from_str(
            &fs::read_to_string(&path)
                .with_context(|| format!("Failed to read task prune marker {}", path.display()))?,
        )
        .with_context(|| format!("Failed to parse task prune marker {}", path.display()))?;
        ensure_safe_storage_id("pruned task id", &marker.task_id)
            .with_context(|| format!("Invalid task prune marker {}", path.display()))?;
        if file_stem != marker.task_id {
            bail!(
                "Task prune marker stem '{}' does not match task id '{}' in {}",
                file_stem,
                marker.task_id,
                path.display()
            );
        }
        remove_dir_all_if_exists(&artifacts_dir.join(&marker.task_id))?;
        remove_file_if_exists(&tasks_dir.join(format!("{}.json", marker.task_id)))?;
        remove_file_if_exists(&path)?;
    }
    Ok(())
}

fn remove_file_if_exists(path: &Path) -> Result<()> {
    match fs::remove_file(path) {
        Ok(()) => Ok(()),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(err) => Err(err).with_context(|| format!("Failed to remove {}", path.display())),
    }
}

fn remove_dir_all_if_exists(path: &Path) -> Result<()> {
    match fs::remove_dir_all(path) {
        Ok(()) => Ok(()),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(err) => Err(err).with_context(|| format!("Failed to remove {}", path.display())),
    }
}

fn build_idempotency_index(tasks: &HashMap<String, TaskRecord>) -> Result<HashMap<String, String>> {
    let mut index = HashMap::new();
    for task in tasks.values() {
        let Some(key) = task.idempotency_key.as_ref() else {
            continue;
        };
        if let Some(existing_id) = index.insert(key.clone(), task.id.clone()) {
            bail!(
                "Duplicate task idempotency key '{}' for tasks '{}' and '{}'",
                key,
                existing_id,
                task.id
            );
        }
    }
    Ok(index)
}

fn resolve_task_id(tasks: &HashMap<String, TaskRecord>, id_or_prefix: &str) -> Result<String> {
    if tasks.contains_key(id_or_prefix) {
        return Ok(id_or_prefix.to_string());
    }
    let matches = tasks
        .keys()
        .filter(|id| id.starts_with(id_or_prefix))
        .cloned()
        .collect::<Vec<_>>();
    match matches.len() {
        0 => bail!("Task not found: {id_or_prefix}"),
        1 => Ok(matches[0].clone()),
        _ => bail!(
            "Ambiguous task prefix '{}': matches {} tasks",
            id_or_prefix,
            matches.len()
        ),
    }
}

fn normalize_task_mode(mode: String) -> Result<String> {
    let normalized = mode.trim().to_ascii_lowercase();
    if matches!(normalized.as_str(), "agent" | "plan" | "yolo") {
        Ok(normalized)
    } else {
        bail!("Invalid task mode '{mode}'. Expected one of: agent, plan, yolo")
    }
}

fn normalize_persisted_task_mode(mode: &str) -> Result<String> {
    match mode.trim().to_ascii_lowercase().as_str() {
        "agent" | "1" => Ok("agent".to_string()),
        "plan" | "2" => Ok("plan".to_string()),
        "yolo" | "3" => Ok("yolo".to_string()),
        _ => bail!("Invalid persisted task mode '{mode}'. Expected one of: agent, plan, yolo"),
    }
}

fn normalize_idempotency_key(key: String) -> Result<String> {
    let normalized = key.trim().to_string();
    if normalized.is_empty() {
        bail!("Task idempotency key cannot be empty");
    }
    if normalized.len() > 512 {
        bail!("Task idempotency key cannot exceed 512 bytes");
    }
    Ok(normalized)
}

fn summarize_json(value: &Value) -> Option<String> {
    let text = serde_json::to_string(value).ok()?;
    Some(summarize_text(&text, TIMELINE_SUMMARY_LIMIT))
}

fn summarize_text(text: &str, limit: usize) -> String {
    let take = limit.saturating_sub(3);
    let mut count = 0;
    let mut out = String::new();
    for ch in text.chars() {
        if count >= take {
            out.push_str("...");
            return out;
        }
        if ch.is_control() && ch != '\n' && ch != '\t' {
            continue;
        }
        out.push(ch);
        count += 1;
    }
    out
}

fn ensure_safe_storage_id(kind: &str, value: &str) -> Result<()> {
    let mut components = Path::new(value).components();
    let Some(component) = components.next() else {
        bail!("{kind} must not be empty");
    };
    if components.next().is_some() || !matches!(component, std::path::Component::Normal(_)) {
        bail!("{kind} must be a single path component");
    }
    Ok(())
}

fn sanitize_filename(input: &str) -> String {
    let mut out = String::new();
    for ch in input.chars() {
        if ch.is_ascii_alphanumeric() || ch == '_' || ch == '-' {
            out.push(ch);
        } else {
            out.push('_');
        }
    }
    if out.is_empty() {
        "artifact".to_string()
    } else {
        out
    }
}

fn duration_ms(start: DateTime<Utc>, end: DateTime<Utc>) -> u64 {
    let millis = (end - start).num_milliseconds();
    if millis.is_negative() {
        0
    } else {
        u64::try_from(millis).unwrap_or(u64::MAX)
    }
}

fn write_json_atomic<T: Serialize>(path: &Path, value: &T) -> Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("Failed to create directory {}", parent.display()))?;
    }
    let payload = serde_json::to_string_pretty(value)?;
    crate::utils::write_atomic(path, payload.as_bytes())
        .with_context(|| format!("Failed to write {}", path.display()))
}

fn default_auto_approve() -> bool {
    false
}

/// Default task manager data location (`~/.codewhale/tasks`, or legacy
/// `~/.deepseek/tasks` when only the legacy directory exists).
#[must_use]
pub fn default_tasks_dir() -> PathBuf {
    if let Ok(path) = std::env::var("DEEPSEEK_TASKS_DIR")
        && !path.trim().is_empty()
    {
        return PathBuf::from(path);
    }
    dirs::home_dir()
        .map(|home| default_tasks_dir_for_home(&home))
        .unwrap_or_else(|| PathBuf::from(".codewhale").join("tasks"))
}

fn default_tasks_dir_for_home(home: &Path) -> PathBuf {
    let primary = home.join(".codewhale").join("tasks");
    if primary.is_dir() {
        return primary;
    }
    let legacy = home.join(".deepseek").join("tasks");
    if legacy.is_dir() {
        return legacy;
    }
    primary
}

/// Wait for a task to reach a terminal status (tests and API helpers).
#[cfg(test)]
pub async fn wait_for_terminal_state(
    manager: &TaskManager,
    task_id: &str,
    timeout: StdDuration,
) -> Result<TaskRecord> {
    let deadline = std::time::Instant::now() + timeout;
    loop {
        let task = manager.get_task(task_id).await?;
        if task.status.is_terminal() {
            return Ok(task);
        }
        if std::time::Instant::now() >= deadline {
            bail!("Timed out waiting for task {task_id}");
        }
        sleep(StdDuration::from_millis(50)).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use tokio::time::Duration;

    struct MockExecutor;

    struct NonTerminalExecutor {
        status: TaskStatus,
    }

    struct DurabilityCheckingExecutor {
        root: PathBuf,
        saw_durable_thread: Arc<AtomicBool>,
    }

    struct CountingExecutor {
        calls: Arc<AtomicUsize>,
        result_text: Option<String>,
    }

    struct ShutdownObservingExecutor {
        started: Arc<AtomicBool>,
        saw_cancel: Arc<AtomicBool>,
    }

    struct FakeTaskTurnRuntime {
        batches: std::sync::Mutex<
            VecDeque<std::result::Result<Vec<crate::runtime_threads::RuntimeEventRecord>, String>>,
        >,
        interrupt_calls: AtomicUsize,
    }

    impl FakeTaskTurnRuntime {
        fn new(
            batches: impl IntoIterator<
                Item = std::result::Result<Vec<crate::runtime_threads::RuntimeEventRecord>, String>,
            >,
        ) -> Self {
            Self {
                batches: std::sync::Mutex::new(batches.into_iter().collect()),
                interrupt_calls: AtomicUsize::new(0),
            }
        }

        fn interrupt_calls(&self) -> usize {
            self.interrupt_calls.load(Ordering::SeqCst)
        }
    }

    #[async_trait]
    impl TaskTurnRuntime for FakeTaskTurnRuntime {
        fn events_since(
            &self,
            _thread_id: &str,
            _since_seq: Option<u64>,
        ) -> Result<Vec<crate::runtime_threads::RuntimeEventRecord>> {
            self.batches
                .lock()
                .expect("fake runtime batches lock")
                .pop_front()
                .unwrap_or_else(|| Ok(Vec::new()))
                .map_err(anyhow::Error::msg)
        }

        async fn interrupt_turn(&self, _thread_id: &str, _turn_id: &str) -> Result<()> {
            self.interrupt_calls.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
    }

    #[async_trait]
    impl TaskExecutor for NonTerminalExecutor {
        async fn execute(
            &self,
            _task: ExecutionTask,
            _reporter: TaskExecutionReporter,
            _cancel: CancellationToken,
        ) -> TaskExecutionResult {
            TaskExecutionResult {
                status: self.status,
                result_text: None,
                error: None,
            }
        }
    }

    #[async_trait]
    impl TaskExecutor for DurabilityCheckingExecutor {
        async fn execute(
            &self,
            task: ExecutionTask,
            reporter: TaskExecutionReporter,
            _cancel: CancellationToken,
        ) -> TaskExecutionResult {
            reporter
                .report(TaskExecutionEvent::ThreadCreated {
                    thread_id: "thr_durable_ack".to_string(),
                })
                .await
                .expect("ThreadCreated must be durably acknowledged");

            let task_path = self.root.join("tasks").join(format!("{}.json", task.id()));
            let persisted: TaskRecord = serde_json::from_str(
                &fs::read_to_string(task_path).expect("ack must follow durable task write"),
            )
            .expect("persisted task record");
            self.saw_durable_thread.store(
                persisted.thread_id.as_deref() == Some("thr_durable_ack"),
                Ordering::SeqCst,
            );

            TaskExecutionResult {
                status: TaskStatus::Completed,
                result_text: Some("done".to_string()),
                error: None,
            }
        }
    }

    #[async_trait]
    impl TaskExecutor for CountingExecutor {
        async fn execute(
            &self,
            _task: ExecutionTask,
            _reporter: TaskExecutionReporter,
            _cancel: CancellationToken,
        ) -> TaskExecutionResult {
            self.calls.fetch_add(1, Ordering::SeqCst);
            TaskExecutionResult {
                status: TaskStatus::Completed,
                result_text: self.result_text.clone(),
                error: None,
            }
        }
    }

    #[async_trait]
    impl TaskExecutor for ShutdownObservingExecutor {
        async fn execute(
            &self,
            _task: ExecutionTask,
            _reporter: TaskExecutionReporter,
            cancel: CancellationToken,
        ) -> TaskExecutionResult {
            self.started.store(true, Ordering::SeqCst);
            cancel.cancelled().await;
            self.saw_cancel.store(true, Ordering::SeqCst);
            TaskExecutionResult {
                status: TaskStatus::Canceled,
                result_text: None,
                error: None,
            }
        }
    }

    #[async_trait]
    impl TaskExecutor for MockExecutor {
        async fn execute(
            &self,
            task: ExecutionTask,
            reporter: TaskExecutionReporter,
            cancel: CancellationToken,
        ) -> TaskExecutionResult {
            reporter
                .report(TaskExecutionEvent::Status {
                    message: format!("running {}", task.id),
                })
                .await
                .expect("persist status");
            reporter
                .report(TaskExecutionEvent::ThreadCreated {
                    thread_id: "thr_test".to_string(),
                })
                .await
                .expect("persist thread");
            reporter
                .report(TaskExecutionEvent::ThreadLinked {
                    thread_id: "thr_test".to_string(),
                    turn_id: "turn_test".to_string(),
                })
                .await
                .expect("persist runtime link");
            reporter
                .report(TaskExecutionEvent::ToolStarted {
                    id: "tool_1".to_string(),
                    name: "read_file".to_string(),
                    input: serde_json::json!({ "path": "README.md" }),
                })
                .await
                .expect("persist tool start");
            sleep(Duration::from_millis(50)).await;
            if cancel.is_cancelled() {
                return TaskExecutionResult {
                    status: TaskStatus::Canceled,
                    result_text: None,
                    error: None,
                };
            }
            reporter
                .report(TaskExecutionEvent::ToolCompleted {
                    id: "tool_1".to_string(),
                    name: "read_file".to_string(),
                    success: true,
                    output: "read ok".to_string(),
                    metadata: Some(serde_json::json!({
                        "duration_ms": 10,
                        "task_updates": {
                            "checklist": {
                                "items": [
                                    { "id": 1, "content": "read fixture", "status": "in_progress" }
                                ],
                                "completion_pct": 0,
                                "in_progress_id": 1,
                                "updated_at": null
                            }
                        }
                    })),
                })
                .await
                .expect("persist tool completion");
            TaskExecutionResult {
                status: TaskStatus::Completed,
                result_text: Some("done".to_string()),
                error: None,
            }
        }
    }

    fn test_config(root: PathBuf) -> TaskManagerConfig {
        TaskManagerConfig {
            data_dir: root,
            worker_count: 1,
            default_workspace: PathBuf::from("."),
            default_model: "deepseek-v4-flash".to_string(),
            default_mode: "agent".to_string(),
            allow_shell: false,
            trust_mode: false,
            max_subagents: 2,
        }
    }

    fn terminal_runtime_event(status: &str) -> crate::runtime_threads::RuntimeEventRecord {
        crate::runtime_threads::RuntimeEventRecord {
            schema_version: 2,
            seq: 1,
            timestamp: Utc::now(),
            thread_id: "thr_active".to_string(),
            turn_id: Some("turn_active".to_string()),
            item_id: None,
            event: "turn.completed".to_string(),
            payload: json!({ "turn": { "status": status } }),
        }
    }

    fn auto_ack_reporter(
        cancel: CancellationToken,
    ) -> (TaskExecutionReporter, tokio::task::JoinHandle<()>) {
        let (tx, mut rx) = mpsc::unbounded_channel::<PendingTaskExecutionEvent>();
        let reporter = TaskExecutionReporter::new(tx, cancel);
        let ack_task = tokio::spawn(async move {
            while let Some(pending) = rx.recv().await {
                let _ = pending.ack.send(Ok(()));
            }
        });
        (reporter, ack_task)
    }

    fn overwrite_persisted_task_mode(root: &Path, task_id: &str, mode: &str) -> Result<()> {
        let path = root.join("tasks").join(format!("{task_id}.json"));
        let mut value: Value = serde_json::from_str(&fs::read_to_string(&path)?)?;
        value["mode"] = json!(mode);
        fs::write(path, serde_json::to_string_pretty(&value)?)?;
        Ok(())
    }

    #[tokio::test]
    async fn task_ids_keep_full_uuid_entropy() -> Result<()> {
        let tempdir = tempfile::tempdir()?;
        let manager = TaskManager::start_with_executor(
            test_config(tempdir.path().to_path_buf()),
            Arc::new(MockExecutor),
        )
        .await?;
        manager.shutdown();

        let task = manager
            .add_task(NewTaskRequest::from_prompt("keep the full task UUID"))
            .await?;
        let uuid = task
            .id
            .strip_prefix("task_")
            .expect("task id must keep the task_ prefix");

        assert_eq!(uuid.len(), 36, "task id must retain the full UUID text");
        assert!(
            Uuid::parse_str(uuid).is_ok(),
            "task id suffix must be a UUID"
        );
        Ok(())
    }

    #[tokio::test]
    async fn task_id_collision_does_not_overwrite_existing_task() -> Result<()> {
        let tempdir = tempfile::tempdir()?;
        let colliding_id = format!("task_{}", Uuid::new_v4());
        let fresh_id = format!("task_{}", Uuid::new_v4());
        let generated_ids = Arc::new(std::sync::Mutex::new(std::collections::VecDeque::from([
            colliding_id.clone(),
            colliding_id,
            fresh_id.clone(),
        ])));
        let ids_for_generator = Arc::clone(&generated_ids);
        let manager = TaskManager::start_with_executor_and_id_generator(
            test_config(tempdir.path().to_path_buf()),
            Arc::new(MockExecutor),
            Arc::new(move || {
                ids_for_generator
                    .lock()
                    .expect("task id generator lock")
                    .pop_front()
                    .expect("test task id")
            }),
        )
        .await?;
        manager.shutdown();

        let first = manager
            .add_task(NewTaskRequest::from_prompt("keep the original task"))
            .await?;
        let second = manager
            .add_task(NewTaskRequest::from_prompt("create a distinct task"))
            .await?;

        assert_ne!(first.id, second.id, "a collision must be retried");
        assert_eq!(second.id, fresh_id);
        assert_eq!(
            manager.get_task(&first.id).await?.prompt,
            "keep the original task"
        );
        assert_eq!(manager.list_tasks(None).await.len(), 2);
        Ok(())
    }

    #[tokio::test]
    async fn executor_never_starts_before_running_record_is_durable() -> Result<()> {
        let tempdir = tempfile::tempdir()?;
        let root = tempdir.path().to_path_buf();
        let calls = Arc::new(AtomicUsize::new(0));
        let manager = TaskManager::start_with_executor(
            test_config(root.clone()),
            Arc::new(CountingExecutor {
                calls: Arc::clone(&calls),
                result_text: Some("done".to_string()),
            }),
        )
        .await?;
        let probe = Arc::new(TestPersistenceProbe::default());
        probe.block_status(TaskStatus::Running);
        manager.install_persistence_probe(Arc::clone(&probe));

        let task = manager
            .add_task(NewTaskRequest::from_prompt("wait for durable Running"))
            .await?;
        let deadline = std::time::Instant::now() + Duration::from_secs(2);
        while probe.task_status_write_count(&task.id, TaskStatus::Running) == 0 {
            if std::time::Instant::now() >= deadline {
                bail!("worker never attempted to persist Running");
            }
            sleep(Duration::from_millis(10)).await;
        }

        assert_eq!(
            calls.load(Ordering::SeqCst),
            0,
            "executor must not start after a rejected Running write"
        );
        assert_eq!(manager.get_task(&task.id).await?.status, TaskStatus::Queued);
        let persisted: TaskRecord = serde_json::from_str(&fs::read_to_string(
            root.join("tasks").join(format!("{}.json", task.id)),
        )?)?;
        assert_eq!(persisted.status, TaskStatus::Queued);

        probe.unblock_status();
        let completed = wait_for_terminal_state(&manager, &task.id, Duration::from_secs(5)).await?;
        assert_eq!(completed.status, TaskStatus::Completed);
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert!(probe.task_status_write_count(&task.id, TaskStatus::Running) >= 2);
        manager.shutdown();
        Ok(())
    }

    #[tokio::test]
    async fn rejected_execution_event_does_not_mutate_memory_or_later_leak_to_disk() -> Result<()> {
        let tempdir = tempfile::tempdir()?;
        let root = tempdir.path().to_path_buf();
        let manager = TaskManager::start_with_executor(
            test_config(root.clone()),
            Arc::new(CountingExecutor {
                calls: Arc::new(AtomicUsize::new(0)),
                result_text: None,
            }),
        )
        .await?;
        manager.shutdown();
        let task = manager
            .add_task(NewTaskRequest::from_prompt("reject one event"))
            .await?;
        let probe = Arc::new(TestPersistenceProbe::default());
        let rejected_summary = "event must remain rejected";
        probe.block_timeline_summary(rejected_summary);
        manager.install_persistence_probe(Arc::clone(&probe));

        let outcome = manager
            .apply_execution_event(
                &task.id,
                TaskExecutionEvent::Status {
                    message: rejected_summary.to_string(),
                },
            )
            .await;
        let leaked_to_memory = manager
            .get_task(&task.id)
            .await?
            .timeline
            .iter()
            .any(|entry| entry.summary == rejected_summary);

        probe.unblock_timeline_summary();
        manager
            .finish_task(
                &task.id,
                TaskExecutionResult {
                    status: TaskStatus::Completed,
                    result_text: Some("done".to_string()),
                    error: None,
                },
                CancellationToken::new(),
                "agent",
            )
            .await?;
        let persisted: TaskRecord = serde_json::from_str(&fs::read_to_string(
            root.join("tasks").join(format!("{}.json", task.id)),
        )?)?;
        let leaked_to_disk = persisted
            .timeline
            .iter()
            .any(|entry| entry.summary == rejected_summary);

        assert!(outcome.is_err(), "the event gate must reject the write");
        assert!(!leaked_to_memory, "a rejected event mutated memory");
        assert!(!leaked_to_disk, "a rejected event leaked during finish");
        Ok(())
    }

    #[tokio::test]
    async fn terminal_state_is_not_visible_until_terminal_write_succeeds() -> Result<()> {
        let tempdir = tempfile::tempdir()?;
        let root = tempdir.path().to_path_buf();
        let calls = Arc::new(AtomicUsize::new(0));
        let manager = TaskManager::start_with_executor(
            test_config(root.clone()),
            Arc::new(CountingExecutor {
                calls: Arc::clone(&calls),
                result_text: Some("x".repeat(ARTIFACT_THRESHOLD + 100)),
            }),
        )
        .await?;
        let probe = Arc::new(TestPersistenceProbe::default());
        probe.block_status(TaskStatus::Completed);
        manager.install_persistence_probe(Arc::clone(&probe));

        let task = manager
            .add_task(NewTaskRequest::from_prompt("hold terminal persistence"))
            .await?;
        let deadline = std::time::Instant::now() + Duration::from_secs(3);
        while probe.task_status_write_count(&task.id, TaskStatus::Completed) == 0 {
            if std::time::Instant::now() >= deadline {
                manager.shutdown();
                bail!("worker never attempted to persist Completed");
            }
            sleep(Duration::from_millis(10)).await;
        }

        let visible_before_release = manager.get_task(&task.id).await?.status;
        let persisted_before_release: TaskRecord = serde_json::from_str(&fs::read_to_string(
            root.join("tasks").join(format!("{}.json", task.id)),
        )?)?;
        let artifact_dir = root.join("artifacts").join(&task.id);
        let artifacts_before_release = fs::read_dir(&artifact_dir)?.count();
        probe.unblock_status();

        if visible_before_release != TaskStatus::Running
            || persisted_before_release.status != TaskStatus::Running
        {
            manager.shutdown();
            bail!(
                "terminal state became visible before persistence: memory={:?}, disk={:?}",
                visible_before_release,
                persisted_before_release.status
            );
        }
        assert_eq!(
            artifacts_before_release, 1,
            "the large result artifact must be prepared once"
        );

        let completed = wait_for_terminal_state(&manager, &task.id, Duration::from_secs(5)).await?;
        assert_eq!(completed.status, TaskStatus::Completed);
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert!(probe.task_status_write_count(&task.id, TaskStatus::Completed) >= 2);
        assert_eq!(
            fs::read_dir(&artifact_dir)?.count(),
            1,
            "terminal retries must reuse the same large-result artifact"
        );
        manager.shutdown();
        Ok(())
    }

    #[tokio::test]
    async fn terminal_artifact_write_failure_retries_without_publishing_terminal_state()
    -> Result<()> {
        let tempdir = tempfile::tempdir()?;
        let root = tempdir.path().to_path_buf();
        let manager = TaskManager::start_with_executor(
            test_config(root.clone()),
            Arc::new(CountingExecutor {
                calls: Arc::new(AtomicUsize::new(0)),
                result_text: Some("x".repeat(ARTIFACT_THRESHOLD + 100)),
            }),
        )
        .await?;
        let probe = Arc::new(TestPersistenceProbe::default());
        probe.block_artifact_writes.store(true, Ordering::SeqCst);
        manager.install_persistence_probe(Arc::clone(&probe));

        let task = manager
            .add_task(NewTaskRequest::from_prompt("retry artifact persistence"))
            .await?;
        let attempt_deadline = std::time::Instant::now() + Duration::from_secs(2);
        while probe.artifact_write_count(&task.id) == 0 {
            if std::time::Instant::now() >= attempt_deadline {
                manager.shutdown();
                bail!("worker never attempted to persist the result artifact");
            }
            sleep(Duration::from_millis(10)).await;
        }
        sleep(Duration::from_millis(130)).await;
        let attempts_while_blocked = probe.artifact_write_count(&task.id);
        let visible_while_blocked = manager.get_task(&task.id).await?;
        let cancel_handle_retained_while_blocked = manager
            .state
            .lock()
            .await
            .running_cancel
            .contains_key(&task.id);
        let persisted_while_blocked: TaskRecord = serde_json::from_str(&fs::read_to_string(
            root.join("tasks").join(format!("{}.json", task.id)),
        )?)?;
        probe.block_artifact_writes.store(false, Ordering::SeqCst);

        assert_eq!(visible_while_blocked.status, TaskStatus::Running);
        assert_eq!(persisted_while_blocked.status, TaskStatus::Running);
        assert!(
            cancel_handle_retained_while_blocked,
            "the running cancel handle must remain until terminal state is durable"
        );
        assert!(
            attempts_while_blocked < 10,
            "artifact retry loop is hot: {attempts_while_blocked} attempts"
        );
        let completed = wait_for_terminal_state(&manager, &task.id, Duration::from_secs(2)).await;
        let completed = match completed {
            Ok(completed) => completed,
            Err(err) => {
                manager.shutdown();
                return Err(err);
            }
        };
        assert_eq!(completed.status, TaskStatus::Completed);
        assert!(
            !manager
                .state
                .lock()
                .await
                .running_cancel
                .contains_key(&task.id),
            "the running cancel handle must be removed after terminal state is durable"
        );
        assert!(probe.artifact_write_count(&task.id) >= 2);
        assert_eq!(
            fs::read_dir(root.join("artifacts").join(&task.id))?.count(),
            1,
            "a successful artifact must be generated exactly once"
        );
        manager.shutdown();

        let shutdown_tempdir = tempfile::tempdir()?;
        let shutdown_root = shutdown_tempdir.path().to_path_buf();
        let shutdown_manager = TaskManager::start_with_executor(
            test_config(shutdown_root.clone()),
            Arc::new(CountingExecutor {
                calls: Arc::new(AtomicUsize::new(0)),
                result_text: Some("y".repeat(ARTIFACT_THRESHOLD + 100)),
            }),
        )
        .await?;
        let shutdown_probe = Arc::new(TestPersistenceProbe::default());
        shutdown_probe
            .block_artifact_writes
            .store(true, Ordering::SeqCst);
        shutdown_manager.install_persistence_probe(Arc::clone(&shutdown_probe));
        let shutdown_task = shutdown_manager
            .add_task(NewTaskRequest::from_prompt("cancel artifact retries"))
            .await?;
        let shutdown_attempt_deadline = std::time::Instant::now() + Duration::from_secs(2);
        while shutdown_probe.artifact_write_count(&shutdown_task.id) == 0 {
            if std::time::Instant::now() >= shutdown_attempt_deadline {
                shutdown_manager.shutdown();
                bail!("shutdown case never attempted artifact persistence");
            }
            sleep(Duration::from_millis(10)).await;
        }
        shutdown_manager.shutdown();
        let attempts_at_shutdown = shutdown_probe.artifact_write_count(&shutdown_task.id);
        sleep(Duration::from_millis(150)).await;

        assert_eq!(
            shutdown_probe.artifact_write_count(&shutdown_task.id),
            attempts_at_shutdown,
            "artifact retries continued after manager shutdown"
        );
        assert_eq!(
            shutdown_manager.get_task(&shutdown_task.id).await?.status,
            TaskStatus::Running
        );
        let shutdown_persisted: TaskRecord = serde_json::from_str(&fs::read_to_string(
            shutdown_root
                .join("tasks")
                .join(format!("{}.json", shutdown_task.id)),
        )?)?;
        assert_eq!(shutdown_persisted.status, TaskStatus::Running);
        assert!(
            shutdown_manager
                .state
                .lock()
                .await
                .running_cancel
                .contains_key(&shutdown_task.id),
            "shutdown must not remove the running cancel handle without a durable terminal state"
        );
        Ok(())
    }

    #[tokio::test]
    async fn cancel_persistence_failure_rolls_back_memory_and_queue() -> Result<()> {
        let tempdir = tempfile::tempdir()?;
        let root = tempdir.path().to_path_buf();
        let manager = TaskManager::start_with_executor(
            test_config(root.clone()),
            Arc::new(CountingExecutor {
                calls: Arc::new(AtomicUsize::new(0)),
                result_text: None,
            }),
        )
        .await?;
        manager.shutdown();
        let task = manager
            .add_task(NewTaskRequest::from_prompt("cancel durably"))
            .await?;
        let probe = Arc::new(TestPersistenceProbe::default());
        manager.install_persistence_probe(Arc::clone(&probe));
        probe.fail_next_task_write.store(true, Ordering::SeqCst);

        let outcome = manager.cancel_task(&task.id).await;
        let memory = manager.get_task(&task.id).await?;
        let queue_contains_task = manager.state.lock().await.queue.contains(&task.id);
        let persisted: TaskRecord = serde_json::from_str(&fs::read_to_string(
            root.join("tasks").join(format!("{}.json", task.id)),
        )?)?;

        assert!(outcome.is_err(), "the injected cancel write must fail");
        assert_eq!(memory.status, TaskStatus::Queued);
        assert_eq!(persisted.status, TaskStatus::Queued);
        assert!(queue_contains_task, "failed cancel removed the queued task");
        Ok(())
    }

    #[tokio::test]
    async fn metadata_persistence_failure_rolls_back_memory() -> Result<()> {
        let tempdir = tempfile::tempdir()?;
        let root = tempdir.path().to_path_buf();
        let manager = TaskManager::start_with_executor(
            test_config(root.clone()),
            Arc::new(CountingExecutor {
                calls: Arc::new(AtomicUsize::new(0)),
                result_text: None,
            }),
        )
        .await?;
        manager.shutdown();
        let task = manager
            .add_task(NewTaskRequest::from_prompt("metadata durably"))
            .await?;
        let probe = Arc::new(TestPersistenceProbe::default());
        manager.install_persistence_probe(Arc::clone(&probe));
        probe.fail_next_task_write.store(true, Ordering::SeqCst);

        let outcome = manager
            .record_tool_metadata(
                &task.id,
                &serde_json::json!({
                    "task_updates": {
                        "gate": {
                            "id": "gate_rejected",
                            "gate": "test",
                            "command": "cargo test",
                            "cwd": ".",
                            "exit_code": 0,
                            "status": "passed",
                            "classification": "passed",
                            "duration_ms": 1,
                            "summary": "must roll back",
                            "log_path": null,
                            "recorded_at": Utc::now()
                        }
                    }
                }),
            )
            .await;
        let memory = manager.get_task(&task.id).await?;
        let persisted: TaskRecord = serde_json::from_str(&fs::read_to_string(
            root.join("tasks").join(format!("{}.json", task.id)),
        )?)?;

        assert!(outcome.is_err(), "the injected metadata write must fail");
        assert!(memory.gates.is_empty(), "failed metadata mutated memory");
        assert!(persisted.gates.is_empty(), "failed metadata mutated disk");
        Ok(())
    }

    #[tokio::test]
    async fn unrelated_tasks_are_not_rewritten_by_start_or_finish() -> Result<()> {
        let startup_tempdir = tempfile::tempdir()?;
        let startup_root = startup_tempdir.path().to_path_buf();
        let seed = TaskManager::start_with_executor(
            test_config(startup_root.clone()),
            Arc::new(CountingExecutor {
                calls: Arc::new(AtomicUsize::new(0)),
                result_text: None,
            }),
        )
        .await?;
        seed.shutdown();
        let unrelated = seed
            .add_task(NewTaskRequest::from_prompt("leave startup bytes alone"))
            .await?;
        let recovered = seed
            .add_task(NewTaskRequest::from_prompt("recover only this task"))
            .await?;
        drop(seed);

        let now = Utc::now();
        let mut unrelated_terminal = unrelated.clone();
        unrelated_terminal.status = TaskStatus::Completed;
        unrelated_terminal.ended_at = Some(now);
        unrelated_terminal.duration_ms = Some(0);
        let unrelated_path = startup_root
            .join("tasks")
            .join(format!("{}.json", unrelated.id));
        fs::write(&unrelated_path, serde_json::to_string(&unrelated_terminal)?)?;
        let unrelated_before_start = fs::read(&unrelated_path)?;

        let mut recovered_running = recovered.clone();
        recovered_running.status = TaskStatus::Running;
        recovered_running.started_at = Some(now);
        recovered_running.timeline.push(TaskTimelineEntry {
            timestamp: now,
            kind: "running".to_string(),
            summary: "Task started".to_string(),
            detail_path: None,
        });
        let recovered_path = startup_root
            .join("tasks")
            .join(format!("{}.json", recovered.id));
        fs::write(&recovered_path, serde_json::to_string(&recovered_running)?)?;

        let reopened = TaskManager::start_with_executor(
            test_config(startup_root),
            Arc::new(CountingExecutor {
                calls: Arc::new(AtomicUsize::new(0)),
                result_text: None,
            }),
        )
        .await?;
        reopened.shutdown();
        let unrelated_unchanged = fs::read(&unrelated_path)? == unrelated_before_start;
        let recovered_persisted: TaskRecord =
            serde_json::from_str(&fs::read_to_string(&recovered_path)?)?;
        drop(reopened);

        let finish_tempdir = tempfile::tempdir()?;
        let finish_root = finish_tempdir.path().to_path_buf();
        let manager = TaskManager::start_with_executor(
            test_config(finish_root.clone()),
            Arc::new(CountingExecutor {
                calls: Arc::new(AtomicUsize::new(0)),
                result_text: None,
            }),
        )
        .await?;
        manager.shutdown();
        let target = manager
            .add_task(NewTaskRequest::from_prompt("finish only this task"))
            .await?;
        let finish_unrelated = manager
            .add_task(NewTaskRequest::from_prompt("do not rewrite this task"))
            .await?;
        let mut running = target.clone();
        running.status = TaskStatus::Running;
        running.started_at = Some(Utc::now());
        write_json_atomic(
            &finish_root
                .join("tasks")
                .join(format!("{}.json", target.id)),
            &running,
        )?;
        {
            let mut state = manager.state.lock().await;
            state.tasks.insert(target.id.clone(), running);
            state.queue.retain(|queued_id| queued_id != &target.id);
            state
                .running_cancel
                .insert(target.id.clone(), CancellationToken::new());
        }
        let probe = Arc::new(TestPersistenceProbe::default());
        manager.install_persistence_probe(Arc::clone(&probe));
        probe.reset_write_counts();

        manager
            .finish_task(
                &target.id,
                TaskExecutionResult {
                    status: TaskStatus::Completed,
                    result_text: Some("done".to_string()),
                    error: None,
                },
                CancellationToken::new(),
                "agent",
            )
            .await?;

        assert!(
            unrelated_unchanged,
            "startup rewrote an unrelated terminal task"
        );
        assert_eq!(recovered_persisted.status, TaskStatus::Failed);
        assert_eq!(probe.task_write_count(&target.id), 1);
        assert_eq!(
            probe.task_write_count(&finish_unrelated.id),
            0,
            "finish rewrote an unrelated task"
        );
        Ok(())
    }

    #[tokio::test]
    async fn shutdown_cancels_running_executor() -> Result<()> {
        let tempdir = tempfile::tempdir()?;
        let started = Arc::new(AtomicBool::new(false));
        let saw_cancel = Arc::new(AtomicBool::new(false));
        let manager = TaskManager::start_with_executor(
            test_config(tempdir.path().to_path_buf()),
            Arc::new(ShutdownObservingExecutor {
                started: Arc::clone(&started),
                saw_cancel: Arc::clone(&saw_cancel),
            }),
        )
        .await?;
        let task = manager
            .add_task(NewTaskRequest::from_prompt("observe manager shutdown"))
            .await?;
        let start_deadline = std::time::Instant::now() + Duration::from_secs(2);
        while !started.load(Ordering::SeqCst) {
            if std::time::Instant::now() >= start_deadline {
                manager.shutdown();
                bail!("executor never started");
            }
            sleep(Duration::from_millis(10)).await;
        }

        manager.shutdown();
        let cancel_deadline = std::time::Instant::now() + Duration::from_millis(500);
        while !saw_cancel.load(Ordering::SeqCst) && std::time::Instant::now() < cancel_deadline {
            sleep(Duration::from_millis(10)).await;
        }
        let shutdown_propagated = saw_cancel.load(Ordering::SeqCst);
        if !shutdown_propagated {
            let running_cancel = manager
                .state
                .lock()
                .await
                .running_cancel
                .get(&task.id)
                .cloned();
            if let Some(cancel) = running_cancel {
                cancel.cancel();
            }
        }

        assert!(
            shutdown_propagated,
            "manager shutdown did not cancel the running executor"
        );
        Ok(())
    }

    #[tokio::test]
    async fn reporter_waits_for_manager_ack() -> Result<()> {
        let (tx, mut rx) = mpsc::unbounded_channel();
        let reporter = TaskExecutionReporter::new(tx, CancellationToken::new());
        let continued = Arc::new(AtomicBool::new(false));
        let continued_after_report = Arc::clone(&continued);

        let reporting = tokio::spawn(async move {
            reporter
                .report(TaskExecutionEvent::Status {
                    message: "wait for persistence".to_string(),
                })
                .await
                .expect("acknowledged report");
            continued_after_report.store(true, Ordering::SeqCst);
        });

        let pending = rx.recv().await.expect("pending event");
        tokio::task::yield_now().await;
        assert!(
            !continued.load(Ordering::SeqCst),
            "executor must not continue before TaskManager acknowledges the event"
        );

        pending.ack.send(Ok(())).expect("send durable ack");
        reporting.await?;
        assert!(continued.load(Ordering::SeqCst));
        Ok(())
    }

    #[tokio::test]
    async fn thread_created_ack_is_durable_and_survives_reopen() -> Result<()> {
        let root = std::env::temp_dir().join(format!("deepseek-task-test-{}", Uuid::new_v4()));
        let saw_durable_thread = Arc::new(AtomicBool::new(false));
        let manager = TaskManager::start_with_executor(
            test_config(root.clone()),
            Arc::new(DurabilityCheckingExecutor {
                root: root.clone(),
                saw_durable_thread: Arc::clone(&saw_durable_thread),
            }),
        )
        .await?;

        let task = manager
            .add_task(NewTaskRequest::from_prompt(
                "persist thread before continuing",
            ))
            .await?;
        let _ = wait_for_terminal_state(&manager, &task.id, Duration::from_secs(10)).await?;
        assert!(
            saw_durable_thread.load(Ordering::SeqCst),
            "executor continued only after the task file contained thread_id"
        );
        manager.shutdown();
        drop(manager);

        let reopened =
            TaskManager::start_with_executor(test_config(root), Arc::new(MockExecutor)).await?;
        let recovered = reopened.get_task(&task.id).await?;
        assert_eq!(recovered.thread_id.as_deref(), Some("thr_durable_ack"));
        reopened.shutdown();
        Ok(())
    }

    #[tokio::test]
    async fn idempotency_key_returns_one_durable_task() -> Result<()> {
        let root = std::env::temp_dir().join(format!("deepseek-task-test-{}", Uuid::new_v4()));
        let manager =
            TaskManager::start_with_executor(test_config(root), Arc::new(MockExecutor)).await?;

        let first = manager
            .add_task_with_idempotency_key(
                NewTaskRequest::from_prompt("scheduled work"),
                "automation:abc:2026-07-10T00:00:00Z",
            )
            .await?;
        let second = manager
            .add_task_with_idempotency_key(
                NewTaskRequest::from_prompt("scheduled work"),
                "automation:abc:2026-07-10T00:00:00Z",
            )
            .await?;

        assert_eq!(first.id, second.id);
        assert_eq!(manager.list_tasks(None).await.len(), 1);
        manager.shutdown();
        Ok(())
    }

    #[tokio::test]
    async fn idempotency_index_is_updated_and_rebuilt_on_reopen() -> Result<()> {
        let tempdir = tempfile::tempdir()?;
        let root = tempdir.path().to_path_buf();
        let key = "automation:indexed:2026-07-10T00:00:00Z";
        let manager =
            TaskManager::start_with_executor(test_config(root.clone()), Arc::new(MockExecutor))
                .await?;
        manager.shutdown();

        let first = manager
            .add_task_with_idempotency_key(NewTaskRequest::from_prompt("indexed work"), key)
            .await?;
        {
            let state = manager.state.lock().await;
            assert_eq!(state.idempotency_index.get(key), Some(&first.id));
        }
        drop(manager);

        let reopened =
            TaskManager::start_with_executor(test_config(root), Arc::new(MockExecutor)).await?;
        reopened.shutdown();
        {
            let state = reopened.state.lock().await;
            assert_eq!(state.idempotency_index.get(key), Some(&first.id));
        }
        let repeated = reopened
            .add_task_with_idempotency_key(NewTaskRequest::from_prompt("ignored retry"), key)
            .await?;

        assert_eq!(repeated.id, first.id);
        assert_eq!(reopened.list_tasks(None).await.len(), 1);
        Ok(())
    }

    #[tokio::test]
    async fn failed_enqueue_persistence_leaves_no_in_memory_task() -> Result<()> {
        let tempdir = tempfile::tempdir()?;
        let root = tempdir.path().to_path_buf();
        let key = "automation:retry-after-write-failure";
        let manager =
            TaskManager::start_with_executor(test_config(root.clone()), Arc::new(MockExecutor))
                .await?;
        manager.shutdown();
        let probe = Arc::new(TestPersistenceProbe::default());
        manager.install_persistence_probe(Arc::clone(&probe));
        probe.fail_next_task_write.store(true, Ordering::SeqCst);

        let failed = manager
            .add_task_with_idempotency_key(NewTaskRequest::from_prompt("retry me"), key)
            .await;

        assert!(failed.is_err(), "the injected task write must fail the add");
        assert!(
            manager.list_tasks(None).await.is_empty(),
            "a failed durable write must not publish a task in memory"
        );
        {
            let state = manager.state.lock().await;
            assert!(state.queue.is_empty());
            assert!(!state.idempotency_index.contains_key(key));
        }

        let retried = manager
            .add_task_with_idempotency_key(NewTaskRequest::from_prompt("retry me"), key)
            .await?;
        assert!(
            root.join("tasks")
                .join(format!("{}.json", retried.id))
                .is_file()
        );
        drop(manager);

        let reopened =
            TaskManager::start_with_executor(test_config(root), Arc::new(MockExecutor)).await?;
        reopened.shutdown();
        let recovered = reopened
            .add_task_with_idempotency_key(NewTaskRequest::from_prompt("ignored"), key)
            .await?;
        assert_eq!(recovered.id, retried.id);
        assert_eq!(reopened.list_tasks(None).await.len(), 1);
        Ok(())
    }

    #[tokio::test]
    async fn queue_cache_failure_returns_durable_task_and_reopens_once() -> Result<()> {
        let tempdir = tempfile::tempdir()?;
        let root = tempdir.path().to_path_buf();
        let key = "automation:queue-cache-failure";
        let manager =
            TaskManager::start_with_executor(test_config(root.clone()), Arc::new(MockExecutor))
                .await?;
        manager.shutdown();
        let probe = Arc::new(TestPersistenceProbe::default());
        manager.install_persistence_probe(Arc::clone(&probe));
        probe.fail_next_queue_write.store(true, Ordering::SeqCst);

        let task = manager
            .add_task_with_idempotency_key(NewTaskRequest::from_prompt("durable work"), key)
            .await
            .expect("queue cache failure must not fail a durable task add");
        assert!(
            root.join("tasks")
                .join(format!("{}.json", task.id))
                .is_file()
        );
        assert_eq!(
            probe.queue_writes.load(Ordering::SeqCst),
            1,
            "add must attempt to refresh the queue ordering cache"
        );
        assert!(
            !probe.fail_next_queue_write.load(Ordering::SeqCst),
            "the injected queue failure must be consumed"
        );
        drop(manager);

        let (loaded_tasks, recovered_queue) =
            load_state(&root.join("tasks"), &root.join("queue.json"))?;
        assert!(loaded_tasks.contains_key(&task.id));
        assert_eq!(
            recovered_queue
                .iter()
                .filter(|queued_id| *queued_id == &task.id)
                .count(),
            1,
            "load_state must rebuild the missing queued task exactly once"
        );

        let reopened =
            TaskManager::start_with_executor(test_config(root), Arc::new(MockExecutor)).await?;
        reopened.shutdown();
        let repeated = reopened
            .add_task_with_idempotency_key(NewTaskRequest::from_prompt("ignored retry"), key)
            .await?;
        assert_eq!(repeated.id, task.id);
        assert_eq!(reopened.list_tasks(None).await.len(), 1);
        Ok(())
    }

    #[tokio::test]
    async fn ten_adds_write_exactly_ten_task_records() -> Result<()> {
        let tempdir = tempfile::tempdir()?;
        let manager = TaskManager::start_with_executor(
            test_config(tempdir.path().to_path_buf()),
            Arc::new(MockExecutor),
        )
        .await?;
        manager.shutdown();
        let probe = Arc::new(TestPersistenceProbe::default());
        manager.install_persistence_probe(Arc::clone(&probe));

        for index in 0..10 {
            manager
                .add_task(NewTaskRequest::from_prompt(format!("task {index}")))
                .await?;
        }

        assert_eq!(
            probe.task_writes.load(Ordering::SeqCst),
            10,
            "each add must persist only its new task record"
        );
        assert_eq!(manager.list_tasks(None).await.len(), 10);
        Ok(())
    }

    #[tokio::test]
    async fn executor_non_terminal_results_fail_closed() -> Result<()> {
        for returned_status in [TaskStatus::Queued, TaskStatus::Running] {
            let root = std::env::temp_dir().join(format!("deepseek-task-test-{}", Uuid::new_v4()));
            let manager = TaskManager::start_with_executor(
                test_config(root),
                Arc::new(NonTerminalExecutor {
                    status: returned_status,
                }),
            )
            .await?;
            let task = manager
                .add_task(NewTaskRequest::from_prompt("return a terminal result"))
                .await?;

            let deadline = std::time::Instant::now() + Duration::from_secs(2);
            let finished = loop {
                let current = manager.get_task(&task.id).await?;
                if current.ended_at.is_some() {
                    break current;
                }
                if std::time::Instant::now() >= deadline {
                    bail!("executor result was not finalized");
                }
                sleep(Duration::from_millis(10)).await;
            };

            assert_eq!(finished.status, TaskStatus::Failed);
            assert!(
                finished
                    .error
                    .as_deref()
                    .is_some_and(|error| error.contains("non-terminal"))
            );
            manager.shutdown();
        }
        Ok(())
    }

    #[tokio::test]
    async fn add_task_rejects_mode_outside_persisted_mode_set() -> Result<()> {
        let root = std::env::temp_dir().join(format!("deepseek-task-test-{}", Uuid::new_v4()));
        let manager =
            TaskManager::start_with_executor(test_config(root), Arc::new(MockExecutor)).await?;
        let mut request = NewTaskRequest::from_prompt("invalid persisted mode");
        request.mode = Some("observer".to_string());

        let error = manager
            .add_task(request)
            .await
            .expect_err("unknown modes must not cross the persistence boundary");

        assert!(error.to_string().contains("agent, plan, yolo"));
        assert!(manager.list_tasks(None).await.is_empty());
        manager.shutdown();
        Ok(())
    }

    #[tokio::test]
    async fn persists_and_recovers_task_records() -> Result<()> {
        let root = std::env::temp_dir().join(format!("deepseek-task-test-{}", Uuid::new_v4()));
        let manager =
            TaskManager::start_with_executor(test_config(root.clone()), Arc::new(MockExecutor))
                .await?;

        let task = manager
            .add_task(NewTaskRequest::from_prompt("test persistence"))
            .await?;
        let finished = wait_for_terminal_state(&manager, &task.id, Duration::from_secs(10)).await?;
        assert_eq!(finished.status, TaskStatus::Completed);
        assert_eq!(finished.thread_id.as_deref(), Some("thr_test"));
        assert_eq!(finished.turn_id.as_deref(), Some("turn_test"));
        assert!(finished.timeline.iter().any(|entry| {
            entry.kind == "runtime_thread" && entry.summary == "Linked runtime thread thr_test"
        }));
        assert_eq!(finished.checklist.items.len(), 1);
        assert_eq!(finished.checklist.in_progress_id, Some(1));

        drop(manager);

        let recovered =
            TaskManager::start_with_executor(test_config(root.clone()), Arc::new(MockExecutor))
                .await?;
        let loaded = recovered.get_task(&task.id).await?;
        assert_eq!(loaded.status, TaskStatus::Completed);
        assert!(!loaded.timeline.is_empty());
        assert_eq!(loaded.checklist.items[0].content, "read fixture");
        Ok(())
    }

    #[test]
    fn running_tasks_are_not_requeued_after_restart() -> Result<()> {
        let root = std::env::temp_dir().join(format!("deepseek-task-test-{}", Uuid::new_v4()));
        let tasks_dir = root.join("tasks");
        fs::create_dir_all(&tasks_dir)?;
        let queue_path = root.join("queue.json");
        let task_id = "task_stale_running".to_string();
        let started_at = Utc::now() - chrono::Duration::seconds(30);
        let task = TaskRecord {
            schema_version: CURRENT_TASK_SCHEMA_VERSION,
            id: task_id.clone(),
            prompt: "long-running shell work".to_string(),
            model: "deepseek-v4-flash".to_string(),
            workspace: PathBuf::from("."),
            mode: "agent".to_string(),
            allow_shell: true,
            trust_mode: false,
            auto_approve: false,
            idempotency_key: None,
            status: TaskStatus::Running,
            created_at: started_at,
            started_at: Some(started_at),
            ended_at: None,
            duration_ms: None,
            result_summary: None,
            result_detail_path: None,
            error: None,
            thread_id: Some("thr_stale".to_string()),
            turn_id: Some("turn_stale".to_string()),
            runtime_event_count: 0,
            checklist: TaskChecklistState::default(),
            gates: Vec::new(),
            attempts: Vec::new(),
            artifacts: Vec::new(),
            github_events: Vec::new(),
            tool_calls: vec![TaskToolCallSummary {
                id: "tool_shell".to_string(),
                name: "task_shell_start".to_string(),
                status: TaskToolStatus::Running,
                started_at,
                ended_at: None,
                duration_ms: None,
                input_summary: Some("shell: sleep 999".to_string()),
                output_summary: None,
                detail_path: None,
                patch_ref: None,
            }],
            timeline: vec![TaskTimelineEntry {
                timestamp: started_at,
                kind: "running".to_string(),
                summary: "Task started".to_string(),
                detail_path: None,
            }],
        };
        fs::write(
            tasks_dir.join(format!("{task_id}.json")),
            serde_json::to_string_pretty(&task)?,
        )?;
        fs::write(
            &queue_path,
            serde_json::to_string_pretty(&QueueFile {
                queue: vec![task_id.clone()],
            })?,
        )?;

        let (tasks, queue) = load_state(&tasks_dir, &queue_path)?;
        let recovered = tasks.get(&task_id).expect("task loaded");

        assert!(queue.is_empty(), "stale running task must not be requeued");
        assert_eq!(recovered.status, TaskStatus::Failed);
        assert!(
            recovered
                .error
                .as_deref()
                .is_some_and(|err| err.contains("prior process is not attached")),
            "recovered task should explain stale process ownership: {recovered:?}"
        );
        assert!(recovered.ended_at.is_some());
        assert!(recovered.duration_ms.is_some());
        assert_eq!(recovered.tool_calls[0].status, TaskToolStatus::Failed);
        assert!(recovered.tool_calls[0].ended_at.is_some());
        assert!(
            recovered
                .timeline
                .iter()
                .any(|entry| entry.kind == "recovered"
                    && entry.summary.contains("prior process is not attached")),
            "recovery timeline should explain why the task is terminal: {:?}",
            recovered.timeline
        );
        Ok(())
    }

    #[tokio::test]
    async fn default_workspace_updates_for_future_tasks() -> Result<()> {
        let root = std::env::temp_dir().join(format!("deepseek-task-test-{}", Uuid::new_v4()));
        let new_workspace =
            std::env::temp_dir().join(format!("deepseek-workspace-{}", Uuid::new_v4()));
        let manager =
            TaskManager::start_with_executor(test_config(root), Arc::new(MockExecutor)).await?;

        manager.set_default_workspace(new_workspace.clone()).await;
        let task = manager
            .add_task(NewTaskRequest::from_prompt("test workspace default"))
            .await?;

        assert_eq!(manager.default_workspace().await, new_workspace);
        assert_eq!(task.workspace, new_workspace);
        Ok(())
    }

    #[tokio::test]
    async fn record_tool_metadata_updates_explicit_task() -> Result<()> {
        let root = std::env::temp_dir().join(format!("deepseek-task-test-{}", Uuid::new_v4()));
        let manager =
            TaskManager::start_with_executor(test_config(root), Arc::new(MockExecutor)).await?;

        let task = manager
            .add_task(NewTaskRequest::from_prompt("test metadata"))
            .await?;
        let finished = wait_for_terminal_state(&manager, &task.id, Duration::from_secs(10)).await?;
        let updated = manager
            .record_tool_metadata(
                &finished.id,
                &serde_json::json!({
                    "task_updates": {
                        "gate": {
                            "id": "gate_test",
                            "gate": "test",
                            "command": "cargo test -p codewhale-tui --lib",
                            "cwd": ".",
                            "exit_code": 0,
                            "status": "passed",
                            "classification": "passed",
                            "duration_ms": 1,
                            "summary": "ok",
                            "log_path": null,
                            "recorded_at": Utc::now()
                        }
                    }
                }),
            )
            .await?;

        assert_eq!(updated.gates.len(), 1);
        assert_eq!(updated.gates[0].classification, "passed");
        Ok(())
    }

    #[tokio::test]
    async fn write_task_artifact_rejects_traversal_task_id() -> Result<()> {
        let temp = tempfile::tempdir()?;
        let root = temp.path().join("tasks-root");
        let escaped = temp.path().join("escape");
        let manager =
            TaskManager::start_with_executor(test_config(root.clone()), Arc::new(MockExecutor))
                .await?;

        let err = manager
            .write_task_artifact("../escape", "result", "artifact body")
            .expect_err("traversal task ids must be rejected");

        assert!(err.to_string().contains("single path component"));
        assert!(!escaped.exists(), "artifact write escaped the task root");
        Ok(())
    }

    #[tokio::test]
    async fn cancel_running_task_marks_canceled() -> Result<()> {
        let root = std::env::temp_dir().join(format!("deepseek-task-test-{}", Uuid::new_v4()));
        let manager =
            TaskManager::start_with_executor(test_config(root), Arc::new(MockExecutor)).await?;

        let task = manager
            .add_task(NewTaskRequest::from_prompt("test cancellation"))
            .await?;

        sleep(Duration::from_millis(10)).await;
        let _ = manager.cancel_task(&task.id).await?;
        let finished = wait_for_terminal_state(&manager, &task.id, Duration::from_secs(10)).await?;
        assert_eq!(finished.status, TaskStatus::Canceled);
        Ok(())
    }

    // GHSA-72w5-pf8h-xfp4 — regression: omitted optional fields must not
    // silently elevate the spawned task's privileges.
    #[tokio::test]
    async fn add_task_without_optional_fields_does_not_grant_shell_or_auto_approve() -> Result<()> {
        let root = std::env::temp_dir().join(format!("deepseek-task-test-{}", Uuid::new_v4()));
        let manager =
            TaskManager::start_with_executor(test_config(root.clone()), Arc::new(MockExecutor))
                .await?;

        let req = NewTaskRequest {
            prompt: "fix TODOs and write a README".to_string(),
            model: None,
            workspace: None,
            mode: None,
            allow_shell: None,
            trust_mode: None,
            auto_approve: None,
        };
        let task = manager.add_task(req).await?;

        assert!(
            !task.allow_shell,
            "model-omitted allow_shell must default to false (no silent shell grant)"
        );
        assert!(
            !task.auto_approve,
            "model-omitted auto_approve must default to false (no silent auto-approval)"
        );
        assert!(
            !task.trust_mode,
            "model-omitted trust_mode must default to false"
        );
        Ok(())
    }

    #[test]
    fn legacy_task_without_auto_approve_deserializes_fail_closed() -> Result<()> {
        let started_at = Utc::now();
        let task = TaskRecord {
            schema_version: CURRENT_TASK_SCHEMA_VERSION,
            id: "task_legacy_policy".to_string(),
            prompt: "legacy task".to_string(),
            model: "legacy-model".to_string(),
            workspace: PathBuf::from("."),
            mode: "agent".to_string(),
            allow_shell: false,
            trust_mode: false,
            auto_approve: false,
            idempotency_key: None,
            status: TaskStatus::Completed,
            created_at: started_at,
            started_at: Some(started_at),
            ended_at: Some(started_at),
            duration_ms: Some(0),
            result_summary: None,
            result_detail_path: None,
            error: None,
            thread_id: None,
            turn_id: None,
            runtime_event_count: 0,
            checklist: TaskChecklistState::default(),
            gates: Vec::new(),
            attempts: Vec::new(),
            artifacts: Vec::new(),
            github_events: Vec::new(),
            tool_calls: Vec::new(),
            timeline: Vec::new(),
        };
        let mut value = serde_json::to_value(task)?;
        value
            .as_object_mut()
            .expect("task is an object")
            .remove("auto_approve");

        let recovered: TaskRecord = serde_json::from_value(value)?;
        assert!(!recovered.auto_approve);
        Ok(())
    }

    #[tokio::test]
    async fn rejects_newer_task_schema_on_recovery() -> Result<()> {
        let root = std::env::temp_dir().join(format!("deepseek-task-test-{}", Uuid::new_v4()));
        let manager =
            TaskManager::start_with_executor(test_config(root.clone()), Arc::new(MockExecutor))
                .await?;

        let task = manager
            .add_task(NewTaskRequest::from_prompt("test schema gate"))
            .await?;
        let _ = wait_for_terminal_state(&manager, &task.id, Duration::from_secs(10)).await?;
        drop(manager);

        let task_path = root.join("tasks").join(format!("{}.json", task.id));
        let mut value: serde_json::Value = serde_json::from_str(&fs::read_to_string(&task_path)?)?;
        value["schema_version"] = serde_json::json!(999);
        fs::write(&task_path, serde_json::to_string_pretty(&value)?)?;

        match TaskManager::start_with_executor(test_config(root), Arc::new(MockExecutor)).await {
            Ok(_) => panic!("manager should reject newer task schema"),
            Err(err) => assert!(err.to_string().contains("newer than supported")),
        }
        Ok(())
    }

    #[tokio::test]
    async fn reporter_failure_cancels_token() -> Result<()> {
        let closed_cancel = CancellationToken::new();
        let (closed_tx, closed_rx) = mpsc::unbounded_channel();
        drop(closed_rx);
        let closed_reporter = TaskExecutionReporter::new(closed_tx, closed_cancel.clone());
        let closed = closed_reporter
            .report(TaskExecutionEvent::Status {
                message: "closed".to_string(),
            })
            .await;
        assert_eq!(closed, Err(TaskExecutionReportError::Closed));
        assert!(closed_cancel.is_cancelled());

        let rejected_cancel = CancellationToken::new();
        let (rejected_tx, mut rejected_rx) = mpsc::unbounded_channel();
        let rejected_reporter = TaskExecutionReporter::new(rejected_tx, rejected_cancel.clone());
        let rejected_task = tokio::spawn(async move {
            rejected_reporter
                .report(TaskExecutionEvent::Status {
                    message: "rejected".to_string(),
                })
                .await
        });
        rejected_rx
            .recv()
            .await
            .expect("pending rejected report")
            .ack
            .send(Err("disk full".to_string()))
            .expect("send rejected ack");
        assert_eq!(
            rejected_task.await?,
            Err(TaskExecutionReportError::Rejected("disk full".to_string()))
        );
        assert!(rejected_cancel.is_cancelled());

        let dropped_cancel = CancellationToken::new();
        let (dropped_tx, mut dropped_rx) = mpsc::unbounded_channel();
        let dropped_reporter = TaskExecutionReporter::new(dropped_tx, dropped_cancel.clone());
        let dropped_task = tokio::spawn(async move {
            dropped_reporter
                .report(TaskExecutionEvent::Status {
                    message: "drop ack".to_string(),
                })
                .await
        });
        drop(dropped_rx.recv().await.expect("pending dropped report"));
        assert_eq!(
            dropped_task.await?,
            Err(TaskExecutionReportError::AcknowledgementDropped)
        );
        assert!(dropped_cancel.is_cancelled());
        Ok(())
    }

    #[tokio::test]
    async fn thread_link_rejection_interrupts_once() -> Result<()> {
        let runtime = Arc::new(FakeTaskTurnRuntime::new([]));
        let cancel = CancellationToken::new();
        let (tx, mut rx) = mpsc::unbounded_channel();
        let reporter = TaskExecutionReporter::new(tx, cancel.clone());
        let runtime_for_task = Arc::clone(&runtime);
        let cancel_for_task = cancel.clone();
        let execution = tokio::spawn(async move {
            drive_active_turn(
                runtime_for_task.as_ref(),
                "task_active",
                "thr_active",
                "turn_active",
                &reporter,
                &cancel_for_task,
            )
            .await
        });

        let pending = rx.recv().await.expect("ThreadLinked report");
        assert!(matches!(
            pending.event,
            TaskExecutionEvent::ThreadLinked { .. }
        ));
        pending
            .ack
            .send(Err("persist link failed".to_string()))
            .expect("send rejected link ack");

        let result = execution.await?;
        assert_eq!(result.status, TaskStatus::Failed);
        assert!(cancel.is_cancelled());
        assert_eq!(runtime.interrupt_calls(), 1);
        Ok(())
    }

    #[tokio::test]
    async fn closed_report_channel_interrupts_once() -> Result<()> {
        let runtime = FakeTaskTurnRuntime::new([]);
        let cancel = CancellationToken::new();
        let (tx, rx) = mpsc::unbounded_channel();
        drop(rx);
        let reporter = TaskExecutionReporter::new(tx, cancel.clone());

        let result = drive_active_turn(
            &runtime,
            "task_active",
            "thr_active",
            "turn_active",
            &reporter,
            &cancel,
        )
        .await;

        assert_eq!(result.status, TaskStatus::Failed);
        assert!(cancel.is_cancelled());
        assert_eq!(runtime.interrupt_calls(), 1);
        Ok(())
    }

    #[tokio::test]
    async fn runtime_event_read_failure_interrupts_once() -> Result<()> {
        let runtime = FakeTaskTurnRuntime::new([Err("corrupt event json".to_string())]);
        let cancel = CancellationToken::new();
        let (reporter, ack_task) = auto_ack_reporter(cancel.clone());

        let result = drive_active_turn(
            &runtime,
            "task_active",
            "thr_active",
            "turn_active",
            &reporter,
            &cancel,
        )
        .await;
        drop(reporter);
        ack_task.await?;

        assert_eq!(result.status, TaskStatus::Failed);
        assert!(
            result
                .error
                .as_deref()
                .is_some_and(|error| error.contains("Failed to read runtime events"))
        );
        assert_eq!(runtime.interrupt_calls(), 1);
        Ok(())
    }

    #[tokio::test]
    async fn successful_terminal_turn_is_not_interrupted() -> Result<()> {
        let runtime = FakeTaskTurnRuntime::new([Ok(vec![terminal_runtime_event("completed")])]);
        let cancel = CancellationToken::new();
        let (reporter, ack_task) = auto_ack_reporter(cancel.clone());

        let result = drive_active_turn(
            &runtime,
            "task_active",
            "thr_active",
            "turn_active",
            &reporter,
            &cancel,
        )
        .await;
        drop(reporter);
        ack_task.await?;

        assert_eq!(result.status, TaskStatus::Completed);
        assert_eq!(runtime.interrupt_calls(), 0);
        Ok(())
    }

    #[tokio::test]
    async fn user_cancellation_interrupts_active_turn_once() -> Result<()> {
        let runtime = FakeTaskTurnRuntime::new([Ok(vec![terminal_runtime_event("canceled")])]);
        let cancel = CancellationToken::new();
        cancel.cancel();
        let (reporter, ack_task) = auto_ack_reporter(cancel.clone());

        let result = drive_active_turn(
            &runtime,
            "task_active",
            "thr_active",
            "turn_active",
            &reporter,
            &cancel,
        )
        .await;
        drop(reporter);
        ack_task.await?;

        assert_eq!(result.status, TaskStatus::Canceled);
        assert_eq!(runtime.interrupt_calls(), 1);
        Ok(())
    }

    #[tokio::test]
    async fn persisted_task_mode_aliases_are_canonicalized() -> Result<()> {
        let tempdir = tempfile::tempdir()?;
        let root = tempdir.path().to_path_buf();
        let seed = TaskManager::start_with_executor(
            test_config(root.clone()),
            Arc::new(CountingExecutor {
                calls: Arc::new(AtomicUsize::new(0)),
                result_text: None,
            }),
        )
        .await?;
        seed.shutdown();

        let aliases = [
            ("agent", " 1 "),
            ("agent", " AgEnT "),
            ("plan", "2"),
            ("plan", " PlAn "),
            ("yolo", " 3 "),
            ("yolo", "YoLo"),
        ];
        let mut task_ids = Vec::new();
        for (canonical, alias) in aliases {
            let mut request = NewTaskRequest::from_prompt(format!("load {alias}"));
            request.mode = Some(canonical.to_string());
            let task = seed.add_task(request).await?;
            overwrite_persisted_task_mode(&root, &task.id, alias)?;
            task_ids.push((task.id, canonical));
        }
        drop(seed);

        let (loaded, _) = load_state(&root.join("tasks"), &root.join("queue.json"))?;
        for (task_id, canonical) in task_ids {
            assert_eq!(
                loaded.get(&task_id).expect("loaded alias task").mode,
                canonical
            );
            let persisted: TaskRecord = serde_json::from_str(&fs::read_to_string(
                root.join("tasks").join(format!("{task_id}.json")),
            )?)?;
            assert_eq!(persisted.mode, canonical);
        }
        Ok(())
    }

    #[tokio::test]
    async fn invalid_mode_isolated_retaining_idempotency() -> Result<()> {
        let tempdir = tempfile::tempdir()?;
        let root = tempdir.path().to_path_buf();
        let seed = TaskManager::start_with_executor(
            test_config(root.clone()),
            Arc::new(CountingExecutor {
                calls: Arc::new(AtomicUsize::new(0)),
                result_text: None,
            }),
        )
        .await?;
        seed.shutdown();
        let healthy = seed
            .add_task(NewTaskRequest::from_prompt("healthy task"))
            .await?;
        let invalid = seed
            .add_task_with_idempotency_key(
                NewTaskRequest::from_prompt("invalid mode task"),
                "invalid-mode-key",
            )
            .await?;
        overwrite_persisted_task_mode(&root, &invalid.id, "observer")?;
        drop(seed);

        let calls = Arc::new(AtomicUsize::new(0));
        let manager = TaskManager::start_with_executor(
            test_config(root.clone()),
            Arc::new(CountingExecutor {
                calls: Arc::clone(&calls),
                result_text: Some("done".to_string()),
            }),
        )
        .await?;
        let completed =
            wait_for_terminal_state(&manager, &healthy.id, Duration::from_secs(5)).await?;
        assert_eq!(completed.status, TaskStatus::Completed);

        let isolated = manager.get_task(&invalid.id).await?;
        assert_eq!(isolated.status, TaskStatus::Failed);
        assert_eq!(isolated.mode, "agent");
        assert_eq!(
            isolated.idempotency_key.as_deref(),
            Some("invalid-mode-key")
        );
        assert!(
            isolated
                .error
                .as_deref()
                .is_some_and(|error| error.contains("Invalid persisted task mode 'observer'"))
        );
        let repeated = manager
            .add_task_with_idempotency_key(
                NewTaskRequest::from_prompt("must reuse isolated task"),
                "invalid-mode-key",
            )
            .await?;
        assert_eq!(repeated.id, invalid.id);
        assert_eq!(calls.load(Ordering::SeqCst), 1);

        let persisted: TaskRecord = serde_json::from_str(&fs::read_to_string(
            root.join("tasks").join(format!("{}.json", invalid.id)),
        )?)?;
        assert_eq!(persisted.status, TaskStatus::Failed);
        assert_eq!(persisted.mode, "agent");
        assert_eq!(
            persisted.idempotency_key.as_deref(),
            Some("invalid-mode-key")
        );
        manager.shutdown();
        Ok(())
    }

    #[tokio::test]
    async fn prune_terminal_tasks_preserves_protected_and_active_tasks() -> Result<()> {
        let tempdir = tempfile::tempdir()?;
        let root = tempdir.path().to_path_buf();
        let manager =
            TaskManager::start_with_executor(test_config(root.clone()), Arc::new(MockExecutor))
                .await?;
        manager.shutdown();

        let prunable = manager
            .add_task_with_idempotency_key(
                NewTaskRequest::from_prompt("prunable terminal"),
                "prunable-key",
            )
            .await?;
        manager.write_task_artifact(&prunable.id, "result", "old artifact")?;
        manager.cancel_task(&prunable.id).await?;

        let protected = manager
            .add_task(NewTaskRequest::from_prompt("protected terminal"))
            .await?;
        manager.write_task_artifact(&protected.id, "result", "keep artifact")?;
        manager.cancel_task(&protected.id).await?;
        let queued = manager
            .add_task(NewTaskRequest::from_prompt("queued task"))
            .await?;

        let protected_ids = HashSet::from([protected.id.clone()]);
        let pruned = manager.prune_terminal_tasks(&protected_ids).await?;

        assert_eq!(pruned, vec![prunable.id.clone()]);
        assert_eq!(manager.task_count().await, 2);
        assert!(manager.get_task(&prunable.id).await.is_err());
        assert!(manager.get_task(&protected.id).await.is_ok());
        assert!(manager.get_task(&queued.id).await.is_ok());
        assert!(
            !root
                .join("tasks")
                .join(format!("{}.json", prunable.id))
                .exists()
        );
        assert!(!root.join("artifacts").join(&prunable.id).exists());
        assert!(root.join("artifacts").join(&protected.id).exists());
        {
            let state = manager.state.lock().await;
            assert!(!state.idempotency_index.contains_key("prunable-key"));
        }
        let replacement = manager
            .add_task_with_idempotency_key(
                NewTaskRequest::from_prompt("replacement"),
                "prunable-key",
            )
            .await?;
        assert_ne!(replacement.id, prunable.id);
        Ok(())
    }

    #[tokio::test]
    async fn startup_finishes_journaled_task_prune() -> Result<()> {
        let tempdir = tempfile::tempdir()?;
        let root = tempdir.path().to_path_buf();
        let manager =
            TaskManager::start_with_executor(test_config(root.clone()), Arc::new(MockExecutor))
                .await?;
        manager.shutdown();
        let task = manager
            .add_task(NewTaskRequest::from_prompt("crash during prune"))
            .await?;
        manager.write_task_artifact(&task.id, "result", "artifact")?;
        manager.cancel_task(&task.id).await?;
        let marker = persist_task_prune_marker(&root.join("tasks"), &task.id)?;
        drop(manager);

        let reopened =
            TaskManager::start_with_executor(test_config(root.clone()), Arc::new(MockExecutor))
                .await?;
        reopened.shutdown();

        assert!(reopened.get_task(&task.id).await.is_err());
        assert!(
            !root
                .join("tasks")
                .join(format!("{}.json", task.id))
                .exists()
        );
        assert!(!root.join("artifacts").join(&task.id).exists());
        assert!(!marker.exists());
        Ok(())
    }

    #[tokio::test]
    async fn load_state_rejects_unsafe_mismatched_and_duplicate_task_ids() -> Result<()> {
        async fn seeded_record(root: &Path) -> Result<(TaskRecord, PathBuf)> {
            let manager = TaskManager::start_with_executor(
                test_config(root.to_path_buf()),
                Arc::new(MockExecutor),
            )
            .await?;
            manager.shutdown();
            let task = manager
                .add_task(NewTaskRequest::from_prompt("identity seed"))
                .await?;
            let path = root.join("tasks").join(format!("{}.json", task.id));
            Ok((task, path))
        }

        let unsafe_root = tempfile::tempdir()?;
        let (mut unsafe_task, unsafe_path) = seeded_record(unsafe_root.path()).await?;
        unsafe_task.id = "../escape".to_string();
        write_json_atomic(&unsafe_path, &unsafe_task)?;
        let unsafe_error = load_state(
            &unsafe_root.path().join("tasks"),
            &unsafe_root.path().join("queue.json"),
        )
        .expect_err("unsafe persisted id must fail closed");
        assert!(unsafe_error.to_string().contains("Invalid task identity"));

        let mismatch_root = tempfile::tempdir()?;
        let (mut mismatch_task, mismatch_path) = seeded_record(mismatch_root.path()).await?;
        mismatch_task.id = "different-safe-id".to_string();
        write_json_atomic(&mismatch_path, &mismatch_task)?;
        let mismatch_error = load_state(
            &mismatch_root.path().join("tasks"),
            &mismatch_root.path().join("queue.json"),
        )
        .expect_err("filename mismatch must fail closed");
        assert!(
            mismatch_error
                .to_string()
                .contains("does not match persisted task id")
        );

        let duplicate_root = tempfile::tempdir()?;
        let (duplicate_task, duplicate_path) = seeded_record(duplicate_root.path()).await?;
        let mut loaded = HashMap::new();
        insert_loaded_task(&mut loaded, duplicate_task.clone(), &duplicate_path)?;
        let duplicate_error = insert_loaded_task(&mut loaded, duplicate_task, &duplicate_path)
            .expect_err("duplicate task id must be rejected");
        assert!(
            duplicate_error
                .to_string()
                .contains("Duplicate persisted task id")
        );
        Ok(())
    }

    #[test]
    fn default_tasks_dir_falls_back_to_legacy_deepseek_tasks() {
        let temp_home = tempfile::tempdir().unwrap();
        let home = temp_home.path();
        let legacy_tasks = home.join(".deepseek").join("tasks");
        std::fs::create_dir_all(&legacy_tasks).unwrap();

        assert_eq!(default_tasks_dir_for_home(home), legacy_tasks);
    }

    #[test]
    fn default_tasks_dir_prefers_existing_codewhale_tasks() {
        let temp_home = tempfile::tempdir().unwrap();
        let home = temp_home.path();
        let primary_tasks = home.join(".codewhale").join("tasks");
        let legacy_tasks = home.join(".deepseek").join("tasks");
        std::fs::create_dir_all(&primary_tasks).unwrap();
        std::fs::create_dir_all(&legacy_tasks).unwrap();

        assert_eq!(default_tasks_dir_for_home(home), primary_tasks);
    }

    #[test]
    fn default_tasks_dir_falls_back_to_legacy_when_primary_is_file() {
        let temp_home = tempfile::tempdir().unwrap();
        let home = temp_home.path();
        let primary_tasks = home.join(".codewhale").join("tasks");
        let legacy_tasks = home.join(".deepseek").join("tasks");
        std::fs::create_dir_all(primary_tasks.parent().unwrap()).unwrap();
        std::fs::write(&primary_tasks, "not a directory").unwrap();
        std::fs::create_dir_all(&legacy_tasks).unwrap();

        assert_eq!(default_tasks_dir_for_home(home), legacy_tasks);
    }

    #[test]
    fn default_tasks_dir_ignores_legacy_file_for_new_installs() {
        let temp_home = tempfile::tempdir().unwrap();
        let home = temp_home.path();
        let primary_tasks = home.join(".codewhale").join("tasks");
        let legacy_tasks = home.join(".deepseek").join("tasks");
        std::fs::create_dir_all(legacy_tasks.parent().unwrap()).unwrap();
        std::fs::write(&legacy_tasks, "not a directory").unwrap();

        assert_eq!(default_tasks_dir_for_home(home), primary_tasks);
    }

    #[test]
    fn default_tasks_dir_uses_codewhale_tasks_for_new_installs() {
        let temp_home = tempfile::tempdir().unwrap();
        let home = temp_home.path();

        assert_eq!(
            default_tasks_dir_for_home(home),
            home.join(".codewhale").join("tasks")
        );
    }
}
