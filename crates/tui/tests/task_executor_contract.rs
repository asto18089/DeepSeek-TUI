use std::path::Path;

use async_trait::async_trait;
use codewhale_tui::task_manager::{
    ExecutionTask, TaskExecutionEvent, TaskExecutionReporter, TaskExecutionResult, TaskExecutor,
    TaskStatus,
};
use tokio_util::sync::CancellationToken;

struct DownstreamExecutor;

#[async_trait]
impl TaskExecutor for DownstreamExecutor {
    async fn execute(
        &self,
        task: ExecutionTask,
        reporter: TaskExecutionReporter,
        _cancel: CancellationToken,
    ) -> TaskExecutionResult {
        let _: &str = task.id();
        let _: &str = task.prompt();
        let _: &str = task.model();
        let _: &Path = task.workspace();
        let _: &str = task.mode_label();
        let _: bool = task.allow_shell();
        let _: bool = task.trust_mode();
        let _: bool = task.auto_approve();

        if let Err(err) = reporter
            .report(TaskExecutionEvent::ThreadCreated {
                thread_id: "sched-contract".to_string(),
            })
            .await
        {
            return TaskExecutionResult {
                status: TaskStatus::Failed,
                result_text: None,
                error: Some(format!("failed to persist executor event: {err}")),
            };
        }

        TaskExecutionResult {
            status: TaskStatus::Completed,
            result_text: None,
            error: None,
        }
    }
}

#[test]
fn downstream_executor_can_compile_against_the_public_contract() {
    fn assert_executor<T: TaskExecutor>() {}
    assert_executor::<DownstreamExecutor>();
}
