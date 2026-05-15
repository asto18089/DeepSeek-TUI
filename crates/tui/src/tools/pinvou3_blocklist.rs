//! pinvou3 工具默认隐藏清单（L1.5）。
//!
//! 列在此处的工具仍然注册进 `ToolRegistry`（仍可被 `tool_search` 激活
//! 调用），但 `ToolRegistry::to_api_tools()` 喂给 LLM 时标记
//! `defer_loading = true`，模型默认看不到。
//!
//! 设计动机与维护流程见 `pinvou3/docs/工具表精简方案.md`。
//!
//! 维护要点：上游每次 rebase 后跑漂移检测（文档 §5.3），新工具默认
//! 进 blocklist 再单独评估是否放出来。

/// 默认对 LLM 隐藏的工具名。维护时请保持按 §3.2 的类别分组排列。
pub const PINVOU3_HIDDEN_TOOLS: &[&str] = &[
    // 状态管理 - durable task（GUI 单 session 不需要持久化）
    "task_create",
    "task_list",
    "task_read",
    "task_cancel",
    "task_gate_run",
    "task_shell_start",
    "task_shell_wait",
    // 状态管理 - PR 跟踪（pinvou3 非 CI 工具）
    "pr_attempt_record",
    "pr_attempt_list",
    "pr_attempt_read",
    "pr_attempt_preflight",
    // 状态管理 - subagent（留给后续 workflow 阶段，模型不应直接调）
    "agent_open",
    "agent_eval",
    "agent_result",
    "agent_cancel",
    "agent_close",
    "agent_list",
    "resume_agent",
    "delegate_to_agent",
    // 状态管理 - RLM（无持久 REPL 场景）
    "rlm_open",
    "rlm_eval",
    "rlm_configure",
    "rlm_close",
    // git（模型用 `exec_shell git ...` 替代）
    "git_status",
    "git_diff",
    "git_log",
    "git_show",
    "git_blame",
    // patch / fim（edit_file 已覆盖；patch DSL 弱模型用不好）
    "apply_patch",
    "fim_edit",
    "revert_turn",
    // 附件预处理（应移到 bridge 上传 pipeline，见文档 §6.1）
    "pandoc_convert",
    "image_ocr",
    "image_analyze",
    // todo 兼容别名（保留 checklist_*；todo_* 是 v0.8.x 之前的 legacy alias）
    "todo_write",
    "todo_add",
    "todo_update",
    "todo_list",
    // Shell 后台管理（同步 exec_shell 卡住靠 GUI turn 中断按钮兜底）
    "exec_shell_cancel",
    // 元工具（pinvou3 普通用户场景用不到）
    "multi_tool_use.parallel",
    "note",
    "diagnostics",
    "validate_data",
    "run_tests",
    "handle_read",
    "retrieve_tool_result",
    "project_map",
    "recall_archive",
    "review",
    "notify",
    "remember",
    "web_run",
];

/// 工具名是否在 pinvou3 隐藏清单内。
#[inline]
pub fn is_pinvou3_hidden(name: &str) -> bool {
    PINVOU3_HIDDEN_TOOLS.contains(&name)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hides_known_state_management_tools() {
        assert!(is_pinvou3_hidden("task_create"));
        assert!(is_pinvou3_hidden("agent_open"));
        assert!(is_pinvou3_hidden("rlm_eval"));
        assert!(is_pinvou3_hidden("pr_attempt_record"));
    }

    #[test]
    fn keeps_core_tools_visible() {
        assert!(!is_pinvou3_hidden("read_file"));
        assert!(!is_pinvou3_hidden("write_file"));
        assert!(!is_pinvou3_hidden("exec_shell"));
        assert!(!is_pinvou3_hidden("web_search"));
        assert!(!is_pinvou3_hidden("checklist_write"));
        assert!(!is_pinvou3_hidden("update_plan"));
    }

    #[test]
    fn hides_legacy_todo_aliases_keeps_checklist() {
        assert!(is_pinvou3_hidden("todo_write"));
        assert!(is_pinvou3_hidden("todo_add"));
        assert!(!is_pinvou3_hidden("checklist_write"));
        assert!(!is_pinvou3_hidden("checklist_add"));
    }
}
