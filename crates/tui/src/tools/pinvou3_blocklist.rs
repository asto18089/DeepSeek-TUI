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
    "agent_spawn", // agent_open 的新版本（fork_context=true 路径）
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
    // 状态管理 - goal（GUI 单 session 不需要持久化目标跟踪）
    "create_goal",
    "get_goal",
    "update_goal",
    // git（模型用 `exec_shell git ...` 替代；git_status/git_diff 已释放）
    "git_log",
    "git_show",
    "git_blame",
    // patch / fim（edit_file 已覆盖；patch DSL 弱模型用不好；revert_turn 已释放）
    "apply_patch",
    "fim_edit",
    // 附件预处理（应移到 bridge 上传 pipeline，见文档 §6.1）
    "pandoc_convert",
    "image_ocr",
    // image_analyze 已放出:Qwen3.6 实测有视觉(2026-05-28),vision_config 指向同一 vllm 端点,
    // 用户附图后由 LLM 调 image_analyze(workspace 相对路径)读图。
    // todo 兼容别名（保留 checklist_*；todo_* 是 v0.8.x 之前的 legacy alias）
    "todo_write",
    "todo_add",
    "todo_update",
    "todo_list",
    // Shell 后台管理 + 异步交互变体（exec_shell_wait 已释放供后台轮询）
    "exec_shell_cancel",
    "exec_shell_interact",
    "exec_wait",
    "exec_interact",
    // Automation 持久化（pinvou3 单 session 不需要）
    "automation_create",
    "automation_delete",
    "automation_list",
    "automation_pause",
    "automation_read",
    "automation_resume",
    "automation_run",
    "automation_update",
    // GitHub 集成（普通用户用不到，开发者用 exec_shell gh 替代）
    "github_issue_context",
    "github_pr_context",
    "github_comment",
    "github_close_issue",
    // 杂项 - 金融数据 + 旧版 web_run（保留 fetch_url）
    "finance",
    "web.run",
    // 元工具（pinvou3 普通用户场景用不到；diagnostics 已释放）
    "multi_tool_use.parallel",
    "note",
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
    // v0.8.53 sync 后补漏(2026-06-04 clean re-fork dump 发现这些新/漏工具暴露给了 LLM):
    // 语音(Xiaomi MiMo,v0.8.53 新增,pinvou3 用不到)
    "speech",
    "tts",
    // rlm 会话对象(漏网:rlm_open/eval/configure/close 已隐藏,就它漏了)
    "rlm_session_objects",
    // github(漏网:其余 github_* 已隐藏)
    "github_close_pr",
    // 验证(run_tests 已隐藏;verifier ensemble pinvou3 不用)
    "run_verifiers",
    // 反 slop ledger(底座内部 anti-slop 机制,非 pinvou3 用户工具)
    "slop_ledger_append",
    "slop_ledger_export",
    "slop_ledger_query",
    "slop_ledger_update",
];

/// 工具名是否在 pinvou3 隐藏清单内。
///
/// **测试豁免**: `PINVOU3_BLOCKLIST_OVERRIDE` env var 列出的工具名(逗号分隔)
/// 即便在 PINVOU3_HIDDEN_TOOLS 里也返回 false。供 L1 dialog harness 临时启用
/// 特定工具评估能力(例:`PINVOU3_BLOCKLIST_OVERRIDE=agent_spawn,agent_eval`)。
/// 生产场景不 set 这个 env,blocklist 行为不变。
#[inline]
pub fn is_pinvou3_hidden(name: &str) -> bool {
    if !PINVOU3_HIDDEN_TOOLS.contains(&name) {
        return false;
    }
    if let Ok(override_list) = std::env::var("PINVOU3_BLOCKLIST_OVERRIDE") {
        if override_list.split(',').any(|t| t.trim() == name) {
            return false;
        }
    }
    true
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
        assert!(!is_pinvou3_hidden("append_file"));
        assert!(!is_pinvou3_hidden("exec_shell"));
        assert!(!is_pinvou3_hidden("web_search"));
        assert!(!is_pinvou3_hidden("checklist_write"));
        assert!(!is_pinvou3_hidden("update_plan"));
        // 视觉:Qwen3.6 有视觉能力,image_analyze 已放出供 LLM 读用户附图
        assert!(!is_pinvou3_hidden("image_analyze"));
    }

    #[test]
    fn hides_legacy_todo_aliases_keeps_checklist() {
        assert!(is_pinvou3_hidden("todo_write"));
        assert!(is_pinvou3_hidden("todo_add"));
        assert!(!is_pinvou3_hidden("checklist_write"));
        assert!(!is_pinvou3_hidden("checklist_add"));
    }

    /// L1 harness 用 PINVOU3_BLOCKLIST_OVERRIDE 临时启用工具评估能力。
    /// 生产场景不 set 这个 env,blocklist 行为不变。
    #[test]
    fn env_override_unhides_listed_tools() {
        // SAFETY: 测试是 single-threaded(`cargo test` 单文件内串行),
        // 且测试函数末尾 remove_var 复原。2024 edition std::env::set_var
        // 标 unsafe 因多线程 race,本场景不 race。
        unsafe {
            // baseline: agent_spawn 在 blocklist 里
            std::env::remove_var("PINVOU3_BLOCKLIST_OVERRIDE");
            assert!(is_pinvou3_hidden("agent_spawn"));

            // 设 env 解锁 agent_spawn + agent_eval
            std::env::set_var("PINVOU3_BLOCKLIST_OVERRIDE", "agent_spawn, agent_eval");
            assert!(
                !is_pinvou3_hidden("agent_spawn"),
                "agent_spawn 应被 env 豁免"
            );
            assert!(!is_pinvou3_hidden("agent_eval"), "agent_eval 应被 env 豁免");
            // 未列出的工具仍隐藏
            assert!(
                is_pinvou3_hidden("task_create"),
                "task_create 未列入 override,仍隐藏"
            );
            // 核心工具不受影响
            assert!(!is_pinvou3_hidden("write_file"));

            std::env::remove_var("PINVOU3_BLOCKLIST_OVERRIDE");
        }
    }
}
