//! Operations submitted by the UI to the core engine.
//!
//! These operations flow from the TUI to the engine via a channel,
//! allowing the UI to remain responsive while the engine processes requests.

use crate::compaction::CompactionConfig;
use crate::models::{Message, SystemPrompt};
use crate::tui::app::AppMode;
use crate::tui::approval::ApprovalMode;
use std::path::PathBuf;

/// Operations that can be submitted to the engine.
#[derive(Debug, Clone)]
pub enum Op {
    /// Send a message to the AI
    SendMessage {
        content: String,
        mode: AppMode,
        model: String,
        goal_objective: Option<String>,
        /// Reasoning-effort tier: `"off" | "low" | "medium" | "high" | "max"`.
        /// `None` lets the provider apply its default.
        reasoning_effort: Option<String>,
        /// True when the user selected auto thinking, even though the UI sends
        /// a concrete per-turn value to the model API.
        reasoning_effort_auto: bool,
        /// True when the user selected auto model routing.
        auto_model: bool,
        allow_shell: bool,
        trust_mode: bool,
        auto_approve: bool,
        approval_mode: ApprovalMode,
        translation_enabled: bool,
        show_thinking: bool,
    },

    /// Cancel the current request
    #[allow(dead_code)]
    CancelRequest,

    /// Approve a tool call that requires permission
    #[allow(dead_code)]
    ApproveToolCall { id: String },

    /// Deny a tool call that requires permission
    #[allow(dead_code)]
    DenyToolCall { id: String },

    /// Spawn a sub-agent.
    ///
    /// [pinvou3-fork] Driven by the Harness Loop (Step C) to dispatch a
    /// workflow role as a real isolated sub-agent — not by the model. The
    /// extra fields carry the registry role config so the sub-agent runs as a
    /// `Custom` agent with the role's tool whitelist and step budget.
    /// `#[allow(dead_code)]` stays until the pinvou3 forwarder wires the call
    /// site (stage 3); the base TUI never constructs this variant.
    #[allow(dead_code)]
    SpawnSubAgent {
        prompt: String,
        /// Workflow role id (e.g. `"requirements_analyst"`) — used as the
        /// sub-agent name and for `workflow:agent_state_changed` correlation.
        role_id: String,
        /// Registry tool whitelist for this role. A `Custom` sub-agent
        /// requires a non-empty list (enforced by `build_allowed_tools`).
        allowed_tools: Vec<String>,
        /// Registry `max_steps` (e.g. slide_writer=80). `None` falls back to
        /// the manager default (`DEFAULT_MAX_STEPS`).
        max_steps: Option<u32>,
        /// [pinvou3-fork] 结构化产出 schema(registry.output_schema)。`Some` 时
        /// 强制 SubAgent 走 submit_output 提交合格产出才能结束(docs/SDAN/12)。
        output_schema: Option<serde_json::Value>,
        /// [pinvou3-fork] 写文件型角色完成闸:无结构化 schema 但 registry.outputs 非空时,
        /// SubAgent 必须成功调用 write_file/append_file 才能完成。
        expects_file_output: bool,
    },

    /// List current sub-agents and their status
    ListSubAgents,

    /// Change the operating mode
    #[allow(dead_code)]
    ChangeMode { mode: AppMode },

    /// Update the model being used
    #[allow(dead_code)]
    SetModel { model: String },

    /// Update auto-compaction settings
    SetCompaction { config: CompactionConfig },

    /// Sync engine session state (used for resume/load)
    SyncSession {
        session_id: Option<String>,
        messages: Vec<Message>,
        system_prompt: Option<SystemPrompt>,
        system_prompt_override: bool,
        model: String,
        workspace: PathBuf,
    },

    /// Run context compaction immediately.
    CompactContext,

    /// Edit the last user message: remove the last user+assistant exchange
    /// from the session, then re-send with the new content.
    #[allow(dead_code)]
    EditLastTurn { new_message: String },

    /// Shutdown the engine
    Shutdown,
}
