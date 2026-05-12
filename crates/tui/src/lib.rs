//! DeepSeek-TUI library — pinvou-platform 复用的底层能力。

pub mod artifacts;
pub mod audit;
pub mod auto_reasoning;
pub mod automation_manager;
pub mod child_env;
pub mod client;
pub mod command_safety;
pub mod commands;
pub mod compaction;
pub mod composer_history;
pub mod composer_stash;
pub mod config;
pub mod config_ui;
pub mod core;
pub mod cost_status;
pub mod cycle_manager;
pub mod deepseek_theme;
pub mod error_taxonomy;
pub mod eval;
pub mod execpolicy;
pub mod features;
pub mod handoff;
pub mod hooks;
pub mod llm_client;
pub mod localization;
pub mod logging;
pub mod lsp;
pub mod mcp;
pub mod mcp_server;
pub mod memory;
pub mod models;
pub mod network_policy;
pub mod palette;
pub mod pricing;
pub mod project_context;
pub mod project_doc;
pub mod prompts;
pub mod repl;
pub mod retry_status;
pub mod rlm;
pub mod runtime_api;
pub mod runtime_log;
pub mod runtime_threads;
pub mod sandbox;
pub mod schema_migration;
pub mod seam_manager;
pub mod session_manager;
pub mod settings;
pub mod skill_state;
pub mod skills;
pub mod snapshot;
pub mod task_manager;
pub mod tools;
pub mod tui;
pub mod utils;
pub mod working_set;
pub mod workspace_trust;

// main.rs 中定义的类型和函数，镜像到 lib 上下文
// 注意: CliAutoRoute.reasoning_effort 在 main.rs 是 Option<ReasoningEffort>，
// 在 lib 上下文用 String 避免引入 tui::app::ReasoningEffort 的类型耦合。
// acp_server.rs 在 binary 上下文使用 main.rs 版本，不受影响。

pub struct CliAutoRoute {
    pub model: String,
    pub reasoning_effort: Option<String>,
    pub auto_model: bool,
}

pub async fn resolve_cli_auto_route(
    config: &config::Config,
    model: &str,
    prompt: &str,
) -> CliAutoRoute {
    use commands::resolve_auto_route_with_flash;
    if model.trim().eq_ignore_ascii_case("auto") {
        let selection = resolve_auto_route_with_flash(config, prompt, "", "auto", "auto").await;
        CliAutoRoute {
            model: selection.model,
            reasoning_effort: selection.reasoning_effort.map(|e| e.as_setting().to_string()),
            auto_model: true,
        }
    } else {
        CliAutoRoute {
            model: model.to_string(),
            reasoning_effort: None,
            auto_model: false,
        }
    }
}
