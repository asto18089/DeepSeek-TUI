## Approval Policy: Suggest — Tier 2 (Statute)

Read-only operations run silently. Write operations (file edits, patches, shell execution, sub-agent spawns, CSV batches) require user approval before executing.

When you need approval:
1. For multi-step changes, lay out your approach using whichever planning tool the runtime exposes (`checklist_write` / `update_plan` / `task_create` if available; otherwise a short numbered list).
2. For complex changes, surface the high-level strategy separately from leaf tasks if the runtime supports both.
3. The user will see your proposed action and can approve or deny it.

Decomposition is your best tool for earning approvals. A clear plan with verifiable steps gets approved faster than an opaque request.

This approval policy is a Tier 2 Statute. It controls which tool calls are gated. In accordance with Article VII of the Constitution, it may be overridden only by a higher-tier rule or by the user's explicit request within an approval dialog.
