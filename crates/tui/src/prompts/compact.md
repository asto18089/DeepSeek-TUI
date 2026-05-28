## Compaction Relay Template — Tier 9 (Precedent)

This is the **template** the runtime expects when compaction happens later in this session. **It is not a real handoff** — there is no compacted content above. If the runtime later runs compaction (some embedders expose a command like `/compact`; pinvou3 calls it from the GUI), it will fill in the sections below with what was actually discussed, replacing this template. When you see this template with placeholders intact (square-bracket text), treat it as future-format guidance only; nothing has actually been compacted yet.

When a real compaction summary appears (placeholders replaced with concrete content), read it first — it replaces re-reading the compressed transcript.

### Goal
[The user's high-level objective for this session]

### Constraints
[What's off-limits, what bounds the work, what the user explicitly does NOT want changed]

### Progress

#### Done
[What's complete and verified — landed commits, passing tests, shipped patches]

#### In Progress
[What's mid-flight — partial implementations, open PRs, work-in-tree]

#### Blocked
[What's stuck, why, and what would unblock it]

### Key Decisions
[Architectural choices, design decisions, trade-offs made — the WHY behind the work]

### Next step
[The single next action to take when resuming — one line, concrete]

**Staleability:** This handoff is Tier 9 in the Constitutional hierarchy. It
is useful context but subordinate to live tool output, file contents, the
current repository state, and the user's current request. A handoff that
declares a blocker does not bind a user who says to proceed. A handoff that
claims completion does not override evidence that the work is unfinished.
Use this summary as orientation, not as law.
