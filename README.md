# CodeWhale

> A community-built agentic terminal for coding with the best available models.

[简体中文 README](README.zh-CN.md) · [日本語 README](README.ja-JP.md) · [Tiếng Việt README](README.vi.md)

[![CI](https://github.com/Hmbown/CodeWhale/actions/workflows/ci.yml/badge.svg)](https://github.com/Hmbown/CodeWhale/actions/workflows/ci.yml)
[![crates.io](https://img.shields.io/crates/v/codewhale-cli?label=crates.io)](https://crates.io/crates/codewhale-cli)
[![DeepWiki project index](https://img.shields.io/badge/DeepWiki-project-blue)](https://deepwiki.com/Hmbown/CodeWhale)

![codewhale screenshot](assets/screenshot.png)

## What is CodeWhale

CodeWhale is an open-source terminal agent you run locally to do real work in
your repositories — read code, run commands, edit files, and ship patches — with
the model *you* choose. It pairs a full tool surface (shell, file edits, git,
web, MCP, and sub-agents) with approval gates, snapshots you can roll back, and
sessions you can resume.

It started as a DeepSeek-inspired TUI. The community turned it into something
broader: a harness that works with the best models available to the most people,
whichever provider that means for you. DeepSeek remains first-class here; it is
no longer the only good route, and it is not a requirement.

Everything runs on your machine. You keep your keys, your repos, and your
judgment in the loop.

## Why people use it

- **Approval-gated tools.** File edits, shell, git, web, MCP, and sub-agent
  calls all route through a sandbox and an approval policy you control.
- **Sub-agents & fleet.** Fan out parallel investigation or implementation
  across headless sub-agent workers, and orchestrate multi-step runs.
- **Snapshots & rollback.** Every turn is snapshotted in side-git, so
  `/restore` undoes a change without touching your repo's `.git`.
- **Live diagnostics.** Language servers light up after edits where available, so
  you see type errors and warnings as they happen.
- **Durable sessions.** Resume, fork, and relay handoffs between turns, sessions,
  and machines — plus runtime APIs for editors and GUIs.
- **Bring your own model.** Route each task to the provider that fits it best.

## Supported models & providers

CodeWhale ships first-class routes for the providers people actually use. Bring a
key and pick the model that fits the task:

- **DeepSeek** — V4 Pro / Flash, plus DeepSeek-compatible gateways
- **GLM / Z.ai** — GLM-5.1, GLM-5.2 (Z.ai Coding Plan)
- **Kimi (Moonshot)** — Kimi K2.6 / K2.7 Code
- **MiniMax** — first-party route
- **OpenRouter** — hundreds of models behind one key
- **NVIDIA NIM · Xiaomi MiMo · SiliconFlow · Fireworks · Novita · StepFun / StepFlash**
- **Self-hosted** — vLLM, SGLang, Ollama
- **Any OpenAI-compatible gateway**

Switch with `/provider` and `/model`. See [docs/PROVIDERS.md](docs/PROVIDERS.md)
for credentials, base URLs, and capability boundaries.

## Install

```bash
cargo install codewhale-cli --locked
cargo install codewhale-tui --locked
codewhale --version
```

On first launch, CodeWhale asks for a provider key and stores it in
`~/.codewhale/config.toml`. Legacy `~/.deepseek/` config is still read for
compatibility.

Other install paths:

```bash
# npm wrapper
npm install -g codewhale

# Platform archives attached to GitHub Releases
# https://github.com/Hmbown/CodeWhale/releases

# CNB mirror, if GitHub is hard to reach
cargo install --git https://cnb.cool/codewhale.net/codewhale --tag v0.8.60 codewhale-cli --locked --force
cargo install --git https://cnb.cool/codewhale.net/codewhale --tag v0.8.60 codewhale-tui --locked --force

# Homebrew (legacy compatibility while the formula is renamed)
brew tap Hmbown/deepseek-tui && brew install deepseek-tui
```

For Docker, direct downloads, China mirrors, Windows/Scoop, Nix, checksums, and
troubleshooting, see [docs/INSTALL.md](docs/INSTALL.md).

**Upgrading from the legacy `deepseek-tui` package?** Your config, sessions,
skills, and MCP settings are preserved. See
[docs/REBRAND.md](docs/REBRAND.md), then run `codewhale doctor` to confirm.

## Quick start

```bash
codewhale auth set --provider zai     # or: deepseek, openrouter, kimi, ...
codewhale auth status
codewhale doctor
codewhale                              # launch the TUI
```

Useful in-session commands:

- `/provider` and `/model` — choose the route and model.
- `/config` — edit runtime settings.
- `/statusline` — current route, cost, and session state.
- `/skills` — load reusable workflows from `~/.codewhale/skills/`.
- `/restore` — roll back a prior turn from side-git snapshots.
- `! cargo test` — run a shell command through the normal approval and sandbox
  path.

## Community & Contributing

CodeWhale is built in the open — and that's the point. The goal is simple: with
the most eyes and the most hands, build the best agent harness for the most
people. What started as one person's DeepSeek-inspired side project has been
shaped by a community into something bigger than its original intent could have
imagined.

**We love issues and pull requests, regardless of how experienced you feel.** Bug
reports, feature ideas, docs fixes, "first PR"s, and curious questions all count
as real project work. Maintainers treat reports and PRs as contributions even
when the final patch has to be narrowed, delayed, or folded into a maintainer
commit — and recurring contributors stay credited in the public record.

- [Open issues](https://github.com/Hmbown/CodeWhale/issues) — good first
  contributions live here.
- [CONTRIBUTING.md](CONTRIBUTING.md) — set up a dev loop and open a PR.
- [Code of Conduct](CODE_OF_CONDUCT.md) — be excellent to each other.
- [Contributors](docs/CONTRIBUTORS.md) — the people who've shaped CodeWhale.

## Docs

The README gets you started; the details live in [`docs/`](docs) and on
[codewhale.net](https://codewhale.net/):

- [User guide](docs/GUIDE.md) — your first hour with CodeWhale.
- [Install guide](docs/INSTALL.md) — every package path and troubleshooting.
- [Configuration](docs/CONFIGURATION.md) — config files and provider settings.
- [Providers](docs/PROVIDERS.md) — model routes, credentials, and capabilities.
- [Modes](docs/MODES.md) — Agent, Plan, and YOLO.
- [Sub-agents](docs/SUBAGENTS.md) — roles, lifecycle, and recovery.
- [Fleet](docs/FLEET.md) — multi-worker runs and headless orchestration.
- [WhaleFlow authoring](docs/WHALEFLOW_AUTHORING.md) — declarative workflows.
- [Runtime API](docs/RUNTIME_API.md) — HTTP/SSE, ACP, and editor/GUI contracts.
- [MCP](docs/MCP.md) — Model Context Protocol servers.
- [Architecture](docs/ARCHITECTURE.md) — crate layout, runtime flow, security.
- [Keybindings](docs/KEYBINDINGS.md) — the full key map.
- [Sandbox & approvals](docs/SANDBOX.md) · [Accessibility](docs/ACCESSIBILITY.md)
  · [Docker](docs/DOCKER.md) · [Memory](docs/MEMORY.md)
- [Full docs index](docs) — everything else.

## Operating identity & the Constitution

CodeWhale is opinionated about *how* an agent should behave in a real workspace,
not just what it can do. That opinion is written down as the
[CodeWhale Constitution](docs/AGENT_ETHOS.md), and it boils down to a few ideas:

- **The agent has an address.** It is an instance in *this* terminal and *this*
  workspace — not a model card or a leaderboard score.
- **Evidence outranks narration.** Tool output beats a guess; a failed command is
  reported as a failed command; verification is part of the task.
- **User intent stays sovereign.** Your current request outranks stale repo
  guidance, memory, and previous handoffs.
- **Local law is explicit.** Repositories can add `.codewhale/constitution.json`
  for durable project authority, protected invariants, and verification rules.
- **Runtime policy is enforced.** Modes, approval gates, sandboxing, rollback,
  and tool schemas are code, not advice the model has to remember.

The product is the ordering layer around the model: who is acting, whose law
applies, what evidence exists, and how the next human or agent can continue. If
that framing is useful to you, great; if not, you can ignore it and just use the
tools.

## Thanks

CodeWhale exists because of the people who use it, break it, and fix it.

- **[DeepSeek](https://github.com/deepseek-ai)** — the models and support that
  got this project started. 感谢 DeepSeek 提供模型与支持。
- **[DataWhale](https://github.com/datawhalechina)** 🐋 — for the support and for
  welcoming us into the Whale Brother family. 感谢 DataWhale 的支持。
- **[OpenWarp](https://github.com/zerx-lab/warp)** and
  **[Open Design](https://github.com/nexu-io/open-design)** — for collaborating
  on a better terminal-agent experience.
- **Every contributor** — the full per-PR record lives in
  [docs/CONTRIBUTORS.md](docs/CONTRIBUTORS.md). Thank you.

## License

[MIT](LICENSE)

> *CodeWhale is an independent community project and is not affiliated with any
> model provider.*

## Star History

[![Star History Chart](https://api.star-history.com/chart?repos=Hmbown/CodeWhale&type=date&legend=top-left)](https://www.star-history.com/?repos=Hmbown%2FCodeWhale&type=date&logscale=&legend=top-left)
