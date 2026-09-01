# pi-herdr-workers

One Pi package for **teams of agents in [Herdr](https://herdr.dev) panes**, **scheduled/event/idle `/loop` re-wakes**, and **background monitors** that are just Herdr panes.

No extra npm dependencies. Peer APIs come from Pi (`pi-coding-agent`, `pi-tui`, `typebox`). Requires a Herdr-managed pane (`HERDR_ENV=1`) for `/team` and monitors.

```bash
pi install git:github.com/tobi/pi-herdr-workers
```

Or a local checkout:

```bash
pi install /path/to/pi-herdr-workers
```

Then `/reload` (or start a new session).

---

## `/team` — orchestrator and workers

Turns this pane into an orchestrator and puts other Pi agents in neighboring Herdr panes.

| Command | Effect |
|---|---|
| `/team add [right\|down\|left\|up] [type] [purpose…]` | Enable team mode and have the LLM create a worker with `CreateAgentPanel` |
| `/team list` | Show workers / orchestrator |
| `/team release <name>` | Stop orchestrating that worker (pane stays open) |
| `/team adopt <name>` | Control an existing Herdr agent without creating a pane |
| `/team from <id>` | Worker: adopt an orchestrator after startup |

`/team` once flips **team mode**: status shows `Orchestrator …`, the system prompt gets the orchestrator brief, and `CreateAgentPanel` appears. The pane is renamed `orchestrator` if it has no Herdr agent name.

### `CreateAgentPanel`

```json
{ "name": "scout", "direction": "right", "type": "explore",
  "purpose": "explore only, never edit files", "model": "xai/grok-4.6",
  "thinking": "low", "initial_prompt": "…" }
```

Workers are named `agent-<name>` (or `agent-<type>` / `agent-N`). First worker on a side splits toward it; further workers on that side stack. Starts `pi --orchestrated-by …` via `herdr agent start`.

### `SendToAgent`

```json
{ "target_id": "agent-explore", "message": "…", "priority": false }
```

`priority: true` steers; otherwise follow-up. Transport is a per-pane inbox under `$XDG_RUNTIME_DIR/pi-herdr-worker/`. Non-Pi targets fall back to `herdr agent prompt`.

---

## `/loop` — re-wake this agent

Same session, no extra workers. Idle-safe wakes when Pi is sitting there.

```text
/loop 5m check the deploy
/loop event tool_execution_end review that tool
/loop finish the release
```

Tools: `LoopCreate`, `LoopList`, `LoopUpdate`, `LoopDelete`.

- Cron: `5m`, `2h`, or a five-field cron expression
- Event: a Pi event source
- Idle/dynamic: `/loop <goal>` then `LoopUpdate` with `continue` / `paused` / `completed`

State lives under `.pi/loops/` (session-scoped by default). Recurring loops expire after seven days unless recreated. Cap: 25 loops.

---

## Monitors — Herdr panes, not hidden processes

`MonitorCreate` / `MonitorList` / `MonitorStop`

- Ensures a **Monitor** tab in the current workspace
- Each command gets a **down-split** pane titled `mon:<hash> <command>`
- **The same command reuses that pane**
- Panes **stay open** when the command finishes
- `MonitorStop` sends `ctrl+c` only — it does not close the pane

```text
MonitorCreate command="npm test" description="Run test suite"
MonitorList
MonitorStop monitorId="1"
```

---

## What’s not in this package

- `@trevonistrevon/pi-loop` kitchen sink (workflows, native tasks, pi-subagents batches)
- `OrchestrationCreate` — use `/team` + `CreateAgentPanel` instead

---

## Layout

```text
extensions/herdr-worker.ts   /team, CreateAgentPanel, SendToAgent
loop/                        /loop + Monitor* (Herdr-backed)
```
