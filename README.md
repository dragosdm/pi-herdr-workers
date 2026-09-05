# pi-herdr-workers

One Pi package for **teams of agents in [Herdr](https://herdr.dev) panes**, **`/split-handoff` / `/split-fork`**, **scheduled/event/idle `/loop` re-wakes**, and **background monitors** that are just Herdr panes.

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

`priority: true` steers; otherwise follow-up. Transport is a per-pane inbox under `$XDG_RUNTIME_DIR/pi-herdr-worker/`. Pi receives a durable, model-visible custom message attributed to the sending agent, not a fabricated user message. Non-Pi targets fall back to `herdr agent prompt`.

### `ReportWorkerRun` and lifecycle events

Each worker assignment has a provider-generated `runId`, separate from an RPC `requestId`, optional caller `correlationId`, worker name, and pane identity. A bound worker can use `ReportWorkerRun` for typed `message`, `completed`, or `failed` reports. The extension supplies trusted identity and ordering fields; ordinary `SendToAgent` prose is never interpreted as an outcome.

Accepted observations are journaled in the parent session before publication on `herdr-workers:lifecycle` and the matching `herdr-workers:<status>` channel. Provider-observed start, worker readiness, explicit outcomes, and scoped uncertainty carry different evidence. Release and extension shutdown do not mean the worker stopped. See `docs/lifecycle-events.md` for the contract, durability, deduplication, and consumer boundaries.

### Inter-extension RPC

Extensions loaded in the same Pi process can probe `herdr-workers:rpc:probe` and call the worker provider directly, without a model turn. Probe first, negotiate protocol 1, select one available provider instance, and use its request-specific reply channels. The fixed capabilities are `spawn`, `send`, `steer`, and team-scoped `inspect`; existing `/team`, `CreateAgentPanel`, and `SendToAgent` behavior is unchanged.

The event bus is process-local, requests are not durable, and callers should use bounded waits and fall back when no provider is available. A spawn reply identifies the run and closes the request; a send receipt confirms transport acceptance; lifecycle observations record evidence; only explicit run-aware `completed` or `failed` reports state an assignment outcome. The registered stop channel is reserved: stop is not advertised and returns `UNSUPPORTED_OPERATION`; it does not release a worker, close a pane, or send Ctrl-C. See `docs/rpc-protocol.md` for the import-free JSON contract, limits, routing, errors, and raw `pi.events` usage.

### Durable run discovery

Each spawn is registered before Herdr side effects. A separate run-query protocol lets extensions probe `herdr-workers:runs:rpc:probe`, fetch one run with `get`, or page through current-session runs with `list`. Strict handles combine immutable assignment and original endpoint facts with lifecycle state from accepted durable evidence. Older lifecycle-only runs remain visible as explicit legacy projections without fabricated registration or assignment fields. See `docs/run-query-protocol.md` for schemas, pagination, routing, and recovery behavior.

---

## `/split-handoff` and `/split-fork`

Unfocused named Pi splits in the current Herdr tab (first split right, later ones down).

| Command | Effect |
|---|---|
| `/split-handoff [goal]` | Runs a **summarize/crystallize** pass on this thread (current model), then starts a new Pi in a split with that handoff — not a raw transcript dump |
| `/split-fork [instruction]` | Clones the active session branch into a new pane; this pane keeps going |
| `/splits` | Durable split history; `/splits cancel` aborts a startup |

Status bar shows `split · summarizing` then pane/start/prompt. Herdr layout is the same as before; the missing piece was the handoff write-up.

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

State is journaled in pi's session JSONL via `appendEntry` by default. Existing session-specific `.pi/loops/` snapshots are imported on first use, without deleting the originals. Explicit project/shared-file scopes still use the locked file store, with a session-log audit mirror. `PI_LOOP_SCOPE=memory` remains ephemeral.

Recurring loops expire after seven days unless recreated. Cap: 25 loops. `LoopCreate` defaults to 25 wakes unless `maxFires` is supplied; `/loop` scheduled/event loops cap at 25, and dynamic goals at 20.

Event subscriptions survive reload/resume. Wakes wait for `agent_settled`, not the end of an individual retry or tool cycle. Built-in `tool_execution_start`, `tool_execution_end`, `turn_start`, `turn_end`, and `agent_settled` events are bridged onto the extension bus; Loop tools do not trigger their own tool-event loops. Other sources must be emitted by an extension.

Read-only wakes enforce a tool-call gate (read/search/list and loop bookkeeping only); shell commands and arbitrary custom tools are blocked until the run settles. This deliberately stays conservative if other work is queued during that run.

---

## Monitors — Herdr panes, not hidden processes

`MonitorCreate` / `MonitorList` / `MonitorStop`

- Ensures a **Monitor** tab in the current workspace
- Each command gets a **down-split** pane titled `mon:<hash> <command>`
- **The same command in the same cwd reuses that pane**
- Panes **stay open** when the command finishes
- Creating an already-running monitor attaches to it; it never sends `ctrl+c` or restarts it
- `MonitorStop` validates the pane identity and current process state before sending `ctrl+c` — it does not close the pane
- Handles and command/cwd metadata survive reload; restored process status starts **unknown**, and `MonitorList` refreshes it

```text
MonitorCreate command="npm test" description="Run test suite"
MonitorList
MonitorStop monitorId="1"
```

---

## Durable state and lifecycle

- Team snapshots are immutable, versioned, restored on `session_start` and `/tree`, and scoped to the source session. A new fork does not inherit another session's worker-control authority. Legacy unversioned team records remain readable.
- Loops and monitor handles are **operational facts**: `/tree` does not undo an external launch or resurrect a deleted controller. New session IDs do not inherit active controllers or monitor handles. In explicit shared-store mode, the shared file remains authoritative.
- Pending loop wakes are journaled before delivery and acknowledged only after their custom message is present in SessionManager. Team messages retain their mailbox file until that same persistence boundary. `message_end` is too early: pi invokes that hook before saving the message.
- Worker lifecycle observations are appended as `herdr-worker.lifecycle.v1` before local publication and restored from all entries in the current session. Duplicate inbox delivery and reload do not republish a new accepted observation; `/tree` does not erase operational lifecycle history.
- Restoring state never creates panes, restarts commands, or re-sends an already recorded wake. A dynamic iteration awaiting `LoopUpdate` stays awaiting an update after reload; interrupted external work is not blindly repeated. Inspect it and provide the update, or pause/resume it explicitly from `/loop` when safe. A reached fire cap cannot be resumed; renewal requires a new authorized controller.
- Timers, subscriptions, in-flight requests, sampled process status and UI contexts are runtime resources, not replayed state. Shutdown aborts/cleans them without killing worker or monitor panes. Headless pi instances do not claim a pane's team mailbox.
- Status items are compact counts, next-wake/awaiting-update state and unverified/unavailable monitors. Detailed rosters remain in `/team list`, `LoopList`, and `MonitorList`.
- Durability follows pi's session storage contract: `--no-session` is still ephemeral, and a brand-new session may buffer entries until its first assistant response. External side effects and local journal appends are not a distributed transaction; inspect ambiguous interrupted operations rather than automatically retrying them.

## What’s not in this package

- `@trevonistrevon/pi-loop` kitchen sink (workflows, native tasks, pi-subagents batches)
- `OrchestrationCreate` — use `/team` + `CreateAgentPanel` instead
- Automatic mapping from worker lifecycle events into loop orchestration; a future loop-owned adapter must preserve reducer ownership and wake acknowledgement

---

## Layout

```text
extensions/herdr-worker.ts   /team, CreateAgentPanel, SendToAgent, ReportWorkerRun
lifecycle/                   worker lifecycle protocol, acceptance, persistence, publication
runs/                        durable run registry and run-query protocol
extensions/split-handoff.ts  /split-handoff, /split-fork, /splits
loop/                        /loop + Monitor* (Herdr-backed)
```
