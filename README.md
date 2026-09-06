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

`priority: true` steers; otherwise follow-up. Transport is a per-pane inbox under `$XDG_RUNTIME_DIR/pi-herdr-worker/`, falling back to the OS temporary directory when `XDG_RUNTIME_DIR` is unset. Pi receives a model-visible custom message attributed to the sending agent, not a fabricated user message. Targets without a listening Pi fall back to `herdr agent prompt`.

An inbox send receipt means the temporary-file write and rename returned. It does not prove receiver acceptance or execution. The receiver keeps queued envelopes until a matching custom entry is visible in SessionManager, then acknowledges through `context` or `agent_settled`. This is a memory-visibility boundary, not confirmation of a session-file write. A fresh receiver also checks matching entries before reading envelope payloads.

The [mailbox guarantee matrix](docs/mailbox-guarantees.md) links publication, scheduling, rejection, acknowledgement, containment, ordinary and lifecycle recovery, binding controls, reload, and Pi storage tests to their assumptions. Each row matches an executing test. Run the isolated filesystem, recovery, and real-Pi compatibility tests with:

```sh
npm ci
node --import tsx --test tests/extensions/herdr-worker-pi-compat.test.ts tests/extensions/herdr-worker-mailbox.test.ts tests/extensions/herdr-worker-recovery.test.ts
```

The compatibility baseline pins development dependencies `@earendil-works/pi-coding-agent` and `@earendil-works/pi-ai` to 0.84.4 without changing consumer peer ranges. Tests assert the project-local coding-agent, its resolved agent-core, and pi-ai versions. Real Pi queues and SessionManager files run with synthetic assistant streams, isolated resources, and no provider requests, credentials, global Pi, or Herdr panes.

Pi 0.84.4 consumes steering before an earlier follow-up once the held assistant finishes. Its custom `message_end` hook precedes SessionManager append; `context` sees the entry and acknowledges it. Reopened JSONL proves recovery separately. Executing `Known contract gap` cases reproduce deletion of the only mailbox copy before the first session write, with persistence disabled, and after a real append error. These are passing loss reproductions, not no-loss guarantees. The controlled host remains a separate test JSONL model. None of these tests establishes exactly-once execution or power-loss durability.

Ordinary and lifecycle recovery tests stop isolated children at synchronous boundaries, send `SIGKILL`, await exit, and start new receivers from surviving files only. Temporary files stay unpublished. A retained final envelope retries after queue or memory loss; a surviving custom entry suppresses another injection and permits cleanup. Acknowledgement with deferred, disabled, or failed message writes can instead leave neither recoverable copy. Retry cases use previously saved team authority. Recovery assumes the same pane, mailbox root, and session file, a valid peer, and no competing receiver.

Lifecycle recovery adds the real registry, acceptor, and query/replay services. A custom report suppresses another injection for its filename; a surviving accepted journal independently suppresses another acceptance. Killing after the journal file write but before its append callback returns produces no live event in the old process. The replacement restores the exact event and sequence through replay without historical publication. Missing journals can instead lose accepted history or cause the surviving report to be accepted again. Journal append errors retain the envelope before any reload. Separate failed-write cases deliberately reload retained journal memory before killing the process to expose acknowledgement without a recovered journal.

Controls persist team state directly, without a custom-message acknowledgement. Tests cover authorization, state-write failure, restart, and cleanup for `orchestrated-by`, `bind-run`, and `released`. Re-reading a retained `bind-run` resets the source identity and can publish readiness again. A recovered uncertainty event needs a surviving scoped journal; a registration, operation checkpoint, or started history alone does not make restart infer uncertainty. These tests seed production records but do not launch actual Herdr workers or classify external side effects.

Extension reload is different from process restart. Retained custom-entry memory suppresses reinjection without proving a file write. Retained queues can survive while the new extension injects again. Both the controlled host and actual Pi 0.84.4 reproduce this duplicate window. The Pi test calls `session.reload()` while streaming; it does not exercise the TUI `/reload` command, whose busy guard prevents that interaction. Injection counts, consumed custom entries, and reopened file entries are separate assertions.

For Pi 0.84.4 persistent sessions with successful local writes, a synthetic provider error triggers automatic continuation of a queued follow-up. The test proves one consumption, exact recovery from the actual session file, envelope deletion, and the next assistant stream without another prompt. This guarantee excludes disabled or failed storage and other Pi versions. A separate injected rejection of `session.sendCustomMessage` reports a `send_message` error but leaves the envelope stuck in flight in that receiver. Provider failure and rejected handoff are different boundaries.

Mailbox scheduling serializes one receiver's sorted directory snapshot. It does not preserve producer-call order across equal timestamps, reserve unique filenames, or prevent two receivers from injecting the same envelope. Tests reproduce suffix reordering, collision overwrite, and bind-run arriving after an assignment. One real-watcher test disables polling; a separate watch-creation failure test drives the poll callback. A dead listener PID selects prompt fallback, without claiming that the prompt executes.

Use a trusted mailbox root. Pane sanitization and acknowledgement filename checks provide lexical containment only. Tests reproduce pane-name aliases, reads through symlinked inbox entries, writes through symlinked ancestor directories, and local writers impersonating a known pane. Roster checks reject unknown or stale panes but do not authenticate the writer. These tests exercise disposable local paths, not hostile production directories.

### `ReportWorkerRun` and lifecycle events

Each worker assignment has a provider-generated `runId`, separate from an RPC `requestId`, optional caller `correlationId`, worker name, and pane identity. New runs durably select lifecycle/report contract 2. A bound worker uses `ReportWorkerRun` for typed `message`, `completed`, or `failed` reports; completion requires a non-empty result and may include bounded artifact references and command/test checks. The extension supplies trusted identity and ordering fields. `SendToAgent` is for questions and ordinary communication, and its prose is never interpreted as a terminal outcome.

Accepted observations are journaled in the parent session before publication on `herdr-workers:lifecycle` and the matching `herdr-workers:<status>` channel. Provider-observed start, worker readiness, explicit outcomes, and scoped uncertainty carry different evidence. Release and extension shutdown do not mean the worker stopped. See `docs/lifecycle-events.md` for the contract, durability, deduplication, and consumer boundaries.

Uncertain contract 2 runs can be resolved through the separate process-local reconciliation provider. Reconciliation compares the caller's inspected accepted sequence, requires bounded evidence, verifies endpoint-bearing observations against the immutable binding, and records `started`, structured `completed`, or `failed` on the original run. It never retries or creates, sends to, stops, releases, or rebinds a worker. See `docs/reconciliation-protocol.md`.

### Inter-extension RPC

Extensions loaded in the same Pi process can probe `herdr-workers:rpc:probe` and call the worker provider directly, without a model turn. Probe first, negotiate protocol 1, select one available provider instance, and use its request-specific reply channels. The fixed capabilities are `spawn`, `send`, `steer`, and team-scoped `inspect`; existing `/team`, `CreateAgentPanel`, and `SendToAgent` behavior is unchanged.

The event bus is process-local, requests are not durable, and callers should use bounded waits and fall back when no provider is available. A spawn reply identifies the run and closes the request; a send receipt confirms transport acceptance; lifecycle observations record evidence; only explicit run-aware `completed` or `failed` reports state an assignment outcome. The registered stop channel is reserved: stop is not advertised and returns `UNSUPPORTED_OPERATION`; it does not release a worker, close a pane, or send Ctrl-C. See `docs/rpc-protocol.md` for the import-free JSON contract, limits, routing, errors, and raw `pi.events` usage.

### Durable run discovery

Each spawn is registered before Herdr side effects. A separate run-query protocol lets extensions probe `herdr-workers:runs:rpc:probe`, fetch one run with `get`, page through current-session runs with `list`, and recover accepted evidence with bounded `replay`. Query protocol 2 is preferred and exposes the selected lifecycle contract, structured completion, and an `orchestrationGradeCompletion` marker. Protocol 1 remains an explicit readable compatibility projection without that marker. Strict handles combine immutable assignment and original endpoint facts with lifecycle state from accepted durable evidence. Optional live endpoint observation enriches only an exact original name-and-pane match and never rewrites durable identity. Older lifecycle-only runs remain visible as explicit legacy projections without fabricated registration or assignment fields. Consumers subscribe first, then list and replay, deduplicating by `(runId, acceptedSequence)`. See `docs/run-query-protocol.md` for schemas, pagination, routing, endpoint trust, and recovery behavior.

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
- Pending loop wakes are journaled before delivery and acknowledged only after their custom message is present in SessionManager. Team messages retain their mailbox file until a matching session-visible custom entry permits acknowledgement. `message_end` is too early because Pi invokes it before appending the custom entry. Session visibility does not prove that a file write succeeded; a mailbox acknowledgement can remove the last recoverable copy when session storage is buffered, disabled, or fails.
- Worker lifecycle observations are appended as `herdr-worker.lifecycle.v1` before local publication and restored from all entries in the current session. A surviving accepted journal suppresses another acceptance and restoration does not republish history. Memory-only journals do not establish process recovery; `/tree` does not erase all-entry lifecycle history.
- An uncertain writer remains unsafe to replace automatically. The orchestration consumer owns explicit resource claims and quarantine, reconciles the same `runId`, releases the claim only under its phase policy after durable resolution, and treats retry as a separate explicit decision. The worker provider does not infer exclusivity from `cwd`.
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
mailbox/                     internal filesystem publication, listening, draining, acknowledgement
lifecycle/                   worker lifecycle protocol, acceptance, persistence, publication
runs/                        durable run registry and run-query protocol
reconciliation/              guarded uncertain-run reconciliation protocol
extensions/split-handoff.ts  /split-handoff, /split-fork, /splits
loop/                        /loop + Monitor* (Herdr-backed)
```
