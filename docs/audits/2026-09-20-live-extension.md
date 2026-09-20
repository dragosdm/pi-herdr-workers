# Extension audit, 20 September 2026

## Result

The worker happy paths worked in live Herdr panes. Monitors and loop recovery have release-blocking failures that the original test suite did not cover.

The highest-priority fixes are:

1. A new monitor can report success without running its command.
2. One impossible cron saved by `LoopCreate` can prevent unrelated event loops from resubscribing after reload.
3. `MonitorList` cannot reliably read terminal output through the installed Herdr CLI.

This was a broad audit, not proof that every possible input, provider, operating system, or crash boundary works. Untested areas are listed below. No production behavior was changed during the audit.

## Environment and method

- Source baseline: `1643689`, following the worker compatibility, mailbox recovery, run-query, completion, and reconciliation commits.
- macOS arm64, Node 24.15.0, Herdr 0.8.0.
- Live Pi: 0.86.0. The repository's real-Pi compatibility fixtures remain pinned to 0.84.4.
- Live model: `openai-codex/gpt-6-astra`. Workers used explicit model selection except the `/team add` test, which inherited the parent's model.
- `HERDR_ENV=1` was already present in the audit agent. It was also explicitly set in the user's `monitor` tab.
- Original automated suite: 538 passed; typecheck passed.
- Final automated suite: 553 passed, zero failures or skips; typecheck and `git diff --check` passed.
- Added 15 deterministic checks in `tests/extensions/audit-loop-monitor.test.ts`. Tests named `Known audit gap` intentionally reproduce current defects. A passing reproduction is not a fix.
- Ran 86 explicit audit-driver requests in a real TUI session, plus ordinary CLI commands, model turns, reloads, process exit/resume, and `/new`.

The opt-in driver in `tests/manual/live-audit.ts` loads the actual three extension entry points. It captures their registered tools and commands, delegates to their original implementations, and records results. Direct adapter invocations are not model-generated tool calls and bypass Pi's parameter-schema validation. Existing protocol/schema tests cover that separate boundary. Real model calls additionally exercised `/team add` → `CreateAgentPanel`, worker `SendToAgent` and `ReportWorkerRun`, `LoopUpdate`, and the read-only bash gate.

Herdr, process inspection, pane creation, filesystem mailboxes, Pi queues, session storage, and the event bus were real in the live tests. The audit driver used no fake Herdr backend. The new deterministic tests isolate loop parsing/state and monitor execution decisions with controlled inputs.

Local raw evidence is under `/tmp/pi-herdr-audit-20260920/`. It includes the driver JSONL, session files, baseline/final logs, snapshots, and monitor canaries. These temporary files are not a portable or permanent artifact. Selected evidence is retained in [2026-09-20-evidence.json](2026-09-20-evidence.json).

## Findings requiring fixes

### A01. High: a newly created monitor can skip the command and still report success

**Reproduced live and deterministically.**

`MonitorCreate` with `printf executed >> /tmp/pi-herdr-audit-20260920/monitor-canary` returned `Monitor #2 started`, with status `running`. The new pane contained only its shell prompt, and the canary did not exist. Calling the same tool again after shell initialization created the canary. The short output command showed the same first-use failure. The disposable HTTP server was also tested with creation followed by reuse; only one server launch was recorded.

`HerdrMonitorManager.create()` treats any busy foreground process as a monitor to attach to, including a shell-startup helper in a pane it just created. It returns before `pane run`. There is no evidence that the requested command is the busy process.

- Source: `loop/runtime/herdr-monitor.ts`, `create()` and `paneIsBusy()`.
- Evidence IDs: `monitor-short`, `monitor-file`, `monitor-file-reuse`, `monitor-long-new`, `monitor-long-run`.
- Fix: distinguish a newly created shell from an existing monitor. Wait for bounded, positively established shell readiness before launch. Return an explicit unstarted/uncertain result if readiness cannot be established. Do not report an attachment to an unrelated startup process as successful execution.
- Regression test: a new pane initially has a startup helper in the foreground, later becomes ready, and runs the command exactly once. Existing busy monitors must still attach without interruption.

Resolution note, 20 September 2026: [A01 implementation 5faabd2](https://github.com/dragosdm/pi-herdr-workers/commit/5faabd248e5bcba950ce42e61cb0572c4aeb65ce) adds bounded shell-readiness checks, retained pending handles and explicit submission/attachment/uncertainty results. [Dedicated regressions](../../tests/extensions/monitor-startup.test.ts) and the converted A01 regression cover those paths. [Implementation evidence](../implementation/A01.md) records automated results. Required disposable live acceptance is still pending supervisor verification; the historical observations above remain unchanged.

### A02. Medium: monitor output tails are unusable with Herdr 0.8.0

**Reproduced live and deterministically.**

`MonitorList` showed `| {}` before reload and `(could not read pane)` afterward. Direct terminal inspection showed `audit-monitor-ok` and `AUDIT_SERVER_READY`.

`readTail()` calls the generic `herdr()` helper, which applies `JSON.parse` to non-empty stdout. `herdr pane read` is a text-output command. Captured output in this environment was sometimes empty; the empty-output fallback produced `{}`. When text reached the parser, it failed as non-JSON.

- Source: `loop/runtime/herdr-monitor.ts`, `herdr()` and `readTail()`.
- Evidence IDs: `monitor-list`, `monitor-after-reuse`, `monitor-long-list`, `monitor-stopped-list`.
- Fix: use a verified machine-readable terminal snapshot API, or a separate raw-text command path. An empty capture must not masquerade as an actual output tail.
- Regression test: real CLI output, non-JSON text, empty output, command errors, and long/ANSI output.

### A03. High: failed cron creation persists a broken controller and poisons reload

**Reproduced live and deterministically.**

1. Call `LoopCreate` with `triggerType: "cron"`, `trigger: "0 0 31 2 *"`, and `maxFires: 1`.
2. The tool throws `No matching time found`.
3. `LoopList` nevertheless shows an active loop for that impossible date.
4. Create a valid event loop, then `/reload`.
5. Emit its event. The valid loop remains at `fireCount: 0`.

`LoopCreate` saves before scheduler registration and does not roll back when registration throws. On restoration, `CronScheduler.start()` throws on the bad entry. `TriggerSystem.start()` never reaches event subscriptions. The slash-command creation path already rolls back this error; the tool path does not.

- Source: `loop/tools/loop-tools.ts`, `LoopCreate`; `loop/scheduler.ts`, `start()`; `loop/trigger-system.ts`, `start()`.
- Evidence IDs: `impossible-cron`, `loop-after-error`, `poison-create`, `poison-event-create`, `poison-emit`, `poison-snapshot`.
- Fix: validate schedulability before persistence or roll back both store and trigger registration. During recovery, quarantine one invalid controller without disabling unrelated controllers.
- Recovery used in this audit: explicitly delete the impossible entry and reload. No loops were left active.

### A04. Medium: fresh worker readiness is lost during startup

**Observed live; the source ordering identifies a likely startup race.**

All six fresh worker launches retained only provider-observed `started` with `readiness: "unconfirmed"`. Workers that later completed or failed sent those terminal reports with `sourceSequence: 2`; their initial readiness report was absent from accepted history. Re-adopting an existing worker produced accepted `worker_ready` evidence and confirmed readiness.

The parent adds the worker relationship and lifecycle binding after `herdr agent start` returns. The child sends readiness during its `session_start`. Parent inbox delivery rejects unknown peers or unbound runs, and those envelopes can be removed rather than retried. The audit did not instrument the exact interprocess arrival instant, so the race explanation is source-supported rather than a captured scheduling trace.

- Source: `extensions/herdr-worker.ts`, `createAgent()`, `deliver()`, `reportWorkerReady()`; `mailbox/transport.ts`, `drainInbox()`.
- Evidence: original run `188ca470-3e8c-4e6d-aeaf-a46c1ea4ccf6` completed at accepted sequence 2 while readiness remained unconfirmed. Re-adopted run `40276dd4-330c-49a8-b5e2-0b5e8895a0a5` did confirm readiness.
- Fix: retain authorized pending-start readiness until binding exists, or establish the relationship/binding before the child can report. Preserve rejection of genuinely unknown writers.
- Regression test: deliver readiness before the provider's start call returns, then finish startup and verify exactly one accepted readiness event.

Unconfirmed readiness does not invalidate an explicit completion. The defect is loss of a useful readiness observation, not failure of the completed assignments.

### A05. Medium: interval normalization accepts zero and silently shortens long intervals

**Deterministic reproduction; some rounding is intentional but undocumented.**

- `parseInterval("0m")` becomes `*/1 * * * *`.
- `parseInterval("2d")` becomes `0 0 * * *`, so a two-day request runs daily.
- Other non-table intervals round to a common cadence rather than preserving the requested duration.

The slash command sometimes explains rounding. `LoopCreate` exposes only the resulting cron, without warning that the requested interval changed. Five-field wall-clock cron is also not an elapsed-time interval timer.

- Source: `loop/loop-parse.ts`, `parseInterval()` and `roundToNearestCommon()`.
- Fix: reject non-positive and non-finite durations. Either support the requested duration or clearly reject/report approximation, especially multi-day intervals. Document wall-clock alignment and jitter.

### A06. Medium: advertised five-field cron differs from conventional cron semantics

**Deterministic reproduction.**

`0 0 1 * 1` requires both the first of the month and Monday. Conventional cron treats restricted day-of-month and weekday as alternatives. Starting on 20 September 2026, the implementation returns 1 February 2027 instead of the following Monday.

The next-fire search also stops after 366 days. `0 0 29 2 *` from September 2026 throws despite having a valid future occurrence. Through `LoopCreate`, this also reaches A03's persistence problem.

- Source: `loop/loop-parse.ts`, `cronToNextFire()`.
- Fix: define the supported cron dialect and implement it consistently. Handle a valid schedule beyond a controller's seven-day lifetime without poisoning persisted state. If these restrictions are intentional, document them and reject unsupported input before creation.

### A07. Medium: hybrid parsing cannot accept a full cron expression

**Reproduced live and deterministically.**

`triggerType: "hybrid"` with `cron: */5 * * * * event: audit:test` throws because the parser extracts only `*/5`. `cron: 1h event: audit:hybrid` works and fired successfully.

- Source: `loop/tools/loop-tools.ts`, `LoopCreate` hybrid parsing.
- Evidence IDs: `hybrid-full-cron`, `loops-hybrid-short`, `hybrid-emit`.
- Fix: use structured cron/event fields or parse a complete documented grammar. Add usable hybrid examples; currently the tool describes a "hybrid spec" without defining it.

### A08. Medium: the once-per-wake update contract is not enforced

**Reproduced live and deterministically.**

A dynamic loop with one dispatched wake accepted two sequential `LoopUpdate(status: "continue")` calls. The second call advanced `iteration` again and replaced the checkpoint without another wake. The in-call state comparison is not a wake identifier or idempotency fence.

- Source: `loop/tools/loop-tools.ts`, `continueDynamicLoop()`; `loop/store.ts`, `continueDynamic()`.
- Evidence IDs: `dynamic-resume-update`, `dynamic-double-update`.
- Fix: require a current awaiting-update token/generation for normal iteration updates. Separate administrative resume from iteration acknowledgement. Duplicate and stale submissions should not advance the controller.

### A09. Medium: pause updates do not update the controller checkpoint

**Reproduced live and deterministically.**

`LoopUpdate(status: "paused", state: "new-paused-state", metrics: "new-paused-metrics", doneCriteria: "new-paused-done")` returned success. The saved controller retained the previous state, metrics, and done criteria. These new values existed only in the separate `herdr-loops.update.v1` audit entry, which restoration does not apply to the dynamic checkpoint.

- Source: `loop/tools/loop-tools.ts`, `stopDynamicLoop()`; `loop/store.ts`, `stopDynamic()`.
- Evidence IDs: `dynamic-once`, the model's `LoopUpdate`, `dynamic-pause-state`, `poison-snapshot`.
- Fix: atomically persist the supplied checkpoint with the pause transition. Future wakes should receive the latest saved checkpoint. Raw audit-history recoverability is not equivalent to restored controller state.

### A10. Medium: a cron expression without a prompt starts a dynamic goal

**Deterministic reproduction only; deliberately not allowed to burn live model turns.**

`/loop 0 9 * * 1-5` is parsed as a dynamic goal rather than a missing-prompt error. Its goal is the cron expression itself. Unlike `/loop 5m`, the full-cron form has no missing-prompt guard. In the wired extension this queues an immediate dynamic wake.

- Source: `loop/commands/loop-command.ts`, `parseLoopCommandRoute()`.
- Fix: recognize a five-field schedule even when there is no sixth word, and ask for the missing prompt. Malformed event-command prefixes deserve similarly explicit validation.

### A11. Medium: awaiting-update dynamic loops bypass runtime expiry

**Deterministic reproduction with an advanced scheduler clock.**

A dynamic loop with `awaitingUpdate: true` remains `active` when `CronScheduler.pump()` runs after `expiresAt`. The awaiting-update early return precedes the expiry check. A continuously running session can therefore keep an expired controller active indefinitely. Startup recovery separately expires entries, so a reload can change the outcome.

- Source: `loop/scheduler.ts`, `pump()`.
- Fix: retire expired controllers before skipping execution for awaiting updates. Expiry must not require the model to respond or the session to restart.

## Other limitations and documentation work

These are distinct from the confirmed failures above.

- **Model default is environment-dependent.** The local available-model listing contained `github-copilot/grok-4.6`, not `xai/grok-4.6`. `explore` and `research` hardcode the latter. Explicitly selected workers worked. A default scout launch was not attempted, so this is a configuration portability risk, not a reproduced launch failure. Make the default configurable or validate availability before creating a pane.
- **Monitor tab matching is case-sensitive.** The supplied `monitor` tab was not reused by `MonitorCreate`; it created `Monitor`. This matches the current literal comparison and README spelling, but is surprising operationally. Decide whether to reuse case-insensitive matches or expose a tab setting.
- **Handoff attribution uses a pane label, not the live agent name.** The parent was named `orchestrator`, but the generated context identified it as `w6:p4`. Pane IDs are valid targets, so delivery was not broken. Prefer live agent identity when available and label it accurately.
- **Large layouts become unusable.** Combining four-direction workers and splits reduced some test panes to two viewport rows. The commands still worked. No minimum-size protection or tab overflow policy was verified.
- **Monitor state is observational.** `running` means a non-shell foreground process, not proven identity of the original command. `stopped` immediately follows Ctrl-C submission, not verified process exit. No exit code is retained. Commands that daemonize or ignore Ctrl-C need a clearer contract or stronger process tracking.
- **Test coverage was uneven.** The original `npm test` suite covered workers, RPC, lifecycle, mailbox, queries, and reconciliation, but had no loop/monitor/split test files. This audit adds targeted loop/monitor coverage, not a complete replacement for live release checks.

## Existing mailbox and recovery risks reproduced by the baseline

The passing original suite includes intentional failure reproductions. Do not interpret its green result as unconditional reliable delivery. See the [mailbox guarantee matrix](../mailbox-guarantees.md) for exact cases and assumptions.

| Risk | Consequence or required boundary |
|---|---|
| Acknowledgement observes SessionManager memory before durable file storage | Buffered, disabled, or failed writes can lose the only recoverable mailbox copy |
| Reload while Pi retains queued messages | Duplicate injection window; the real-Pi test exercises SDK reload while streaming, not the guarded TUI command |
| Rejected custom-message handoff | Envelope can remain in flight without retry in the same receiver |
| Filename collisions and multiple receivers | Overwrite or duplicate execution; no exactly-once guarantee |
| Native watcher startup race | A listener marker is not proof of native watch readiness; production polling is part of recovery |
| Lexical path checks and local writers | Symlink access, pane-name aliases, and known-pane impersonation remain possible in an untrusted mailbox root |
| Lifecycle journals surviving only in memory | Process recovery can lose acceptance or accept a surviving report again |
| Retained `bind-run` control | Reprocessing can reset source generation and publish readiness again |
| Abrupt process death | Registration/start history alone does not reconstruct a missing uncertainty observation |

The documented trust model requires a trusted local mailbox root. These tests do not establish power-loss durability, cross-host delivery, or authentication against hostile local writers.

## Coverage ledger

| Area | Live result | Remaining boundary |
|---|---|---|
| `/team`, status, release | Passed; release left panes open and retained run history | Detailed argument/case/quoting behavior covered by existing adapter tests |
| `/team add` | Real model called `CreateAgentPanel`; new worker reported `AUDIT_TEAM_ADD_OK` | Other providers and model defaults not live-tested |
| Worker placement | Right/down/left/up, right-side stacking, collision suffix passed | Extreme geometry and cross-workspace movement not tested |
| Re-adoption | Same pane, new run, readiness and completion passed | Abrupt death during rebind remains fixture-covered only |
| Ordinary messaging | Real worker returned `AUDIT_PONG` through `SendToAgent` | Live fallback to a non-Pi agent not tested |
| Priority | RPC returned inbox steer receipt and worker received prompt | Busy-turn ordering verified by pinned Pi fixture, not a timed live race |
| Validation | Bad worker name, self-send, missing target rejected | Full schema/routing/error matrix covered by original tests |
| Completion/failure | Explicit worker reports reached accepted journal and queries | Artifact copying/verification is intentionally not provided |
| Readiness | Fresh-start observation missing, A04; re-adoption confirmed | Exact arrival timing not instrumented |
| Discovery/replay | Probes, list, get, live endpoint observation, replay passed | Raw multi-provider discovery fuzzing remains automated |
| Reconciliation | Live provider probe passed | Resolution, sequence races, and endpoint guards exercised by existing tests, not live uncertain external work |
| Reload and restart | Team restored; seven run records survived real parent exit/resume | Lossy-storage tests remain on Pi 0.84.4 |
| `/new` | No inherited team tools, monitors, or run records | Live `/tree`, compaction and fork-authority inspection not independently exercised |
| Monitors | Reuse, busy attachment, single server launch, stop and pane retention passed after readiness | A01/A02; no signals sent to foreign panes |
| Monitor ownership | Renamed pane rejected stop; process remained running; restoring identity allowed stop | PID replacement/daemonization not tested |
| Cron | One-minute idle wake produced `AUDIT_CRON_OK`, then respected maxFires | A03/A05/A06; DST/timezone matrix not tested |
| Events | Custom event fired once; subscriptions survived healthy reload | Poisoned reload failed, A03 |
| Hybrid | Shorthand cron plus event fired and respected cap | Full cron failed, A07; timer/event race not live stress-tested |
| Dynamic | Immediate wake, model pause, continue, completed deletion worked | A08/A09/A11; reload of in-flight iteration remains fixture/source-limited |
| Read-only gate | Real bash call returned `Read-only loop wake: bash is not allowed.` | No claim that all third-party tools are read-only |
| Limits | Event/cron one-fire cap live; 25-controller cap and dynamic renewal rejection deterministic | Seven-day expiry simulated, not seven days of wall time |
| Split fork | Persisted branch clone launched; child replied `AUDIT_FORK_OK` | Cancellation after pane creation not live-tested |
| Split handoff | Model-generated summary, second split downward; child replied `AUDIT_HANDOFF_OK` | Large-document file path, model failure fallback, and cancellation not live-tested |
| Split guards | Empty conversation rejected; busy agent rejected | No-session fork and concurrent startups not live-tested |

## Reproduce safely

Run the deterministic suite without creating panes:

```sh
npm ci
npm run typecheck
npm test
node --import tsx --test tests/extensions/audit-loop-monitor.test.ts
```

For live work, use a disposable shell pane in a user-approved Herdr tab. Do not run this driver in a valuable existing conversation. It can create real workers, invoke a model, and launch commands.

```sh
export HERDR_ENV=1
export HERDR_AUDIT_DIR="$(mktemp -d)"
pi --no-extensions --no-skills --no-prompt-templates --no-context-files \
  -e /absolute/path/to/pi-herdr-workers/tests/manual/live-audit.ts \
  --session-dir "$HERDR_AUDIT_DIR/sessions" \
  --model YOUR_AVAILABLE_PROVIDER/MODEL
```

From another authorized pane, using the actual returned pane ID and that same output directory:

```sh
node tests/manual/audit-command.mjs PANE_ID OUTPUT_DIR \
  '{"id":"unique-case-id","action":"snapshot"}'
```

The driver supports `tool`, `command`, `rpc`, `runs`, `reconcile`, `emit`, `snapshot`, and `shutdown` actions. Use unique case IDs. Its direct-tool mode bypasses model/schema validation and must not be described as a model-to-tool test. JSONL snapshots can contain conversation messages; do not publish raw logs from non-disposable sessions.

## Cleanup

All audit-created worker and split panes were closed. The audit-created capitalized `Monitor` tab was closed after the disposable server was stopped and shell readiness was verified. The user's original lowercase `monitor` tab and main tab were retained. The test parent exited, and its active/paused loops were removed before exit. No provider credentials, global Pi settings, production source files, or unrelated panes were changed.

Audit sessions and canaries remain under the temporary output directory for inspection. Fresh workers also wrote normal Pi session files through their installed configuration. Those files were not deleted.
