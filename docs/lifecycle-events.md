# Worker Lifecycle Events

Worker lifecycle events describe evidence observed after a worker assignment is dispatched. They complement the worker RPC protocol: an RPC reply closes one request, while lifecycle history follows one assignment identified by a provider-generated `runId`.

## Channels

Every accepted observation is published on the canonical stream first and then on one status-specific projection. Both emissions carry the same object.

| Stream | Channel |
|---|---|
| Canonical | `herdr-workers:lifecycle` |
| Started | `herdr-workers:started` |
| Message | `herdr-workers:message` |
| Completed | `herdr-workers:completed` |
| Failed | `herdr-workers:failed` |
| Stopped (reserved) | `herdr-workers:stopped` |
| Uncertain | `herdr-workers:uncertain` |

Subscribe to the canonical stream for complete ordered history or to selected projections for narrow reactions. Do not subscribe to both forms for the same purpose, because each accepted observation intentionally appears on both.

```ts
pi.events.on("herdr-workers:lifecycle", (event) => {
  if (event.protocol !== 1 && event.protocol !== 2) return;
  console.log(event.runId, event.acceptedSequence, event.status);
});
```

## Event shape

```ts
type AcceptedLifecycleEvent = {
  protocol: 1 | 2;
  eventId: string;
  runId: string;
  sourceInstanceId: string;
  sourceSequence?: number;
  acceptedSequence: number;
  status: "started" | "message" | "completed" | "failed" | "stopped" | "uncertain";
  worker: { name: string; paneId?: string };
  observedAt: number;
  source: "provider" | "worker" | "reconciler" | "controller";
  correlationId?: string;
  evidence: WorkerLifecycleEvidence;
};
```

All identities and evidence fields are runtime validated. IDs use `[A-Za-z0-9._-]{1,128}`. Worker names are limited to 32 characters, pane IDs to 128, messages and results to 65,536 UTF-8 bytes, errors and diagnostic details to 4,096 UTF-8 bytes. Unknown object fields are tolerated for forward compatibility, but known fields retain their selected contract meaning.

Contract 1 remains readable for existing bindings and journal entries. New runs select contract 2 in their durable registration and carry that selection through startup or `bind-run`. The acceptor rejects a candidate whose `protocol` differs from its run binding rather than reinterpreting evidence across versions.

## Identity scopes

| Identity | Meaning |
|---|---|
| `requestId` | One transient RPC request/reply exchange |
| `correlationId` | Optional caller-owned reference known before dispatch |
| `runId` | Canonical provider-generated identity for one assignment |
| `paneId` | Concrete Herdr worker location, once known |
| `eventId` | Source-created identity used for event deduplication |
| `sourceInstanceId` + `sourceSequence` | Ordering within one producer generation |
| `acceptedSequence` | Parent-assigned order of accepted observations within one run |

Worker names select team members and may be reused. Pane IDs identify live locations and can outlive assignments. Neither replaces `runId`. A controller may put its dispatch ID in `correlationId`, but lifecycle payloads do not carry loop revisions, ownership generations, or phase policy.

## Evidence semantics

| Status | What it establishes | What does not establish it |
|---|---|---|
| `started` | Provider start returned after relationship persistence, or the worker reported run-aware readiness | Pane creation, prompt receipt, or later Herdr presence alone |
| `message` | A trusted worker-authored run report crossed the inbox and persistence boundary | Message prose does not imply progress or settlement |
| `completed` | The worker explicitly reported run-aware success; contract 2 requires a final result | Spawn success, send receipt, ordinary message, or `agent_settled` |
| `failed` | An explicit run-aware worker failure or conclusive reconciliation | Caller timeout or an ambiguous external command result |
| `stopped` | Reserved for a future explicit stop acknowledgement | Release, shutdown, disappearance, idle state, or `agent_settled` |
| `uncertain` | An external side effect may exist but the available evidence cannot classify the run safely | A known failure, a stop acknowledgement, or permission to retry |

The provider classifies ambiguous pane split, agent start, assignment delivery, and shutdown-interruption outcomes as `uncertain`. If start and relationship persistence already succeeded, the accepted `started` observation remains in history and assignment-delivery ambiguity is appended afterward. Later trusted readiness, completion, failure, or reconciliation may strengthen uncertainty. Accepted terminal outcomes do not regress to uncertainty, and conflicting terminal evidence is recorded diagnostically without changing canonical state.

Pure validation failures occur before worker side effects and remain RPC or tool failures; they do not invent a worker terminal event. One missing inspection result also does not create a lifecycle outcome.

## Reconciliation evidence

Reconciliation is a separate trusted provider operation for contract 2 runs. It can resolve only a currently `uncertain` run and requires the caller's inspected `expectedAcceptedSequence` to still equal the canonical sequence. A race returns `STALE_ACCEPTED_SEQUENCE`; the caller must refresh durable state and inspect again instead of overwriting newer worker evidence.

Every resolution carries a non-empty bounded observation list. Journal, Git, and filesystem observations are durable-state claims. Herdr and worker observations additionally carry `{ agentName, paneId }`, which must exactly equal the immutable endpoint binding. A missing endpoint is allowed for non-endpoint evidence and rejected for endpoint-bearing claims.

Accepted reconciliation evidence is discriminated as `reconciled_started_v2`, `reconciled_completed_v2`, or `reconciled_failed_v2`. Completion includes a required result and may include the same artifact and verification shapes as worker completion. The acceptor appends, folds, remembers, and publishes reconciliation through the ordinary canonical path, preserving the original `runId`, registration, endpoint, and accepted ordering. Reconciliation does not spawn, retry, send, stop, release, or rebind a worker.

The generic provider does not infer exclusive resources from `cwd`. An orchestration consumer owns explicit resource claims and quarantine policy. It must retain an uncertain writer's claim, reconcile the same run, durably consume and approve the result before release, and make retry a separate explicit decision. See `docs/reconciliation-protocol.md`.

## Durability and ordering

The parent acceptor performs this sequence:

```text
validate candidate and trusted run binding
  -> reject duplicate, stale, or conflicting evidence
  -> assign the next acceptedSequence for the run
  -> append herdr-worker.lifecycle.v1 to the parent session
  -> update folded in-memory state
  -> emit the canonical channel
  -> emit the status projection
```

Restoration scans `SessionManager.getEntries()` for the current session, including entries outside the active `/tree` branch. It reconstructs run bindings, accepted order, event IDs, source sequences, readiness, and terminal evidence without republishing historical events. Repeated `eventId` values, non-increasing source sequences, and duplicate restored entries are ignored. Lifecycle acceptance is idempotent when the relevant accepted journal survives. Inbox retries can inject a report more than once; acknowledgement before a recoverable write can also lose the only copy. Neither unconditional at-least-once recovery nor globally exactly-once delivery follows from memory visibility.

Consumers that restart or miss publication can recover accepted evidence through `herdr-workers:runs:rpc:replay`. Subscribe to the canonical lifecycle channel first, list durable run records, replay each run after its listed `acceptedSequence`, and deduplicate live and replayed observations by `(runId, acceptedSequence)`. See `docs/run-query-protocol.md` for routing, bounds, and the complete recovery algorithm.

Worker-originated reports use the existing atomic filesystem inbox. The parent first verifies the configured peer and pane/run binding and injects a `herdr-worker.lifecycle-report` custom message. A matching entry in `SessionManager.getEntries()` permits the `context` or `agent_settled` hook to attempt acceptance. This checks memory, not a confirmed session-file write. A fresh receiver also handles matching entries before reading the envelope. A recorded unbound report remains pending; malformed details, pane mismatch, duplicate or stale evidence, contract mismatch, and terminal conflict are handled rejections that permit cleanup without new acceptance.

The journal append callback returns before the acceptor folds, remembers, and publishes the event. If it throws, that acceptor does not advance and the report envelope remains. Pi 0.84.4 can nevertheless retain the journal candidate in memory after a real file error. An immediate retry can append another raw candidate; an extension reload can restore the failed candidate from memory without republishing it. Raw journal counts, accepted replay history, and reopened file entries are separate facts. The controlled full-snapshot storage model can write both candidates on retry, while actual Pi appends only the new entry and does not backfill the failed one.

Durability follows Pi's session storage contract. Fresh sessions can defer their first write, `--no-session` is ephemeral, and failed writes can leave entries visible in memory. A readable report after process termination can be accepted again when its accepted journal did not survive. A readable accepted journal restores exact event identity and sequence through query/replay without historical publication. These are local file-recovery claims, not power-loss durability or a distributed transaction with Herdr pane operations. The [executing mailbox matrix](mailbox-guarantees.md) records each tested boundary and its storage assumptions.

Binding controls persist team state directly and have no custom-message envelope acknowledgement. Duplicate `bind-run` processing can reset the source generation and repeat readiness publication. Startup does not turn a retained envelope, saved registration, operation checkpoint, or started history into uncertainty. Recovered uncertainty requires an existing scoped lifecycle journal. Graceful shutdown during creation can append such evidence; SIGKILL cannot run that shutdown handler.

## Reporting from a worker

`ReportWorkerRun` is active only in a worker with a valid bound run. The model supplies assignment content only; the extension supplies run identity, worker and pane identity, event identity, source identity, source order, and timestamp.

```json
{ "status": "message", "message": "Mapped the authentication entry points." }
```

```json
{
  "status": "completed",
  "result": "Implemented and verified the requested change.",
  "artifacts": [{ "path": "reports/result.md", "description": "Final report" }],
  "checks": [{ "kind": "test", "command": "npm test", "outcome": "passed" }]
}
```

```json
{ "status": "failed", "error": "The required upstream API is unavailable." }
```

For contract 2, `result` is required and non-empty. `artifacts` and `checks` are optional and each is limited to 32 entries. Artifact paths are retained exactly as authored, must be non-empty, contain no control characters, and fit within 4,096 UTF-8 bytes. Descriptions fit within 1,024 bytes. Check commands and outcomes are non-empty and fit within 4,096 bytes. Lifecycle acceptance does not resolve paths or inspect, copy, hash, upload, or promise the continued existence of referenced files.

Contract 2 worker completion evidence uses `kind: "worker_completed_v2"`; trusted recovered completion uses `kind: "reconciled_completed_v2"`. Contract 1 `kind: "worker_completed"` history, including old evidence without a result, remains restorable and replayable but is not orchestration-grade completion. Ordinary `SendToAgent` messages remain general communication and never become terminal lifecycle reports. Bound-worker guidance reserves `ReportWorkerRun` as the only worker-authored terminal completion or failure path.

## Consumer boundary

Lifecycle journal acceptance happens before publication. Event-bus errors and individual subscriber failures cannot roll back the journal, change a worker operation, alter an RPC result, or block isolated subscribers.

Events notify consumers; they do not approve phases, start subsequent work, retry uncertain spawns, terminate panes, or mutate loop orchestration. A future loop-owned adapter may correlate `correlationId -> runId -> paneId`, apply reducer transitions with fresh ownership fences, and request only reducer-derived parent wakes.
