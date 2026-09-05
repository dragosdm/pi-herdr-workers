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
  if (event.protocol !== 1) return;
  console.log(event.runId, event.acceptedSequence, event.status);
});
```

## Event shape

```ts
type AcceptedLifecycleEvent = {
  protocol: 1;
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

All identities and evidence fields are runtime validated. IDs use `[A-Za-z0-9._-]{1,128}`. Worker names are limited to 32 characters, pane IDs to 128, messages and results to 65,536 UTF-8 bytes, errors and diagnostic details to 4,096 UTF-8 bytes. Unknown object fields are tolerated for forward compatibility, but known fields retain their protocol-1 meaning.

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
| `completed` | The worker or assignment owner explicitly reported run-aware success | Spawn success, send receipt, ordinary message, or `agent_settled` |
| `failed` | An explicit run-aware worker failure or conclusive reconciliation | Caller timeout or an ambiguous external command result |
| `stopped` | Reserved for a future explicit stop acknowledgement | Release, shutdown, disappearance, idle state, or `agent_settled` |
| `uncertain` | An external side effect may exist but the available evidence cannot classify the run safely | A known failure, a stop acknowledgement, or permission to retry |

The provider classifies ambiguous pane split, agent start, assignment delivery, and shutdown-interruption outcomes as `uncertain`. If start and relationship persistence already succeeded, the accepted `started` observation remains in history and assignment-delivery ambiguity is appended afterward. Later trusted readiness, completion, failure, or reconciliation may strengthen uncertainty. Accepted terminal outcomes do not regress to uncertainty, and conflicting terminal evidence is recorded diagnostically without changing canonical state.

Pure validation failures occur before worker side effects and remain RPC or tool failures; they do not invent a worker terminal event. One missing inspection result also does not create a lifecycle outcome.

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

Restoration scans `SessionManager.getEntries()` for the current session, including entries outside the active `/tree` branch. It reconstructs run bindings, accepted order, event IDs, source sequences, readiness, and terminal evidence without republishing historical events. Repeated `eventId` values, non-increasing source sequences, and duplicate restored entries are ignored. Delivery is therefore at least once at the inbox boundary and idempotent at lifecycle acceptance, not globally exactly once.

Consumers that restart or miss publication can recover accepted evidence through `herdr-workers:runs:rpc:replay`. Subscribe to the canonical lifecycle channel first, list durable run records, replay each run after its listed `acceptedSequence`, and deduplicate live and replayed observations by `(runId, acceptedSequence)`. See `docs/run-query-protocol.md` for routing, bounds, and the complete recovery algorithm.

Worker-originated reports use the existing atomic filesystem inbox. The parent first verifies the configured peer and pane/run binding, injects a `herdr-worker.lifecycle-report` custom message, and waits until that message exists in session persistence. Only then does it append and publish the lifecycle observation and remove the inbox file.

Durability follows Pi's session storage contract. `--no-session` is ephemeral, and lifecycle entries are not a distributed transaction with Herdr pane operations.

## Reporting from a worker

`ReportWorkerRun` is active only in a worker with a valid bound run. The model supplies assignment content only; the extension supplies run identity, worker and pane identity, event identity, source identity, source order, and timestamp.

```json
{ "status": "message", "message": "Mapped the authentication entry points." }
```

```json
{ "status": "completed", "result": "Implemented and verified the requested change." }
```

```json
{ "status": "failed", "error": "The required upstream API is unavailable." }
```

Ordinary `SendToAgent` messages remain general communication and never become terminal lifecycle reports.

## Consumer boundary

Lifecycle journal acceptance happens before publication. Event-bus errors and individual subscriber failures cannot roll back the journal, change a worker operation, alter an RPC result, or block isolated subscribers.

Events notify consumers; they do not approve phases, start subsequent work, retry uncertain spawns, terminate panes, or mutate loop orchestration. A future loop-owned adapter may correlate `correlationId -> runId -> paneId`, apply reducer transitions with fresh ownership fences, and request only reducer-derived parent wakes.
