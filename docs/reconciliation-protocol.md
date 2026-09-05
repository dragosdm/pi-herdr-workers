# Worker Reconciliation Protocol

The reconciliation protocol is a process-local mutation boundary for resolving durable contract 2 worker runs whose canonical lifecycle status is `uncertain`. It is separate from worker RPC and the read-only run-query protocol. Reconciliation preserves the original run and never performs replacement work.

## Channels and routing

Protocol 1 uses two request channels:

| Operation | Request channel | Default client timeout |
|---|---|---:|
| Probe | `herdr-workers:reconciliation:rpc:probe` | 2 seconds |
| Reconcile | `herdr-workers:reconciliation:rpc:reconcile` | 20 seconds |

Replies use `<request-channel>:reply:<requestId>`. A client subscribes before emitting, probes with `supportedProtocols: [1]`, and copies the selected `providerInstanceId` and protocol into the addressed request. Reload creates a new provider generation; an operation addressed to an old generation is ignored.

The provider becomes available only after the session's run registry and lifecycle acceptor restore. It is session-scoped and does not require a Herdr pane or interactive model turn.

## Request

```json
{
  "requestId": "reconcile-1",
  "providerInstanceId": "reconciliation-generation-1",
  "protocol": 1,
  "runId": "run-1",
  "expectedAcceptedSequence": 2,
  "resolution": {
    "status": "completed",
    "result": "Implemented and verified the requested change.",
    "detail": "Git and test evidence establish completion.",
    "observations": [
      {
        "source": "git",
        "detail": "The expected commit and clean worktree are present.",
        "observedAt": 1788351000300
      }
    ],
    "artifacts": [{ "path": "reports/result.md" }],
    "checks": [{ "kind": "test", "command": "npm test", "outcome": "passed" }]
  }
}
```

Resolution status is `started`, `completed`, or `failed`. Every variant requires non-empty `detail` and one to 32 observations. Completion also requires a non-empty `result` and may carry up to 32 artifact references and 32 verification checks using lifecycle contract 2 limits.

Journal, Git, and filesystem observations contain `source`, `detail`, and `observedAt`. Herdr and worker observations also require an endpoint:

```json
{
  "source": "herdr",
  "endpoint": { "agentName": "agent-builder", "paneId": "pane-7" },
  "detail": "The original worker pane is still active.",
  "observedAt": 1788351000250
}
```

Endpoint-bearing observations must exactly match both fields of the immutable run endpoint. Non-endpoint observations can resolve a run whose pane was never identified. Paths are retained exactly as evidence claims; reconciliation does not inspect, resolve, copy, hash, or upload artifacts.

## Compare and set

The lifecycle acceptor checks the operation synchronously immediately before acceptance:

```text
validate transport and bounded evidence
  -> find the original contract 2 run
  -> compare expectedAcceptedSequence
  -> require canonical status uncertain
  -> verify every endpoint-bearing observation
  -> construct trusted reconciler evidence
  -> append lifecycle journal
  -> fold canonical state
  -> retain replay event
  -> publish canonical and status channels
```

Sequence comparison happens before status replacement. If worker evidence arrives after inspection, the operation returns `STALE_ACCEPTED_SEQUENCE` and changes nothing. The caller refreshes query/replay state and inspects again; timestamps do not win the race.

Accepted evidence uses `reconciled_started_v2`, `reconciled_completed_v2`, or `reconciled_failed_v2`. The success reply contains the newly accepted lifecycle event, including its provider-supplied reconciler identity and canonical `acceptedSequence`.

## Errors

| Code | Meaning |
|---|---|
| `INVALID_REQUEST` | Transport shape, bounds, observations, or resolution evidence is invalid. |
| `UNSUPPORTED_PROTOCOL` | No common reconciliation protocol exists. |
| `PROVIDER_UNAVAILABLE` | Session authorities are not ready or are shutting down. |
| `NOT_FOUND` | The run has no lifecycle authority in this session. |
| `NOT_UNCERTAIN` | The current canonical status is not uncertain. |
| `STALE_ACCEPTED_SEQUENCE` | The run changed after the caller inspected it. |
| `ENDPOINT_MISMATCH` | An endpoint claim does not exactly match immutable run facts. |
| `UNSUPPORTED_LIFECYCLE_PROTOCOL` | The run uses readable legacy lifecycle contract 1. |
| `INTERNAL_ERROR` | A provider failure was sanitized. |

Client timeout and abort end only the caller's wait and remove its reply listener. They do not cancel or retry a provider operation. Callers recover by querying the run and comparing accepted sequence.

## Consumer-owned quarantine

The worker provider reports durable uncertainty and resolution but does not own worktree locks or infer exclusivity from assignment `cwd`. An automation consumer should retain its explicit write claim while a run is uncertain, inspect durable and endpoint state, reconcile the same run, and release only after the accepted resolution and its own phase policy permit it. `started` retains the claim. `completed` requires durable consumption and approval. `failed` permits release only when the consumer's evidence establishes that writing has stopped. Retry remains a separate explicit controller decision.
