# Worker Run Query Protocol

The run-query protocol exposes durable, session-scoped worker assignment records over Pi's process-local `pi.events` bus. It is independent of worker RPC protocol 1: spawning, sending, and inspection keep their existing contract, while this protocol provides durable discovery after a caller or extension reload.

## Channels and routing

Protocols 2 and 1 share four request channels:

| Operation | Request channel | Default client timeout |
|---|---|---:|
| Probe | `herdr-workers:runs:rpc:probe` | 2 seconds |
| Get | `herdr-workers:runs:rpc:get` | 20 seconds |
| List | `herdr-workers:runs:rpc:list` | 20 seconds |
| Replay | `herdr-workers:runs:rpc:replay` | 20 seconds |

Replies use `<request-channel>:reply:<requestId>`. Subscribe before emitting. Probe negotiates the highest shared protocol, preferring 2, and returns a query-specific `providerInstanceId`; addressed calls must copy that ID and the negotiated protocol. Reload creates a new provider instance, so requests addressed to an old generation are ignored.

```json
{
  "requestId": "probe-1",
  "supportedProtocols": [2, 1]
}
```

```json
{
  "requestId": "probe-1",
  "protocol": 1,
  "success": true,
  "data": {
    "protocol": 2,
    "provider": "herdr-runs",
    "providerInstanceId": "query-generation-1",
    "sessionId": "pi-session-1",
    "available": true,
    "constraints": {
      "sessionScoped": true,
      "requiresHerdrPane": false,
      "requiresInteractivePi": false
    }
  }
}
```

The provider becomes available after the session run registry and lifecycle acceptor are restored. Durable queries do not require an interactive Pi or a live Herdr pane.

## Get

`get` returns one strict handle when durable registration exists, or one legacy projection when only accepted lifecycle history exists. Protocol 2 handles expose `lifecycleProtocol` and `lifecycle.orchestrationGradeCompletion`.

```json
{
  "requestId": "get-1",
  "providerInstanceId": "query-generation-1",
  "protocol": 2,
  "runId": "run-1"
}
```

A strict handle contains immutable registration and original endpoint facts. Lifecycle status and `acceptedSequence` come from the durable lifecycle fold.

```json
{
  "protocol": 2,
  "lifecycleProtocol": 2,
  "runId": "run-1",
  "correlationId": "dispatch-1",
  "requestId": "spawn-1",
  "sessionId": "pi-session-1",
  "registeredAt": 1788351000000,
  "lifecycle": {
    "status": "started",
    "acceptedSequence": 1,
    "readiness": "unconfirmed",
    "orchestrationGradeCompletion": false
  },
  "assignment": {
    "cwd": "/workspace/project",
    "model": "test/model",
    "role": "research"
  },
  "endpoint": {
    "agentName": "agent-scout",
    "paneId": "pane-1",
    "observedAt": 1788351000100
  }
}
```

A lifecycle-only record is explicitly discriminated and does not invent assignment, registration, request, or endpoint facts. Its protocol 2 projection also exposes the lifecycle contract inferred from accepted history and the orchestration-grade marker.

```json
{
  "protocol": 2,
  "legacy": true,
  "lifecycleProtocol": 1,
  "runId": "legacy-run-1",
  "sessionId": "pi-session-1",
  "lifecycle": {
    "status": "completed",
    "acceptedSequence": 2,
    "readiness": "confirmed",
    "orchestrationGradeCompletion": false
  },
  "worker": {
    "agentName": "agent-legacy",
    "paneId": "pane-9"
  }
}
```

Unknown runs return `NOT_FOUND`. Set `includeEndpointObservation: true` for best-effort response-time enrichment. The provider resolves only the registered `agentName` and enriches the response only when the live name and pane both equal the original binding. A match refreshes `endpoint.observedAt` and may add `endpoint.herdrStatus`. A missing, renamed, moved, or reused endpoint leaves the durable endpoint unchanged. Legacy records are never enriched, and observation appends no registry, team, or lifecycle entries.

## List

`list` unions registered and lifecycle-only current-session runs, sorts them lexicographically by `runId`, and returns a bounded page.

```json
{
  "requestId": "list-1",
  "providerInstanceId": "query-generation-1",
  "protocol": 2,
  "limit": 50
}
```

The default limit is 50 and the maximum is 100. `nextCursor` is an opaque encoding of the page's last `runId`; pass it unchanged to continue. A cursor is exclusive. Restart from the beginning to discover runs added later whose IDs sort before an old cursor.

## Replay

`replay` reads accepted current-session lifecycle evidence for one run. The accepted-sequence cursor is exclusive, results preserve ascending canonical order and the original event identity fields, and replay never republishes lifecycle channels.

```json
{
  "requestId": "replay-1",
  "providerInstanceId": "query-generation-1",
  "protocol": 1,
  "runId": "run-1",
  "afterAcceptedSequence": 1,
  "limit": 50
}
```

```json
{
  "events": [
    {
      "protocol": 2,
      "eventId": "event-2",
      "runId": "run-1",
      "sourceInstanceId": "worker-generation-1",
      "sourceSequence": 1,
      "acceptedSequence": 2,
      "status": "completed",
      "worker": { "name": "agent-scout", "paneId": "pane-1" },
      "observedAt": 1788351000200,
      "source": "worker",
      "evidence": {
        "kind": "worker_completed_v2",
        "result": "Done",
        "artifacts": [{ "path": "reports/result.md" }],
        "checks": [{ "kind": "test", "command": "npm test", "outcome": "passed" }]
      }
    }
  ],
  "hasMore": false
}
```

The default replay limit is 50 and the maximum is 100. `hasMore` means another request with the last returned `acceptedSequence` can continue the same run. Registered runs with no accepted evidence return an empty page; a run absent from both registration and lifecycle authority returns `NOT_FOUND`.

Protocol 2 returns native contract 1 or contract 2 accepted events and preserves structured worker or reconciliation completion evidence. Protocol 1 is an explicit compatibility projection: it omits lifecycle-contract and orchestration-grade fields, and projects `worker_completed_v2` or `reconciled_completed_v2` to readable `worker_completed` evidence containing its result. Artifact, check, reconciliation-detail, and observation fields are intentionally absent from that projection, so a protocol 1 response can never acquire an orchestration-grade marker.

An accepted reconciliation appears in `get`, `list`, and `replay` like every other canonical lifecycle event. It advances `acceptedSequence` on the same run and remains available after provider reload without being republished. Endpoint sampling remains response-only and cannot create or alter a reconciliation result.

## Restart recovery

To avoid a snapshot/publication race, subscribe to `herdr-workers:lifecycle` before querying. Probe the run-query provider, page through `list`, then call `replay` for each tracked run after the record's `lifecycle.acceptedSequence`. Merge concurrent live and replayed events by `(runId, acceptedSequence)`. The same event may appear through both paths, but canonical sequence deduplication produces one ordered consumer state.

## Validation and errors

Safe IDs use `[A-Za-z0-9._-]{1,128}`. Known string fields have both character and UTF-8 byte limits: session, model, role, pane, and status fields allow 128 bytes; agent names allow 32; CWD allows 4,096; error messages allow 1,024. Unknown fields remain additive, but malformed known fields, nested lifecycle data, endpoint data, cursors, page limits, accepted events, mixed-run replay pages, and non-increasing replay sequences are rejected.

Both server and client validate successful results against the negotiated version. Service failures are sanitized to fixed errors. Both query protocols use `INVALID_REQUEST`, `UNSUPPORTED_PROTOCOL`, `PROVIDER_UNAVAILABLE`, `NOT_FOUND`, and `INTERNAL_ERROR`.

Timeout and abort bound only the caller's wait. The client removes reply listeners after success, failure, timeout, abort, and synchronous event-bus errors.
