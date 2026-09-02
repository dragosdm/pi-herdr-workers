# Herdr Worker RPC Protocol

The Herdr worker extension exposes a versioned JSON request/reply protocol over Pi's process-local `pi.events` bus. It lets another extension discover one provider and call worker operations without a model turn. This is an event-bus contract, not a package import API or a network protocol.

## Channels and replies

Protocol 1 defines five request channels:

| Operation | Request channel | Default client timeout |
|---|---|---:|
| Probe | `herdr-workers:rpc:probe` | 2 seconds |
| Spawn | `herdr-workers:rpc:spawn` | 120 seconds |
| Send | `herdr-workers:rpc:send` | 20 seconds |
| Inspect | `herdr-workers:rpc:inspect` | 20 seconds |
| Stop (reserved) | `herdr-workers:rpc:stop` | 20 seconds |

Every response is emitted on `<request-channel>:reply:<requestId>`. A caller must subscribe to that channel before emitting its request. `requestId` provides correlation only: it does not provide deduplication, retry safety, persistence, or idempotency.

Successful and failed replies have these shapes:

```json
{ "requestId": "req-1", "protocol": 1, "ok": true, "data": {} }
```

```json
{ "requestId": "req-1", "protocol": 1, "ok": false, "error": { "code": "INVALID_REQUEST", "message": "Request is invalid." } }
```

## Discovery and routing

Probe before every operation generation:

```json
{ "requestId": "probe-1", "supportedProtocols": [1] }
```

A provider chooses the highest mutually supported version and replies with:

```json
{
  "requestId": "probe-1",
  "protocol": 1,
  "ok": true,
  "data": {
    "protocol": 1,
    "providerInstanceId": "opaque-instance-id",
    "available": true,
    "capabilities": ["spawn", "send", "steer", "inspect"]
  }
}
```

An unavailable provider still replies successfully to probe and includes one reason: `SESSION_NOT_READY`, `NOT_INTERACTIVE`, `NOT_IN_HERDR`, or `SHUTTING_DOWN`. The reason precedence is shutdown, Herdr environment, interactive mode, then session readiness. Callers should fall back when no provider is available.

There may briefly be multiple providers on the shared bus. Select one available probe reply and copy its exact `providerInstanceId` and negotiated `protocol` into every addressed request. Providers silently ignore requests addressed to another instance. Reload creates a new instance ID; requests carrying the old ID are therefore no-ops.

## Operations

All addressed requests include `requestId`, `providerInstanceId`, and `protocol: 1`.

### Spawn

```json
{
  "requestId": "spawn-1",
  "providerInstanceId": "opaque-instance-id",
  "protocol": 1,
  "name": "scout",
  "direction": "right",
  "model": "xai/grok-4.6",
  "thinking": "high",
  "type": "explore",
  "purpose": "Map the subsystem",
  "cwd": "/workspace/project",
  "initialPrompt": "Inspect the implementation and report findings."
}
```

Every operation-specific field is optional. Direction is `right`, `down`, `left`, or `up`; thinking is `off`, `minimal`, `low`, `medium`, `high`, `xhigh`, or `max`. An omitted CWD uses the active Pi context. A supplied CWD must be an accessible absolute directory. Success returns:

```json
{
  "name": "agent-scout",
  "paneId": "%12",
  "cwd": "/workspace/project",
  "model": "xai/grok-4.6",
  "type": "explore",
  "purpose": "Map the subsystem",
  "adopted": false
}
```

Spawn may leave external side effects when a request times out or is aborted. The protocol does not roll back panes and does not make retries idempotent; inspect ambiguous outcomes before retrying.

### Send

```json
{
  "requestId": "send-1",
  "providerInstanceId": "opaque-instance-id",
  "protocol": 1,
  "target": "agent-scout",
  "message": "Please report your findings.",
  "mode": "steer"
}
```

`mode` is `follow-up` or `steer`. For compatibility, callers may instead send boolean `priority`; `true` requests steer. If both are present, `mode` determines intent. The receipt reports the requested mode and whether priority was actually applied:

```json
{
  "target": "agent-scout",
  "paneId": "%12",
  "kind": "pi",
  "status": "idle",
  "transport": "inbox",
  "requestedMode": "steer",
  "priorityApplied": true
}
```

`transport` is `inbox` or `herdr-prompt`. Prompt fallback cannot apply steer priority. Message bodies are never returned in replies or wire errors.

### Inspect

```json
{
  "requestId": "inspect-1",
  "providerInstanceId": "opaque-instance-id",
  "protocol": 1,
  "target": "agent-scout"
}
```

Inspect authorizes the selector against session-restored worker/orchestrator relationships before asking Herdr for live state. Unknown, self, and outside-team selectors return `NOT_TEAM_MEMBER` without probing an arbitrary agent. A configured peer absent from live Herdr state returns `NOT_FOUND`.

```json
{
  "name": "agent-scout",
  "paneId": "%12",
  "kind": "pi",
  "status": "idle",
  "cwd": "/workspace/project",
  "type": "explore",
  "purpose": "Map the subsystem",
  "model": "xai/grok-4.6",
  "relationship": "worker",
  "managedBySession": true
}
```

Saved `type`, `purpose`, and `model` apply to workers and may be omitted. `managedBySession` is true only for a worker controlled by this session; it is false for the orchestrator. Responses omit tab IDs, process details, pane contents, prompts, raw CLI output, and unrelated agents.

### Stop

The stop channel is registered so callers receive a deterministic response, but version 1 does not support termination. Stop is omitted from capabilities and always returns `UNSUPPORTED_OPERATION` after request, routing, and protocol validation, even when the provider is unavailable. It never releases a team member, closes a pane, sends Ctrl-C, runs service code, or performs shutdown cleanup.

## Validation and compatibility

All payloads are runtime validated before dispatch. IDs (`requestId` and `providerInstanceId`) use `[A-Za-z0-9._-]{1,128}`. Other limits are:

| Field | Limit |
|---|---:|
| `supportedProtocols` | 1-8 unique positive integers |
| worker `name` | 1-32 characters; domain naming rules also apply |
| `target` | 1-128 characters |
| `model`, `type` | 1-128 characters each |
| `purpose` | 1-1,024 characters |
| `cwd` | 1-4,096 characters |
| `message`, `initialPrompt` | 1-65,536 characters each |
| error `message` | 1-1,024 characters |

Unknown object fields are ignored for forward compatibility. Known fields retain their version-1 meaning and limits. New protocol versions should be added to probe negotiation rather than silently changing version-1 behavior.

Errors use only `INVALID_REQUEST`, `UNSUPPORTED_PROTOCOL`, `PROVIDER_UNAVAILABLE`, `NOT_FOUND`, `NOT_TEAM_MEMBER`, `UNSUPPORTED_OPERATION`, and `INTERNAL_ERROR`. Unknown service failures map to a fixed `INTERNAL_ERROR`; replies do not expose stacks, environment values, raw Herdr errors, prompts, or message bodies.

The bundled client accepts a positive finite timeout up to 300 seconds and cleans up its reply listener on success, failure, timeout, abort, or synchronous emit failure. Aborting or timing out bounds the caller's wait, not necessarily external side effects already started by the provider.

## Raw `pi.events` example

This generic example uses no imports from this package:

```ts
const requestId = crypto.randomUUID();
const requestChannel = "herdr-workers:rpc:probe";
const replyChannel = `${requestChannel}:reply:${requestId}`;

const result = await new Promise((resolve, reject) => {
  const unsubscribe = pi.events.on(replyChannel, (reply) => {
    unsubscribe();
    clearTimeout(timer);
    resolve(reply);
  });
  const timer = setTimeout(() => {
    unsubscribe();
    reject(new Error("probe timed out"));
  }, 2000);
  pi.events.emit(requestChannel, { requestId, supportedProtocols: [1] });
});
```

Validate replies in the calling extension, select an available provider, then repeat the listener-before-emit pattern on the chosen operation channel.
