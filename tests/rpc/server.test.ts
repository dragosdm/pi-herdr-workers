import assert from "node:assert/strict";
import test from "node:test";
import { CHANNELS, LIMITS, WorkerRpcServiceError, replyChannel, type RpcReply } from "../../rpc/protocol.js";
import { registerWorkerRpcServer, type WorkerRpcService } from "../../rpc/server.js";
import { FakeEventBus } from "../support/fake-event-bus.js";

function service(overrides: Partial<WorkerRpcService> = {}): WorkerRpcService {
  return {
    spawn: async () => ({ runId: "run-1", name: "worker", paneId: "%1", cwd: "/tmp", adopted: false }),
    send: async () => ({ target: "worker", paneId: "%1", transport: "inbox", requestedMode: "follow-up", priorityApplied: false }),
    inspect: async () => ({ name: "worker", paneId: "%1", relationship: "worker", managedBySession: true }),
    ...overrides,
  };
}

async function flush(): Promise<void> { await new Promise((resolve) => setImmediate(resolve)); }

test("probe negotiates, reports capabilities, and excludes stop", async () => {
  const events = new FakeEventBus();
  registerWorkerRpcServer({ events, service: service(), getProviderState: () => ({ available: true }), createInstanceId: () => "instance" });
  let reply: RpcReply | undefined;
  events.on(replyChannel(CHANNELS.probe, "probe"), (payload) => { reply = payload as RpcReply; });
  events.emit(CHANNELS.probe, { requestId: "probe", supportedProtocols: [2, 1], future: true });
  await flush();
  assert.equal(reply?.success, true);
  if (reply?.success) assert.deepEqual(reply.data, { protocol: 1, provider: "herdr", providerInstanceId: "instance", available: true, capabilities: ["spawn", "send", "steer", "inspect"], constraints: { requiresHerdrPane: true, requiresInteractivePi: true } });
});

test("validation, routing, protocol, stop, and availability gates precede service", async () => {
  const events = new FakeEventBus();
  let calls = 0;
  registerWorkerRpcServer({ events, service: service({ spawn: async () => { calls++; throw new Error("secret stack and body"); } }), getProviderState: () => ({ available: false }), createInstanceId: () => "instance" });
  const replies: RpcReply[] = [];
  for (const id of ["bad", "protocol", "stop", "unavailable"]) events.on(replyChannel(CHANNELS.spawn, id), (p) => replies.push(p as RpcReply));
  events.on(replyChannel(CHANNELS.stop, "stop"), (p) => replies.push(p as RpcReply));
  events.emit(CHANNELS.spawn, { requestId: "bad", providerInstanceId: "instance", protocol: 1, name: "x".repeat(33) });
  events.emit(CHANNELS.spawn, { requestId: "ignored", providerInstanceId: "other", protocol: 1 });
  events.emit(CHANNELS.spawn, { requestId: "protocol", providerInstanceId: "instance", protocol: 2 });
  events.emit(CHANNELS.stop, { requestId: "stop", providerInstanceId: "instance", protocol: 1 });
  events.emit(CHANNELS.spawn, { requestId: "unavailable", providerInstanceId: "instance", protocol: 1 });
  events.emit(CHANNELS.spawn, { providerInstanceId: "instance", protocol: 1 });
  await flush();
  assert.deepEqual(replies.map((reply) => reply.success ? "success" : reply.error.code), ["INVALID_REQUEST", "UNSUPPORTED_PROTOCOL", "UNSUPPORTED_OPERATION", "PROVIDER_UNAVAILABLE"]);
  assert.equal(calls, 0);
});

test("oversized UTF-8 fields are rejected before service dispatch", async () => {
  const events = new FakeEventBus();
  let spawnCalls = 0;
  let sendCalls = 0;
  registerWorkerRpcServer({
    events,
    service: service({
      spawn: async () => { spawnCalls++; return { runId: "run-1", name: "worker", paneId: "%1", cwd: "/tmp", adopted: false }; },
      send: async () => { sendCalls++; return { target: "worker", paneId: "%1", transport: "inbox", requestedMode: "follow-up", priorityApplied: false }; },
    }),
    getProviderState: () => ({ available: true }),
    createInstanceId: () => "instance",
  });
  const replies: RpcReply[] = [];
  events.on(replyChannel(CHANNELS.spawn, "large-prompt"), (payload) => { replies.push(payload as RpcReply); });
  events.on(replyChannel(CHANNELS.send, "large-message"), (payload) => { replies.push(payload as RpcReply); });
  const oversized = "é".repeat((LIMITS.message / 2) + 1);
  events.emit(CHANNELS.spawn, { requestId: "large-prompt", providerInstanceId: "instance", protocol: 1, initialPrompt: oversized });
  events.emit(CHANNELS.send, { requestId: "large-message", providerInstanceId: "instance", protocol: 1, target: "worker", message: oversized });
  await flush();
  assert.equal(spawnCalls, 0);
  assert.equal(sendCalls, 0);
  assert.deepEqual(replies.map((reply) => reply.success ? "success" : reply.error.code), ["INVALID_REQUEST", "INVALID_REQUEST"]);
});

test("service dispatches once, ignores unknown fields, and sanitizes failures", async () => {
  const events = new FakeEventBus();
  let calls = 0;
  registerWorkerRpcServer({ events, service: service({ send: async (input) => { calls++; assert.equal(input.priority, true); throw new Error("private message"); } }), getProviderState: () => ({ available: true }), createInstanceId: () => "instance" });
  let reply: RpcReply | undefined;
  events.on(replyChannel(CHANNELS.send, "send"), (p) => { reply = p as RpcReply; });
  events.emit(CHANNELS.send, { requestId: "send", providerInstanceId: "instance", protocol: 1, target: "worker", message: "body", priority: true, future: 1 });
  await flush();
  assert.equal(calls, 1);
  assert.deepEqual(reply, { requestId: "send", protocol: 1, success: false, error: { code: "INTERNAL_ERROR", message: "The worker operation failed." } });
});

test("malformed generated and service results become fixed internal errors", async () => {
  const events = new FakeEventBus();
  let providerStateCalls = 0;
  registerWorkerRpcServer({
    events,
    service: service({
      spawn: async () => ({ runId: "private/run", name: "worker", paneId: "%1", cwd: "/tmp", adopted: false } as never),
      send: async () => ({ target: "worker", paneId: "%1", transport: "private", requestedMode: "steer", priorityApplied: true } as never),
      inspect: async () => ({ name: "worker", paneId: "%1", relationship: "private", managedBySession: true } as never),
    }),
    getProviderState: () => providerStateCalls++ === 0
      ? { available: false, reason: "PRIVATE_REASON" as never }
      : { available: true },
    createInstanceId: () => "instance",
  });
  const requests = [
    { channel: CHANNELS.probe, id: "probe-result", payload: { requestId: "probe-result", supportedProtocols: [1] } },
    { channel: CHANNELS.spawn, id: "spawn-result", payload: { requestId: "spawn-result", providerInstanceId: "instance", protocol: 1 } },
    { channel: CHANNELS.send, id: "send-result", payload: { requestId: "send-result", providerInstanceId: "instance", protocol: 1, target: "worker", message: "hello" } },
    { channel: CHANNELS.inspect, id: "inspect-result", payload: { requestId: "inspect-result", providerInstanceId: "instance", protocol: 1, target: "worker" } },
  ] as const;
  const replies: RpcReply[] = [];
  for (const request of requests) {
    events.on(replyChannel(request.channel, request.id), (payload) => { replies.push(payload as RpcReply); });
    events.emit(request.channel, request.payload);
  }
  await flush();
  assert.equal(replies.length, 4);
  for (const reply of replies) {
    assert.deepEqual(reply, {
      requestId: reply.requestId,
      protocol: 1,
      success: false,
      error: { code: "INTERNAL_ERROR", message: "The worker operation failed." },
    });
    assert.doesNotMatch(JSON.stringify(reply), /private/i);
  }
});

test("addressed spawn forwards bounded input once and returns worker facts", async () => {
  const events = new FakeEventBus();
  let calls = 0;
  registerWorkerRpcServer({
    events,
    service: service({ spawn: async (input, provenance) => {
      calls++;
      assert.deepEqual(input, { correlationId: "dispatch-1", name: "scout", direction: "left", cwd: "/workspace", thinking: "high", initialPrompt: "Investigate" });
      assert.deepEqual(provenance, { requestId: "spawn", providerInstanceId: "instance" });
      return { runId: "run-1", correlationId: "dispatch-1", name: "agent-scout", paneId: "%2", cwd: "/workspace", model: "test/model", type: "explore", purpose: "Investigate", adopted: false };
    } }),
    getProviderState: () => ({ available: true }),
    createInstanceId: () => "instance",
  });
  let reply: RpcReply | undefined;
  events.on(replyChannel(CHANNELS.spawn, "spawn"), (payload) => { reply = payload as RpcReply; });
  events.emit(CHANNELS.spawn, { requestId: "spawn", providerInstanceId: "instance", protocol: 1, correlationId: "dispatch-1", name: "scout", direction: "left", cwd: "/workspace", thinking: "high", initialPrompt: "Investigate", future: true });
  await flush();
  assert.equal(calls, 1);
  assert.deepEqual(reply, { requestId: "spawn", protocol: 1, success: true, data: { runId: "run-1", correlationId: "dispatch-1", name: "agent-scout", paneId: "%2", cwd: "/workspace", model: "test/model", type: "explore", purpose: "Investigate", adopted: false } });
});

test("send validates before delegation and returns typed transport facts", async () => {
  const events = new FakeEventBus();
  const inputs: unknown[] = [];
  registerWorkerRpcServer({
    events,
    service: service({ send: async (input) => {
      inputs.push(input);
      return input.mode === "steer"
        ? { target: "worker", paneId: "%1", kind: "pi", status: "idle", transport: "inbox", requestedMode: "steer", priorityApplied: true }
        : { target: "worker", paneId: "%1", kind: "shell", transport: "herdr-prompt", requestedMode: "follow-up", priorityApplied: false };
    } }),
    getProviderState: () => ({ available: true }),
    createInstanceId: () => "instance",
  });
  const replies: RpcReply[] = [];
  for (const id of ["invalid-send", "steer", "follow-up"]) events.on(replyChannel(CHANNELS.send, id), (payload) => { replies.push(payload as RpcReply); });
  events.emit(CHANNELS.send, { requestId: "invalid-send", providerInstanceId: "instance", protocol: 1, target: "worker", message: "" });
  events.emit(CHANNELS.send, { requestId: "steer", providerInstanceId: "instance", protocol: 1, runId: "run-1", target: "worker", message: "private steer body", mode: "steer" });
  events.emit(CHANNELS.send, { requestId: "follow-up", providerInstanceId: "instance", protocol: 1, target: "worker", message: "private follow-up body", priority: false });
  await flush();
  assert.equal(inputs.length, 2);
  assert.deepEqual(inputs, [
    { runId: "run-1", target: "worker", message: "private steer body", mode: "steer" },
    { target: "worker", message: "private follow-up body", priority: false },
  ]);
  assert.equal(replies[0].success, false);
  assert.deepEqual(replies.slice(1).map((reply) => reply.success && reply.data), [
    { target: "worker", paneId: "%1", kind: "pi", status: "idle", transport: "inbox", requestedMode: "steer", priorityApplied: true },
    { target: "worker", paneId: "%1", kind: "shell", transport: "herdr-prompt", requestedMode: "follow-up", priorityApplied: false },
  ]);
  assert.doesNotMatch(JSON.stringify(replies), /private .* body/);
});

test("explicit domain failures cross the wire safely", async () => {
  const events = new FakeEventBus();
  registerWorkerRpcServer({
    events,
    service: service({ inspect: async () => { throw new WorkerRpcServiceError("NOT_TEAM_MEMBER", "Target is not a team member."); } }),
    getProviderState: () => ({ available: true }),
    createInstanceId: () => "instance",
  });
  let reply: RpcReply | undefined;
  events.on(replyChannel(CHANNELS.inspect, "inspect"), (payload) => { reply = payload as RpcReply; });
  events.emit(CHANNELS.inspect, { requestId: "inspect", providerInstanceId: "instance", protocol: 1, target: "other" });
  await flush();
  assert.deepEqual(reply, { requestId: "inspect", protocol: 1, success: false, error: { code: "NOT_TEAM_MEMBER", message: "Target is not a team member." } });
});

test("inspect returns only the service's sanitized team projection", async () => {
  const events = new FakeEventBus();
  let input: unknown;
  registerWorkerRpcServer({
    events,
    service: service({ inspect: async (value) => {
      input = value;
      return { name: "worker", paneId: "%1", kind: "pi", status: "idle", cwd: "/tmp", type: "review", purpose: "Check RPC", model: "test/model", relationship: "worker", managedBySession: true };
    } }),
    getProviderState: () => ({ available: true }),
    createInstanceId: () => "instance",
  });
  let reply: RpcReply | undefined;
  events.on(replyChannel(CHANNELS.inspect, "inspect-safe"), (payload) => { reply = payload as RpcReply; });
  events.emit(CHANNELS.inspect, { requestId: "inspect-safe", providerInstanceId: "instance", protocol: 1, target: "worker", future: "ignored" });
  await flush();
  assert.deepEqual(input, { target: "worker" });
  assert.equal(reply?.success, true);
  assert.doesNotMatch(JSON.stringify(reply), /tabId|process|prompt|raw/);
});

test("dispose removes all five subscriptions idempotently", () => {
  const events = new FakeEventBus();
  const server = registerWorkerRpcServer({ events, service: service(), getProviderState: () => ({ available: true }) });
  assert.equal(events.listenerCount(), 5);
  server.dispose();
  server.dispose();
  assert.equal(events.listenerCount(), 0);
});
