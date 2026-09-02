import assert from "node:assert/strict";
import test from "node:test";
import { CHANNELS, WorkerRpcServiceError, replyChannel, type RpcReply } from "../../rpc/protocol.js";
import { registerWorkerRpcServer, type WorkerRpcService } from "../../rpc/server.js";
import { FakeEventBus } from "../support/fake-event-bus.js";

function service(overrides: Partial<WorkerRpcService> = {}): WorkerRpcService {
  return {
    spawn: async () => ({ name: "worker", paneId: "%1", cwd: "/tmp", adopted: false }),
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
  assert.equal(reply?.ok, true);
  if (reply?.ok) assert.deepEqual(reply.data, { protocol: 1, providerInstanceId: "instance", available: true, capabilities: ["spawn", "send", "steer", "inspect"] });
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
  assert.deepEqual(replies.map((reply) => reply.ok ? "ok" : reply.error.code), ["INVALID_REQUEST", "UNSUPPORTED_PROTOCOL", "UNSUPPORTED_OPERATION", "PROVIDER_UNAVAILABLE"]);
  assert.equal(calls, 0);
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
  assert.deepEqual(reply, { requestId: "send", protocol: 1, ok: false, error: { code: "INTERNAL_ERROR", message: "The worker operation failed." } });
});

test("addressed spawn forwards bounded input once and returns worker facts", async () => {
  const events = new FakeEventBus();
  let calls = 0;
  registerWorkerRpcServer({
    events,
    service: service({ spawn: async (input) => {
      calls++;
      assert.deepEqual(input, { name: "scout", direction: "left", cwd: "/workspace", thinking: "high", initialPrompt: "Investigate" });
      return { name: "agent-scout", paneId: "%2", cwd: "/workspace", model: "test/model", type: "explore", purpose: "Investigate", adopted: false };
    } }),
    getProviderState: () => ({ available: true }),
    createInstanceId: () => "instance",
  });
  let reply: RpcReply | undefined;
  events.on(replyChannel(CHANNELS.spawn, "spawn"), (payload) => { reply = payload as RpcReply; });
  events.emit(CHANNELS.spawn, { requestId: "spawn", providerInstanceId: "instance", protocol: 1, name: "scout", direction: "left", cwd: "/workspace", thinking: "high", initialPrompt: "Investigate", future: true });
  await flush();
  assert.equal(calls, 1);
  assert.deepEqual(reply, { requestId: "spawn", protocol: 1, ok: true, data: { name: "agent-scout", paneId: "%2", cwd: "/workspace", model: "test/model", type: "explore", purpose: "Investigate", adopted: false } });
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
  assert.deepEqual(reply, { requestId: "inspect", protocol: 1, ok: false, error: { code: "NOT_TEAM_MEMBER", message: "Target is not a team member." } });
});

test("dispose removes all five subscriptions idempotently", () => {
  const events = new FakeEventBus();
  const server = registerWorkerRpcServer({ events, service: service(), getProviderState: () => ({ available: true }) });
  assert.equal(events.listenerCount(), 5);
  server.dispose();
  server.dispose();
  assert.equal(events.listenerCount(), 0);
});
