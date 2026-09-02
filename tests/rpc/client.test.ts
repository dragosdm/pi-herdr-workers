import assert from "node:assert/strict";
import test from "node:test";
import { createWorkerRpcClient, RpcAbortError, RpcResponseError, RpcTimeoutError } from "../../rpc/client.js";
import { CHANNELS, replyChannel, success } from "../../rpc/protocol.js";
import { FakeEventBus } from "../support/fake-event-bus.js";

const provider = { protocol: 1 as const, providerInstanceId: "provider", available: true, capabilities: [] };

test("probe installs listener before emit and selects an available provider", async () => {
  const events = new FakeEventBus();
  events.on(CHANNELS.probe, (payload) => {
    const request = payload as { requestId: string };
    events.emit(replyChannel(CHANNELS.probe, request.requestId), success(request.requestId, provider));
  });
  const selected = await createWorkerRpcClient({ events, createRequestId: () => "probe-1" }).probe();
  assert.equal(selected.providerInstanceId, "provider");
  assert.equal(events.listenerCount(replyChannel(CHANNELS.probe, "probe-1")), 0);
});

test("concurrent calls correlate replies and ignore duplicates", async () => {
  const events = new FakeEventBus();
  const ids = ["one", "two"];
  const client = createWorkerRpcClient({ events, createRequestId: () => ids.shift()! });
  const first = client.inspect({ target: "a" }, provider);
  const second = client.inspect({ target: "b" }, provider);
  events.emit(replyChannel(CHANNELS.inspect, "two"), success("two", { name: "b" }));
  events.emit(replyChannel(CHANNELS.inspect, "two"), success("two", { name: "wrong" }));
  events.emit(replyChannel(CHANNELS.inspect, "one"), success("one", { name: "a" }));
  assert.deepEqual(await Promise.all([first, second]), [{ name: "a" }, { name: "b" }]);
  assert.equal(events.listenerCount(), 0);
});

test("timeout, abort, server error, and emit failure clean up", async () => {
  const timeoutBus = new FakeEventBus();
  await assert.rejects(createWorkerRpcClient({ events: timeoutBus, createRequestId: () => "timeout" }).inspect(
    { target: "a" }, provider, { timeoutMs: 1 },
  ), RpcTimeoutError);
  assert.equal(timeoutBus.listenerCount(), 0);

  const abortBus = new FakeEventBus();
  const controller = new AbortController();
  const waiting = createWorkerRpcClient({ events: abortBus, createRequestId: () => "abort" }).send(
    { target: "a", message: "hello" }, provider, { signal: controller.signal },
  );
  controller.abort();
  await assert.rejects(waiting, RpcAbortError);
  assert.equal(abortBus.listenerCount(), 0);

  const errorBus = new FakeEventBus();
  errorBus.on(CHANNELS.stop, (payload) => {
    const request = payload as { requestId: string };
    errorBus.emit(replyChannel(CHANNELS.stop, request.requestId), {
      requestId: request.requestId, protocol: 1, ok: false, error: { code: "UNSUPPORTED_OPERATION", message: "Stop is not supported." },
    });
  });
  await assert.rejects(createWorkerRpcClient({ events: errorBus, createRequestId: () => "stop" }).stop(undefined, provider), RpcResponseError);

  const throwBus = { on: () => () => {}, emit: () => { throw new Error("emit failed"); } };
  await assert.rejects(createWorkerRpcClient({ events: throwBus, createRequestId: () => "emit" }).inspect({ target: "a" }, provider), /emit failed/);
});

test("timeout overrides are bounded", async () => {
  const events = new FakeEventBus();
  const client = createWorkerRpcClient({ events });
  await assert.rejects(client.inspect({ target: "a" }, provider, { timeoutMs: 300_001 }), RangeError);
});
