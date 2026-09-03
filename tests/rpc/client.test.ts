import assert from "node:assert/strict";
import test from "node:test";
import { createWorkerRpcClient, RpcAbortError, RpcProtocolError, RpcResponseError, RpcTimeoutError } from "../../rpc/client.js";
import { CHANNELS, failure, replyChannel, success } from "../../rpc/protocol.js";
import { FakeEventBus } from "../support/fake-event-bus.js";

const provider = { protocol: 1 as const, provider: "herdr" as const, providerInstanceId: "provider", available: true as const, capabilities: [], constraints: { requiresHerdrPane: true as const, requiresInteractivePi: true as const } };
const workerReference = { name: "worker", paneId: "%1", cwd: "/tmp", adopted: false };
const deliveryReceipt = { target: "worker", paneId: "%1", transport: "inbox" as const, requestedMode: "follow-up" as const, priorityApplied: false };
const inspection = { name: "worker", paneId: "%1", relationship: "worker" as const, managedBySession: true };

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

test("probe selects a compatible provider after an earlier unsupported responder", async () => {
  const events = new FakeEventBus();
  events.on(CHANNELS.probe, (payload) => {
    const request = payload as { requestId: string };
    events.emit(
      replyChannel(CHANNELS.probe, request.requestId),
      failure(request.requestId, "UNSUPPORTED_PROTOCOL", "Responder only supports a newer protocol."),
    );
  });
  events.on(CHANNELS.probe, (payload) => {
    const request = payload as { requestId: string };
    events.emit(replyChannel(CHANNELS.probe, request.requestId), success(request.requestId, provider));
  });

  const selected = await createWorkerRpcClient({ events, createRequestId: () => "probe-compatible" }).probe();

  assert.equal(selected.providerInstanceId, "provider");
  assert.equal(events.listenerCount(replyChannel(CHANNELS.probe, "probe-compatible")), 0);
});

test("probe timeout prefers a compatible unavailable provider over unsupported protocol", async () => {
  const events = new FakeEventBus();
  const unavailable = { ...provider, providerInstanceId: "unavailable", available: false as const, reason: "NOT_INTERACTIVE" as const };
  events.on(CHANNELS.probe, (payload) => {
    const request = payload as { requestId: string };
    events.emit(
      replyChannel(CHANNELS.probe, request.requestId),
      failure(request.requestId, "UNSUPPORTED_PROTOCOL", "Protocol mismatch."),
    );
  });
  events.on(CHANNELS.probe, (payload) => {
    const request = payload as { requestId: string };
    events.emit(replyChannel(CHANNELS.probe, request.requestId), success(request.requestId, unavailable));
  });

  const selected = await createWorkerRpcClient({ events, createRequestId: () => "probe-unavailable-protocol" }).probe({ timeoutMs: 1 });

  assert.equal(selected.providerInstanceId, "unavailable");
  assert.equal(selected.available, false);
  assert.equal(events.listenerCount(replyChannel(CHANNELS.probe, "probe-unavailable-protocol")), 0);
});

test("probe timeout prefers the first unsupported protocol error over malformed successes", async () => {
  const events = new FakeEventBus();
  events.on(CHANNELS.probe, (payload) => {
    const request = payload as { requestId: string };
    const channel = replyChannel(CHANNELS.probe, request.requestId);
    events.emit(channel, failure(request.requestId, "UNSUPPORTED_PROTOCOL", "First protocol mismatch."));
    events.emit(channel, success(request.requestId, { ...provider, constraints: {} }));
    events.emit(channel, failure(request.requestId, "UNSUPPORTED_PROTOCOL", "Second protocol mismatch."));
  });

  const call = createWorkerRpcClient({ events, createRequestId: () => "probe-protocol-malformed" }).probe({ timeoutMs: 1 });

  await assert.rejects(call, (error: unknown) => {
    assert.ok(error instanceof RpcResponseError);
    assert.equal(error.code, "UNSUPPORTED_PROTOCOL");
    assert.equal(error.message, "First protocol mismatch.");
    return true;
  });
  assert.equal(events.listenerCount(replyChannel(CHANNELS.probe, "probe-protocol-malformed")), 0);
});

test("unsupported-only probe waits until timeout before rejecting and cleans up", async () => {
  const events = new FakeEventBus();
  events.on(CHANNELS.probe, (payload) => {
    const request = payload as { requestId: string };
    events.emit(
      replyChannel(CHANNELS.probe, request.requestId),
      failure(request.requestId, "UNSUPPORTED_PROTOCOL", "No mutually supported protocol."),
    );
  });
  const call = createWorkerRpcClient({ events, createRequestId: () => "probe-protocol-only" }).probe({ timeoutMs: 5 });
  let completed = false;
  void call.then(() => { completed = true; }, () => { completed = true; });

  await Promise.resolve();
  assert.equal(completed, false);
  await assert.rejects(call, (error: unknown) => {
    assert.ok(error instanceof RpcResponseError);
    assert.equal(error.code, "UNSUPPORTED_PROTOCOL");
    assert.equal(error.message, "No mutually supported protocol.");
    return true;
  });
  assert.equal(events.listenerCount(replyChannel(CHANNELS.probe, "probe-protocol-only")), 0);
});

test("probe abort and synchronous emit failure take precedence after unsupported protocol", async () => {
  const abortBus = new FakeEventBus();
  abortBus.on(CHANNELS.probe, (payload) => {
    const request = payload as { requestId: string };
    abortBus.emit(
      replyChannel(CHANNELS.probe, request.requestId),
      failure(request.requestId, "UNSUPPORTED_PROTOCOL", "Protocol mismatch."),
    );
  });
  const controller = new AbortController();
  const aborted = createWorkerRpcClient({ events: abortBus, createRequestId: () => "probe-protocol-abort" }).probe({ timeoutMs: 50, signal: controller.signal });
  controller.abort();
  await assert.rejects(aborted, RpcAbortError);
  assert.equal(abortBus.listenerCount(replyChannel(CHANNELS.probe, "probe-protocol-abort")), 0);

  let replyListener: ((payload: unknown) => void) | undefined;
  const throwBus = {
    on: (_channel: string, listener: (payload: unknown) => void) => { replyListener = listener; return () => { replyListener = undefined; }; },
    emit: (_channel: string, payload: unknown) => {
      const request = payload as { requestId: string };
      replyListener?.(failure(request.requestId, "UNSUPPORTED_PROTOCOL", "Protocol mismatch."));
      throw new Error("emit failed after unsupported response");
    },
  };
  await assert.rejects(
    createWorkerRpcClient({ events: throwBus, createRequestId: () => "probe-protocol-emit" }).probe(),
    /emit failed after unsupported response/,
  );
  assert.equal(replyListener, undefined);
});

test("probe keeps an immediately selected provider despite later responder activity", async () => {
  const events = new FakeEventBus();
  events.on(CHANNELS.probe, (payload) => {
    const request = payload as { requestId: string };
    events.emit(replyChannel(CHANNELS.probe, request.requestId), success(request.requestId, provider));
  });
  events.on(CHANNELS.probe, (payload) => {
    const request = payload as { requestId: string };
    const channel = replyChannel(CHANNELS.probe, request.requestId);
    events.emit(channel, failure(request.requestId, "UNSUPPORTED_PROTOCOL", "Late protocol mismatch."));
    events.emit(channel, success(request.requestId, {}));
  });

  const selected = await createWorkerRpcClient({ events, createRequestId: () => "probe-selected-first" }).probe();

  assert.equal(selected, provider);
  assert.equal(events.listenerCount(replyChannel(CHANNELS.probe, "probe-selected-first")), 0);
});

test("probe skips malformed successes and selects a later valid provider", async () => {
  const events = new FakeEventBus();
  events.on(CHANNELS.probe, (payload) => {
    const request = payload as { requestId: string };
    const channel = replyChannel(CHANNELS.probe, request.requestId);
    events.emit(channel, success(request.requestId, { ...provider, capabilities: ["private"] }));
    events.emit(channel, success(request.requestId, provider));
  });
  const selected = await createWorkerRpcClient({ events, createRequestId: () => "probe-fallback" }).probe();
  assert.equal(selected.providerInstanceId, "provider");
  assert.equal(events.listenerCount(), 1);
});

test("probe timeout prefers a valid unavailable provider over malformed successes", async () => {
  const events = new FakeEventBus();
  const unavailable = { ...provider, available: false, reason: "SESSION_NOT_READY" as const };
  events.on(CHANNELS.probe, (payload) => {
    const request = payload as { requestId: string };
    const channel = replyChannel(CHANNELS.probe, request.requestId);
    events.emit(channel, success(request.requestId, { ...provider, constraints: {} }));
    events.emit(channel, success(request.requestId, unavailable));
  });
  const selected = await createWorkerRpcClient({ events, createRequestId: () => "probe-unavailable" }).probe({ timeoutMs: 1 });
  assert.equal(selected.available, false);
  assert.equal(events.listenerCount(), 1);
});

test("malformed-only probe rejects with a fixed protocol error and cleans up", async () => {
  const events = new FakeEventBus();
  events.on(CHANNELS.probe, (payload) => {
    const request = payload as { requestId: string };
    const channel = replyChannel(CHANNELS.probe, request.requestId);
    events.emit(channel, success(request.requestId, { secret: "provider details" }));
    events.emit(channel, success(request.requestId, { secret: "duplicate details" }));
  });
  const call = createWorkerRpcClient({ events, createRequestId: () => "probe-malformed" }).probe({ timeoutMs: 1 });
  await assert.rejects(call, (error: unknown) => {
    assert.ok(error instanceof RpcProtocolError);
    assert.equal(error.code, "INVALID_RESPONSE");
    assert.equal(error.message, "The worker provider returned an invalid response.");
    assert.doesNotMatch(error.message, /secret|details/);
    return true;
  });
  assert.equal(events.listenerCount(), 1);
});

test("probe abort and synchronous emit failure take precedence after malformed success", async () => {
  const abortBus = new FakeEventBus();
  abortBus.on(CHANNELS.probe, (payload) => {
    const request = payload as { requestId: string };
    abortBus.emit(replyChannel(CHANNELS.probe, request.requestId), success(request.requestId, {}));
  });
  const controller = new AbortController();
  const aborted = createWorkerRpcClient({ events: abortBus, createRequestId: () => "probe-abort" }).probe({ timeoutMs: 50, signal: controller.signal });
  controller.abort();
  await assert.rejects(aborted, RpcAbortError);

  let replyListener: ((payload: unknown) => void) | undefined;
  const throwBus = {
    on: (_channel: string, listener: (payload: unknown) => void) => { replyListener = listener; return () => { replyListener = undefined; }; },
    emit: (_channel: string, payload: unknown) => {
      const request = payload as { requestId: string };
      replyListener?.(success(request.requestId, {}));
      throw new Error("emit failed after malformed response");
    },
  };
  await assert.rejects(
    createWorkerRpcClient({ events: throwBus, createRequestId: () => "probe-emit" }).probe(),
    /emit failed after malformed response/,
  );
});

test("concurrent calls correlate replies and ignore duplicates", async () => {
  const events = new FakeEventBus();
  const ids = ["one", "two"];
  const client = createWorkerRpcClient({ events, createRequestId: () => ids.shift()! });
  const first = client.inspect({ target: "a" }, provider);
  const second = client.inspect({ target: "b" }, provider);
  events.emit(replyChannel(CHANNELS.inspect, "two"), success("two", { ...inspection, name: "b" }));
  events.emit(replyChannel(CHANNELS.inspect, "two"), success("two", { ...inspection, name: "wrong" }));
  events.emit(replyChannel(CHANNELS.inspect, "one"), success("one", { ...inspection, name: "a" }));
  assert.deepEqual(await Promise.all([first, second]), [{ ...inspection, name: "a" }, { ...inspection, name: "b" }]);
  assert.equal(events.listenerCount(), 0);
});

test("addressed operations validate every successful result shape", async () => {
  const validCases = [
    { operation: "spawn", result: workerReference },
    { operation: "send", result: deliveryReceipt },
    { operation: "inspect", result: inspection },
  ] as const;
  for (const testCase of validCases) {
    const events = new FakeEventBus();
    events.on(CHANNELS[testCase.operation], (payload) => {
      const request = payload as { requestId: string };
      events.emit(replyChannel(CHANNELS[testCase.operation], request.requestId), success(request.requestId, { ...testCase.result, future: true }));
    });
    const client = createWorkerRpcClient({ events, createRequestId: () => `valid-${testCase.operation}` });
    const result = testCase.operation === "spawn"
      ? await client.spawn({}, provider)
      : testCase.operation === "send"
        ? await client.send({ target: "worker", message: "hello" }, provider)
        : await client.inspect({ target: "worker" }, provider);
    assert.equal(result.paneId, "%1");
  }
});

test("malformed addressed successes reject without exposing provider data and clean up", async () => {
  const malformedCases = [
    { operation: "spawn", result: { ...workerReference, adopted: "secret-spawn" } },
    { operation: "send", result: { ...deliveryReceipt, transport: "secret-send" } },
    { operation: "inspect", result: { ...inspection, relationship: "secret-inspect" } },
  ] as const;
  for (const testCase of malformedCases) {
    const events = new FakeEventBus();
    events.on(CHANNELS[testCase.operation], (payload) => {
      const request = payload as { requestId: string };
      events.emit(replyChannel(CHANNELS[testCase.operation], request.requestId), success(request.requestId, testCase.result));
    });
    const client = createWorkerRpcClient({ events, createRequestId: () => `invalid-${testCase.operation}` });
    const call = testCase.operation === "spawn"
      ? client.spawn({}, provider)
      : testCase.operation === "send"
        ? client.send({ target: "worker", message: "hello" }, provider)
        : client.inspect({ target: "worker" }, provider);
    await assert.rejects(call, (error: unknown) => {
      assert.ok(error instanceof RpcProtocolError);
      assert.equal(error.code, "INVALID_RESPONSE");
      assert.equal(error.message, "The worker provider returned an invalid response.");
      assert.doesNotMatch(error.message, /secret|adopted|transport|relationship/);
      return true;
    });
    assert.equal(events.listenerCount(), 1);
  }
});

test("stop rejects an unexpected success as an invalid response", async () => {
  const events = new FakeEventBus();
  events.on(CHANNELS.stop, (payload) => {
    const request = payload as { requestId: string };
    events.emit(replyChannel(CHANNELS.stop, request.requestId), success(request.requestId, { secret: true }));
  });
  await assert.rejects(
    createWorkerRpcClient({ events, createRequestId: () => "stop-success" }).stop(undefined, provider),
    RpcProtocolError,
  );
  assert.equal(events.listenerCount(), 1);
});

test("spawn forwards direction and thinking in the addressed request", async () => {
  const events = new FakeEventBus();
  events.on(CHANNELS.spawn, (payload) => {
    const request = payload as Record<string, unknown> & { requestId: string };
    assert.equal(request.direction, "down");
    assert.equal(request.thinking, "xhigh");
    events.emit(replyChannel(CHANNELS.spawn, request.requestId), success(request.requestId, { name: "agent-test", paneId: "%1", cwd: "/tmp", adopted: false }));
  });
  const result = await createWorkerRpcClient({ events, createRequestId: () => "spawn-options" }).spawn(
    { name: "test", direction: "down", thinking: "xhigh" }, provider,
  );
  assert.equal(result.name, "agent-test");
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
      requestId: request.requestId, protocol: 1, success: false, error: { code: "UNSUPPORTED_OPERATION", message: "Stop is not supported." },
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
