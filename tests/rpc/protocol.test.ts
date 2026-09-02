import assert from "node:assert/strict";
import test from "node:test";
import { Check } from "typebox/value";
import {
  CHANNELS, LIMITS, ProbeRequestSchema, ReplyEnvelopeSchema, SendRequestSchema, SpawnRequestSchema,
  failure, replyChannel, success,
} from "../../rpc/protocol.js";

test("request schemas enforce identifiers, limits, and unique protocols", () => {
  assert.equal(Check(ProbeRequestSchema, { requestId: "safe-1", supportedProtocols: [1] }), true);
  assert.equal(Check(ProbeRequestSchema, { requestId: "unsafe/id", supportedProtocols: [1] }), false);
  assert.equal(Check(ProbeRequestSchema, { requestId: "safe", supportedProtocols: [1, 1] }), false);
  assert.equal(Check(SendRequestSchema, {
    requestId: "r", providerInstanceId: "p", protocol: 1, target: "worker", message: "x".repeat(LIMITS.message + 1),
  }), false);
  const spawnBase = { requestId: "r", providerInstanceId: "p", protocol: 1 };
  assert.equal(Check(SpawnRequestSchema, { ...spawnBase, direction: "left", thinking: "xhigh" }), true);
  assert.equal(Check(SpawnRequestSchema, { ...spawnBase, direction: "diagonal" }), false);
  assert.equal(Check(SpawnRequestSchema, { ...spawnBase, thinking: "unlimited" }), false);
});

test("unknown request and reply fields are accepted", () => {
  assert.equal(Check(SendRequestSchema, {
    requestId: "r", providerInstanceId: "p", protocol: 1, target: "worker", message: "hello", future: true,
  }), true);
  assert.equal(Check(ReplyEnvelopeSchema, { ...success("r", { value: 1 }), future: true }), true);
});

test("reply channels reject unsafe IDs and envelopes validate", () => {
  assert.equal(replyChannel(CHANNELS.spawn, "request_1"), "herdr-workers:rpc:spawn:reply:request_1");
  assert.throws(() => replyChannel(CHANNELS.spawn, "bad/id"), TypeError);
  assert.equal(Check(ReplyEnvelopeSchema, success("r", { ok: true })), true);
  assert.equal(Check(ReplyEnvelopeSchema, failure("r", "NOT_FOUND", "Not found.")), true);
  assert.equal(Check(ReplyEnvelopeSchema, { requestId: "r", protocol: 1, ok: false, error: { code: "SECRET", message: "x" } }), false);
});
