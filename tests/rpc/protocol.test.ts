import assert from "node:assert/strict";
import test from "node:test";
import { Check } from "typebox/value";
import {
  CHANNELS, DeliveryReceiptSchema, InspectionSchema, LIMITS, ProbeDataSchema, ProbeRequestSchema,
  ReplyEnvelopeSchema, SendRequestSchema, SpawnRequestSchema, WorkerReferenceSchema,
  failure, isValidRequest, replyChannel, success,
} from "../../rpc/protocol.js";

const addressed = { requestId: "r", providerInstanceId: "p", protocol: 1 };

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

test("message and initialPrompt enforce UTF-8 byte budgets", () => {
  const cases = [
    { channel: CHANNELS.send, field: "message", base: { ...addressed, target: "worker" } },
    { channel: CHANNELS.spawn, field: "initialPrompt", base: addressed },
  ] as const;
  const asciiAtLimit = "a".repeat(LIMITS.message);
  const asciiOverLimit = `${asciiAtLimit}a`;
  const multibyteBelowLimit = "é".repeat((LIMITS.message / 2) - 1);
  const multibyteAtLimit = "é".repeat(LIMITS.message / 2);
  const multibyteOverLimit = `${multibyteAtLimit}é`;
  const fourByteCrossing = `${"a".repeat(LIMITS.message - 3)}😀`;

  for (const { channel, field, base } of cases) {
    const limit = field === "message" ? LIMITS.message : LIMITS.initialPrompt;
    assert.equal(limit, LIMITS.message);
    assert.equal(isValidRequest(channel, { ...base, [field]: asciiAtLimit }), true, `${field}: exact ASCII limit`);
    assert.equal(isValidRequest(channel, { ...base, [field]: asciiOverLimit }), false, `${field}: one ASCII byte over`);
    assert.equal(isValidRequest(channel, { ...base, [field]: multibyteBelowLimit }), true, `${field}: multibyte below limit`);
    assert.equal(isValidRequest(channel, { ...base, [field]: multibyteAtLimit }), true, `${field}: exact multibyte limit`);
    assert.equal(isValidRequest(channel, { ...base, [field]: multibyteOverLimit }), false, `${field}: multibyte over limit`);
    assert.equal(isValidRequest(channel, { ...base, [field]: fourByteCrossing }), false, `${field}: four-byte character crosses limit`);
  }
});

test("unknown request and reply fields are accepted", () => {
  assert.equal(Check(SendRequestSchema, {
    requestId: "r", providerInstanceId: "p", protocol: 1, target: "worker", message: "hello", future: true,
  }), true);
  assert.equal(Check(ReplyEnvelopeSchema, { ...success("r", { value: 1 }), future: true }), true);
});

test("result schemas validate complete shapes and accept unknown fields", () => {
  const results = [
    {
      schema: ProbeDataSchema,
      valid: {
        protocol: 1, provider: "herdr", providerInstanceId: "provider", available: false,
        reason: "SESSION_NOT_READY", capabilities: ["spawn", "send", "steer", "inspect"],
        constraints: { requiresHerdrPane: true, requiresInteractivePi: true, future: true }, future: true,
      },
      invalid: {
        protocol: 1, provider: "other", providerInstanceId: "provider", available: true,
        capabilities: [], constraints: { requiresHerdrPane: true, requiresInteractivePi: true },
      },
    },
    {
      schema: WorkerReferenceSchema,
      valid: { name: "worker", paneId: "%1", cwd: "/tmp", adopted: false, future: true },
      invalid: { name: "worker", paneId: "%1", cwd: "/tmp", adopted: "no" },
    },
    {
      schema: DeliveryReceiptSchema,
      valid: {
        target: "worker", paneId: "%1", transport: "inbox", requestedMode: "steer",
        priorityApplied: true, future: true,
      },
      invalid: {
        target: "worker", paneId: "%1", transport: "socket", requestedMode: "steer",
        priorityApplied: true,
      },
    },
    {
      schema: InspectionSchema,
      valid: {
        name: "worker", paneId: "%1", relationship: "worker", managedBySession: true, future: true,
      },
      invalid: { name: "worker", paneId: "%1", relationship: "peer", managedBySession: true },
    },
  ] as const;

  for (const { schema, valid, invalid } of results) {
    assert.equal(Check(schema, valid), true);
    assert.equal(Check(schema, invalid), false);
  }
});

test("reply channels reject unsafe IDs and envelopes validate", () => {
  assert.equal(replyChannel(CHANNELS.spawn, "request_1"), "herdr-workers:rpc:spawn:reply:request_1");
  assert.throws(() => replyChannel(CHANNELS.spawn, "bad/id"), TypeError);
  assert.equal(Check(ReplyEnvelopeSchema, success("r", { value: true })), true);
  assert.equal(Check(ReplyEnvelopeSchema, failure("r", "NOT_FOUND", "Not found.")), true);
  assert.equal(Check(ReplyEnvelopeSchema, { requestId: "r", protocol: 1, success: false, error: { code: "SECRET", message: "x" } }), false);
});
