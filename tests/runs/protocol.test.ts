import assert from "node:assert/strict";
import test from "node:test";
import { Check } from "typebox/value";
import { CAPABILITIES, CHANNELS, WorkerReferenceSchema } from "../../rpc/protocol.js";
import {
	GetRunRequestSchema,
	LegacyWorkerRunProjectionV1Schema,
	ListRunsRequestSchema,
	RUN_QUERY_CHANNELS,
	RUN_QUERY_LIMITS,
	RunQueryProbeRequestSchema,
	WorkerRunHandleV1Schema,
	WorkerRunRecordV1Schema,
	decodeRunQueryCursor,
	encodeRunQueryCursor,
	isValidRunQueryRecord,
	isValidRunQueryRequest,
	runQueryFailure,
	runQueryReplyChannel,
	runQuerySuccess,
} from "../../runs/protocol.js";

const addressed = { requestId: "request-1", providerInstanceId: "provider-1", protocol: 1 };
const strict = {
	protocol: 1,
	runId: "run-1",
	correlationId: "dispatch-1",
	requestId: "spawn-1",
	sessionId: "session-1",
	registeredAt: 1_786_000_000_000,
	lifecycle: { status: "started", acceptedSequence: 1, readiness: "unconfirmed" },
	assignment: { cwd: "/workspace", model: "test/model", role: "research" },
	endpoint: { agentName: "agent-scout", paneId: "pane-1", observedAt: 1_786_000_000_100 },
};
const legacy = {
	protocol: 1,
	legacy: true,
	runId: "run-legacy",
	sessionId: "session-1",
	lifecycle: { status: "completed", acceptedSequence: 2, readiness: "confirmed" },
	worker: { agentName: "agent-old", paneId: "pane-old" },
};

test("defines independent versioned channels and validates safe addressed requests", () => {
	assert.deepEqual(RUN_QUERY_CHANNELS, {
		probe: "herdr-workers:runs:rpc:probe",
		get: "herdr-workers:runs:rpc:get",
		list: "herdr-workers:runs:rpc:list",
	});
	assert.equal(Check(RunQueryProbeRequestSchema, { requestId: "probe-1", supportedProtocols: [2, 1] }), true);
	assert.equal(Check(RunQueryProbeRequestSchema, { requestId: "bad/id", supportedProtocols: [1] }), false);
	assert.equal(Check(GetRunRequestSchema, { ...addressed, runId: "run-1", includeEndpointObservation: true }), true);
	assert.equal(Check(GetRunRequestSchema, { ...addressed, runId: "bad/id" }), false);
	assert.equal(Check(ListRunsRequestSchema, { ...addressed, limit: RUN_QUERY_LIMITS.maxPageSize }), true);
	assert.equal(Check(ListRunsRequestSchema, { ...addressed, limit: RUN_QUERY_LIMITS.maxPageSize + 1 }), false);
	assert.equal(Check(ListRunsRequestSchema, { ...addressed, limit: 0 }), false);
});

test("round trips canonical opaque cursors and rejects malformed encodings", () => {
	const cursor = encodeRunQueryCursor("run._-1");
	assert.equal(decodeRunQueryCursor(cursor), "run._-1");
	assert.equal(isValidRunQueryRequest(RUN_QUERY_CHANNELS.list, { ...addressed, cursor }), true);
	for (const malformed of ["***", "cmVuLTE=", "bm90L3NhZmU", "AA"]) {
		assert.equal(decodeRunQueryCursor(malformed), undefined, malformed);
		assert.equal(isValidRunQueryRequest(RUN_QUERY_CHANNELS.list, { ...addressed, cursor: malformed }), false, malformed);
	}
	assert.throws(() => encodeRunQueryCursor("bad/id"), TypeError);
});

test("validates strict and legacy records as distinct additive wire shapes", () => {
	assert.equal(Check(WorkerRunHandleV1Schema, { ...strict, future: true }), true);
	assert.equal(Check(LegacyWorkerRunProjectionV1Schema, { ...legacy, future: true }), true);
	assert.equal(Check(WorkerRunRecordV1Schema, strict), true);
	assert.equal(Check(WorkerRunRecordV1Schema, legacy), true);
	assert.equal(Check(WorkerRunHandleV1Schema, legacy), false);
	assert.equal(Check(LegacyWorkerRunProjectionV1Schema, strict), false);
	assert.equal(Check(LegacyWorkerRunProjectionV1Schema, { ...legacy, lifecycle: { ...legacy.lifecycle, acceptedSequence: 0 } }), false);
});

test("rejects malformed nested lifecycle and endpoint fields", () => {
	const malformed = [
		{ ...strict, protocol: 2 },
		{ ...strict, lifecycle: { status: "running", acceptedSequence: 1 } },
		{ ...strict, lifecycle: { status: "started", acceptedSequence: -1 } },
		{ ...strict, lifecycle: { status: "started", acceptedSequence: 1, readiness: "ready" } },
		{ ...strict, assignment: { cwd: "" } },
		{ ...strict, endpoint: { ...strict.endpoint, agentName: "" } },
		{ ...strict, endpoint: { ...strict.endpoint, observedAt: -1 } },
		{ ...legacy, worker: {} },
	];
	for (const value of malformed) assert.equal(isValidRunQueryRecord(value), false);
});

test("enforces UTF-8 byte budgets in projected text fields", () => {
	const atLimit = "é".repeat(RUN_QUERY_LIMITS.role / 2);
	const overLimit = `${atLimit}é`;
	assert.equal(isValidRunQueryRecord({ ...strict, assignment: { ...strict.assignment, role: atLimit } }), true);
	assert.equal(isValidRunQueryRecord({ ...strict, assignment: { ...strict.assignment, role: overLimit } }), false);
	assert.equal(isValidRunQueryRecord({ ...legacy, worker: { agentName: "é".repeat(17) } }), false);
});

test("builds validated reply channels and fixed error envelopes", () => {
	assert.equal(runQueryReplyChannel(RUN_QUERY_CHANNELS.get, "request_1"), "herdr-workers:runs:rpc:get:reply:request_1");
	assert.throws(() => runQueryReplyChannel(RUN_QUERY_CHANNELS.get, "bad/id"), TypeError);
	assert.deepEqual(runQuerySuccess("request-1", strict), { requestId: "request-1", protocol: 1, success: true, data: strict });
	assert.deepEqual(runQueryFailure("request-1", "NOT_FOUND", "Missing."), {
		requestId: "request-1", protocol: 1, success: false, error: { code: "NOT_FOUND", message: "Missing." },
	});
});

test("leaves worker RPC v1 channels, capabilities, and spawn result unchanged", () => {
	assert.deepEqual(CHANNELS, {
		probe: "herdr-workers:rpc:probe",
		spawn: "herdr-workers:rpc:spawn",
		send: "herdr-workers:rpc:send",
		inspect: "herdr-workers:rpc:inspect",
		stop: "herdr-workers:rpc:stop",
	});
	assert.deepEqual(CAPABILITIES, ["spawn", "send", "steer", "inspect"]);
	assert.equal(Check(WorkerReferenceSchema, {
		runId: "run-1",
		correlationId: "dispatch-1",
		name: "agent-scout",
		paneId: "pane-1",
		cwd: "/workspace",
		model: "test/model",
		type: "research",
		purpose: "Map the system",
		adopted: false,
	}), true);
	assert.equal(Check(WorkerReferenceSchema, { runId: "run-1", name: "agent-scout", paneId: "pane-1", cwd: "/workspace" }), false);
});
