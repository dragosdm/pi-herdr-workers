import assert from "node:assert/strict";
import test from "node:test";
import { RUN_QUERY_CHANNELS, RunQueryServiceError, encodeRunQueryCursor, runQueryReplyChannel, type RunQueryReply } from "../../runs/protocol.js";
import { registerRunQueryServer, type RunQueryService } from "../../runs/server.js";
import { FakeEventBus } from "../support/fake-event-bus.js";

const record = {
	protocol: 1 as const,
	runId: "run-1",
	sessionId: "session-1",
	registeredAt: 1,
	lifecycle: { status: "registered" as const, acceptedSequence: 0 },
	assignment: { cwd: "/tmp" },
};

function service(overrides: Partial<RunQueryService> = {}): RunQueryService {
	return {
		get: async () => structuredClone(record),
		list: async () => ({ runs: [structuredClone(record)] }),
		...overrides,
	};
}

async function flush(): Promise<void> { await new Promise((resolve) => setImmediate(resolve)); }

test("probe negotiates a query-specific available provider", async () => {
	const events = new FakeEventBus();
	registerRunQueryServer({ events, service: service(), sessionId: "session-1", getProviderState: () => ({ available: true }), createInstanceId: () => "query-instance" });
	let reply: RunQueryReply | undefined;
	events.on(runQueryReplyChannel(RUN_QUERY_CHANNELS.probe, "probe"), (payload) => { reply = payload as RunQueryReply; });
	events.emit(RUN_QUERY_CHANNELS.probe, { requestId: "probe", supportedProtocols: [2, 1], future: true });
	await flush();
	assert.deepEqual(reply?.success && reply.data, {
		protocol: 1,
		provider: "herdr-runs",
		providerInstanceId: "query-instance",
		sessionId: "session-1",
		available: true,
		constraints: { sessionScoped: true, requiresHerdrPane: false, requiresInteractivePi: false },
	});
});

test("validation, stale addressing, protocol, and availability gates precede service", async () => {
	const events = new FakeEventBus();
	let calls = 0;
	registerRunQueryServer({
		events,
		service: service({ get: async () => { calls++; return record; } }),
		sessionId: "session-1",
		getProviderState: () => ({ available: false }),
		createInstanceId: () => "query-instance",
	});
	const replies: RunQueryReply[] = [];
	for (const id of ["invalid", "protocol", "unavailable"]) {
		events.on(runQueryReplyChannel(RUN_QUERY_CHANNELS.get, id), (payload) => replies.push(payload as RunQueryReply));
	}
	events.emit(RUN_QUERY_CHANNELS.get, { requestId: "invalid", providerInstanceId: "query-instance", protocol: 1, runId: "bad/id" });
	events.emit(RUN_QUERY_CHANNELS.get, { requestId: "stale", providerInstanceId: "old-instance", protocol: 1, runId: "run-1" });
	events.emit(RUN_QUERY_CHANNELS.get, { requestId: "protocol", providerInstanceId: "query-instance", protocol: 2, runId: "run-1" });
	events.emit(RUN_QUERY_CHANNELS.get, { requestId: "unavailable", providerInstanceId: "query-instance", protocol: 1, runId: "run-1" });
	await flush();
	assert.deepEqual(replies.map((reply) => reply.success ? "success" : reply.error.code), ["INVALID_REQUEST", "UNSUPPORTED_PROTOCOL", "PROVIDER_UNAVAILABLE"]);
	assert.equal(calls, 0);
});

test("projects known service inputs, validates outputs, and sanitizes failures", async () => {
	const events = new FakeEventBus();
	const inputs: unknown[] = [];
	let getCalls = 0;
	registerRunQueryServer({
		events,
		service: service({
			get: async (input) => {
				inputs.push(input);
				getCalls++;
				if (getCalls === 1) throw new RunQueryServiceError("NOT_FOUND", "Worker run was not found.");
				if (getCalls === 2) return { ...record, assignment: { cwd: "" } };
				throw new Error("secret query state");
			},
			list: async (input) => { inputs.push(input); return { runs: [record] }; },
		}),
		sessionId: "session-1",
		getProviderState: () => ({ available: true }),
		createInstanceId: () => "query-instance",
	});
	const requests = [
		{ channel: RUN_QUERY_CHANNELS.get, id: "missing", payload: { runId: "run-missing", includeEndpointObservation: true, future: "ignored" } },
		{ channel: RUN_QUERY_CHANNELS.get, id: "malformed", payload: { runId: "run-malformed" } },
		{ channel: RUN_QUERY_CHANNELS.get, id: "private", payload: { runId: "run-private" } },
		{ channel: RUN_QUERY_CHANNELS.list, id: "list", payload: { cursor: encodeRunQueryCursor("run-0"), limit: 2, future: "ignored" } },
	] as const;
	const replies: RunQueryReply[] = [];
	for (const request of requests) {
		events.on(runQueryReplyChannel(request.channel, request.id), (payload) => replies.push(payload as RunQueryReply));
		events.emit(request.channel, { requestId: request.id, providerInstanceId: "query-instance", protocol: 1, ...request.payload });
	}
	await flush();
	assert.deepEqual(inputs, [
		{ runId: "run-missing", includeEndpointObservation: true },
		{ runId: "run-malformed" },
		{ runId: "run-private" },
		{ cursor: encodeRunQueryCursor("run-0"), limit: 2 },
	]);
	assert.deepEqual(replies.map((reply) => reply.success ? "success" : reply.error.code), ["NOT_FOUND", "INTERNAL_ERROR", "INTERNAL_ERROR", "success"]);
	assert.doesNotMatch(JSON.stringify(replies), /secret|query state/);
});

test("dispose removes all query subscriptions idempotently", () => {
	const events = new FakeEventBus();
	const server = registerRunQueryServer({ events, service: service(), sessionId: "session-1", getProviderState: () => ({ available: true }) });
	assert.equal(events.listenerCount(), 3);
	server.dispose();
	server.dispose();
	assert.equal(events.listenerCount(), 0);
});
