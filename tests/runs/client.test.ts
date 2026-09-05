import assert from "node:assert/strict";
import test from "node:test";
import {
	RunQueryAbortError,
	RunQueryProtocolError,
	RunQueryResponseError,
	RunQueryTimeoutError,
	createRunQueryClient,
} from "../../runs/client.js";
import {
	RUN_QUERY_CHANNELS,
	runQueryFailure,
	runQueryReplyChannel,
	runQuerySuccess,
} from "../../runs/protocol.js";
import { FakeEventBus } from "../support/fake-event-bus.js";

const provider = {
	protocol: 1 as const,
	provider: "herdr-runs" as const,
	providerInstanceId: "query-provider",
	sessionId: "session-1",
	available: true as const,
	constraints: { sessionScoped: true as const, requiresHerdrPane: false as const, requiresInteractivePi: false as const },
};
const record = {
	protocol: 1 as const,
	runId: "run-1",
	sessionId: "session-1",
	registeredAt: 1,
	lifecycle: { status: "registered" as const, acceptedSequence: 0 },
	assignment: { cwd: "/tmp" },
};

test("probe installs its listener before emit and selects an available provider", async () => {
	const events = new FakeEventBus();
	events.on(RUN_QUERY_CHANNELS.probe, (payload) => {
		const request = payload as { requestId: string };
		events.emit(runQueryReplyChannel(RUN_QUERY_CHANNELS.probe, request.requestId), runQuerySuccess(request.requestId, provider));
	});
	const selected = await createRunQueryClient({ events, createRequestId: () => "probe-1" }).probe();
	assert.equal(selected.providerInstanceId, "query-provider");
	assert.equal(events.listenerCount(runQueryReplyChannel(RUN_QUERY_CHANNELS.probe, "probe-1")), 0);
});

test("probe aggregates unavailable, unsupported, and malformed responders", async () => {
	const unavailable = { ...provider, available: false as const, reason: "SESSION_NOT_READY" as const };
	const events = new FakeEventBus();
	events.on(RUN_QUERY_CHANNELS.probe, (payload) => {
		const request = payload as { requestId: string };
		const channel = runQueryReplyChannel(RUN_QUERY_CHANNELS.probe, request.requestId);
		events.emit(channel, runQuerySuccess(request.requestId, {}));
		events.emit(channel, runQueryFailure(request.requestId, "UNSUPPORTED_PROTOCOL", "No shared protocol."));
		events.emit(channel, runQuerySuccess(request.requestId, unavailable));
	});
	const selected = await createRunQueryClient({ events, createRequestId: () => "probe-fallback" }).probe({ timeoutMs: 1 });
	assert.equal(selected.available, false);

	const unsupportedEvents = new FakeEventBus();
	unsupportedEvents.on(RUN_QUERY_CHANNELS.probe, (payload) => {
		const request = payload as { requestId: string };
		unsupportedEvents.emit(runQueryReplyChannel(RUN_QUERY_CHANNELS.probe, request.requestId), runQueryFailure(request.requestId, "UNSUPPORTED_PROTOCOL", "No shared protocol."));
	});
	await assert.rejects(
		createRunQueryClient({ events: unsupportedEvents, createRequestId: () => "unsupported" }).probe({ timeoutMs: 1 }),
		(error: unknown) => error instanceof RunQueryResponseError && error.code === "UNSUPPORTED_PROTOCOL",
	);

	const malformedEvents = new FakeEventBus();
	malformedEvents.on(RUN_QUERY_CHANNELS.probe, (payload) => {
		const request = payload as { requestId: string };
		malformedEvents.emit(runQueryReplyChannel(RUN_QUERY_CHANNELS.probe, request.requestId), runQuerySuccess(request.requestId, { secret: true }));
	});
	await assert.rejects(
		createRunQueryClient({ events: malformedEvents, createRequestId: () => "malformed" }).probe({ timeoutMs: 1 }),
		RunQueryProtocolError,
	);
});

test("concurrent get calls correlate replies and validate successful records", async () => {
	const events = new FakeEventBus();
	const ids = ["one", "two"];
	const client = createRunQueryClient({ events, createRequestId: () => ids.shift()! });
	const first = client.get({ runId: "run-a" }, provider);
	const second = client.get({ runId: "run-b", includeEndpointObservation: true }, provider);
	events.emit(runQueryReplyChannel(RUN_QUERY_CHANNELS.get, "two"), runQuerySuccess("two", { ...record, runId: "run-b" }));
	events.emit(runQueryReplyChannel(RUN_QUERY_CHANNELS.get, "two"), runQuerySuccess("two", { ...record, runId: "wrong" }));
	events.emit(runQueryReplyChannel(RUN_QUERY_CHANNELS.get, "one"), runQuerySuccess("one", { ...record, runId: "run-a" }));
	assert.deepEqual((await Promise.all([first, second])).map((item) => item.runId), ["run-a", "run-b"]);
	assert.equal(events.listenerCount(), 0);
});

test("get and list forward addressed inputs and validate server results", async () => {
	const events = new FakeEventBus();
	const requests: unknown[] = [];
	for (const channel of [RUN_QUERY_CHANNELS.get, RUN_QUERY_CHANNELS.list]) {
		events.on(channel, (payload) => {
			requests.push(payload);
			const request = payload as { requestId: string };
			const data = channel === RUN_QUERY_CHANNELS.get ? record : { runs: [record] };
			events.emit(runQueryReplyChannel(channel, request.requestId), runQuerySuccess(request.requestId, data));
		});
	}
	const ids = ["get-1", "list-1"];
	const client = createRunQueryClient({ events, createRequestId: () => ids.shift()! });
	await client.get({ runId: "run-1", includeEndpointObservation: false }, provider);
	await client.list({ limit: 10 }, provider);
	assert.deepEqual(requests, [
		{ runId: "run-1", includeEndpointObservation: false, requestId: "get-1", providerInstanceId: "query-provider", protocol: 1 },
		{ limit: 10, requestId: "list-1", providerInstanceId: "query-provider", protocol: 1 },
	]);

	const malformed = new FakeEventBus();
	malformed.on(RUN_QUERY_CHANNELS.get, (payload) => {
		const request = payload as { requestId: string };
		malformed.emit(runQueryReplyChannel(RUN_QUERY_CHANNELS.get, request.requestId), runQuerySuccess(request.requestId, { ...record, lifecycle: {} }));
	});
	await assert.rejects(createRunQueryClient({ events: malformed, createRequestId: () => "bad-result" }).get({ runId: "run-1" }, provider), (error: unknown) => {
		assert.ok(error instanceof RunQueryProtocolError);
		assert.doesNotMatch(error.message, /lifecycle|acceptedSequence/);
		return true;
	});
});

test("failure, timeout, abort, and synchronous emit errors clean up listeners", async () => {
	const failureEvents = new FakeEventBus();
	failureEvents.on(RUN_QUERY_CHANNELS.get, (payload) => {
		const request = payload as { requestId: string };
		failureEvents.emit(runQueryReplyChannel(RUN_QUERY_CHANNELS.get, request.requestId), runQueryFailure(request.requestId, "NOT_FOUND", "Missing."));
	});
	await assert.rejects(createRunQueryClient({ events: failureEvents, createRequestId: () => "missing" }).get({ runId: "run-1" }, provider), RunQueryResponseError);
	assert.equal(failureEvents.listenerCount(), 1);

	const timeoutEvents = new FakeEventBus();
	await assert.rejects(createRunQueryClient({ events: timeoutEvents, createRequestId: () => "timeout" }).list({}, provider, { timeoutMs: 1 }), RunQueryTimeoutError);
	assert.equal(timeoutEvents.listenerCount(), 0);

	const abortEvents = new FakeEventBus();
	const controller = new AbortController();
	const waiting = createRunQueryClient({ events: abortEvents, createRequestId: () => "abort" }).get({ runId: "run-1" }, provider, { signal: controller.signal });
	controller.abort();
	await assert.rejects(waiting, RunQueryAbortError);
	assert.equal(abortEvents.listenerCount(), 0);

	let replyListener: ((payload: unknown) => void) | undefined;
	const throwEvents = {
		on: (_channel: string, listener: (payload: unknown) => void) => { replyListener = listener; return () => { replyListener = undefined; }; },
		emit: () => { throw new Error("emit failed"); },
	};
	await assert.rejects(createRunQueryClient({ events: throwEvents, createRequestId: () => "emit" }).get({ runId: "run-1" }, provider), /emit failed/);
	assert.equal(replyListener, undefined);
});

test("timeout overrides are bounded", async () => {
	await assert.rejects(
		createRunQueryClient({ events: new FakeEventBus() }).list({}, provider, { timeoutMs: 300_001 }),
		RangeError,
	);
});
