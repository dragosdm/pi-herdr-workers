import assert from "node:assert/strict";
import test from "node:test";
import {
	ReconciliationAbortError,
	ReconciliationProtocolError,
	ReconciliationResponseError,
	ReconciliationTimeoutError,
	createReconciliationClient,
} from "../../reconciliation/client.js";
import {
	RECONCILIATION_CHANNELS,
	reconciliationFailure,
	reconciliationReplyChannel,
	reconciliationSuccess,
} from "../../reconciliation/protocol.js";
import { FakeEventBus } from "../support/fake-event-bus.js";

const provider = {
	protocol: 1 as const,
	provider: "herdr-reconciliation" as const,
	providerInstanceId: "provider-1",
	sessionId: "session-1",
	available: true as const,
	constraints: { sessionScoped: true as const, requiresExactAcceptedSequence: true as const },
};
const input = {
	runId: "run-1",
	expectedAcceptedSequence: 1,
	resolution: { status: "started" as const, detail: "Still active", observations: [{ source: "journal" as const, detail: "Uncertain event is durable", observedAt: 1 }] },
};
const event = {
	protocol: 2 as const,
	eventId: "event-2",
	runId: "run-1",
	sourceInstanceId: "provider-1",
	worker: { name: "agent-one", paneId: "pane-1" },
	observedAt: 2,
	source: "reconciler" as const,
	status: "started" as const,
	evidence: { kind: "reconciled_started_v2" as const, detail: "Still active", observations: input.resolution.observations },
	acceptedSequence: 2,
};

test("probe installs its listener first and selects an available provider", async () => {
	const events = new FakeEventBus();
	events.on(RECONCILIATION_CHANNELS.probe, (payload) => {
		const request = payload as { requestId: string; supportedProtocols: number[] };
		assert.deepEqual(request.supportedProtocols, [1]);
		events.emit(reconciliationReplyChannel(RECONCILIATION_CHANNELS.probe, request.requestId), reconciliationSuccess(request.requestId, provider));
	});
	const result = await createReconciliationClient({ events, createRequestId: () => "probe-1" }).probe();
	assert.equal(result.providerInstanceId, "provider-1");
	assert.equal(events.listenerCount(reconciliationReplyChannel(RECONCILIATION_CHANNELS.probe, "probe-1")), 0);
});

test("reconcile addresses one provider and validates the canonical event", async () => {
	const events = new FakeEventBus();
	events.on(RECONCILIATION_CHANNELS.reconcile, (payload) => {
		const request = payload as Record<string, unknown> & { requestId: string };
		assert.equal(request.providerInstanceId, "provider-1");
		assert.equal(request.expectedAcceptedSequence, 1);
		events.emit(reconciliationReplyChannel(RECONCILIATION_CHANNELS.reconcile, request.requestId), reconciliationSuccess(request.requestId, event));
	});
	const result = await createReconciliationClient({ events, createRequestId: () => "reconcile-1" }).reconcile(input, provider);
	assert.deepEqual(result, event);
	assert.equal(events.listenerCount(reconciliationReplyChannel(RECONCILIATION_CHANNELS.reconcile, "reconcile-1")), 0);
});

test("typed rejection, malformed success, timeout, and abort clean up listeners", async () => {
	const rejectedBus = new FakeEventBus();
	rejectedBus.on(RECONCILIATION_CHANNELS.reconcile, (payload) => {
		const request = payload as { requestId: string };
		rejectedBus.emit(reconciliationReplyChannel(RECONCILIATION_CHANNELS.reconcile, request.requestId), reconciliationFailure(request.requestId, "STALE_ACCEPTED_SEQUENCE", "Worker run changed after inspection."));
	});
	await assert.rejects(createReconciliationClient({ events: rejectedBus, createRequestId: () => "rejected" }).reconcile(input, provider), (error: unknown) => {
		assert.ok(error instanceof ReconciliationResponseError);
		assert.equal(error.code, "STALE_ACCEPTED_SEQUENCE");
		return true;
	});

	const malformedBus = new FakeEventBus();
	malformedBus.on(RECONCILIATION_CHANNELS.reconcile, (payload) => {
		const request = payload as { requestId: string };
		malformedBus.emit(reconciliationReplyChannel(RECONCILIATION_CHANNELS.reconcile, request.requestId), reconciliationSuccess(request.requestId, { private: "data" }));
	});
	await assert.rejects(createReconciliationClient({ events: malformedBus, createRequestId: () => "malformed" }).reconcile(input, provider), ReconciliationProtocolError);

	const timeoutBus = new FakeEventBus();
	await assert.rejects(createReconciliationClient({ events: timeoutBus, createRequestId: () => "timeout" }).reconcile(input, provider, { timeoutMs: 1 }), ReconciliationTimeoutError);
	assert.equal(timeoutBus.listenerCount(), 0);

	const abortBus = new FakeEventBus();
	const controller = new AbortController();
	const waiting = createReconciliationClient({ events: abortBus, createRequestId: () => "abort" }).reconcile(input, provider, { signal: controller.signal });
	controller.abort();
	await assert.rejects(waiting, ReconciliationAbortError);
	assert.equal(abortBus.listenerCount(), 0);
});
