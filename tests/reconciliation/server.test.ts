import assert from "node:assert/strict";
import test from "node:test";
import {
	RECONCILIATION_CHANNELS,
	ReconciliationServiceError,
	reconciliationReplyChannel,
	type ReconciliationReply,
} from "../../reconciliation/protocol.js";
import { registerReconciliationServer, type ReconciliationService } from "../../reconciliation/server.js";
import { FakeEventBus } from "../support/fake-event-bus.js";

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
const flush = async () => { await new Promise((resolve) => setImmediate(resolve)); };

test("probe negotiates one process-local provider generation", async () => {
	const events = new FakeEventBus();
	registerReconciliationServer({ events, service: { reconcile: async () => event }, sessionId: "session-1", getProviderState: () => ({ available: true }), createInstanceId: () => "provider-1" });
	let reply: ReconciliationReply | undefined;
	events.on(reconciliationReplyChannel(RECONCILIATION_CHANNELS.probe, "probe"), (payload) => { reply = payload as ReconciliationReply; });
	events.emit(RECONCILIATION_CHANNELS.probe, { requestId: "probe", supportedProtocols: [2, 1] });
	await flush();
	assert.equal(reply?.success, true);
	const data = reply?.success ? reply.data as Record<string, unknown> : undefined;
	assert.equal(data?.providerInstanceId, "provider-1");
	assert.equal(data?.provider, "herdr-reconciliation");
	assert.equal(data?.protocol, 1);
});

test("validates and routes before service dispatch", async () => {
	const events = new FakeEventBus();
	let calls = 0;
	registerReconciliationServer({ events, service: { reconcile: async () => { calls++; return event; } }, sessionId: "session-1", getProviderState: () => ({ available: false }), createInstanceId: () => "provider-1" });
	const cases = [
		{ id: "invalid", payload: { requestId: "invalid", providerInstanceId: "provider-1", protocol: 1, ...input, resolution: { ...input.resolution, observations: [] } } },
		{ id: "protocol", payload: { requestId: "protocol", providerInstanceId: "provider-1", protocol: 2, ...input } },
		{ id: "unavailable", payload: { requestId: "unavailable", providerInstanceId: "provider-1", protocol: 1, ...input } },
	] as const;
	const replies: ReconciliationReply[] = [];
	for (const item of cases) events.on(reconciliationReplyChannel(RECONCILIATION_CHANNELS.reconcile, item.id), (payload) => replies.push(payload as ReconciliationReply));
	for (const item of cases) events.emit(RECONCILIATION_CHANNELS.reconcile, item.payload);
	events.emit(RECONCILIATION_CHANNELS.reconcile, { requestId: "stale-provider", providerInstanceId: "other", protocol: 1, ...input });
	await flush();
	assert.deepEqual(replies.map((reply) => reply.success ? "success" : reply.error.code), ["INVALID_REQUEST", "UNSUPPORTED_PROTOCOL", "PROVIDER_UNAVAILABLE"]);
	assert.equal(calls, 0);
});

test("supplies trusted authority, strips transport fields, and maps domain errors", async () => {
	const events = new FakeEventBus();
	let received: unknown;
	let authority: unknown;
	const service: ReconciliationService = {
		async reconcile(value, trusted) {
			received = value;
			authority = trusted;
			throw new ReconciliationServiceError("STALE_ACCEPTED_SEQUENCE", "Worker run changed after inspection.");
		},
	};
	registerReconciliationServer({ events, service, sessionId: "session-1", getProviderState: () => ({ available: true }), createInstanceId: () => "provider-1" });
	let reply: ReconciliationReply | undefined;
	events.on(reconciliationReplyChannel(RECONCILIATION_CHANNELS.reconcile, "request-1"), (payload) => { reply = payload as ReconciliationReply; });
	events.emit(RECONCILIATION_CHANNELS.reconcile, { requestId: "request-1", providerInstanceId: "provider-1", protocol: 1, ...input, future: "ignored" });
	await flush();
	assert.deepEqual(received, input);
	assert.deepEqual(authority, { sourceInstanceId: "provider-1" });
	assert.equal(reply?.success ? undefined : reply?.error.code, "STALE_ACCEPTED_SEQUENCE");
});

test("returns accepted canonical events, sanitizes failures, and disposes idempotently", async () => {
	const events = new FakeEventBus();
	const server = registerReconciliationServer({ events, service: { reconcile: async () => event }, sessionId: "session-1", getProviderState: () => ({ available: true }), createInstanceId: () => "provider-1" });
	let reply: ReconciliationReply | undefined;
	events.on(reconciliationReplyChannel(RECONCILIATION_CHANNELS.reconcile, "ok"), (payload) => { reply = payload as ReconciliationReply; });
	events.emit(RECONCILIATION_CHANNELS.reconcile, { requestId: "ok", providerInstanceId: "provider-1", protocol: 1, ...input });
	await flush();
	assert.deepEqual(reply?.success && reply.data, event);
	assert.equal(events.listenerCount(), 3);
	server.dispose();
	server.dispose();
	assert.equal(events.listenerCount(), 1);

	const failing = new FakeEventBus();
	registerReconciliationServer({ events: failing, service: { reconcile: async () => { throw new Error("private stack"); } }, sessionId: "session-1", getProviderState: () => ({ available: true }), createInstanceId: () => "provider-1" });
	let failure: ReconciliationReply | undefined;
	failing.on(reconciliationReplyChannel(RECONCILIATION_CHANNELS.reconcile, "fail"), (payload) => { failure = payload as ReconciliationReply; });
	failing.emit(RECONCILIATION_CHANNELS.reconcile, { requestId: "fail", providerInstanceId: "provider-1", protocol: 1, ...input });
	await flush();
	assert.deepEqual(failure, { requestId: "fail", protocol: 1, success: false, error: { code: "INTERNAL_ERROR", message: "The reconciliation operation failed." } });
});
