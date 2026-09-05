import assert from "node:assert/strict";
import test from "node:test";
import {
	RUN_ENDPOINT_BINDING_ENTRY,
	RUN_REGISTRATION_ENTRY,
	createRunRegistry,
	type RunEndpointBindingV1,
	type RunRegistrationV1,
} from "../../runs/registry.js";

const registration: RunRegistrationV1 = {
	version: 1,
	runId: "run-1",
	sessionId: "session-1",
	registeredAt: 1_786_000_000_000,
	correlationId: "dispatch-1",
	requestId: "request-1",
	assignment: { cwd: "/workspace", model: "test/model", role: "research" },
};

const endpoint: RunEndpointBindingV1 = {
	version: 1,
	runId: registration.runId,
	sessionId: registration.sessionId,
	agentName: "agent-scout",
	paneId: "pane-1",
	observedAt: 1_786_000_000_100,
};

function setup(initialEntries: any[] = []) {
	const appended: Array<{ customType: string; data: RunRegistrationV1 | RunEndpointBindingV1 }> = [];
	const registry = createRunRegistry({
		sessionId: registration.sessionId,
		getEntries: () => initialEntries,
		appendEntry: (customType, data) => appended.push({ customType, data }),
	});
	return { registry, appended };
}

test("appends registrations and endpoint bindings before exposing them in memory", () => {
	const h = setup();
	const registered = h.registry.register(registration);
	assert.deepEqual(h.appended, [{ customType: RUN_REGISTRATION_ENTRY, data: registration }]);
	assert.deepEqual(h.registry.getRegistration(registration.runId), registration);
	assert.notEqual(registered, registration);

	const bound = h.registry.bindEndpoint(endpoint);
	assert.deepEqual(h.appended[1], { customType: RUN_ENDPOINT_BINDING_ENTRY, data: endpoint });
	assert.deepEqual(h.registry.getEndpoint(registration.runId), endpoint);
	assert.notEqual(bound, endpoint);

	const failed = createRunRegistry({
		sessionId: registration.sessionId,
		getEntries: () => [],
		appendEntry: () => { throw new Error("disk unavailable"); },
	});
	assert.throws(() => failed.register(registration), /disk unavailable/);
	assert.equal(failed.getRegistration(registration.runId), undefined);
});

test("treats identical writes as idempotent and rejects conflicting immutable facts", () => {
	const h = setup();
	h.registry.register(registration);
	h.registry.register(structuredClone(registration));
	assert.equal(h.appended.length, 1);
	assert.throws(() => h.registry.register({ ...registration, registeredAt: registration.registeredAt + 1 }), /Conflicting registration/);

	h.registry.bindEndpoint(endpoint);
	h.registry.bindEndpoint(structuredClone(endpoint));
	assert.equal(h.appended.length, 2);
	assert.throws(() => h.registry.bindEndpoint({ ...endpoint, paneId: "pane-2" }), /Conflicting endpoint binding/);
	assert.throws(() => h.registry.bindEndpoint({ ...endpoint, runId: "run-unregistered" }), /unregistered run/);
	assert.deepEqual(h.registry.getEndpoint(registration.runId), endpoint);
});

test("restores only current-session validated facts and preserves first valid writes", () => {
	const additiveRegistration = { ...registration, futureRegistrationFact: { retained: true } };
	const additiveEndpoint = { ...endpoint, futureEndpointFact: "retained" };
	const h = setup([
		{ type: "custom", customType: RUN_REGISTRATION_ENTRY, data: { ...registration, runId: "bad id" } },
		{ type: "custom", customType: RUN_REGISTRATION_ENTRY, data: { ...registration, sessionId: "other-session" } },
		{ type: "custom", customType: RUN_ENDPOINT_BINDING_ENTRY, data: endpoint },
		{ type: "custom", customType: RUN_REGISTRATION_ENTRY, data: additiveRegistration },
		{ type: "custom", customType: RUN_REGISTRATION_ENTRY, data: { ...registration, registeredAt: 5 } },
		{ type: "custom", customType: RUN_ENDPOINT_BINDING_ENTRY, data: additiveEndpoint },
		{ type: "custom", customType: RUN_ENDPOINT_BINDING_ENTRY, data: { ...endpoint, paneId: "pane-conflict" } },
		{ type: "custom", customType: RUN_ENDPOINT_BINDING_ENTRY, data: { ...endpoint, runId: "missing-run" } },
	]);

	assert.deepEqual(h.registry.getRegistration(registration.runId), additiveRegistration);
	assert.deepEqual(h.registry.getEndpoint(registration.runId), additiveEndpoint);
	assert.equal(h.appended.length, 0);
});

test("validates session fences and known nested fields", () => {
	const h = setup();
	assert.throws(() => h.registry.register({ ...registration, sessionId: "other-session" }), /Invalid run registration/);
	assert.throws(() => h.registry.register({ ...registration, assignment: { cwd: "" } }), /Invalid run registration/);
	h.registry.register(registration);
	assert.throws(() => h.registry.bindEndpoint({ ...endpoint, observedAt: -1 }), /Invalid run endpoint binding/);
});

test("returns defensive clones for registrations, endpoints, and lists", () => {
	const h = setup();
	const mutableRegistration = structuredClone(registration);
	const mutableEndpoint = structuredClone(endpoint);
	h.registry.register(mutableRegistration);
	h.registry.bindEndpoint(mutableEndpoint);
	mutableRegistration.assignment.cwd = "/changed-input";
	mutableEndpoint.paneId = "changed-input";

	const readRegistration = h.registry.getRegistration(registration.runId)!;
	const readEndpoint = h.registry.getEndpoint(registration.runId)!;
	const listed = h.registry.listRegistrations();
	readRegistration.assignment.cwd = "/changed-read";
	readEndpoint.paneId = "changed-read";
	listed[0].assignment.cwd = "/changed-list";

	assert.deepEqual(h.registry.getRegistration(registration.runId), registration);
	assert.deepEqual(h.registry.getEndpoint(registration.runId), endpoint);
});

test("enriches only an exactly matching endpoint observation without persisting it", () => {
	const h = setup();
	h.registry.register(registration);
	h.registry.bindEndpoint(endpoint);
	const appendCount = h.appended.length;
	const matching = h.registry.projectRun(registration.runId, undefined, {
		agentName: endpoint.agentName,
		paneId: endpoint.paneId,
		observedAt: endpoint.observedAt + 100,
		herdrStatus: "idle",
	});
	assert.deepEqual(matching && !("legacy" in matching) && matching.endpoint, {
		agentName: endpoint.agentName,
		paneId: endpoint.paneId,
		observedAt: endpoint.observedAt + 100,
		herdrStatus: "idle",
	});
	for (const observation of [
		{ agentName: "agent-renamed", paneId: endpoint.paneId, observedAt: endpoint.observedAt + 200, herdrStatus: "busy" },
		{ agentName: endpoint.agentName, paneId: "pane-moved", observedAt: endpoint.observedAt + 200, herdrStatus: "busy" },
	]) {
		const projected = h.registry.projectRun(registration.runId, undefined, observation);
		assert.deepEqual(projected && !("legacy" in projected) && projected.endpoint, {
			agentName: endpoint.agentName,
			paneId: endpoint.paneId,
			observedAt: endpoint.observedAt,
		});
	}
	assert.equal(h.appended.length, appendCount);
	assert.deepEqual(h.registry.getEndpoint(registration.runId), endpoint);
});

test("persists selected lifecycle contract and defaults old registrations to contract 1", () => {
	const old = setup();
	old.registry.register(registration);
	const oldRecord = old.registry.projectRun(registration.runId)!;
	assert.equal(oldRecord.protocol, 2);
	assert.equal(oldRecord.lifecycleProtocol, 1);
	assert.equal(oldRecord.lifecycle.orchestrationGradeCompletion, false);

	const selectedRegistration = { ...registration, runId: "run-v2", lifecycleProtocol: 2 as const };
	const selected = setup();
	selected.registry.register(selectedRegistration);
	assert.equal(selected.appended[0].data && "lifecycleProtocol" in selected.appended[0].data
		? selected.appended[0].data.lifecycleProtocol
		: undefined, 2);
	assert.equal(selected.registry.getRegistration("run-v2")?.lifecycleProtocol, 2);
	assert.equal(selected.registry.projectRun("run-v2")?.lifecycleProtocol, 2);
});

test("marks only matching contract 2 terminal evidence as orchestration-grade", () => {
	const h = setup();
	h.registry.register({ ...registration, lifecycleProtocol: 2 });
	const baseLifecycle = {
		runId: registration.runId,
		lifecycleProtocol: 2 as const,
		worker: { name: "agent-scout", paneId: "pane-1" },
		status: "completed" as const,
		acceptedSequence: 1,
		eventIds: new Set<string>(),
		sourceSequences: new Map<string, number>(),
	};
	assert.equal(h.registry.projectRun(registration.runId, {
		...baseLifecycle,
		terminalEvidence: { kind: "worker_completed_v2", result: "Done" },
	})?.lifecycle.orchestrationGradeCompletion, true);
	assert.equal(h.registry.projectRun(registration.runId, {
		...baseLifecycle,
		terminalEvidence: { kind: "worker_completed", result: "Legacy" },
	})?.lifecycle.orchestrationGradeCompletion, false);
});
