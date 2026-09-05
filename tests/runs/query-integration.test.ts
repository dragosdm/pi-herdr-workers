import assert from "node:assert/strict";
import test from "node:test";
import { createLifecycleAcceptor } from "../../lifecycle/acceptor.js";
import type { LifecycleCandidate } from "../../lifecycle/protocol.js";
import { LIFECYCLE_CHANNELS, type AcceptedLifecycleEvent } from "../../lifecycle/protocol.js";
import { createRunQueryClient } from "../../runs/client.js";
import { RUN_QUERY_LIMITS, RunQueryServiceError, decodeRunQueryCursor, encodeRunQueryCursor } from "../../runs/protocol.js";
import { createRunRegistry } from "../../runs/registry.js";
import { registerRunQueryServer, type RunQueryService } from "../../runs/server.js";
import { FakeEventBus } from "../support/fake-event-bus.js";

function candidate(runId: string, name: string, status: "started" | "completed" = "started"): LifecycleCandidate {
	return status === "started"
		? {
			protocol: 1, eventId: `event-${runId}`, runId, sourceInstanceId: "provider-1", sourceSequence: 1,
			status, worker: { name, paneId: `pane-${runId}` }, observedAt: 10, source: "provider",
			evidence: { kind: "agent_start_returned", readiness: "unconfirmed" },
		}
		: {
			protocol: 1, eventId: `event-${runId}`, runId, sourceInstanceId: "worker-1", sourceSequence: 1,
			status, worker: { name, paneId: `pane-${runId}` }, observedAt: 10, source: "worker",
			evidence: { kind: "worker_completed", result: "Done" },
		};
}

test("queries deterministic mixed strict and legacy pages from durable authorities", async () => {
	const entries: any[] = [];
	const registry = createRunRegistry({ sessionId: "session-1", getEntries: () => entries, appendEntry: () => {} });
	for (const runId of ["run-b", "run-c"]) {
		registry.register({ version: 1, runId, sessionId: "session-1", registeredAt: 1, assignment: { cwd: "/tmp" } });
	}
	registry.bindEndpoint({ version: 1, runId: "run-b", sessionId: "session-1", agentName: "agent-b", paneId: "pane-run-b", observedAt: 2 });

	const acceptor = createLifecycleAcceptor({ sessionId: "session-1", getEntries: () => entries, appendEntry: () => {}, emit: () => {} });
	acceptor.bindRun({ runId: "run-a", worker: { name: "agent-a", paneId: "pane-run-a" } });
	acceptor.bindRun({ runId: "run-b", worker: { name: "agent-b", paneId: "pane-run-b" } });
	assert.equal(acceptor.accept(candidate("run-a", "agent-a", "completed")).accepted, true);
	assert.equal(acceptor.accept(candidate("run-b", "agent-b")).accepted, true);

	const service: RunQueryService = {
		async get(input) {
			const result = registry.projectRun(input.runId, acceptor.getRun(input.runId));
			if (!result) throw new RunQueryServiceError("NOT_FOUND", "Worker run was not found.");
			return result;
		},
		async list(input) {
			const records = registry.listRunRecords(acceptor.listRuns());
			const after = input.cursor === undefined ? undefined : decodeRunQueryCursor(input.cursor);
			const start = after === undefined ? 0 : records.findIndex((record) => record.runId > after);
			const normalizedStart = start < 0 ? records.length : start;
			const limit = input.limit ?? RUN_QUERY_LIMITS.defaultPageSize;
			const runs = records.slice(normalizedStart, normalizedStart + limit);
			return {
				runs,
				...(normalizedStart + runs.length < records.length ? { nextCursor: encodeRunQueryCursor(runs[runs.length - 1].runId) } : {}),
			};
		},
		async replay(input) {
			const replay = acceptor.replayRun(input.runId, input.afterAcceptedSequence, input.limit ?? RUN_QUERY_LIMITS.defaultPageSize);
			if (replay) return replay;
			if (registry.getRegistration(input.runId)) return { events: [], hasMore: false };
			throw new RunQueryServiceError("NOT_FOUND", "Worker run was not found.");
		},
	};
	const events = new FakeEventBus();
	registerRunQueryServer({ events, service, sessionId: "session-1", getProviderState: () => ({ available: true }), createInstanceId: () => "query-1" });
	const requestIds = ["probe", "page-1", "page-2", "get-b", "get-missing"];
	const client = createRunQueryClient({ events, createRequestId: () => requestIds.shift()! });
	const provider = await client.probe();
	const first = await client.list({ limit: 2 }, provider);
	const second = await client.list({ cursor: first.nextCursor, limit: 2 }, provider);
	assert.deepEqual(first.runs.map((record) => [record.runId, "legacy" in record]), [["run-a", true], ["run-b", false]]);
	assert.equal(first.nextCursor, encodeRunQueryCursor("run-b"));
	assert.deepEqual(second.runs.map((record) => record.runId), ["run-c"]);

	const strict = await client.get({ runId: "run-b" }, provider);
	assert.equal("legacy" in strict, false);
	assert.deepEqual(strict.lifecycle, { status: "started", acceptedSequence: 1, readiness: "unconfirmed" });
	assert.deepEqual("legacy" in strict ? undefined : strict.endpoint, { agentName: "agent-b", paneId: "pane-run-b", observedAt: 2 });
	await assert.rejects(client.get({ runId: "run-missing" }, provider), (error: any) => error.code === "NOT_FOUND");
});

test("registration-only and lifecycle projections are defensively cloned", () => {
	const registry = createRunRegistry({ sessionId: "session-1", getEntries: () => [], appendEntry: () => {} });
	registry.register({ version: 1, runId: "run-only", sessionId: "session-1", registeredAt: 1, assignment: { cwd: "/tmp" } });
	const acceptor = createLifecycleAcceptor({ sessionId: "session-1", getEntries: () => [], appendEntry: () => {}, emit: () => {} });
	acceptor.bindRun({ runId: "run-legacy", worker: { name: "agent-old" } });
	assert.equal(acceptor.accept(candidate("run-legacy", "agent-old")).accepted, false);
	const registered = registry.projectRun("run-only")!;
	assert.deepEqual(registered.lifecycle, { status: "registered", acceptedSequence: 0 });
	if (!("legacy" in registered)) registered.assignment.cwd = "/changed";
	assert.equal((registry.projectRun("run-only") as any).assignment.cwd, "/tmp");

	const listed = acceptor.listRuns();
	listed[0].worker.name = "changed";
	listed[0].eventIds.add("mutated");
	assert.equal(acceptor.getRun("run-legacy")?.worker.name, "agent-old");
	assert.equal(acceptor.getRun("run-legacy")?.eventIds.has("mutated"), false);
});

test("subscribe, list, and replay closes the concurrent acceptance race after sequence deduplication", async () => {
	const entries: any[] = [];
	const events = new FakeEventBus();
	const registry = createRunRegistry({ sessionId: "session-1", getEntries: () => entries, appendEntry: () => {} });
	registry.register({ version: 1, runId: "run-race", sessionId: "session-1", registeredAt: 1, assignment: { cwd: "/tmp" } });
	const acceptor = createLifecycleAcceptor({
		sessionId: "session-1",
		getEntries: () => entries,
		appendEntry: () => {},
		emit: (channel, payload) => events.emit(channel, payload),
	});
	acceptor.bindRun({ runId: "run-race", worker: { name: "agent-race", paneId: "pane-run-race" } });
	assert.equal(acceptor.accept(candidate("run-race", "agent-race")).accepted, true);

	const received: AcceptedLifecycleEvent[] = [];
	events.on(LIFECYCLE_CHANNELS.lifecycle, (payload) => received.push(payload as AcceptedLifecycleEvent));
	let acceptedConcurrent = false;
	const service: RunQueryService = {
		async get(input) {
			const result = registry.projectRun(input.runId, acceptor.getRun(input.runId));
			if (!result) throw new RunQueryServiceError("NOT_FOUND", "Worker run was not found.");
			return result;
		},
		async list() {
			const snapshot = registry.listRunRecords(acceptor.listRuns());
			if (!acceptedConcurrent) {
				acceptedConcurrent = true;
				assert.equal(acceptor.accept({
					...candidate("run-race", "agent-race", "completed"),
					eventId: "event-race-completed",
				}).accepted, true);
			}
			return { runs: snapshot };
		},
		async replay(input) {
			const result = acceptor.replayRun(input.runId, input.afterAcceptedSequence, input.limit ?? RUN_QUERY_LIMITS.defaultPageSize);
			if (!result) throw new RunQueryServiceError("NOT_FOUND", "Worker run was not found.");
			return result;
		},
	};
	registerRunQueryServer({ events, service, sessionId: "session-1", getProviderState: () => ({ available: true }), createInstanceId: () => "query-race" });
	const requestIds = ["probe-race", "list-race", "replay-race"];
	const client = createRunQueryClient({ events, createRequestId: () => requestIds.shift()! });
	const provider = await client.probe();
	const listed = await client.list({}, provider);
	assert.equal(listed.runs[0].lifecycle.acceptedSequence, 1);
	assert.deepEqual(received.map((event) => event.acceptedSequence), [2]);
	const replayed = await client.replay({ runId: "run-race", afterAcceptedSequence: listed.runs[0].lifecycle.acceptedSequence }, provider);
	assert.deepEqual(replayed.events.map((event) => event.acceptedSequence), [2]);
	const deduplicated = new Map([...received, ...replayed.events].map((event) => [`${event.runId}:${event.acceptedSequence}`, event]));
	assert.deepEqual([...deduplicated.values()].map((event) => event.acceptedSequence), [2]);
});
