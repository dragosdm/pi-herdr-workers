import assert from "node:assert/strict";
import test from "node:test";
import {
	LIFECYCLE_JOURNAL_ENTRY,
	LIFECYCLE_REJECTION_ENTRY,
	createLifecycleAcceptor,
	type LifecycleJournalEntry,
	type LifecycleRejectionEntry,
} from "../../lifecycle/acceptor.js";
import { LIFECYCLE_CHANNELS, type LifecycleCandidate } from "../../lifecycle/protocol.js";
import { FakeIsolatedEventBus } from "../support/fake-isolated-event-bus.js";

const binding = {
	runId: "run-1",
	lifecycleProtocol: 1 as const,
	correlationId: "dispatch-1",
	worker: { name: "agent-scout", paneId: "pane-1" },
	requestId: "request-1",
	providerInstanceId: "provider-1",
};

function candidate(overrides: Partial<LifecycleCandidate> = {}): LifecycleCandidate {
	return {
		protocol: 1,
		eventId: "event-1",
		runId: binding.runId,
		sourceInstanceId: "provider-1",
		sourceSequence: 1,
		status: "started",
		worker: { ...binding.worker },
		observedAt: 1_786_000_000_000,
		source: "provider",
		correlationId: binding.correlationId,
		evidence: { kind: "agent_start_returned", readiness: "unconfirmed" },
		...overrides,
	} as LifecycleCandidate;
}

function setup(initialEntries: any[] = []) {
	const timeline: string[] = [];
	const journal: LifecycleJournalEntry[] = [];
	const rejections: LifecycleRejectionEntry[] = [];
	const emissions: Array<{ channel: string; payload: any }> = [];
	const acceptor = createLifecycleAcceptor({
		sessionId: "session-1",
		getEntries: () => initialEntries,
		appendEntry: (customType, data) => {
			timeline.push(`append:${customType}`);
			if (customType === LIFECYCLE_JOURNAL_ENTRY && "event" in data) journal.push(data);
			if (customType === LIFECYCLE_REJECTION_ENTRY && "candidate" in data) rejections.push(data);
		},
		emit: (channel, payload) => {
			timeline.push(`emit:${channel}`);
			emissions.push({ channel, payload });
		},
	});
	return { acceptor, timeline, journal, rejections, emissions };
}

test("journals before canonical and projected publication with one accepted object", () => {
	const h = setup();
	assert.equal(h.acceptor.bindRun(binding), true);
	const result = h.acceptor.accept(candidate());
	assert.equal(result.accepted, true);
	if (!result.accepted) return;
	assert.deepEqual(h.timeline, [
		`append:${LIFECYCLE_JOURNAL_ENTRY}`,
		`emit:${LIFECYCLE_CHANNELS.lifecycle}`,
		`emit:${LIFECYCLE_CHANNELS.started}`,
	]);
	assert.equal(h.journal[0].event, result.event);
	assert.equal(h.emissions[0].payload, result.event);
	assert.equal(h.emissions[1].payload, result.event);
	assert.equal(result.event.acceptedSequence, 1);
	assert.deepEqual(result.record.status, "started");
	assert.deepEqual(result.record.readiness, "unconfirmed");
});

test("rejects malformed, unbound, mismatched, duplicate, and stale observations", () => {
	const h = setup();
	assert.deepEqual(h.acceptor.accept({}), { accepted: false, reason: "invalid_candidate" });
	assert.equal(h.acceptor.accept(candidate()).accepted, false);
	assert.equal(h.acceptor.bindRun(binding), true);
	assert.equal(h.acceptor.accept(candidate({ worker: { name: "agent-other", paneId: "pane-1" } })).accepted, false);
	assert.equal(h.acceptor.accept(candidate({ sourceInstanceId: "provider-other" })).accepted, false);
	assert.equal(h.acceptor.accept(candidate()).accepted, true);
	assert.equal(h.acceptor.accept(candidate()).accepted, false);
	const stale = h.acceptor.accept(candidate({ eventId: "event-2", sourceSequence: 1 }));
	assert.deepEqual(stale.accepted ? undefined : stale.reason, "stale_source");
	assert.equal(h.journal.length, 1);
});

test("folds readiness, messages, uncertainty, and terminal precedence", () => {
	const h = setup();
	h.acceptor.bindRun(binding);
	assert.equal(h.acceptor.accept(candidate()).accepted, true);
	assert.equal(h.acceptor.accept(candidate({
		eventId: "event-ready",
		sourceInstanceId: "worker-1",
		sourceSequence: 1,
		source: "worker",
		status: "started",
		evidence: { kind: "worker_ready", readiness: "confirmed" },
	})).accepted, true);
	assert.equal(h.acceptor.getRun(binding.runId)?.readiness, "confirmed");
	assert.equal(h.acceptor.accept(candidate({
		eventId: "event-message",
		sourceInstanceId: "worker-1",
		sourceSequence: 2,
		source: "worker",
		status: "message",
		evidence: { kind: "worker_message", message: "Still working" },
	})).accepted, true);
	assert.equal(h.acceptor.getRun(binding.runId)?.status, "started");
	assert.equal(h.acceptor.accept(candidate({
		eventId: "event-uncertain",
		sourceSequence: 2,
		status: "uncertain",
		evidence: { kind: "uncertain", scope: "assignment_delivery", detail: "Prompt outcome is unknown" },
	})).accepted, true);
	assert.equal(h.acceptor.getRun(binding.runId)?.status, "uncertain");
	assert.equal(h.acceptor.accept(candidate({
		eventId: "event-completed",
		sourceInstanceId: "worker-1",
		sourceSequence: 3,
		source: "worker",
		status: "completed",
		evidence: { kind: "worker_completed", result: "Done" },
	})).accepted, true);
	assert.equal(h.acceptor.getRun(binding.runId)?.status, "completed");
	const repeated = h.acceptor.accept(candidate({
		eventId: "event-completed-again",
		sourceInstanceId: "worker-1",
		sourceSequence: 4,
		source: "worker",
		status: "completed",
		evidence: { kind: "worker_completed", result: "Done" },
	}));
	assert.deepEqual(repeated.accepted ? undefined : repeated.reason, "terminal_duplicate");
	const conflict = h.acceptor.accept(candidate({
		eventId: "event-failed",
		sourceInstanceId: "worker-1",
		sourceSequence: 4,
		source: "worker",
		status: "failed",
		evidence: { kind: "worker_failed", error: "Too late" },
	}));
	assert.deepEqual(conflict.accepted ? undefined : conflict.reason, "terminal_conflict");
	const regression = h.acceptor.accept(candidate({
		eventId: "event-late-uncertain",
		sourceSequence: 3,
		status: "uncertain",
		evidence: { kind: "uncertain", scope: "reconciliation", detail: "Late ambiguity" },
	}));
	assert.deepEqual(regression.accepted ? undefined : regression.reason, "invalid_transition");
	assert.equal(h.acceptor.getRun(binding.runId)?.acceptedSequence, 5);
});

test("restores all current-session journal entries without replay publication", () => {
	const first = setup();
	first.acceptor.bindRun(binding);
	const accepted = first.acceptor.accept(candidate());
	assert.equal(accepted.accepted, true);
	const entries = [
		{ type: "custom", customType: LIFECYCLE_JOURNAL_ENTRY, data: first.journal[0] },
		{ type: "custom", customType: LIFECYCLE_JOURNAL_ENTRY, data: { ...first.journal[0], sessionId: "other-session" } },
		{ type: "custom", customType: LIFECYCLE_JOURNAL_ENTRY, data: { version: 1, sessionId: "session-1", event: {} } },
	];
	const restored = setup(entries);
	assert.equal(restored.emissions.length, 0);
	assert.equal(restored.acceptor.getRun(binding.runId)?.acceptedSequence, 1);
	assert.equal(restored.acceptor.bindRun(binding), true);
	const next = restored.acceptor.accept(candidate({
		eventId: "event-2",
		sourceInstanceId: "worker-1",
		sourceSequence: 1,
		source: "worker",
		status: "message",
		evidence: { kind: "worker_message", message: "Restored" },
	}));
	assert.equal(next.accepted && next.event.acceptedSequence, 2);
});

test("enriches a pane-less ambiguous binding when stronger evidence identifies the worker", () => {
	const h = setup();
	assert.equal(h.acceptor.bindRun({ ...binding, worker: { name: binding.worker.name } }), true);
	assert.equal(h.acceptor.accept(candidate({
		eventId: "event-uncertain",
		worker: { name: binding.worker.name },
		status: "uncertain",
		evidence: { kind: "uncertain", scope: "pane_creation", detail: "Pane identity was not returned" },
	})).accepted, true);

	assert.equal(h.acceptor.bindRun(binding), true);
	const reconciled = h.acceptor.accept(candidate({
		eventId: "event-reconciled",
		sourceInstanceId: "reconciler-1",
		sourceSequence: 1,
		source: "reconciler",
		status: "started",
		evidence: { kind: "reconciled_started", detail: "The worker pane was found" },
	}));
	assert.equal(reconciled.accepted, true);
	assert.deepEqual(h.acceptor.getRun(binding.runId)?.worker, binding.worker);
	assert.equal(h.acceptor.getRun(binding.runId)?.status, "started");
});

test("accepts informational worker messages after settlement without changing terminal status", () => {
	const h = setup();
	h.acceptor.bindRun(binding);
	assert.equal(h.acceptor.accept(candidate({
		eventId: "event-completed",
		sourceInstanceId: "worker-1",
		sourceSequence: 1,
		source: "worker",
		status: "completed",
		evidence: { kind: "worker_completed", result: "Done" },
	})).accepted, true);
	const message = h.acceptor.accept(candidate({
		eventId: "event-final-note",
		sourceInstanceId: "worker-1",
		sourceSequence: 2,
		source: "worker",
		status: "message",
		evidence: { kind: "worker_message", message: "Artifacts are in the workspace" },
	}));
	assert.equal(message.accepted, true);
	assert.equal(h.acceptor.getRun(binding.runId)?.status, "completed");
	assert.equal(h.acceptor.getRun(binding.runId)?.acceptedSequence, 2);
});

test("records authenticated transition conflicts without mutating canonical state", () => {
	const h = setup();
	h.acceptor.bindRun(binding);
	assert.equal(h.acceptor.accept(candidate({
		eventId: "event-completed",
		sourceInstanceId: "worker-1",
		sourceSequence: 1,
		source: "worker",
		status: "completed",
		evidence: { kind: "worker_completed", result: "Done" },
	})).accepted, true);

	const duplicate = h.acceptor.accept(candidate({
		eventId: "event-completed-duplicate",
		sourceInstanceId: "worker-1",
		sourceSequence: 2,
		source: "worker",
		status: "completed",
		evidence: { kind: "worker_completed", result: "Done" },
	}));
	assert.deepEqual(duplicate.accepted ? undefined : duplicate.reason, "terminal_duplicate");
	assert.equal(h.rejections.length, 0);

	const conflict = h.acceptor.accept(candidate({
		eventId: "event-completed-conflict",
		sourceInstanceId: "worker-1",
		sourceSequence: 2,
		source: "worker",
		status: "completed",
		evidence: { kind: "worker_completed", result: "Different result" },
	}));
	assert.deepEqual(conflict.accepted ? undefined : conflict.reason, "terminal_conflict");
	assert.equal(h.rejections.length, 1);
	assert.equal(h.rejections[0].candidate.eventId, "event-completed-conflict");
	assert.equal(h.acceptor.getRun(binding.runId)?.acceptedSequence, 1);
	assert.deepEqual(h.acceptor.getRun(binding.runId)?.terminalEvidence, { kind: "worker_completed", result: "Done" });
});

test("enforces source precedence and journals reserved stop rejection", () => {
	const h = setup();
	h.acceptor.bindRun(binding);
	assert.equal(h.acceptor.accept(candidate({
		eventId: "event-ready",
		sourceInstanceId: "worker-1",
		sourceSequence: 1,
		source: "worker",
		status: "started",
		evidence: { kind: "worker_ready", readiness: "confirmed" },
	})).accepted, true);
	const weaker = h.acceptor.accept(candidate({
		eventId: "event-weaker",
		sourceSequence: 2,
		status: "uncertain",
		evidence: { kind: "uncertain", scope: "agent_start", detail: "Late provider ambiguity" },
	}));
	assert.deepEqual(weaker.accepted ? undefined : weaker.reason, "invalid_transition");
	assert.equal(h.acceptor.getRun(binding.runId)?.status, "started");

	const stopped = h.acceptor.accept(candidate({
		eventId: "event-stopped",
		sourceInstanceId: "controller-1",
		sourceSequence: 1,
		source: "controller",
		status: "stopped",
		evidence: { kind: "stop_acknowledged", stopRequestId: "stop-1" },
	}));
	assert.deepEqual(stopped.accepted ? undefined : stopped.reason, "unsupported_stopped");
	assert.deepEqual(h.rejections.map((entry) => entry.reason), ["invalid_transition", "unsupported_stopped"]);
	assert.equal(h.journal.length, 1);
});

test("restores terminal evidence and keeps duplicate replay idempotent", () => {
	const first = setup();
	first.acceptor.bindRun(binding);
	assert.equal(first.acceptor.accept(candidate({
		eventId: "event-failed",
		sourceInstanceId: "worker-1",
		sourceSequence: 1,
		source: "worker",
		status: "failed",
		evidence: { kind: "worker_failed", error: "Blocked" },
	})).accepted, true);
	const entry = { type: "custom", customType: LIFECYCLE_JOURNAL_ENTRY, data: first.journal[0] };
	const restored = setup([entry, structuredClone(entry)]);
	assert.equal(restored.acceptor.getRun(binding.runId)?.acceptedSequence, 1);
	assert.deepEqual(restored.acceptor.getRun(binding.runId)?.terminalEvidence, { kind: "worker_failed", error: "Blocked" });
	assert.equal(restored.emissions.length, 0);
	assert.equal(restored.acceptor.bindRun(binding), true);
	const duplicate = restored.acceptor.accept(candidate({
		eventId: "event-failed-again",
		sourceInstanceId: "worker-1",
		sourceSequence: 2,
		source: "worker",
		status: "failed",
		evidence: { kind: "worker_failed", error: "Blocked" },
	}));
	assert.deepEqual(duplicate.accepted ? undefined : duplicate.reason, "terminal_duplicate");
});

test("isolates subscriber failures and preserves later delivery", async () => {
	const bus = new FakeIsolatedEventBus();
	const delivered: string[] = [];
	bus.on(LIFECYCLE_CHANNELS.lifecycle, () => { throw new Error("sync listener failed"); });
	bus.on(LIFECYCLE_CHANNELS.lifecycle, () => delivered.push("canonical"));
	bus.on(LIFECYCLE_CHANNELS.started, async () => { throw new Error("async listener failed"); });
	bus.on(LIFECYCLE_CHANNELS.started, () => delivered.push("started"));
	const acceptor = createLifecycleAcceptor({
		sessionId: "session-1",
		getEntries: () => [],
		appendEntry() {},
		emit: (channel, payload) => bus.emit(channel, payload),
	});
	acceptor.bindRun(binding);
	assert.equal(acceptor.accept(candidate()).accepted, true);
	await new Promise((resolve) => setImmediate(resolve));
	assert.deepEqual(delivered, ["canonical", "started"]);
});

test("lists current records in run order as defensive clones", () => {
	const h = setup();
	h.acceptor.bindRun({ ...binding, runId: "run-z", worker: { name: "agent-z" } });
	h.acceptor.bindRun(binding);
	assert.equal(h.acceptor.accept(candidate()).accepted, true);
	const listed = h.acceptor.listRuns();
	assert.deepEqual(listed.map((record) => record.runId), ["run-1", "run-z"]);
	listed[0].worker.name = "changed";
	listed[0].eventIds.add("mutated");
	listed[0].sourceSequences.set("mutated", 99);
	assert.equal(h.acceptor.getRun("run-1")?.worker.name, "agent-scout");
	assert.equal(h.acceptor.getRun("run-1")?.eventIds.has("mutated"), false);
	assert.equal(h.acceptor.getRun("run-1")?.sourceSequences.has("mutated"), false);
});

test("replays cloned accepted history after an exclusive cursor with exact bounds", () => {
	const h = setup();
	h.acceptor.bindRun(binding);
	assert.equal(h.acceptor.accept(candidate()).accepted, true);
	assert.equal(h.acceptor.accept(candidate({
		eventId: "event-message", sourceInstanceId: "worker-1", sourceSequence: 1, source: "worker",
		status: "message", evidence: { kind: "worker_message", message: "Working" },
	})).accepted, true);
	assert.equal(h.acceptor.accept(candidate({
		eventId: "event-completed", sourceInstanceId: "worker-1", sourceSequence: 2, source: "worker",
		status: "completed", evidence: { kind: "worker_completed", result: "Done" },
	})).accepted, true);

	assert.deepEqual(h.acceptor.replayRun(binding.runId, 0, 2)?.events.map((event) => event.acceptedSequence), [1, 2]);
	assert.equal(h.acceptor.replayRun(binding.runId, 0, 2)?.hasMore, true);
	const terminal = h.acceptor.replayRun(binding.runId, 2, 2);
	assert.deepEqual(terminal?.events.map((event) => [event.eventId, event.sourceInstanceId, event.acceptedSequence]), [["event-completed", "worker-1", 3]]);
	assert.equal(terminal?.hasMore, false);
	assert.deepEqual(h.acceptor.replayRun(binding.runId, 3, 2), { events: [], hasMore: false });
	assert.equal(h.acceptor.replayRun("missing", 0, 1), undefined);
	terminal!.events[0].worker.name = "changed";
	assert.equal(h.acceptor.replayRun(binding.runId, 2, 1)?.events[0].worker.name, "agent-scout");
});

test("restored replay retains only contiguous current-session history without publication", () => {
	const first = setup();
	first.acceptor.bindRun(binding);
	assert.equal(first.acceptor.accept(candidate()).accepted, true);
	const event1 = first.journal[0].event;
	const event2 = { ...event1, eventId: "event-2", acceptedSequence: 2, sourceInstanceId: "worker-1", sourceSequence: 1,
		source: "worker" as const, status: "message" as const, evidence: { kind: "worker_message" as const, message: "Restored" } };
	const event4 = { ...event2, eventId: "event-4", acceptedSequence: 4, sourceSequence: 2 };
	const restored = setup([
		{ type: "custom", customType: LIFECYCLE_JOURNAL_ENTRY, data: { version: 1, sessionId: "session-1", event: event4 } },
		{ type: "custom", customType: LIFECYCLE_JOURNAL_ENTRY, data: { version: 1, sessionId: "other-session", event: event2 } },
		{ type: "custom", customType: LIFECYCLE_JOURNAL_ENTRY, data: { version: 1, sessionId: "session-1", event: event2 } },
		{ type: "custom", customType: LIFECYCLE_JOURNAL_ENTRY, data: { version: 1, sessionId: "session-1", event: event1 } },
	]);
	assert.deepEqual(restored.acceptor.replayRun(binding.runId, 0, 10)?.events.map((event) => event.acceptedSequence), [1, 2]);
	assert.equal(restored.emissions.length, 0);
});

test("binds lifecycle versions and restores contract 2 completion evidence exactly", () => {
	const h = setup();
	const v2Binding = { ...binding, runId: "run-v2", lifecycleProtocol: 2 as const };
	assert.equal(h.acceptor.bindRun(v2Binding), true);
	assert.equal(h.acceptor.accept(candidate({ runId: "run-v2", worker: v2Binding.worker })).accepted, false);
	assert.equal(h.acceptor.accept({
		...candidate({ runId: "run-v2", worker: v2Binding.worker }),
		protocol: 2,
		eventId: "event-v2-completed",
		sourceInstanceId: "worker-v2",
		source: "worker",
		status: "completed",
		evidence: {
			kind: "worker_completed_v2",
			result: "Implemented and verified",
			artifacts: [{ path: "reports/result.md", description: "Final report" }],
			checks: [{ kind: "test", command: "npm test", outcome: "passed" }],
		},
	}).accepted, true);
	assert.deepEqual(h.acceptor.getRun("run-v2")?.terminalEvidence, {
		kind: "worker_completed_v2",
		result: "Implemented and verified",
		artifacts: [{ path: "reports/result.md", description: "Final report" }],
		checks: [{ kind: "test", command: "npm test", outcome: "passed" }],
	});

	const restored = setup([{ type: "custom", customType: LIFECYCLE_JOURNAL_ENTRY, data: h.journal[0] }]);
	assert.equal(restored.acceptor.getRun("run-v2")?.lifecycleProtocol, 2);
	assert.deepEqual(restored.acceptor.replayRun("run-v2", 0, 10)?.events, [h.journal[0].event]);
	assert.equal(restored.acceptor.bindRun(v2Binding), true);
	assert.equal(restored.acceptor.bindRun({ ...v2Binding, lifecycleProtocol: 1 }), false);
});

test("reconciles only an unchanged uncertain contract 2 run with exact endpoint evidence", () => {
	const h = setup();
	const v2Binding = { ...binding, runId: "run-reconcile", lifecycleProtocol: 2 as const };
	h.acceptor.bindRun(v2Binding);
	assert.equal(h.acceptor.accept({
		...candidate({ runId: v2Binding.runId, worker: v2Binding.worker }),
		protocol: 2,
		eventId: "event-uncertain-v2",
		status: "uncertain",
		evidence: { kind: "uncertain", scope: "assignment_delivery", detail: "Delivery acknowledgement was interrupted" },
	}).accepted, true);

	const input = {
		runId: v2Binding.runId,
		expectedAcceptedSequence: 1,
		resolution: {
			status: "started" as const,
			detail: "The bound worker is still active.",
			observations: [{
				source: "herdr" as const,
				endpoint: { agentName: v2Binding.worker.name, paneId: v2Binding.worker.paneId },
				detail: "Herdr reports the original pane is active.",
				observedAt: 1_786_000_000_100,
			}],
		},
	};
	const mismatch = h.acceptor.reconcile(input, { sourceInstanceId: "reconciler-1" }, { agentName: v2Binding.worker.name, paneId: "pane-other" });
	assert.deepEqual(mismatch.accepted ? undefined : mismatch.reason, "endpoint_mismatch");
	assert.equal(h.acceptor.getRun(v2Binding.runId)?.status, "uncertain");
	assert.equal(h.journal.length, 1);

	const stale = h.acceptor.reconcile({ ...input, expectedAcceptedSequence: 2 }, { sourceInstanceId: "reconciler-1" }, {
		agentName: v2Binding.worker.name,
		paneId: v2Binding.worker.paneId,
	});
	assert.deepEqual(stale.accepted ? undefined : stale.reason, "stale_accepted_sequence");

	const reconciled = h.acceptor.reconcile(input, {
		sourceInstanceId: "reconciler-1",
		eventId: "event-reconciled-v2",
		observedAt: 1_786_000_000_200,
	}, { agentName: v2Binding.worker.name, paneId: v2Binding.worker.paneId });
	assert.equal(reconciled.accepted, true);
	if (!reconciled.accepted) return;
	assert.equal(reconciled.event.acceptedSequence, 2);
	assert.equal(reconciled.event.source, "reconciler");
	assert.equal(reconciled.event.evidence.kind, "reconciled_started_v2");
	assert.deepEqual(h.timeline.slice(-3), [
		`append:${LIFECYCLE_JOURNAL_ENTRY}`,
		`emit:${LIFECYCLE_CHANNELS.lifecycle}`,
		`emit:${LIFECYCLE_CHANNELS.started}`,
	]);
	assert.equal(h.acceptor.getRun(v2Binding.runId)?.status, "started");
	const repeated = h.acceptor.reconcile({ ...input, expectedAcceptedSequence: 2 }, { sourceInstanceId: "reconciler-1" }, {
		agentName: v2Binding.worker.name,
		paneId: v2Binding.worker.paneId,
	});
	assert.deepEqual(repeated.accepted ? undefined : repeated.reason, "not_uncertain");
});

test("reconciled completion is durable structured terminal evidence", () => {
	const h = setup();
	const v2Binding = { ...binding, runId: "run-reconciled-completion", lifecycleProtocol: 2 as const };
	h.acceptor.bindRun(v2Binding);
	h.acceptor.accept({
		...candidate({ runId: v2Binding.runId, worker: v2Binding.worker }),
		protocol: 2,
		eventId: "event-uncertain-completion",
		status: "uncertain",
		evidence: { kind: "uncertain", scope: "agent_start", detail: "Start result was ambiguous" },
	});
	const completed = h.acceptor.reconcile({
		runId: v2Binding.runId,
		expectedAcceptedSequence: 1,
		resolution: {
			status: "completed",
			result: "The requested implementation is complete.",
			detail: "Git and test evidence establish the result.",
			observations: [{ source: "git", detail: "Expected commit and clean worktree are present.", observedAt: 10 }],
			artifacts: [{ path: "reports/result.md" }],
			checks: [{ kind: "test", command: "npm test", outcome: "passed" }],
		},
	}, { sourceInstanceId: "reconciler-completion", eventId: "event-reconciled-completion", observedAt: 11 });
	assert.equal(completed.accepted, true);
	assert.equal(h.acceptor.getRun(v2Binding.runId)?.terminalEvidence?.kind, "reconciled_completed_v2");

	const restored = setup(h.journal.map((data) => ({ type: "custom", customType: LIFECYCLE_JOURNAL_ENTRY, data })));
	assert.deepEqual(restored.acceptor.replayRun(v2Binding.runId, 1, 10)?.events, [h.journal[1].event]);
	assert.deepEqual(restored.acceptor.getRun(v2Binding.runId)?.terminalEvidence, h.journal[1].event.evidence);
});
