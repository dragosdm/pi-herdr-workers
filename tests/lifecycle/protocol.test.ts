import assert from "node:assert/strict";
import test from "node:test";
import { Check } from "typebox/value";
import {
	AcceptedLifecycleEventSchema,
	ArtifactReferenceSchema,
	LIFECYCLE_CHANNELS,
	LIFECYCLE_LIMITS,
	LIFECYCLE_SUPPORTED_PROTOCOLS,
	LIFECYCLE_SOURCES,
	LIFECYCLE_STATUSES,
	LifecycleCandidateSchema,
	WorkerRunBindingSchema,
	WorkerRunReportInputSchema,
	WorkerRunReportInputV1Schema,
	WorkerRunReportInputV2Schema,
	WorkerRunReportSchema,
	WorkerLifecycleEvidenceSchema,
	VerificationCheckSchema,
	isAcceptedLifecycleEvent,
	isLifecycleCandidate,
	isWorkerRunBinding,
	isWorkerRunReport,
	isWorkerRunReportInput,
	lifecycleChannel,
} from "../../lifecycle/protocol.js";

const base = {
	protocol: 1 as const,
	eventId: "event-1",
	runId: "run-1",
	sourceInstanceId: "source-1",
	sourceSequence: 1,
	worker: { name: "agent-scout", paneId: "pane-1" },
	observedAt: 1_786_000_000_000,
	correlationId: "dispatch-1",
};

test("exports canonical and projected channels for every lifecycle status", () => {
	assert.equal(LIFECYCLE_CHANNELS.lifecycle, "herdr-workers:lifecycle");
	assert.deepEqual(LIFECYCLE_STATUSES, ["started", "message", "completed", "failed", "stopped", "uncertain"]);
	assert.deepEqual(LIFECYCLE_SOURCES, ["provider", "worker", "reconciler", "controller"]);
	assert.deepEqual(LIFECYCLE_SUPPORTED_PROTOCOLS, [2, 1]);
	for (const status of LIFECYCLE_STATUSES) assert.equal(lifecycleChannel(status), `herdr-workers:${status}`);
});

test("accepts each status only with its evidence variant", () => {
	const cases = [
		{ source: "provider", status: "started", evidence: { kind: "agent_start_returned", readiness: "unconfirmed" } },
		{ source: "worker", status: "started", evidence: { kind: "worker_ready", readiness: "confirmed" } },
		{ source: "worker", status: "message", evidence: { kind: "worker_message", message: "Working" } },
		{ source: "worker", status: "completed", evidence: { kind: "worker_completed", result: "Done" } },
		{ source: "worker", status: "failed", evidence: { kind: "worker_failed", error: "Blocked" } },
		{ source: "reconciler", status: "failed", evidence: { kind: "reconciled_failure", detail: "No process exists" } },
		{ source: "controller", status: "stopped", evidence: { kind: "stop_acknowledged", stopRequestId: "stop-1" } },
		{ source: "provider", status: "uncertain", evidence: { kind: "uncertain", scope: "agent_start", detail: "Command outcome unknown" } },
	] as const;

	for (const item of cases) assert.equal(isLifecycleCandidate({ ...base, ...item }), true, item.status);
	assert.equal(isLifecycleCandidate({ ...base, source: "worker", status: "completed", evidence: { kind: "worker_message", message: "Done" } }), false);
	assert.equal(isLifecycleCandidate({ ...base, source: "provider", status: "completed", evidence: { kind: "worker_completed", result: "Done" } }), false);
	assert.equal(Check(WorkerLifecycleEvidenceSchema, { kind: "stop_acknowledged", stopRequestId: "stop-1" }), true);
	assert.equal(Check(WorkerLifecycleEvidenceSchema, { kind: "stopped" }), false);
});

test("enforces safe identities and structural requirements", () => {
	const candidate = { ...base, source: "worker", status: "message", evidence: { kind: "worker_message", message: "hello" } };
	assert.equal(Check(LifecycleCandidateSchema, candidate), true);
	for (const field of ["eventId", "runId", "sourceInstanceId", "correlationId"] as const) {
		assert.equal(isLifecycleCandidate({ ...candidate, [field]: "unsafe/id" }), false, field);
	}
	assert.equal(isLifecycleCandidate({ ...candidate, worker: { name: "" } }), false);
	assert.equal(isLifecycleCandidate({ ...candidate, sourceSequence: 0 }), false);
	assert.equal(isLifecycleCandidate({ ...candidate, observedAt: -1 }), false);
	assert.equal(isLifecycleCandidate({ ...candidate, protocol: 2 }), true);
	assert.equal(isLifecycleCandidate({ ...candidate, protocol: 3 }), false);
});

test("enforces UTF-8 byte budgets for bounded evidence text", () => {
	const atLimit = "é".repeat(LIFECYCLE_LIMITS.error / 2);
	const overLimit = `${atLimit}é`;
	const candidate = { ...base, source: "worker", status: "failed", evidence: { kind: "worker_failed", error: atLimit } };
	assert.equal(isLifecycleCandidate(candidate), true);
	assert.equal(isLifecycleCandidate({ ...candidate, evidence: { ...candidate.evidence, error: overLimit } }), false);

	const crossing = `${"a".repeat(LIFECYCLE_LIMITS.detail - 3)}😀`;
	assert.equal(isLifecycleCandidate({
		...base,
		source: "provider",
		status: "uncertain",
		evidence: { kind: "uncertain", scope: "pane_creation", detail: crossing },
	}), false);
});

test("accepts future fields while preserving known-field validation", () => {
	const candidate = {
		...base,
		source: "worker",
		status: "completed",
		evidence: { kind: "worker_completed", result: "done", futureEvidence: true },
		futureRoot: true,
	};
	assert.equal(isLifecycleCandidate(candidate), true);
	assert.equal(isLifecycleCandidate({ ...candidate, status: "future" }), false);
});

test("accepted events require a positive parent-assigned sequence", () => {
	const candidate = { ...base, source: "worker", status: "message", evidence: { kind: "worker_message", message: "hello" } };
	assert.equal(isLifecycleCandidate(candidate), true);
	assert.equal(Check(AcceptedLifecycleEventSchema, candidate), false);
	assert.equal(isAcceptedLifecycleEvent({ ...candidate, acceptedSequence: 1 }), true);
	assert.equal(isAcceptedLifecycleEvent({ ...candidate, acceptedSequence: 0 }), false);
});

test("validates structured run bindings and worker-owned reports", () => {
	const binding = { protocol: 1, runId: "run-1", correlationId: "dispatch-1" };
	assert.equal(Check(WorkerRunBindingSchema, binding), true);
	assert.equal(isWorkerRunBinding(binding), true);
	assert.equal(isWorkerRunBinding({ ...binding, runId: "unsafe/run" }), false);

	const reports = [
		{ status: "started", evidence: { kind: "worker_ready", readiness: "confirmed" } },
		{ status: "message", evidence: { kind: "worker_message", message: "Working" } },
		{ status: "completed", evidence: { kind: "worker_completed", result: "Done" } },
		{ status: "failed", evidence: { kind: "worker_failed", error: "Blocked" } },
	] as const;
	for (const report of reports) {
		const value = {
			protocol: 1,
			eventId: `event-${report.status}`,
			runId: "run-1",
			sourceInstanceId: "worker-source-1",
			sourceSequence: 1,
			observedAt: 1_786_000_000_000,
			...report,
		};
		assert.equal(Check(WorkerRunReportSchema, value), true, report.status);
		assert.equal(isWorkerRunReport(value), true, report.status);
	}
	assert.equal(isWorkerRunReport({
		protocol: 1,
		eventId: "event-bad",
		runId: "run-1",
		sourceInstanceId: "worker-source-1",
		sourceSequence: 1,
		observedAt: 1,
		status: "completed",
		evidence: { kind: "worker_message", message: "Done" },
	}), false);
});

test("validates only model-owned report fields with bounded content", () => {
	const inputs = [
		{ status: "message", message: "Working" },
		{ status: "completed", result: "Done" },
		{ status: "failed", error: "Blocked" },
	] as const;
	for (const input of inputs) {
		assert.equal(Check(WorkerRunReportInputSchema, input), true);
		assert.equal(isWorkerRunReportInput(input), true);
	}
	assert.equal(isWorkerRunReportInput({ status: "message" }), false);
	assert.equal(isWorkerRunReportInput({ status: "completed" }), false);
	assert.equal(isWorkerRunReportInput({ status: "completed" }, 1), true);
	assert.equal(isWorkerRunReportInput({ status: "failed", error: "😀".repeat(LIFECYCLE_LIMITS.error) }), false);
});

test("validates contract 2 structured completion and preserves contract 1 compatibility", () => {
	const completion = {
		status: "completed",
		result: "Implemented the lifecycle contract.",
		artifacts: [{ path: "reports/result.md", description: "Final report" }],
		checks: [{ kind: "test", command: "npm test", outcome: "passed" }],
	} as const;
	assert.equal(Check(WorkerRunReportInputV2Schema, completion), true);
	assert.equal(isWorkerRunReportInput(completion, 2), true);
	assert.equal(Check(ArtifactReferenceSchema, completion.artifacts[0]), true);
	assert.equal(Check(VerificationCheckSchema, completion.checks[0]), true);
	assert.equal(Check(WorkerRunReportInputV1Schema, { status: "completed" }), true);

	const report = {
		...base,
		protocol: 2,
		status: "completed",
		evidence: { kind: "worker_completed_v2", result: completion.result, artifacts: completion.artifacts, checks: completion.checks },
	};
	assert.equal(isWorkerRunReport(report), true);
	assert.equal(isLifecycleCandidate({ ...report, worker: base.worker, source: "worker" }), true);
	assert.equal(isWorkerRunReport({ ...report, protocol: 1 }), false);
	assert.equal(isWorkerRunReport({ ...report, evidence: { kind: "worker_completed_v2", result: "" } }), false);
});

test("enforces contract 2 completion cardinality, byte, and path safety limits", () => {
	const valid = { status: "completed", result: "Done" } as const;
	assert.equal(isWorkerRunReportInput({ ...valid, artifacts: Array.from({ length: 32 }, (_, index) => ({ path: `artifact-${index}` })) }), true);
	assert.equal(isWorkerRunReportInput({ ...valid, artifacts: Array.from({ length: 33 }, (_, index) => ({ path: `artifact-${index}` })) }), false);
	assert.equal(isWorkerRunReportInput({ ...valid, checks: Array.from({ length: 33 }, () => ({ kind: "command", command: "true", outcome: "passed" })) }), false);
	for (const path of ["bad\npath", "bad\u0000path", "bad\u007fpath"]) {
		assert.equal(isWorkerRunReportInput({ ...valid, artifacts: [{ path }] }), false, JSON.stringify(path));
	}
	assert.equal(isWorkerRunReportInput({ ...valid, artifacts: [{ path: "é".repeat(LIFECYCLE_LIMITS.artifactPath / 2) }] }), true);
	assert.equal(isWorkerRunReportInput({ ...valid, artifacts: [{ path: `${"é".repeat(LIFECYCLE_LIMITS.artifactPath / 2)}é` }] }), false);
	assert.equal(isWorkerRunReportInput({ ...valid, checks: [{ kind: "test", command: "", outcome: "passed" }] }), false);
});
