import assert from "node:assert/strict";
import test from "node:test";
import {
	RECONCILIATION_CHANNELS,
	RECONCILIATION_LIMITS,
	isReconcileRunInput,
	isValidReconciliationRequest,
	reconciliationReplyChannel,
} from "../../reconciliation/protocol.js";

const observation = { source: "git" as const, detail: "Commit contains the requested change.", observedAt: 1_786_000_000_000 };

test("accepts bounded reconciliation variants and endpoint observations", () => {
	assert.equal(isReconcileRunInput({
		runId: "run-1",
		expectedAcceptedSequence: 2,
		resolution: { status: "started", detail: "Worker is still active.", observations: [
			{ source: "worker", endpoint: { agentName: "agent-one", paneId: "pane-1" }, detail: "Worker acknowledged the run.", observedAt: 10 },
		] },
	}), true);
	assert.equal(isReconcileRunInput({
		runId: "run-1",
		expectedAcceptedSequence: 2,
		resolution: {
			status: "completed",
			result: "Implemented and verified.",
			detail: "Git and test evidence establish completion.",
			observations: [observation],
			artifacts: [{ path: "reports/result.md", description: "Result" }],
			checks: [{ kind: "test", command: "npm test", outcome: "passed" }],
		},
	}), true);
	assert.equal(isReconcileRunInput({
		runId: "run-1",
		expectedAcceptedSequence: 2,
		resolution: { status: "failed", detail: "Worker can no longer write.", observations: [observation] },
	}), true);
});

test("rejects empty evidence, malformed endpoints, excess observations, and oversized UTF-8", () => {
	const base = { runId: "run-1", expectedAcceptedSequence: 1 };
	assert.equal(isReconcileRunInput({ ...base, resolution: { status: "started", detail: "Active", observations: [] } }), false);
	assert.equal(isReconcileRunInput({ ...base, resolution: {
		status: "started", detail: "Active", observations: [
			{ source: "worker", endpoint: { agentName: "agent", paneId: "" }, detail: "Seen", observedAt: 1 },
		],
	} }), false);
	assert.equal(isReconcileRunInput({ ...base, resolution: {
		status: "failed", detail: "Stopped", observations: Array.from({ length: RECONCILIATION_LIMITS.observations + 1 }, () => observation),
	} }), false);
	assert.equal(isReconcileRunInput({ ...base, resolution: {
		status: "completed", result: "é".repeat(32_769), detail: "Done", observations: [observation],
	} }), false);
});

test("validates addressed transport fields independently from model input", () => {
	const request = {
		requestId: "request-1",
		providerInstanceId: "provider-1",
		protocol: 1,
		runId: "run-1",
		expectedAcceptedSequence: 1,
		resolution: { status: "started", detail: "Active", observations: [observation] },
	};
	assert.equal(isValidReconciliationRequest(RECONCILIATION_CHANNELS.reconcile, request), true);
	assert.equal(isValidReconciliationRequest(RECONCILIATION_CHANNELS.reconcile, { ...request, providerInstanceId: "bad/id" }), false);
	assert.equal(reconciliationReplyChannel(RECONCILIATION_CHANNELS.reconcile, "request-1"), "herdr-workers:reconciliation:rpc:reconcile:reply:request-1");
});
