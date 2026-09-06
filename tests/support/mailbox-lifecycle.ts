import assert from "node:assert/strict";
import { createLifecycleAcceptor, LIFECYCLE_JOURNAL_ENTRY, type LifecycleJournalEntry } from "../../lifecycle/acceptor.js";
import type { WorkerRunReport } from "../../lifecycle/protocol.js";
import { createRunQueryClient } from "../../runs/client.js";
import { createRunRegistry } from "../../runs/registry.js";
import type { RpcEventBus } from "../../rpc/client.js";
import type { ControlledEntry, ControlledPiHost } from "./controlled-pi-host.js";

export const recoveryRunId = "lifecycle-recovery-run";
export const lifecycleEnvelope = {
	type: "lifecycle", ts: 1,
	from: { id: "agent-scout", paneId: "worker-pane", name: "agent-scout", role: "worker" },
	report: { protocol: 2, runId: recoveryRunId, eventId: "recovery-completed", sourceInstanceId: "recovery-source",
		sourceSequence: 1, observedAt: 1, status: "completed", evidence: { kind: "worker_completed_v2", result: "Recovery result" } } satisfies WorkerRunReport,
};

export function selectedTeamEntry(entry: ControlledEntry) {
	return entry.type === "custom" && entry.customType === "herdr-worker" && !(entry.data as { offBranch?: boolean }).offBranch;
}

export function teamState(entries: readonly ControlledEntry[]): any {
	return (entries.filter(selectedTeamEntry).at(-1) as { data?: unknown } | undefined)?.data;
}

export function journals(entries: readonly ControlledEntry[]): LifecycleJournalEntry[] {
	return entries.filter((entry) => entry.type === "custom" && entry.customType === LIFECYCLE_JOURNAL_ENTRY)
		.map((entry) => (entry as { data: LifecycleJournalEntry }).data);
}

export function seedLifecycleAuthority(h: Pick<ControlledPiHost, "appendEntry" | "ctx"> & {
	entries: readonly { type?: unknown; customType?: unknown; data?: unknown }[];
}, bound = true) {
	const sessionId = h.ctx.sessionManager.getSessionId();
	const registry = createRunRegistry({ sessionId, getEntries: () => h.entries, appendEntry: h.appendEntry });
	registry.register({ version: 1, sessionId, runId: recoveryRunId, lifecycleProtocol: 2,
		correlationId: "recovery-correlation", registeredAt: 1, assignment: { cwd: h.ctx.cwd, role: "test" } });
	registry.bindEndpoint({ version: 1, sessionId, runId: recoveryRunId, agentName: "agent-scout", paneId: "worker-pane", observedAt: 2 });
	h.appendEntry("herdr-worker", { version: 1, sessionId, workers: ["agent-scout"],
		...(bound ? { meta: { "agent-scout": { runId: recoveryRunId, paneId: "worker-pane", lifecycleProtocol: 2, correlationId: "recovery-correlation" } } } : {}) });
}

export function seedProviderEvidence(h: ControlledPiHost, status: "started" | "uncertain") {
	const acceptor = createLifecycleAcceptor({ sessionId: "session", getEntries: () => h.entries,
		appendEntry: h.appendEntry, emit: (channel, event) => h.events.emit(channel, event) });
	assert.equal(acceptor.bindRun({ runId: recoveryRunId, lifecycleProtocol: 2, correlationId: "recovery-correlation",
		worker: { name: "agent-scout", paneId: "worker-pane" }, providerInstanceId: "seed-provider" }), true);
	const result = acceptor.accept({ protocol: 2, runId: recoveryRunId, eventId: `seed-${status}`, sourceInstanceId: "seed-provider",
		sourceSequence: 1, observedAt: 3, source: "provider", status, correlationId: "recovery-correlation",
		worker: { name: "agent-scout", paneId: "worker-pane" }, evidence: status === "started"
			? { kind: "agent_start_returned", readiness: "unconfirmed" }
			: { kind: "uncertain", scope: "assignment_delivery", detail: "Seeded ambiguous assignment delivery; no external worker launched" } });
	assert.equal(result.accepted, true);
}

export async function queryLifecycle(events: RpcEventBus) {
	const client = createRunQueryClient({ events });
	const options = { timeoutMs: 2000 };
	const provider = await client.probe(options);
	assert.equal(provider.protocol, 2);
	const record = await client.get({ runId: recoveryRunId }, provider, options);
	const replay = await client.replay({ runId: recoveryRunId, afterAcceptedSequence: 0 }, provider, options);
	assert.equal(replay.hasMore, false);
	return { record, replay };
}
