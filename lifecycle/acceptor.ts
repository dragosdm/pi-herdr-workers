import { Check } from "typebox/value";
import {
	LIFECYCLE_CHANNELS,
	LifecycleCorrelationIdSchema,
	LifecycleRunIdSchema,
	LifecycleSourceInstanceIdSchema,
	isAcceptedLifecycleEvent,
	isLifecycleCandidate,
	lifecycleChannel,
	type AcceptedLifecycleEvent,
	type LifecycleCandidate,
	type LifecycleStatus,
	type WorkerLifecycleEvidence,
} from "./protocol.js";

export const LIFECYCLE_JOURNAL_ENTRY = "herdr-worker.lifecycle.v1";
export const LIFECYCLE_REJECTION_ENTRY = "herdr-worker.lifecycle-rejection.v1";

export interface LifecycleJournalEntry {
	version: 1;
	sessionId: string;
	event: AcceptedLifecycleEvent;
}

export interface LifecycleRejectionEntry {
	version: 1;
	sessionId: string;
	reason: Extract<LifecycleRejectionReason, "invalid_transition" | "terminal_conflict" | "unsupported_stopped">;
	candidate: LifecycleCandidate;
}

export interface RunBinding {
	runId: string;
	correlationId?: string;
	worker: { name: string; paneId?: string };
	requestId?: string;
	providerInstanceId?: string;
}

export interface RunLifecycleRecord extends RunBinding {
	status?: LifecycleStatus;
	readiness?: "unconfirmed" | "confirmed";
	terminalEvidence?: WorkerLifecycleEvidence;
	acceptedSequence: number;
	eventIds: Set<string>;
	sourceSequences: Map<string, number>;
}

export type LifecycleRejectionReason =
	| "invalid_candidate"
	| "unbound_run"
	| "binding_mismatch"
	| "duplicate"
	| "stale_source"
	| "invalid_transition"
	| "terminal_duplicate"
	| "terminal_conflict"
	| "unsupported_stopped";

export type LifecycleAcceptanceResult =
	| { accepted: true; event: AcceptedLifecycleEvent; record: RunLifecycleRecord }
	| { accepted: false; reason: LifecycleRejectionReason; record?: RunLifecycleRecord };

interface SessionEntry {
	type?: unknown;
	customType?: unknown;
	data?: unknown;
}

export interface LifecycleAcceptorOptions {
	sessionId: string;
	getEntries: () => readonly SessionEntry[];
	appendEntry: (customType: string, data: LifecycleJournalEntry | LifecycleRejectionEntry) => void;
	emit: (channel: string, payload: AcceptedLifecycleEvent) => unknown;
}

const TERMINAL_STATUSES = new Set<LifecycleStatus>(["completed", "failed", "stopped"]);

function copyRecord(record: RunLifecycleRecord): RunLifecycleRecord {
	return {
		...record,
		worker: { ...record.worker },
		...(record.terminalEvidence === undefined ? {} : { terminalEvidence: structuredClone(record.terminalEvidence) }),
		eventIds: new Set(record.eventIds),
		sourceSequences: new Map(record.sourceSequences),
	};
}

function sameEvidence(left: WorkerLifecycleEvidence | undefined, right: WorkerLifecycleEvidence): boolean {
	return left !== undefined && JSON.stringify(left) === JSON.stringify(right);
}

function validBinding(binding: RunBinding): boolean {
	return Check(LifecycleRunIdSchema, binding.runId)
		&& (binding.correlationId === undefined || Check(LifecycleCorrelationIdSchema, binding.correlationId))
		&& typeof binding.worker.name === "string"
		&& binding.worker.name.length > 0
		&& (binding.worker.paneId === undefined || binding.worker.paneId.length > 0)
		&& (binding.providerInstanceId === undefined || Check(LifecycleSourceInstanceIdSchema, binding.providerInstanceId));
}

function sameBinding(record: RunLifecycleRecord, candidate: Pick<LifecycleCandidate, "correlationId" | "worker">): boolean {
	return record.correlationId === candidate.correlationId
		&& record.worker.name === candidate.worker.name
		&& record.worker.paneId === candidate.worker.paneId;
}

function compatibleBinding(record: RunLifecycleRecord, binding: RunBinding): boolean {
	return record.correlationId === binding.correlationId
		&& record.worker.name === binding.worker.name
		&& (record.worker.paneId === undefined || binding.worker.paneId === undefined || record.worker.paneId === binding.worker.paneId);
}

function transitionReason(record: RunLifecycleRecord, candidate: LifecycleCandidate): LifecycleRejectionReason | undefined {
	const current = record.status;
	if (candidate.status === "stopped") return "unsupported_stopped";
	if (candidate.status === "message") return undefined;
	if (TERMINAL_STATUSES.has(current as LifecycleStatus)) {
		if (candidate.status === current && sameEvidence(record.terminalEvidence, candidate.evidence)) return "terminal_duplicate";
		if (TERMINAL_STATUSES.has(candidate.status)) return "terminal_conflict";
		return "invalid_transition";
	}
	if (candidate.status === "uncertain") {
		if (current === "started" && record.readiness === "confirmed" && candidate.evidence.scope !== "assignment_delivery") {
			return "invalid_transition";
		}
		return undefined;
	}
	if (candidate.status === "started") {
		if (current === undefined || current === "uncertain") return undefined;
		if (current === "started" && candidate.evidence.kind === "worker_ready" && record.readiness !== "confirmed") return undefined;
		return "invalid_transition";
	}
	return undefined;
}

function applyAccepted(record: RunLifecycleRecord, event: AcceptedLifecycleEvent): void {
	record.acceptedSequence = event.acceptedSequence;
	record.eventIds.add(event.eventId);
	if (event.sourceSequence !== undefined) record.sourceSequences.set(event.sourceInstanceId, event.sourceSequence);
	if (event.status === "message") return;
	if (event.status === "started") {
		record.status = "started";
		if (event.evidence.kind === "worker_ready") record.readiness = "confirmed";
		else if (record.readiness === undefined) record.readiness = "unconfirmed";
		return;
	}
	record.status = event.status;
	if (TERMINAL_STATUSES.has(event.status)) record.terminalEvidence = structuredClone(event.evidence);
}

function safeEmit(emit: LifecycleAcceptorOptions["emit"], channel: string, event: AcceptedLifecycleEvent): void {
	try {
		const result = emit(channel, event);
		if (result && typeof (result as PromiseLike<unknown>).then === "function") {
			void Promise.resolve(result).catch(() => {});
		}
	} catch {}
}

export class LifecycleAcceptor {
	private readonly records = new Map<string, RunLifecycleRecord>();
	private readonly acceptedEvents = new Map<string, AcceptedLifecycleEvent[]>();

	constructor(private readonly options: LifecycleAcceptorOptions) {
		this.restore();
	}

	bindRun(binding: RunBinding): boolean {
		if (!validBinding(binding)) return false;
		const current = this.records.get(binding.runId);
		if (current) {
			const matches = compatibleBinding(current, binding)
				&& (current.requestId === undefined || binding.requestId === undefined || current.requestId === binding.requestId)
				&& (current.providerInstanceId === undefined || binding.providerInstanceId === undefined || current.providerInstanceId === binding.providerInstanceId);
			if (matches) {
				current.worker.paneId ??= binding.worker.paneId;
				current.requestId ??= binding.requestId;
				current.providerInstanceId ??= binding.providerInstanceId;
			}
			return matches;
		}
		this.records.set(binding.runId, {
			...binding,
			worker: { ...binding.worker },
			acceptedSequence: 0,
			eventIds: new Set(),
			sourceSequences: new Map(),
		});
		return true;
	}

	getRun(runId: string): RunLifecycleRecord | undefined {
		const record = this.records.get(runId);
		return record ? copyRecord(record) : undefined;
	}

	listRuns(): RunLifecycleRecord[] {
		return [...this.records.values()]
			.sort((left, right) => left.runId < right.runId ? -1 : left.runId > right.runId ? 1 : 0)
			.map(copyRecord);
	}

	replayRun(runId: string, afterAcceptedSequence: number, limit: number): { events: AcceptedLifecycleEvent[]; hasMore: boolean } | undefined {
		if (!Number.isInteger(afterAcceptedSequence) || afterAcceptedSequence < 0) throw new RangeError("Invalid accepted sequence cursor.");
		if (!Number.isInteger(limit) || limit < 1) throw new RangeError("Invalid replay limit.");
		if (!this.records.has(runId)) return undefined;
		const matching = (this.acceptedEvents.get(runId) ?? []).filter((event) => event.acceptedSequence > afterAcceptedSequence);
		return {
			events: matching.slice(0, limit).map((event) => structuredClone(event)),
			hasMore: matching.length > limit,
		};
	}

	accept(value: unknown): LifecycleAcceptanceResult {
		if (!isLifecycleCandidate(value)) return { accepted: false, reason: "invalid_candidate" };
		const candidate = value;
		const record = this.records.get(candidate.runId);
		if (!record) return { accepted: false, reason: "unbound_run" };
		if (!sameBinding(record, candidate)) return { accepted: false, reason: "binding_mismatch", record: copyRecord(record) };
		if (candidate.source === "provider" && record.providerInstanceId !== undefined && candidate.sourceInstanceId !== record.providerInstanceId) {
			return { accepted: false, reason: "binding_mismatch", record: copyRecord(record) };
		}
		if (record.eventIds.has(candidate.eventId)) return { accepted: false, reason: "duplicate", record: copyRecord(record) };
		const priorSourceSequence = record.sourceSequences.get(candidate.sourceInstanceId);
		if (candidate.sourceSequence !== undefined && priorSourceSequence !== undefined && candidate.sourceSequence <= priorSourceSequence) {
			return { accepted: false, reason: "stale_source", record: copyRecord(record) };
		}
		const reason = transitionReason(record, candidate);
		if (reason) {
			this.recordRejection(reason, candidate);
			return { accepted: false, reason, record: copyRecord(record) };
		}

		const event = { ...candidate, acceptedSequence: record.acceptedSequence + 1 } as AcceptedLifecycleEvent;
		this.options.appendEntry(LIFECYCLE_JOURNAL_ENTRY, {
			version: 1,
			sessionId: this.options.sessionId,
			event,
		});
		applyAccepted(record, event);
		this.rememberAccepted(event);
		safeEmit(this.options.emit, LIFECYCLE_CHANNELS.lifecycle, event);
		safeEmit(this.options.emit, lifecycleChannel(event.status), event);
		return { accepted: true, event, record: copyRecord(record) };
	}

	private recordRejection(reason: LifecycleRejectionReason, candidate: LifecycleCandidate): void {
		if (reason !== "invalid_transition" && reason !== "terminal_conflict" && reason !== "unsupported_stopped") return;
		try {
			this.options.appendEntry(LIFECYCLE_REJECTION_ENTRY, {
				version: 1,
				sessionId: this.options.sessionId,
				reason,
				candidate,
			});
		} catch {}
	}

	private rememberAccepted(event: AcceptedLifecycleEvent): void {
		const events = this.acceptedEvents.get(event.runId) ?? [];
		events.push(structuredClone(event));
		this.acceptedEvents.set(event.runId, events);
	}

	private restore(): void {
		const eventsByRun = new Map<string, AcceptedLifecycleEvent[]>();
		for (const entry of this.options.getEntries()) {
			if (entry.type !== "custom" || entry.customType !== LIFECYCLE_JOURNAL_ENTRY) continue;
			const journal = entry.data as Partial<LifecycleJournalEntry> | undefined;
			if (journal?.version !== 1 || journal.sessionId !== this.options.sessionId || !isAcceptedLifecycleEvent(journal.event)) continue;
			const events = eventsByRun.get(journal.event.runId) ?? [];
			events.push(journal.event);
			eventsByRun.set(journal.event.runId, events);
		}

		for (const events of eventsByRun.values()) {
			events.sort((left, right) => left.acceptedSequence - right.acceptedSequence);
			for (const event of events) {
				let record = this.records.get(event.runId);
				if (!record) {
						record = {
						runId: event.runId,
						...(event.correlationId === undefined ? {} : { correlationId: event.correlationId }),
							worker: { ...event.worker },
							...(event.source === "provider" ? { providerInstanceId: event.sourceInstanceId } : {}),
						acceptedSequence: 0,
						eventIds: new Set(),
						sourceSequences: new Map(),
					};
					this.records.set(event.runId, record);
				}
				if (event.acceptedSequence !== record.acceptedSequence + 1 || !sameBinding(record, event) || record.eventIds.has(event.eventId)) continue;
				const priorSourceSequence = record.sourceSequences.get(event.sourceInstanceId);
				if (event.sourceSequence !== undefined && priorSourceSequence !== undefined && event.sourceSequence <= priorSourceSequence) continue;
				if (transitionReason(record, event)) continue;
				applyAccepted(record, event);
				this.rememberAccepted(event);
			}
		}
	}
}

export function createLifecycleAcceptor(options: LifecycleAcceptorOptions): LifecycleAcceptor {
	return new LifecycleAcceptor(options);
}
