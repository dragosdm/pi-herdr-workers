import { Buffer } from "node:buffer";
import { Type, type Static, type TSchema } from "typebox";
import { Check } from "typebox/value";

export const LIFECYCLE_PROTOCOL_V1 = 1 as const;
export const LIFECYCLE_PROTOCOL_V2 = 2 as const;
export const LIFECYCLE_SUPPORTED_PROTOCOLS = [LIFECYCLE_PROTOCOL_V2, LIFECYCLE_PROTOCOL_V1] as const;
export type LifecycleProtocol = (typeof LIFECYCLE_SUPPORTED_PROTOCOLS)[number];

export const LIFECYCLE_CHANNELS = {
	lifecycle: "herdr-workers:lifecycle",
	started: "herdr-workers:started",
	message: "herdr-workers:message",
	completed: "herdr-workers:completed",
	failed: "herdr-workers:failed",
	stopped: "herdr-workers:stopped",
	uncertain: "herdr-workers:uncertain",
} as const;

export const LIFECYCLE_STATUSES = ["started", "message", "completed", "failed", "stopped", "uncertain"] as const;
export const LIFECYCLE_SOURCES = ["provider", "worker", "reconciler", "controller"] as const;

export type LifecycleStatus = (typeof LIFECYCLE_STATUSES)[number];
export type LifecycleSource = (typeof LIFECYCLE_SOURCES)[number];

export const LIFECYCLE_LIMITS = {
	id: 128,
	workerName: 32,
	paneId: 128,
	message: 65_536,
	result: 65_536,
	error: 4_096,
	detail: 4_096,
	artifactPath: 4_096,
	artifactDescription: 1_024,
	checkCommand: 4_096,
	checkOutcome: 4_096,
	completionItems: 32,
} as const;

const SafeIdSchema = Type.String({ minLength: 1, maxLength: LIFECYCLE_LIMITS.id, pattern: "^[A-Za-z0-9._-]+$" });
const bounded = (maxLength: number) => Type.String({ minLength: 1, maxLength });
const SourceSchema = Type.Union(LIFECYCLE_SOURCES.map((source) => Type.Literal(source)));
export const LifecycleEventIdSchema = SafeIdSchema;
export const LifecycleRunIdSchema = SafeIdSchema;
export const LifecycleSourceInstanceIdSchema = SafeIdSchema;
export const LifecycleCorrelationIdSchema = SafeIdSchema;

export const WorkerRunBindingSchema = Type.Object({
	protocol: Type.Union([Type.Literal(LIFECYCLE_PROTOCOL_V1), Type.Literal(LIFECYCLE_PROTOCOL_V2)]),
	runId: LifecycleRunIdSchema,
	correlationId: Type.Optional(LifecycleCorrelationIdSchema),
});

export const ArtifactReferenceSchema = Type.Object({
	path: Type.String({
		minLength: 1,
		maxLength: LIFECYCLE_LIMITS.artifactPath,
		pattern: "^[^\\x00-\\x1F\\x7F-\\x9F]+$",
	}),
	description: Type.Optional(bounded(LIFECYCLE_LIMITS.artifactDescription)),
});
export const VerificationCheckSchema = Type.Object({
	kind: Type.Union([Type.Literal("command"), Type.Literal("test")]),
	command: bounded(LIFECYCLE_LIMITS.checkCommand),
	outcome: bounded(LIFECYCLE_LIMITS.checkOutcome),
});
export type ArtifactReference = Static<typeof ArtifactReferenceSchema>;
export type VerificationCheck = Static<typeof VerificationCheckSchema>;

export const ProviderStartedEvidenceSchema = Type.Object({
	kind: Type.Literal("agent_start_returned"),
	readiness: Type.Literal("unconfirmed"),
});
export const WorkerReadyEvidenceSchema = Type.Object({
	kind: Type.Literal("worker_ready"),
	readiness: Type.Literal("confirmed"),
});
export const ReconciledStartedEvidenceSchema = Type.Object({
	kind: Type.Literal("reconciled_started"),
	detail: bounded(LIFECYCLE_LIMITS.detail),
});
export const WorkerMessageEvidenceSchema = Type.Object({
	kind: Type.Literal("worker_message"),
	message: bounded(LIFECYCLE_LIMITS.message),
});
export const WorkerCompletedEvidenceSchema = Type.Object({
	kind: Type.Literal("worker_completed"),
	result: Type.Optional(bounded(LIFECYCLE_LIMITS.result)),
});
export const WorkerCompletedEvidenceV2Schema = Type.Object({
	kind: Type.Literal("worker_completed_v2"),
	result: bounded(LIFECYCLE_LIMITS.result),
	artifacts: Type.Optional(Type.Array(ArtifactReferenceSchema, { maxItems: LIFECYCLE_LIMITS.completionItems })),
	checks: Type.Optional(Type.Array(VerificationCheckSchema, { maxItems: LIFECYCLE_LIMITS.completionItems })),
});
export const WorkerFailedEvidenceSchema = Type.Object({
	kind: Type.Literal("worker_failed"),
	error: bounded(LIFECYCLE_LIMITS.error),
});
export const ReconciledFailureEvidenceSchema = Type.Object({
	kind: Type.Literal("reconciled_failure"),
	detail: bounded(LIFECYCLE_LIMITS.detail),
});
export const StopAcknowledgedEvidenceSchema = Type.Object({
	kind: Type.Literal("stop_acknowledged"),
	stopRequestId: SafeIdSchema,
});
export const UncertainEvidenceSchema = Type.Object({
	kind: Type.Literal("uncertain"),
	scope: Type.Union([
		Type.Literal("pane_creation"),
		Type.Literal("agent_start"),
		Type.Literal("assignment_delivery"),
		Type.Literal("ownership"),
		Type.Literal("reconciliation"),
	]),
	detail: bounded(LIFECYCLE_LIMITS.detail),
});

export const WorkerLifecycleEvidenceSchema = Type.Union([
	ProviderStartedEvidenceSchema,
	WorkerReadyEvidenceSchema,
	ReconciledStartedEvidenceSchema,
	WorkerMessageEvidenceSchema,
	WorkerCompletedEvidenceSchema,
	WorkerCompletedEvidenceV2Schema,
	WorkerFailedEvidenceSchema,
	ReconciledFailureEvidenceSchema,
	StopAcknowledgedEvidenceSchema,
	UncertainEvidenceSchema,
]);

export type WorkerLifecycleEvidence = Static<typeof WorkerLifecycleEvidenceSchema>;

const WorkerRunReportBase = {
	eventId: LifecycleEventIdSchema,
	runId: LifecycleRunIdSchema,
	sourceInstanceId: LifecycleSourceInstanceIdSchema,
	sourceSequence: Type.Integer({ minimum: 1 }),
	observedAt: Type.Integer({ minimum: 0 }),
};

export const WorkerRunReportV1Schema = Type.Union([
	Type.Object({
		protocol: Type.Literal(LIFECYCLE_PROTOCOL_V1),
		...WorkerRunReportBase,
		status: Type.Literal("started"),
		evidence: WorkerReadyEvidenceSchema,
	}),
	Type.Object({
		protocol: Type.Literal(LIFECYCLE_PROTOCOL_V1),
		...WorkerRunReportBase,
		status: Type.Literal("message"),
		evidence: WorkerMessageEvidenceSchema,
	}),
	Type.Object({
		protocol: Type.Literal(LIFECYCLE_PROTOCOL_V1),
		...WorkerRunReportBase,
		status: Type.Literal("completed"),
		evidence: WorkerCompletedEvidenceSchema,
	}),
	Type.Object({
		protocol: Type.Literal(LIFECYCLE_PROTOCOL_V1),
		...WorkerRunReportBase,
		status: Type.Literal("failed"),
		evidence: WorkerFailedEvidenceSchema,
	}),
]);

export const WorkerRunReportV2Schema = Type.Union([
	Type.Object({ protocol: Type.Literal(LIFECYCLE_PROTOCOL_V2), ...WorkerRunReportBase, status: Type.Literal("started"), evidence: WorkerReadyEvidenceSchema }),
	Type.Object({ protocol: Type.Literal(LIFECYCLE_PROTOCOL_V2), ...WorkerRunReportBase, status: Type.Literal("message"), evidence: WorkerMessageEvidenceSchema }),
	Type.Object({ protocol: Type.Literal(LIFECYCLE_PROTOCOL_V2), ...WorkerRunReportBase, status: Type.Literal("completed"), evidence: WorkerCompletedEvidenceV2Schema }),
	Type.Object({ protocol: Type.Literal(LIFECYCLE_PROTOCOL_V2), ...WorkerRunReportBase, status: Type.Literal("failed"), evidence: WorkerFailedEvidenceSchema }),
]);
export const WorkerRunReportSchema = Type.Union([WorkerRunReportV1Schema, WorkerRunReportV2Schema]);

export const WorkerRunReportInputV1Schema = Type.Union([
	Type.Object({ status: Type.Literal("message"), message: bounded(LIFECYCLE_LIMITS.message) }),
	Type.Object({ status: Type.Literal("completed"), result: Type.Optional(bounded(LIFECYCLE_LIMITS.result)) }),
	Type.Object({ status: Type.Literal("failed"), error: bounded(LIFECYCLE_LIMITS.error) }),
]);
export const WorkerRunReportInputV2Schema = Type.Union([
	Type.Object({ status: Type.Literal("message"), message: bounded(LIFECYCLE_LIMITS.message) }),
	Type.Object({
		status: Type.Literal("completed"),
		result: bounded(LIFECYCLE_LIMITS.result),
		artifacts: Type.Optional(Type.Array(ArtifactReferenceSchema, { maxItems: LIFECYCLE_LIMITS.completionItems })),
		checks: Type.Optional(Type.Array(VerificationCheckSchema, { maxItems: LIFECYCLE_LIMITS.completionItems })),
	}),
	Type.Object({ status: Type.Literal("failed"), error: bounded(LIFECYCLE_LIMITS.error) }),
]);
export const WorkerRunReportInputSchema = WorkerRunReportInputV2Schema;

export type WorkerRunBinding = Static<typeof WorkerRunBindingSchema>;
export type WorkerRunReport = Static<typeof WorkerRunReportSchema>;
export type WorkerRunReportInput = Static<typeof WorkerRunReportInputSchema>;
export type WorkerRunReportInputV1 = Static<typeof WorkerRunReportInputV1Schema>;
export type WorkerRunReportInputV2 = Static<typeof WorkerRunReportInputV2Schema>;

const WorkerIdentitySchema = Type.Object({
	name: bounded(LIFECYCLE_LIMITS.workerName),
	paneId: Type.Optional(bounded(LIFECYCLE_LIMITS.paneId)),
});
const CandidateBase = {
	eventId: LifecycleEventIdSchema,
	runId: LifecycleRunIdSchema,
	sourceInstanceId: LifecycleSourceInstanceIdSchema,
	sourceSequence: Type.Optional(Type.Integer({ minimum: 1 })),
	worker: WorkerIdentitySchema,
	observedAt: Type.Integer({ minimum: 0 }),
	correlationId: Type.Optional(LifecycleCorrelationIdSchema),
};

const commonCandidateVariants = (protocol: typeof LIFECYCLE_PROTOCOL_V1 | typeof LIFECYCLE_PROTOCOL_V2) => [
	Type.Object({ protocol: Type.Literal(protocol), ...CandidateBase, source: Type.Literal("provider"), status: Type.Literal("started"), evidence: ProviderStartedEvidenceSchema }),
	Type.Object({ protocol: Type.Literal(protocol), ...CandidateBase, source: Type.Literal("worker"), status: Type.Literal("started"), evidence: WorkerReadyEvidenceSchema }),
	Type.Object({ protocol: Type.Literal(protocol), ...CandidateBase, source: Type.Literal("reconciler"), status: Type.Literal("started"), evidence: ReconciledStartedEvidenceSchema }),
	Type.Object({ protocol: Type.Literal(protocol), ...CandidateBase, source: Type.Literal("worker"), status: Type.Literal("message"), evidence: WorkerMessageEvidenceSchema }),
	Type.Object({ protocol: Type.Literal(protocol), ...CandidateBase, source: Type.Literal("worker"), status: Type.Literal("failed"), evidence: WorkerFailedEvidenceSchema }),
	Type.Object({ protocol: Type.Literal(protocol), ...CandidateBase, source: Type.Literal("reconciler"), status: Type.Literal("failed"), evidence: ReconciledFailureEvidenceSchema }),
	Type.Object({ protocol: Type.Literal(protocol), ...CandidateBase, source: Type.Union([Type.Literal("controller"), Type.Literal("reconciler")]), status: Type.Literal("stopped"), evidence: StopAcknowledgedEvidenceSchema }),
	Type.Object({ protocol: Type.Literal(protocol), ...CandidateBase, source: SourceSchema, status: Type.Literal("uncertain"), evidence: UncertainEvidenceSchema }),
] as const;

const CandidateVariants = [
	...commonCandidateVariants(LIFECYCLE_PROTOCOL_V1),
	Type.Object({ protocol: Type.Literal(LIFECYCLE_PROTOCOL_V1), ...CandidateBase, source: Type.Union([Type.Literal("worker"), Type.Literal("controller")]), status: Type.Literal("completed"), evidence: WorkerCompletedEvidenceSchema }),
	...commonCandidateVariants(LIFECYCLE_PROTOCOL_V2),
	Type.Object({ protocol: Type.Literal(LIFECYCLE_PROTOCOL_V2), ...CandidateBase, source: Type.Literal("worker"), status: Type.Literal("completed"), evidence: WorkerCompletedEvidenceV2Schema }),
] as const;

export const LifecycleCandidateSchema = Type.Union([...CandidateVariants]);
export const AcceptedLifecycleEventSchema = Type.Intersect([
	LifecycleCandidateSchema,
	Type.Object({ acceptedSequence: Type.Integer({ minimum: 1 }) }),
]);

export type LifecycleCandidate = Static<typeof LifecycleCandidateSchema>;
export type AcceptedLifecycleEvent = Static<typeof AcceptedLifecycleEventSchema>;

function withinUtf8Limit(value: string, limit: number): boolean {
	return Buffer.byteLength(value, "utf8") <= limit;
}

function evidenceWithinUtf8Limits(evidence: WorkerLifecycleEvidence): boolean {
	switch (evidence.kind) {
		case "worker_message": return withinUtf8Limit(evidence.message, LIFECYCLE_LIMITS.message);
		case "worker_completed": return evidence.result === undefined || withinUtf8Limit(evidence.result, LIFECYCLE_LIMITS.result);
		case "worker_completed_v2": return completionEvidenceWithinUtf8Limits(evidence);
		case "worker_failed": return withinUtf8Limit(evidence.error, LIFECYCLE_LIMITS.error);
		case "reconciled_failure":
		case "reconciled_started":
		case "uncertain": return withinUtf8Limit(evidence.detail, LIFECYCLE_LIMITS.detail);
		default: return true;
	}
}

function completionEvidenceWithinUtf8Limits(evidence: Static<typeof WorkerCompletedEvidenceV2Schema>): boolean {
	return withinUtf8Limit(evidence.result, LIFECYCLE_LIMITS.result)
		&& (evidence.artifacts ?? []).every((artifact) => withinUtf8Limit(artifact.path, LIFECYCLE_LIMITS.artifactPath)
			&& (artifact.description === undefined || withinUtf8Limit(artifact.description, LIFECYCLE_LIMITS.artifactDescription)))
		&& (evidence.checks ?? []).every((check) => withinUtf8Limit(check.command, LIFECYCLE_LIMITS.checkCommand)
			&& withinUtf8Limit(check.outcome, LIFECYCLE_LIMITS.checkOutcome));
}

function checkLifecycle<T>(schema: TSchema, value: unknown): value is T {
	if (!Check(schema, value)) return false;
	return evidenceWithinUtf8Limits((value as { evidence: WorkerLifecycleEvidence }).evidence);
}

export function isLifecycleCandidate(value: unknown): value is LifecycleCandidate {
	return checkLifecycle(LifecycleCandidateSchema, value);
}

export function isAcceptedLifecycleEvent(value: unknown): value is AcceptedLifecycleEvent {
	return checkLifecycle(AcceptedLifecycleEventSchema, value);
}

export function isWorkerRunBinding(value: unknown): value is WorkerRunBinding {
	return Check(WorkerRunBindingSchema, value);
}

export function isWorkerRunReport(value: unknown): value is WorkerRunReport {
	return checkLifecycle(WorkerRunReportSchema, value);
}

export function isWorkerRunReportInput(value: unknown, protocol: LifecycleProtocol = LIFECYCLE_PROTOCOL_V2): value is WorkerRunReportInputV1 | WorkerRunReportInputV2 {
	const schema = protocol === LIFECYCLE_PROTOCOL_V2 ? WorkerRunReportInputV2Schema : WorkerRunReportInputV1Schema;
	if (!Check(schema, value)) return false;
	const input = value as WorkerRunReportInputV1 | WorkerRunReportInputV2;
	switch (input.status) {
		case "message": return withinUtf8Limit(input.message, LIFECYCLE_LIMITS.message);
		case "completed": {
			if (protocol === LIFECYCLE_PROTOCOL_V1) return input.result === undefined || withinUtf8Limit(input.result, LIFECYCLE_LIMITS.result);
			return completionEvidenceWithinUtf8Limits({ kind: "worker_completed_v2", ...(input as WorkerRunReportInputV2 & { status: "completed" }) });
		}
		case "failed": return withinUtf8Limit(input.error, LIFECYCLE_LIMITS.error);
	}
}

export function lifecycleChannel(status: LifecycleStatus): (typeof LIFECYCLE_CHANNELS)[LifecycleStatus] {
	return LIFECYCLE_CHANNELS[status];
}
