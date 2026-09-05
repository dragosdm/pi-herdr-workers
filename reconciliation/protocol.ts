import { Buffer } from "node:buffer";
import { Type, type Static, type TSchema } from "typebox";
import { Check } from "typebox/value";
import {
	AcceptedLifecycleEventSchema,
	ArtifactReferenceSchema,
	LIFECYCLE_LIMITS,
	LifecycleRunIdSchema,
	ReconciliationObservationSchema,
	VerificationCheckSchema,
	type AcceptedLifecycleEvent,
} from "../lifecycle/protocol.js";

export const RECONCILIATION_PROTOCOL_V1 = 1 as const;
export const RECONCILIATION_SUPPORTED_PROTOCOLS = [RECONCILIATION_PROTOCOL_V1] as const;

export const RECONCILIATION_CHANNELS = {
	probe: "herdr-workers:reconciliation:rpc:probe",
	reconcile: "herdr-workers:reconciliation:rpc:reconcile",
} as const;

export type ReconciliationOperation = keyof typeof RECONCILIATION_CHANNELS;
export type ReconciliationRequestChannel = (typeof RECONCILIATION_CHANNELS)[ReconciliationOperation];

export const RECONCILIATION_ERROR_CODES = [
	"INVALID_REQUEST",
	"UNSUPPORTED_PROTOCOL",
	"PROVIDER_UNAVAILABLE",
	"NOT_FOUND",
	"NOT_UNCERTAIN",
	"STALE_ACCEPTED_SEQUENCE",
	"ENDPOINT_MISMATCH",
	"UNSUPPORTED_LIFECYCLE_PROTOCOL",
	"INTERNAL_ERROR",
] as const;
export type ReconciliationErrorCode = (typeof RECONCILIATION_ERROR_CODES)[number];

export const RECONCILIATION_AVAILABILITY_REASONS = ["SESSION_NOT_READY", "SHUTTING_DOWN"] as const;
export type ReconciliationAvailabilityReason = (typeof RECONCILIATION_AVAILABILITY_REASONS)[number];

export const RECONCILIATION_LIMITS = {
	id: LIFECYCLE_LIMITS.id,
	protocols: 8,
	sessionId: 128,
	errorMessage: 1_024,
	observations: LIFECYCLE_LIMITS.reconciliationObservations,
} as const;

const SafeIdSchema = Type.String({ minLength: 1, maxLength: RECONCILIATION_LIMITS.id, pattern: "^[A-Za-z0-9._-]+$" });
const ProtocolSchema = Type.Integer({ minimum: 1 });
const ProtocolV1Schema = Type.Literal(RECONCILIATION_PROTOCOL_V1);
const bounded = (maxLength: number) => Type.String({ minLength: 1, maxLength });

export const ReconciliationRequestIdSchema = SafeIdSchema;
export const ReconciliationProviderInstanceIdSchema = SafeIdSchema;
export { ReconciliationObservationSchema };

const ObservationsSchema = Type.Array(ReconciliationObservationSchema, {
	minItems: 1,
	maxItems: RECONCILIATION_LIMITS.observations,
});

export const ReconcileRunInputSchema = Type.Object({
	runId: LifecycleRunIdSchema,
	expectedAcceptedSequence: Type.Integer({ minimum: 1 }),
	resolution: Type.Union([
		Type.Object({
			status: Type.Literal("started"),
			detail: bounded(LIFECYCLE_LIMITS.detail),
			observations: ObservationsSchema,
		}),
		Type.Object({
			status: Type.Literal("completed"),
			result: bounded(LIFECYCLE_LIMITS.result),
			detail: bounded(LIFECYCLE_LIMITS.detail),
			observations: ObservationsSchema,
			artifacts: Type.Optional(Type.Array(ArtifactReferenceSchema, { maxItems: LIFECYCLE_LIMITS.completionItems })),
			checks: Type.Optional(Type.Array(VerificationCheckSchema, { maxItems: LIFECYCLE_LIMITS.completionItems })),
		}),
		Type.Object({
			status: Type.Literal("failed"),
			detail: bounded(LIFECYCLE_LIMITS.detail),
			observations: ObservationsSchema,
		}),
	]),
});

export const ReconciliationProbeRequestSchema = Type.Object({
	requestId: ReconciliationRequestIdSchema,
	supportedProtocols: Type.Array(ProtocolSchema, { minItems: 1, maxItems: RECONCILIATION_LIMITS.protocols, uniqueItems: true }),
});
export const ReconcileRunRequestSchema = Type.Object({
	requestId: ReconciliationRequestIdSchema,
	providerInstanceId: ReconciliationProviderInstanceIdSchema,
	protocol: ProtocolSchema,
	...ReconcileRunInputSchema.properties,
});

const AvailabilityReasonSchema = Type.Union(RECONCILIATION_AVAILABILITY_REASONS.map((reason) => Type.Literal(reason)));
export const ReconciliationProbeDataSchema = Type.Union([
	Type.Object({
		protocol: ProtocolV1Schema,
		provider: Type.Literal("herdr-reconciliation"),
		providerInstanceId: ReconciliationProviderInstanceIdSchema,
		sessionId: bounded(RECONCILIATION_LIMITS.sessionId),
		available: Type.Literal(true),
		reason: Type.Optional(Type.Never()),
		constraints: Type.Object({ sessionScoped: Type.Literal(true), requiresExactAcceptedSequence: Type.Literal(true) }),
	}),
	Type.Object({
		protocol: ProtocolV1Schema,
		provider: Type.Literal("herdr-reconciliation"),
		providerInstanceId: ReconciliationProviderInstanceIdSchema,
		sessionId: bounded(RECONCILIATION_LIMITS.sessionId),
		available: Type.Literal(false),
		reason: AvailabilityReasonSchema,
		constraints: Type.Object({ sessionScoped: Type.Literal(true), requiresExactAcceptedSequence: Type.Literal(true) }),
	}),
]);

export type ReconciliationObservation = Static<typeof ReconciliationObservationSchema>;
export type ReconcileRunInput = Static<typeof ReconcileRunInputSchema>;
export type ReconciliationProbeRequest = Static<typeof ReconciliationProbeRequestSchema>;
export type ReconcileRunRequest = Static<typeof ReconcileRunRequestSchema>;
export type ReconciliationProbeData = Static<typeof ReconciliationProbeDataSchema>;

export interface ReconciliationError { code: ReconciliationErrorCode; message: string }
export type ReconciliationReply<T = unknown> =
	| { requestId: string; protocol: 1; success: true; data: T }
	| { requestId: string; protocol: 1; success: false; error: ReconciliationError };

const ErrorSchema = Type.Object({
	code: Type.Union(RECONCILIATION_ERROR_CODES.map((code) => Type.Literal(code))),
	message: bounded(RECONCILIATION_LIMITS.errorMessage),
});
export const ReconciliationReplyEnvelopeSchema = Type.Union([
	Type.Object({ requestId: ReconciliationRequestIdSchema, protocol: ProtocolV1Schema, success: Type.Literal(true), data: Type.Unknown() }),
	Type.Object({ requestId: ReconciliationRequestIdSchema, protocol: ProtocolV1Schema, success: Type.Literal(false), error: ErrorSchema }),
]);

export const RECONCILIATION_REQUEST_SCHEMAS: Record<ReconciliationRequestChannel, TSchema> = {
	[RECONCILIATION_CHANNELS.probe]: ReconciliationProbeRequestSchema,
	[RECONCILIATION_CHANNELS.reconcile]: ReconcileRunRequestSchema,
};

function withinUtf8(value: string, limit: number): boolean {
	return Buffer.byteLength(value, "utf8") <= limit;
}

function validInputUtf8(input: ReconcileRunInput): boolean {
	const { resolution } = input;
	return withinUtf8(resolution.detail, LIFECYCLE_LIMITS.detail)
		&& resolution.observations.every((observation) => withinUtf8(observation.detail, LIFECYCLE_LIMITS.detail)
			&& (!("endpoint" in observation)
				|| (withinUtf8(observation.endpoint.agentName, LIFECYCLE_LIMITS.workerName)
					&& withinUtf8(observation.endpoint.paneId, LIFECYCLE_LIMITS.paneId))))
		&& (resolution.status !== "completed"
			|| (withinUtf8(resolution.result, LIFECYCLE_LIMITS.result)
				&& (resolution.artifacts ?? []).every((artifact) => withinUtf8(artifact.path, LIFECYCLE_LIMITS.artifactPath)
					&& (artifact.description === undefined || withinUtf8(artifact.description, LIFECYCLE_LIMITS.artifactDescription)))
				&& (resolution.checks ?? []).every((check) => withinUtf8(check.command, LIFECYCLE_LIMITS.checkCommand)
					&& withinUtf8(check.outcome, LIFECYCLE_LIMITS.checkOutcome))));
}

export function isReconcileRunInput(value: unknown): value is ReconcileRunInput {
	return Check(ReconcileRunInputSchema, value) && validInputUtf8(value as ReconcileRunInput);
}

export function isValidReconciliationRequest(channel: ReconciliationRequestChannel, value: unknown): boolean {
	if (!Check(RECONCILIATION_REQUEST_SCHEMAS[channel], value)) return false;
	return channel === RECONCILIATION_CHANNELS.probe || validInputUtf8(value as ReconcileRunRequest);
}

export function isValidReconciliationResult(operation: ReconciliationOperation, value: unknown): boolean {
	if (operation === "probe") {
		return Check(ReconciliationProbeDataSchema, value)
			&& withinUtf8((value as ReconciliationProbeData).sessionId, RECONCILIATION_LIMITS.sessionId);
	}
	return Check(AcceptedLifecycleEventSchema, value)
		&& (value as AcceptedLifecycleEvent).protocol === 2
		&& (value as AcceptedLifecycleEvent).source === "reconciler";
}

export function isReconciliationReplyEnvelope(value: unknown): value is ReconciliationReply {
	return Check(ReconciliationReplyEnvelopeSchema, value);
}

export function extractReconciliationRequestId(value: unknown): string | undefined {
	if (typeof value !== "object" || value === null) return undefined;
	const requestId = (value as { requestId?: unknown }).requestId;
	return Check(ReconciliationRequestIdSchema, requestId) ? requestId as string : undefined;
}

export function reconciliationReplyChannel(channel: ReconciliationRequestChannel, requestId: string): string {
	if (!Check(ReconciliationRequestIdSchema, requestId)) throw new TypeError("Invalid request ID.");
	return `${channel}:reply:${requestId}`;
}

export function reconciliationSuccess<T>(requestId: string, data: T): ReconciliationReply<T> {
	return { requestId, protocol: 1, success: true, data };
}

export function reconciliationFailure(requestId: string, code: ReconciliationErrorCode, message: string): ReconciliationReply<never> {
	const safeMessage = message.slice(0, RECONCILIATION_LIMITS.errorMessage) || "Request failed.";
	return { requestId, protocol: 1, success: false, error: { code, message: safeMessage } };
}

export class ReconciliationServiceError extends Error {
	constructor(
		public readonly code: Extract<ReconciliationErrorCode,
			"NOT_FOUND" | "NOT_UNCERTAIN" | "STALE_ACCEPTED_SEQUENCE" | "ENDPOINT_MISMATCH" | "UNSUPPORTED_LIFECYCLE_PROTOCOL">,
		message: string,
	) {
		super(message.slice(0, RECONCILIATION_LIMITS.errorMessage) || "Request failed.");
		this.name = "ReconciliationServiceError";
	}
}
