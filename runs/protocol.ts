import { Buffer } from "node:buffer";
import { Type, type Static, type TSchema } from "typebox";
import { Check } from "typebox/value";
import { AcceptedLifecycleEventSchema, isAcceptedLifecycleEvent, type AcceptedLifecycleEvent } from "../lifecycle/protocol.js";

export const RUN_QUERY_PROTOCOL_V1 = 1 as const;
export const RUN_QUERY_SUPPORTED_PROTOCOLS = [RUN_QUERY_PROTOCOL_V1] as const;

export const RUN_QUERY_CHANNELS = {
	probe: "herdr-workers:runs:rpc:probe",
	get: "herdr-workers:runs:rpc:get",
	list: "herdr-workers:runs:rpc:list",
	replay: "herdr-workers:runs:rpc:replay",
} as const;

export type RunQueryOperation = keyof typeof RUN_QUERY_CHANNELS;
export type RunQueryRequestChannel = (typeof RUN_QUERY_CHANNELS)[RunQueryOperation];

export const RUN_QUERY_ERROR_CODES = [
	"INVALID_REQUEST",
	"UNSUPPORTED_PROTOCOL",
	"PROVIDER_UNAVAILABLE",
	"NOT_FOUND",
	"INTERNAL_ERROR",
] as const;
export type RunQueryErrorCode = (typeof RUN_QUERY_ERROR_CODES)[number];

export const RUN_QUERY_AVAILABILITY_REASONS = ["SESSION_NOT_READY", "SHUTTING_DOWN"] as const;
export type RunQueryAvailabilityReason = (typeof RUN_QUERY_AVAILABILITY_REASONS)[number];

export const RUN_QUERY_LIMITS = {
	id: 128,
	protocols: 8,
	sessionId: 128,
	cwd: 4096,
	model: 128,
	role: 128,
	agentName: 32,
	paneId: 128,
	herdrStatus: 128,
	cursor: 256,
	defaultPageSize: 50,
	maxPageSize: 100,
	errorMessage: 1024,
} as const;

const SafeIdSchema = Type.String({ minLength: 1, maxLength: RUN_QUERY_LIMITS.id, pattern: "^[A-Za-z0-9._-]+$" });
const ProtocolSchema = Type.Integer({ minimum: 1 });
const ProtocolV1Schema = Type.Literal(RUN_QUERY_PROTOCOL_V1);
const bounded = (maxLength: number) => Type.String({ minLength: 1, maxLength });

export const RunQueryRequestIdSchema = SafeIdSchema;
export const RunQueryProviderInstanceIdSchema = SafeIdSchema;
export const RunQueryRunIdSchema = SafeIdSchema;
export const RunQueryCursorSchema = Type.String({
	minLength: 1,
	maxLength: RUN_QUERY_LIMITS.cursor,
	pattern: "^[A-Za-z0-9_-]+$",
});

export const RunQueryProbeRequestSchema = Type.Object({
	requestId: RunQueryRequestIdSchema,
	supportedProtocols: Type.Array(ProtocolSchema, {
		minItems: 1,
		maxItems: RUN_QUERY_LIMITS.protocols,
		uniqueItems: true,
	}),
});
const AddressedRequestProperties = {
	requestId: RunQueryRequestIdSchema,
	providerInstanceId: RunQueryProviderInstanceIdSchema,
	protocol: ProtocolSchema,
};
export const GetRunRequestSchema = Type.Object({
	...AddressedRequestProperties,
	runId: RunQueryRunIdSchema,
	includeEndpointObservation: Type.Optional(Type.Boolean()),
});
export const ListRunsRequestSchema = Type.Object({
	...AddressedRequestProperties,
	cursor: Type.Optional(RunQueryCursorSchema),
	limit: Type.Optional(Type.Integer({ minimum: 1, maximum: RUN_QUERY_LIMITS.maxPageSize })),
});
export const ReplayRunRequestSchema = Type.Object({
	...AddressedRequestProperties,
	runId: RunQueryRunIdSchema,
	afterAcceptedSequence: Type.Integer({ minimum: 0 }),
	limit: Type.Optional(Type.Integer({ minimum: 1, maximum: RUN_QUERY_LIMITS.maxPageSize })),
});

export type RunQueryProbeRequest = Static<typeof RunQueryProbeRequestSchema>;
export type GetRunRequest = Static<typeof GetRunRequestSchema>;
export type ListRunsRequest = Static<typeof ListRunsRequestSchema>;
export type ReplayRunRequest = Static<typeof ReplayRunRequestSchema>;
export type RunQueryAddressedRequest = GetRunRequest | ListRunsRequest | ReplayRunRequest;
export interface GetRunInput { runId: string; includeEndpointObservation?: boolean }
export interface ListRunsInput { cursor?: string; limit?: number }
export interface ReplayRunInput { runId: string; afterAcceptedSequence: number; limit?: number }

const AvailabilityReasonSchema = Type.Union(RUN_QUERY_AVAILABILITY_REASONS.map((reason) => Type.Literal(reason)));
const ProbeDataProperties = {
	protocol: ProtocolV1Schema,
	provider: Type.Literal("herdr-runs"),
	providerInstanceId: RunQueryProviderInstanceIdSchema,
	sessionId: bounded(RUN_QUERY_LIMITS.sessionId),
	constraints: Type.Object({
		sessionScoped: Type.Literal(true),
		requiresHerdrPane: Type.Literal(false),
		requiresInteractivePi: Type.Literal(false),
	}),
};
export const RunQueryProbeDataSchema = Type.Union([
	Type.Object({ ...ProbeDataProperties, available: Type.Literal(true), reason: Type.Optional(Type.Never()) }),
	Type.Object({ ...ProbeDataProperties, available: Type.Literal(false), reason: AvailabilityReasonSchema }),
]);

const WorkerRunStatusSchema = Type.Union([
	Type.Literal("registered"),
	Type.Literal("started"),
	Type.Literal("completed"),
	Type.Literal("failed"),
	Type.Literal("uncertain"),
]);
const ReadinessSchema = Type.Union([Type.Literal("unconfirmed"), Type.Literal("confirmed")]);
const lifecycleSchema = (minimum: number) => Type.Object({
	status: WorkerRunStatusSchema,
	acceptedSequence: Type.Integer({ minimum }),
	readiness: Type.Optional(ReadinessSchema),
});

export const WorkerRunHandleV1Schema = Type.Object({
	protocol: ProtocolV1Schema,
	runId: RunQueryRunIdSchema,
	correlationId: Type.Optional(SafeIdSchema),
	requestId: Type.Optional(SafeIdSchema),
	sessionId: bounded(RUN_QUERY_LIMITS.sessionId),
	registeredAt: Type.Integer({ minimum: 0 }),
	lifecycle: lifecycleSchema(0),
	assignment: Type.Object({
		cwd: bounded(RUN_QUERY_LIMITS.cwd),
		model: Type.Optional(bounded(RUN_QUERY_LIMITS.model)),
		role: Type.Optional(bounded(RUN_QUERY_LIMITS.role)),
	}),
	endpoint: Type.Optional(Type.Object({
		agentName: bounded(RUN_QUERY_LIMITS.agentName),
		paneId: bounded(RUN_QUERY_LIMITS.paneId),
		observedAt: Type.Integer({ minimum: 0 }),
		herdrStatus: Type.Optional(bounded(RUN_QUERY_LIMITS.herdrStatus)),
	})),
});
export const LegacyWorkerRunProjectionV1Schema = Type.Object({
	protocol: ProtocolV1Schema,
	legacy: Type.Literal(true),
	runId: RunQueryRunIdSchema,
	correlationId: Type.Optional(SafeIdSchema),
	sessionId: bounded(RUN_QUERY_LIMITS.sessionId),
	lifecycle: lifecycleSchema(1),
	worker: Type.Object({
		agentName: bounded(RUN_QUERY_LIMITS.agentName),
		paneId: Type.Optional(bounded(RUN_QUERY_LIMITS.paneId)),
	}),
});
export const WorkerRunRecordV1Schema = Type.Union([WorkerRunHandleV1Schema, LegacyWorkerRunProjectionV1Schema]);
export const ListRunsResultSchema = Type.Object({
	runs: Type.Array(WorkerRunRecordV1Schema, { maxItems: RUN_QUERY_LIMITS.maxPageSize }),
	nextCursor: Type.Optional(RunQueryCursorSchema),
});
export const ReplayRunResultSchema = Type.Object({
	events: Type.Array(AcceptedLifecycleEventSchema, { maxItems: RUN_QUERY_LIMITS.maxPageSize }),
	hasMore: Type.Boolean(),
});

export type RunQueryProbeData = Static<typeof RunQueryProbeDataSchema>;
export type WorkerRunStatus = Static<typeof WorkerRunStatusSchema>;
export type WorkerRunHandleV1 = Static<typeof WorkerRunHandleV1Schema>;
export type LegacyWorkerRunProjectionV1 = Static<typeof LegacyWorkerRunProjectionV1Schema>;
export type WorkerRunRecordV1 = Static<typeof WorkerRunRecordV1Schema>;
export type ListRunsResult = Static<typeof ListRunsResultSchema>;
export interface ReplayRunResult { events: AcceptedLifecycleEvent[]; hasMore: boolean }

export const RUN_QUERY_RESULT_SCHEMAS = {
	probe: RunQueryProbeDataSchema,
	get: WorkerRunRecordV1Schema,
	list: ListRunsResultSchema,
	replay: ReplayRunResultSchema,
} as const;

export interface RunQueryError { code: RunQueryErrorCode; message: string }
export type RunQueryReply<T = unknown> =
	| { requestId: string; protocol: 1; success: true; data: T }
	| { requestId: string; protocol: 1; success: false; error: RunQueryError };

const ErrorSchema = Type.Object({
	code: Type.Union(RUN_QUERY_ERROR_CODES.map((code) => Type.Literal(code))),
	message: bounded(RUN_QUERY_LIMITS.errorMessage),
});
export const RunQueryReplyEnvelopeSchema = Type.Union([
	Type.Object({ requestId: RunQueryRequestIdSchema, protocol: ProtocolV1Schema, success: Type.Literal(true), data: Type.Unknown() }),
	Type.Object({ requestId: RunQueryRequestIdSchema, protocol: ProtocolV1Schema, success: Type.Literal(false), error: ErrorSchema }),
]);

export const RUN_QUERY_REQUEST_SCHEMAS: Record<RunQueryRequestChannel, TSchema> = {
	[RUN_QUERY_CHANNELS.probe]: RunQueryProbeRequestSchema,
	[RUN_QUERY_CHANNELS.get]: GetRunRequestSchema,
	[RUN_QUERY_CHANNELS.list]: ListRunsRequestSchema,
	[RUN_QUERY_CHANNELS.replay]: ReplayRunRequestSchema,
};

export function encodeRunQueryCursor(runId: string): string {
	if (!Check(RunQueryRunIdSchema, runId)) throw new TypeError("Invalid run ID.");
	return Buffer.from(runId, "utf8").toString("base64url");
}

export function decodeRunQueryCursor(cursor: string): string | undefined {
	if (!Check(RunQueryCursorSchema, cursor)) return undefined;
	try {
		const runId = Buffer.from(cursor, "base64url").toString("utf8");
		if (!Check(RunQueryRunIdSchema, runId) || encodeRunQueryCursor(runId) !== cursor) return undefined;
		return runId;
	} catch {
		return undefined;
	}
}

export function isValidRunQueryRequest(channel: RunQueryRequestChannel, value: unknown): boolean {
	if (!Check(RUN_QUERY_REQUEST_SCHEMAS[channel], value)) return false;
	if (channel === RUN_QUERY_CHANNELS.list) {
		const cursor = (value as ListRunsRequest).cursor;
		return cursor === undefined || decodeRunQueryCursor(cursor) !== undefined;
	}
	return true;
}

function withinUtf8Limit(value: string | undefined, limit: number): boolean {
	return value === undefined || Buffer.byteLength(value, "utf8") <= limit;
}

export function isValidRunQueryRecord(value: unknown): value is WorkerRunRecordV1 {
	if (!Check(WorkerRunRecordV1Schema, value)) return false;
	const record = value as WorkerRunRecordV1;
	if (!withinUtf8Limit(record.sessionId, RUN_QUERY_LIMITS.sessionId)) return false;
	if ("legacy" in record) {
		return withinUtf8Limit(record.worker.agentName, RUN_QUERY_LIMITS.agentName)
			&& withinUtf8Limit(record.worker.paneId, RUN_QUERY_LIMITS.paneId);
	}
	return withinUtf8Limit(record.assignment.cwd, RUN_QUERY_LIMITS.cwd)
		&& withinUtf8Limit(record.assignment.model, RUN_QUERY_LIMITS.model)
		&& withinUtf8Limit(record.assignment.role, RUN_QUERY_LIMITS.role)
		&& (record.endpoint === undefined
			|| (withinUtf8Limit(record.endpoint.agentName, RUN_QUERY_LIMITS.agentName)
				&& withinUtf8Limit(record.endpoint.paneId, RUN_QUERY_LIMITS.paneId)
				&& withinUtf8Limit(record.endpoint.herdrStatus, RUN_QUERY_LIMITS.herdrStatus)));
}

export function isValidReplayRunResult(value: unknown, input?: ReplayRunInput): value is ReplayRunResult {
	if (!Check(ReplayRunResultSchema, value)) return false;
	const events = (value as ReplayRunResult).events;
	let runId: string | undefined;
	let acceptedSequence = input?.afterAcceptedSequence ?? 0;
	for (const event of events) {
		if (!isAcceptedLifecycleEvent(event)) return false;
		runId ??= event.runId;
		if (event.runId !== runId || (input && event.runId !== input.runId) || event.acceptedSequence <= acceptedSequence) return false;
		acceptedSequence = event.acceptedSequence;
	}
	const limit = input?.limit ?? RUN_QUERY_LIMITS.defaultPageSize;
	return events.length <= limit;
}

export function isValidRunQueryResult(operation: RunQueryOperation, value: unknown): boolean {
	if (!Check(RUN_QUERY_RESULT_SCHEMAS[operation], value)) return false;
	if (operation === "probe") {
		return withinUtf8Limit((value as RunQueryProbeData).sessionId, RUN_QUERY_LIMITS.sessionId);
	}
	if (operation === "get") return isValidRunQueryRecord(value);
	if (operation === "list") {
		const result = value as ListRunsResult;
		return result.runs.every(isValidRunQueryRecord)
			&& (result.nextCursor === undefined || decodeRunQueryCursor(result.nextCursor) !== undefined);
	}
	return isValidReplayRunResult(value);
}

export function isRunQueryReplyEnvelope(value: unknown): value is RunQueryReply {
	return Check(RunQueryReplyEnvelopeSchema, value);
}

export function extractRunQueryRequestId(value: unknown): string | undefined {
	if (typeof value !== "object" || value === null) return undefined;
	const requestId = (value as { requestId?: unknown }).requestId;
	return Check(RunQueryRequestIdSchema, requestId) ? requestId as string : undefined;
}

export function runQueryReplyChannel(channel: RunQueryRequestChannel, requestId: string): string {
	if (!Check(RunQueryRequestIdSchema, requestId)) throw new TypeError("Invalid request ID.");
	return `${channel}:reply:${requestId}`;
}

export function runQuerySuccess<T>(requestId: string, data: T): RunQueryReply<T> {
	return { requestId, protocol: 1, success: true, data };
}

export function runQueryFailure(requestId: string, code: RunQueryErrorCode, message: string): RunQueryReply<never> {
	const safeMessage = message.slice(0, RUN_QUERY_LIMITS.errorMessage) || "Request failed.";
	return { requestId, protocol: 1, success: false, error: { code, message: safeMessage } };
}

export class RunQueryServiceError extends Error {
	constructor(public readonly code: Extract<RunQueryErrorCode, "NOT_FOUND">, message: string) {
		super(message.slice(0, RUN_QUERY_LIMITS.errorMessage) || "Request failed.");
		this.name = "RunQueryServiceError";
	}
}
