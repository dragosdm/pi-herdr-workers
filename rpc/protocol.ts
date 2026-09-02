import { Type, type Static, type TSchema } from "typebox";
import { Check } from "typebox/value";

export const PROTOCOL_V1 = 1 as const;
export const SUPPORTED_PROTOCOLS = [PROTOCOL_V1] as const;

export const CHANNELS = {
  probe: "herdr-workers:rpc:probe",
  spawn: "herdr-workers:rpc:spawn",
  send: "herdr-workers:rpc:send",
  inspect: "herdr-workers:rpc:inspect",
  stop: "herdr-workers:rpc:stop",
} as const;

export type RpcOperation = keyof typeof CHANNELS;
export type RequestChannel = (typeof CHANNELS)[RpcOperation];

export const CAPABILITIES = ["spawn", "send", "steer", "inspect"] as const;
export type Capability = (typeof CAPABILITIES)[number];

export const ERROR_CODES = [
  "INVALID_REQUEST",
  "UNSUPPORTED_PROTOCOL",
  "PROVIDER_UNAVAILABLE",
  "NOT_FOUND",
  "NOT_TEAM_MEMBER",
  "UNSUPPORTED_OPERATION",
  "INTERNAL_ERROR",
] as const;
export type RpcErrorCode = (typeof ERROR_CODES)[number];

export const AVAILABILITY_REASONS = [
  "SESSION_NOT_READY",
  "NOT_INTERACTIVE",
  "NOT_IN_HERDR",
  "SHUTTING_DOWN",
] as const;
export type AvailabilityReason = (typeof AVAILABILITY_REASONS)[number];

export const LIMITS = {
  id: 128,
  protocols: 8,
  workerName: 32,
  target: 128,
  model: 128,
  type: 128,
  purpose: 1024,
  cwd: 4096,
  message: 65_536,
  initialPrompt: 65_536,
  errorMessage: 1024,
} as const;
export const SPAWN_DIRECTIONS = ["right", "down", "left", "up"] as const;
export const THINKING_LEVELS = ["off", "minimal", "low", "medium", "high", "xhigh", "max"] as const;
export type SpawnDirection = (typeof SPAWN_DIRECTIONS)[number];
export type ThinkingLevel = (typeof THINKING_LEVELS)[number];

const IdSchema = Type.String({ minLength: 1, maxLength: LIMITS.id, pattern: "^[A-Za-z0-9._-]+$" });
const ProtocolSchema = Type.Integer({ minimum: 1 });
const ProtocolV1Schema = Type.Literal(PROTOCOL_V1);
const bounded = (maxLength: number) => Type.String({ minLength: 1, maxLength });

export const RequestIdSchema = IdSchema;
export const ProviderInstanceIdSchema = IdSchema;
export const ProbeRequestSchema = Type.Object({
  requestId: RequestIdSchema,
  supportedProtocols: Type.Array(ProtocolSchema, { minItems: 1, maxItems: LIMITS.protocols, uniqueItems: true }),
});
const AddressedRequestSchema = {
  requestId: RequestIdSchema,
  providerInstanceId: ProviderInstanceIdSchema,
  protocol: ProtocolSchema,
};
export const SpawnRequestSchema = Type.Object({
  ...AddressedRequestSchema,
  name: Type.Optional(bounded(LIMITS.workerName)),
  direction: Type.Optional(Type.Union(SPAWN_DIRECTIONS.map((direction) => Type.Literal(direction)))),
  model: Type.Optional(bounded(LIMITS.model)),
  thinking: Type.Optional(Type.Union(THINKING_LEVELS.map((thinking) => Type.Literal(thinking)))),
  type: Type.Optional(bounded(LIMITS.type)),
  purpose: Type.Optional(bounded(LIMITS.purpose)),
  cwd: Type.Optional(bounded(LIMITS.cwd)),
  initialPrompt: Type.Optional(bounded(LIMITS.initialPrompt)),
});
export const SendRequestSchema = Type.Object({
  ...AddressedRequestSchema,
  target: bounded(LIMITS.target),
  message: bounded(LIMITS.message),
  mode: Type.Optional(Type.Union([Type.Literal("follow-up"), Type.Literal("steer")])),
  priority: Type.Optional(Type.Boolean()),
});
export const InspectRequestSchema = Type.Object({ ...AddressedRequestSchema, target: bounded(LIMITS.target) });
export const StopRequestSchema = Type.Object({ ...AddressedRequestSchema, target: Type.Optional(bounded(LIMITS.target)) });

export type ProbeRequest = Static<typeof ProbeRequestSchema>;
export type SpawnRequest = Static<typeof SpawnRequestSchema>;
export type SendRequest = Static<typeof SendRequestSchema>;
export type InspectRequest = Static<typeof InspectRequestSchema>;
export type StopRequest = Static<typeof StopRequestSchema>;
export type AddressedRequest = SpawnRequest | SendRequest | InspectRequest | StopRequest;

export interface SpawnInput { name?: string; direction?: SpawnDirection; model?: string; thinking?: ThinkingLevel; type?: string; purpose?: string; cwd?: string; initialPrompt?: string }
export interface SendInput { target: string; message: string; mode?: "follow-up" | "steer"; priority?: boolean }
export interface InspectInput { target: string }
export interface WorkerReference { name: string; paneId: string; model?: string; cwd: string; type?: string; purpose?: string; adopted: boolean }
export interface DeliveryReceipt { target: string; paneId: string; kind?: string; status?: string; transport: "inbox" | "herdr-prompt"; requestedMode: "follow-up" | "steer"; priorityApplied: boolean }
export interface Inspection { name: string; paneId: string; kind?: string; status?: string; cwd?: string; relationship: "worker" | "orchestrator"; managedBySession: boolean }
export interface ProbeData { protocol: 1; providerInstanceId: string; available: boolean; reason?: AvailabilityReason; capabilities: Capability[] }

export interface RpcError { code: RpcErrorCode; message: string }
export type RpcReply<T = unknown> =
  | { requestId: string; protocol: 1; ok: true; data: T }
  | { requestId: string; protocol: 1; ok: false; error: RpcError };

const ErrorSchema = Type.Object({ code: Type.Union(ERROR_CODES.map((code) => Type.Literal(code))), message: bounded(LIMITS.errorMessage) });
export const ReplyEnvelopeSchema = Type.Union([
  Type.Object({ requestId: RequestIdSchema, protocol: ProtocolV1Schema, ok: Type.Literal(true), data: Type.Unknown() }),
  Type.Object({ requestId: RequestIdSchema, protocol: ProtocolV1Schema, ok: Type.Literal(false), error: ErrorSchema }),
]);

export const REQUEST_SCHEMAS: Record<RequestChannel, TSchema> = {
  [CHANNELS.probe]: ProbeRequestSchema,
  [CHANNELS.spawn]: SpawnRequestSchema,
  [CHANNELS.send]: SendRequestSchema,
  [CHANNELS.inspect]: InspectRequestSchema,
  [CHANNELS.stop]: StopRequestSchema,
};

export function isValid<T>(schema: TSchema, value: unknown): value is T { return Check(schema, value); }
export function isReplyEnvelope(value: unknown): value is RpcReply { return Check(ReplyEnvelopeSchema, value); }
export function extractUsableRequestId(value: unknown): string | undefined {
  if (typeof value !== "object" || value === null) return undefined;
  const requestId = (value as { requestId?: unknown }).requestId;
  return Check(RequestIdSchema, requestId) ? requestId as string : undefined;
}
export function replyChannel(channel: RequestChannel, requestId: string): string {
  if (!Check(RequestIdSchema, requestId)) throw new TypeError("Invalid request ID.");
  return `${channel}:reply:${requestId}`;
}
export function success<T>(requestId: string, data: T): RpcReply<T> { return { requestId, protocol: 1, ok: true, data }; }
export function failure(requestId: string, code: RpcErrorCode, message: string): RpcReply<never> {
  const safeMessage = message.slice(0, LIMITS.errorMessage) || "Request failed.";
  return { requestId, protocol: 1, ok: false, error: { code, message: safeMessage } };
}

export class WorkerRpcServiceError extends Error {
  constructor(public readonly code: Extract<RpcErrorCode, "NOT_FOUND" | "NOT_TEAM_MEMBER">, message: string) {
    super(message.slice(0, LIMITS.errorMessage) || "Request failed.");
    this.name = "WorkerRpcServiceError";
  }
}
