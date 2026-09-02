import { randomUUID } from "node:crypto";
import type { RpcEventBus } from "./client.js";
import {
  CAPABILITIES,
  CHANNELS,
  PROTOCOL_V1,
  REQUEST_SCHEMAS,
  failure,
  success,
  extractUsableRequestId,
  isValid,
  replyChannel,
  type AvailabilityReason,
  type InspectInput,
  type Inspection,
  type ProbeRequest,
  type RequestChannel,
  type SendInput,
  type DeliveryReceipt,
  type SpawnInput,
  type WorkerReference,
  WorkerRpcServiceError,
} from "./protocol.js";

export interface WorkerRpcService {
  spawn(input: SpawnInput, signal?: AbortSignal): Promise<WorkerReference>;
  send(input: SendInput, signal?: AbortSignal): Promise<DeliveryReceipt>;
  inspect(input: InspectInput, signal?: AbortSignal): Promise<Inspection>;
}
export interface ProviderState { available: boolean; reason?: AvailabilityReason }
export interface WorkerRpcServerOptions {
  events: RpcEventBus;
  service: WorkerRpcService;
  getProviderState: () => ProviderState;
  createInstanceId?: () => string;
}
export interface WorkerRpcServer { providerInstanceId: string; dispose(): void }

const FIXED_MESSAGES = {
  INVALID_REQUEST: "Request is invalid.",
  UNSUPPORTED_PROTOCOL: "Protocol is not supported.",
  PROVIDER_UNAVAILABLE: "Herdr worker provider is unavailable.",
  UNSUPPORTED_OPERATION: "Stop is not supported.",
  INTERNAL_ERROR: "The worker operation failed.",
} as const;

export function registerWorkerRpcServer(options: WorkerRpcServerOptions): WorkerRpcServer {
  const providerInstanceId = options.createInstanceId?.() ?? randomUUID();
  const unsubscribers: Array<() => void> = [];
  let disposed = false;

  const emit = (channel: RequestChannel, requestId: string, envelope: unknown) => {
    options.events.emit(replyChannel(channel, requestId), envelope);
  };
  const handle = async (channel: RequestChannel, payload: unknown) => {
    const requestId = extractUsableRequestId(payload);
    if (!requestId) return;
    if (!isValid(REQUEST_SCHEMAS[channel], payload)) {
      emit(channel, requestId, failure(requestId, "INVALID_REQUEST", FIXED_MESSAGES.INVALID_REQUEST));
      return;
    }
    if (channel === CHANNELS.probe) {
      const request = payload as ProbeRequest;
      const protocol = [...request.supportedProtocols].sort((a, b) => b - a).find((item) => item === PROTOCOL_V1);
      if (!protocol) {
        emit(channel, requestId, failure(requestId, "UNSUPPORTED_PROTOCOL", FIXED_MESSAGES.UNSUPPORTED_PROTOCOL));
        return;
      }
      const state = options.getProviderState();
      emit(channel, requestId, success(requestId, {
        protocol,
        providerInstanceId,
        available: state.available,
        ...(state.available ? {} : { reason: state.reason ?? "SESSION_NOT_READY" }),
        capabilities: [...CAPABILITIES],
      }));
      return;
    }
    const request = payload as Record<string, unknown> & { providerInstanceId: string; protocol: number };
    if (request.providerInstanceId !== providerInstanceId) return;
    if (request.protocol !== PROTOCOL_V1) {
      emit(channel, requestId, failure(requestId, "UNSUPPORTED_PROTOCOL", FIXED_MESSAGES.UNSUPPORTED_PROTOCOL));
      return;
    }
    if (channel === CHANNELS.stop) {
      emit(channel, requestId, failure(requestId, "UNSUPPORTED_OPERATION", FIXED_MESSAGES.UNSUPPORTED_OPERATION));
      return;
    }
    if (!options.getProviderState().available) {
      emit(channel, requestId, failure(requestId, "PROVIDER_UNAVAILABLE", FIXED_MESSAGES.PROVIDER_UNAVAILABLE));
      return;
    }
    try {
      let data: unknown;
      if (channel === CHANNELS.spawn) {
        const input = request as unknown as SpawnInput;
        data = await options.service.spawn({
          ...(input.name === undefined ? {} : { name: input.name }),
          ...(input.direction === undefined ? {} : { direction: input.direction }),
          ...(input.model === undefined ? {} : { model: input.model }),
          ...(input.thinking === undefined ? {} : { thinking: input.thinking }),
          ...(input.type === undefined ? {} : { type: input.type }),
          ...(input.purpose === undefined ? {} : { purpose: input.purpose }),
          ...(input.cwd === undefined ? {} : { cwd: input.cwd }),
          ...(input.initialPrompt === undefined ? {} : { initialPrompt: input.initialPrompt }),
        });
      } else if (channel === CHANNELS.send) {
        const { requestId: _, providerInstanceId: __, protocol: ___, ...input } = request;
        data = await options.service.send(input as unknown as SendInput);
      } else {
        const { requestId: _, providerInstanceId: __, protocol: ___, ...input } = request;
        data = await options.service.inspect(input as unknown as InspectInput);
      }
      emit(channel, requestId, success(requestId, data));
    } catch (error) {
      if (error instanceof WorkerRpcServiceError) {
        emit(channel, requestId, failure(requestId, error.code, error.message));
      } else {
        emit(channel, requestId, failure(requestId, "INTERNAL_ERROR", FIXED_MESSAGES.INTERNAL_ERROR));
      }
    }
  };

  for (const channel of Object.values(CHANNELS)) {
    unsubscribers.push(options.events.on(channel, (payload) => { void handle(channel, payload); }));
  }
  return {
    providerInstanceId,
    dispose() {
      if (disposed) return;
      disposed = true;
      for (const unsubscribe of unsubscribers.splice(0)) unsubscribe();
    },
  };
}
