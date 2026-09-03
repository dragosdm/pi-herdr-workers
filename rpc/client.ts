import { randomUUID } from "node:crypto";
import type { TSchema } from "typebox";
import {
  CHANNELS,
  LIMITS,
  PROTOCOL_V1,
  RESULT_SCHEMAS,
  type AddressedRequest,
  type InspectInput,
  type Inspection,
  type ProbeData,
  type RequestChannel,
  type RpcReply,
  type SendInput,
  type DeliveryReceipt,
  type SpawnInput,
  type WorkerReference,
  isReplyEnvelope,
  isValid,
  replyChannel,
} from "./protocol.js";

export interface RpcEventBus {
  on(channel: string, listener: (payload: unknown) => void): () => void;
  emit(channel: string, payload: unknown): void;
}

export interface CallOptions { timeoutMs?: number; signal?: AbortSignal }
export interface WorkerRpcClientOptions { events: RpcEventBus; createRequestId?: () => string }

const DEFAULT_TIMEOUTS: Record<keyof typeof CHANNELS, number> = {
  probe: 2_000,
  spawn: 120_000,
  send: 20_000,
  inspect: 20_000,
  stop: 20_000,
};

export class RpcTimeoutError extends Error {
  constructor() { super("RPC request timed out."); this.name = "RpcTimeoutError"; }
}
export class RpcAbortError extends Error {
  constructor() { super("RPC request was aborted."); this.name = "RpcAbortError"; }
}
export class RpcResponseError extends Error {
  constructor(readonly code: string, message: string) { super(message); this.name = "RpcResponseError"; }
}
export class RpcProtocolError extends Error {
  readonly code = "INVALID_RESPONSE";
  constructor() {
    super("The worker provider returned an invalid response.");
    this.name = "RpcProtocolError";
  }
}

function timeoutFor(operation: keyof typeof CHANNELS, requested?: number): number {
  if (requested === undefined) return DEFAULT_TIMEOUTS[operation];
  if (!Number.isFinite(requested) || requested <= 0 || requested > 300_000) {
    throw new RangeError("timeoutMs must be finite and between 1 and 300000.");
  }
  return requested;
}

export class WorkerRpcClient {
  private readonly events: RpcEventBus;
  private readonly createRequestId: () => string;

  constructor(options: WorkerRpcClientOptions) {
    this.events = options.events;
    this.createRequestId = options.createRequestId ?? randomUUID;
  }

  probe(options: CallOptions = {}): Promise<ProbeData> {
    const requestId = this.createRequestId();
    const timeoutMs = timeoutFor("probe", options.timeoutMs);
    return new Promise((resolve, reject) => {
      let settled = false;
      let fallback: ProbeData | undefined;
      let unsupportedProtocol: RpcResponseError | undefined;
      let malformedSuccess = false;
      let timer: ReturnType<typeof setTimeout> | undefined;
      let unsubscribe = () => {};
      const cleanup = () => {
        unsubscribe();
        if (timer !== undefined) clearTimeout(timer);
        options.signal?.removeEventListener("abort", onAbort);
      };
      const finish = (action: () => void) => {
        if (settled) return;
        settled = true;
        cleanup();
        action();
      };
      const onAbort = () => finish(() => reject(new RpcAbortError()));
      if (options.signal?.aborted) return onAbort();
      unsubscribe = this.events.on(replyChannel(CHANNELS.probe, requestId), (payload) => {
        if (!isReplyEnvelope(payload) || payload.requestId !== requestId) return;
        if (!payload.success) {
          if (payload.error.code !== "UNSUPPORTED_PROTOCOL") return;
          unsupportedProtocol ??= new RpcResponseError(payload.error.code, payload.error.message);
          return;
        }
        if (!isValid<ProbeData>(RESULT_SCHEMAS.probe, payload.data)) {
          malformedSuccess = true;
          return;
        }
        const data = payload.data;
        if (data.available) finish(() => resolve(data));
        else fallback ??= data;
      });
      timer = setTimeout(() => finish(() => {
        if (fallback) resolve(fallback);
        else if (unsupportedProtocol) reject(unsupportedProtocol);
        else reject(malformedSuccess ? new RpcProtocolError() : new RpcTimeoutError());
      }), timeoutMs);
      options.signal?.addEventListener("abort", onAbort, { once: true });
      try {
        this.events.emit(CHANNELS.probe, { requestId, supportedProtocols: [PROTOCOL_V1] });
      } catch (error) {
        finish(() => reject(error));
      }
    });
  }

  async spawn(input: SpawnInput, provider: ProbeData, options?: CallOptions): Promise<WorkerReference> {
    return this.operation("spawn", input, provider, RESULT_SCHEMAS.spawn, options);
  }
  async send(input: SendInput, provider: ProbeData, options?: CallOptions): Promise<DeliveryReceipt> {
    return this.operation("send", input, provider, RESULT_SCHEMAS.send, options);
  }
  async inspect(input: InspectInput, provider: ProbeData, options?: CallOptions): Promise<Inspection> {
    return this.operation("inspect", input, provider, RESULT_SCHEMAS.inspect, options);
  }
  async stop(target: string | undefined, provider: ProbeData, options?: CallOptions): Promise<never> {
    return this.operation("stop", target ? { target } : {}, provider, undefined, options);
  }

  private operation<T>(operation: "spawn" | "send" | "inspect" | "stop", input: object, provider: ProbeData, resultSchema: TSchema | undefined, options: CallOptions = {}): Promise<T> {
    const requestId = this.createRequestId();
    const request = { ...input, requestId, providerInstanceId: provider.providerInstanceId, protocol: provider.protocol } as AddressedRequest;
    return this.call(CHANNELS[operation], request, resultSchema, timeoutFor(operation, options.timeoutMs), options.signal);
  }

  private call<T>(channel: RequestChannel, request: AddressedRequest, resultSchema: TSchema | undefined, timeoutMs: number, signal?: AbortSignal): Promise<T> {
    return new Promise((resolve, reject) => {
      let settled = false;
      let timer: ReturnType<typeof setTimeout> | undefined;
      let unsubscribe = () => {};
      const cleanup = () => { unsubscribe(); if (timer !== undefined) clearTimeout(timer); signal?.removeEventListener("abort", onAbort); };
      const finish = (action: () => void) => { if (settled) return; settled = true; cleanup(); action(); };
      const onAbort = () => finish(() => reject(new RpcAbortError()));
      if (signal?.aborted) return onAbort();
      unsubscribe = this.events.on(replyChannel(channel, request.requestId), (payload) => {
        if (!isReplyEnvelope(payload) || payload.requestId !== request.requestId) return;
        if (!payload.success) {
          finish(() => reject(new RpcResponseError(payload.error.code, payload.error.message)));
          return;
        }
        const data = payload.data;
        if (resultSchema === undefined || !isValid<T>(resultSchema, data)) {
          finish(() => reject(new RpcProtocolError()));
          return;
        }
        finish(() => resolve(data));
      });
      timer = setTimeout(() => finish(() => reject(new RpcTimeoutError())), timeoutMs);
      signal?.addEventListener("abort", onAbort, { once: true });
      try { this.events.emit(channel, request); } catch (error) { finish(() => reject(error)); }
    });
  }
}

export function createWorkerRpcClient(options: WorkerRpcClientOptions): WorkerRpcClient {
  return new WorkerRpcClient(options);
}

export { DEFAULT_TIMEOUTS, LIMITS };
