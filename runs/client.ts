import { randomUUID } from "node:crypto";
import type { TSchema } from "typebox";
import type { RpcEventBus } from "../rpc/client.js";
import {
	RUN_QUERY_CHANNELS,
	RUN_QUERY_PROTOCOL_V1,
	RUN_QUERY_RESULT_SCHEMAS,
	isRunQueryReplyEnvelope,
	isValidReplayRunResult,
	isValidRunQueryResult,
	runQueryReplyChannel,
	type GetRunInput,
	type ListRunsInput,
	type ListRunsResult,
	type ReplayRunInput,
	type ReplayRunResult,
	type RunQueryAddressedRequest,
	type RunQueryProbeData,
	type RunQueryReply,
	type RunQueryRequestChannel,
	type WorkerRunRecordV1,
} from "./protocol.js";

export interface RunQueryCallOptions { timeoutMs?: number; signal?: AbortSignal }
export interface RunQueryClientOptions { events: RpcEventBus; createRequestId?: () => string }

const RUN_QUERY_DEFAULT_TIMEOUTS = { probe: 2_000, get: 20_000, list: 20_000, replay: 20_000 } as const;

export class RunQueryTimeoutError extends Error {
	constructor() { super("Run query request timed out."); this.name = "RunQueryTimeoutError"; }
}
export class RunQueryAbortError extends Error {
	constructor() { super("Run query request was aborted."); this.name = "RunQueryAbortError"; }
}
export class RunQueryResponseError extends Error {
	constructor(readonly code: string, message: string) { super(message); this.name = "RunQueryResponseError"; }
}
export class RunQueryProtocolError extends Error {
	readonly code = "INVALID_RESPONSE";
	constructor() {
		super("The run query provider returned an invalid response.");
		this.name = "RunQueryProtocolError";
	}
}

function timeoutFor(operation: keyof typeof RUN_QUERY_CHANNELS, requested?: number): number {
	if (requested === undefined) return RUN_QUERY_DEFAULT_TIMEOUTS[operation];
	if (!Number.isFinite(requested) || requested <= 0 || requested > 300_000) {
		throw new RangeError("timeoutMs must be finite and between 1 and 300000.");
	}
	return requested;
}

export class RunQueryClient {
	private readonly events: RpcEventBus;
	private readonly createRequestId: () => string;

	constructor(options: RunQueryClientOptions) {
		this.events = options.events;
		this.createRequestId = options.createRequestId ?? randomUUID;
	}

	probe(options: RunQueryCallOptions = {}): Promise<RunQueryProbeData> {
		const requestId = this.createRequestId();
		const timeoutMs = timeoutFor("probe", options.timeoutMs);
		return new Promise((resolve, reject) => {
			let settled = false;
			let fallback: RunQueryProbeData | undefined;
			let unsupportedProtocol: RunQueryResponseError | undefined;
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
			const onAbort = () => finish(() => reject(new RunQueryAbortError()));
			if (options.signal?.aborted) return onAbort();
			unsubscribe = this.events.on(runQueryReplyChannel(RUN_QUERY_CHANNELS.probe, requestId), (payload) => {
				if (!isRunQueryReplyEnvelope(payload) || payload.requestId !== requestId) return;
				if (!payload.success) {
					if (payload.error.code !== "UNSUPPORTED_PROTOCOL") return;
					unsupportedProtocol ??= new RunQueryResponseError(payload.error.code, payload.error.message);
					return;
				}
				if (!isValidRunQueryResult("probe", payload.data)) {
					malformedSuccess = true;
					return;
				}
				const data = payload.data as RunQueryProbeData;
				if (data.available) finish(() => resolve(data));
				else fallback ??= data;
			});
			timer = setTimeout(() => finish(() => {
				if (fallback) resolve(fallback);
				else if (unsupportedProtocol) reject(unsupportedProtocol);
				else reject(malformedSuccess ? new RunQueryProtocolError() : new RunQueryTimeoutError());
			}), timeoutMs);
			options.signal?.addEventListener("abort", onAbort, { once: true });
			try {
				this.events.emit(RUN_QUERY_CHANNELS.probe, { requestId, supportedProtocols: [RUN_QUERY_PROTOCOL_V1] });
			} catch (error) {
				finish(() => reject(error));
			}
		});
	}

	async get(input: GetRunInput, provider: RunQueryProbeData, options?: RunQueryCallOptions): Promise<WorkerRunRecordV1> {
		return this.operation("get", input, provider, RUN_QUERY_RESULT_SCHEMAS.get, options);
	}

	async list(input: ListRunsInput, provider: RunQueryProbeData, options?: RunQueryCallOptions): Promise<ListRunsResult> {
		return this.operation("list", input, provider, RUN_QUERY_RESULT_SCHEMAS.list, options);
	}

	async replay(input: ReplayRunInput, provider: RunQueryProbeData, options?: RunQueryCallOptions): Promise<ReplayRunResult> {
		const result = await this.operation<ReplayRunResult>("replay", input, provider, RUN_QUERY_RESULT_SCHEMAS.replay, options);
		if (!isValidReplayRunResult(result, input)) throw new RunQueryProtocolError();
		return result;
	}

	private operation<T>(operation: "get" | "list" | "replay", input: object, provider: RunQueryProbeData, resultSchema: TSchema, options: RunQueryCallOptions = {}): Promise<T> {
		const requestId = this.createRequestId();
		const request = {
			...input,
			requestId,
			providerInstanceId: provider.providerInstanceId,
			protocol: provider.protocol,
		} as RunQueryAddressedRequest;
		return this.call(RUN_QUERY_CHANNELS[operation], request, resultSchema, timeoutFor(operation, options.timeoutMs), options.signal);
	}

	private call<T>(channel: RunQueryRequestChannel, request: RunQueryAddressedRequest, _resultSchema: TSchema, timeoutMs: number, signal?: AbortSignal): Promise<T> {
		return new Promise((resolve, reject) => {
			let settled = false;
			let timer: ReturnType<typeof setTimeout> | undefined;
			let unsubscribe = () => {};
			const cleanup = () => { unsubscribe(); if (timer !== undefined) clearTimeout(timer); signal?.removeEventListener("abort", onAbort); };
			const finish = (action: () => void) => { if (settled) return; settled = true; cleanup(); action(); };
			const onAbort = () => finish(() => reject(new RunQueryAbortError()));
			if (signal?.aborted) return onAbort();
			unsubscribe = this.events.on(runQueryReplyChannel(channel, request.requestId), (payload) => {
				if (!isRunQueryReplyEnvelope(payload) || payload.requestId !== request.requestId) return;
				if (!payload.success) {
					finish(() => reject(new RunQueryResponseError(payload.error.code, payload.error.message)));
					return;
				}
				const operation = channel === RUN_QUERY_CHANNELS.get ? "get" : channel === RUN_QUERY_CHANNELS.list ? "list" : "replay";
				if (!isValidRunQueryResult(operation, payload.data)) {
					finish(() => reject(new RunQueryProtocolError()));
					return;
				}
				finish(() => resolve(payload.data as T));
			});
			timer = setTimeout(() => finish(() => reject(new RunQueryTimeoutError())), timeoutMs);
			signal?.addEventListener("abort", onAbort, { once: true });
			try { this.events.emit(channel, request); } catch (error) { finish(() => reject(error)); }
		});
	}
}

export function createRunQueryClient(options: RunQueryClientOptions): RunQueryClient {
	return new RunQueryClient(options);
}

export { RUN_QUERY_DEFAULT_TIMEOUTS };
