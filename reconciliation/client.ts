import { randomUUID } from "node:crypto";
import type { RpcEventBus } from "../rpc/client.js";
import {
	RECONCILIATION_CHANNELS,
	RECONCILIATION_PROTOCOL_V1,
	isReconciliationReplyEnvelope,
	isValidReconciliationResult,
	reconciliationReplyChannel,
	type ReconcileRunInput,
	type ReconcileRunRequest,
	type ReconciliationProbeData,
	type ReconciliationReply,
} from "./protocol.js";
import type { AcceptedLifecycleEvent } from "../lifecycle/protocol.js";

export interface ReconciliationCallOptions { timeoutMs?: number; signal?: AbortSignal }
export interface ReconciliationClientOptions { events: RpcEventBus; createRequestId?: () => string }

const RECONCILIATION_DEFAULT_TIMEOUTS = { probe: 2_000, reconcile: 20_000 } as const;

export class ReconciliationTimeoutError extends Error {
	constructor() { super("Reconciliation request timed out."); this.name = "ReconciliationTimeoutError"; }
}
export class ReconciliationAbortError extends Error {
	constructor() { super("Reconciliation request was aborted."); this.name = "ReconciliationAbortError"; }
}
export class ReconciliationResponseError extends Error {
	constructor(readonly code: string, message: string) { super(message); this.name = "ReconciliationResponseError"; }
}
export class ReconciliationProtocolError extends Error {
	readonly code = "INVALID_RESPONSE";
	constructor() {
		super("The reconciliation provider returned an invalid response.");
		this.name = "ReconciliationProtocolError";
	}
}

function timeoutFor(operation: keyof typeof RECONCILIATION_CHANNELS, requested?: number): number {
	if (requested === undefined) return RECONCILIATION_DEFAULT_TIMEOUTS[operation];
	if (!Number.isFinite(requested) || requested <= 0 || requested > 300_000) {
		throw new RangeError("timeoutMs must be finite and between 1 and 300000.");
	}
	return requested;
}

export class ReconciliationClient {
	private readonly events: RpcEventBus;
	private readonly createRequestId: () => string;

	constructor(options: ReconciliationClientOptions) {
		this.events = options.events;
		this.createRequestId = options.createRequestId ?? randomUUID;
	}

	probe(options: ReconciliationCallOptions = {}): Promise<ReconciliationProbeData> {
		const requestId = this.createRequestId();
		return new Promise((resolve, reject) => {
			let fallback: ReconciliationProbeData | undefined;
			let unsupported: ReconciliationResponseError | undefined;
			let malformed = false;
			this.waitForReply<ReconciliationProbeData>(
				RECONCILIATION_CHANNELS.probe,
				requestId,
				timeoutFor("probe", options.timeoutMs),
				options.signal,
				(payload, finish) => {
					if (!payload.success) {
						if (payload.error.code === "UNSUPPORTED_PROTOCOL") unsupported ??= new ReconciliationResponseError(payload.error.code, payload.error.message);
						return;
					}
					if (!isValidReconciliationResult("probe", payload.data)) { malformed = true; return; }
					const data = payload.data as ReconciliationProbeData;
					if (data.available) finish(() => resolve(data));
					else fallback ??= data;
				},
				() => {
					if (fallback) resolve(fallback);
					else if (unsupported) reject(unsupported);
					else reject(malformed ? new ReconciliationProtocolError() : new ReconciliationTimeoutError());
				},
				(error) => reject(error),
				{ requestId, supportedProtocols: [RECONCILIATION_PROTOCOL_V1] },
			);
		});
	}

	reconcile(input: ReconcileRunInput, provider: ReconciliationProbeData, options: ReconciliationCallOptions = {}): Promise<AcceptedLifecycleEvent> {
		const requestId = this.createRequestId();
		const request = {
			...input,
			requestId,
			providerInstanceId: provider.providerInstanceId,
			protocol: provider.protocol,
		} as ReconcileRunRequest;
		return new Promise((resolve, reject) => {
			this.waitForReply<AcceptedLifecycleEvent>(
				RECONCILIATION_CHANNELS.reconcile,
				requestId,
				timeoutFor("reconcile", options.timeoutMs),
				options.signal,
				(payload, finish) => {
					if (!payload.success) {
						finish(() => reject(new ReconciliationResponseError(payload.error.code, payload.error.message)));
						return;
					}
					if (!isValidReconciliationResult("reconcile", payload.data)) {
						finish(() => reject(new ReconciliationProtocolError()));
						return;
					}
					finish(() => resolve(payload.data as AcceptedLifecycleEvent));
				},
				() => reject(new ReconciliationTimeoutError()),
				(error) => reject(error),
				request,
			);
		});
	}

	private waitForReply<T>(
		channel: (typeof RECONCILIATION_CHANNELS)[keyof typeof RECONCILIATION_CHANNELS],
		requestId: string,
		timeoutMs: number,
		signal: AbortSignal | undefined,
		onReply: (payload: ReconciliationReply, finish: (action: () => void) => void) => void,
		onTimeout: () => void,
		onError: (error: unknown) => void,
		request: object,
	): void {
		let settled = false;
		let timer: ReturnType<typeof setTimeout> | undefined;
		let unsubscribe = () => {};
		const cleanup = () => { unsubscribe(); if (timer !== undefined) clearTimeout(timer); signal?.removeEventListener("abort", onAbort); };
		const finish = (action: () => void) => { if (settled) return; settled = true; cleanup(); action(); };
		const onAbort = () => finish(() => onError(new ReconciliationAbortError()));
		if (signal?.aborted) { onAbort(); return; }
		unsubscribe = this.events.on(reconciliationReplyChannel(channel, requestId), (payload) => {
			if (!isReconciliationReplyEnvelope(payload) || payload.requestId !== requestId) return;
			onReply(payload, finish);
		});
		timer = setTimeout(() => finish(onTimeout), timeoutMs);
		signal?.addEventListener("abort", onAbort, { once: true });
		try { this.events.emit(channel, request); } catch (error) { finish(() => onError(error)); }
	}
}

export function createReconciliationClient(options: ReconciliationClientOptions): ReconciliationClient {
	return new ReconciliationClient(options);
}

export { RECONCILIATION_DEFAULT_TIMEOUTS };
