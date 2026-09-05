import { randomUUID } from "node:crypto";
import type { AcceptedLifecycleEvent } from "../lifecycle/protocol.js";
import type { RpcEventBus } from "../rpc/client.js";
import {
	RECONCILIATION_CHANNELS,
	RECONCILIATION_PROTOCOL_V1,
	ReconciliationServiceError,
	extractReconciliationRequestId,
	isValidReconciliationRequest,
	isValidReconciliationResult,
	reconciliationFailure,
	reconciliationReplyChannel,
	reconciliationSuccess,
	type ReconcileRunInput,
	type ReconciliationAvailabilityReason,
	type ReconciliationProbeRequest,
	type ReconciliationRequestChannel,
} from "./protocol.js";

export interface ReconciliationAuthority { sourceInstanceId: string }
export interface ReconciliationService {
	reconcile(input: ReconcileRunInput, authority: ReconciliationAuthority, signal?: AbortSignal): Promise<AcceptedLifecycleEvent>;
}
export interface ReconciliationProviderState { available: boolean; reason?: ReconciliationAvailabilityReason }
export interface ReconciliationServerOptions {
	events: RpcEventBus;
	service: ReconciliationService;
	sessionId: string;
	getProviderState: () => ReconciliationProviderState;
	createInstanceId?: () => string;
}
export interface ReconciliationServer { providerInstanceId: string; dispose(): void }

const FIXED_MESSAGES = {
	INVALID_REQUEST: "Request is invalid.",
	UNSUPPORTED_PROTOCOL: "Protocol is not supported.",
	PROVIDER_UNAVAILABLE: "Reconciliation provider is unavailable.",
	INTERNAL_ERROR: "The reconciliation operation failed.",
} as const;

export function registerReconciliationServer(options: ReconciliationServerOptions): ReconciliationServer {
	const providerInstanceId = options.createInstanceId?.() ?? randomUUID();
	const unsubscribers: Array<() => void> = [];
	let disposed = false;

	const emit = (channel: ReconciliationRequestChannel, requestId: string, envelope: unknown) => {
		options.events.emit(reconciliationReplyChannel(channel, requestId), envelope);
	};
	const handle = async (channel: ReconciliationRequestChannel, payload: unknown) => {
		const requestId = extractReconciliationRequestId(payload);
		if (!requestId) return;
		if (!isValidReconciliationRequest(channel, payload)) {
			emit(channel, requestId, reconciliationFailure(requestId, "INVALID_REQUEST", FIXED_MESSAGES.INVALID_REQUEST));
			return;
		}
		if (channel === RECONCILIATION_CHANNELS.probe) {
			const request = payload as ReconciliationProbeRequest;
			const protocol = [...request.supportedProtocols]
				.sort((left, right) => right - left)
				.find((item) => item === RECONCILIATION_PROTOCOL_V1);
			if (!protocol) {
				emit(channel, requestId, reconciliationFailure(requestId, "UNSUPPORTED_PROTOCOL", FIXED_MESSAGES.UNSUPPORTED_PROTOCOL));
				return;
			}
			const state = options.getProviderState();
			const data = {
				protocol,
				provider: "herdr-reconciliation",
				providerInstanceId,
				sessionId: options.sessionId,
				available: state.available,
				...(state.available ? {} : { reason: state.reason ?? "SESSION_NOT_READY" }),
				constraints: { sessionScoped: true, requiresExactAcceptedSequence: true },
			};
			emit(channel, requestId, isValidReconciliationResult("probe", data)
				? reconciliationSuccess(requestId, data)
				: reconciliationFailure(requestId, "INTERNAL_ERROR", FIXED_MESSAGES.INTERNAL_ERROR));
			return;
		}

		const request = payload as Record<string, unknown> & { providerInstanceId: string; protocol: number };
		if (request.providerInstanceId !== providerInstanceId) return;
		if (request.protocol !== RECONCILIATION_PROTOCOL_V1) {
			emit(channel, requestId, reconciliationFailure(requestId, "UNSUPPORTED_PROTOCOL", FIXED_MESSAGES.UNSUPPORTED_PROTOCOL));
			return;
		}
		if (!options.getProviderState().available) {
			emit(channel, requestId, reconciliationFailure(requestId, "PROVIDER_UNAVAILABLE", FIXED_MESSAGES.PROVIDER_UNAVAILABLE));
			return;
		}

		try {
			const input: ReconcileRunInput = {
				runId: request.runId as string,
				expectedAcceptedSequence: request.expectedAcceptedSequence as number,
				resolution: structuredClone(request.resolution) as ReconcileRunInput["resolution"],
			};
			const data = await options.service.reconcile(input, { sourceInstanceId: providerInstanceId });
			emit(channel, requestId, isValidReconciliationResult("reconcile", data)
				? reconciliationSuccess(requestId, data)
				: reconciliationFailure(requestId, "INTERNAL_ERROR", FIXED_MESSAGES.INTERNAL_ERROR));
		} catch (error) {
			if (error instanceof ReconciliationServiceError) {
				emit(channel, requestId, reconciliationFailure(requestId, error.code, error.message));
			} else {
				emit(channel, requestId, reconciliationFailure(requestId, "INTERNAL_ERROR", FIXED_MESSAGES.INTERNAL_ERROR));
			}
		}
	};

	for (const channel of Object.values(RECONCILIATION_CHANNELS)) {
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
