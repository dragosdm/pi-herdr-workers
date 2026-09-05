import { randomUUID } from "node:crypto";
import type { RpcEventBus } from "../rpc/client.js";
import {
	RUN_QUERY_CHANNELS,
	RUN_QUERY_PROTOCOL_V1,
	RUN_QUERY_RESULT_SCHEMAS,
	RunQueryServiceError,
	extractRunQueryRequestId,
	isValidRunQueryRequest,
	isValidRunQueryResult,
	runQueryFailure,
	runQueryReplyChannel,
	runQuerySuccess,
	type GetRunInput,
	type ListRunsInput,
	type ListRunsResult,
	type RunQueryAvailabilityReason,
	type RunQueryProbeRequest,
	type RunQueryRequestChannel,
	type WorkerRunRecordV1,
} from "./protocol.js";

export interface RunQueryService {
	get(input: GetRunInput, signal?: AbortSignal): Promise<WorkerRunRecordV1>;
	list(input: ListRunsInput, signal?: AbortSignal): Promise<ListRunsResult>;
}

export interface RunQueryProviderState { available: boolean; reason?: RunQueryAvailabilityReason }
export interface RunQueryServerOptions {
	events: RpcEventBus;
	service: RunQueryService;
	sessionId: string;
	getProviderState: () => RunQueryProviderState;
	createInstanceId?: () => string;
}
export interface RunQueryServer { providerInstanceId: string; dispose(): void }

const FIXED_MESSAGES = {
	INVALID_REQUEST: "Request is invalid.",
	UNSUPPORTED_PROTOCOL: "Protocol is not supported.",
	PROVIDER_UNAVAILABLE: "Run query provider is unavailable.",
	INTERNAL_ERROR: "The run query failed.",
} as const;

export function registerRunQueryServer(options: RunQueryServerOptions): RunQueryServer {
	const providerInstanceId = options.createInstanceId?.() ?? randomUUID();
	const unsubscribers: Array<() => void> = [];
	let disposed = false;

	const emit = (channel: RunQueryRequestChannel, requestId: string, envelope: unknown) => {
		options.events.emit(runQueryReplyChannel(channel, requestId), envelope);
	};
	const handle = async (channel: RunQueryRequestChannel, payload: unknown) => {
		const requestId = extractRunQueryRequestId(payload);
		if (!requestId) return;
		if (!isValidRunQueryRequest(channel, payload)) {
			emit(channel, requestId, runQueryFailure(requestId, "INVALID_REQUEST", FIXED_MESSAGES.INVALID_REQUEST));
			return;
		}
		if (channel === RUN_QUERY_CHANNELS.probe) {
			const request = payload as RunQueryProbeRequest;
			const protocol = [...request.supportedProtocols]
				.sort((left, right) => right - left)
				.find((item) => item === RUN_QUERY_PROTOCOL_V1);
			if (!protocol) {
				emit(channel, requestId, runQueryFailure(requestId, "UNSUPPORTED_PROTOCOL", FIXED_MESSAGES.UNSUPPORTED_PROTOCOL));
				return;
			}
			const state = options.getProviderState();
			const data = {
				protocol,
				provider: "herdr-runs",
				providerInstanceId,
				sessionId: options.sessionId,
				available: state.available,
				...(state.available ? {} : { reason: state.reason ?? "SESSION_NOT_READY" }),
				constraints: { sessionScoped: true, requiresHerdrPane: false, requiresInteractivePi: false },
			};
			emit(channel, requestId, isValidRunQueryResult("probe", data)
				? runQuerySuccess(requestId, data)
				: runQueryFailure(requestId, "INTERNAL_ERROR", FIXED_MESSAGES.INTERNAL_ERROR));
			return;
		}

		const request = payload as Record<string, unknown> & { providerInstanceId: string; protocol: number };
		if (request.providerInstanceId !== providerInstanceId) return;
		if (request.protocol !== RUN_QUERY_PROTOCOL_V1) {
			emit(channel, requestId, runQueryFailure(requestId, "UNSUPPORTED_PROTOCOL", FIXED_MESSAGES.UNSUPPORTED_PROTOCOL));
			return;
		}
		if (!options.getProviderState().available) {
			emit(channel, requestId, runQueryFailure(requestId, "PROVIDER_UNAVAILABLE", FIXED_MESSAGES.PROVIDER_UNAVAILABLE));
			return;
		}

		try {
			const data = channel === RUN_QUERY_CHANNELS.get
				? await options.service.get({
					runId: request.runId as string,
					...(request.includeEndpointObservation === undefined ? {} : { includeEndpointObservation: request.includeEndpointObservation as boolean }),
				})
				: await options.service.list({
					...(request.cursor === undefined ? {} : { cursor: request.cursor as string }),
					...(request.limit === undefined ? {} : { limit: request.limit as number }),
				});
			const operation = channel === RUN_QUERY_CHANNELS.get ? "get" : "list";
			emit(channel, requestId, isValidRunQueryResult(operation, data)
				? runQuerySuccess(requestId, data)
				: runQueryFailure(requestId, "INTERNAL_ERROR", FIXED_MESSAGES.INTERNAL_ERROR));
		} catch (error) {
			if (error instanceof RunQueryServiceError) {
				emit(channel, requestId, runQueryFailure(requestId, error.code, error.message));
			} else {
				emit(channel, requestId, runQueryFailure(requestId, "INTERNAL_ERROR", FIXED_MESSAGES.INTERNAL_ERROR));
			}
		}
	};

	for (const channel of Object.values(RUN_QUERY_CHANNELS)) {
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
