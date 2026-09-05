import { isDeepStrictEqual } from "node:util";
import { Type, type Static } from "typebox";
import { Check } from "typebox/value";
import type { RunLifecycleRecord } from "../lifecycle/acceptor.js";
import type { WorkerRunRecordV1, WorkerRunStatus } from "./protocol.js";

export const RUN_REGISTRATION_ENTRY = "herdr-worker.run-registration.v1";
export const RUN_ENDPOINT_BINDING_ENTRY = "herdr-worker.run-endpoint-bound.v1";

const LIMITS = {
	id: 128,
	sessionId: 128,
	cwd: 4096,
	model: 128,
	role: 128,
	agentName: 32,
	paneId: 128,
} as const;

const safeId = Type.String({ minLength: 1, maxLength: LIMITS.id, pattern: "^[A-Za-z0-9._-]+$" });
const bounded = (maxLength: number) => Type.String({ minLength: 1, maxLength });

export const RunRegistrationV1Schema = Type.Object({
	version: Type.Literal(1),
	runId: safeId,
	sessionId: bounded(LIMITS.sessionId),
	registeredAt: Type.Integer({ minimum: 0 }),
	correlationId: Type.Optional(safeId),
	requestId: Type.Optional(safeId),
	assignment: Type.Object({
		cwd: bounded(LIMITS.cwd),
		model: Type.Optional(bounded(LIMITS.model)),
		role: Type.Optional(bounded(LIMITS.role)),
	}),
});

export const RunEndpointBindingV1Schema = Type.Object({
	version: Type.Literal(1),
	runId: safeId,
	sessionId: bounded(LIMITS.sessionId),
	agentName: bounded(LIMITS.agentName),
	paneId: bounded(LIMITS.paneId),
	observedAt: Type.Integer({ minimum: 0 }),
});

export type RunRegistrationV1 = Static<typeof RunRegistrationV1Schema>;
export type RunEndpointBindingV1 = Static<typeof RunEndpointBindingV1Schema>;

interface SessionEntry {
	type?: unknown;
	customType?: unknown;
	data?: unknown;
}

export interface RunRegistryOptions {
	sessionId: string;
	getEntries: () => readonly SessionEntry[];
	appendEntry: (customType: string, data: RunRegistrationV1 | RunEndpointBindingV1) => void;
}

function clone<T>(value: T): T {
	return structuredClone(value);
}

export function isRunRegistrationV1(value: unknown): value is RunRegistrationV1 {
	return Check(RunRegistrationV1Schema, value);
}

export function isRunEndpointBindingV1(value: unknown): value is RunEndpointBindingV1 {
	return Check(RunEndpointBindingV1Schema, value);
}

export class RunRegistry {
	private readonly registrations = new Map<string, RunRegistrationV1>();
	private readonly endpoints = new Map<string, RunEndpointBindingV1>();

	constructor(private readonly options: RunRegistryOptions) {
		if (typeof options.sessionId !== "string" || options.sessionId.length === 0 || options.sessionId.length > LIMITS.sessionId) {
			throw new TypeError("Invalid run registry session ID.");
		}
		this.restore();
	}

	register(value: RunRegistrationV1): RunRegistrationV1 {
		if (!isRunRegistrationV1(value) || value.sessionId !== this.options.sessionId) {
			throw new TypeError("Invalid run registration.");
		}
		const registration = clone(value);
		const current = this.registrations.get(registration.runId);
		if (current) {
			if (!isDeepStrictEqual(current, registration)) throw new Error(`Conflicting registration for run ${registration.runId}.`);
			return clone(current);
		}
		this.options.appendEntry(RUN_REGISTRATION_ENTRY, clone(registration));
		this.registrations.set(registration.runId, registration);
		return clone(registration);
	}

	bindEndpoint(value: RunEndpointBindingV1): RunEndpointBindingV1 {
		if (!isRunEndpointBindingV1(value) || value.sessionId !== this.options.sessionId) {
			throw new TypeError("Invalid run endpoint binding.");
		}
		const binding = clone(value);
		if (!this.registrations.has(binding.runId)) throw new Error(`Cannot bind an unregistered run ${binding.runId}.`);
		const current = this.endpoints.get(binding.runId);
		if (current) {
			if (!isDeepStrictEqual(current, binding)) throw new Error(`Conflicting endpoint binding for run ${binding.runId}.`);
			return clone(current);
		}
		this.options.appendEntry(RUN_ENDPOINT_BINDING_ENTRY, clone(binding));
		this.endpoints.set(binding.runId, binding);
		return clone(binding);
	}

	getRegistration(runId: string): RunRegistrationV1 | undefined {
		const registration = this.registrations.get(runId);
		return registration ? clone(registration) : undefined;
	}

	getEndpoint(runId: string): RunEndpointBindingV1 | undefined {
		const endpoint = this.endpoints.get(runId);
		return endpoint ? clone(endpoint) : undefined;
	}

	listRegistrations(): RunRegistrationV1[] {
		return [...this.registrations.values()].map(clone);
	}

	projectRun(runId: string, lifecycle?: RunLifecycleRecord): WorkerRunRecordV1 | undefined {
		const registration = this.registrations.get(runId);
		if (registration) {
			const endpoint = this.endpoints.get(runId);
			return clone({
				protocol: 1,
				runId: registration.runId,
				...(registration.correlationId === undefined ? {} : { correlationId: registration.correlationId }),
				...(registration.requestId === undefined ? {} : { requestId: registration.requestId }),
				sessionId: registration.sessionId,
				registeredAt: registration.registeredAt,
				lifecycle: lifecycleView(lifecycle),
				assignment: clone(registration.assignment),
				...(endpoint === undefined ? {} : {
					endpoint: {
						agentName: endpoint.agentName,
						paneId: endpoint.paneId,
						observedAt: endpoint.observedAt,
					},
				}),
			});
		}
		if (!lifecycle || lifecycle.acceptedSequence === 0) return undefined;
		return clone({
			protocol: 1,
			legacy: true,
			runId: lifecycle.runId,
			...(lifecycle.correlationId === undefined ? {} : { correlationId: lifecycle.correlationId }),
			sessionId: this.options.sessionId,
			lifecycle: lifecycleView(lifecycle),
			worker: {
				agentName: lifecycle.worker.name,
				...(lifecycle.worker.paneId === undefined ? {} : { paneId: lifecycle.worker.paneId }),
			},
		});
	}

	listRunRecords(lifecycleRecords: readonly RunLifecycleRecord[]): WorkerRunRecordV1[] {
		const lifecycleByRun = new Map(lifecycleRecords.map((record) => [record.runId, record]));
		const runIds = new Set(this.registrations.keys());
		for (const record of lifecycleRecords) if (record.acceptedSequence > 0) runIds.add(record.runId);
		return [...runIds]
			.sort()
			.map((runId) => this.projectRun(runId, lifecycleByRun.get(runId)))
			.filter((record): record is WorkerRunRecordV1 => record !== undefined);
	}

	private restore(): void {
		for (const entry of this.options.getEntries()) {
			if (entry.type !== "custom") continue;
			if (entry.customType === RUN_REGISTRATION_ENTRY) {
				if (!isRunRegistrationV1(entry.data) || entry.data.sessionId !== this.options.sessionId) continue;
				const current = this.registrations.get(entry.data.runId);
				if (!current) this.registrations.set(entry.data.runId, clone(entry.data));
				continue;
			}
			if (entry.customType === RUN_ENDPOINT_BINDING_ENTRY) {
				if (!isRunEndpointBindingV1(entry.data) || entry.data.sessionId !== this.options.sessionId) continue;
				if (!this.registrations.has(entry.data.runId) || this.endpoints.has(entry.data.runId)) continue;
				this.endpoints.set(entry.data.runId, clone(entry.data));
			}
		}
	}
}

function lifecycleView(record?: RunLifecycleRecord): WorkerRunRecordV1["lifecycle"] {
	let status: WorkerRunStatus = "registered";
	if (record?.status === "started" || record?.status === "completed" || record?.status === "failed" || record?.status === "uncertain") {
		status = record.status;
	}
	return {
		status,
		acceptedSequence: record?.acceptedSequence ?? 0,
		...(record?.readiness === undefined ? {} : { readiness: record.readiness }),
	};
}

export function createRunRegistry(options: RunRegistryOptions): RunRegistry {
	return new RunRegistry(options);
}
