/**
 * Herdr worker — orchestrator/worker messaging between pi agents living in herdr panes.
 *
 * Orchestrator side:
 *   /team add [right|down|left|up] [name]   start a pi worker agent-<name> (default agent-N) beside this
 *                                          pane; further workers stack along that side
 *   /team list                              show workers
 *   /team release <name>                    drop a worker (pane stays open)
 *   /team adopt <name>                      control an existing herdr agent without creating a pane
 *
 * Worker side:
 *   pi --orchestrated-by <id>      (this is what /team add passes when starting the worker)
 *   /team from <id>                adopt an orchestrator after startup
 *   /orchestrated-by <id>          same
 *
 * Both sides get the SendToAgent tool. Delivery goes through a per-pane inbox that this
 * extension watches; receivers inject a custom message (steer when priority,
 * follow-up otherwise). Targets that are not a listening pi fall back to `herdr agent prompt`.
 */

import * as fs from "node:fs";
import * as path from "node:path";
import { Buffer } from "node:buffer";
import { randomUUID } from "node:crypto";
import type { ExtensionAPI, ExtensionContext } from "@earendil-works/pi-coding-agent";
import { Type } from "typebox";
import { StringEnum } from "@earendil-works/pi-ai";
import { Text } from "@earendil-works/pi-tui";
import { createLifecycleAcceptor, type LifecycleAcceptor } from "../lifecycle/acceptor.js";
import { mailboxRoot } from "../mailbox/paths.js";
import { createMailboxTransport, type MailboxCallbacks, type MailboxTransport, type MailboxTransportOptions } from "../mailbox/transport.js";
import {
	LIFECYCLE_PROTOCOL_V1,
	LIFECYCLE_PROTOCOL_V2,
	WorkerRunReportInputSchema,
	isWorkerRunBinding,
	isWorkerRunReport,
	isWorkerRunReportInput,
	type WorkerRunBinding,
	type WorkerRunReport,
	type WorkerRunReportInput,
	type WorkerRunReportInputV1,
	type WorkerRunReportInputV2,
} from "../lifecycle/protocol.js";
import { registerWorkerRpcServer, type WorkerRpcService } from "../rpc/server.js";
import { WorkerRpcServiceError, type InspectInput, type Inspection, type SendInput, type DeliveryReceipt, type SpawnInput, type SpawnProvenance } from "../rpc/protocol.js";
import { ReconciliationServiceError } from "../reconciliation/protocol.js";
import { registerReconciliationServer, type ReconciliationServer, type ReconciliationService } from "../reconciliation/server.js";
import { createRunRegistry, type RunRegistry } from "../runs/registry.js";
import {
	RUN_QUERY_LIMITS,
	RunQueryServiceError,
	decodeRunQueryCursor,
	encodeRunQueryCursor,
	type ListRunsResult,
	type ReplayRunResult,
} from "../runs/protocol.js";
import { registerRunQueryServer, type RunQueryServer, type RunQueryService } from "../runs/server.js";

// ───────────────────────── herdr env ─────────────────────────

const HERDR_ENV = process.env.HERDR_ENV === "1";
const SELF_PANE = process.env.HERDR_PANE_ID ?? "";
const ENTRY_TYPE = "herdr-worker";
const STATUS_KEY = "herdr-worker";
const META_SOURCE = "pi-herdr-worker";
const TOOL_NAME = "SendToAgent";
const CREATE_TOOL = "CreateAgentPanel";
const REPORT_TOOL = "ReportWorkerRun";
const MESSAGE_CUSTOM_TYPE = "herdr-worker.message";
const LIFECYCLE_REPORT_CUSTOM_TYPE = "herdr-worker.lifecycle-report";
const NAME_RE = /^[a-z][a-z0-9_-]{0,31}$/;
const RESERVED = new Set(["add", "list", "release", "from", "status", "help", "adopt", "right", "down", "left", "up"]);
type Direction = "right" | "down" | "left" | "up";
const DIRECTIONS: Direction[] = ["right", "down", "left", "up"];
const isDirection = (s: string | undefined): s is Direction => !!s && (DIRECTIONS as string[]).includes(s);
const SELF_TAB = process.env.HERDR_TAB_ID ?? "";
const WORKER_PREFIX = "agent-";

// ───────────────────────── types ─────────────────────────

interface WorkerMeta {
	type?: string; // e.g. "explore", "implement", "review"
	purpose?: string; // free text, e.g. "explore only, never edit files"
	model?: string; // provider/id
	paneId?: string;
	runId?: string;
	correlationId?: string;
	requestId?: string;
	providerInstanceId?: string;
	lifecycleProtocol?: 1 | 2;
}

interface State {
	version?: 1;
	sessionId?: string;
	teamMode?: boolean; // set once /team is used here -> this pi is an orchestrator
	workers: string[]; // herdr agent names we orchestrate
	meta?: Record<string, WorkerMeta>;
	orchestratedBy?: string; // herdr agent name or pane id
	role?: { type?: string; purpose?: string }; // what our orchestrator said we are for
	activeRun?: WorkerRunBinding & { sourceInstanceId: string; sourceSequence: number };
}

/** Agent types with a preferred model. Anything else defaults to the orchestrator's model. */
const TYPE_MODELS: Record<string, string> = {
	explore: "xai/grok-4.6",
	research: "xai/grok-4.6",
};
const TYPE_HINTS: Record<string, string> = {
	explore: "Explore only: read, search, run read-only commands, and report findings. Do not create, edit, or delete files, and do not commit.",
	research: "Research only: gather facts, read code/docs, and report. No file modifications.",
	review: "Review only: read diffs/code and report actionable findings. Do not modify files.",
	implement: "Implement: make the requested code changes, verify them, and report what changed and how it was tested.",
	test: "Testing: write/run tests, report failures with repro steps. Avoid unrelated changes.",
};

interface AgentInfo {
	paneId: string;
	tabId?: string;
	name?: string;
	kind?: string;
	status?: string;
	cwd?: string;
}

interface Sender {
	id: string;
	paneId: string;
	name?: string;
	role: "orchestrator" | "worker" | "agent";
}
interface MessageEnvelope {
	type: "message";
	from: Sender;
	message: string;
	priority: boolean;
	ts: number;
	runId?: string;
}
interface ControlEnvelope {
	type: "control";
	from: Sender;
	action: "orchestrated-by" | "released" | "bind-run";
	binding?: WorkerRunBinding;
	ts: number;
}
interface LifecycleEnvelope {
	type: "lifecycle";
	from: Sender;
	report: WorkerRunReport;
	ts: number;
}
type Envelope = MessageEnvelope | ControlEnvelope | LifecycleEnvelope;

// ───────────────────────── message framing ─────────────────────────

function roleLabel(role: Sender["role"]): string {
	return role === "orchestrator" ? "Orchestrator" : role === "worker" ? "Worker" : "Agent";
}

function frameMessage(env: MessageEnvelope): string {
	const label = roleLabel(env.from.role);
	const who = env.from.name ? `${label} "${env.from.name}" in pane ${env.from.paneId}` : `${label} in pane ${env.from.paneId}`;
	return [
		`[agent] A message just arrived from another of your user's agents: ${who}.`,
		"This is another agent reaching out — not the user typing here. It arrived asynchronously, and your user can already see it in this chat.",
		"",
		env.message.trim(),
		"",
		`If it needs a reply or an action, handle it: reply to ${label} with SendToAgent (their id: ${env.from.id}), which reaches them on a later turn — not a live back-and-forth. If it is just an FYI with nothing for you to do, it is fine to stay silent — no need to reply just to acknowledge it.`,
	].join("\n");
}

// ───────────────────────── extension ─────────────────────────

interface HerdrWorkerTestOptions {
	createMailbox?: (callbacks: MailboxCallbacks, defaults: MailboxTransportOptions) => MailboxTransport;
	disableInbox?: boolean;
	isListening?: (paneId: string) => boolean;
	writeEnvelope?: (paneId: string, envelope: Envelope) => void;
	onInboxHandler?: (deliverEnvelope: (envelope: unknown, envelopeId: string) => Promise<void>) => void;
}

interface CreateOpts {
	runId: string;
	lifecycleProtocol: 2;
	correlationId?: string;
	provenance?: SpawnProvenance;
	name?: string;
	direction?: Direction;
	type?: string;
	purpose?: string;
	model?: string;
	thinking?: string;
	initialPrompt?: string;
}

interface PendingSpawnLifecycle {
	opts: CreateOpts;
	name: string;
	paneId?: string;
	scope: "pane_creation" | "agent_start" | "assignment_delivery";
}

export default function (pi: ExtensionAPI, testOptions: HerdrWorkerTestOptions = {}) {
	let state: State = { workers: [] };
	let ctxRef: ExtensionContext | undefined;
	let selfInfo: AgentInfo | undefined;
	let stopped = false;
	const lifetime = new AbortController();
	let createQueue = Promise.resolve();
	let lifecycleAcceptor: LifecycleAcceptor | undefined;
	let runRegistry: RunRegistry | undefined;
	let runQueryServer: RunQueryServer | undefined;
	let reconciliationServer: ReconciliationServer | undefined;
	let providerLifecycleSequence = 0;
	const pendingSpawnLifecycles = new Map<string, PendingSpawnLifecycle>();
	const interactive = () => HERDR_ENV && !!SELF_PANE && ctxRef?.mode === "tui" && !stopped;
	const providerState = () => {
		if (stopped) return { available: false as const, reason: "SHUTTING_DOWN" as const };
		if (!HERDR_ENV || !SELF_PANE) return { available: false as const, reason: "NOT_IN_HERDR" as const };
		if (ctxRef && ctxRef.mode !== "tui") return { available: false as const, reason: "NOT_INTERACTIVE" as const };
		if (!ctxRef) return { available: false as const, reason: "SESSION_NOT_READY" as const };
		return { available: true as const };
	};

	pi.registerFlag("orchestrated-by", {
		description: "Herdr worker: id (agent name or pane id) of the orchestrator controlling this pi",
		type: "string",
	});
	pi.registerFlag("team-role", {
		description: "Herdr worker: '<type>: <purpose>' describing what this worker is for (set by the orchestrator)",
		type: "string",
	});
	pi.registerFlag("worker-run-id", {
		description: "Herdr worker: internal assignment run identity",
		type: "string",
	});
	pi.registerFlag("worker-correlation-id", {
		description: "Herdr worker: internal caller correlation identity",
		type: "string",
	});
	pi.registerFlag("worker-lifecycle-protocol", {
		description: "Herdr worker: lifecycle/report contract selected for this assignment",
		type: "string",
	});

	// ── herdr helpers ──

	async function herdr(args: string[], opts: { timeout?: number; signal?: AbortSignal } = {}): Promise<any> {
		const operationSignal = opts.signal ?? ctxRef?.signal;
		const signal = operationSignal ? AbortSignal.any([operationSignal, lifetime.signal]) : lifetime.signal;
		signal.throwIfAborted();
		const res = await pi.exec("herdr", args, { timeout: opts.timeout ?? 15000, signal });
		signal.throwIfAborted();
		const out = (res.stdout || "").trim();
		const err = (res.stderr || "").trim();
		if (res.code !== 0) {
			let msg = err || out || `herdr ${args[0]} ${args[1] ?? ""} failed (exit ${res.code})`;
			try {
				const j = JSON.parse(err || out);
				msg = j.error?.message ? `${j.error.code}: ${j.error.message}` : msg;
			} catch {}
			throw new Error(msg);
		}
		try {
			return JSON.parse(out);
		} catch {
			return out;
		}
	}

	function toAgentInfo(a: any): AgentInfo | undefined {
		if (!a?.pane_id) return undefined;
		return { paneId: a.pane_id, tabId: a.tab_id ?? undefined, name: a.name ?? undefined, kind: a.agent ?? undefined, status: a.agent_status, cwd: a.cwd };
	}

	async function agentGet(target: string, signal?: AbortSignal): Promise<AgentInfo | undefined> {
		try {
			const j = await herdr(["agent", "get", target], { signal });
			return toAgentInfo(j?.result?.agent);
		} catch {
			return undefined;
		}
	}

	async function agentList(): Promise<AgentInfo[]> {
		try {
			const j = await herdr(["agent", "list"]);
			return (j?.result?.agents ?? []).map(toAgentInfo).filter(Boolean) as AgentInfo[];
		} catch {
			return [];
		}
	}

	async function refreshSelf(): Promise<AgentInfo> {
		selfInfo = (SELF_PANE && (await agentGet(SELF_PANE))) || { paneId: SELF_PANE };
		return selfInfo;
	}

	function selfId(): string {
		return selfInfo?.name || selfInfo?.paneId || SELF_PANE || "unknown";
	}

	function myRoleToward(targetId: string, target?: AgentInfo): Sender["role"] {
		const ids = [targetId, target?.name, target?.paneId].filter(Boolean) as string[];
		if (ids.some((id) => state.workers.includes(id))) return "orchestrator";
		if (state.orchestratedBy && ids.includes(state.orchestratedBy)) return "worker";
		return "agent";
	}

	// ── persistence + ui ──

	function persist() {
		if (stopped || !ctxRef) return;
		state.version = 1;
		state.sessionId = ctxRef.sessionManager.getSessionId();
		pi.appendEntry(ENTRY_TYPE, structuredClone(state));
	}

	function restore(ctx: ExtensionContext) {
		let found: State | undefined;
		for (const entry of ctx.sessionManager.getBranch()) {
			if (entry.type === "custom" && entry.customType === ENTRY_TYPE) found = entry.data as State;
		}
		// A clone must not acquire the source session's live team authority.
		if (found?.sessionId && found.sessionId !== ctx.sessionManager.getSessionId()) found = undefined;
		state = found && Array.isArray(found.workers) ? structuredClone(found) : { workers: [] };
		if (state.activeRun && (!isWorkerRunBinding(state.activeRun)
			|| typeof state.activeRun.sourceInstanceId !== "string"
			|| !/^[A-Za-z0-9._-]{1,128}$/.test(state.activeRun.sourceInstanceId)
			|| !Number.isInteger(state.activeRun.sourceSequence)
			|| state.activeRun.sourceSequence < 0)) {
			state.activeRun = undefined;
		}
	}

	function statusText(): string | undefined {
		const parts: string[] = [];
		if (state.workers.length) parts.push(`team · ${state.workers.length} worker${state.workers.length === 1 ? "" : "s"} · /team list`);
		else if (state.teamMode && !state.orchestratedBy) parts.push("team · orchestrator");
		if (state.orchestratedBy) parts.push(`team ⇐ ${state.orchestratedBy.replace(/[\x00-\x1f\x7f-\x9f]/g, " ").slice(0, 40)}`);
		return parts.length ? parts.join(" · ") : undefined;
	}

	function inTeam(): boolean {
		return interactive() && (state.workers.length > 0 || !!state.orchestratedBy);
	}

	function isOrchestrator(): boolean {
		return interactive() && !!state.teamMode && !state.orchestratedBy;
	}

	/**
	 * SendToAgent exists only while we are part of a team (orchestrating or orchestrated).
	 * CreateAgentPanel exists only for an orchestrator (after /team was used here; never for workers).
	 */
	function syncTool() {
		const active = pi.getActiveTools();
		const want = new Map<string, boolean>([
			[TOOL_NAME, inTeam()],
			[CREATE_TOOL, isOrchestrator()],
			[REPORT_TOOL, interactive() && !!state.orchestratedBy && !!state.activeRun],
		]);
		let next = active.filter((t) => want.get(t) !== false);
		for (const [tool, on] of want) if (on && !next.includes(tool)) next = [...next, tool];
		if (next.length !== active.length || next.some((t, i) => t !== active[i])) pi.setActiveTools(next);
	}

	function updateUi() {
		syncTool();
		const ctx = ctxRef;
		if (!ctx?.hasUI) return;
		const text = statusText();
		ctx.ui.setStatus(STATUS_KEY, text);
		void updatePaneTitle();
	}

	async function updatePaneTitle() {
		if (!HERDR_ENV || !SELF_PANE) return;
		try {
			if (state.orchestratedBy) {
				const me = selfInfo?.name ? `${selfInfo.name} ` : "";
				await herdr(["pane", "report-metadata", SELF_PANE, "--source", META_SOURCE, "--title", `${me}⇐ ${state.orchestratedBy}`]);
			} else if (state.workers.length) {
				await herdr(["pane", "report-metadata", SELF_PANE, "--source", META_SOURCE, "--title", `orchestrating ${state.workers.join(", ")}`]);
			} else {
				await herdr(["pane", "report-metadata", SELF_PANE, "--source", META_SOURCE, "--clear-title"]);
			}
		} catch {}
	}

	// ── inbox (receiving) ──

	let acknowledgementContext: ExtensionContext | undefined;
	const mailbox = (testOptions.createMailbox ?? createMailboxTransport)({
		selfId,
		isInteractive: interactive,
		isStopped: () => stopped,
		deliver,
		getAcknowledgedEntries: () => (acknowledgementContext ?? ctxRef)?.sessionManager.getEntries()
			.filter((entry) => entry.type === "custom_message")
			.filter((entry) => entry.customType === MESSAGE_CUSTOM_TYPE || entry.customType === LIFECYCLE_REPORT_CUSTOM_TYPE) ?? [],
		handleAcknowledged: (entry) => entry.customType !== LIFECYCLE_REPORT_CUSTOM_TYPE || acceptPersistedLifecycleReport(entry),
		warn: (error) => {
			if (ctxRef?.hasUI) ctxRef.ui.notify(`Team inbox: ${error instanceof Error ? error.message : String(error)}`, "warning");
		},
	}, { root: mailboxRoot(), paneId: SELF_PANE });

	/**
	 * Resolve our known peers (workers + orchestrator) to live pane ids. A sender is trusted only if
	 * its claimed pane currently hosts one of those peers — the inbox is writable by anyone local.
	 */
	async function knownPeerPanes(): Promise<Map<string, string>> {
		const ids = [...state.workers, ...(state.orchestratedBy ? [state.orchestratedBy] : [])];
		const out = new Map<string, string>(); // paneId -> id we know it by
		if (!ids.length) return out;
		const agents = await agentList();
		for (const id of ids) {
			const a = agents.find((x) => x.name === id || x.paneId === id);
			if (a) out.set(a.paneId, id);
		}
		return out;
	}

	function acceptPersistedLifecycleReport(value: unknown): boolean {
		const entry = value as { customType?: unknown; details?: unknown } | undefined;
		if (entry?.customType !== LIFECYCLE_REPORT_CUSTOM_TYPE || !lifecycleAcceptor) return false;
		const details = entry.details as { from?: Sender; report?: unknown } | undefined;
		if (!details?.from || typeof details.from.paneId !== "string" || !isWorkerRunReport(details.report)) return true;
		const report = details.report;
		const record = lifecycleAcceptor.getRun(report.runId);
		if (!record) return false;
		if (record.worker.paneId !== details.from.paneId) return true;
		try {
			lifecycleAcceptor.accept({
				...report,
				worker: { ...record.worker },
				source: "worker",
				...(record.correlationId === undefined ? {} : { correlationId: record.correlationId }),
			});
			return true;
		} catch {
			return false;
		}
	}

	async function deliver(env: unknown, envelopeId: string) {
		const ctx = ctxRef;
		if (!env || typeof env !== "object") return;
		const envelope = env as Partial<Envelope> & { from?: Partial<Sender> };
		if (!envelope.from || typeof envelope.from.id !== "string" || typeof envelope.from.paneId !== "string") return;
		const peers = await knownPeerPanes();
		if (stopped) return;
		const known = peers.get(envelope.from.paneId);

		if (envelope.type === "control") {
			if (envelope.action === "orchestrated-by") {
				// Adoption handshake: only while we have no orchestrator (or it is the current one re-asserting).
				if (state.orchestratedBy && !known) {
					ctx?.hasUI && ctx.ui.notify(`Ignored adoption request from ${envelope.from.id} (pane ${envelope.from.paneId}); already orchestrated by ${state.orchestratedBy}.`, "warning");
					return;
				}
				if (state.orchestratedBy !== envelope.from.id) state.activeRun = undefined;
				state.orchestratedBy = envelope.from.id;
				persist();
				updateUi();
				ctx?.hasUI && ctx.ui.notify(`Now orchestrated by ${envelope.from.id}`, "info");
			} else if (envelope.action === "released" && known && state.orchestratedBy === envelope.from.id) {
				state.orchestratedBy = undefined;
				state.activeRun = undefined;
				persist();
				updateUi();
				ctx?.hasUI && ctx.ui.notify(`Released by orchestrator ${envelope.from.id}`, "info");
			} else if (envelope.action === "bind-run"
				&& (!state.orchestratedBy || (known && state.orchestratedBy === envelope.from.id))
				&& isWorkerRunBinding(envelope.binding)) {
				state.orchestratedBy ??= envelope.from.id;
				state.activeRun = { ...envelope.binding, sourceInstanceId: randomUUID(), sourceSequence: 0 };
				persist();
				updateUi();
				void reportWorkerReady().catch(() => {});
			}
			return;
		}

		if (!known) {
			ctx?.hasUI &&
				ctx.ui.notify(
					`Dropped message from unknown agent ${envelope.from.id} (pane ${envelope.from.paneId}). Only your orchestrator and workers may message you; use /team adopt or /orchestrated-by to allow it.`,
					"warning",
				);
			return;
		}
		if (envelope.type === "lifecycle") {
			if (!isWorkerRunReport(envelope.report)) return;
			const record = lifecycleAcceptor?.getRun(envelope.report.runId);
			if (!record || record.worker.paneId !== envelope.from.paneId) return;
			mailbox.markInFlight(envelopeId);
			pi.sendMessage({
				customType: LIFECYCLE_REPORT_CUSTOM_TYPE,
				content: `Worker ${record.worker.name} reported ${envelope.report.status} for run ${envelope.report.runId}.`,
				display: true,
				details: { envelopeId, from: envelope.from, report: envelope.report },
			}, { triggerTurn: true, deliverAs: "followUp" });
			return;
		}
		if (envelope.type !== "message" || typeof envelope.message !== "string" || typeof envelope.priority !== "boolean") return;
		const messageEnvelope = envelope as MessageEnvelope;
		mailbox.markInFlight(envelopeId);
		// Acknowledgement observes SessionManager memory, not a confirmed session-file write.
		pi.sendMessage({ customType: MESSAGE_CUSTOM_TYPE, content: frameMessage(messageEnvelope), display: true,
			details: { envelopeId, from: messageEnvelope.from } }, { triggerTurn: true, deliverAs: messageEnvelope.priority ? "steer" : "followUp" });
	}
	testOptions.onInboxHandler?.(deliver);

	// ── sending ──

	function isListening(paneId: string): boolean {
		if (testOptions.isListening) return testOptions.isListening(paneId);
		return mailbox.isListening(paneId);
	}

	function writeEnvelope(paneId: string, env: Envelope) {
		if (testOptions.writeEnvelope) {
			testOptions.writeEnvelope(paneId, env);
			return;
		}
		mailbox.writeEnvelope(paneId, env);
	}

	async function makeSender(targetId: string, target: AgentInfo | undefined): Promise<Sender> {
		if (!selfInfo) await refreshSelf();
		return {
			id: selfId(),
			paneId: SELF_PANE,
			...(selfInfo?.name === undefined ? {} : { name: selfInfo.name }),
			role: myRoleToward(targetId, target),
		};
	}

	async function makeMessageEnvelope(targetId: string, target: AgentInfo | undefined, message: string, priority: boolean, runId?: string): Promise<MessageEnvelope> {
		return {
			type: "message",
			from: await makeSender(targetId, target),
			message,
			priority,
			ts: Date.now(),
			...(runId === undefined ? {} : { runId }),
		};
	}

	class SendServiceError extends WorkerRpcServiceError {
		constructor(code: "NOT_FOUND" | "NOT_TEAM_MEMBER", message: string, readonly toolMessage: string) {
			super(code, message);
		}
	}

	async function send(targetId: string, message: string, priority: boolean, signal?: AbortSignal, runId?: string): Promise<DeliveryReceipt> {
		if (!HERDR_ENV) throw new Error("Not running inside herdr (HERDR_ENV != 1); SendToAgent is unavailable.");
		const target = await agentGet(targetId);
		if (!target) {
			const known = (await agentList()).map((a) => a.name ?? a.paneId);
			throw new SendServiceError("NOT_FOUND", "Target agent was not found.", `No live herdr agent "${targetId}". Known agents: ${known.join(", ") || "(none)"}${state.workers.length ? `. Your workers: ${state.workers.join(", ")}` : ""}${state.orchestratedBy ? `. Your orchestrator: ${state.orchestratedBy}` : ""}`);
		}
		if (target.paneId === SELF_PANE) throw new SendServiceError("NOT_TEAM_MEMBER", "Target is not a team member.", "Refusing to send a message to yourself.");
		const peer = state.workers.includes(targetId) || state.workers.includes(target.name ?? "") || state.workers.includes(target.paneId) || [state.orchestratedBy].includes(targetId) || [state.orchestratedBy].includes(target.name ?? "") || [state.orchestratedBy].includes(target.paneId);
		if (!peer) {
			throw new SendServiceError("NOT_TEAM_MEMBER", "Target is not a team member.", `"${targetId}" is not in your team (it would drop the message anyway). Workers: ${state.workers.join(", ") || "(none)"}; orchestrator: ${state.orchestratedBy ?? "(none)"}. Use /team add or /team adopt first.`);
		}

		const env = await makeMessageEnvelope(targetId, target, message, priority, runId);
		const label = target.name ?? target.paneId;

		if (target.kind === "pi" && isListening(target.paneId)) {
			writeEnvelope(target.paneId, env);
			return { target: label, paneId: target.paneId, ...(target.kind === undefined ? {} : { kind: target.kind }), ...(target.status === undefined ? {} : { status: target.status }), transport: "inbox", requestedMode: priority ? "steer" : "follow-up", priorityApplied: priority };
		}

		// Fallback: type it into the agent's pane. No steer/follow-up control here.
		await herdr(["agent", "prompt", target.paneId, frameMessage(env)], { timeout: 20000, signal });
		return { target: label, paneId: target.paneId, ...(target.kind === undefined ? {} : { kind: target.kind }), ...(target.status === undefined ? {} : { status: target.status }), transport: "herdr-prompt", requestedMode: priority ? "steer" : "follow-up", priorityApplied: false };
	}

	function sendReceiptText(receipt: DeliveryReceipt): string {
		if (receipt.transport === "inbox") return `Delivered to ${receipt.target} (pane ${receipt.paneId}, ${receipt.status ?? "unknown"}) via inbox as ${receipt.requestedMode === "steer" ? "steer (priority)" : "follow-up"}. Replies arrive on a later turn.`;
		return `Typed into ${receipt.target} (pane ${receipt.paneId}, ${receipt.kind ?? "agent"}, no inbox listener) via \`herdr agent prompt\`. Priority flag not applicable there.`;
	}

	async function sendControl(targetId: string, action: ControlEnvelope["action"], binding: WorkerRunBinding | undefined = undefined): Promise<boolean> {
		const target = await agentGet(targetId);
		if (!target || !isListening(target.paneId)) return false;
		const env: ControlEnvelope = {
			type: "control",
			from: await makeSender(targetId, target),
			action,
			...(binding === undefined ? {} : { binding }),
			ts: Date.now(),
		};
		writeEnvelope(target.paneId, env);
		return true;
	}

	async function sendWorkerReport(input: WorkerRunReportInput | WorkerRunReportInputV1): Promise<WorkerRunReport> {
		const activeRun = state.activeRun;
		if (!activeRun || !state.orchestratedBy) throw new Error("No active worker run is bound.");
		if (!isWorkerRunReportInput(input, activeRun.protocol)) throw new Error("Invalid worker lifecycle report for the bound contract.");
		const target = await agentGet(state.orchestratedBy);
		if (!target || !isListening(target.paneId)) throw new Error("The orchestrator is not listening for worker lifecycle reports.");
		const sourceSequence = activeRun.sourceSequence + 1;
		const evidence = input.status === "message"
			? { kind: "worker_message" as const, message: input.message }
			: input.status === "completed"
				? activeRun.protocol === LIFECYCLE_PROTOCOL_V2
					? {
						kind: "worker_completed_v2" as const,
						result: (input as WorkerRunReportInputV2 & { status: "completed" }).result,
						...((input as WorkerRunReportInputV2 & { status: "completed" }).artifacts === undefined ? {} : { artifacts: (input as WorkerRunReportInputV2 & { status: "completed" }).artifacts }),
						...((input as WorkerRunReportInputV2 & { status: "completed" }).checks === undefined ? {} : { checks: (input as WorkerRunReportInputV2 & { status: "completed" }).checks }),
					}
					: { kind: "worker_completed" as const, ...(input.result === undefined ? {} : { result: input.result }) }
				: { kind: "worker_failed" as const, error: input.error };
		const report = {
			protocol: activeRun.protocol,
			eventId: randomUUID(),
			runId: activeRun.runId,
			sourceInstanceId: activeRun.sourceInstanceId,
			sourceSequence,
			observedAt: Date.now(),
			status: input.status,
			evidence,
		} as WorkerRunReport;
		state.activeRun = { ...activeRun, sourceSequence };
		persist();
		writeEnvelope(target.paneId, {
			type: "lifecycle",
			from: await makeSender(state.orchestratedBy, target),
			report,
			ts: Date.now(),
		});
		return report;
	}

	async function reportWorkerReady(): Promise<void> {
		const activeRun = state.activeRun;
		if (!activeRun || !state.orchestratedBy) return;
		const target = await agentGet(state.orchestratedBy);
		if (!target || !isListening(target.paneId)) return;
		const sourceSequence = activeRun.sourceSequence + 1;
		const report: WorkerRunReport = {
			protocol: activeRun.protocol,
			eventId: randomUUID(),
			runId: activeRun.runId,
			sourceInstanceId: activeRun.sourceInstanceId,
			sourceSequence,
			observedAt: Date.now(),
			status: "started",
			evidence: { kind: "worker_ready", readiness: "confirmed" },
		};
		state.activeRun = { ...activeRun, sourceSequence };
		persist();
		writeEnvelope(target.paneId, {
			type: "lifecycle",
			from: await makeSender(state.orchestratedBy, target),
			report,
			ts: Date.now(),
		});
	}

	// ── tool ──

	pi.registerTool({
		name: "SendToAgent",
		label: "Send To Agent",
		description:
			"Send a message to another of your user's agents running in a herdr pane (an orchestrator or a worker). " +
			"The message is delivered asynchronously and shows up in their chat as an '[agent] ...' custom message; replies come back the same way on a later turn. " +
			"target_id is the herdr agent name (e.g. a worker name) or pane id. priority=true steers the target mid-task (interrupts its current turn); priority=false queues a follow-up after its current work finishes.",
		promptSnippet: "Message another herdr-hosted agent (orchestrator ↔ worker) asynchronously",
		promptGuidelines: [
			"Use SendToAgent for questions and ordinary communication. A bound worker must use ReportWorkerRun for terminal completion or failure.",
			"When delegating to workers, write self-contained messages with goal, context, constraints, and what to report back.",
			"SendToAgent is fire-and-forget: do not wait or poll for a reply — end your turn and react when the '[agent]' message arrives.",
		],
		parameters: Type.Object({
			message: Type.String({ description: "Full message body (Markdown). Self-contained: the recipient does not see your conversation." }),
			target_id: Type.String({ description: "Herdr agent name or pane id of the recipient" }),
			priority: Type.Optional(Type.Boolean({ description: "true = steer (interrupt recipient's current turn). false/omitted = follow-up after its current work.", default: false })),
		}),
		async execute(_id, params, signal) {
			let receipt: DeliveryReceipt;
			try {
				receipt = await workerService.send({ target: params.target_id, message: params.message, priority: params.priority ?? false }, signal);
			} catch (error) {
				if (error instanceof SendServiceError) throw new Error(error.toolMessage);
				throw error;
			}
			const text = sendReceiptText(receipt);
			return { content: [{ type: "text", text }], details: { target: params.target_id, priority: params.priority ?? false, message: params.message, status: text } };
		},
		// Inline UX: the interesting part is the message itself, not that one was sent.
		renderCall(args, theme) {
			const arrow = args.priority ? theme.fg("warning", " ⇨ ") : theme.fg("muted", " → ");
			let head = theme.fg("toolTitle", theme.bold("SendToAgent")) + arrow + theme.fg("accent", args.target_id ?? "…");
			if (args.priority) head += theme.fg("warning", " (priority / steer)");
			return new Text(head, 0, 0);
		},
		renderResult(result, { expanded, isPartial }, theme, context) {
			const d = (result.details ?? {}) as { message?: string; status?: string };
			const message = d.message ?? (context.args as any)?.message ?? "";
			if (isPartial) return new Text(theme.fg("dim", "sending…"), 0, 0);
			if (context.isError) {
				const err = result.content?.map((c: any) => c.text ?? "").join("\n") ?? "failed";
				return new Text(theme.fg("error", err), 0, 0);
			}
			const lines = message.split("\n");
			const maxLines = expanded ? Number.POSITIVE_INFINITY : 12;
			const shown = lines.slice(0, maxLines);
			let out = shown.map((l: string) => theme.fg("accent", "│ ") + l).join("\n");
			if (lines.length > shown.length) out += "\n" + theme.fg("dim", `│ … ${lines.length - shown.length} more lines`);
			if (d.status) out += "\n" + theme.fg("dim", d.status);
			return new Text(out, 0, 0);
		},
	});

	pi.registerTool({
		name: REPORT_TOOL,
		label: "Report Worker Run",
		description: "Report an authoritative message, successful completion, or failure for the worker's currently bound assignment. Identity and ordering are supplied by the extension.",
		promptSnippet: "Report progress or the explicit outcome of the current worker assignment",
		promptGuidelines: [
			"ReportWorkerRun is the only terminal reporting path for a bound assignment. Use SendToAgent only for questions and ordinary communication.",
			"Report completed only after the assignment is actually complete, with a non-empty result and any artifact or verification references. Report failed with a concrete error when it cannot be completed.",
		],
		parameters: WorkerRunReportInputSchema,
		async execute(_id, params) {
			const report = await sendWorkerReport(params);
			const text = report.status === "message"
				? "Worker message reported to the orchestrator."
				: `Worker run reported ${report.status} to the orchestrator.`;
			return { content: [{ type: "text", text }], details: { runId: report.runId, eventId: report.eventId, status: report.status } };
		},
	});

	// ── orchestrator commands ──

	async function adopt(name: string): Promise<string> {
		const existing = await agentGet(name);
		if (!existing) throw new Error(`No live herdr agent "${name}" to adopt.`);
		if (existing.paneId === SELF_PANE) throw new Error("That is this pane.");
		await refreshSelf();
		if (!state.workers.includes(name)) state.workers.push(name);
		persist();
		updateUi();
		const ok = await sendControl(name, "orchestrated-by");
		return ok
			? `Adopted existing agent ${name} (pane ${existing.paneId}) as worker.`
			: `Adopted ${name} (pane ${existing.paneId}) — it is not a listening pi; run \`/orchestrated-by ${selfId()}\` inside it (or it can't SendToAgent back).`;
	}

	/**
	 * All workers are named `agent-<something>` (herdr agent names are global, so the prefix both
	 * avoids clashes with hand-started agents and marks the pane as part of the worker stack).
	 * `/team add`           -> agent-1, agent-2, … (first free number)
	 * `/team add ci`        -> agent-ci, or agent-ci-2, … if taken
	 */
	async function uniqueName(requested: string): Promise<string> {
		const taken = new Set((await agentList()).flatMap((a) => (a.name ? [a.name] : [])));
		if (!requested) {
			for (let i = 1; i < 1000; i++) if (!taken.has(`${WORKER_PREFIX}${i}`)) return `${WORKER_PREFIX}${i}`;
			throw new Error("Could not find a free agent-N name");
		}
		const base = (requested.startsWith(WORKER_PREFIX) ? requested : `${WORKER_PREFIX}${requested}`).slice(0, 32);
		if (!taken.has(base)) return base;
		for (let i = 2; i < 1000; i++) {
			const suffix = `-${i}`;
			const candidate = `${base.slice(0, 32 - suffix.length)}${suffix}`;
			if (!taken.has(candidate)) return candidate;
		}
		throw new Error(`Could not find a free name based on "${base}"`);
	}

	/**
	 * Workers live in a stack on one side of the orchestrator (default: a column to the right).
	 * - Nothing (or a non-worker) on that side -> split the orchestrator toward `dir`.
	 *   herdr can only split right/down, so left/up = split right/down, then swap the two panes.
	 * - An `agent-*` pane on that side -> walk along the stack (down for a left/right column,
	 *   right for an up/down row) and split the last one to extend it.
	 */
	async function pickSplit(dir: Direction): Promise<{ pane: string; direction: "right" | "down"; swap: boolean; how: string }> {
		// `herdr pane neighbor` is focus-relative, so derive adjacency from the layout rects.
		type Rect = { x: number; y: number; width: number; height: number };
		const layout = await herdr(["pane", "layout", "--pane", SELF_PANE]).catch(() => undefined);
		const rects = new Map<string, Rect>();
		for (const p of layout?.result?.layout?.panes ?? []) if (p?.pane_id && p.rect) rects.set(p.pane_id, p.rect);
		const agents = await agentList();
		const workerPanes = new Set(agents.filter((a) => a.tabId === SELF_TAB && a.name?.startsWith(WORKER_PREFIX)).map((a) => a.paneId));
		const overlaps = (a0: number, a1: number, b0: number, b1: number) => Math.min(a1, b1) - Math.max(a0, b0) > 0;
		const adjacent = (id: string, d: Direction): string | undefined => {
			const r = rects.get(id);
			if (!r) return undefined;
			const hit = [...rects].find(([pid, o]) => {
				if (pid === id) return false;
				switch (d) {
					case "right": return Math.abs(o.x - (r.x + r.width)) <= 1 && overlaps(r.y, r.y + r.height, o.y, o.y + o.height);
					case "left": return Math.abs(r.x - (o.x + o.width)) <= 1 && overlaps(r.y, r.y + r.height, o.y, o.y + o.height);
					case "down": return Math.abs(o.y - (r.y + r.height)) <= 1 && overlaps(r.x, r.x + r.width, o.x, o.x + o.width);
					case "up": return Math.abs(r.y - (o.y + o.height)) <= 1 && overlaps(r.x, r.x + r.width, o.x, o.x + o.width);
				}
			});
			return hit?.[0];
		};

		const first = adjacent(SELF_PANE, dir);
		if (!first || !workerPanes.has(first)) {
			const native: "right" | "down" = dir === "left" || dir === "right" ? "right" : "down";
			return { pane: SELF_PANE, direction: native, swap: dir === "left" || dir === "up", how: `new ${native === "right" ? "column" : "row"} ${dir}` };
		}
		const along: Direction = dir === "left" || dir === "right" ? "down" : "right";
		let last = first;
		for (let i = 0; i < 64; i++) {
			const next = adjacent(last, along);
			if (!next || !workerPanes.has(next)) break;
			last = next;
		}
		return { pane: last, direction: along, swap: false, how: `stacked ${along === "down" ? "below" : "right of"} ${last}` };
	}

	interface CreateResult {
		runId: string;
		correlationId?: string;
		name: string;
		paneId: string;
		model?: string;
		type?: string;
		purpose?: string;
		cwd: string;
		how: string;
		adopted: boolean;
	}

	interface WorkerService {
		spawn(input: SpawnInput, signal?: AbortSignal, provenance?: SpawnProvenance): Promise<CreateResult>;
		send(input: SendInput, signal?: AbortSignal): Promise<DeliveryReceipt>;
		inspect(input: InspectInput, signal?: AbortSignal): Promise<Inspection>;
	}

	function bindLifecycleRun(opts: CreateOpts, name: string, paneId: string): boolean {
		return lifecycleAcceptor?.bindRun({
			runId: opts.runId,
			lifecycleProtocol: opts.lifecycleProtocol,
			...(opts.correlationId === undefined ? {} : { correlationId: opts.correlationId }),
			worker: { name, paneId },
			...(opts.provenance?.requestId === undefined ? {} : { requestId: opts.provenance.requestId }),
			providerInstanceId: opts.provenance?.providerInstanceId ?? rpcServer.providerInstanceId,
		}) ?? false;
	}

	function observeProviderStarted(opts: CreateOpts, name: string, paneId: string): void {
		try {
			if (!bindLifecycleRun(opts, name, paneId)) return;
			lifecycleAcceptor?.accept({
				protocol: opts.lifecycleProtocol,
				eventId: randomUUID(),
				runId: opts.runId,
				sourceInstanceId: opts.provenance?.providerInstanceId ?? rpcServer.providerInstanceId,
				sourceSequence: ++providerLifecycleSequence,
				status: "started",
				worker: { name, paneId },
				observedAt: Date.now(),
				source: "provider",
				...(opts.correlationId === undefined ? {} : { correlationId: opts.correlationId }),
				evidence: { kind: "agent_start_returned", readiness: "unconfirmed" },
			});
		} catch {
			// Lifecycle persistence and publication must not change worker creation outcomes.
		}
	}

	function observeProviderUncertain(
		opts: CreateOpts,
		name: string,
		paneId: string | undefined,
		scope: PendingSpawnLifecycle["scope"],
		detail: string,
	): void {
		try {
			const bound = paneId === undefined
				? lifecycleAcceptor?.bindRun({
					runId: opts.runId,
					lifecycleProtocol: opts.lifecycleProtocol,
					...(opts.correlationId === undefined ? {} : { correlationId: opts.correlationId }),
					worker: { name },
					...(opts.provenance?.requestId === undefined ? {} : { requestId: opts.provenance.requestId }),
					providerInstanceId: opts.provenance?.providerInstanceId ?? rpcServer.providerInstanceId,
				})
				: bindLifecycleRun(opts, name, paneId);
			if (!bound) return;
			lifecycleAcceptor?.accept({
				protocol: opts.lifecycleProtocol,
				eventId: randomUUID(),
				runId: opts.runId,
				sourceInstanceId: opts.provenance?.providerInstanceId ?? rpcServer.providerInstanceId,
				sourceSequence: ++providerLifecycleSequence,
				status: "uncertain",
				worker: { name, ...(paneId === undefined ? {} : { paneId }) },
				observedAt: Date.now(),
				source: "provider",
				...(opts.correlationId === undefined ? {} : { correlationId: opts.correlationId }),
				evidence: { kind: "uncertain", scope, detail },
			});
		} catch {
			// Lifecycle persistence and publication must not replace the operation's own failure.
		}
	}

	async function createAgent(opts: CreateOpts, ctx: ExtensionContext, cwd: string): Promise<CreateResult> {
		if (!HERDR_ENV || !SELF_PANE) throw new Error("Not running inside herdr.");
		const dir: Direction = opts.direction ?? "right";
		const type = opts.type;
		const requested = (opts.name?.trim() || type || "").toLowerCase();
		const model = opts.model;

		await refreshSelf();
		// Give ourselves an addressable name if we don't have one.
		if (!selfInfo?.name) {
			try {
				await herdr(["agent", "rename", SELF_PANE, "orchestrator"]);
				await refreshSelf();
			} catch {}
		}
		const me = selfId();

		// Re-adopt an existing same-tab worker with that name (e.g. after /reload or a dropped session).
		if (requested) {
			const wanted = requested.startsWith(WORKER_PREFIX) ? requested : `${WORKER_PREFIX}${requested}`;
			const existing = await agentGet(wanted);
			if (existing && existing.paneId !== SELF_PANE && existing.tabId === SELF_TAB && !state.workers.includes(wanted)) {
				runRegistry?.bindEndpoint({
					version: 1,
					runId: opts.runId,
					sessionId: ctx.sessionManager.getSessionId(),
					agentName: wanted,
					paneId: existing.paneId,
					observedAt: Date.now(),
				});
				await adopt(wanted);
				const priorMeta = state.meta?.[wanted];
				state.meta = { ...(state.meta ?? {}), [wanted]: {
					...priorMeta,
					paneId: existing.paneId,
					runId: opts.runId,
					correlationId: opts.correlationId,
					requestId: opts.provenance?.requestId,
					providerInstanceId: opts.provenance?.providerInstanceId,
					lifecycleProtocol: opts.lifecycleProtocol,
				} };
				persist();
				bindLifecycleRun(opts, wanted, existing.paneId);
				pendingSpawnLifecycles.set(opts.runId, { opts, name: wanted, paneId: existing.paneId, scope: "assignment_delivery" });
				try {
					await sendControl(wanted, "bind-run", { protocol: opts.lifecycleProtocol, runId: opts.runId, ...(opts.correlationId === undefined ? {} : { correlationId: opts.correlationId }) });
					if (opts.initialPrompt) await send(wanted, opts.initialPrompt, false, undefined, opts.runId);
				} catch (error) {
					observeProviderUncertain(opts, wanted, existing.paneId, "assignment_delivery", "The re-adopted worker assignment delivery outcome is ambiguous.");
					throw error;
				} finally {
					pendingSpawnLifecycles.delete(opts.runId);
				}
				const meta = state.meta?.[wanted];
				return { runId: opts.runId, correlationId: opts.correlationId, name: wanted, paneId: existing.paneId, model: meta?.model, type: meta?.type, purpose: meta?.purpose, cwd: existing.cwd?.trim() ? existing.cwd : cwd, how: "re-adopted existing pane", adopted: true };
			}
		}
		const name = await uniqueName(requested);

		// Create the pane: start a worker stack on the requested side, or extend the existing one.
		const target = await pickSplit(dir);
		pendingSpawnLifecycles.set(opts.runId, { opts, name, scope: "pane_creation" });
		let split: any;
		try {
			split = await herdr(["pane", "split", target.pane, "--direction", target.direction, "--cwd", cwd, "--no-focus"]);
		} catch (error) {
			observeProviderUncertain(opts, name, undefined, "pane_creation", "The pane creation outcome is ambiguous.");
			pendingSpawnLifecycles.delete(opts.runId);
			throw error;
		}
		const paneId: string | undefined = split?.result?.pane?.pane_id;
		if (!paneId) {
			observeProviderUncertain(opts, name, undefined, "pane_creation", "Pane creation returned without a pane identity, so external effects cannot be excluded.");
			pendingSpawnLifecycles.delete(opts.runId);
			throw new Error(`pane split returned no pane id: ${JSON.stringify(split)}`);
		}
		try {
			runRegistry?.bindEndpoint({
				version: 1,
				runId: opts.runId,
				sessionId: ctx.sessionManager.getSessionId(),
				agentName: name,
				paneId,
				observedAt: Date.now(),
			});
		} catch (error) {
			observeProviderUncertain(opts, name, paneId, "pane_creation", "A pane was created but its endpoint binding could not be persisted.");
			pendingSpawnLifecycles.delete(opts.runId);
			throw error;
		}
		pendingSpawnLifecycles.set(opts.runId, { opts, name, paneId, scope: "agent_start" });
		try {
			pi.appendEntry("herdr-worker.operation.v1", { name, paneId, phase: "pane-created", at: Date.now() });
		} catch (error) {
			observeProviderUncertain(opts, name, paneId, "pane_creation", "A pane was created but its operation checkpoint could not be persisted.");
			pendingSpawnLifecycles.delete(opts.runId);
			throw error;
		}
		if (target.swap) {
			await herdr(["pane", "swap", "--source-pane", SELF_PANE, "--target-pane", paneId]).catch(() => {});
		}

		await new Promise((r) => setTimeout(r, 1200)); // let the shell come up
		const piArgs = ["--orchestrated-by", me, "--worker-run-id", opts.runId];
		piArgs.push("--worker-lifecycle-protocol", String(opts.lifecycleProtocol));
		if (opts.correlationId) piArgs.push("--worker-correlation-id", opts.correlationId);
		if (model) piArgs.push("--model", opts.thinking ? `${model}:${opts.thinking}` : model);
		const role = [type, opts.purpose?.trim()].filter(Boolean).join(": ");
		if (role) piArgs.push("--team-role", role);
		try {
			await herdr(["agent", "start", name, "--kind", "pi", "--pane", paneId, "--timeout", "60000", "--", ...piArgs], { timeout: 70000 });
		} catch (e: any) {
			if (!stopped) {
				try { pi.appendEntry("herdr-worker.operation.v1", { name, paneId, phase: "start-failed", at: Date.now() }); } catch {}
			}
			observeProviderUncertain(opts, name, paneId, "agent_start", "Agent start did not return conclusively after pane creation.");
			pendingSpawnLifecycles.delete(opts.runId);
			throw new Error(`Started pane ${paneId} but agent start failed: ${e.message}. Check \`herdr pane read ${paneId}\`.`);
		}

		try {
			if (!state.workers.includes(name)) state.workers.push(name);
			state.meta = { ...(state.meta ?? {}), [name]: {
				type,
				purpose: opts.purpose?.trim() || undefined,
				model,
				paneId,
				runId: opts.runId,
				correlationId: opts.correlationId,
				requestId: opts.provenance?.requestId,
				providerInstanceId: opts.provenance?.providerInstanceId,
				lifecycleProtocol: opts.lifecycleProtocol,
			} };
			persist();
			updateUi();
		} catch (error) {
			observeProviderUncertain(opts, name, paneId, "agent_start", "Agent start returned but the worker relationship could not be persisted.");
			pendingSpawnLifecycles.delete(opts.runId);
			throw error;
		}
		observeProviderStarted(opts, name, paneId);

		if (opts.initialPrompt?.trim()) {
			pendingSpawnLifecycles.set(opts.runId, { opts, name, paneId, scope: "assignment_delivery" });
			// Wait for the worker's extension to come up and register its inbox, then hand over the brief.
			for (let i = 0; i < 20 && !isListening(paneId); i++) await new Promise((r) => setTimeout(r, 500));
			try {
				await send(name, opts.initialPrompt, false, undefined, opts.runId);
			} catch (error) {
				observeProviderUncertain(opts, name, paneId, "assignment_delivery", "The initial assignment delivery outcome is ambiguous after worker start.");
				pendingSpawnLifecycles.delete(opts.runId);
				throw error;
			}
		}
		pendingSpawnLifecycles.delete(opts.runId);
		return { runId: opts.runId, correlationId: opts.correlationId, name, paneId, model, type, purpose: opts.purpose?.trim() || undefined, cwd, how: target.how, adopted: false };
	}

	function validateSpawnCwd(value: string): string {
		if (!path.isAbsolute(value)) throw new Error("Worker cwd must be an absolute accessible directory.");
		let stat: fs.Stats;
		try {
			fs.accessSync(value, fs.constants.R_OK | fs.constants.X_OK);
			stat = fs.statSync(value);
		} catch {
			throw new Error("Worker cwd must be an absolute accessible directory.");
		}
		if (!stat.isDirectory()) throw new Error("Worker cwd must be an absolute accessible directory.");
		return value;
	}

	type SpawnOptions = SpawnInput;
	function resolveAssignment(input: SpawnOptions, ctx: ExtensionContext, cwd: string) {
		const role = input.type?.trim().toLowerCase() || undefined;
		const model = input.model?.trim() || (role && TYPE_MODELS[role]) || (ctx.model ? `${ctx.model.provider}/${ctx.model.id}` : undefined);
		return { cwd, model, role };
	}

	async function spawnWorker(input: SpawnOptions, signal?: AbortSignal, provenance?: SpawnProvenance): Promise<CreateResult> {
			const ctx = ctxRef;
			if (!ctx || !runRegistry) throw new Error("Session is not ready.");
			const cwd = validateSpawnCwd(input.cwd ?? ctx.cwd);
			const requested = (input.name?.trim() || input.type?.trim() || "").toLowerCase();
			if (requested && (!NAME_RE.test(requested) || RESERVED.has(requested))) throw new Error(`Invalid worker name "${requested}" (use [a-z][a-z0-9_-]{0,31}; not ${[...RESERVED].join("/")})`);
			signal?.throwIfAborted();
			const runId = randomUUID();
			const assignment = resolveAssignment(input, ctx, cwd);
			runRegistry.register({
				version: 1,
				runId,
				sessionId: ctx.sessionManager.getSessionId(),
				registeredAt: Date.now(),
				...(input.correlationId === undefined ? {} : { correlationId: input.correlationId }),
				...(provenance?.requestId === undefined ? {} : { requestId: provenance.requestId }),
				lifecycleProtocol: LIFECYCLE_PROTOCOL_V2,
				assignment: {
					cwd: assignment.cwd,
					...(assignment.model === undefined ? {} : { model: assignment.model }),
					...(assignment.role === undefined ? {} : { role: assignment.role }),
				},
			});
			if (!state.orchestratedBy && !state.teamMode) {
				state.teamMode = true;
				persist();
				updateUi();
			}
			const pending = createQueue.then(() => {
				signal?.throwIfAborted();
				return createAgent({ runId, lifecycleProtocol: LIFECYCLE_PROTOCOL_V2, correlationId: input.correlationId, provenance, name: input.name, direction: input.direction, model: assignment.model, type: assignment.role, purpose: input.purpose, thinking: input.thinking, initialPrompt: input.initialPrompt }, ctx, cwd);
			});
			createQueue = pending.then(() => {}, () => {});
			const result = await pending;
			return result;
	}
	const workerService: WorkerService = {
		spawn: spawnWorker,
		async send(input: SendInput, signal?: AbortSignal): Promise<DeliveryReceipt> {
			const priority = input.mode === undefined ? input.priority ?? false : input.mode === "steer";
			return send(input.target, input.message, priority, signal, input.runId);
		},
		async inspect(input: InspectInput, signal?: AbortSignal): Promise<Inspection> {
			const target = input.target;
			if (target === SELF_PANE || target === selfInfo?.name || target === selfInfo?.paneId) {
				throw new WorkerRpcServiceError("NOT_TEAM_MEMBER", "Target is not a team member.");
			}
			const relationships = [
				...state.workers.map((id) => ({ id, relationship: "worker" as const, meta: state.meta?.[id] })),
				...(state.orchestratedBy ? [{ id: state.orchestratedBy, relationship: "orchestrator" as const, meta: undefined }] : []),
			];
			const direct = relationships.find(({ id, meta }) => id === target || meta?.paneId === target);
			let match: (typeof relationships)[number] | undefined;
			let live: AgentInfo | undefined;
			for (const configured of direct ? [direct] : relationships) {
				signal?.throwIfAborted();
				const resolved = await agentGet(configured.id, signal);
				if (resolved && (direct || resolved.name === target || resolved.paneId === target)) {
					match = configured;
					live = resolved;
					break;
				}
			}
			if (!match || !live) {
				if (direct) throw new WorkerRpcServiceError("NOT_FOUND", "Target agent was not found.");
				throw new WorkerRpcServiceError("NOT_TEAM_MEMBER", "Target is not a team member.");
			}
			const { relationship, meta } = match;
			return {
				name: live.name ?? match.id,
				paneId: live.paneId,
				...(live.kind === undefined ? {} : { kind: live.kind }),
				...(live.status === undefined ? {} : { status: live.status }),
				...(live.cwd === undefined ? {} : { cwd: live.cwd }),
				...(meta?.type === undefined ? {} : { type: meta.type }),
				...(meta?.purpose === undefined ? {} : { purpose: meta.purpose }),
				...(meta?.model === undefined ? {} : { model: meta.model }),
				relationship,
				managedBySession: relationship === "worker",
			};
		},
	};
	const rpcService: WorkerRpcService = {
		async spawn(input, provenance, signal) {
			const result = await workerService.spawn(input, signal, provenance);
			return {
				runId: result.runId,
				...(result.correlationId === undefined ? {} : { correlationId: result.correlationId }),
				name: result.name,
				paneId: result.paneId,
				cwd: result.cwd,
				adopted: result.adopted,
				...(result.model === undefined ? {} : { model: result.model }),
				...(result.type === undefined ? {} : { type: result.type }),
				...(result.purpose === undefined ? {} : { purpose: result.purpose }),
			};
		},
		send: workerService.send,
		inspect: workerService.inspect,
	};
	const rpcServer = registerWorkerRpcServer({ events: pi.events, service: rpcService, getProviderState: providerState });

	async function release(name: string): Promise<string> {
		if (!state.workers.includes(name)) throw new Error(`Not orchestrating "${name}". Workers: ${state.workers.join(", ") || "(none)"}`);
		state.workers = state.workers.filter((w) => w !== name);
		if (state.meta) delete state.meta[name];
		persist();
		updateUi();
		await sendControl(name, "released").catch(() => false);
		return `Released ${name}. Its pane stays open.`;
	}

	async function setOrchestrator(id: string): Promise<string> {
		if (!id) throw new Error("Usage: /orchestrated-by <agent name or pane id>");
		if (state.orchestratedBy !== id) state.activeRun = undefined;
		state.orchestratedBy = id;
		persist();
		updateUi();
		return `This agent is now orchestrated by ${id}.`;
	}

	// ── CreateAgentPanel tool (orchestrator only) ──

	pi.registerTool({
		name: CREATE_TOOL,
		label: "Create Agent Panel",
		description:
			"Create a new team member: splits a herdr pane next to you and starts a pi worker there that is orchestrated by you. " +
			"Workers are named agent-<name> (default agent-<type> or agent-N). Use `type` to describe the kind of agent (explore, research, review, implement, test, …) and `purpose` for a one-line charter " +
			"(e.g. 'explore only, never edit files'). `model` defaults to your own model, except explore/research default to xai/grok-4.6. " +
			"`initial_prompt` is delivered as the worker's first brief right after startup. Returns the worker's id for SendToAgent.",
		promptSnippet: "Spawn a new orchestrated pi worker in a neighboring herdr pane (type, purpose, model, initial brief)",
		promptGuidelines: [
			"Use CreateAgentPanel when the user asks for a new team member / worker / agent panel, or when a task benefits from a separate agent (e.g. an explore-only scout). Give it a concrete initial_prompt.",
		],
		parameters: Type.Object({
			name: Type.Optional(Type.String({ description: "Short name; becomes agent-<name>. Defaults to the type or a number." })),
			direction: Type.Optional(StringEnum(["right", "down", "left", "up"] as const, { description: "Side of your pane to place the worker (default right). Further workers on that side stack." })),
			type: Type.Optional(Type.String({ description: "Kind of agent: explore | research | review | implement | test | <anything>" })),
			purpose: Type.Optional(Type.String({ description: "One-line charter for this worker, e.g. 'explore only, report findings, never edit files'" })),
			model: Type.Optional(Type.String({ description: "provider/id, e.g. xai/grok-4.6 or anthropic/claude-sonnet-4-5. Default: your model (explore/research: xai/grok-4.6)." })),
			thinking: Type.Optional(StringEnum(["off", "minimal", "low", "medium", "high", "xhigh", "max"] as const, { description: "Thinking level for the worker (default: model default)" })),
			initial_prompt: Type.Optional(Type.String({ description: "First brief for the worker (Markdown). Self-contained: goal, context, constraints, what to report back." })),
		}),
		async execute(_id, params, _signal, onUpdate, ctx) {
			if (!isOrchestrator()) throw new Error("CreateAgentPanel is only available to an orchestrator (run /team here first).");
			onUpdate?.({ content: [{ type: "text", text: "Splitting pane and starting worker…" }], details: {} });
			const create = () => workerService.spawn(
				{ name: params.name, direction: params.direction as Direction | undefined, type: params.type, purpose: params.purpose, model: params.model, thinking: params.thinking, initialPrompt: params.initial_prompt },
				_signal,
			);
			const r = await create();
			const meta = state.meta?.[r.name];
			const text = [
				`Worker ${r.name} ready in pane ${r.paneId} (${r.how}).`,
				meta?.type || meta?.purpose ? `Role: ${[meta?.type, meta?.purpose].filter(Boolean).join(" — ")}` : undefined,
				r.model ? `Model: ${r.model}` : undefined,
				params.initial_prompt ? "Initial brief delivered; its report will arrive as an [agent] message on a later turn." : `No initial brief given; use SendToAgent({ target_id: "${r.name}", … }) to task it.`,
			]
				.filter(Boolean)
				.join("\n");
			return { content: [{ type: "text", text }], details: { ...r, type: meta?.type, purpose: meta?.purpose, initial_prompt: params.initial_prompt } };
		},
		renderCall(args, theme) {
			const bits = [theme.fg("toolTitle", theme.bold("CreateAgentPanel")), theme.fg("accent", `agent-${args.name ?? args.type ?? "N"}`)];
			if (args.direction) bits.push(theme.fg("muted", args.direction));
			if (args.type) bits.push(theme.fg("muted", `type=${args.type}`));
			if (args.model) bits.push(theme.fg("dim", args.model));
			return new Text(bits.join(" "), 0, 0);
		},
		renderResult(result, { expanded, isPartial }, theme, context) {
			if (isPartial) return new Text(theme.fg("dim", "starting worker…"), 0, 0);
			const txt = result.content?.map((c: any) => c.text ?? "").join("\n") ?? "";
			if (context.isError) return new Text(theme.fg("error", txt), 0, 0);
			const d = (result.details ?? {}) as { purpose?: string; initial_prompt?: string };
			let out = theme.fg("success", txt);
			if (d.purpose) out += "\n" + theme.fg("muted", `purpose: ${d.purpose}`);
			if (d.initial_prompt) {
				const lines = d.initial_prompt.split("\n");
				const shown = expanded ? lines : lines.slice(0, 8);
				out += "\n" + shown.map((l) => theme.fg("accent", "│ ") + l).join("\n");
				if (shown.length < lines.length) out += "\n" + theme.fg("dim", `│ … ${lines.length - shown.length} more lines`);
			}
			return new Text(out, 0, 0);
		},
	});

	/** /team add → enable team mode and ask the orchestrator LLM to create the member with CreateAgentPanel. */
	function requestAdd(words: string[], ctx: ExtensionContext): string {
		let dir: Direction | undefined;
		let type: string | undefined;
		const rest: string[] = [];
		for (const w of words) {
			if (!dir && isDirection(w)) dir = w;
			else if (!type) type = w;
			else rest.push(w);
		}
		const purpose = rest.join(" ").replace(/^["']|["']$/g, "").trim();
		const lines = [
			`Add a team member now using the CreateAgentPanel tool (do not use herdr CLI commands for this).`,
			`- direction: ${dir ?? "right"}`,
			type ? `- type: ${type}${TYPE_HINTS[type] ? ` (${TYPE_HINTS[type]})` : ""}` : `- type: (none given — pick one that fits, or leave it generic)`,
			purpose ? `- purpose: ${purpose}` : `- purpose: (not given — write a one-line charter${type ? ` for a ${type} agent` : ""})`,
			`- model: ${type && TYPE_MODELS[type] ? `default for ${type} is ${TYPE_MODELS[type]}` : "default (same as yours) unless there is a reason otherwise"}`,
			`- initial_prompt: write a concrete first brief from the current conversation context and the purpose. If there is genuinely nothing to do yet, a short orientation brief (repo, cwd, purpose, how to report back) is fine.`,
			`Then tell me briefly who was created and what you asked it to do.`,
		];
		const text = lines.join("\n");
		if (ctx.isIdle()) pi.sendUserMessage(text);
		else pi.sendUserMessage(text, { deliverAs: "followUp" });
		return `Asked the orchestrator to create a ${type ? `${type} ` : ""}team member${dir ? ` (${dir})` : ""}${ctx.isIdle() ? "" : " (queued as follow-up)"}.`;
	}

	pi.registerCommand("team", {
		description: "Herdr team: /team add [right|down|left|up] [type] [purpose…] → orchestrator creates a pi worker via CreateAgentPanel · list · release <name> · adopt <name> · from <id>",
		getArgumentCompletions: (prefix) => {
			const items = [
				{ value: "add ", label: "add [direction] [type] [purpose…]", description: "Have the orchestrator create a worker (default: stack right)" },
				...DIRECTIONS.map((d) => ({ value: `add ${d} `, label: `add ${d} [type] [purpose…]`, description: `Worker ${d} of this pane` })),
				{ value: "add right explore ", label: "add right explore [purpose…]", description: "Explore-only scout on xai/grok-4.6" },
				{ value: "list", label: "list", description: "Show workers / orchestrator" },
				...state.workers.map((w) => ({ value: `release ${w}`, label: `release ${w}`, description: "Stop orchestrating this worker" })),
				{ value: "from ", label: "from <id>", description: "Mark this pi as a worker of <id>" },
				{ value: "adopt ", label: "adopt <name>", description: "Control an existing herdr agent without creating a pane" },
			].filter((i) => i.value.startsWith(prefix));
			return items.length ? items : null;
		},
		handler: async (args, ctx) => {
			const [verb, ...rest] = args.trim().split(/\s+/).filter(Boolean);
			if (!interactive()) { ctx.ui.notify("Teams require an interactive pi in Herdr.", "error"); return; }
			try {
				let msg: string;
				if (!state.orchestratedBy && !state.teamMode) {
					state.teamMode = true; // this pane is now an orchestrator -> CreateAgentPanel becomes available
					persist();
					updateUi();
				}
				if (!verb || verb === "list" || verb === "status") {
					msg = [statusText(), ...state.workers.map((w) => `${w}${state.meta?.[w]?.paneId ? ` · ${state.meta[w].paneId}` : ""}`)].filter(Boolean).join("\n") || "No team.";
				} else if (verb === "release") {
					msg = await release(rest[0] ?? "");
				} else if (verb === "from") {
					msg = await setOrchestrator(rest[0] ?? "");
				} else if (verb === "adopt") {
					msg = await adopt(rest[0] ?? "");
				} else if (verb === "help") {
					msg = "/team add [right|down|left|up] [type] [purpose…] | list | release <name> | adopt <name> | from <id>";
				} else if (verb === "add" || isDirection(verb)) {
					if (state.orchestratedBy) throw new Error(`This pane is a worker of ${state.orchestratedBy}; only orchestrators add team members.`);
					msg = requestAdd(verb === "add" ? rest : [verb, ...rest], ctx);
				} else {
					throw new Error(`Unknown subcommand "${verb}". Usage: /team add [right|down|left|up] [type] [purpose…] | list | release <name> | adopt <name> | from <id>`);
				}
				ctx.ui.notify(msg, "info");
			} catch (e: any) {
				ctx.ui.notify(e.message ?? String(e), "error");
			}
		},
	});

	pi.registerCommand("orchestrated-by", {
		description: "Herdr: mark this pi as a worker controlled by <agent name or pane id>",
		handler: async (args, ctx) => {
			try {
				if (!interactive()) throw new Error("Teams require an interactive pi in Herdr.");
				ctx.ui.notify(await setOrchestrator(args.trim()), "info");
			} catch (e: any) {
				ctx.ui.notify(e.message ?? String(e), "error");
			}
		},
	});

	// ── system prompt ──

	pi.on("before_agent_start", async (event) => {
		if (!inTeam() && !isOrchestrator()) return;
		const me = selfId();
		const sections: string[] = ["## Herdr orchestration"];
		if (isOrchestrator()) {
			const roster = state.workers.map((w) => {
				const m = state.meta?.[w];
				const bits = [m?.type, m?.purpose, m?.model ? `model ${m.model}` : undefined].filter(Boolean);
				return bits.length ? `${w} (${bits.join("; ")})` : w;
			});
			sections.push(
				state.workers.length
					? `You are the orchestrator for ${roster.join(", ")} — you work through them, but you work hard on giving them the right tasks. Each worker is a separate pi agent in its own herdr pane (same working directory, its own context; it sees nothing of this conversation).`
					: `You are an orchestrator with no workers yet. Create team members with CreateAgentPanel (type/purpose/model/initial_prompt); they become pi agents in neighboring herdr panes that you task through SendToAgent.`,
				`- Add workers with CreateAgentPanel. Explore/research scouts default to xai/grok-4.6; implementers default to your model. Always hand over a concrete initial_prompt.`,
				`- Respect each worker's charter: do not ask an explore-only agent to edit files; spawn an implement agent instead.`,
				`- Delegate with SendToAgent({ target_id, message, priority }). Briefs must be self-contained: goal, relevant context you already have, constraints, definition of done, and what to report back.`,
				`- priority: true steers the worker mid-task (interrupts its current turn) — use for corrections/stop. priority: false (default) queues a follow-up after its current work.`,
				`- Replies arrive asynchronously as "[agent]" custom messages on a later turn. Do not poll or block for them; finish your turn. To peek at a worker: \`herdr agent get <id>\`, \`herdr agent read <id> --source recent-unwrapped --lines 80\`.`,
				`- Your id (workers reply to it): ${me}.`,
			);
		}
		if (state.orchestratedBy) {
			sections.push(
				`You are being controlled by an orchestrator: ${state.orchestratedBy} — another of your user's pi agents in a neighboring herdr pane. Task briefs arrive as "[agent]" custom messages; your user sees them too and may chime in directly.`,
				...(state.role?.type || state.role?.purpose
					? [
							`- Your role in the team: ${[state.role.type, state.role.purpose].filter(Boolean).join(" — ")}.${state.role.type && TYPE_HINTS[state.role.type] ? ` ${TYPE_HINTS[state.role.type]}` : ""} Stay within this charter; if a request falls outside it, say so to the orchestrator instead of doing it.`,
						]
					: []),
				`- Use SendToAgent for questions and ordinary communication. If a brief is ambiguous or blocked, ask the orchestrator rather than guessing; there is no live back-and-forth.`,
				`- Report terminal outcomes only with ReportWorkerRun. When complete, report status "completed", a non-empty result, and any artifact or verification references. If the assignment cannot be completed, report status "failed" with a concrete error.`,
				`- Your id: ${me}.`,
			);
		}
		return { systemPrompt: `${event.systemPrompt}\n\n${sections.join("\n")}` };
	});

	// ── lifecycle ──

	pi.on("session_start", async (event, ctx) => {
		ctxRef = ctx;
		restore(ctx);
		runRegistry = createRunRegistry({
			sessionId: ctx.sessionManager.getSessionId(),
			getEntries: () => ctx.sessionManager.getEntries(),
			appendEntry: (customType, data) => pi.appendEntry(customType, data),
		});
		lifecycleAcceptor = createLifecycleAcceptor({
			sessionId: ctx.sessionManager.getSessionId(),
			getEntries: () => ctx.sessionManager.getEntries(),
			appendEntry: (customType, data) => pi.appendEntry(customType, data),
			emit: (channel, payload) => pi.events.emit(channel, payload),
		});
		const queryService: RunQueryService = {
			async get(input, signal) {
				const lifecycle = lifecycleAcceptor?.getRun(input.runId);
				let record = runRegistry?.projectRun(input.runId, lifecycle);
				if (!record) throw new RunQueryServiceError("NOT_FOUND", "Worker run was not found.");
				if (input.includeEndpointObservation && !("legacy" in record) && record.endpoint) {
					const live = await agentGet(record.endpoint.agentName, signal);
					if (live?.name === record.endpoint.agentName && live.paneId === record.endpoint.paneId) {
						const herdrStatus = typeof live.status === "string"
							&& live.status.length > 0
							&& Buffer.byteLength(live.status, "utf8") <= RUN_QUERY_LIMITS.herdrStatus
							? live.status
							: undefined;
						record = runRegistry?.projectRun(input.runId, lifecycle, {
							agentName: live.name,
							paneId: live.paneId,
							observedAt: Date.now(),
							...(herdrStatus === undefined ? {} : { herdrStatus }),
						}) ?? record;
					}
				}
				return record;
			},
			async list(input): Promise<ListRunsResult> {
				const records = runRegistry?.listRunRecords(lifecycleAcceptor?.listRuns() ?? []) ?? [];
				const afterRunId = input.cursor === undefined ? undefined : decodeRunQueryCursor(input.cursor);
				const start = afterRunId === undefined
					? 0
					: records.findIndex((record) => record.runId > afterRunId);
				const normalizedStart = start < 0 ? records.length : start;
				const limit = input.limit ?? RUN_QUERY_LIMITS.defaultPageSize;
				const runs = records.slice(normalizedStart, normalizedStart + limit);
				const hasMore = normalizedStart + runs.length < records.length;
				return {
					runs,
					...(hasMore && runs.length > 0 ? { nextCursor: encodeRunQueryCursor(runs[runs.length - 1].runId) } : {}),
				};
			},
			async replay(input): Promise<ReplayRunResult> {
				const limit = input.limit ?? RUN_QUERY_LIMITS.defaultPageSize;
				const replay = lifecycleAcceptor?.replayRun(input.runId, input.afterAcceptedSequence, limit);
				if (replay) return replay;
				if (runRegistry?.getRegistration(input.runId)) return { events: [], hasMore: false };
				throw new RunQueryServiceError("NOT_FOUND", "Worker run was not found.");
			},
		};
		runQueryServer = registerRunQueryServer({
			events: pi.events,
			service: queryService,
			sessionId: ctx.sessionManager.getSessionId(),
			getProviderState: () => ({
				available: !stopped && runRegistry !== undefined && lifecycleAcceptor !== undefined,
				...(!stopped && runRegistry !== undefined && lifecycleAcceptor !== undefined
					? {}
					: { reason: stopped ? "SHUTTING_DOWN" as const : "SESSION_NOT_READY" as const }),
			}),
		});
		const reconciliationService: ReconciliationService = {
			async reconcile(input, authority) {
				const result = lifecycleAcceptor?.reconcile(input, authority, runRegistry?.getEndpoint(input.runId));
				if (result?.accepted) return result.event;
				const reason = result?.reason;
				if (reason === "unbound_run") throw new ReconciliationServiceError("NOT_FOUND", "Worker run was not found.");
				if (reason === "not_uncertain") throw new ReconciliationServiceError("NOT_UNCERTAIN", "Worker run is not uncertain.");
				if (reason === "stale_accepted_sequence") throw new ReconciliationServiceError("STALE_ACCEPTED_SEQUENCE", "Worker run changed after inspection.");
				if (reason === "endpoint_mismatch") throw new ReconciliationServiceError("ENDPOINT_MISMATCH", "Observation endpoint does not match the run binding.");
				if (reason === "unsupported_lifecycle_protocol") throw new ReconciliationServiceError("UNSUPPORTED_LIFECYCLE_PROTOCOL", "Worker run does not use lifecycle contract 2.");
				throw new Error("Lifecycle reconciliation was rejected.");
			},
		};
		reconciliationServer = registerReconciliationServer({
			events: pi.events,
			service: reconciliationService,
			sessionId: ctx.sessionManager.getSessionId(),
			getProviderState: () => ({
				available: !stopped && runRegistry !== undefined && lifecycleAcceptor !== undefined,
				...(!stopped && runRegistry !== undefined && lifecycleAcceptor !== undefined
					? {}
					: { reason: stopped ? "SHUTTING_DOWN" as const : "SESSION_NOT_READY" as const }),
			}),
		});
		for (const name of state.workers) {
			const meta = state.meta?.[name];
			if (!meta?.runId || !meta.paneId) continue;
			const registration = runRegistry.getRegistration(meta.runId);
			lifecycleAcceptor.bindRun({
				runId: meta.runId,
				lifecycleProtocol: meta.lifecycleProtocol ?? registration?.lifecycleProtocol ?? LIFECYCLE_PROTOCOL_V1,
				...(meta.correlationId === undefined ? {} : { correlationId: meta.correlationId }),
				worker: { name, paneId: meta.paneId },
				...(meta.requestId === undefined ? {} : { requestId: meta.requestId }),
				...(meta.providerInstanceId === undefined ? {} : { providerInstanceId: meta.providerInstanceId }),
			});
		}
		if (!interactive()) {
			state = { workers: [] };
			syncTool(); // no herdr -> no team -> no tool
			return;
		}
		await refreshSelf();

		const flag = pi.getFlag("orchestrated-by");
		let shouldReportReady = false;
		if (typeof flag === "string" && flag && event.reason === "startup" && !state.orchestratedBy) {
			state.orchestratedBy = flag;
			const role = pi.getFlag("team-role");
			if (typeof role === "string" && role) {
				const m = role.match(/^([a-z0-9_-]+):\s*(.*)$/i);
				state.role = m ? { type: m[1].toLowerCase(), purpose: m[2] || undefined } : { type: role.toLowerCase() };
			}
			persist();
		}
		const runFlag = pi.getFlag("worker-run-id");
		const correlationFlag = pi.getFlag("worker-correlation-id");
		const lifecycleProtocolFlag = pi.getFlag("worker-lifecycle-protocol");
		const startupBinding = {
			protocol: lifecycleProtocolFlag === "2" ? LIFECYCLE_PROTOCOL_V2 : LIFECYCLE_PROTOCOL_V1,
			runId: runFlag,
			...(typeof correlationFlag === "string" ? { correlationId: correlationFlag } : {}),
		};
		if (event.reason === "startup" && state.orchestratedBy && isWorkerRunBinding(startupBinding)) {
			state.activeRun = { ...startupBinding, sourceInstanceId: randomUUID(), sourceSequence: 0 };
			shouldReportReady = true;
			persist();
		}

		if (!testOptions.disableInbox) mailbox.startListening();
		updateUi(); // also gates the SendToAgent tool
		for (const entry of ctx.sessionManager.getEntries()) acceptPersistedLifecycleReport(entry);
		if (shouldReportReady) await reportWorkerReady().catch(() => {});
	});

	function acknowledgePersistedInbox(ctx: ExtensionContext) {
		// Hooks use their supplied context; drains keep using the session/tree context.
		acknowledgementContext = ctx;
		try {
			mailbox.acknowledgeEntries();
		} finally {
			acknowledgementContext = undefined;
		}
	}
	pi.on("context", (_event, ctx) => acknowledgePersistedInbox(ctx));
	pi.on("agent_settled", (_event, ctx) => acknowledgePersistedInbox(ctx));
	pi.on("session_tree", (_event, ctx) => {
		ctxRef = ctx;
		restore(ctx);
		updateUi();
	});
	pi.on("session_shutdown", async () => {
		// Headless sessions must not remove the interactive pane's listener file.
		const wasListening = mailbox.isStarted();
		for (const pending of pendingSpawnLifecycles.values()) {
			observeProviderUncertain(
				pending.opts,
				pending.name,
				pending.paneId,
				pending.scope,
				"Provider shutdown interrupted worker creation after an external side effect may have occurred.",
			);
		}
		pendingSpawnLifecycles.clear();
		stopped = true;
		reconciliationServer?.dispose();
		reconciliationServer = undefined;
		runQueryServer?.dispose();
		runQueryServer = undefined;
		rpcServer.dispose();
		lifetime.abort();
		if (wasListening) mailbox.stopListening();
		if (ctxRef?.hasUI) ctxRef.ui.setStatus(STATUS_KEY, undefined);
		runRegistry = undefined;
		lifecycleAcceptor = undefined;
		ctxRef = undefined;
	});
}
