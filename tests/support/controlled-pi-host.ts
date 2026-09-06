import assert from "node:assert/strict";
import * as fs from "node:fs";
import type herdrWorker from "../../extensions/herdr-worker.js";
import { FakeIsolatedEventBus } from "./fake-isolated-event-bus.js";

export interface ControlledMessage {
	customType: string;
	content: string;
	display: boolean;
	details?: unknown;
}

export type ControlledEntry =
	| { type: "custom"; customType: string; data: unknown }
	| ({ type: "custom_message" } & ControlledMessage);

export interface QueuedMessage {
	message: ControlledMessage;
	options: { triggerTurn: boolean; deliverAs: "steer" | "followUp" };
}

export type ControlledPersistenceMode = "explicit" | "file-backed" | "deferred-first-write" | "disabled" | "write-failed";

export interface ControlledPiHostOptions {
	cwd: string;
	sessionFile: string;
	createMailbox: NonNullable<Parameters<typeof herdrWorker>[1]>["createMailbox"];
	mode?: "tui" | "rpc";
	reopen?: boolean;
	persistenceMode?: ControlledPersistenceMode;
}

export async function createControlledPiHost(options: ControlledPiHostOptions) {
	const { default: workerExtension } = await import("../../extensions/herdr-worker.js");
	const timeline: string[] = [];
	const events = new FakeIsolatedEventBus((channel) => timeline.push(`emit:${channel}`));
	const handlers = new Map<string, Array<(...args: any[]) => unknown>>();
	const tools = new Map<string, any>();
	const entries: ControlledEntry[] = [];
	const queued: QueuedMessage[] = [];
	const sentMessages: QueuedMessage[] = [];
	const execCalls: string[][] = [];
	const warnings: string[] = [];
	let activeTools: string[] = [];
	let shutdown = false;
	let sendError: Error | undefined;
	let onSend: ((delivery: QueuedMessage) => void) | undefined;
	let persistenceMode = options.persistenceMode ?? "explicit";
	const agents = [
		{ pane_id: "self-pane", tab_id: "tab-1", name: "orchestrator", agent: "pi", cwd: options.cwd },
		{ pane_id: "worker-pane", tab_id: "tab-1", name: "agent-scout", agent: "pi", agent_status: "idle", cwd: options.cwd },
	];

	function reopen(): ControlledEntry[] {
		if (!fs.existsSync(options.sessionFile)) return [];
		return fs.readFileSync(options.sessionFile, "utf8").split("\n").filter(Boolean).map((line) => JSON.parse(line));
	}

	function append<T extends ControlledEntry>(entry: T): T {
		const copy = structuredClone(entry);
		entries.push(copy);
		timeline.push(`memory:${entry.customType}`);
		return copy;
	}

	function write() {
		if (persistenceMode === "disabled" || persistenceMode === "deferred-first-write") {
			timeline.push(`write:${persistenceMode}`);
			return;
		}
		if (persistenceMode === "write-failed") {
			timeline.push("write:failed");
			throw new Error("Controlled session write failed after memory insertion");
		}
		fs.writeFileSync(options.sessionFile, entries.map((entry) => JSON.stringify(entry) + "\n").join(""));
		timeline.push("write:session");
	}

	if (options.reopen) entries.push(...reopen());
	else append({ type: "custom", customType: "herdr-worker", data: {
		version: 1, sessionId: "session", workers: ["agent-scout"],
	} });

	const pi: any = {
		events,
		registerFlag() {},
		getFlag() { return undefined; },
		registerTool(tool: any) { tools.set(tool.name, tool); },
		registerCommand() {},
		on(name: string, handler: (...args: any[]) => unknown) {
			const list = handlers.get(name) ?? [];
			list.push(handler);
			handlers.set(name, list);
		},
		getActiveTools: () => activeTools,
		setActiveTools(value: string[]) { activeTools = value; },
		appendEntry(customType: string, data: unknown) {
			append({ type: "custom", customType, data });
			write();
		},
		sendMessage(message: ControlledMessage, messageOptions: QueuedMessage["options"]) {
			const delivery = structuredClone({ message, options: messageOptions });
			sentMessages.push(delivery);
			timeline.push(`send:${message.customType}`);
			onSend?.(delivery);
			if (sendError) {
				const error = sendError;
				sendError = undefined;
				throw error;
			}
			queued.push(delivery);
		},
		sendUserMessage() { throw new Error("Unexpected user message in controlled host"); },
		async exec(command: string, args: string[]) {
			assert.equal(command, "herdr");
			execCalls.push(args);
			if (args[0] === "agent" && args[1] === "get") {
				const agent = agents.find((agent) => agent.pane_id === args[2] || agent.name === args[2]);
				return { code: agent ? 0 : 1, stdout: JSON.stringify({ result: { agent } }), stderr: agent ? "" : "missing" };
			}
			if (args[0] === "agent" && args[1] === "list") {
				return { code: 0, stdout: JSON.stringify({ result: { agents } }), stderr: "" };
			}
			if (args[0] === "agent" && args[1] === "prompt") {
				assert.ok(agents.some((agent) => agent.pane_id === args[2]));
				assert.equal(args.length, 4);
				return { code: 0, stdout: "{}", stderr: "" };
			}
			throw new Error(`Unexpected Herdr call: ${args.join(" ")}`);
		},
	};
	const ctx: any = {
		mode: options.mode ?? "tui", cwd: options.cwd, hasUI: false,
		model: { provider: "test", id: "model" }, signal: new AbortController().signal,
		sessionManager: { getSessionId: () => "session", getBranch: () => entries, getEntries: () => entries },
		ui: { setStatus() {}, notify(message: string) { warnings.push(message); } },
		isIdle: () => queued.length === 0,
	};

	async function hook(name: string, event: unknown = {}) {
		timeline.push(`hook:${name}`);
		for (const handler of handlers.get(name) ?? []) await handler(event, ctx);
	}

	workerExtension(pi, { createMailbox: options.createMailbox });
	return {
		events, entries, queued, sentMessages, execCalls, warnings, timeline, ctx, tools,
		hook, append, write, reopen,
		setPeerPane(paneId: string) { agents[1].pane_id = paneId; },
		failNextSend(error: Error) { sendError = error; },
		observeSend(callback: (delivery: QueuedMessage) => void) { onSend = callback; },
		consume(): QueuedMessage {
			const delivery = queued.shift();
			assert.ok(delivery, "expected a queued custom message");
			timeline.push(`consume:${delivery.message.customType}`);
			return delivery;
		},
		appendMessage(delivery: QueuedMessage) {
			const entry = append({ type: "custom_message", ...delivery.message });
			if (persistenceMode !== "explicit") write();
			return entry;
		},
		setPersistenceMode(mode: ControlledPersistenceMode) { persistenceMode = mode; },
		flushFirstWrite() {
			assert.equal(persistenceMode, "deferred-first-write");
			persistenceMode = "file-backed";
			write();
		},
		start: () => hook("session_start", { reason: "startup" }),
		async shutdown() {
			if (shutdown) return;
			shutdown = true;
			await hook("session_shutdown", { reason: "exit" });
			assert.equal(events.listenerCount(), 0, "extension must dispose all event subscriptions");
		},
	};
}

export type ControlledPiHost = Awaited<ReturnType<typeof createControlledPiHost>>;
