import assert from "node:assert/strict";
import { execFile } from "node:child_process";
import test, { type TestContext } from "node:test";
import { fileURLToPath } from "node:url";
import { promisify } from "node:util";
import { LIFECYCLE_JOURNAL_ENTRY } from "../../lifecycle/acceptor.js";
import { LIFECYCLE_CHANNELS, type AcceptedLifecycleEvent } from "../../lifecycle/protocol.js";
import { RpcAbortError, WorkerRpcClient } from "../../rpc/client.js";
import { CHANNELS, replyChannel, type DeliveryReceipt, type Inspection, type ProbeData, type RpcReply, type SendInput, type SpawnInput, type WorkerReference } from "../../rpc/protocol.js";
import { RECONCILIATION_CHANNELS, reconciliationReplyChannel, type ReconciliationReply } from "../../reconciliation/protocol.js";
import { RUN_QUERY_CHANNELS, runQueryReplyChannel, type RunQueryReply, type WorkerRunRecordV1 } from "../../runs/protocol.js";
import { RUN_ENDPOINT_BINDING_ENTRY, RUN_REGISTRATION_ENTRY } from "../../runs/registry.js";
import { FakeIsolatedEventBus } from "../support/fake-isolated-event-bus.js";

if (!process.env.TEAM_COMMAND_ENV_CASE) {
	process.env.HERDR_ENV = "1";
	process.env.HERDR_PANE_ID = "self-pane";
	process.env.HERDR_TAB_ID = "tab-1";
}

type ExecOptions = { timeout: number; signal: AbortSignal };

async function harness(options: {
	listening?: boolean;
	branch?: any[];
	sessionEntries?: any[];
	missingAgents?: string[];
	contextCwd?: string;
	workerCwd?: string | null;
	flags?: Record<string, unknown>;
	selfName?: string;
	agents?: any[];
	execOverride?: (args: string[], options: ExecOptions) => Promise<any> | any;
	writeEnvelopeErrorForType?: string;
	isIdle?: boolean;
	hasUI?: boolean;
	mode?: string;
	model?: { provider: string; id: string } | null;
	activeTools?: string[];
	splitPaneIds?: string[];
	onAppend?: (type: string, data: any) => void;
} = {}) {
	const { default: herdrWorker } = await import("../../extensions/herdr-worker.js");
	const timeline: string[] = [];
	const events = new FakeIsolatedEventBus((channel) => timeline.push(`emit:${channel}`));
	const tools = new Map<string, any>();
	const commands = new Map<string, any>();
	const handlers = new Map<string, Array<(...args: any[]) => any>>();
	const entries: Array<{ type: string; data: any }> = [];
	const sessionEntries = structuredClone(options.sessionEntries ?? []);
	const branch = structuredClone(options.branch ?? []);
	const execCalls: string[][] = [];
	const execOptions: ExecOptions[] = [];
	const partialToolUpdates: Array<{ id: string; update: any }> = [];
	const pendingExec = new Set<Promise<any>>();
	const paneMetadataCalls: string[][] = [];
	const sentUserMessages: Array<[text: string, options?: { deliverAs: "followUp" }]> = [];
	const notifications: Array<{ text: string; level: string }> = [];
	const statusUpdates: Array<{ key: string; text: string | undefined }> = [];
	const activeToolUpdates: string[][] = [];
	const writtenEnvelopes: Array<{ paneId: string; envelope: any }> = [];
	const sentMessages: Array<{ message: any; options: any }> = [];
	const agentGetTargets: string[] = [];
	const startedAgents = new Map<string, string>();
	let splitCount = 0;
	let inboxHandler: ((envelope: unknown, envelopeId: string) => Promise<void>) | undefined;
	let activeTools: string[] = [...(options.activeTools ?? [])];
	const pi: any = {
		events,
		registerFlag() {},
		getFlag(name: string) { return options.flags?.[name]; },
		registerTool(tool: any) { tools.set(tool.name, tool); },
		registerCommand(name: string, command: any) { commands.set(name, command); },
		on(name: string, handler: (...args: any[]) => any) {
			const list = handlers.get(name) ?? [];
			list.push(handler);
			handlers.set(name, list);
		},
		getActiveTools() { return activeTools; },
		setActiveTools(value: string[]) {
			activeTools = [...value];
			activeToolUpdates.push([...value]);
			timeline.push("tools");
		},
		appendEntry(type: string, data: any) {
			entries.push({ type, data });
			sessionEntries.push({ type: "custom", customType: type, data });
			timeline.push(`append:${type}`);
			options.onAppend?.(type, data);
		},
		sendMessage(message: any, messageOptions: any) { sentMessages.push({ message, options: messageOptions }); },
		sendUserMessage(...args: [string, { deliverAs: "followUp" }?]) { sentUserMessages.push(args); },
		exec(_command: string, args: string[], callOptions: ExecOptions) {
			const call = execute(args, callOptions);
			pendingExec.add(call);
			void call.then(() => pendingExec.delete(call), () => pendingExec.delete(call));
			return call;
		},
	};
	async function execute(args: string[], callOptions: ExecOptions) {
		execCalls.push(args);
		execOptions.push(callOptions);
		if (args[0] === "pane" && args[1] === "report-metadata") paneMetadataCalls.push(args);
		timeline.push(`exec:${args[0]}:${args[1] ?? ""}`);
		const overridden = await options.execOverride?.(args, callOptions);
		if (overridden !== undefined) {
			if (args[0] === "agent" && args[1] === "start" && overridden.code === 0) startedAgents.set(args[2], args[args.indexOf("--pane") + 1]);
			return overridden;
		}
		if (args[0] === "pane" && args[1] === "layout") return { code: 0, stdout: JSON.stringify({ result: { layout: { panes: [] } } }), stderr: "" };
		if (args[0] === "pane" && args[1] === "split") return { code: 0, stdout: JSON.stringify({ result: { pane: { pane_id: options.splitPaneIds?.[splitCount++] ?? "new-pane" } } }), stderr: "" };
		if (args[0] === "agent" && args[1] === "get") {
			const target = args[2];
			agentGetTargets.push(target);
			const agent = target === "self-pane"
				? { pane_id: "self-pane", tab_id: "tab-1", name: options.selfName ?? "orchestrator", agent: "pi", cwd: "/tmp" }
				: (target === "agent-scout" || target === "worker-pane" || startedAgents.has(target)) && !options.missingAgents?.includes(target)
					? {
						pane_id: startedAgents.get(target) ?? "worker-pane", tab_id: "tab-1", name: startedAgents.has(target) ? target : "agent-scout", agent: "pi", agent_status: "idle",
						...(options.workerCwd === null ? {} : { cwd: options.workerCwd ?? "/tmp" }),
					}
					: target === "boss" && !options.missingAgents?.includes(target)
						? { pane_id: "boss-pane", tab_id: "tab-1", name: "boss", agent: "pi", agent_status: "busy", cwd: "/workspace" }
					: undefined;
			return { code: agent ? 0 : 1, stdout: agent ? JSON.stringify({ result: { agent } }) : "", stderr: agent ? "" : "missing" };
		}
		if (args[0] === "agent" && args[1] === "list") return { code: 0, stdout: JSON.stringify({ result: { agents: options.agents ?? [] } }), stderr: "" };
		if (args[0] === "agent" && args[1] === "start") startedAgents.set(args[2], args[args.indexOf("--pane") + 1]);
		return { code: 0, stdout: JSON.stringify({ result: {} }), stderr: "" };
	}
	const ctx: any = {
		mode: options.mode ?? "tui",
		cwd: options.contextCwd ?? "/tmp",
		hasUI: options.hasUI ?? false,
		model: options.model === null ? undefined : options.model ?? { provider: "test", id: "model" },
		signal: new AbortController().signal,
		sessionManager: { getSessionId: () => "session", getBranch: () => branch, getEntries: () => sessionEntries },
		ui: {
			setStatus(key: string, text: string | undefined) { statusUpdates.push({ key, text }); },
			notify(text: string, level: string) { notifications.push({ text, level }); timeline.push("notify"); },
		},
		isIdle: () => options.isIdle ?? true,
	};
	herdrWorker(pi, {
		disableInbox: true,
		isListening: () => options.listening ?? false,
		writeEnvelope: (paneId, envelope) => {
			timeline.push(`publish:${envelope.type}`);
			if (envelope.type === options.writeEnvelopeErrorForType) throw new Error("mailbox write failed");
			writtenEnvelopes.push({ paneId, envelope });
			timeline.push(`envelope:${envelope.type}`);
		},
		onInboxHandler: (handler) => { inboxHandler = handler; },
	});
	return {
		events, tools, commands, handlers, entries, sessionEntries, timeline, execCalls, agentGetTargets,
		writtenEnvelopes, sentMessages, ctx, pi, herdrWorker, activeTools: () => activeTools,
		branch, sentUserMessages, notifications, statusUpdates, paneMetadataCalls, activeToolUpdates,
		execOptions, partialToolUpdates,
		executeTool(name: string, params: Record<string, unknown>, signal: AbortSignal = ctx.signal, id = "call") {
			return tools.get(name).execute(id, params, signal, (update: any) => {
				partialToolUpdates.push({ id, update: structuredClone(update) });
				timeline.push(`update:${name}`);
			}, ctx);
		},
		async settleExec() {
			while (pendingExec.size) await Promise.all([...pendingExec]);
		},
		deliverInbox: (envelope: unknown, envelopeId: string) => inboxHandler!(envelope, envelopeId),
	};
}

async function emitForReply<T>(events: FakeIsolatedEventBus, channel: typeof CHANNELS.spawn | typeof CHANNELS.probe | typeof CHANNELS.send | typeof CHANNELS.inspect, requestId: string, payload: unknown): Promise<RpcReply<T>> {
	return await new Promise((resolve) => {
		const unsubscribe = events.on(replyChannel(channel, requestId), (reply) => {
			unsubscribe();
			resolve(reply as RpcReply<T>);
		});
		events.emit(channel, payload);
	});
}

async function emitRunQueryForReply<T>(events: FakeIsolatedEventBus, channel: (typeof RUN_QUERY_CHANNELS)[keyof typeof RUN_QUERY_CHANNELS], requestId: string, payload: unknown): Promise<RunQueryReply<T>> {
	return await new Promise((resolve) => {
		const unsubscribe = events.on(runQueryReplyChannel(channel, requestId), (reply) => {
			unsubscribe();
			resolve(reply as RunQueryReply<T>);
		});
		events.emit(channel, payload);
	});
}

async function emitReconciliationForReply<T>(events: FakeIsolatedEventBus, channel: (typeof RECONCILIATION_CHANNELS)[keyof typeof RECONCILIATION_CHANNELS], requestId: string, payload: unknown): Promise<ReconciliationReply<T>> {
	return await new Promise((resolve) => {
		const unsubscribe = events.on(reconciliationReplyChannel(channel, requestId), (reply) => {
			unsubscribe();
			resolve(reply as ReconciliationReply<T>);
		});
		events.emit(channel, payload);
	});
}

function teamBranch(data: any): any[] {
	return [{ type: "custom", customType: "herdr-worker", data: { version: 1, sessionId: "session", workers: [], ...data } }];
}

function workerReport(runId: string, overrides: Record<string, unknown> = {}) {
	return {
		protocol: 2,
		eventId: "worker-event-1",
		runId,
		sourceInstanceId: "worker-source-1",
		sourceSequence: 1,
		observedAt: 1_786_000_000_000,
		status: "completed",
		evidence: { kind: "worker_completed_v2", result: "Done" },
		...overrides,
	};
}

async function commandHarness(t: TestContext, options: Parameters<typeof harness>[0] = {}) {
	const h = await harness(options);
	t.after(async () => {
		try { await h.settleExec(); }
		finally { await h.handlers.get("session_shutdown")![0](); }
	});
	await h.handlers.get("session_start")![0]({ reason: "startup" }, h.ctx);
	await h.settleExec();
	return h;
}

async function command(h: Awaited<ReturnType<typeof harness>>, args: string, name = "team") {
	await h.commands.get(name).handler(args, h.ctx);
	await h.settleExec();
}

function assertRelationshipOnly(h: Awaited<ReturnType<typeof harness>>) {
	assert.deepEqual(h.execCalls.filter((args) => !(
		(args[0] === "agent" && ["get", "list"].includes(args[1]))
		|| (args[0] === "pane" && args[1] === "report-metadata")
	)), [], "commands do not create, rename, start, stop, close, or prompt agents");
	assert.equal(h.entries.every((entry) => entry.type === "herdr-worker"), true,
		"relationship commands append no assignment, endpoint, operation, or lifecycle journal");
}

const TEAM_HELP = "/team add [right|down|left|up] [type] [purpose\u2026] | list | release <name> | adopt <name> | from <id>";
const CLEAR_TITLE = ["pane", "report-metadata", "self-pane", "--source", "pi-herdr-worker", "--clear-title"];

for (const args of ["", "  \t\n", "list", "status"]) {
	for (const row of [
		{ label: "empty orchestrator", state: {}, text: "team \u00b7 orchestrator", tools: ["read", "CreateAgentPanel"] },
		{ label: "worker", state: { orchestratedBy: "boss" }, text: "team \u21d0 boss", tools: ["read", "SendToAgent"] },
		{
			label: "saved roster with missing live peers",
			state: { workers: ["agent-scout", "agent-gone"], meta: { "agent-scout": { paneId: "saved-pane" } } },
			text: "team \u00b7 2 workers \u00b7 /team list\nagent-scout \u00b7 saved-pane\nagent-gone",
			tools: ["read", "SendToAgent", "CreateAgentPanel"],
		},
		{
			label: "combined worker and roster",
			state: { workers: ["agent-scout"], orchestratedBy: "boss" },
			text: "team \u00b7 1 worker \u00b7 /team list \u00b7 team \u21d0 boss\nagent-scout",
			tools: ["read", "SendToAgent"],
		},
	]) {
		test(`team ${JSON.stringify(args)} lists ${row.label} without refreshing the roster`, async (t) => {
			const h = await commandHarness(t, { branch: teamBranch(row.state), missingAgents: ["agent-scout", "agent-gone"], activeTools: ["read"] });
			const before = h.execCalls.length;
			await command(h, args);
			assert.deepEqual(h.notifications, [{ text: row.text, level: "info" }]);
			assert.deepEqual(h.execCalls.slice(before), []);
			assert.deepEqual(h.activeTools(), row.tools);
			assert.deepEqual(h.entries, "orchestratedBy" in row.state ? [] : [{
				type: "herdr-worker", data: { ...teamBranch(row.state)[0].data, teamMode: true },
			}]);
			assert.deepEqual(h.sentUserMessages, []);
			assertRelationshipOnly(h);
		});
	}
}

for (const row of [
	{ args: "help", text: TEAM_HELP, level: "info" },
	{ args: "unknown", text: `Unknown subcommand "unknown". Usage: ${TEAM_HELP}`, level: "error" },
	{ args: "ADD right", text: `Unknown subcommand "ADD". Usage: ${TEAM_HELP}`, level: "error" },
	{ args: "Right explore", text: `Unknown subcommand "Right". Usage: ${TEAM_HELP}`, level: "error" },
	{ args: "release", text: 'Not orchestrating "". Workers: (none)', level: "error" },
	{ args: "adopt", text: 'No live herdr agent "" to adopt.', level: "error" },
	{ args: "from", text: "Usage: /orchestrated-by <agent name or pane id>", level: "error" },
]) {
	test(`team ${row.args} preserves exact help/errors after enabling mode`, async (t) => {
		const h = await commandHarness(t, { activeTools: ["read"] });
		await command(h, row.args);
		assert.deepEqual(h.notifications, [{ text: row.text, level: row.level }]);
		assert.deepEqual(h.entries, [{ type: "herdr-worker", data: { workers: [], teamMode: true, version: 1, sessionId: "session" } }]);
		assert.deepEqual(h.activeToolUpdates, [["read", "CreateAgentPanel"]]);
		assert.ok(h.timeline.indexOf("append:herdr-worker") < h.timeline.indexOf("tools"));
		assert.ok(h.timeline.indexOf("tools") < h.timeline.indexOf("notify"));
		assert.deepEqual(h.sentUserMessages, []);
		assertRelationshipOnly(h);
	});
}

const ADD_PREFIX = "Add a team member now using the CreateAgentPanel tool (do not use herdr CLI commands for this).\n";
const ADD_SUFFIX = "\n- initial_prompt: write a concrete first brief from the current conversation context and the purpose. If there is genuinely nothing to do yet, a short orientation brief (repo, cwd, purpose, how to report back) is fine.\nThen tell me briefly who was created and what you asked it to do.";
const SAME_MODEL = "\n- model: default (same as yours) unless there is a reason otherwise";

const addCases = [
	{
		args: "add",
		lines: "- direction: right\n- type: (none given \u2014 pick one that fits, or leave it generic)\n- purpose: (not given \u2014 write a one-line charter)" + SAME_MODEL,
		notice: "Asked the orchestrator to create a team member",
	},
	{
		args: "add right",
		lines: "- direction: right\n- type: (none given \u2014 pick one that fits, or leave it generic)\n- purpose: (not given \u2014 write a one-line charter)" + SAME_MODEL,
		notice: "Asked the orchestrator to create a team member (right)",
	},
	{
		args: "add right explore inspect the authentication system",
		lines: "- direction: right\n- type: explore (Explore only: read, search, run read-only commands, and report findings. Do not create, edit, or delete files, and do not commit.)\n- purpose: inspect the authentication system\n- model: default for explore is xai/grok-4.6",
		notice: "Asked the orchestrator to create a explore team member (right)",
	},
	{
		args: "add research",
		lines: "- direction: right\n- type: research (Research only: gather facts, read code/docs, and report. No file modifications.)\n- purpose: (not given \u2014 write a one-line charter for a research agent)\n- model: default for research is xai/grok-4.6",
		notice: "Asked the orchestrator to create a research team member",
	},
	{
		args: "right review inspect auth",
		lines: "- direction: right\n- type: review (Review only: read diffs/code and report actionable findings. Do not modify files.)\n- purpose: inspect auth" + SAME_MODEL,
		notice: "Asked the orchestrator to create a review team member (right)",
	},
	{
		args: "down implement fix auth",
		lines: "- direction: down\n- type: implement (Implement: make the requested code changes, verify them, and report what changed and how it was tested.)\n- purpose: fix auth" + SAME_MODEL,
		notice: "Asked the orchestrator to create a implement team member (down)",
	},
	{
		args: "left test check auth",
		lines: "- direction: left\n- type: test (Testing: write/run tests, report failures with repro steps. Avoid unrelated changes.)\n- purpose: check auth" + SAME_MODEL,
		notice: "Asked the orchestrator to create a test team member (left)",
	},
	{
		args: "up custom inspect auth",
		lines: "- direction: up\n- type: custom\n- purpose: inspect auth" + SAME_MODEL,
		notice: "Asked the orchestrator to create a custom team member (up)",
	},
	{
		args: "add explore down inspect auth",
		lines: "- direction: down\n- type: explore (Explore only: read, search, run read-only commands, and report findings. Do not create, edit, or delete files, and do not commit.)\n- purpose: inspect auth\n- model: default for explore is xai/grok-4.6",
		notice: "Asked the orchestrator to create a explore team member (down)",
	},
	{
		args: "add custom inspect up auth",
		lines: "- direction: up\n- type: custom\n- purpose: inspect auth" + SAME_MODEL,
		notice: "Asked the orchestrator to create a custom team member (up)",
	},
	{
		args: "add right down explore left",
		lines: "- direction: right\n- type: down\n- purpose: explore left" + SAME_MODEL,
		notice: "Asked the orchestrator to create a down team member (right)",
	},
	{
		args: "add right custom right up",
		lines: "- direction: right\n- type: custom\n- purpose: right up" + SAME_MODEL,
		notice: "Asked the orchestrator to create a custom team member (right)",
	},
	{
		args: "add Right explore",
		lines: "- direction: right\n- type: Right\n- purpose: explore" + SAME_MODEL,
		notice: "Asked the orchestrator to create a Right team member",
	},
	{
		args: "add Explore inspect auth",
		lines: "- direction: right\n- type: Explore\n- purpose: inspect auth" + SAME_MODEL,
		notice: "Asked the orchestrator to create a Explore team member",
	},
	{
		args: " \tadd\nleft\tcustom   'inspect   auth'  ",
		lines: "- direction: left\n- type: custom\n- purpose: inspect auth" + SAME_MODEL,
		notice: "Asked the orchestrator to create a custom team member (left)",
	},
	{
		args: 'add custom "inspect auth"',
		lines: "- direction: right\n- type: custom\n- purpose: inspect auth" + SAME_MODEL,
		notice: "Asked the orchestrator to create a custom team member",
	},
	{
		args: `add custom "inspect 'auth' again'`,
		lines: "- direction: right\n- type: custom\n- purpose: inspect 'auth' again" + SAME_MODEL,
		notice: "Asked the orchestrator to create a custom team member",
	},
	{
		args: 'add "custom type" "inspect auth"',
		lines: '- direction: right\n- type: "custom\n- purpose: type" "inspect auth' + SAME_MODEL,
		notice: 'Asked the orchestrator to create a "custom team member',
	},
	{
		args: `add custom ''`,
		lines: "- direction: right\n- type: custom\n- purpose: (not given \u2014 write a one-line charter for a custom agent)" + SAME_MODEL,
		notice: "Asked the orchestrator to create a custom team member",
	},
];

for (const row of addCases) {
	for (const isIdle of [true, false]) {
		test(`team ${JSON.stringify(row.args)} submits exact ${isIdle ? "idle" : "follow-up"} intent without creating a worker`, async (t) => {
			const h = await commandHarness(t, { isIdle, activeTools: ["read"], model: isIdle ? { provider: "other", id: "active-model" } : null });
			await command(h, row.args);
			const text = ADD_PREFIX + row.lines + ADD_SUFFIX;
			assert.deepEqual(h.sentUserMessages, [isIdle ? [text] : [text, { deliverAs: "followUp" }]]);
			assert.deepEqual(h.notifications, [{ text: row.notice + (isIdle ? "." : " (queued as follow-up)."), level: "info" }]);
			assert.deepEqual(h.entries, [{ type: "herdr-worker", data: { workers: [], teamMode: true, version: 1, sessionId: "session" } }]);
			assert.deepEqual(h.activeToolUpdates, [["read", "CreateAgentPanel"]]);
			assert.deepEqual(h.writtenEnvelopes, []);
			assert.deepEqual(h.sentMessages, []);
			assertRelationshipOnly(h);
		});
	}
}

for (const args of ["add", "right explore", "down", "left", "up"]) {
	test(`worker rejects team ${args} and keeps the creation tool gated`, async (t) => {
		const h = await commandHarness(t, { branch: teamBranch({ teamMode: true, orchestratedBy: "boss" }), activeTools: ["read"] });
		await command(h, args);
		assert.deepEqual(h.notifications, [{ text: "This pane is a worker of boss; only orchestrators add team members.", level: "error" }]);
		assert.deepEqual(h.activeTools(), ["read", "SendToAgent"]);
		await assert.rejects(h.tools.get("CreateAgentPanel").execute("gate", {}, h.ctx.signal, undefined, h.ctx), {
			message: "CreateAgentPanel is only available to an orchestrator (run /team here first).",
		});
		assert.deepEqual(h.entries, []);
		assert.deepEqual(h.sentUserMessages, []);
		assertRelationshipOnly(h);
	});
}

for (const [name, args] of [["team", ""], ["team", "unknown"], ["team", "add"], ["team", "adopt agent-scout"], ["team", "release agent-scout"], ["team", "from boss"], ["orchestrated-by", "boss"]]) {
	test(`${name} ${args} rejects non-TUI context before mutation`, async (t) => {
		const h = await commandHarness(t, { mode: "rpc", hasUI: true, branch: teamBranch({ workers: ["agent-scout"], teamMode: true }), activeTools: ["read", "CreateAgentPanel", "SendToAgent"] });
		await command(h, args, name);
		assert.deepEqual(h.notifications, [{ text: "Teams require an interactive pi in Herdr.", level: "error" }]);
		assert.deepEqual(h.entries, []);
		assert.deepEqual(h.activeTools(), ["read"]);
		assert.deepEqual(h.execCalls, []);
		assert.deepEqual(h.statusUpdates, []);
		assert.deepEqual(h.sentUserMessages, []);
	});
}

if (process.env.TEAM_COMMAND_ENV_CASE) {
	test("isolated environment command gate", async (t) => {
		const h = await commandHarness(t, { hasUI: true, activeTools: ["read", "CreateAgentPanel", "SendToAgent"] });
		for (const [name, args] of [["team", ""], ["team", "add"], ["team", "unknown"], ["orchestrated-by", "boss"]]) {
			await command(h, args, name);
		}
		assert.deepEqual(h.notifications, Array.from({ length: 4 }, () => ({ text: "Teams require an interactive pi in Herdr.", level: "error" })));
		assert.deepEqual(h.entries, []);
		assert.deepEqual(h.execCalls, []);
		assert.deepEqual(h.sentUserMessages, []);
		assert.deepEqual(h.activeTools(), ["read"]);
		assert.deepEqual(h.statusUpdates, []);
	});
} else {
	for (const variable of ["HERDR_ENV", "HERDR_PANE_ID"]) {
		test(`commands reject absent ${variable} captured at module load`, async () => {
			const env: NodeJS.ProcessEnv = { ...process.env, TEAM_COMMAND_ENV_CASE: variable };
			delete env[variable];
			// A child test runner must not inherit the parent's worker IPC context.
			delete env.NODE_TEST_CONTEXT;
			const { stdout } = await promisify(execFile)(process.execPath, [
				"--import", "tsx", "--test", "--test-reporter=tap", "--test-name-pattern=^isolated environment command gate$", fileURLToPath(import.meta.url),
			], { env, timeout: 30_000 });
			assert.match(stdout, /ok \d+ - isolated environment command gate/);
			assert.match(stdout, /# pass 1\b/);
			assert.match(stdout, /# fail 0\b/);
		});
	}
}

for (const row of [
	{ label: "listening", listening: true, text: "Adopted existing agent agent-scout (pane worker-pane) as worker.", level: "info" },
	{ label: "not listening", listening: false, text: "Adopted agent-scout (pane worker-pane) \u2014 it is not a listening pi; run `/orchestrated-by orchestrator` inside it (or it can't SendToAgent back).", level: "info" },
	{ label: "failed control publication", listening: true, writeEnvelopeErrorForType: "control", text: "mailbox write failed", level: "error" },
]) {
	test(`adoption with ${row.label} persists before control publication and does not duplicate the relationship`, async (t) => {
		const branch = teamBranch({ teamMode: true, meta: { "agent-scout": { purpose: "Saved charter", paneId: "saved-pane" } } });
		const h = await commandHarness(t, { ...row, branch, hasUI: true, activeTools: ["read"] });
		for (const args of ["adopt agent-scout ignored words", "adopt agent-scout"]) {
			const before = h.timeline.length;
			await command(h, args);
			assert.deepEqual(h.entries.at(-1), { type: "herdr-worker", data: { ...branch[0].data, workers: ["agent-scout"] } });
			assert.deepEqual(h.notifications.at(-1), { text: row.text, level: row.level });
			const timeline = h.timeline.slice(before);
			if (row.listening) {
				assert.ok(timeline.indexOf("publish:control") > timeline.indexOf("append:herdr-worker"));
			}
			assert.deepEqual(h.activeTools(), ["read", "CreateAgentPanel", "SendToAgent"]);
			assert.deepEqual(h.statusUpdates.at(-1), { key: "herdr-worker", text: "team \u00b7 1 worker \u00b7 /team list" });
			assert.deepEqual(h.paneMetadataCalls.at(-1), ["pane", "report-metadata", "self-pane", "--source", "pi-herdr-worker", "--title", "orchestrating agent-scout"]);
		}
		assert.equal(h.agentGetTargets.includes("ignored"), false);
		assert.deepEqual(h.activeToolUpdates, [["read", "CreateAgentPanel"], ["read", "CreateAgentPanel", "SendToAgent"]]);
		if (row.listening && !row.writeEnvelopeErrorForType) {
			assert.deepEqual(h.writtenEnvelopes.map(({ paneId, envelope }) => ({ paneId, envelope: { ...envelope, ts: 0 } })), [0, 1].map(() => ({
				paneId: "worker-pane", envelope: { type: "control", action: "orchestrated-by", from: { id: "orchestrator", paneId: "self-pane", name: "orchestrator", role: "orchestrator" }, ts: 0 },
			})));
		} else assert.deepEqual(h.writtenEnvelopes, []);
		assert.deepEqual(branch[0].data.workers, [], "fixture inputs stay isolated");
		assertRelationshipOnly(h);
	});
}

for (const row of [
	{ args: "adopt self-pane", text: "That is this pane." },
	{ args: "adopt missing", text: 'No live herdr agent "missing" to adopt.' },
	{ args: "adopt agent-scout", text: 'No live herdr agent "agent-scout" to adopt.', missingAgents: ["agent-scout"] },
]) {
	test(`${row.args} preserves adoption rejection`, async (t) => {
		const h = await commandHarness(t, { ...row, branch: teamBranch({ teamMode: true }) });
		await command(h, row.args);
		assert.deepEqual(h.notifications, [{ text: row.text, level: "error" }]);
		assert.deepEqual(h.entries, []);
		assert.deepEqual(h.writtenEnvelopes, []);
		assertRelationshipOnly(h);
	});
}

test("adoption stores the supplied pane identifier and release retains other workers and metadata", async (t) => {
	const branch = teamBranch({ teamMode: true, workers: ["agent-other"], meta: { "agent-other": { paneId: "other-pane", purpose: "Keep working" } } });
	const h = await commandHarness(t, { branch, listening: true, hasUI: true });
	await command(h, "adopt worker-pane ignored");
	assert.deepEqual(h.entries.at(-1)?.data, { ...branch[0].data, workers: ["agent-other", "worker-pane"] });
	assert.deepEqual(h.notifications.at(-1), { text: "Adopted existing agent worker-pane (pane worker-pane) as worker.", level: "info" });
	await command(h, "release agent-scout");
	assert.deepEqual(h.notifications.at(-1), { text: 'Not orchestrating "agent-scout". Workers: agent-other, worker-pane', level: "error" });
	await command(h, "release worker-pane ignored");
	assert.deepEqual(h.entries.at(-1)?.data, branch[0].data);
	assert.deepEqual(h.notifications.at(-1), { text: "Released worker-pane. Its pane stays open.", level: "info" });
	assert.deepEqual(h.activeTools(), ["SendToAgent", "CreateAgentPanel"]);
	assert.deepEqual(h.statusUpdates.at(-1), { key: "herdr-worker", text: "team \u00b7 1 worker \u00b7 /team list" });
	assert.deepEqual(h.paneMetadataCalls.at(-1), ["pane", "report-metadata", "self-pane", "--source", "pi-herdr-worker", "--title", "orchestrating agent-other"]);
	assertRelationshipOnly(h);
});

const ACTIVE_RUN = { protocol: 2, runId: "run-owned", correlationId: "dispatch-owned", sourceInstanceId: "worker-source", sourceSequence: 4 };
for (const row of [
	{ name: "team", args: "from boss ignored words", id: "boss", same: true },
	{ name: "team", args: "from new-boss ignored words", id: "new-boss", same: false },
	{ name: "orchestrated-by", args: "  boss  ", id: "boss", same: true },
	{ name: "orchestrated-by", args: "  boss extra words  ", id: "boss extra words", same: false },
]) {
	test(`${row.name} ${row.args} ${row.same ? "preserves" : "clears"} the active binding`, async (t) => {
		const data = { orchestratedBy: "boss", activeRun: ACTIVE_RUN };
		const h = await commandHarness(t, { branch: teamBranch(data), activeTools: ["read"], hasUI: true });
		const before = h.execCalls.length;
		await command(h, row.args, row.name);
		assert.deepEqual(h.entries, [{ type: "herdr-worker", data: {
			...teamBranch(data)[0].data, orchestratedBy: row.id, activeRun: row.same ? ACTIVE_RUN : undefined,
		} }]);
		assert.deepEqual(h.notifications, [{ text: `This agent is now orchestrated by ${row.id}.`, level: "info" }]);
		assert.deepEqual(h.activeTools(), row.same ? ["read", "SendToAgent", "ReportWorkerRun"] : ["read", "SendToAgent"]);
		assert.deepEqual(h.execCalls.slice(before), [["pane", "report-metadata", "self-pane", "--source", "pi-herdr-worker", "--title", `orchestrator \u21d0 ${row.id}`]]);
		assert.deepEqual(h.writtenEnvelopes, []);
		assertRelationshipOnly(h);
	});
}

for (const row of [
	{ name: "team", args: "from boss extra words", id: "boss", teamMode: true },
	{ name: "orchestrated-by", args: "  boss extra words  ", id: "boss extra words", teamMode: false },
]) {
	test(`fresh ${row.name} uses its argument contract and ${row.teamMode ? "enables" : "does not enable"} team mode`, async (t) => {
		const h = await commandHarness(t, { activeTools: ["read"] });
		await command(h, row.args, row.name);
		const base = { workers: [], version: 1, sessionId: "session" };
		assert.deepEqual(h.entries, [
			...(row.teamMode ? [{ type: "herdr-worker", data: { ...base, teamMode: true } }] : []),
			{ type: "herdr-worker", data: { ...base, ...(row.teamMode ? { teamMode: true } : {}), activeRun: undefined, orchestratedBy: row.id } },
		]);
		assert.deepEqual(h.notifications, [{ text: `This agent is now orchestrated by ${row.id}.`, level: "info" }]);
		assert.deepEqual(h.activeToolUpdates, row.teamMode ? [["read", "CreateAgentPanel"], ["read", "SendToAgent"]] : [["read", "SendToAgent"]]);
		assert.deepEqual(h.execCalls, [["agent", "get", "self-pane"]], "ownership accepts unvalidated identifiers");
		assertRelationshipOnly(h);
	});
}

test("orchestrated-by rejects blank input without enabling mode or dropping a binding", async (t) => {
	const h = await commandHarness(t, { branch: teamBranch({ orchestratedBy: "boss", activeRun: ACTIVE_RUN }) });
	await command(h, "  \t ", "orchestrated-by");
	assert.deepEqual(h.notifications, [{ text: "Usage: /orchestrated-by <agent name or pane id>", level: "error" }]);
	assert.deepEqual(h.entries, []);
	assert.deepEqual(h.activeTools(), ["SendToAgent", "ReportWorkerRun"]);
});

for (const failControl of [false, true]) {
	test(`release removes metadata and refreshes tools/UI while preserving run history, control failure=${failControl}`, async (t) => {
		const history = [
			{ type: "custom", customType: RUN_REGISTRATION_ENTRY, data: { version: 1, runId: "run-release", sessionId: "session", registeredAt: 10, lifecycleProtocol: 2, assignment: { cwd: "/tmp" } } },
			{ type: "custom", customType: RUN_ENDPOINT_BINDING_ENTRY, data: { version: 1, runId: "run-release", sessionId: "session", agentName: "agent-scout", paneId: "worker-pane", observedAt: 20 } },
			{ type: "custom", customType: LIFECYCLE_JOURNAL_ENTRY, data: { version: 1, sessionId: "session", event: {
				protocol: 2, eventId: "ready-event", runId: "run-release", sourceInstanceId: "worker-source", sourceSequence: 1,
				observedAt: 30, acceptedSequence: 1, status: "started", source: "worker", worker: { name: "agent-scout", paneId: "worker-pane" },
				evidence: { kind: "worker_ready", readiness: "confirmed" },
			} } },
		];
		const h = await commandHarness(t, {
			hasUI: true, listening: true, activeTools: ["read"], sessionEntries: history,
			writeEnvelopeErrorForType: failControl ? "control" : undefined,
			branch: teamBranch({ teamMode: true, workers: ["agent-scout"], meta: { "agent-scout": { paneId: "worker-pane", runId: "run-release", lifecycleProtocol: 2 } } }),
		});
		for (const args of ["release worker-pane", "release AGENT-SCOUT", "release"]) await command(h, args);
		assert.deepEqual(h.notifications, [
			{ text: 'Not orchestrating "worker-pane". Workers: agent-scout', level: "error" },
			{ text: 'Not orchestrating "AGENT-SCOUT". Workers: agent-scout', level: "error" },
			{ text: 'Not orchestrating "". Workers: agent-scout', level: "error" },
		]);
		assert.deepEqual(h.entries, []);
		await command(h, "release agent-scout ignored words");
		assert.deepEqual(h.notifications.at(-1), { text: "Released agent-scout. Its pane stays open.", level: "info" });
		assert.deepEqual(h.entries, [{ type: "herdr-worker", data: { version: 1, sessionId: "session", teamMode: true, workers: [], meta: {} } }]);
		assert.deepEqual(h.sessionEntries.filter((entry) => entry.customType !== "herdr-worker"), history);
		assert.deepEqual(h.activeToolUpdates, [["read", "SendToAgent", "CreateAgentPanel"], ["read", "CreateAgentPanel"]]);
		assert.deepEqual(h.statusUpdates.at(-1), { key: "herdr-worker", text: "team \u00b7 orchestrator" });
		assert.deepEqual(h.paneMetadataCalls.at(-1), CLEAR_TITLE);
		assert.ok(h.timeline.indexOf("publish:control") > h.timeline.indexOf("append:herdr-worker"));
		if (failControl) assert.deepEqual(h.writtenEnvelopes, []);
		else assert.deepEqual(h.writtenEnvelopes.map(({ paneId, envelope }) => ({ paneId, envelope: { ...envelope, ts: 0 } })), [{
			paneId: "worker-pane", envelope: { type: "control", action: "released", from: { id: "orchestrator", paneId: "self-pane", name: "orchestrator", role: "agent" }, ts: 0 },
		}]);
		for (const channel of [LIFECYCLE_CHANNELS.completed, LIFECYCLE_CHANNELS.failed, LIFECYCLE_CHANNELS.stopped]) {
			assert.equal(h.events.emissions.some((event) => event.channel === channel), false);
		}
		const probe = await emitRunQueryForReply<any>(h.events, RUN_QUERY_CHANNELS.probe, "release-history-probe", { requestId: "release-history-probe", supportedProtocols: [2] });
		assert.equal(probe.success, true);
		const record = await emitRunQueryForReply<any>(h.events, RUN_QUERY_CHANNELS.get, "release-history", {
			requestId: "release-history", providerInstanceId: probe.success ? probe.data.providerInstanceId : "", protocol: 2, runId: "run-release",
		});
		assert.equal(record.success, true);
		assert.equal(record.success && record.data.lifecycle.status, "started");
		assert.equal(record.success && record.data.lifecycle.acceptedSequence, 1);
		assertRelationshipOnly(h);
	});
}

const BASE_COMPLETIONS = [
	{ value: "add ", label: "add [direction] [type] [purpose\u2026]", description: "Have the orchestrator create a worker (default: stack right)" },
	{ value: "add right ", label: "add right [type] [purpose\u2026]", description: "Worker right of this pane" },
	{ value: "add down ", label: "add down [type] [purpose\u2026]", description: "Worker down of this pane" },
	{ value: "add left ", label: "add left [type] [purpose\u2026]", description: "Worker left of this pane" },
	{ value: "add up ", label: "add up [type] [purpose\u2026]", description: "Worker up of this pane" },
	{ value: "add right explore ", label: "add right explore [purpose\u2026]", description: "Explore-only scout on xai/grok-4.6" },
	{ value: "list", label: "list", description: "Show workers / orchestrator" },
];
const END_COMPLETIONS = [
	{ value: "from ", label: "from <id>", description: "Mark this pi as a worker of <id>" },
	{ value: "adopt ", label: "adopt <name>", description: "Control an existing herdr agent without creating a pane" },
];
const RELEASE_COMPLETIONS = [
	{ value: "release agent-z", label: "release agent-z", description: "Stop orchestrating this worker" },
	{ value: "release agent-scout", label: "release agent-scout", description: "Stop orchestrating this worker" },
];
for (const row of [
	{ prefix: "", expected: [...BASE_COMPLETIONS, ...RELEASE_COMPLETIONS, ...END_COMPLETIONS] },
	{ prefix: "a", expected: [...BASE_COMPLETIONS.slice(0, 6), END_COMPLETIONS[1]] },
	{ prefix: "add right", expected: [BASE_COMPLETIONS[1], BASE_COMPLETIONS[5]] },
	{ prefix: "release agent-", expected: RELEASE_COMPLETIONS },
	{ prefix: "release agent-s", expected: [RELEASE_COMPLETIONS[1]] },
	...(["help", "status", "right", "down", "left", "up", "Add", " add", "missing"].map((prefix) => ({ prefix, expected: null }))),
]) {
	test(`team completion ${JSON.stringify(row.prefix)} preserves objects, ordering, and prefix filtering`, async (t) => {
		const h = await commandHarness(t, { branch: teamBranch({ workers: ["agent-z", "agent-scout"] }) });
		assert.deepEqual(h.commands.get("team").getArgumentCompletions(row.prefix), row.expected);
		assert.deepEqual(h.entries, []);
	});
}

test("release completions follow adoption and release without static aliases", async (t) => {
	const h = await commandHarness(t);
	const complete = h.commands.get("team").getArgumentCompletions;
	assert.deepEqual(complete(""), [...BASE_COMPLETIONS, ...END_COMPLETIONS]);
	assert.equal(complete("release"), null);
	await command(h, "adopt agent-scout");
	assert.deepEqual(complete("release"), [RELEASE_COMPLETIONS[1]]);
	await command(h, "release agent-scout");
	assert.equal(complete("release"), null);
});

for (const row of [
	{ label: "empty", state: {}, status: undefined, title: CLEAR_TITLE },
	{ label: "enabled without workers", state: { teamMode: true }, status: "team \u00b7 orchestrator", title: CLEAR_TITLE },
	{ label: "one worker", state: { workers: ["agent-scout"] }, status: "team \u00b7 1 worker \u00b7 /team list", title: ["pane", "report-metadata", "self-pane", "--source", "pi-herdr-worker", "--title", "orchestrating agent-scout"] },
	{ label: "two workers", state: { workers: ["agent-z", "agent-scout"] }, status: "team \u00b7 2 workers \u00b7 /team list", title: ["pane", "report-metadata", "self-pane", "--source", "pi-herdr-worker", "--title", "orchestrating agent-z, agent-scout"] },
	{ label: "worker", state: { orchestratedBy: "boss" }, status: "team \u21d0 boss", title: ["pane", "report-metadata", "self-pane", "--source", "pi-herdr-worker", "--title", "orchestrator \u21d0 boss"] },
	{ label: "unnamed worker", state: { orchestratedBy: "boss" }, selfName: "", status: "team \u21d0 boss", title: ["pane", "report-metadata", "self-pane", "--source", "pi-herdr-worker", "--title", "\u21d0 boss"] },
	{ label: "combined", state: { workers: ["agent-scout"], orchestratedBy: "boss" }, status: "team \u00b7 1 worker \u00b7 /team list \u00b7 team \u21d0 boss", title: ["pane", "report-metadata", "self-pane", "--source", "pi-herdr-worker", "--title", "orchestrator \u21d0 boss"] },
]) {
	for (const hasUI of [true, false]) {
		test(`${row.label} status and pane metadata respect hasUI=${hasUI}`, async (t) => {
			const h = await commandHarness(t, { branch: teamBranch(row.state), selfName: row.selfName, hasUI });
			assert.deepEqual(h.statusUpdates, hasUI ? [{ key: "herdr-worker", text: row.status }] : []);
			assert.deepEqual(h.paneMetadataCalls, hasUI ? [row.title] : []);
			assertRelationshipOnly(h);
		});
	}
}

test("only status sanitizes controls and truncates the orchestrator identifier", async (t) => {
	const id = "boss\x00\x1f\x7f\x85\x9f0123456789012345678901234567890123456789";
	const h = await commandHarness(t, { hasUI: true });
	await command(h, id, "orchestrated-by");
	assert.deepEqual(h.statusUpdates.at(-1), { key: "herdr-worker", text: "team \u21d0 boss     0123456789012345678901234567890" });
	assert.deepEqual(h.paneMetadataCalls.at(-1), ["pane", "report-metadata", "self-pane", "--source", "pi-herdr-worker", "--title", `orchestrator \u21d0 ${id}`]);
	assert.deepEqual(h.notifications, [{ text: `This agent is now orchestrated by ${id}.`, level: "info" }]);
	assert.equal(h.entries.at(-1)?.data.orchestratedBy, id);
	await command(h, "list");
	assert.deepEqual(h.notifications.at(-1), { text: "team \u21d0 boss     0123456789012345678901234567890", level: "info" });
});

test("bare team enables an unnamed pane without renaming it and ignores metadata failure", async (t) => {
	const h = await commandHarness(t, { hasUI: true, selfName: "", execOverride: (args) => args[1] === "report-metadata" ? { code: 1, stdout: "", stderr: "metadata failed" } : undefined });
	await command(h, "");
	assert.deepEqual(h.notifications, [{ text: "team \u00b7 orchestrator", level: "info" }]);
	assert.deepEqual(h.paneMetadataCalls.at(-1), CLEAR_TITLE);
	assert.deepEqual(h.activeTools(), ["CreateAgentPanel"]);
	assertRelationshipOnly(h);
});

test("commands still notify and refresh tools with hasUI false without setting status or pane titles", async (t) => {
	const h = await commandHarness(t, { hasUI: false, listening: true, activeTools: ["read"] });
	await command(h, "adopt agent-scout");
	await command(h, "release agent-scout");
	assert.deepEqual(h.notifications, [
		{ text: "Adopted existing agent agent-scout (pane worker-pane) as worker.", level: "info" },
		{ text: "Released agent-scout. Its pane stays open.", level: "info" },
	]);
	assert.deepEqual(h.activeToolUpdates, [["read", "CreateAgentPanel"], ["read", "CreateAgentPanel", "SendToAgent"], ["read", "CreateAgentPanel"]]);
	assert.deepEqual(h.statusUpdates, []);
	assert.deepEqual(h.paneMetadataCalls, []);
	assertRelationshipOnly(h);
});

test("tree restoration clears a removed relationship title and shutdown clears status", async (t) => {
	const h = await commandHarness(t, { hasUI: true, branch: teamBranch({ orchestratedBy: "boss" }) });
	h.branch.splice(0);
	await h.handlers.get("session_tree")![0]({}, h.ctx);
	await h.settleExec();
	assert.deepEqual(h.statusUpdates.at(-1), { key: "herdr-worker", text: undefined });
	assert.deepEqual(h.paneMetadataCalls.at(-1), CLEAR_TITLE);
	assert.deepEqual(h.activeTools(), []);
	await command(h, "from boss");
	assert.deepEqual(h.statusUpdates.at(-1), { key: "herdr-worker", text: "team \u21d0 boss" });
	await h.handlers.get("session_shutdown")![0]();
	assert.deepEqual(h.statusUpdates.at(-1), { key: "herdr-worker", text: undefined });
	assert.equal(h.events.listenerCount(), 0);
});

// Public expectations in this section are fixed to 112410c, before service rewiring.
const CREATE_PROGRESS = { content: [{ type: "text", text: "Splitting pane and starting worker\u2026" }], details: {} };
const BRIEF_DELIVERED = "Initial brief delivered; its report will arrive as an [agent] message on a later turn.";
const BUILDER_UNTASKED = 'No initial brief given; use SendToAgent({ target_id: "agent-builder", \u2026 }) to task it.';
const SCOUT_UNTASKED = 'No initial brief given; use SendToAgent({ target_id: "agent-scout", \u2026 }) to task it.';

test("baseline tool definitions preserve names, descriptions, prompt guidance, and public JSON schemas", async (t) => {
	const h = await commandHarness(t);
	assert.deepEqual([...h.tools.keys()].sort(), ["CreateAgentPanel", "ReportWorkerRun", "SendToAgent"]);
	const create = h.tools.get("CreateAgentPanel");
	assert.deepEqual({ name: create.name, label: create.label, description: create.description, promptSnippet: create.promptSnippet, promptGuidelines: create.promptGuidelines }, {
		name: "CreateAgentPanel", label: "Create Agent Panel",
		description: "Create a new team member: splits a herdr pane next to you and starts a pi worker there that is orchestrated by you. Workers are named agent-<name> (default agent-<type> or agent-N). Use `type` to describe the kind of agent (explore, research, review, implement, test, \u2026) and `purpose` for a one-line charter (e.g. 'explore only, never edit files'). `model` defaults to your own model, except explore/research default to xai/grok-4.6. `initial_prompt` is delivered as the worker's first brief right after startup. Returns the worker's id for SendToAgent.",
		promptSnippet: "Spawn a new orchestrated pi worker in a neighboring herdr pane (type, purpose, model, initial brief)",
		promptGuidelines: ["Use CreateAgentPanel when the user asks for a new team member / worker / agent panel, or when a task benefits from a separate agent (e.g. an explore-only scout). Give it a concrete initial_prompt."],
	});
	assert.deepEqual(JSON.parse(JSON.stringify(create.parameters)), {
		type: "object", properties: {
			name: { type: "string", description: "Short name; becomes agent-<name>. Defaults to the type or a number." },
			direction: { type: "string", enum: ["right", "down", "left", "up"], description: "Side of your pane to place the worker (default right). Further workers on that side stack." },
			type: { type: "string", description: "Kind of agent: explore | research | review | implement | test | <anything>" },
			purpose: { type: "string", description: "One-line charter for this worker, e.g. 'explore only, report findings, never edit files'" },
			model: { type: "string", description: "provider/id, e.g. xai/grok-4.6 or anthropic/claude-sonnet-4-5. Default: your model (explore/research: xai/grok-4.6)." },
			thinking: { type: "string", enum: ["off", "minimal", "low", "medium", "high", "xhigh", "max"], description: "Thinking level for the worker (default: model default)" },
			initial_prompt: { type: "string", description: "First brief for the worker (Markdown). Self-contained: goal, context, constraints, what to report back." },
		},
	});
	const send = h.tools.get("SendToAgent");
	assert.deepEqual({ name: send.name, label: send.label, description: send.description, promptSnippet: send.promptSnippet, promptGuidelines: send.promptGuidelines }, {
		name: "SendToAgent", label: "Send To Agent",
		description: "Send a message to another of your user's agents running in a herdr pane (an orchestrator or a worker). The message is delivered asynchronously and shows up in their chat as an '[agent] ...' custom message; replies come back the same way on a later turn. target_id is the herdr agent name (e.g. a worker name) or pane id. priority=true steers the target mid-task (interrupts its current turn); priority=false queues a follow-up after its current work finishes.",
		promptSnippet: "Message another herdr-hosted agent (orchestrator \u2194 worker) asynchronously",
		promptGuidelines: [
			"Use SendToAgent for questions and ordinary communication. A bound worker must use ReportWorkerRun for terminal completion or failure.",
			"When delegating to workers, write self-contained messages with goal, context, constraints, and what to report back.",
			"SendToAgent is fire-and-forget: do not wait or poll for a reply \u2014 end your turn and react when the '[agent]' message arrives.",
		],
	});
	assert.deepEqual(JSON.parse(JSON.stringify(send.parameters)), {
		type: "object", required: ["message", "target_id"], properties: {
			message: { type: "string", description: "Full message body (Markdown). Self-contained: the recipient does not see your conversation." },
			target_id: { type: "string", description: "Herdr agent name or pane id of the recipient" },
			priority: { type: "boolean", description: "true = steer (interrupt recipient's current turn). false/omitted = follow-up after its current work.", default: false },
		},
	});
	const report = h.tools.get("ReportWorkerRun");
	assert.deepEqual({ name: report.name, label: report.label, description: report.description, promptSnippet: report.promptSnippet, promptGuidelines: report.promptGuidelines }, {
		name: "ReportWorkerRun", label: "Report Worker Run",
		description: "Report an authoritative message, successful completion, or failure for the worker's currently bound assignment. Identity and ordering are supplied by the extension.",
		promptSnippet: "Report progress or the explicit outcome of the current worker assignment",
		promptGuidelines: [
			"ReportWorkerRun is the only terminal reporting path for a bound assignment. Use SendToAgent only for questions and ordinary communication.",
			"Report completed only after the assignment is actually complete, with a non-empty result and any artifact or verification references. Report failed with a concrete error when it cannot be completed.",
		],
	});
});

for (const row of [
	{ label: "empty", state: {}, active: [] },
	{ label: "enabled orchestrator", state: { teamMode: true }, active: ["CreateAgentPanel"] },
	{ label: "owned roster without team mode", state: { workers: ["agent-scout"] }, active: ["SendToAgent"] },
	{ label: "enabled with workers", state: { teamMode: true, workers: ["agent-scout"] }, active: ["CreateAgentPanel", "SendToAgent"] },
	{ label: "worker", state: { orchestratedBy: "boss" }, active: ["SendToAgent"] },
	{ label: "mixed roster and bound worker", state: { teamMode: true, workers: ["agent-scout"], orchestratedBy: "boss", activeRun: ACTIVE_RUN }, active: ["SendToAgent", "ReportWorkerRun"] },
	{ label: "binding without ownership", state: { teamMode: true, activeRun: ACTIVE_RUN }, active: ["CreateAgentPanel"] },
	{ label: "noninteractive", state: { teamMode: true, workers: ["agent-scout"], orchestratedBy: "boss", activeRun: ACTIVE_RUN }, mode: "rpc", active: [] },
]) {
	test(`baseline tool activation for ${row.label} preserves unrelated tools and shutdown behavior`, async (t) => {
		const h = await commandHarness(t, { branch: teamBranch(row.state), mode: row.mode, activeTools: ["read", "CreateAgentPanel", "SendToAgent", "ReportWorkerRun", "bash"] });
		const expectedActive: string[] = row.active;
		assert.deepEqual(h.activeTools(), ["read", ...expectedActive, "bash"]);
		if (!expectedActive.includes("CreateAgentPanel")) {
			await assert.rejects(h.executeTool("CreateAgentPanel", {}), { message: "CreateAgentPanel is only available to an orchestrator (run /team here first)." });
			assert.deepEqual(h.partialToolUpdates, []);
		}
		if (!expectedActive.includes("ReportWorkerRun") && !("activeRun" in row.state && "orchestratedBy" in row.state)) {
			await assert.rejects(h.executeTool("ReportWorkerRun", { status: "message", message: "Progress" }), { message: "No active worker run is bound." });
		}
		const before = [...h.activeTools()];
		await h.handlers.get("session_shutdown")![0]();
		assert.deepEqual(h.activeTools(), before, "shutdown does not itself refresh the host tool list");
		await assert.rejects(h.executeTool("CreateAgentPanel", {}), { message: "CreateAgentPanel is only available to an orchestrator (run /team here first)." });
		assert.deepEqual(h.partialToolUpdates, []);
		assert.equal(h.events.listenerCount(), 0);
	});
}

type AdapterHarness = Awaited<ReturnType<typeof harness>>;

function assertCreation(h: AdapterHarness, result: any, expected: {
	name: string; paneId?: string; model?: string; type?: string; purpose?: string; cwd?: string;
	how?: string; adopted?: boolean; initial_prompt?: string; text: string;
}, assignment: Record<string, string>) {
	const { text, ...details } = expected;
	assert.deepEqual(result, { content: [{ type: "text", text }], details: {
		runId: result.details.runId, correlationId: undefined, paneId: "new-pane", model: undefined, type: undefined,
		purpose: undefined, cwd: "/tmp", how: "new column right", adopted: false, initial_prompt: undefined, ...details,
	} });
	assert.match(result.details.runId, /^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/);
	assert.deepEqual(h.partialToolUpdates, [{ id: "call", update: CREATE_PROGRESS }]);
	const registration = h.entries.find((entry) => entry.type === RUN_REGISTRATION_ENTRY)?.data;
	assert.ok(Number.isFinite(registration?.registeredAt));
	assert.deepEqual({ ...registration, registeredAt: 0 }, {
		version: 1, runId: result.details.runId, sessionId: "session", registeredAt: 0, lifecycleProtocol: 2, assignment,
	});
	const endpoint = h.entries.find((entry) => entry.type === RUN_ENDPOINT_BINDING_ENTRY)?.data;
	assert.ok(Number.isFinite(endpoint?.observedAt));
	assert.deepEqual({ ...endpoint, observedAt: 0 }, {
		version: 1, runId: result.details.runId, sessionId: "session", agentName: expected.name,
		paneId: expected.paneId ?? "new-pane", observedAt: 0,
	});
	const meta = h.entries.filter((entry) => entry.type === "herdr-worker").at(-1)?.data.meta[expected.name];
	assert.equal(meta.runId, result.details.runId);
	assert.equal(meta.paneId, result.details.paneId);
	assert.equal(meta.lifecycleProtocol, 2);
	assert.ok(h.timeline.indexOf("update:CreateAgentPanel") < h.timeline.indexOf(`append:${RUN_REGISTRATION_ENTRY}`));
	assert.ok(h.timeline.indexOf(`append:${RUN_REGISTRATION_ENTRY}`) < h.timeline.indexOf(`append:${RUN_ENDPOINT_BINDING_ENTRY}`));
	for (const { envelope } of h.writtenEnvelopes) {
		if (envelope.type === "message") assert.equal(envelope.runId, result.details.runId);
		if (envelope.action === "bind-run") assert.deepEqual(envelope.binding, { protocol: 2, runId: result.details.runId });
	}
	for (const entry of h.entries.filter((entry) => entry.type === LIFECYCLE_JOURNAL_ENTRY)) {
		assert.equal(entry.data.event.runId, result.details.runId);
		assert.deepEqual(entry.data.event.worker, { name: expected.name, paneId: result.details.paneId });
	}
}

const newWorkerCases: Array<{
	label: string; params: Record<string, unknown>; options?: Parameters<typeof harness>[0];
	expected: Parameters<typeof assertCreation>[2]; assignment: Record<string, string>; launch: string[];
}> = [
	{
		label: "all omitted", params: {},
		expected: { name: "agent-1", model: "test/model", text: 'Worker agent-1 ready in pane new-pane (new column right).\nModel: test/model\nNo initial brief given; use SendToAgent({ target_id: "agent-1", \u2026 }) to task it.' },
		assignment: { cwd: "/tmp", model: "test/model" }, launch: ["--model", "test/model"],
	},
	{
		label: "automatic name collision", params: {}, options: { agents: [{ pane_id: "taken", name: "agent-1" }] },
		expected: { name: "agent-2", model: "test/model", text: 'Worker agent-2 ready in pane new-pane (new column right).\nModel: test/model\nNo initial brief given; use SendToAgent({ target_id: "agent-2", \u2026 }) to task it.' },
		assignment: { cwd: "/tmp", model: "test/model" }, launch: ["--model", "test/model"],
	},
	{
		label: "every optional argument", params: { name: "  BuIlDeR  ", direction: "left", type: " Research ", purpose: "  Map the code  ", model: " other/explicit ", thinking: "high", initial_prompt: "  Inspect auth\nReport findings  " },
		expected: { name: "agent-builder", model: "other/explicit", type: "research", purpose: "Map the code", how: "new column left", initial_prompt: "  Inspect auth\nReport findings  ", text: "Worker agent-builder ready in pane new-pane (new column left).\nRole: research \u2014 Map the code\nModel: other/explicit\n" + BRIEF_DELIVERED },
		assignment: { cwd: "/tmp", model: "other/explicit", role: "research" }, launch: ["--model", "other/explicit:high", "--team-role", "research: Map the code"],
	},
	{
		label: "research type name and default model", params: { name: "  ", type: " ReSeArCh ", model: "  " },
		expected: { name: "agent-research", model: "xai/grok-4.6", type: "research", text: 'Worker agent-research ready in pane new-pane (new column right).\nRole: research\nModel: xai/grok-4.6\nNo initial brief given; use SendToAgent({ target_id: "agent-research", \u2026 }) to task it.' },
		assignment: { cwd: "/tmp", model: "xai/grok-4.6", role: "research" }, launch: ["--model", "xai/grok-4.6", "--team-role", "research"],
	},
	{
		label: "explore default without context model", params: { type: " Explore " }, options: { model: null },
		expected: { name: "agent-explore", model: "xai/grok-4.6", type: "explore", text: 'Worker agent-explore ready in pane new-pane (new column right).\nRole: explore\nModel: xai/grok-4.6\nNo initial brief given; use SendToAgent({ target_id: "agent-explore", \u2026 }) to task it.' },
		assignment: { cwd: "/tmp", model: "xai/grok-4.6", role: "explore" }, launch: ["--model", "xai/grok-4.6", "--team-role", "explore"],
	},
	{
		label: "custom type with context model", params: { name: "agent-builder", type: " CuStOm ", purpose: "  " }, options: { model: { provider: "context", id: "selected" } },
		expected: { name: "agent-builder", model: "context/selected", type: "custom", text: "Worker agent-builder ready in pane new-pane (new column right).\nRole: custom\nModel: context/selected\n" + BUILDER_UNTASKED },
		assignment: { cwd: "/tmp", model: "context/selected", role: "custom" }, launch: ["--model", "context/selected", "--team-role", "custom"],
	},
	{
		label: "absent model ignores thinking with purpose only", params: { name: "builder", type: " ", purpose: "  Check auth  ", thinking: "max", initial_prompt: "" }, options: { model: null },
		expected: { name: "agent-builder", purpose: "Check auth", initial_prompt: "", text: "Worker agent-builder ready in pane new-pane (new column right).\nRole: Check auth\n" + BUILDER_UNTASKED },
		assignment: { cwd: "/tmp" }, launch: ["--team-role", "Check auth"],
	},
	{
		label: "owned name collision", params: { name: "scout" }, options: { branch: teamBranch({ teamMode: true, workers: ["agent-scout"] }), agents: [{ pane_id: "worker-pane", name: "agent-scout" }, { pane_id: "taken", name: "agent-scout-2" }] },
		expected: { name: "agent-scout-3", model: "test/model", text: 'Worker agent-scout-3 ready in pane new-pane (new column right).\nModel: test/model\nNo initial brief given; use SendToAgent({ target_id: "agent-scout-3", \u2026 }) to task it.' },
		assignment: { cwd: "/tmp", model: "test/model" }, launch: ["--model", "test/model"],
	},
	{
		label: "whitespace brief echoed but not delivered", params: { name: "builder", initial_prompt: " \n " },
		expected: { name: "agent-builder", model: "test/model", initial_prompt: " \n ", text: "Worker agent-builder ready in pane new-pane (new column right).\nModel: test/model\n" + BRIEF_DELIVERED },
		assignment: { cwd: "/tmp", model: "test/model" }, launch: ["--model", "test/model"],
	},
];

for (const row of newWorkerCases) {
	test(`baseline tool creation: ${row.label}`, async (t) => {
		const h = await commandHarness(t, { branch: teamBranch({ teamMode: true }), listening: true, ...row.options });
		const result = await h.executeTool("CreateAgentPanel", row.params);
		assertCreation(h, result, row.expected, row.assignment);
		const split = h.execCalls.find((args) => args[1] === "split");
		assert.deepEqual(split, ["pane", "split", "self-pane", "--direction", "right", "--cwd", "/tmp", "--no-focus"]);
		const startIndex = h.execCalls.findIndex((args) => args[1] === "start");
		assert.deepEqual(h.execCalls[startIndex], ["agent", "start", row.expected.name, "--kind", "pi", "--pane", "new-pane", "--timeout", "60000", "--", "--orchestrated-by", "orchestrator", "--worker-run-id", result.details.runId, "--worker-lifecycle-protocol", "2", ...row.launch]);
		assert.equal(h.execOptions[startIndex].timeout, 70000);
		assert.equal(h.execOptions[h.execCalls.indexOf(split!)].timeout, 15000);
		assert.deepEqual(h.execCalls.filter((args) => args[1] === "swap"), row.params.direction === "left" ? [["pane", "swap", "--source-pane", "self-pane", "--target-pane", "new-pane"]] : []);
		assert.deepEqual(h.writtenEnvelopes.map(({ paneId, envelope }) => ({ paneId, envelope: { ...envelope, ts: 0 } })), row.label === "every optional argument" ? [{
			paneId: "new-pane", envelope: { type: "message", from: { id: "orchestrator", paneId: "self-pane", name: "orchestrator", role: "orchestrator" }, message: "  Inspect auth\nReport findings  ", priority: false, ts: 0, runId: result.details.runId },
		}] : []);
		assert.deepEqual(h.entries.filter((entry) => entry.type === "herdr-worker").at(-1)?.data.meta[row.expected.name], {
			type: row.expected.type, purpose: row.expected.purpose, model: row.expected.model, paneId: "new-pane", runId: result.details.runId,
			correlationId: undefined, requestId: undefined, providerInstanceId: undefined, lifecycleProtocol: 2,
		});
	});
}

for (const row of [
	{ direction: "right", native: "right", how: "new column right", swap: false },
	{ direction: "down", native: "down", how: "new row down", swap: false },
	{ direction: "left", native: "right", how: "new column left", swap: true },
	{ direction: "up", native: "down", how: "new row up", swap: true },
]) {
	test(`baseline tool direction ${row.direction} uses native split and optional swap`, async (t) => {
		const h = await commandHarness(t, { branch: teamBranch({ teamMode: true }), model: null });
		const result = await h.executeTool("CreateAgentPanel", { name: "builder", direction: row.direction });
		assertCreation(h, result, { name: "agent-builder", how: row.how, text: `Worker agent-builder ready in pane new-pane (${row.how}).\n` + BUILDER_UNTASKED }, { cwd: "/tmp" });
		assert.deepEqual(h.execCalls.filter((args) => ["split", "swap"].includes(args[1])), [
			["pane", "split", "self-pane", "--direction", row.native, "--cwd", "/tmp", "--no-focus"],
			...(row.swap ? [["pane", "swap", "--source-pane", "self-pane", "--target-pane", "new-pane"]] : []),
		]);
	});
}

for (const thinking of ["off", "minimal", "low", "medium", "high", "xhigh", "max"]) {
	test(`baseline tool thinking ${thinking} changes launch but not returned model`, async (t) => {
		const h = await commandHarness(t, { branch: teamBranch({ teamMode: true }) });
		const result = await h.executeTool("CreateAgentPanel", { name: "builder", thinking });
		assertCreation(h, result, { name: "agent-builder", model: "test/model", text: "Worker agent-builder ready in pane new-pane (new column right).\nModel: test/model\n" + BUILDER_UNTASKED }, { cwd: "/tmp", model: "test/model" });
		const start = h.execCalls.find((args) => args[1] === "start")!;
		assert.deepEqual(start.slice(start.indexOf("--model")), ["--model", `test/model:${thinking}`]);
	});
}

for (const saved of [false, true]) {
	for (const brief of [undefined, "", "  Map auth\nReport findings  ", " \n "]) {
		test(`baseline tool re-adoption preserves ${saved ? "stored" : "absent"} metadata and brief ${JSON.stringify(brief)}`, async (t) => {
			const prior = saved ? { type: "review", purpose: "Keep the charter", model: "saved/model", paneId: "old-pane", runId: "old-run", correlationId: "old-correlation", requestId: "old-request", providerInstanceId: "old-provider", lifecycleProtocol: 1 } : undefined;
			const h = await commandHarness(t, { branch: teamBranch({ teamMode: true, ...(saved ? { meta: { "agent-scout": prior } } : {}) }), listening: true, workerCwd: "/workspace/live" });
			const result = await h.executeTool("CreateAgentPanel", { name: " ScOuT ", direction: "up", type: " Research ", purpose: "Replace charter", model: "replace/model", thinking: "max", ...(brief === undefined ? {} : { initial_prompt: brief }) });
			assertCreation(h, result, {
				name: "agent-scout", paneId: "worker-pane", cwd: "/workspace/live", adopted: true, how: "re-adopted existing pane",
				type: saved ? "review" : undefined, purpose: saved ? "Keep the charter" : undefined, model: saved ? "saved/model" : undefined, initial_prompt: brief,
				text: "Worker agent-scout ready in pane worker-pane (re-adopted existing pane).\n" + (saved ? "Role: review \u2014 Keep the charter\nModel: saved/model\n" : "") + (brief ? BRIEF_DELIVERED : SCOUT_UNTASKED),
			}, { cwd: "/tmp", model: "replace/model", role: "research" });
			assert.deepEqual(h.entries.filter((entry) => entry.type === "herdr-worker").at(-1)?.data.meta["agent-scout"], {
				...prior, paneId: "worker-pane", runId: result.details.runId, correlationId: undefined, requestId: undefined, providerInstanceId: undefined, lifecycleProtocol: 2,
			});
			assert.deepEqual(h.execCalls.filter((args) => ["split", "swap", "start", "prompt"].includes(args[1])), []);
			assert.deepEqual(h.entries.filter((entry) => entry.type === LIFECYCLE_JOURNAL_ENTRY), []);
			assert.deepEqual(h.writtenEnvelopes.map(({ paneId, envelope }) => ({ paneId, envelope: { ...envelope, ts: 0 } })), [
				{ paneId: "worker-pane", envelope: { type: "control", action: "orchestrated-by", from: { id: "orchestrator", paneId: "self-pane", name: "orchestrator", role: "orchestrator" }, ts: 0 } },
				{ paneId: "worker-pane", envelope: { type: "control", action: "bind-run", from: { id: "orchestrator", paneId: "self-pane", name: "orchestrator", role: "orchestrator" }, ts: 0, binding: { protocol: 2, runId: result.details.runId } } },
				...(brief ? [{ paneId: "worker-pane", envelope: { type: "message", from: { id: "orchestrator", paneId: "self-pane", name: "orchestrator", role: "orchestrator" }, message: brief, priority: false, ts: 0, runId: result.details.runId } }] : []),
			]);
		});
	}
}

for (const row of [
	{ label: "missing", workerCwd: null, cwd: "/tmp" },
	{ label: "empty", workerCwd: "", cwd: "/tmp" },
	{ label: "blank", workerCwd: " \t ", cwd: "/tmp" },
	{ label: "untrimmed", workerCwd: " /workspace/live ", cwd: " /workspace/live " },
]) {
	test(`baseline tool re-adoption uses ${row.label} live CWD contract`, async (t) => {
		const h = await commandHarness(t, { branch: teamBranch({ teamMode: true }), workerCwd: row.workerCwd });
		const result = await h.executeTool("CreateAgentPanel", { name: "scout" });
		assertCreation(h, result, { name: "agent-scout", paneId: "worker-pane", cwd: row.cwd, adopted: true, how: "re-adopted existing pane", text: "Worker agent-scout ready in pane worker-pane (re-adopted existing pane).\n" + SCOUT_UNTASKED }, { cwd: "/tmp", model: "test/model" });
		assert.deepEqual(h.writtenEnvelopes, [], "a non-listening existing worker can still be re-adopted");
	});
}

test("baseline tool name truncation leaves room for collision suffixes", async (t) => {
	const h = await commandHarness(t, {
		branch: teamBranch({ teamMode: true }), model: null,
		agents: [{ pane_id: "taken", name: "agent-abcdefghijklmnopqrstuvwxyz" }],
	});
	const result = await h.executeTool("CreateAgentPanel", { name: "abcdefghijklmnopqrstuvwxyz123456" });
	assertCreation(h, result, {
		name: "agent-abcdefghijklmnopqrstuvwx-2",
		text: 'Worker agent-abcdefghijklmnopqrstuvwx-2 ready in pane new-pane (new column right).\nNo initial brief given; use SendToAgent({ target_id: "agent-abcdefghijklmnopqrstuvwx-2", \u2026 }) to task it.',
	}, { cwd: "/tmp" });
	assert.equal(h.execCalls.find((args) => args[1] === "start")?.[2], "agent-abcdefghijklmnopqrstuvwx-2");
});

test("baseline tool does not re-adopt a requested worker in another tab", async (t) => {
	const h = await commandHarness(t, {
		branch: teamBranch({ teamMode: true }), model: null,
		agents: [{ pane_id: "foreign-pane", name: "agent-scout", tab_id: "other-tab" }],
		execOverride: (args) => args[1] === "get" && args[2] === "agent-scout"
			? { code: 0, stdout: '{"result":{"agent":{"pane_id":"foreign-pane","name":"agent-scout","tab_id":"other-tab"}}}', stderr: "" }
			: undefined,
	});
	const result = await h.executeTool("CreateAgentPanel", { name: "scout" });
	assertCreation(h, result, {
		name: "agent-scout-2",
		text: 'Worker agent-scout-2 ready in pane new-pane (new column right).\nNo initial brief given; use SendToAgent({ target_id: "agent-scout-2", \u2026 }) to task it.',
	}, { cwd: "/tmp" });
	assert.deepEqual(h.writtenEnvelopes, []);
});

test("baseline tool creation names an unnamed orchestrator before launching its worker", async (t) => {
	let renamed = false;
	const h = await commandHarness(t, {
		branch: teamBranch({ teamMode: true }), selfName: "", model: null,
		execOverride: (args) => {
			if (args[1] === "rename") renamed = true;
			if (renamed && args[1] === "get" && args[2] === "self-pane") {
				return { code: 0, stdout: '{"result":{"agent":{"pane_id":"self-pane","name":"orchestrator","tab_id":"tab-1"}}}', stderr: "" };
			}
		},
	});
	const result = await h.executeTool("CreateAgentPanel", { name: "builder" });
	assertCreation(h, result, { name: "agent-builder", text: "Worker agent-builder ready in pane new-pane (new column right).\n" + BUILDER_UNTASKED }, { cwd: "/tmp" });
	assert.deepEqual(h.execCalls.find((args) => args[1] === "rename"), ["agent", "rename", "self-pane", "orchestrator"]);
	const start = h.execCalls.find((args) => args[1] === "start")!;
	assert.equal(start[start.indexOf("--orchestrated-by") + 1], "orchestrator");
	assert.ok(h.timeline.indexOf("exec:agent:rename") < h.timeline.indexOf("exec:pane:split"));
});

for (const row of [
	{ label: "invalid name", params: { name: "  Bad Name  " }, message: 'Invalid worker name "bad name" (use [a-z][a-z0-9_-]{0,31}; not add/list/release/from/status/help/adopt/right/down/left/up)' },
	{ label: "overlong name", params: { name: "abcdefghijklmnopqrstuvwxyz1234567" }, message: 'Invalid worker name "abcdefghijklmnopqrstuvwxyz1234567" (use [a-z][a-z0-9_-]{0,31}; not add/list/release/from/status/help/adopt/right/down/left/up)' },
	{ label: "reserved type name", params: { type: " UP " }, message: 'Invalid worker name "up" (use [a-z][a-z0-9_-]{0,31}; not add/list/release/from/status/help/adopt/right/down/left/up)' },
	{ label: "relative CWD", params: {}, contextCwd: "relative", message: "Worker cwd must be an absolute accessible directory." },
	{ label: "missing CWD", params: {}, contextCwd: "/no-such-team-contract-directory", message: "Worker cwd must be an absolute accessible directory." },
	{ label: "file CWD", params: {}, contextCwd: fileURLToPath(import.meta.url), message: "Worker cwd must be an absolute accessible directory." },
]) {
	test(`baseline tool rejects ${row.label} after progress but before registration`, async (t) => {
		const h = await commandHarness(t, { branch: teamBranch({ teamMode: true }), contextCwd: row.contextCwd });
		const before = h.execCalls.length;
		await assert.rejects(h.executeTool("CreateAgentPanel", row.params), { message: row.message });
		assert.deepEqual(h.partialToolUpdates, [{ id: "call", update: CREATE_PROGRESS }]);
		assert.deepEqual(h.entries, []);
		assert.deepEqual(h.execCalls.slice(before), []);
		assert.deepEqual(h.writtenEnvelopes, []);
	});
}

for (const row of [
	{ label: "split stderr", stage: "split", response: { code: 1, stdout: "ignored stdout", stderr: "split failed: private path /tmp/worker" }, message: "split failed: private path /tmp/worker", lifecycle: [["uncertain", "pane_creation"]] },
	{ label: "structured split error", stage: "split", response: { code: 1, stdout: "", stderr: '{"error":{"code":"SPLIT_DENIED","message":"private split detail"}}' }, message: "SPLIT_DENIED: private split detail", lifecycle: [["uncertain", "pane_creation"]] },
	{ label: "split stdout fallback", stage: "split", response: { code: 1, stdout: "stdout split detail", stderr: "" }, message: "stdout split detail", lifecycle: [["uncertain", "pane_creation"]] },
	{ label: "split exit fallback", stage: "split", response: { code: 9, stdout: "", stderr: "" }, message: "herdr pane split failed (exit 9)", lifecycle: [["uncertain", "pane_creation"]] },
	{ label: "missing split identity", stage: "split", response: { code: 0, stdout: '{"result":{"unexpected":"detail"}}', stderr: "" }, message: 'pane split returned no pane id: {"result":{"unexpected":"detail"}}', lifecycle: [["uncertain", "pane_creation"]] },
	{ label: "start", stage: "start", response: { code: 1, stdout: "", stderr: "start failed: private launch detail" }, message: "Started pane new-pane but agent start failed: start failed: private launch detail. Check `herdr pane read new-pane`.", lifecycle: [["uncertain", "agent_start"]] },
	{ label: "new assignment", writeEnvelopeErrorForType: "message", message: "mailbox write failed", lifecycle: [["started", undefined], ["uncertain", "assignment_delivery"]] },
	{ label: "re-adopted assignment", name: "scout", writeEnvelopeErrorForType: "message", message: "mailbox write failed", lifecycle: [["uncertain", "assignment_delivery"]] },
]) {
	test(`baseline tool preserves detailed ${row.label} failure`, async (t) => {
		const h = await commandHarness(t, {
			branch: teamBranch({ teamMode: true }), listening: true, writeEnvelopeErrorForType: row.writeEnvelopeErrorForType,
			execOverride: (args) => row.stage && args[1] === row.stage ? row.response : undefined,
		});
		await assert.rejects(h.executeTool("CreateAgentPanel", { name: row.name ?? "builder", initial_prompt: "Do the work" }), { message: row.message });
		assert.deepEqual(h.partialToolUpdates, [{ id: "call", update: CREATE_PROGRESS }]);
		assert.equal(h.entries.filter((entry) => entry.type === RUN_REGISTRATION_ENTRY).length, 1);
		assert.deepEqual(h.entries.filter((entry) => entry.type === LIFECYCLE_JOURNAL_ENTRY).map(({ data }) => [data.event.status, data.event.evidence.scope]), row.lifecycle);
		assert.equal(h.execCalls.some((args) => ["close", "stop", "kill"].includes(args[1])), false);
	});
}

test("baseline tool already-aborted creation emits progress but registers no run", async (t) => {
	const h = await commandHarness(t, { branch: teamBranch({ teamMode: true }) });
	const controller = new AbortController();
	controller.abort(new Error("tool cancelled before registration"));
	const before = h.execCalls.length;
	await assert.rejects(h.executeTool("CreateAgentPanel", { name: "builder" }, controller.signal), { message: "tool cancelled before registration" });
	assert.deepEqual(h.partialToolUpdates, [{ id: "call", update: CREATE_PROGRESS }]);
	assert.deepEqual(h.entries, []);
	assert.deepEqual(h.execCalls.slice(before), []);
});

test("baseline tool creation does not forward a distinct tool signal after queue entry", async (t) => {
	const controller = new AbortController();
	const h = await commandHarness(t, {
		branch: teamBranch({ teamMode: true }), listening: true,
		execOverride: (args, options) => {
			if (args[1] === "split") {
				controller.abort(new Error("tool cancelled during split"));
				assert.equal(options.signal.aborted, false);
			}
		},
	});
	const result = await h.executeTool("CreateAgentPanel", { name: "builder", initial_prompt: "Continue assignment" }, controller.signal);
	assertCreation(h, result, { name: "agent-builder", model: "test/model", initial_prompt: "Continue assignment", text: "Worker agent-builder ready in pane new-pane (new column right).\nModel: test/model\n" + BRIEF_DELIVERED }, { cwd: "/tmp", model: "test/model" });
	assert.equal(h.execOptions.every((options) => !options.signal.aborted), true);
	assert.equal(h.writtenEnvelopes.at(-1)?.envelope.message, "Continue assignment");
});

const SEND_MESSAGE = "  Inspect auth\nReport findings  ";
const FRAMED_SEND_MESSAGE = '[agent] A message just arrived from another of your user\'s agents: Orchestrator "orchestrator" in pane self-pane.\nThis is another agent reaching out \u2014 not the user typing here. It arrived asynchronously, and your user can already see it in this chat.\n\nInspect auth\nReport findings\n\nIf it needs a reply or an action, handle it: reply to Orchestrator with SendToAgent (their id: orchestrator), which reaches them on a later turn \u2014 not a live back-and-forth. If it is just an FYI with nothing for you to do, it is fine to stay silent \u2014 no need to reply just to acknowledge it.';

for (const fails of [false, true]) {
	test(`baseline tool re-adopted brief uses prompt fallback, failure=${fails}`, async (t) => {
		const h = await commandHarness(t, {
			branch: teamBranch({ teamMode: true }),
			execOverride: (args) => fails && args[1] === "prompt" ? { code: 1, stdout: "", stderr: "assignment prompt failed: private detail" } : undefined,
		});
		const creation = h.executeTool("CreateAgentPanel", { name: "scout", initial_prompt: SEND_MESSAGE });
		if (fails) {
			await assert.rejects(creation, { message: "assignment prompt failed: private detail" });
			assert.deepEqual(h.partialToolUpdates, [{ id: "call", update: CREATE_PROGRESS }]);
			assert.deepEqual(h.entries.filter((entry) => entry.type === LIFECYCLE_JOURNAL_ENTRY).map(({ data }) => [data.event.status, data.event.evidence.scope]), [["uncertain", "assignment_delivery"]]);
		} else {
			assertCreation(h, await creation, {
				name: "agent-scout", paneId: "worker-pane", adopted: true, how: "re-adopted existing pane", initial_prompt: SEND_MESSAGE,
				text: "Worker agent-scout ready in pane worker-pane (re-adopted existing pane).\n" + BRIEF_DELIVERED,
			}, { cwd: "/tmp", model: "test/model" });
		}
		assert.deepEqual(h.execCalls.filter((args) => args[1] === "prompt"), [["agent", "prompt", "worker-pane", FRAMED_SEND_MESSAGE]]);
		assert.deepEqual(h.writtenEnvelopes, []);
	});
}

test("baseline tool bodies and purpose do not acquire RPC string limits", async (t) => {
	const h = await commandHarness(t, { branch: teamBranch({ teamMode: true }), listening: true, model: null });
	const purpose = "p".repeat(1025);
	const body = "b".repeat(65537);
	const created = await h.executeTool("CreateAgentPanel", { name: "builder", purpose, initial_prompt: body });
	assertCreation(h, created, {
		name: "agent-builder", purpose, initial_prompt: body,
		text: "Worker agent-builder ready in pane new-pane (new column right).\nRole: " + purpose + "\n" + BRIEF_DELIVERED,
	}, { cwd: "/tmp" });
	const sent = await h.executeTool("SendToAgent", { target_id: "agent-builder", message: body });
	assert.deepEqual(sent, {
		content: [{ type: "text", text: "Delivered to agent-builder (pane new-pane, idle) via inbox as follow-up. Replies arrive on a later turn." }],
		details: { target: "agent-builder", message: body, priority: false, status: "Delivered to agent-builder (pane new-pane, idle) via inbox as follow-up. Replies arrive on a later turn." },
	});
	assert.equal(h.writtenEnvelopes.length, 2);
	assert.equal(h.writtenEnvelopes[0].envelope.message, body);
	assert.equal(h.writtenEnvelopes[1].envelope.message, body);
});

for (const priority of [undefined, false, true]) {
	for (const listening of [false, true]) {
		for (const target of ["agent-scout", "worker-pane"]) {
			test(`baseline tool send selector=${target} priority=${priority} listening=${listening}`, async (t) => {
				const h = await commandHarness(t, { branch: teamBranch({ workers: ["agent-scout"] }), listening });
				const result = await h.executeTool("SendToAgent", { target_id: target, message: SEND_MESSAGE, ...(priority === undefined ? {} : { priority }) });
				const text = listening
					? priority ? "Delivered to agent-scout (pane worker-pane, idle) via inbox as steer (priority). Replies arrive on a later turn." : "Delivered to agent-scout (pane worker-pane, idle) via inbox as follow-up. Replies arrive on a later turn."
					: "Typed into agent-scout (pane worker-pane, pi, no inbox listener) via `herdr agent prompt`. Priority flag not applicable there.";
				assert.deepEqual(result, { content: [{ type: "text", text }], details: { target, priority: priority ?? false, message: SEND_MESSAGE, status: text } });
				assert.deepEqual(h.partialToolUpdates, []);
				assert.deepEqual(h.entries, [], "ordinary messaging creates no assignment or terminal evidence");
				if (listening) {
					assert.deepEqual(h.writtenEnvelopes.map(({ paneId, envelope }) => ({ paneId, envelope: { ...envelope, ts: 0 } })), [{
						paneId: "worker-pane", envelope: { type: "message", from: { id: "orchestrator", paneId: "self-pane", name: "orchestrator", role: "orchestrator" }, message: SEND_MESSAGE, priority: priority ?? false, ts: 0 },
					}]);
					assert.deepEqual(h.execCalls.filter((args) => args[1] === "prompt"), []);
				} else {
					assert.deepEqual(h.writtenEnvelopes, []);
					const index = h.execCalls.findIndex((args) => args[1] === "prompt");
					assert.deepEqual(h.execCalls[index], ["agent", "prompt", "worker-pane", FRAMED_SEND_MESSAGE]);
					assert.equal(h.execOptions[index].timeout, 20000);
				}
			});
		}
	}
}

for (const row of [
	{ label: "missing status", agent: { pane_id: "worker-pane", name: "agent-scout", agent: "pi" }, listening: true, text: "Delivered to agent-scout (pane worker-pane, unknown) via inbox as follow-up. Replies arrive on a later turn." },
	{ label: "missing kind and name", agent: { pane_id: "worker-pane" }, listening: true, text: "Typed into worker-pane (pane worker-pane, agent, no inbox listener) via `herdr agent prompt`. Priority flag not applicable there." },
	{ label: "other agent kind", agent: { pane_id: "worker-pane", name: "agent-scout", agent: "claude" }, listening: true, text: "Typed into agent-scout (pane worker-pane, claude, no inbox listener) via `herdr agent prompt`. Priority flag not applicable there." },
]) {
	test(`baseline tool send receipt handles ${row.label}`, async (t) => {
		const h = await commandHarness(t, {
			branch: teamBranch({ workers: ["worker-pane"] }), listening: row.listening,
			execOverride: (args) => args[1] === "get" && args[2] === "worker-pane" ? { code: 0, stdout: JSON.stringify({ result: { agent: row.agent } }), stderr: "" } : undefined,
		});
		const result = await h.executeTool("SendToAgent", { target_id: "worker-pane", message: "FYI" });
		assert.deepEqual(result, { content: [{ type: "text", text: row.text }], details: { target: "worker-pane", message: "FYI", priority: false, status: row.text } });
		assert.equal(h.writtenEnvelopes.length, row.label === "missing status" ? 1 : 0);
	});
}

for (const row of [
	{ label: "missing target with empty roster", target: "missing", state: {}, agents: [], message: 'No live herdr agent "missing". Known agents: (none)' },
	{ label: "missing target with team context", target: "missing", state: { workers: ["agent-scout"], orchestratedBy: "boss" }, agents: [{ pane_id: "worker-pane", name: "agent-scout" }, { pane_id: "unnamed-pane" }], message: 'No live herdr agent "missing". Known agents: agent-scout, unnamed-pane. Your workers: agent-scout. Your orchestrator: boss' },
	{ label: "self", target: "self-pane", state: { workers: ["agent-scout"] }, message: "Refusing to send a message to yourself." },
	{ label: "outside empty team", target: "agent-scout", state: {}, message: '"agent-scout" is not in your team (it would drop the message anyway). Workers: (none); orchestrator: (none). Use /team add or /team adopt first.' },
	{ label: "outside populated team", target: "agent-scout", state: { workers: ["agent-other"], orchestratedBy: "boss" }, message: '"agent-scout" is not in your team (it would drop the message anyway). Workers: agent-other; orchestrator: boss. Use /team add or /team adopt first.' },
	{ label: "Herdr prompt failure", target: "agent-scout", state: { workers: ["agent-scout"] }, fail: true, message: "PROMPT_FAILED: private prompt detail" },
]) {
	test(`baseline tool send exposes detailed ${row.label} error`, async (t) => {
		const h = await commandHarness(t, {
			branch: teamBranch(row.state), agents: row.agents,
			execOverride: (args) => row.fail && args[1] === "prompt" ? { code: 1, stdout: "", stderr: '{"error":{"code":"PROMPT_FAILED","message":"private prompt detail"}}' } : undefined,
		});
		await assert.rejects(h.executeTool("SendToAgent", { target_id: row.target, message: "FYI" }), { message: row.message });
		assert.deepEqual(h.partialToolUpdates, []);
		assert.deepEqual(h.writtenEnvelopes, []);
		assert.deepEqual(h.entries, []);
	});
}

for (const listening of [true, false]) {
	test(`baseline tool already-aborted send preserves ${listening ? "inbox publication" : "lookup then prompt cancellation"}`, async (t) => {
		const h = await commandHarness(t, { branch: teamBranch({ workers: ["agent-scout"] }), listening });
		const controller = new AbortController();
		controller.abort(new Error("send cancelled"));
		const before = h.execCalls.length;
		const send = h.executeTool("SendToAgent", { target_id: "agent-scout", message: "FYI" }, controller.signal);
		if (listening) {
			assert.deepEqual(await send, {
				content: [{ type: "text", text: "Delivered to agent-scout (pane worker-pane, idle) via inbox as follow-up. Replies arrive on a later turn." }],
				details: { target: "agent-scout", message: "FYI", priority: false, status: "Delivered to agent-scout (pane worker-pane, idle) via inbox as follow-up. Replies arrive on a later turn." },
			});
			assert.equal(h.writtenEnvelopes.length, 1);
		} else {
			await assert.rejects(send, { message: "send cancelled" });
			assert.deepEqual(h.writtenEnvelopes, []);
		}
		assert.deepEqual(h.execCalls.slice(before), [["agent", "get", "agent-scout"]]);
		assert.equal(h.execOptions.at(-1)?.signal.aborted, false, "target lookup uses context signal, not tool signal");
	});
}

test("baseline tool send forwards cancellation to an in-progress prompt fallback", async (t) => {
	const controller = new AbortController();
	const h = await commandHarness(t, {
		branch: teamBranch({ workers: ["agent-scout"] }),
		execOverride: (args, options) => {
			if (args[1] !== "prompt") return;
			assert.equal(options.timeout, 20000);
			assert.equal(options.signal.aborted, false);
			controller.abort(new Error("cancel prompt now"));
			assert.equal(options.signal.aborted, true);
			assert.equal(options.signal.reason, controller.signal.reason);
			return { code: 0, stdout: '{"result":{}}', stderr: "" };
		},
	});
	await assert.rejects(h.executeTool("SendToAgent", { target_id: "agent-scout", message: SEND_MESSAGE }, controller.signal), { message: "cancel prompt now" });
	assert.deepEqual(h.execCalls.find((args) => args[1] === "prompt"), ["agent", "prompt", "worker-pane", FRAMED_SEND_MESSAGE]);
	assert.equal(h.ctx.signal.aborted, false);
	assert.deepEqual(h.entries, []);
});

type ThemeCall = ["bold", string] | ["fg", string, string];
function rendererTheme() {
	const calls: ThemeCall[] = [];
	return {
		calls,
		theme: {
			bold(text: string) { calls.push(["bold", text]); return `<bold>${text}</bold>`; },
			fg(token: string, text: string) { calls.push(["fg", token, text]); return `<${token}>${text}</${token}>`; },
		},
	};
}

function assertRendered(component: { render(width: number): string[] }, expected: string) {
	// Text pads each line to the host width. Keep extension text, including spaces, exact.
	assert.deepEqual(component.render(4096), expected ? expected.split("\n").map((line) => line.padEnd(4096)) : []);
}

for (const row of [
	{ tool: "CreateAgentPanel", label: "default call", args: {}, text: "<toolTitle><bold>CreateAgentPanel</bold></toolTitle> <accent>agent-N</accent>", calls: [["bold", "CreateAgentPanel"], ["fg", "toolTitle", "<bold>CreateAgentPanel</bold>"], ["fg", "accent", "agent-N"]] },
	{ tool: "CreateAgentPanel", label: "type call", args: { type: "explore" }, text: "<toolTitle><bold>CreateAgentPanel</bold></toolTitle> <accent>agent-explore</accent> <muted>type=explore</muted>", calls: [["bold", "CreateAgentPanel"], ["fg", "toolTitle", "<bold>CreateAgentPanel</bold>"], ["fg", "accent", "agent-explore"], ["fg", "muted", "type=explore"]] },
	{ tool: "CreateAgentPanel", label: "un-normalized explicit call", args: { name: "agent-Mixed", direction: "up", type: " Research ", model: "other/model", purpose: "hidden", thinking: "high", initial_prompt: "hidden" }, text: "<toolTitle><bold>CreateAgentPanel</bold></toolTitle> <accent>agent-agent-Mixed</accent> <muted>up</muted> <muted>type= Research </muted> <dim>other/model</dim>", calls: [["bold", "CreateAgentPanel"], ["fg", "toolTitle", "<bold>CreateAgentPanel</bold>"], ["fg", "accent", "agent-agent-Mixed"], ["fg", "muted", "up"], ["fg", "muted", "type= Research "], ["fg", "dim", "other/model"]] },
	{ tool: "SendToAgent", label: "normal call", args: { target_id: "agent-scout", message: "hidden", priority: false }, text: "<toolTitle><bold>SendToAgent</bold></toolTitle><muted> \u2192 </muted><accent>agent-scout</accent>", calls: [["fg", "muted", " \u2192 "], ["bold", "SendToAgent"], ["fg", "toolTitle", "<bold>SendToAgent</bold>"], ["fg", "accent", "agent-scout"]] },
	{ tool: "SendToAgent", label: "priority call", args: { target_id: "worker-pane", priority: true }, text: "<toolTitle><bold>SendToAgent</bold></toolTitle><warning> \u21e8 </warning><accent>worker-pane</accent><warning> (priority / steer)</warning>", calls: [["fg", "warning", " \u21e8 "], ["bold", "SendToAgent"], ["fg", "toolTitle", "<bold>SendToAgent</bold>"], ["fg", "accent", "worker-pane"], ["fg", "warning", " (priority / steer)"]] },
	{ tool: "SendToAgent", label: "missing target call", args: {}, text: "<toolTitle><bold>SendToAgent</bold></toolTitle><muted> \u2192 </muted><accent>\u2026</accent>", calls: [["fg", "muted", " \u2192 "], ["bold", "SendToAgent"], ["fg", "toolTitle", "<bold>SendToAgent</bold>"], ["fg", "accent", "\u2026"]] },
]) {
	test(`baseline tool renderer ${row.tool} ${row.label}`, async (t) => {
		const h = await commandHarness(t);
		const { theme, calls } = rendererTheme();
		assertRendered(h.tools.get(row.tool).renderCall(row.args, theme), row.text);
		assert.deepEqual(calls, row.calls);
	});
}

for (const row of [
	{ tool: "CreateAgentPanel", label: "partial overrides error", result: CREATE_PROGRESS, isPartial: true, isError: true, text: "<dim>starting worker\u2026</dim>", calls: [["fg", "dim", "starting worker\u2026"]] },
	{ tool: "SendToAgent", label: "partial overrides error", result: {}, isPartial: true, isError: true, text: "<dim>sending\u2026</dim>", calls: [["fg", "dim", "sending\u2026"]] },
	{ tool: "CreateAgentPanel", label: "multi-block error", result: { content: [{ type: "text", text: "split failed" }, { type: "image" }, { type: "text", text: "private detail" }], details: { initial_prompt: "not shown", purpose: "not shown" } }, isError: true, text: "<error>split failed\n\nprivate detail</error>", calls: [["fg", "error", "split failed\n\nprivate detail"]] },
	{ tool: "SendToAgent", label: "multi-block error", result: { content: [{ type: "text", text: "send failed" }, { type: "image" }, { type: "text", text: "private detail" }], details: { message: "not shown", status: "not shown" } }, isError: true, text: "<error>send failed\n\nprivate detail</error>", calls: [["fg", "error", "send failed\n\nprivate detail"]] },
	{ tool: "SendToAgent", label: "missing error content", result: {}, isError: true, text: "<error>failed</error>", calls: [["fg", "error", "failed"]] },
	{ tool: "CreateAgentPanel", label: "missing error content", result: {}, isError: true, text: "<error></error>", calls: [["fg", "error", ""]] },
	{ tool: "CreateAgentPanel", label: "success without details", result: { content: [{ type: "text", text: "Ready" }] }, text: "<success>Ready</success>", calls: [["fg", "success", "Ready"]] },
	{ tool: "SendToAgent", label: "message fallback from context", result: { details: { status: "Receipt" } }, args: { message: "Context line\nSecond" }, text: "<accent>\u2502 </accent>Context line\n<accent>\u2502 </accent>Second\n<dim>Receipt</dim>", calls: [["fg", "accent", "\u2502 "], ["fg", "accent", "\u2502 "], ["fg", "dim", "Receipt"]] },
	{ tool: "SendToAgent", label: "empty details message wins over context", result: { details: { message: "" } }, args: { message: "Not shown" }, text: "<accent>\u2502 </accent>", calls: [["fg", "accent", "\u2502 "]] },
	{ tool: "SendToAgent", label: "absent message and context", result: {}, text: "<accent>\u2502 </accent>", calls: [["fg", "accent", "\u2502 "]] },
] as Array<{ tool: string; label: string; result: any; args?: any; isPartial?: boolean; isError?: boolean; text: string; calls: string[][] }>) {
	test(`baseline tool renderer ${row.tool} ${row.label}`, async (t) => {
		const h = await commandHarness(t);
		const { theme, calls } = rendererTheme();
		assertRendered(h.tools.get(row.tool).renderResult(row.result, { expanded: false, isPartial: row.isPartial ?? false }, theme, { args: row.args, isError: row.isError ?? false }), row.text);
		assert.deepEqual(calls, row.calls);
	});
}

const RENDER_LINES = ["first", "", "third", "fourth", "fifth", "sixth", "seventh", "eighth", "ninth", "tenth", "eleventh", "twelfth", "thirteenth", "fourteenth", ""];
for (const tool of ["CreateAgentPanel", "SendToAgent"]) {
	for (const expanded of [false, true]) {
		for (const { length, collapsedLines, omitted } of tool === "CreateAgentPanel"
			? [{ length: 8, collapsedLines: 8, omitted: 0 }, { length: 9, collapsedLines: 8, omitted: 1 }, { length: 15, collapsedLines: 8, omitted: 7 }]
			: [{ length: 12, collapsedLines: 12, omitted: 0 }, { length: 13, collapsedLines: 12, omitted: 1 }, { length: 15, collapsedLines: 12, omitted: 3 }]) {
			test(`baseline tool renderer ${tool} ${expanded ? "expanded" : "collapsed"} ${length}-line result`, async (t) => {
				const h = await commandHarness(t);
				const { theme, calls } = rendererTheme();
				const lines = RENDER_LINES.slice(0, length);
				const create = tool === "CreateAgentPanel";
				const shown = lines.slice(0, expanded ? length : collapsedLines);
				const body = shown.map((line) => `<accent>\u2502 </accent>${line}`).join("\n");
				const more = expanded ? 0 : omitted;
				const result = create
					? { content: [{ type: "text", text: "Worker ready" }], details: { purpose: "Map auth", initial_prompt: lines.join("\n") } }
					: { content: [{ type: "text", text: "Not the displayed body" }], details: { message: lines.join("\n"), status: "Delivered" } };
				assertRendered(h.tools.get(tool).renderResult(result, { expanded, isPartial: false }, theme, { args: { message: "Not used" }, isError: false }),
					(create ? "<success>Worker ready</success>\n<muted>purpose: Map auth</muted>\n" : "") + body
					+ (more ? `\n<dim>\u2502 \u2026 ${more} more lines</dim>` : "") + (create ? "" : "\n<dim>Delivered</dim>"));
				assert.deepEqual(calls, [
					...(create ? [["fg", "success", "Worker ready"], ["fg", "muted", "purpose: Map auth"]] : []),
					...shown.map(() => ["fg", "accent", "\u2502 "]),
					...(more ? [["fg", "dim", `\u2502 \u2026 ${more} more lines`]] : []),
					...(create ? [] : [["fg", "dim", "Delivered"]]),
				]);
			});
		}
	}
}

test("registers once, exposes live availability, and disposes on shutdown", async () => {
	const h = await harness();
	assert.equal(h.events.listenerCount(), 5);
	assert.deepEqual([...h.tools.keys()].sort(), ["CreateAgentPanel", "ReportWorkerRun", "SendToAgent"]);
	assert.deepEqual([...h.commands.keys()].sort(), ["orchestrated-by", "team"]);
	let probe = await emitForReply<any>(h.events, CHANNELS.probe, "before", { requestId: "before", supportedProtocols: [1] });
	assert.equal(probe.success && probe.data.reason, "SESSION_NOT_READY");
	await h.handlers.get("session_start")![0]({ reason: "startup" }, h.ctx);
	assert.equal(h.events.listenerCount(), 11);
	probe = await emitForReply<any>(h.events, CHANNELS.probe, "after", { requestId: "after", supportedProtocols: [1] });
	assert.equal(probe.success && probe.data.available, true);
	await h.handlers.get("session_tree")![0]({}, h.ctx);
	assert.equal(h.events.listenerCount(), 11);
	await h.handlers.get("session_shutdown")![0]();
	assert.equal(h.events.listenerCount(), 0);
});

test("RPC spawn validates cwd before side effects, activates team mode, and returns canonical facts", async () => {
	const h = await harness();
	await h.handlers.get("session_start")![0]({ reason: "startup" }, h.ctx);
	const probe = await emitForReply<any>(h.events, CHANNELS.probe, "probe", { requestId: "probe", supportedProtocols: [1] });
	assert.equal(probe.success, true);
	const providerInstanceId = probe.success ? probe.data.providerInstanceId : "";
	const callsBefore = h.execCalls.length;
	const invalid = await emitForReply(h.events, CHANNELS.spawn, "invalid", { requestId: "invalid", providerInstanceId, protocol: 1, cwd: "relative" });
	assert.equal(invalid.success, false);
	assert.equal(h.execCalls.length, callsBefore);
	assert.equal(h.entries.length, 0);

	const spawned = await emitForReply<WorkerReference>(h.events, CHANNELS.spawn, "spawn", { requestId: "spawn", providerInstanceId, protocol: 1, name: "scout" });
	assert.equal(spawned.success, true);
	if (!spawned.success) return;
	assert.match(spawned.data.runId, /^[0-9a-f-]{36}$/);
	assert.deepEqual({ ...spawned.data, runId: "<run>" }, { runId: "<run>", name: "agent-scout", paneId: "worker-pane", cwd: "/tmp", adopted: true });
	assert.equal(h.entries.some((entry) => entry.data.teamMode === true), true);
	assert.equal(h.entries.some((entry) => entry.data.meta?.["agent-scout"]?.runId === spawned.data.runId), true);
	const registration = h.entries.find((entry) => entry.type === RUN_REGISTRATION_ENTRY)?.data;
	assert.deepEqual(registration && { ...registration, runId: "<run>", registeredAt: 0 }, {
		version: 1,
		runId: "<run>",
		sessionId: "session",
		registeredAt: 0,
		requestId: "spawn",
		lifecycleProtocol: 2,
		assignment: { cwd: "/tmp", model: "test/model" },
	});
	assert.equal(h.activeTools().includes("CreateAgentPanel"), true);
});

test("CreateAgentPanel preserves parameter mapping through the shared spawn facade", async () => {
	const h = await harness({ workerCwd: "/workspace/live" });
	await h.handlers.get("session_start")![0]({ reason: "startup" }, h.ctx);
	await h.commands.get("team").handler("list", h.ctx);
	const result = await h.tools.get("CreateAgentPanel").execute("call", { name: "scout", type: " Research " }, h.ctx.signal, undefined, h.ctx);
	assert.match(result.content[0].text, /Worker agent-scout ready in pane worker-pane/);
	assert.equal(result.details.adopted, true);
	assert.equal(result.details.cwd, "/workspace/live");
	assert.match(result.details.runId, /^[0-9a-f-]{36}$/);
	const registration = h.entries.find((entry) => entry.type === RUN_REGISTRATION_ENTRY)?.data;
	assert.deepEqual(registration && { ...registration, runId: "<run>", registeredAt: 0 }, {
		version: 1,
		runId: "<run>",
		sessionId: "session",
		registeredAt: 0,
		lifecycleProtocol: 2,
		assignment: { cwd: "/tmp", model: "xai/grok-4.6", role: "research" },
	});
});

test("registration and endpoint journals fence every worker creation side effect", async () => {
	const h = await harness({ listening: true });
	await h.handlers.get("session_start")![0]({ reason: "startup" }, h.ctx);
	const probe = await emitForReply<any>(h.events, CHANNELS.probe, "ordering-probe", { requestId: "ordering-probe", supportedProtocols: [1] });
	const providerInstanceId = probe.success ? probe.data.providerInstanceId : "";
	const spawned = await emitForReply<WorkerReference>(h.events, CHANNELS.spawn, "ordering", {
		requestId: "ordering",
		providerInstanceId,
		protocol: 1,
		name: "builder",
		type: " Research ",
		initialPrompt: "Map the system",
	});
	assert.equal(spawned.success, true);

	const registration = h.timeline.indexOf(`append:${RUN_REGISTRATION_ENTRY}`);
	const endpoint = h.timeline.indexOf(`append:${RUN_ENDPOINT_BINDING_ENTRY}`);
	const split = h.timeline.indexOf("exec:pane:split");
	const start = h.timeline.indexOf("exec:agent:start");
	const lifecycle = h.timeline.indexOf(`append:${LIFECYCLE_JOURNAL_ENTRY}`);
	const assignment = h.timeline.indexOf("envelope:message");
	assert.ok(registration >= 0);
	assert.ok(split > registration);
	assert.ok(endpoint > split);
	assert.ok(start > endpoint);
	assert.ok(lifecycle > endpoint);
	assert.ok(assignment > lifecycle);
	for (const [index, item] of h.timeline.entries()) {
		if (item === "append:herdr-worker") assert.ok(index > registration);
	}
	const data = h.entries.find((entry) => entry.type === RUN_REGISTRATION_ENTRY)?.data;
	assert.deepEqual(data?.assignment, { cwd: "/tmp", model: "xai/grok-4.6", role: "research" });
	assert.equal(data?.requestId, "ordering");
});

test("re-adoption sends the run binding before its assignment prompt", async () => {
	const h = await harness({ listening: true });
	await h.handlers.get("session_start")![0]({ reason: "startup" }, h.ctx);
	const probe = await emitForReply<any>(h.events, CHANNELS.probe, "binding-probe", { requestId: "binding-probe", supportedProtocols: [1] });
	const providerInstanceId = probe.success ? probe.data.providerInstanceId : "";
	const spawned = await emitForReply<WorkerReference>(h.events, CHANNELS.spawn, "binding", {
		requestId: "binding", providerInstanceId, protocol: 1, correlationId: "dispatch-1", name: "scout", initialPrompt: "Map the code",
	});
	assert.equal(spawned.success, true);
	if (!spawned.success) return;
	assert.equal(spawned.data.correlationId, "dispatch-1");
	const bindingIndex = h.writtenEnvelopes.findIndex(({ envelope }) => envelope.type === "control" && envelope.action === "bind-run");
	const promptIndex = h.writtenEnvelopes.findIndex(({ envelope }) => envelope.type === "message" && envelope.message === "Map the code");
	assert.ok(bindingIndex >= 0);
	assert.ok(promptIndex > bindingIndex);
	assert.equal(h.writtenEnvelopes[bindingIndex].envelope.binding.runId, spawned.data.runId);
	assert.equal(h.writtenEnvelopes[bindingIndex].envelope.binding.protocol, 2);
	assert.equal(h.writtenEnvelopes[bindingIndex].envelope.binding.correlationId, "dispatch-1");
	assert.equal(h.writtenEnvelopes[promptIndex].envelope.runId, spawned.data.runId);
	const registrationIndex = h.entries.findIndex((entry) => entry.type === RUN_REGISTRATION_ENTRY);
	const endpointIndex = h.entries.findIndex((entry) => entry.type === RUN_ENDPOINT_BINDING_ENTRY);
	const relationshipIndex = h.entries.findIndex((entry) => entry.type === "herdr-worker" && entry.data.workers?.includes("agent-scout"));
	assert.ok(registrationIndex >= 0);
	assert.ok(endpointIndex > registrationIndex);
	assert.ok(relationshipIndex > endpointIndex);
});

test("new workers bind startup flags, report readiness, and gate ReportWorkerRun", async () => {
	const h = await harness({
		listening: true,
		selfName: "agent-child",
		flags: { "orchestrated-by": "boss", "worker-run-id": "run-child", "worker-correlation-id": "dispatch-child", "worker-lifecycle-protocol": "2" },
	});
	assert.equal(h.activeTools().includes("ReportWorkerRun"), false);
	await h.handlers.get("session_start")![0]({ reason: "startup" }, h.ctx);
	assert.equal(h.activeTools().includes("ReportWorkerRun"), true);
	const ready = h.writtenEnvelopes[0];
	assert.equal(ready.paneId, "boss-pane");
	assert.equal(ready.envelope.type, "lifecycle");
	assert.equal(ready.envelope.report.runId, "run-child");
	assert.equal(ready.envelope.report.protocol, 2);
	assert.equal(ready.envelope.report.sourceSequence, 1);
	assert.deepEqual(ready.envelope.report.evidence, { kind: "worker_ready", readiness: "confirmed" });

	const messageResult = await h.tools.get("ReportWorkerRun").execute("report-1", { status: "message", message: "Still working" });
	await assert.rejects(
		h.tools.get("ReportWorkerRun").execute("report-invalid", { status: "completed" }),
		/bound contract/,
	);
	const completedResult = await h.tools.get("ReportWorkerRun").execute("report-2", {
		status: "completed",
		result: "Implemented",
		artifacts: [{ path: "reports/result.md", description: "Final report" }],
		checks: [{ kind: "test", command: "npm test", outcome: "passed" }],
	});
	assert.equal(messageResult.details.runId, "run-child");
	assert.equal(completedResult.details.status, "completed");
	const reports = h.writtenEnvelopes.map(({ envelope }) => envelope.report).filter(Boolean);
	assert.deepEqual(reports.map((report) => report.sourceSequence), [1, 2, 3]);
	assert.deepEqual(reports[1].evidence, { kind: "worker_message", message: "Still working" });
	assert.deepEqual(reports[2].evidence, {
		kind: "worker_completed_v2",
		result: "Implemented",
		artifacts: [{ path: "reports/result.md", description: "Final report" }],
		checks: [{ kind: "test", command: "npm test", outcome: "passed" }],
	});
	assert.equal(h.entries.some((entry) => entry.data.activeRun?.sourceSequence === 3), true);
});

test("bound worker prompt reserves terminal reporting for ReportWorkerRun", async () => {
	const h = await harness({
		listening: true,
		selfName: "agent-child",
		flags: { "orchestrated-by": "boss", "worker-run-id": "run-child", "worker-lifecycle-protocol": "2" },
	});
	await h.handlers.get("session_start")![0]({ reason: "startup" }, h.ctx);
	const prompt = await h.handlers.get("before_agent_start")![0]({ systemPrompt: "base" });
	assert.match(prompt.systemPrompt, /Use SendToAgent for questions and ordinary communication/);
	assert.match(prompt.systemPrompt, /Report terminal outcomes only with ReportWorkerRun/);
	assert.match(prompt.systemPrompt, /non-empty result/);
});

test("re-adopted workers apply a trusted run binding before reporting ready", async () => {
	const h = await harness({
		listening: true,
		selfName: "agent-child",
		flags: { "orchestrated-by": "boss" },
		agents: [{ pane_id: "boss-pane", tab_id: "tab-1", name: "boss", agent: "pi" }],
	});
	await h.handlers.get("session_start")![0]({ reason: "startup" }, h.ctx);
	assert.equal(h.activeTools().includes("ReportWorkerRun"), false);
	await h.deliverInbox({
		type: "control",
		from: { id: "boss", paneId: "boss-pane", name: "boss", role: "orchestrator" },
		action: "bind-run",
		binding: { protocol: 2, runId: "run-adopted", correlationId: "dispatch-adopted" },
		ts: Date.now(),
	}, "bind-run.json");
	await new Promise((resolve) => setImmediate(resolve));
	assert.equal(h.activeTools().includes("ReportWorkerRun"), true);
	assert.equal(h.writtenEnvelopes.length, 1);
	assert.equal(h.writtenEnvelopes[0].envelope.report.status, "started");
	assert.equal(h.writtenEnvelopes[0].envelope.report.runId, "run-adopted");
});

test("parent accepts trusted lifecycle reports only after custom-message persistence", async () => {
	const runId = "run-parent";
	const h = await harness({
		branch: teamBranch({ workers: ["agent-scout"], meta: { "agent-scout": { paneId: "worker-pane", runId, correlationId: "dispatch-parent", lifecycleProtocol: 2 } } }),
		agents: [{ pane_id: "worker-pane", tab_id: "tab-1", name: "agent-scout", agent: "pi" }],
	});
	await h.handlers.get("session_start")![0]({ reason: "startup" }, h.ctx);
	const envelope = {
		type: "lifecycle",
		from: { id: "agent-scout", paneId: "worker-pane", name: "agent-scout", role: "worker" },
		report: workerReport(runId),
		ts: Date.now(),
	};
	await h.deliverInbox(envelope, "report-1.json");
	assert.equal(h.sentMessages.length, 1);
	assert.equal(h.sentMessages[0].message.customType, "herdr-worker.lifecycle-report");
	assert.equal(h.entries.some((entry) => entry.type === LIFECYCLE_JOURNAL_ENTRY), false);
	assert.equal(h.events.emissions.some(({ channel }) => channel === LIFECYCLE_CHANNELS.completed), false);

	h.sessionEntries.push({ type: "custom_message", ...h.sentMessages[0].message });
	h.handlers.get("context")![0]({}, h.ctx);
	const journal = h.entries.filter((entry) => entry.type === LIFECYCLE_JOURNAL_ENTRY);
	assert.equal(journal.length, 1);
	assert.equal(journal[0].data.event.runId, runId);
	assert.equal(journal[0].data.event.correlationId, "dispatch-parent");
	assert.deepEqual(journal[0].data.event.worker, { name: "agent-scout", paneId: "worker-pane" });
	assert.equal(h.events.emissions.some(({ channel }) => channel === LIFECYCLE_CHANNELS.completed), true);

	await h.deliverInbox(envelope, "report-duplicate.json");
	h.sessionEntries.push({ type: "custom_message", ...h.sentMessages[1].message });
	h.handlers.get("agent_settled")![0]({}, h.ctx);
	assert.equal(h.entries.filter((entry) => entry.type === LIFECYCLE_JOURNAL_ENTRY).length, 1);
});

test("parent rejects untrusted and pane-mismatched lifecycle senders", async () => {
	const runId = "run-trusted";
	const h = await harness({
		branch: teamBranch({ workers: ["agent-scout"], meta: { "agent-scout": { paneId: "worker-pane", runId, lifecycleProtocol: 2 } } }),
		agents: [{ pane_id: "worker-pane", tab_id: "tab-1", name: "agent-scout", agent: "pi" }],
	});
	await h.handlers.get("session_start")![0]({ reason: "startup" }, h.ctx);
	for (const [id, paneId] of [["stranger", "outside-pane"], ["agent-scout", "stale-pane"]]) {
		await h.deliverInbox({
			type: "lifecycle",
			from: { id, paneId, role: "worker" },
			report: workerReport(runId),
			ts: Date.now(),
		}, `${id}.json`);
	}
	assert.equal(h.sentMessages.length, 0);
	assert.equal(h.entries.some((entry) => entry.type === LIFECYCLE_JOURNAL_ENTRY), false);
});

test("reload accepts a persisted trusted report that was not yet journaled", async () => {
	const runId = "run-reload-report";
	const report = workerReport(runId, { status: "failed", evidence: { kind: "worker_failed", error: "Blocked" } });
	const sessionEntries = [{
		type: "custom_message",
		customType: "herdr-worker.lifecycle-report",
		details: {
			envelopeId: "persisted-report.json",
			from: { id: "agent-scout", paneId: "worker-pane", name: "agent-scout", role: "worker" },
			report,
		},
	}];
	const h = await harness({
		branch: teamBranch({ workers: ["agent-scout"], meta: { "agent-scout": { paneId: "worker-pane", runId, lifecycleProtocol: 2 } } }),
		sessionEntries,
	});
	await h.handlers.get("session_start")![0]({ reason: "startup" }, h.ctx);
	const lifecycle = h.entries.filter((entry) => entry.type === LIFECYCLE_JOURNAL_ENTRY);
	assert.equal(lifecycle.length, 1);
	assert.equal(lifecycle[0].data.event.status, "failed");
	assert.equal(h.events.emissions.some(({ channel }) => channel === LIFECYCLE_CHANNELS.failed), true);
});

test("RPC re-adoption prefers observed cwd over a different explicit request", async () => {
	const h = await harness({ workerCwd: "/workspace/live" });
	await h.handlers.get("session_start")![0]({ reason: "startup" }, h.ctx);
	const probe = await emitForReply<any>(h.events, CHANNELS.probe, "cwd-probe", { requestId: "cwd-probe", supportedProtocols: [1] });
	const providerInstanceId = probe.success ? probe.data.providerInstanceId : "";
	const spawned = await emitForReply<WorkerReference>(h.events, CHANNELS.spawn, "cwd-observed", {
		requestId: "cwd-observed", providerInstanceId, protocol: 1, name: "scout", cwd: "/",
	});
	assert.equal(spawned.success && spawned.data.cwd, "/workspace/live");
});

test("RPC re-adoption falls back to the validated contextual cwd when observation is absent or empty", async () => {
	for (const [label, workerCwd] of [["missing", null], ["empty", "   "]] as const) {
		const h = await harness({ contextCwd: "/tmp", workerCwd });
		await h.handlers.get("session_start")![0]({ reason: "startup" }, h.ctx);
		const probeId = `cwd-${label}-probe`;
		const probe = await emitForReply<any>(h.events, CHANNELS.probe, probeId, { requestId: probeId, supportedProtocols: [1] });
		const providerInstanceId = probe.success ? probe.data.providerInstanceId : "";
		const spawned = await emitForReply<WorkerReference>(h.events, CHANNELS.spawn, `cwd-${label}`, {
			requestId: `cwd-${label}`, providerInstanceId, protocol: 1, name: "scout",
		});
		assert.equal(spawned.success && spawned.data.cwd, "/tmp", label);
	}
});

test("SendToAgent and RPC send share delivery while preserving tool details and sanitizing receipts", async () => {
	const h = await harness();
	await h.handlers.get("session_start")![0]({ reason: "startup" }, h.ctx);
	await h.commands.get("team").handler("adopt agent-scout", h.ctx);
	const tool = await h.tools.get("SendToAgent").execute("call", { target_id: "agent-scout", message: "tool secret", priority: true }, h.ctx.signal);
	assert.match(tool.content[0].text, /Typed into agent-scout .* Priority flag not applicable there/);
	assert.deepEqual(tool.details, { target: "agent-scout", priority: true, message: "tool secret", status: tool.content[0].text });

	const probe = await emitForReply<any>(h.events, CHANNELS.probe, "send-probe", { requestId: "send-probe", supportedProtocols: [1] });
	const providerInstanceId = probe.success ? probe.data.providerInstanceId : "";
	const reply = await emitForReply<DeliveryReceipt>(h.events, CHANNELS.send, "send", { requestId: "send", providerInstanceId, protocol: 1, target: "agent-scout", message: "rpc secret", mode: "steer" });
	assert.deepEqual(reply.success && reply.data, { target: "agent-scout", paneId: "worker-pane", kind: "pi", status: "idle", transport: "herdr-prompt", requestedMode: "steer", priorityApplied: false });
	assert.doesNotMatch(JSON.stringify(reply), /rpc secret/);
	assert.equal(h.execCalls.filter((args) => args[0] === "agent" && args[1] === "prompt").length, 2);
});

test("RPC inbox receipts report requested mode and actual priority without writing a mailbox", async () => {
	const h = await harness({ listening: true });
	await h.handlers.get("session_start")![0]({ reason: "startup" }, h.ctx);
	await h.commands.get("team").handler("adopt agent-scout", h.ctx);
	const probe = await emitForReply<any>(h.events, CHANNELS.probe, "inbox-probe", { requestId: "inbox-probe", supportedProtocols: [1] });
	const providerInstanceId = probe.success ? probe.data.providerInstanceId : "";
	const reply = await emitForReply<DeliveryReceipt>(h.events, CHANNELS.send, "inbox", { requestId: "inbox", providerInstanceId, protocol: 1, target: "agent-scout", message: "priority body", priority: true });
	assert.deepEqual(reply.success && reply.data, { target: "agent-scout", paneId: "worker-pane", kind: "pi", status: "idle", transport: "inbox", requestedMode: "steer", priorityApplied: true });
	const messages = h.writtenEnvelopes.filter(({ envelope }) => envelope.type === "message");
	assert.equal(messages.length, 1);
	assert.equal(messages[0].envelope.priority, true);
	assert.doesNotMatch(JSON.stringify(reply), /priority body/);
});

test("RPC direction and thinking reach the canonical creation sequence", async () => {
	const h = await harness();
	await h.handlers.get("session_start")![0]({ reason: "startup" }, h.ctx);
	const probe = await emitForReply<any>(h.events, CHANNELS.probe, "options-probe", { requestId: "options-probe", supportedProtocols: [1] });
	const providerInstanceId = probe.success ? probe.data.providerInstanceId : "";
	const spawned = await emitForReply<WorkerReference>(h.events, CHANNELS.spawn, "options", {
		requestId: "options", providerInstanceId, protocol: 1, name: "builder", direction: "left", cwd: "/", model: "test/model", thinking: "high",
	});
	assert.equal(spawned.success && spawned.data.paneId, "new-pane");
	assert.equal(spawned.success && spawned.data.cwd, "/");
	assert.equal(h.execCalls.some((args) => args[0] === "pane" && args[1] === "split" && args.includes("--direction") && args.includes("right")), true);
	assert.equal(h.execCalls.some((args) => args[0] === "pane" && args[1] === "split" && args.includes("--cwd") && args.includes("/")), true);
	assert.equal(h.execCalls.some((args) => args[0] === "pane" && args[1] === "swap"), true);
	const start = h.execCalls.find((args) => args[0] === "agent" && args[1] === "start");
	assert.ok(start);
	assert.equal(start.includes("test/model:high"), true);
	assert.equal(start.includes("--worker-run-id"), true);
	assert.equal(start[start.indexOf("--worker-lifecycle-protocol") + 1], "2");
	assert.equal(start[start.indexOf("--worker-run-id") + 1], spawned.success ? spawned.data.runId : undefined);
});

test("new workers receive RPC correlation with the generated run identity", async () => {
	const h = await harness();
	await h.handlers.get("session_start")![0]({ reason: "startup" }, h.ctx);
	const probe = await emitForReply<any>(h.events, CHANNELS.probe, "correlation-probe", { requestId: "correlation-probe", supportedProtocols: [1] });
	const providerInstanceId = probe.success ? probe.data.providerInstanceId : "";
	const spawned = await emitForReply<WorkerReference>(h.events, CHANNELS.spawn, "correlation", {
		requestId: "correlation", providerInstanceId, protocol: 1, correlationId: "dispatch-new", name: "builder",
	});
	assert.equal(spawned.success, true);
	if (!spawned.success) return;
	const start = h.execCalls.find((args) => args[0] === "agent" && args[1] === "start");
	assert.ok(start);
	assert.equal(start[start.indexOf("--worker-run-id") + 1], spawned.data.runId);
	assert.equal(start[start.indexOf("--worker-correlation-id") + 1], "dispatch-new");
	const team = [...h.entries].reverse().find((entry) => entry.data.meta?.["agent-builder"]);
	assert.equal(team?.data.meta["agent-builder"].runId, spawned.data.runId);
	assert.equal(team?.data.meta["agent-builder"].requestId, "correlation");
	assert.equal(team?.data.meta["agent-builder"].providerInstanceId, providerInstanceId);
});

test("new worker start is journaled before canonical and projected publication", async () => {
	const h = await harness();
	const delivered: AcceptedLifecycleEvent[] = [];
	h.events.on(LIFECYCLE_CHANNELS.lifecycle, () => { throw new Error("subscriber failed"); });
	h.events.on(LIFECYCLE_CHANNELS.lifecycle, (payload) => delivered.push(payload as AcceptedLifecycleEvent));
	await h.handlers.get("session_start")![0]({ reason: "startup" }, h.ctx);
	const probe = await emitForReply<any>(h.events, CHANNELS.probe, "lifecycle-probe", { requestId: "lifecycle-probe", supportedProtocols: [1] });
	const providerInstanceId = probe.success ? probe.data.providerInstanceId : "";
	const spawned = await emitForReply<WorkerReference>(h.events, CHANNELS.spawn, "lifecycle-spawn", {
		requestId: "lifecycle-spawn", providerInstanceId, protocol: 1, correlationId: "dispatch-lifecycle", name: "builder",
	});
	assert.equal(spawned.success, true);
	if (!spawned.success) return;

	const lifecycleEntries = h.entries.filter((entry) => entry.type === LIFECYCLE_JOURNAL_ENTRY);
	assert.equal(lifecycleEntries.length, 1);
	const event = lifecycleEntries[0].data.event as AcceptedLifecycleEvent;
	assert.equal(event.runId, spawned.data.runId);
	assert.equal(event.correlationId, "dispatch-lifecycle");
	assert.deepEqual(event.worker, { name: "agent-builder", paneId: "new-pane" });
	assert.equal(event.sourceInstanceId, providerInstanceId);
	assert.equal(event.acceptedSequence, 1);
	assert.deepEqual(event.evidence, { kind: "agent_start_returned", readiness: "unconfirmed" });
	assert.equal(delivered.length, 1);
	assert.equal(delivered[0], event);

	const canonicalEmission = h.events.emissions.find((item) => item.channel === LIFECYCLE_CHANNELS.lifecycle);
	const projectedEmission = h.events.emissions.find((item) => item.channel === LIFECYCLE_CHANNELS.started);
	assert.equal(canonicalEmission?.payload, event);
	assert.equal(projectedEmission?.payload, event);
	const lifecycleAppend = h.timeline.indexOf(`append:${LIFECYCLE_JOURNAL_ENTRY}`);
	const canonical = h.timeline.indexOf(`emit:${LIFECYCLE_CHANNELS.lifecycle}`);
	const projected = h.timeline.indexOf(`emit:${LIFECYCLE_CHANNELS.started}`);
	assert.ok(lifecycleAppend > h.timeline.findIndex((item) => item === "append:herdr-worker"));
	assert.ok(canonical > lifecycleAppend);
	assert.ok(projected > canonical);
});

test("spawn failures preserve the external side-effect evidence boundary", async () => {
	const cases = [
		{
			label: "split command",
			execOverride: (args: string[]) => args[0] === "pane" && args[1] === "split"
				? { code: 1, stdout: "", stderr: "split failed" }
				: undefined,
			expected: [{ status: "uncertain", paneId: undefined, scope: "pane_creation" }],
		},
		{
			label: "missing split identity",
			execOverride: (args: string[]) => args[0] === "pane" && args[1] === "split"
				? { code: 0, stdout: JSON.stringify({ result: {} }), stderr: "" }
				: undefined,
			expected: [{ status: "uncertain", paneId: undefined, scope: "pane_creation" }],
		},
		{
			label: "agent start",
			execOverride: (args: string[]) => args[0] === "agent" && args[1] === "start"
				? { code: 1, stdout: "", stderr: "start failed" }
				: undefined,
			expected: [{ status: "uncertain", paneId: "new-pane", scope: "agent_start" }],
		},
	] as const;

	for (const item of cases) {
		const h = await harness({ execOverride: item.execOverride });
		await h.handlers.get("session_start")![0]({ reason: "startup" }, h.ctx);
		const requestId = item.label.replaceAll(" ", "-");
		const probe = await emitForReply<any>(h.events, CHANNELS.probe, `${requestId}-probe`, { requestId: `${requestId}-probe`, supportedProtocols: [1] });
		const providerInstanceId = probe.success ? probe.data.providerInstanceId : "";
		const reply = await emitForReply<WorkerReference>(h.events, CHANNELS.spawn, requestId, {
			requestId, providerInstanceId, protocol: 1, name: "builder",
		});
		assert.equal(reply.success ? "success" : reply.error.code, "INTERNAL_ERROR", item.label);
		const lifecycle = h.entries.filter((entry) => entry.type === LIFECYCLE_JOURNAL_ENTRY).map((entry) => entry.data.event);
		assert.deepEqual(lifecycle.map((event) => ({
			status: event.status,
			paneId: event.worker.paneId,
			scope: event.evidence.scope,
		})), item.expected, item.label);
		assert.equal(h.events.emissions.some(({ channel }) => channel === LIFECYCLE_CHANNELS.failed), false, item.label);
	}
});

test("post-start assignment failure retains started evidence and records uncertainty", async () => {
	const h = await harness({ listening: true, writeEnvelopeErrorForType: "message" });
	await h.handlers.get("session_start")![0]({ reason: "startup" }, h.ctx);
	const probe = await emitForReply<any>(h.events, CHANNELS.probe, "prompt-probe", { requestId: "prompt-probe", supportedProtocols: [1] });
	const providerInstanceId = probe.success ? probe.data.providerInstanceId : "";
	const reply = await emitForReply<WorkerReference>(h.events, CHANNELS.spawn, "prompt-failure", {
		requestId: "prompt-failure", providerInstanceId, protocol: 1, name: "builder", initialPrompt: "Do the work",
	});
	assert.equal(reply.success ? "success" : reply.error.code, "INTERNAL_ERROR");
	const lifecycle = h.entries.filter((entry) => entry.type === LIFECYCLE_JOURNAL_ENTRY).map((entry) => entry.data.event);
	assert.deepEqual(lifecycle.map((event) => [event.status, event.evidence.kind, event.evidence.scope]), [
		["started", "agent_start_returned", undefined],
		["uncertain", "uncertain", "assignment_delivery"],
	]);
	assert.deepEqual(lifecycle.map((event) => event.acceptedSequence), [1, 2]);
});

test("reconciliation resolves the original uncertain run without worker side effects and survives reload", async () => {
	const h = await harness({ listening: true, writeEnvelopeErrorForType: "message" });
	await h.handlers.get("session_start")![0]({ reason: "startup" }, h.ctx);
	const workerProbe = await emitForReply<any>(h.events, CHANNELS.probe, "reconcile-worker-probe", { requestId: "reconcile-worker-probe", supportedProtocols: [1] });
	const workerProviderId = workerProbe.success ? workerProbe.data.providerInstanceId : "";
	const spawned = await emitForReply<WorkerReference>(h.events, CHANNELS.spawn, "reconcile-spawn", {
		requestId: "reconcile-spawn", providerInstanceId: workerProviderId, protocol: 1, name: "builder", initialPrompt: "Do the work",
	});
	assert.equal(spawned.success, false);
	const registration = h.entries.find((entry) => entry.type === RUN_REGISTRATION_ENTRY)?.data;
	const runId = registration.runId as string;
	const endpointEntriesBefore = h.entries.filter((entry) => entry.type === RUN_ENDPOINT_BINDING_ENTRY).length;
	const execCallsBefore = h.execCalls.length;

	const probe = await emitReconciliationForReply<any>(h.events, RECONCILIATION_CHANNELS.probe, "reconciliation-probe", {
		requestId: "reconciliation-probe", supportedProtocols: [1],
	});
	assert.equal(probe.success && probe.data.available, true);
	const providerInstanceId = probe.success ? probe.data.providerInstanceId : "";
	const completed = await emitReconciliationForReply<AcceptedLifecycleEvent>(h.events, RECONCILIATION_CHANNELS.reconcile, "reconciliation-complete", {
		requestId: "reconciliation-complete",
		providerInstanceId,
		protocol: 1,
		runId,
		expectedAcceptedSequence: 2,
		resolution: {
			status: "completed",
			result: "Recovered the completed implementation.",
			detail: "Git and tests establish completion on the original run.",
			observations: [{ source: "git", detail: "Expected changes are present in the worktree.", observedAt: 1_786_000_000_100 }],
			artifacts: [{ path: "reports/result.md" }],
			checks: [{ kind: "test", command: "npm test", outcome: "passed" }],
		},
	});
	assert.equal(completed.success, true);
	if (!completed.success) return;
	assert.equal(completed.data.runId, runId);
	assert.equal(completed.data.acceptedSequence, 3);
	assert.equal(completed.data.evidence.kind, "reconciled_completed_v2");
	assert.equal(h.execCalls.length, execCallsBefore);
	assert.equal(h.entries.filter((entry) => entry.type === RUN_ENDPOINT_BINDING_ENTRY).length, endpointEntriesBefore);
	assert.equal(h.entries.filter((entry) => entry.type === RUN_REGISTRATION_ENTRY).length, 1);

	const stale = await emitReconciliationForReply(h.events, RECONCILIATION_CHANNELS.reconcile, "reconciliation-stale", {
		requestId: "reconciliation-stale",
		providerInstanceId,
		protocol: 1,
		runId,
		expectedAcceptedSequence: 2,
		resolution: { status: "started", detail: "Still active", observations: [{ source: "journal", detail: "Old snapshot", observedAt: 1 }] },
	});
	assert.equal(stale.success ? "success" : stale.error.code, "STALE_ACCEPTED_SEQUENCE");

	await h.handlers.get("session_shutdown")![0]();
	h.herdrWorker(h.pi, { disableInbox: true });
	await h.handlers.get("session_start")![1]({ reason: "startup" }, h.ctx);
	const queryProbe = await emitRunQueryForReply<any>(h.events, RUN_QUERY_CHANNELS.probe, "reconciled-query-probe", { requestId: "reconciled-query-probe", supportedProtocols: [2, 1] });
	const queryProviderId = queryProbe.success ? queryProbe.data.providerInstanceId : "";
	const record = await emitRunQueryForReply<any>(h.events, RUN_QUERY_CHANNELS.get, "reconciled-query-get", {
		requestId: "reconciled-query-get", providerInstanceId: queryProviderId, protocol: 2, runId,
	});
	assert.equal(record.success && record.data.lifecycle.status, "completed");
	assert.equal(record.success && record.data.lifecycle.acceptedSequence, 3);
	assert.equal(record.success && record.data.lifecycle.orchestrationGradeCompletion, true);
	const replay = await emitRunQueryForReply<any>(h.events, RUN_QUERY_CHANNELS.replay, "reconciled-query-replay", {
		requestId: "reconciled-query-replay", providerInstanceId: queryProviderId, protocol: 2, runId, afterAcceptedSequence: 2,
	});
	assert.deepEqual(replay.success && replay.data.events, [completed.data]);
});

test("shutdown during creation records uncertainty without claiming a stop", async () => {
	let releaseStart!: () => void;
	let markStartEntered!: () => void;
	const startEntered = new Promise<void>((resolve) => { markStartEntered = resolve; });
	const blockedStart = new Promise<void>((resolve) => { releaseStart = resolve; });
	const h = await harness({
		execOverride: async (args) => {
			if (args[0] !== "agent" || args[1] !== "start") return undefined;
			markStartEntered();
			await blockedStart;
			return { code: 0, stdout: JSON.stringify({ result: {} }), stderr: "" };
		},
	});
	await h.handlers.get("session_start")![0]({ reason: "startup" }, h.ctx);
	await h.commands.get("team").handler("list", h.ctx);
	const creation = h.tools.get("CreateAgentPanel").execute("shutdown-create", { name: "builder" }, h.ctx.signal, undefined, h.ctx);
	await startEntered;
	await h.handlers.get("session_shutdown")![0]();
	releaseStart();
	await assert.rejects(creation);
	const lifecycle = h.entries.filter((entry) => entry.type === LIFECYCLE_JOURNAL_ENTRY).map((entry) => entry.data.event);
	assert.deepEqual(lifecycle.map((event) => [event.status, event.evidence.scope]), [["uncertain", "agent_start"]]);
	assert.equal(h.events.emissions.some(({ channel }) => channel === LIFECYCLE_CHANNELS.stopped), false);
});

test("release and ordinary send receipts do not synthesize terminal lifecycle events", async () => {
	const h = await harness({ branch: teamBranch({ workers: ["agent-scout"], meta: { "agent-scout": { paneId: "worker-pane", runId: "run-release" } } }) });
	await h.handlers.get("session_start")![0]({ reason: "startup" }, h.ctx);
	await h.tools.get("SendToAgent").execute("send", { target_id: "agent-scout", message: "FYI", priority: false }, h.ctx.signal);
	await h.commands.get("team").handler("release agent-scout", h.ctx);
	await h.handlers.get("session_shutdown")![0]();
	for (const channel of [LIFECYCLE_CHANNELS.completed, LIFECYCLE_CHANNELS.failed, LIFECYCLE_CHANNELS.stopped]) {
		assert.equal(h.events.emissions.some((item) => item.channel === channel), false, channel);
	}
});

test("re-adoption binds a run without claiming a provider-observed start", async () => {
	const h = await harness({ listening: true });
	await h.handlers.get("session_start")![0]({ reason: "startup" }, h.ctx);
	const probe = await emitForReply<any>(h.events, CHANNELS.probe, "adopt-lifecycle-probe", { requestId: "adopt-lifecycle-probe", supportedProtocols: [1] });
	const providerInstanceId = probe.success ? probe.data.providerInstanceId : "";
	const spawned = await emitForReply<WorkerReference>(h.events, CHANNELS.spawn, "adopt-lifecycle", {
		requestId: "adopt-lifecycle", providerInstanceId, protocol: 1, name: "scout",
	});
	assert.equal(spawned.success && spawned.data.adopted, true);
	assert.equal(h.entries.some((entry) => entry.type === LIFECYCLE_JOURNAL_ENTRY), false);
	assert.equal(h.events.emissions.some((item) => item.channel === LIFECYCLE_CHANNELS.started), false);
});

test("extension reload restores lifecycle history without replaying publication", async () => {
	const h = await harness();
	await h.handlers.get("session_start")![0]({ reason: "startup" }, h.ctx);
	const probe = await emitForReply<any>(h.events, CHANNELS.probe, "restore-probe", { requestId: "restore-probe", supportedProtocols: [1] });
	const providerInstanceId = probe.success ? probe.data.providerInstanceId : "";
	const spawned = await emitForReply<WorkerReference>(h.events, CHANNELS.spawn, "restore-spawn", {
		requestId: "restore-spawn", providerInstanceId, protocol: 1, name: "builder",
	});
	assert.equal(spawned.success, true);
	const before = h.events.emissions.filter((item) => item.channel === LIFECYCLE_CHANNELS.lifecycle).length;
	assert.equal(before, 1);
	await h.handlers.get("session_shutdown")![0]();
	h.herdrWorker(h.pi, { disableInbox: true });
	await h.handlers.get("session_start")![1]({ reason: "startup" }, h.ctx);
	assert.equal(h.events.emissions.filter((item) => item.channel === LIFECYCLE_CHANNELS.lifecycle).length, before);
	assert.equal(h.sessionEntries.filter((entry) => entry.customType === LIFECYCLE_JOURNAL_ENTRY).length, 1);
});

test("run query returns durable strict handles across provider reloads", async () => {
	const h = await harness();
	await h.handlers.get("session_start")![0]({ reason: "startup" }, h.ctx);
	const workerProbe = await emitForReply<any>(h.events, CHANNELS.probe, "worker-probe", { requestId: "worker-probe", supportedProtocols: [1] });
	const workerProviderId = workerProbe.success ? workerProbe.data.providerInstanceId : "";
	const spawned = await emitForReply<WorkerReference>(h.events, CHANNELS.spawn, "query-spawn", {
		requestId: "query-spawn", providerInstanceId: workerProviderId, protocol: 1, correlationId: "dispatch-query", name: "builder",
	});
	assert.equal(spawned.success, true);
	if (!spawned.success) return;

	const firstProbe = await emitRunQueryForReply<any>(h.events, RUN_QUERY_CHANNELS.probe, "query-probe-1", { requestId: "query-probe-1", supportedProtocols: [1] });
	assert.equal(firstProbe.success && firstProbe.data.available, true);
	const firstProviderId = firstProbe.success ? firstProbe.data.providerInstanceId : "";
	const firstGet = await emitRunQueryForReply<WorkerRunRecordV1>(h.events, RUN_QUERY_CHANNELS.get, "query-get-1", {
		requestId: "query-get-1", providerInstanceId: firstProviderId, protocol: 1, runId: spawned.data.runId,
	});
	assert.equal(firstGet.success, true);
	assert.deepEqual(firstGet.success && firstGet.data.lifecycle, { status: "started", acceptedSequence: 1, readiness: "unconfirmed" });
	assert.deepEqual(firstGet.success && !("legacy" in firstGet.data) && firstGet.data.assignment, { cwd: "/tmp", model: "test/model" });
	assert.deepEqual(firstGet.success && !("legacy" in firstGet.data) && firstGet.data.endpoint && { ...firstGet.data.endpoint, observedAt: 0 }, {
		agentName: "agent-builder", paneId: "new-pane", observedAt: 0,
	});

	await h.handlers.get("session_shutdown")![0]();
	h.herdrWorker(h.pi, { disableInbox: true });
	await h.handlers.get("session_start")![1]({ reason: "startup" }, h.ctx);
	const secondProbe = await emitRunQueryForReply<any>(h.events, RUN_QUERY_CHANNELS.probe, "query-probe-2", { requestId: "query-probe-2", supportedProtocols: [1] });
	assert.equal(secondProbe.success, true);
	if (!secondProbe.success) return;
	assert.notEqual(secondProbe.data.providerInstanceId, firstProviderId);
	const secondGet = await emitRunQueryForReply<WorkerRunRecordV1>(h.events, RUN_QUERY_CHANNELS.get, "query-get-2", {
		requestId: "query-get-2", providerInstanceId: secondProbe.data.providerInstanceId, protocol: 1, runId: spawned.data.runId,
	});
	assert.deepEqual(secondGet.success && secondGet.data, firstGet.success && firstGet.data);
	assert.equal(h.events.listenerCount(), 11);
});

test("run query lists lifecycle-only history without fabricating strict facts", async () => {
	const event = {
		protocol: 1,
		eventId: "legacy-event-1",
		runId: "legacy-run-1",
		sourceInstanceId: "worker-source-1",
		sourceSequence: 1,
		status: "completed",
		worker: { name: "agent-legacy", paneId: "legacy-pane" },
		observedAt: 1_786_000_000_000,
		source: "worker",
		evidence: { kind: "worker_completed", result: "Done" },
		acceptedSequence: 1,
	};
	const h = await harness({ sessionEntries: [{
		type: "custom",
		customType: LIFECYCLE_JOURNAL_ENTRY,
		data: { version: 1, sessionId: "session", event },
	}] });
	await h.handlers.get("session_start")![0]({ reason: "startup" }, h.ctx);
	const probe = await emitRunQueryForReply<any>(h.events, RUN_QUERY_CHANNELS.probe, "legacy-probe", { requestId: "legacy-probe", supportedProtocols: [1] });
	const providerInstanceId = probe.success ? probe.data.providerInstanceId : "";
	const listed = await emitRunQueryForReply<any>(h.events, RUN_QUERY_CHANNELS.list, "legacy-list", {
		requestId: "legacy-list", providerInstanceId, protocol: 1,
	});
	assert.deepEqual(listed.success && listed.data.runs, [{
		protocol: 1,
		legacy: true,
		runId: "legacy-run-1",
		sessionId: "session",
		lifecycle: { status: "completed", acceptedSequence: 1 },
		worker: { agentName: "agent-legacy", paneId: "legacy-pane" },
	}]);
	assert.doesNotMatch(JSON.stringify(listed), /assignment|registeredAt|requestId":"spawn|endpoint/);
});

test("run query replays accepted evidence without republishing it", async () => {
	const h = await harness();
	await h.handlers.get("session_start")![0]({ reason: "startup" }, h.ctx);
	const workerProbe = await emitForReply<any>(h.events, CHANNELS.probe, "replay-worker-probe", { requestId: "replay-worker-probe", supportedProtocols: [1] });
	const workerProviderId = workerProbe.success ? workerProbe.data.providerInstanceId : "";
	const spawned = await emitForReply<WorkerReference>(h.events, CHANNELS.spawn, "replay-spawn", {
		requestId: "replay-spawn", providerInstanceId: workerProviderId, protocol: 1, name: "builder",
	});
	assert.equal(spawned.success, true);
	if (!spawned.success) return;
	const beforeReplay = h.events.emissions.filter((item) => item.channel === LIFECYCLE_CHANNELS.lifecycle).length;
	const queryProbe = await emitRunQueryForReply<any>(h.events, RUN_QUERY_CHANNELS.probe, "replay-probe", { requestId: "replay-probe", supportedProtocols: [1] });
	const queryProviderId = queryProbe.success ? queryProbe.data.providerInstanceId : "";
	const replayed = await emitRunQueryForReply<any>(h.events, RUN_QUERY_CHANNELS.replay, "replay-page", {
		requestId: "replay-page", providerInstanceId: queryProviderId, protocol: 1, runId: spawned.data.runId, afterAcceptedSequence: 0, limit: 1,
	});
	assert.deepEqual(replayed.success && replayed.data.events.map((event: AcceptedLifecycleEvent) => [event.runId, event.acceptedSequence, event.eventId]), [
		[spawned.data.runId, 1, h.entries.find((entry) => entry.type === LIFECYCLE_JOURNAL_ENTRY)?.data.event.eventId],
	]);
	assert.equal(replayed.success && replayed.data.hasMore, false);
	assert.equal(h.events.emissions.filter((item) => item.channel === LIFECYCLE_CHANNELS.lifecycle).length, beforeReplay);
	const empty = await emitRunQueryForReply<any>(h.events, RUN_QUERY_CHANNELS.replay, "replay-empty", {
		requestId: "replay-empty", providerInstanceId: queryProviderId, protocol: 1, runId: spawned.data.runId, afterAcceptedSequence: 1,
	});
	assert.deepEqual(empty.success && empty.data, { events: [], hasMore: false });
	const missing = await emitRunQueryForReply<any>(h.events, RUN_QUERY_CHANNELS.replay, "replay-missing", {
		requestId: "replay-missing", providerInstanceId: queryProviderId, protocol: 1, runId: "run-missing", afterAcceptedSequence: 0,
	});
	assert.equal(missing.success ? "success" : missing.error.code, "NOT_FOUND");
});

test("live endpoint observation enriches exact matches and fails closed without durable writes", async () => {
	const registration = {
		type: "custom", customType: RUN_REGISTRATION_ENTRY,
		data: { version: 1, runId: "run-observe", sessionId: "session", registeredAt: 10, assignment: { cwd: "/tmp" } },
	};
	const endpoint = {
		type: "custom", customType: RUN_ENDPOINT_BINDING_ENTRY,
		data: { version: 1, runId: "run-observe", sessionId: "session", agentName: "agent-bound", paneId: "pane-bound", observedAt: 20 },
	};
	const cases = [
		{ label: "matching", live: { pane_id: "pane-bound", name: "agent-bound", agent_status: "busy" }, enriched: true },
		{ label: "missing", live: undefined, enriched: false },
		{ label: "renamed", live: { pane_id: "pane-bound", name: "agent-renamed", agent_status: "busy" }, enriched: false },
		{ label: "moved", live: { pane_id: "pane-moved", name: "agent-bound", agent_status: "busy" }, enriched: false },
		{ label: "reused", live: { pane_id: "pane-bound", name: "agent-reused", agent_status: "busy" }, enriched: false },
	] as const;
	for (const item of cases) {
		const sessionEntries = [structuredClone(registration), structuredClone(endpoint)];
		const h = await harness({
			sessionEntries,
			execOverride: (args) => {
				if (args[0] !== "agent" || args[1] !== "get" || args[2] !== "agent-bound") return undefined;
				return item.live
					? { code: 0, stdout: JSON.stringify({ result: { agent: item.live } }), stderr: "" }
					: { code: 1, stdout: "", stderr: "missing" };
			},
		});
		await h.handlers.get("session_start")![0]({ reason: "startup" }, h.ctx);
		const probe = await emitRunQueryForReply<any>(h.events, RUN_QUERY_CHANNELS.probe, `observe-probe-${item.label}`, { requestId: `observe-probe-${item.label}`, supportedProtocols: [1] });
		const providerInstanceId = probe.success ? probe.data.providerInstanceId : "";
		const result = await emitRunQueryForReply<WorkerRunRecordV1>(h.events, RUN_QUERY_CHANNELS.get, `observe-get-${item.label}`, {
			requestId: `observe-get-${item.label}`, providerInstanceId, protocol: 1, runId: "run-observe", includeEndpointObservation: true,
		});
		assert.equal(result.success, true, item.label);
		if (!result.success || "legacy" in result.data) continue;
		assert.equal(result.data.endpoint?.agentName, "agent-bound", item.label);
		assert.equal(result.data.endpoint?.paneId, "pane-bound", item.label);
		assert.equal(result.data.endpoint?.herdrStatus, item.enriched ? "busy" : undefined, item.label);
		assert.equal(item.enriched ? (result.data.endpoint?.observedAt ?? 0) >= 20 : result.data.endpoint?.observedAt, item.enriched ? true : 20, item.label);
		assert.equal(h.entries.length, 0, item.label);
		assert.deepEqual(h.sessionEntries, [registration, endpoint], item.label);
	}
});

test("RPC inspect projects worker and orchestrator facts from authorized relationships", async () => {
	const h = await harness({ branch: teamBranch({
		workers: ["agent-scout"],
		orchestratedBy: "boss",
		meta: { "agent-scout": { type: "explore", purpose: "Map internals", model: "test/scout", paneId: "worker-pane" } },
	}) });
	await h.handlers.get("session_start")![0]({ reason: "startup" }, h.ctx);
	const probe = await emitForReply<any>(h.events, CHANNELS.probe, "inspect-probe", { requestId: "inspect-probe", supportedProtocols: [1] });
	const providerInstanceId = probe.success ? probe.data.providerInstanceId : "";
	const worker = await emitForReply<Inspection>(h.events, CHANNELS.inspect, "worker", { requestId: "worker", providerInstanceId, protocol: 1, target: "worker-pane" });
	assert.deepEqual(worker.success && worker.data, {
		name: "agent-scout", paneId: "worker-pane", kind: "pi", status: "idle", cwd: "/tmp",
		type: "explore", purpose: "Map internals", model: "test/scout", relationship: "worker", managedBySession: true,
	});
	const workerByName = await emitForReply<Inspection>(h.events, CHANNELS.inspect, "worker-name", { requestId: "worker-name", providerInstanceId, protocol: 1, target: "agent-scout" });
	assert.deepEqual(workerByName.success && workerByName.data, worker.success && worker.data);
	const orchestrator = await emitForReply<Inspection>(h.events, CHANNELS.inspect, "boss", { requestId: "boss", providerInstanceId, protocol: 1, target: "boss-pane" });
	assert.deepEqual(orchestrator.success && orchestrator.data, {
		name: "boss", paneId: "boss-pane", kind: "pi", status: "busy", cwd: "/workspace", relationship: "orchestrator", managedBySession: false,
	});
	assert.deepEqual(h.agentGetTargets, ["self-pane", "agent-scout", "agent-scout", "agent-scout", "boss"]);
	assert.equal(h.agentGetTargets.includes("worker-pane"), false);
	assert.equal(h.agentGetTargets.includes("boss-pane"), false);
	assert.doesNotMatch(JSON.stringify([worker, workerByName, orchestrator]), /tab-1|process|prompt/);
});

test("RPC inspect never probes requested unauthorized selectors and classifies stale peers", async () => {
	const h = await harness({ branch: teamBranch({ workers: ["agent-scout"] }), missingAgents: ["agent-scout"] });
	await h.handlers.get("session_start")![0]({ reason: "startup" }, h.ctx);
	const probe = await emitForReply<any>(h.events, CHANNELS.probe, "auth-probe", { requestId: "auth-probe", supportedProtocols: [1] });
	const providerInstanceId = probe.success ? probe.data.providerInstanceId : "";
	for (const target of ["self-pane", "outside-agent"]) {
		const reply = await emitForReply<Inspection>(h.events, CHANNELS.inspect, `inspect-${target}`, { requestId: `inspect-${target}`, providerInstanceId, protocol: 1, target });
		assert.equal(reply.success ? "success" : reply.error.code, "NOT_TEAM_MEMBER");
	}
	assert.deepEqual(h.agentGetTargets, ["self-pane", "agent-scout"]);
	assert.equal(h.agentGetTargets.includes("outside-agent"), false);
	const stale = await emitForReply<Inspection>(h.events, CHANNELS.inspect, "stale", { requestId: "stale", providerInstanceId, protocol: 1, target: "agent-scout" });
	assert.equal(stale.success ? "success" : stale.error.code, "NOT_FOUND");
	assert.deepEqual(h.agentGetTargets, ["self-pane", "agent-scout", "agent-scout"]);
});

test("session tree replaces inspect authority without replacing the provider instance", async () => {
	const branch = teamBranch({ workers: ["agent-scout"] });
	const h = await harness({ branch });
	await h.handlers.get("session_start")![0]({ reason: "startup" }, h.ctx);
	const first = await emitForReply<any>(h.events, CHANNELS.probe, "first-generation", { requestId: "first-generation", supportedProtocols: [1] });
	h.branch[0].data.workers = [];
	await h.handlers.get("session_tree")![0]({}, h.ctx);
	const second = await emitForReply<any>(h.events, CHANNELS.probe, "same-generation", { requestId: "same-generation", supportedProtocols: [1] });
	assert.equal(first.success && second.success && first.data.providerInstanceId, second.success && second.data.providerInstanceId);
	const providerInstanceId = second.success ? second.data.providerInstanceId : "";
	const denied = await emitForReply<Inspection>(h.events, CHANNELS.inspect, "after-tree", { requestId: "after-tree", providerInstanceId, protocol: 1, target: "agent-scout" });
	assert.equal(denied.success ? "success" : denied.error.code, "NOT_TEAM_MEMBER");
	assert.deepEqual(h.agentGetTargets, ["self-pane"]);
});

test("reload replaces the provider instance and stale addressed requests are no-ops", async () => {
	const h = await harness();
	await h.handlers.get("session_start")![0]({ reason: "startup" }, h.ctx);
	const oldProbe = await emitForReply<any>(h.events, CHANNELS.probe, "old-provider", { requestId: "old-provider", supportedProtocols: [1] });
	const oldInstanceId = oldProbe.success ? oldProbe.data.providerInstanceId : "";
	await h.handlers.get("session_shutdown")![0]();
	h.herdrWorker(h.pi, { disableInbox: true });
	await h.handlers.get("session_start")![1]({ reason: "startup" }, h.ctx);
	const newProbe = await emitForReply<any>(h.events, CHANNELS.probe, "new-provider", { requestId: "new-provider", supportedProtocols: [1] });
	const newInstanceId = newProbe.success ? newProbe.data.providerInstanceId : "";
	assert.notEqual(newInstanceId, oldInstanceId);
	let replied = false;
	const unsubscribe = h.events.on(replyChannel(CHANNELS.inspect, "stale-provider"), () => { replied = true; });
	h.events.emit(CHANNELS.inspect, { requestId: "stale-provider", providerInstanceId: oldInstanceId, protocol: 1, target: "agent-scout" });
	await new Promise((resolve) => setImmediate(resolve));
	unsubscribe();
	assert.equal(replied, false);
	assert.equal(h.events.listenerCount(), 11);
});

type EntryAdapter = "tool" | "rpc";

async function rpcAdapter(h: AdapterHarness) {
	let sequence = 0;
	const client = new WorkerRpcClient({ events: h.events, createRequestId: () => `paired-${++sequence}` });
	const provider = await client.probe();
	assert.equal(provider.available, true);
	return { client, provider };
}

function spawnVia(h: AdapterHarness, rpc: Awaited<ReturnType<typeof rpcAdapter>>, adapter: EntryAdapter, input: SpawnInput, signal?: AbortSignal): Promise<any> {
	if (adapter === "rpc") return rpc.client.spawn(input, rpc.provider, { signal });
	const { cwd, correlationId, initialPrompt, ...params } = input;
	assert.equal(cwd, h.ctx.cwd, "paired tool uses the same contextual CWD as RPC");
	assert.equal(correlationId, undefined, "tool does not expose correlation");
	return h.executeTool("CreateAgentPanel", { ...params, ...(initialPrompt === undefined ? {} : { initial_prompt: initialPrompt }) }, signal);
}

function externalCalls(h: AdapterHarness) {
	return h.execCalls.filter((args) => (args[0] === "pane" && ["split", "swap"].includes(args[1]))
		|| (args[0] === "agent" && ["rename", "start", "prompt"].includes(args[1])));
}

// Compare journals and effects only after checking the adapter-specific provenance.
function sharedEffects(h: AdapterHarness, provider: ProbeData, adapter: EntryAdapter, correlationId?: string) {
	const registrations = h.entries.filter((entry) => entry.type === RUN_REGISTRATION_ENTRY);
	assert.ok(registrations.length <= 1);
	const runId = registrations[0]?.data.runId;
	const request = h.events.emissions.find(({ channel }) => channel === CHANNELS.spawn)?.payload as any;
	if (request) {
		assert.equal(adapter, "rpc");
		assert.equal(request.providerInstanceId, provider.providerInstanceId);
		assert.equal(request.correlationId, correlationId);
	}
	if (runId) {
		assert.match(runId, /^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/);
		assert.equal(registrations[0].data.requestId, request?.requestId);
		assert.equal(Object.hasOwn(registrations[0].data, "requestId"), adapter === "rpc");
		assert.equal(registrations[0].data.correlationId, correlationId);
		for (const { data } of h.entries.filter((entry) => entry.type === "herdr-worker")) {
			for (const meta of Object.values(data.meta ?? {}) as any[]) {
				if (meta.runId !== runId) continue;
				assert.equal(meta.requestId, request?.requestId);
				assert.equal(meta.providerInstanceId, adapter === "rpc" ? provider.providerInstanceId : undefined);
				assert.equal(meta.correlationId, correlationId);
			}
		}
	}
	const eventIds = new Map<string, string>();
	function normalize(value: any, key?: string): any {
		if (["registeredAt", "observedAt", "at", "ts"].includes(key ?? "")) {
			assert.equal(typeof value, "number");
			assert.ok(Number.isFinite(value));
			return 0;
		}
		if (key === "runId") { assert.equal(value, runId); return "<run>"; }
		if (key === "sourceInstanceId") { assert.equal(value, provider.providerInstanceId); return "<provider>"; }
		if (key === "eventId") {
			assert.match(value, /^[0-9a-f-]{36}$/);
			if (!eventIds.has(value)) eventIds.set(value, `<event-${eventIds.size + 1}>`);
			return eventIds.get(value);
		}
		if (Array.isArray(value)) return value.map((item) => normalize(item));
		if (value && typeof value === "object") {
			return Object.fromEntries(Object.entries(value).flatMap(([field, item]) => {
				if (["requestId", "providerInstanceId", "correlationId"].includes(field)) {
					assert.equal(item, field === "requestId" ? request?.requestId : field === "providerInstanceId" ? request?.providerInstanceId : correlationId);
					return [];
				}
				return [[field, normalize(item, field)]];
			}));
		}
		return value;
	}
	const calls = externalCalls(h).map((args) => {
		const normalized = [...args];
		if (args[1] === "start") {
			assert.equal(args[args.indexOf("--worker-run-id") + 1], runId);
			normalized[args.indexOf("--worker-run-id") + 1] = "<run>";
			const index = args.indexOf("--worker-correlation-id");
			assert.equal(index !== -1, correlationId !== undefined);
			if (index !== -1) {
				assert.equal(args[index + 1], correlationId);
				normalized.splice(index, 2);
			}
		}
		return normalized;
	});
	return {
		entries: normalize(h.entries), calls, envelopes: normalize(h.writtenEnvelopes),
		lifecycle: normalize(h.events.emissions.filter(({ channel }) => Object.values(LIFECYCLE_CHANNELS).includes(channel as any))),
		timeline: h.timeline.filter((item) => item.startsWith("append:") || item.startsWith("publish:") || item.startsWith("envelope:")
			|| ["exec:pane:split", "exec:pane:swap", "exec:agent:start", "exec:agent:prompt", "exec:agent:rename"].includes(item)
			|| Object.values(LIFECYCLE_CHANNELS).some((channel) => item === `emit:${channel}`)),
	};
}

function assertBefore(h: AdapterHarness, earlier: string, later: string) {
	const first = h.timeline.indexOf(earlier);
	const second = h.timeline.indexOf(later);
	assert.ok(first >= 0 && second > first, `${earlier} must precede ${later}`);
}

function assertWireSpawn(result: WorkerReference, tool: any, correlationId?: string) {
	assert.deepEqual(result, {
		runId: result.runId, name: tool.details.name, paneId: tool.details.paneId, cwd: tool.details.cwd, adopted: tool.details.adopted,
		...(correlationId === undefined ? {} : { correlationId }),
		...(tool.details.model === undefined ? {} : { model: tool.details.model }),
		...(tool.details.type === undefined ? {} : { type: tool.details.type }),
		...(tool.details.purpose === undefined ? {} : { purpose: tool.details.purpose }),
	});
	assert.deepEqual(Object.keys(tool.details).sort(), ["runId", "correlationId", "name", "paneId", "cwd", "adopted", "model", "type", "purpose", "how", "initial_prompt"].sort());
}

for (const direction of ["right", "down", "left", "up"] as const) {
	for (const stacked of [false, true]) {
		test(`shared effects: fresh ${direction} creation, stacked=${stacked}`, async (t) => {
			const vertical = direction === "left" || direction === "right";
			const x = direction === "left" ? -10 : direction === "right" ? 10 : 0;
			const y = direction === "up" ? -10 : direction === "down" ? 10 : 0;
			const panes = stacked ? [
				{ pane_id: "self-pane", rect: { x: 0, y: 0, width: 10, height: 10 } },
				{ pane_id: "stack-first", rect: { x, y, width: 10, height: 10 } },
				{ pane_id: "stack-last", rect: { x: vertical ? x : x + 10, y: vertical ? y + 10 : y, width: 10, height: 10 } },
			] : [];
			const input: SpawnInput = { cwd: "/", direction, type: " Research ", purpose: "  Map auth  ", thinking: "high", initialPrompt: SEND_MESSAGE };
			const effects: ReturnType<typeof sharedEffects>[] = [];
			let tool: any;
			for (const adapter of ["tool", "rpc"] as const) {
				const h = await commandHarness(t, {
					branch: teamBranch({ teamMode: true }), contextCwd: "/", listening: true,
					agents: panes.slice(1).map(({ pane_id }, index) => ({ pane_id, name: `agent-stack-${index}`, tab_id: "tab-1" })),
					execOverride: (args) => args[1] === "layout" ? { code: 0, stdout: JSON.stringify({ result: { layout: { panes } } }), stderr: "" } : undefined,
				});
				const rpc = await rpcAdapter(h);
				const correlationId = adapter === "rpc" ? "dispatch-paired" : undefined;
				const result = await spawnVia(h, rpc, adapter, { ...input, ...(correlationId ? { correlationId } : {}) });
				const details = adapter === "tool" ? result.details : result;
				assert.equal(details.name, "agent-research");
				assert.equal(details.model, "xai/grok-4.6");
				assert.equal(details.type, "research");
				assert.equal(details.purpose, "Map auth");
				assert.deepEqual(h.entries.find(({ type }) => type === RUN_REGISTRATION_ENTRY)?.data.assignment, { cwd: "/", model: "xai/grok-4.6", role: "research" });
				const endpoint = h.entries.find(({ type }) => type === RUN_ENDPOINT_BINDING_ENTRY)?.data;
				assert.deepEqual([endpoint.runId, endpoint.agentName, endpoint.paneId], [details.runId, details.name, details.paneId]);
				const start = h.execCalls.find((args) => args[1] === "start")!;
				assert.deepEqual(start.slice(start.indexOf("--model")), ["--model", "xai/grok-4.6:high", "--team-role", "research: Map auth"]);
				assert.deepEqual(h.execCalls.filter((args) => ["split", "swap"].includes(args[1])), [
					["pane", "split", stacked ? "stack-last" : "self-pane", "--direction", stacked ? vertical ? "down" : "right" : vertical ? "right" : "down", "--cwd", "/", "--no-focus"],
					...(!stacked && ["left", "up"].includes(direction) ? [["pane", "swap", "--source-pane", "self-pane", "--target-pane", "new-pane"]] : []),
				]);
				assertBefore(h, `append:${RUN_REGISTRATION_ENTRY}`, "exec:pane:split");
				assertBefore(h, "exec:pane:split", `append:${RUN_ENDPOINT_BINDING_ENTRY}`);
				assertBefore(h, `append:${RUN_ENDPOINT_BINDING_ENTRY}`, "exec:agent:start");
				assertBefore(h, "exec:agent:start", `append:${LIFECYCLE_JOURNAL_ENTRY}`);
				assertBefore(h, `append:${LIFECYCLE_JOURNAL_ENTRY}`, "envelope:message");
				assert.equal(h.writtenEnvelopes[0].envelope.message, SEND_MESSAGE);
				assert.equal(h.writtenEnvelopes[0].envelope.runId, details.runId);
				if (adapter === "tool") tool = result;
				else assertWireSpawn(result, tool, correlationId);
				effects.push(sharedEffects(h, rpc.provider, adapter, correlationId));
			}
			assert.deepEqual(effects[1], effects[0]);
		});
	}
}

for (const row of [
	{ label: "automatic collision and explicit model", input: { model: " other/model ", thinking: "max" }, options: { agents: [{ name: "agent-1", pane_id: "taken" }] }, name: "agent-2", model: "other/model" },
	{ label: "context model and normalized name", input: { name: " BuIlDeR " }, options: {}, name: "agent-builder", model: "test/model" },
	{ label: "no model or role", input: { thinking: "high" }, options: { model: null }, name: "agent-1", model: undefined },
] satisfies Array<{ label: string; input: SpawnInput; options: Parameters<typeof harness>[0]; name: string; model?: string }>) {
	test(`shared effects: ${row.label}`, async (t) => {
		const effects = [];
		let tool: any;
		for (const adapter of ["tool", "rpc"] as const) {
			const h = await commandHarness(t, { branch: teamBranch({ teamMode: true }), ...row.options });
			const rpc = await rpcAdapter(h);
			const result = await spawnVia(h, rpc, adapter, { cwd: "/tmp", ...row.input });
			const details = adapter === "tool" ? result.details : result;
			assert.equal(details.name, row.name);
			assert.equal(details.model, row.model);
			if (adapter === "tool") tool = result;
			else assertWireSpawn(result, tool);
			effects.push(sharedEffects(h, rpc.provider, adapter));
		}
		assert.deepEqual(effects[1], effects[0]);
	});
}

for (const workerCwd of ["/workspace/live", null, "  "]) {
	for (const saved of [false, true]) {
		test(`shared effects: re-adoption cwd=${JSON.stringify(workerCwd)}, saved=${saved}`, async (t) => {
			const effects = [];
			let tool: any;
			for (const adapter of ["tool", "rpc"] as const) {
				const prior = { type: "review", purpose: "Keep charter", model: "saved/model" };
				const h = await commandHarness(t, { contextCwd: "/", workerCwd, listening: true, branch: teamBranch({ teamMode: true, ...(saved ? { meta: { "agent-scout": prior } } : {}) }) });
				const rpc = await rpcAdapter(h);
				const correlationId = adapter === "rpc" ? "dispatch-adopted" : undefined;
				const result = await spawnVia(h, rpc, adapter, { cwd: "/", name: " ScOuT ", type: "research", purpose: "Replace charter", model: "replace/model", initialPrompt: SEND_MESSAGE, ...(correlationId ? { correlationId } : {}) });
				const details = adapter === "tool" ? result.details : result;
				assert.equal(details.adopted, true);
				assert.equal(details.cwd, workerCwd?.trim() ? workerCwd : "/");
				assert.equal(details.paneId, "worker-pane");
				assert.equal(details.model, saved ? prior.model : undefined);
				assert.equal(details.type, saved ? prior.type : undefined);
				assert.equal(details.purpose, saved ? prior.purpose : undefined);
				assert.deepEqual(externalCalls(h), []);
				assert.deepEqual(h.entries.filter(({ type }) => type === LIFECYCLE_JOURNAL_ENTRY), []);
				assertBefore(h, `append:${RUN_REGISTRATION_ENTRY}`, `append:${RUN_ENDPOINT_BINDING_ENTRY}`);
				assertBefore(h, `append:${RUN_ENDPOINT_BINDING_ENTRY}`, "append:herdr-worker");
				assert.deepEqual(h.writtenEnvelopes.map(({ envelope }) => envelope.action ?? envelope.type), ["orchestrated-by", "bind-run", "message"]);
				assert.equal(h.writtenEnvelopes[1].envelope.binding.runId, details.runId);
				assert.equal(h.writtenEnvelopes[2].envelope.runId, details.runId);
				if (adapter === "tool") tool = result;
				else { assertWireSpawn(result, tool, correlationId); assert.notEqual(result.runId, tool.details.runId); }
				effects.push(sharedEffects(h, rpc.provider, adapter, correlationId));
			}
			assert.deepEqual(effects[1], effects[0]);
		});
	}
}

for (const listening of [false, true]) {
	for (const row of [
		{ label: "default", input: {}, priority: false },
		{ label: "legacy priority", input: { priority: true }, priority: true },
		{ label: "follow-up overrides priority", input: { mode: "follow-up", priority: true }, priority: false },
		{ label: "steer overrides priority", input: { mode: "steer", priority: false }, priority: true },
	] satisfies Array<{ label: string; input: Partial<SendInput>; priority: boolean }>) {
		test(`shared effects: send ${row.label}, listening=${listening}`, async (t) => {
			const effects = [];
			for (const adapter of ["tool", "rpc"] as const) {
				const h = await commandHarness(t, { listening, branch: teamBranch({ workers: ["agent-scout"] }) });
				const rpc = await rpcAdapter(h);
				if (adapter === "tool") {
					const result = await h.executeTool("SendToAgent", { target_id: "worker-pane", message: SEND_MESSAGE, priority: row.priority });
					const status = listening
						? `Delivered to agent-scout (pane worker-pane, idle) via inbox as ${row.priority ? "steer (priority)" : "follow-up"}. Replies arrive on a later turn.`
						: "Typed into agent-scout (pane worker-pane, pi, no inbox listener) via `herdr agent prompt`. Priority flag not applicable there.";
					assert.deepEqual(result, { content: [{ type: "text", text: status }], details: { target: "worker-pane", priority: row.priority, message: SEND_MESSAGE, status } });
				} else {
					assert.deepEqual(await rpc.client.send({ target: "worker-pane", message: SEND_MESSAGE, ...row.input }, rpc.provider), {
						target: "agent-scout", paneId: "worker-pane", kind: "pi", status: "idle", transport: listening ? "inbox" : "herdr-prompt", requestedMode: row.priority ? "steer" : "follow-up", priorityApplied: listening && row.priority,
					});
				}
				assert.deepEqual(h.entries, []);
				assert.deepEqual(h.events.emissions.filter(({ channel }) => Object.values(LIFECYCLE_CHANNELS).includes(channel as any)), []);
				if (listening) {
					assert.equal(h.writtenEnvelopes[0].envelope.priority, row.priority);
					assert.equal(h.writtenEnvelopes[0].envelope.message, SEND_MESSAGE);
				} else assert.deepEqual(externalCalls(h), [["agent", "prompt", "worker-pane", FRAMED_SEND_MESSAGE]]);
				effects.push(sharedEffects(h, rpc.provider, adapter));
			}
			assert.deepEqual(effects[1], effects[0]);
		});
	}
}

for (const row of [
	{ label: "invalid name", input: { name: "bad name" }, error: 'Invalid worker name "bad name" (use [a-z][a-z0-9_-]{0,31}; not add/list/release/from/status/help/adopt/right/down/left/up)', statuses: [] },
	{ label: "invalid cwd", cwd: "relative", error: "Worker cwd must be an absolute accessible directory.", statuses: [] },
	{ label: "split failure", stage: "split", response: { code: 1, stdout: "", stderr: "private split failure" }, error: "private split failure", statuses: [["uncertain", "pane_creation"]] },
	{ label: "missing pane identity", stage: "split", response: { code: 0, stdout: '{"result":{"private":"detail"}}', stderr: "" }, error: 'pane split returned no pane id: {"result":{"private":"detail"}}', statuses: [["uncertain", "pane_creation"]] },
	{ label: "start failure", stage: "start", response: { code: 1, stdout: "", stderr: "private start failure" }, error: "Started pane new-pane but agent start failed: private start failure. Check `herdr pane read new-pane`.", statuses: [["uncertain", "agent_start"]] },
	{ label: "assignment delivery", failDelivery: true, error: "mailbox write failed", statuses: [["started", undefined], ["uncertain", "assignment_delivery"]] },
	{ label: "re-adopted assignment delivery", input: { name: "scout" }, failDelivery: true, error: "mailbox write failed", statuses: [["uncertain", "assignment_delivery"]] },
]) {
	test(`shared effects: ${row.label} retains detailed local and safe wire errors`, async (t) => {
		const effects = [];
		for (const adapter of ["tool", "rpc"] as const) {
			const h = await commandHarness(t, {
				branch: teamBranch({ teamMode: true }), contextCwd: row.cwd ?? "/tmp", listening: true,
				writeEnvelopeErrorForType: row.failDelivery ? "message" : undefined,
				execOverride: (args) => row.stage && args[1] === row.stage ? row.response : undefined,
			});
			const rpc = await rpcAdapter(h);
			await assert.rejects(spawnVia(h, rpc, adapter, { cwd: h.ctx.cwd, name: "builder", initialPrompt: SEND_MESSAGE, ...row.input }), adapter === "tool"
				? { message: row.error }
				: { name: "RpcResponseError", code: "INTERNAL_ERROR", message: "The worker operation failed." });
			if (adapter === "rpc") assert.deepEqual(h.events.emissions.find(({ channel }) => channel === replyChannel(CHANNELS.spawn, "paired-2"))?.payload, {
				requestId: "paired-2", protocol: 1, success: false, error: { code: "INTERNAL_ERROR", message: "The worker operation failed." },
			});
			assert.deepEqual(h.entries.filter(({ type }) => type === LIFECYCLE_JOURNAL_ENTRY).map(({ data }) => [data.event.status, data.event.evidence.scope]), row.statuses);
			if (row.statuses.length === 0) { assert.deepEqual(h.entries, []); assert.deepEqual(externalCalls(h), []); }
			else assertBefore(h, `append:${RUN_REGISTRATION_ENTRY}`, `append:${LIFECYCLE_JOURNAL_ENTRY}`);
			effects.push(sharedEffects(h, rpc.provider, adapter));
		}
		assert.deepEqual(effects[1], effects[0]);
	});
}

function promiseGate() {
	let release!: () => void;
	const promise = new Promise<void>((resolve) => { release = resolve; });
	return { promise, release };
}

for (const row of [
	{ target: "missing", workers: ["agent-scout"], message: 'No live herdr agent "missing". Known agents: (none). Your workers: agent-scout', code: "NOT_FOUND", safe: "Target agent was not found." },
	{ target: "self-pane", workers: ["agent-scout"], message: "Refusing to send a message to yourself.", code: "NOT_TEAM_MEMBER", safe: "Target is not a team member." },
	{ target: "agent-scout", workers: [], message: '"agent-scout" is not in your team (it would drop the message anyway). Workers: (none); orchestrator: (none). Use /team add or /team adopt first.', code: "NOT_TEAM_MEMBER", safe: "Target is not a team member." },
	{ target: "agent-scout", workers: ["agent-scout"], fail: true, message: "private prompt failure", code: "INTERNAL_ERROR", safe: "The worker operation failed." },
]) {
	test(`shared effects: send error ${row.code} for ${row.target}, prompt failure=${!!row.fail}`, async (t) => {
		const effects = [];
		for (const adapter of ["tool", "rpc"] as const) {
			const h = await commandHarness(t, {
				branch: teamBranch({ workers: row.workers }),
				execOverride: (args) => row.fail && args[1] === "prompt" ? { code: 1, stdout: "", stderr: "private prompt failure" } : undefined,
			});
			const rpc = await rpcAdapter(h);
			if (adapter === "tool") await assert.rejects(h.executeTool("SendToAgent", { target_id: row.target, message: SEND_MESSAGE }), { message: row.message });
			else {
				await assert.rejects(rpc.client.send({ target: row.target, message: SEND_MESSAGE }, rpc.provider), { code: row.code, message: row.safe });
				assert.deepEqual(h.events.emissions.find(({ channel }) => channel === replyChannel(CHANNELS.send, "paired-2"))?.payload, {
					requestId: "paired-2", protocol: 1, success: false, error: { code: row.code, message: row.safe },
				});
			}
			assert.deepEqual(h.entries, []);
			assert.deepEqual(h.writtenEnvelopes, []);
			effects.push(sharedEffects(h, rpc.provider, adapter));
		}
		assert.deepEqual(effects[1], effects[0]);
	});
}

for (const initialPrompt of [undefined, " \n ", SEND_MESSAGE]) {
	test(`shared effects: re-adoption without an inbox, brief=${JSON.stringify(initialPrompt)}`, async (t) => {
		const effects = [];
		let tool: any;
		for (const adapter of ["tool", "rpc"] as const) {
			const h = await commandHarness(t, { branch: teamBranch({ teamMode: true }) });
			const rpc = await rpcAdapter(h);
			const result = await spawnVia(h, rpc, adapter, { cwd: "/tmp", name: "scout", ...(initialPrompt === undefined ? {} : { initialPrompt }) });
			assert.deepEqual(h.writtenEnvelopes, []);
			assert.equal(externalCalls(h).length, initialPrompt ? 1 : 0);
			if (initialPrompt === SEND_MESSAGE) assert.deepEqual(externalCalls(h), [["agent", "prompt", "worker-pane", FRAMED_SEND_MESSAGE]]);
			assert.deepEqual(h.entries.filter(({ type }) => type === LIFECYCLE_JOURNAL_ENTRY), []);
			if (adapter === "tool") tool = result;
			else assertWireSpawn(result, tool);
			effects.push(sharedEffects(h, rpc.provider, adapter));
		}
		assert.deepEqual(effects[1], effects[0]);
	});
}

for (const firstAdapter of ["tool", "rpc"] as const) {
	for (const rejectFirst of [false, true]) {
		test(`shared queue: ${firstAdapter} first, rejection=${rejectFirst}`, { timeout: 15_000 }, async (t) => {
			const entered = promiseGate();
			const blocked = promiseGate();
			const registeredSecond = promiseGate();
			let registrations = 0;
			const h = await commandHarness(t, {
				branch: teamBranch({ teamMode: true }), listening: true, splitPaneIds: ["first-pane", "second-pane"],
				onAppend: (type) => { if (type === RUN_REGISTRATION_ENTRY && ++registrations === 2) registeredSecond.release(); },
				execOverride: async (args) => {
					if (args[1] === "start" && args[2] === "agent-first") {
						entered.release();
						await blocked.promise;
						return { code: rejectFirst ? 1 : 0, stdout: '{"result":{}}', stderr: rejectFirst ? "first start failed" : "" };
					}
				},
			});
			const rpc = await rpcAdapter(h);
			const operations: Promise<any>[] = [];
			try {
				operations.push(spawnVia(h, rpc, firstAdapter, { cwd: "/tmp", name: "first", initialPrompt: "First assignment" }).then(
					(value) => { h.timeline.push("first:resolved"); return { value }; },
					(error) => { h.timeline.push("first:rejected"); return { error }; },
				));
				await entered.promise;
				operations.push(spawnVia(h, rpc, firstAdapter === "tool" ? "rpc" : "tool", { cwd: "/tmp", name: "second", initialPrompt: "Second assignment" }));
				await registeredSecond.promise;
				await new Promise<void>((resolve) => setImmediate(resolve));
				assert.equal(h.entries.filter(({ type }) => type === RUN_REGISTRATION_ENTRY).length, 2, "both runs register before entering the queue");
				assert.deepEqual(h.execCalls.filter((args) => args[1] === "split").map((args) => args[2]), ["self-pane"]);
				assert.deepEqual(h.execCalls.filter((args) => args[1] === "start").map((args) => args[2]), ["agent-first"]);
				blocked.release();
				const [first, second] = await Promise.all(operations);
				assert.equal("error" in first, rejectFirst);
				assert.equal((firstAdapter === "tool" ? second : second.details).name, "agent-second");
				const endpoints = h.entries.filter(({ type }) => type === RUN_ENDPOINT_BINDING_ENTRY).map(({ data }) => data);
				assert.deepEqual(endpoints.map(({ agentName, paneId }) => [agentName, paneId]), [["agent-first", "first-pane"], ["agent-second", "second-pane"]]);
				assert.notEqual(endpoints[0].runId, endpoints[1].runId);
				assert.deepEqual(h.entries.filter(({ type }) => type === LIFECYCLE_JOURNAL_ENTRY).map(({ data }) => [data.event.runId, data.event.status, data.event.evidence.scope]), [
					[endpoints[0].runId, rejectFirst ? "uncertain" : "started", rejectFirst ? "agent_start" : undefined],
					[endpoints[1].runId, "started", undefined],
				]);
				const splitPositions = h.timeline.flatMap((item, index) => item === "exec:pane:split" ? [index] : []);
				const firstSettled = h.timeline.indexOf(rejectFirst ? "first:rejected" : "first:resolved");
				assert.ok(firstSettled >= 0 && splitPositions[1] > firstSettled);
				assert.deepEqual(h.writtenEnvelopes.filter(({ envelope }) => envelope.type === "message").map(({ paneId, envelope }) => [paneId, envelope.runId, envelope.message]), [
					...(rejectFirst ? [] : [["first-pane", endpoints[0].runId, "First assignment"]]),
					["second-pane", endpoints[1].runId, "Second assignment"],
				]);
			} finally {
				blocked.release();
				await Promise.allSettled(operations);
			}
		});
	}
}

test("shared queue: tool aborted while waiting keeps registration but never enters creation", { timeout: 15_000 }, async (t) => {
	const entered = promiseGate();
	const blocked = promiseGate();
	const registeredSecond = promiseGate();
	let registrations = 0;
	const h = await commandHarness(t, {
		branch: teamBranch({ teamMode: true }), listening: true,
		onAppend: (type) => { if (type === RUN_REGISTRATION_ENTRY && ++registrations === 2) registeredSecond.release(); },
		execOverride: async (args) => { if (args[1] === "split") { entered.release(); await blocked.promise; } },
	});
	const rpc = await rpcAdapter(h);
	const controller = new AbortController();
	const first = rpc.client.spawn({ name: "first" }, rpc.provider);
	let rejected: Promise<void> | undefined;
	try {
		await entered.promise;
		const second = h.executeTool("CreateAgentPanel", { name: "second" }, controller.signal);
		rejected = assert.rejects(second, { message: "cancel queued tool" });
		await registeredSecond.promise;
		const queuedRun = h.entries.filter(({ type }) => type === RUN_REGISTRATION_ENTRY)[1].data.runId;
		controller.abort(new Error("cancel queued tool"));
		blocked.release();
		await Promise.all([first, rejected]);
		assert.equal(h.entries.filter(({ type }) => type === RUN_REGISTRATION_ENTRY).length, 2);
		assert.equal(h.entries.some(({ type, data }) => type === RUN_ENDPOINT_BINDING_ENTRY && data.runId === queuedRun), false);
		assert.equal(h.entries.some(({ type, data }) => type === LIFECYCLE_JOURNAL_ENTRY && data.event.runId === queuedRun), false);
		assert.deepEqual(h.execCalls.filter((args) => args[1] === "start").map((args) => args[2]), ["agent-first"]);
		assert.equal(h.execCalls.filter((args) => args[1] === "split").length, 1);
		assert.deepEqual(h.partialToolUpdates, [{ id: "call", update: CREATE_PROGRESS }]);
	} finally { blocked.release(); await Promise.allSettled([first, ...(rejected ? [rejected] : [])]); }
});

test("shared service: RPC client abort cleans its listener while provider finishes and replies later", { timeout: 15_000 }, async (t) => {
	const entered = promiseGate();
	const blocked = promiseGate();
	let providerSignal: AbortSignal | undefined;
	const h = await commandHarness(t, {
		listening: true,
		execOverride: async (args, options) => {
			if (args[1] === "split") { providerSignal = options.signal; entered.release(); await blocked.promise; }
		},
	});
	const rpc = await rpcAdapter(h);
	const controller = new AbortController();
	const waiting = rpc.client.spawn({ name: "builder", initialPrompt: SEND_MESSAGE }, rpc.provider, { signal: controller.signal });
	const rejected = assert.rejects(waiting, RpcAbortError);
	const channel = replyChannel(CHANNELS.spawn, "paired-2");
	let unsubscribe = () => {};
	let lateReply: Promise<RpcReply<WorkerReference>> | undefined;
	try {
		await entered.promise;
		assert.equal(h.events.listenerCount(channel), 1);
		controller.abort();
		await rejected;
		assert.equal(h.events.listenerCount(channel), 0);
		assert.equal(h.events.listenerCount(), 11);
		assert.equal(providerSignal?.aborted, false);
		assert.equal(h.writtenEnvelopes.length, 0);
		lateReply = new Promise((resolve) => { unsubscribe = h.events.on(channel, (reply) => resolve(reply as RpcReply<WorkerReference>)); });
		blocked.release();
		const reply = await lateReply;
		assert.equal(reply.success, true);
		if (!reply.success) assert.fail("provider should finish after caller abort");
		assert.equal(h.writtenEnvelopes.at(-1)?.envelope.runId, reply.data.runId);
		assert.equal(h.writtenEnvelopes.at(-1)?.envelope.message, SEND_MESSAGE);
		assert.deepEqual(h.entries.filter(({ type }) => type === LIFECYCLE_JOURNAL_ENTRY).map(({ data }) => data.event.status), ["started"]);
		assertBefore(h, "envelope:message", `emit:${channel}`);
		assert.equal(providerSignal?.aborted, false);
	} finally {
		blocked.release();
		await rejected;
		if (lateReply) await lateReply;
		unsubscribe();
	}
	assert.equal(h.events.listenerCount(channel), 0);
});

test("shared service: fixed probe facts and readiness stay independent of team mode across reload", async (t) => {
	const h = await commandHarness(t, { activeTools: ["read"] });
	const first = await rpcAdapter(h);
	assert.deepEqual(first.provider, { protocol: 1, provider: "herdr", providerInstanceId: first.provider.providerInstanceId, available: true, capabilities: ["spawn", "send", "steer", "inspect"], constraints: { requiresHerdrPane: true, requiresInteractivePi: true } });
	assert.deepEqual(h.activeTools(), ["read"]);
	await first.client.spawn({ name: "scout" }, first.provider);
	assert.ok(h.activeTools().includes("CreateAgentPanel"));
	await h.handlers.get("session_shutdown")![0]();
	assert.equal(h.events.listenerCount(), 0);
	h.herdrWorker(h.pi, { disableInbox: true, isListening: () => false });
	t.after(async () => { await h.handlers.get("session_shutdown")![1](); });
	await h.handlers.get("session_start")![1]({ reason: "startup" }, h.ctx);
	const second = await rpcAdapter(h);
	assert.notEqual(second.provider.providerInstanceId, first.provider.providerInstanceId);
	const before = { calls: h.execCalls.length, entries: h.entries.length, envelopes: h.writtenEnvelopes.length };
	for (const [channel, input] of [[CHANNELS.spawn, { name: "builder" }], [CHANNELS.send, { target: "agent-scout", message: "stale" }], [CHANNELS.inspect, { target: "agent-scout" }]] as const) {
		const requestId = `stale-${channel.split(":").at(-1)}`;
		h.events.emit(channel, { requestId, providerInstanceId: first.provider.providerInstanceId, protocol: 1, ...input });
		await new Promise<void>((resolve) => setImmediate(resolve));
		assert.equal(h.events.emissions.some((item) => item.channel === replyChannel(channel, requestId)), false);
	}
	assert.deepEqual({ calls: h.execCalls.length, entries: h.entries.length, envelopes: h.writtenEnvelopes.length }, before);
	assert.equal(h.events.listenerCount(), 11);
	await h.handlers.get("session_shutdown")![1]();
	assert.equal(h.events.listenerCount(), 0);
});
