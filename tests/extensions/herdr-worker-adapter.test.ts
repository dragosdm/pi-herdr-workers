import assert from "node:assert/strict";
import { execFile } from "node:child_process";
import test, { type TestContext } from "node:test";
import { fileURLToPath } from "node:url";
import { promisify } from "node:util";
import { LIFECYCLE_JOURNAL_ENTRY } from "../../lifecycle/acceptor.js";
import { LIFECYCLE_CHANNELS, type AcceptedLifecycleEvent } from "../../lifecycle/protocol.js";
import { CHANNELS, replyChannel, type DeliveryReceipt, type Inspection, type RpcReply, type WorkerReference } from "../../rpc/protocol.js";
import { RECONCILIATION_CHANNELS, reconciliationReplyChannel, type ReconciliationReply } from "../../reconciliation/protocol.js";
import { RUN_QUERY_CHANNELS, runQueryReplyChannel, type RunQueryReply, type WorkerRunRecordV1 } from "../../runs/protocol.js";
import { RUN_ENDPOINT_BINDING_ENTRY, RUN_REGISTRATION_ENTRY } from "../../runs/registry.js";
import { FakeIsolatedEventBus } from "../support/fake-isolated-event-bus.js";

if (!process.env.TEAM_COMMAND_ENV_CASE) {
	process.env.HERDR_ENV = "1";
	process.env.HERDR_PANE_ID = "self-pane";
	process.env.HERDR_TAB_ID = "tab-1";
}

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
	execOverride?: (args: string[]) => Promise<any> | any;
	writeEnvelopeErrorForType?: string;
	isIdle?: boolean;
	hasUI?: boolean;
	mode?: string;
	model?: { provider: string; id: string } | null;
	activeTools?: string[];
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
	const pendingExec = new Set<Promise<any>>();
	const paneMetadataCalls: string[][] = [];
	const sentUserMessages: Array<[text: string, options?: { deliverAs: "followUp" }]> = [];
	const notifications: Array<{ text: string; level: string }> = [];
	const statusUpdates: Array<{ key: string; text: string | undefined }> = [];
	const activeToolUpdates: string[][] = [];
	const writtenEnvelopes: Array<{ paneId: string; envelope: any }> = [];
	const sentMessages: Array<{ message: any; options: any }> = [];
	const agentGetTargets: string[] = [];
	const startedAgents = new Set<string>();
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
		},
		sendMessage(message: any, messageOptions: any) { sentMessages.push({ message, options: messageOptions }); },
		sendUserMessage(...args: [string, { deliverAs: "followUp" }?]) { sentUserMessages.push(args); },
		exec(_command: string, args: string[]) {
			const call = execute(args);
			pendingExec.add(call);
			void call.then(() => pendingExec.delete(call), () => pendingExec.delete(call));
			return call;
		},
	};
	async function execute(args: string[]) {
		execCalls.push(args);
		if (args[0] === "pane" && args[1] === "report-metadata") paneMetadataCalls.push(args);
		timeline.push(`exec:${args[0]}:${args[1] ?? ""}`);
		const overridden = await options.execOverride?.(args);
		if (overridden !== undefined) {
			if (args[0] === "agent" && args[1] === "start" && overridden.code === 0) startedAgents.add(args[2]);
			return overridden;
		}
		if (args[0] === "pane" && args[1] === "layout") return { code: 0, stdout: JSON.stringify({ result: { layout: { panes: [] } } }), stderr: "" };
		if (args[0] === "pane" && args[1] === "split") return { code: 0, stdout: JSON.stringify({ result: { pane: { pane_id: "new-pane" } } }), stderr: "" };
		if (args[0] === "agent" && args[1] === "get") {
			const target = args[2];
			agentGetTargets.push(target);
			const agent = target === "self-pane"
				? { pane_id: "self-pane", tab_id: "tab-1", name: options.selfName ?? "orchestrator", agent: "pi", cwd: "/tmp" }
				: (target === "agent-scout" || target === "worker-pane" || startedAgents.has(target)) && !options.missingAgents?.includes(target)
					? {
						pane_id: startedAgents.has(target) ? "new-pane" : "worker-pane", tab_id: "tab-1", name: startedAgents.has(target) ? target : "agent-scout", agent: "pi", agent_status: "idle",
						...(options.workerCwd === null ? {} : { cwd: options.workerCwd ?? "/tmp" }),
					}
					: target === "boss" && !options.missingAgents?.includes(target)
						? { pane_id: "boss-pane", tab_id: "tab-1", name: "boss", agent: "pi", agent_status: "busy", cwd: "/workspace" }
					: undefined;
			return { code: agent ? 0 : 1, stdout: agent ? JSON.stringify({ result: { agent } }) : "", stderr: agent ? "" : "missing" };
		}
		if (args[0] === "agent" && args[1] === "list") return { code: 0, stdout: JSON.stringify({ result: { agents: options.agents ?? [] } }), stderr: "" };
		if (args[0] === "agent" && args[1] === "start") startedAgents.add(args[2]);
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
