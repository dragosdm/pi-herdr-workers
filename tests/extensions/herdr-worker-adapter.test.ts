import assert from "node:assert/strict";
import test from "node:test";
import { CHANNELS, replyChannel, type DeliveryReceipt, type Inspection, type RpcReply, type WorkerReference } from "../../rpc/protocol.js";
import { FakeEventBus } from "../support/fake-event-bus.js";

process.env.HERDR_ENV = "1";
process.env.HERDR_PANE_ID = "self-pane";
process.env.HERDR_TAB_ID = "tab-1";

async function harness(options: { listening?: boolean; branch?: any[]; missingAgents?: string[] } = {}) {
	const { default: herdrWorker } = await import("../../extensions/herdr-worker.js");
	const events = new FakeEventBus();
	const tools = new Map<string, any>();
	const commands = new Map<string, any>();
	const handlers = new Map<string, Array<(...args: any[]) => any>>();
	const entries: Array<{ type: string; data: any }> = [];
	const execCalls: string[][] = [];
	const writtenEnvelopes: Array<{ paneId: string; envelope: any }> = [];
	const agentGetTargets: string[] = [];
	let activeTools: string[] = [];
	const pi: any = {
		events,
		registerFlag() {},
		getFlag() { return undefined; },
		registerTool(tool: any) { tools.set(tool.name, tool); },
		registerCommand(name: string, command: any) { commands.set(name, command); },
		on(name: string, handler: (...args: any[]) => any) {
			const list = handlers.get(name) ?? [];
			list.push(handler);
			handlers.set(name, list);
		},
		getActiveTools() { return activeTools; },
		setActiveTools(value: string[]) { activeTools = value; },
		appendEntry(type: string, data: any) { entries.push({ type, data }); },
		sendMessage() {},
		sendUserMessage() {},
		async exec(_command: string, args: string[]) {
			execCalls.push(args);
			if (args[0] === "pane" && args[1] === "layout") return { code: 0, stdout: JSON.stringify({ result: { layout: { panes: [] } } }), stderr: "" };
			if (args[0] === "pane" && args[1] === "split") return { code: 0, stdout: JSON.stringify({ result: { pane: { pane_id: "new-pane" } } }), stderr: "" };
			if (args[0] === "agent" && args[1] === "get") {
				const target = args[2];
				agentGetTargets.push(target);
				const agent = target === "self-pane"
					? { pane_id: "self-pane", tab_id: "tab-1", name: "orchestrator", agent: "pi", cwd: "/tmp" }
					: (target === "agent-scout" || target === "worker-pane") && !options.missingAgents?.includes(target)
						? { pane_id: "worker-pane", tab_id: "tab-1", name: "agent-scout", agent: "pi", agent_status: "idle", cwd: "/tmp" }
						: target === "boss" && !options.missingAgents?.includes(target)
							? { pane_id: "boss-pane", tab_id: "tab-1", name: "boss", agent: "pi", agent_status: "busy", cwd: "/workspace" }
						: undefined;
				return { code: agent ? 0 : 1, stdout: agent ? JSON.stringify({ result: { agent } }) : "", stderr: agent ? "" : "missing" };
			}
			if (args[0] === "agent" && args[1] === "list") return { code: 0, stdout: JSON.stringify({ result: { agents: [] } }), stderr: "" };
			return { code: 0, stdout: JSON.stringify({ result: {} }), stderr: "" };
		},
	};
	const ctx: any = {
		mode: "tui",
		cwd: "/tmp",
		hasUI: false,
		model: { provider: "test", id: "model" },
		signal: new AbortController().signal,
		sessionManager: { getSessionId: () => "session", getBranch: () => options.branch ?? [], getEntries: () => [] },
		ui: { setStatus() {}, notify() {} },
		isIdle: () => true,
	};
	herdrWorker(pi, { disableInbox: true, isListening: () => options.listening ?? false, writeEnvelope: (paneId, envelope) => writtenEnvelopes.push({ paneId, envelope }) });
	return { events, tools, commands, handlers, entries, execCalls, agentGetTargets, writtenEnvelopes, ctx, pi, herdrWorker, activeTools: () => activeTools };
}

async function emitForReply<T>(events: FakeEventBus, channel: typeof CHANNELS.spawn | typeof CHANNELS.probe | typeof CHANNELS.send | typeof CHANNELS.inspect, requestId: string, payload: unknown): Promise<RpcReply<T>> {
	return await new Promise((resolve) => {
		const unsubscribe = events.on(replyChannel(channel, requestId), (reply) => {
			unsubscribe();
			resolve(reply as RpcReply<T>);
		});
		events.emit(channel, payload);
	});
}

function teamBranch(data: any): any[] {
	return [{ type: "custom", customType: "herdr-worker", data: { version: 1, sessionId: "session", workers: [], ...data } }];
}

test("registers once, exposes live availability, and disposes on shutdown", async () => {
	const h = await harness();
	assert.equal(h.events.listenerCount(), 5);
	assert.deepEqual([...h.tools.keys()].sort(), ["CreateAgentPanel", "SendToAgent"]);
	assert.deepEqual([...h.commands.keys()].sort(), ["orchestrated-by", "team"]);
	let probe = await emitForReply<any>(h.events, CHANNELS.probe, "before", { requestId: "before", supportedProtocols: [1] });
	assert.equal(probe.ok && probe.data.reason, "SESSION_NOT_READY");
	await h.handlers.get("session_start")![0]({ reason: "startup" }, h.ctx);
	probe = await emitForReply<any>(h.events, CHANNELS.probe, "after", { requestId: "after", supportedProtocols: [1] });
	assert.equal(probe.ok && probe.data.available, true);
	await h.handlers.get("session_tree")![0]({}, h.ctx);
	assert.equal(h.events.listenerCount(), 5);
	await h.handlers.get("session_shutdown")![0]();
	assert.equal(h.events.listenerCount(), 0);
});

test("RPC spawn validates cwd before side effects, activates team mode, and returns canonical facts", async () => {
	const h = await harness();
	await h.handlers.get("session_start")![0]({ reason: "startup" }, h.ctx);
	const probe = await emitForReply<any>(h.events, CHANNELS.probe, "probe", { requestId: "probe", supportedProtocols: [1] });
	assert.equal(probe.ok, true);
	const providerInstanceId = probe.ok ? probe.data.providerInstanceId : "";
	const callsBefore = h.execCalls.length;
	const invalid = await emitForReply(h.events, CHANNELS.spawn, "invalid", { requestId: "invalid", providerInstanceId, protocol: 1, cwd: "relative" });
	assert.equal(invalid.ok, false);
	assert.equal(h.execCalls.length, callsBefore);
	assert.equal(h.entries.length, 0);

	const spawned = await emitForReply<WorkerReference>(h.events, CHANNELS.spawn, "spawn", { requestId: "spawn", providerInstanceId, protocol: 1, name: "scout" });
	assert.deepEqual(spawned.ok && spawned.data, { name: "agent-scout", paneId: "worker-pane", cwd: "/tmp", adopted: true });
	assert.equal(h.entries.some((entry) => entry.data.teamMode === true), true);
	assert.equal(h.activeTools().includes("CreateAgentPanel"), true);
});

test("CreateAgentPanel preserves parameter mapping through the shared spawn facade", async () => {
	const h = await harness();
	await h.handlers.get("session_start")![0]({ reason: "startup" }, h.ctx);
	await h.commands.get("team").handler("list", h.ctx);
	const result = await h.tools.get("CreateAgentPanel").execute("call", { name: "scout" }, h.ctx.signal, undefined, h.ctx);
	assert.match(result.content[0].text, /Worker agent-scout ready in pane worker-pane/);
	assert.equal(result.details.adopted, true);
	assert.equal(result.details.cwd, "/tmp");
});

test("SendToAgent and RPC send share delivery while preserving tool details and sanitizing receipts", async () => {
	const h = await harness();
	await h.handlers.get("session_start")![0]({ reason: "startup" }, h.ctx);
	await h.commands.get("team").handler("adopt agent-scout", h.ctx);
	const tool = await h.tools.get("SendToAgent").execute("call", { target_id: "agent-scout", message: "tool secret", priority: true }, h.ctx.signal);
	assert.match(tool.content[0].text, /Typed into agent-scout .* Priority flag not applicable there/);
	assert.deepEqual(tool.details, { target: "agent-scout", priority: true, message: "tool secret", status: tool.content[0].text });

	const probe = await emitForReply<any>(h.events, CHANNELS.probe, "send-probe", { requestId: "send-probe", supportedProtocols: [1] });
	const providerInstanceId = probe.ok ? probe.data.providerInstanceId : "";
	const reply = await emitForReply<DeliveryReceipt>(h.events, CHANNELS.send, "send", { requestId: "send", providerInstanceId, protocol: 1, target: "agent-scout", message: "rpc secret", mode: "steer" });
	assert.deepEqual(reply.ok && reply.data, { target: "agent-scout", paneId: "worker-pane", kind: "pi", status: "idle", transport: "herdr-prompt", requestedMode: "steer", priorityApplied: false });
	assert.doesNotMatch(JSON.stringify(reply), /rpc secret/);
	assert.equal(h.execCalls.filter((args) => args[0] === "agent" && args[1] === "prompt").length, 2);
});

test("RPC inbox receipts report requested mode and actual priority without writing a mailbox", async () => {
	const h = await harness({ listening: true });
	await h.handlers.get("session_start")![0]({ reason: "startup" }, h.ctx);
	await h.commands.get("team").handler("adopt agent-scout", h.ctx);
	const probe = await emitForReply<any>(h.events, CHANNELS.probe, "inbox-probe", { requestId: "inbox-probe", supportedProtocols: [1] });
	const providerInstanceId = probe.ok ? probe.data.providerInstanceId : "";
	const reply = await emitForReply<DeliveryReceipt>(h.events, CHANNELS.send, "inbox", { requestId: "inbox", providerInstanceId, protocol: 1, target: "agent-scout", message: "priority body", priority: true });
	assert.deepEqual(reply.ok && reply.data, { target: "agent-scout", paneId: "worker-pane", kind: "pi", status: "idle", transport: "inbox", requestedMode: "steer", priorityApplied: true });
	const messages = h.writtenEnvelopes.filter(({ envelope }) => envelope.type === "message");
	assert.equal(messages.length, 1);
	assert.equal(messages[0].envelope.priority, true);
	assert.doesNotMatch(JSON.stringify(reply), /priority body/);
});

test("RPC direction and thinking reach the canonical creation sequence", async () => {
	const h = await harness();
	await h.handlers.get("session_start")![0]({ reason: "startup" }, h.ctx);
	const probe = await emitForReply<any>(h.events, CHANNELS.probe, "options-probe", { requestId: "options-probe", supportedProtocols: [1] });
	const providerInstanceId = probe.ok ? probe.data.providerInstanceId : "";
	const spawned = await emitForReply<WorkerReference>(h.events, CHANNELS.spawn, "options", {
		requestId: "options", providerInstanceId, protocol: 1, name: "builder", direction: "left", model: "test/model", thinking: "high",
	});
	assert.equal(spawned.ok && spawned.data.paneId, "new-pane");
	assert.equal(h.execCalls.some((args) => args[0] === "pane" && args[1] === "split" && args.includes("--direction") && args.includes("right")), true);
	assert.equal(h.execCalls.some((args) => args[0] === "pane" && args[1] === "swap"), true);
	const start = h.execCalls.find((args) => args[0] === "agent" && args[1] === "start");
	assert.ok(start);
	assert.equal(start.includes("test/model:high"), true);
});

test("RPC inspect projects worker and orchestrator facts from authorized relationships", async () => {
	const h = await harness({ branch: teamBranch({
		workers: ["agent-scout"],
		orchestratedBy: "boss",
		meta: { "agent-scout": { type: "explore", purpose: "Map internals", model: "test/scout", paneId: "worker-pane" } },
	}) });
	await h.handlers.get("session_start")![0]({ reason: "startup" }, h.ctx);
	const probe = await emitForReply<any>(h.events, CHANNELS.probe, "inspect-probe", { requestId: "inspect-probe", supportedProtocols: [1] });
	const providerInstanceId = probe.ok ? probe.data.providerInstanceId : "";
	const worker = await emitForReply<Inspection>(h.events, CHANNELS.inspect, "worker", { requestId: "worker", providerInstanceId, protocol: 1, target: "worker-pane" });
	assert.deepEqual(worker.ok && worker.data, {
		name: "agent-scout", paneId: "worker-pane", kind: "pi", status: "idle", cwd: "/tmp",
		type: "explore", purpose: "Map internals", model: "test/scout", relationship: "worker", managedBySession: true,
	});
	const orchestrator = await emitForReply<Inspection>(h.events, CHANNELS.inspect, "boss", { requestId: "boss", providerInstanceId, protocol: 1, target: "boss" });
	assert.deepEqual(orchestrator.ok && orchestrator.data, {
		name: "boss", paneId: "boss-pane", kind: "pi", status: "busy", cwd: "/workspace", relationship: "orchestrator", managedBySession: false,
	});
	assert.doesNotMatch(JSON.stringify([worker, orchestrator]), /tab-1|process|prompt/);
});

test("RPC inspect rejects self and outsiders before lookup and classifies stale peers", async () => {
	const h = await harness({ branch: teamBranch({ workers: ["agent-scout"] }), missingAgents: ["agent-scout"] });
	await h.handlers.get("session_start")![0]({ reason: "startup" }, h.ctx);
	const probe = await emitForReply<any>(h.events, CHANNELS.probe, "auth-probe", { requestId: "auth-probe", supportedProtocols: [1] });
	const providerInstanceId = probe.ok ? probe.data.providerInstanceId : "";
	for (const target of ["self-pane", "outside-agent"]) {
		const reply = await emitForReply<Inspection>(h.events, CHANNELS.inspect, `inspect-${target}`, { requestId: `inspect-${target}`, providerInstanceId, protocol: 1, target });
		assert.equal(reply.ok ? "ok" : reply.error.code, "NOT_TEAM_MEMBER");
	}
	assert.deepEqual(h.agentGetTargets, ["self-pane"]);
	const stale = await emitForReply<Inspection>(h.events, CHANNELS.inspect, "stale", { requestId: "stale", providerInstanceId, protocol: 1, target: "agent-scout" });
	assert.equal(stale.ok ? "ok" : stale.error.code, "NOT_FOUND");
	assert.deepEqual(h.agentGetTargets, ["self-pane", "agent-scout"]);
});

test("session tree replaces inspect authority without replacing the provider instance", async () => {
	const branch = teamBranch({ workers: ["agent-scout"] });
	const h = await harness({ branch });
	await h.handlers.get("session_start")![0]({ reason: "startup" }, h.ctx);
	const first = await emitForReply<any>(h.events, CHANNELS.probe, "first-generation", { requestId: "first-generation", supportedProtocols: [1] });
	branch[0].data.workers = [];
	await h.handlers.get("session_tree")![0]({}, h.ctx);
	const second = await emitForReply<any>(h.events, CHANNELS.probe, "same-generation", { requestId: "same-generation", supportedProtocols: [1] });
	assert.equal(first.ok && second.ok && first.data.providerInstanceId, second.ok && second.data.providerInstanceId);
	const providerInstanceId = second.ok ? second.data.providerInstanceId : "";
	const denied = await emitForReply<Inspection>(h.events, CHANNELS.inspect, "after-tree", { requestId: "after-tree", providerInstanceId, protocol: 1, target: "agent-scout" });
	assert.equal(denied.ok ? "ok" : denied.error.code, "NOT_TEAM_MEMBER");
	assert.deepEqual(h.agentGetTargets, ["self-pane"]);
});

test("reload replaces the provider instance and stale addressed requests are no-ops", async () => {
	const h = await harness();
	await h.handlers.get("session_start")![0]({ reason: "startup" }, h.ctx);
	const oldProbe = await emitForReply<any>(h.events, CHANNELS.probe, "old-provider", { requestId: "old-provider", supportedProtocols: [1] });
	const oldInstanceId = oldProbe.ok ? oldProbe.data.providerInstanceId : "";
	await h.handlers.get("session_shutdown")![0]();
	h.herdrWorker(h.pi, { disableInbox: true });
	await h.handlers.get("session_start")![1]({ reason: "startup" }, h.ctx);
	const newProbe = await emitForReply<any>(h.events, CHANNELS.probe, "new-provider", { requestId: "new-provider", supportedProtocols: [1] });
	const newInstanceId = newProbe.ok ? newProbe.data.providerInstanceId : "";
	assert.notEqual(newInstanceId, oldInstanceId);
	let replied = false;
	const unsubscribe = h.events.on(replyChannel(CHANNELS.inspect, "stale-provider"), () => { replied = true; });
	h.events.emit(CHANNELS.inspect, { requestId: "stale-provider", providerInstanceId: oldInstanceId, protocol: 1, target: "agent-scout" });
	await new Promise((resolve) => setImmediate(resolve));
	unsubscribe();
	assert.equal(replied, false);
	assert.equal(h.events.listenerCount(), 5);
});
