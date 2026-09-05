import assert from "node:assert/strict";
import test from "node:test";
import { LIFECYCLE_JOURNAL_ENTRY } from "../../lifecycle/acceptor.js";
import { LIFECYCLE_CHANNELS, type AcceptedLifecycleEvent } from "../../lifecycle/protocol.js";
import { CHANNELS, replyChannel, type DeliveryReceipt, type Inspection, type RpcReply, type WorkerReference } from "../../rpc/protocol.js";
import { RECONCILIATION_CHANNELS, reconciliationReplyChannel, type ReconciliationReply } from "../../reconciliation/protocol.js";
import { RUN_QUERY_CHANNELS, runQueryReplyChannel, type RunQueryReply, type WorkerRunRecordV1 } from "../../runs/protocol.js";
import { RUN_ENDPOINT_BINDING_ENTRY, RUN_REGISTRATION_ENTRY } from "../../runs/registry.js";
import { FakeIsolatedEventBus } from "../support/fake-isolated-event-bus.js";

process.env.HERDR_ENV = "1";
process.env.HERDR_PANE_ID = "self-pane";
process.env.HERDR_TAB_ID = "tab-1";

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
} = {}) {
	const { default: herdrWorker } = await import("../../extensions/herdr-worker.js");
	const timeline: string[] = [];
	const events = new FakeIsolatedEventBus((channel) => timeline.push(`emit:${channel}`));
	const tools = new Map<string, any>();
	const commands = new Map<string, any>();
	const handlers = new Map<string, Array<(...args: any[]) => any>>();
	const entries: Array<{ type: string; data: any }> = [];
	const sessionEntries = options.sessionEntries ?? [];
	const execCalls: string[][] = [];
	const writtenEnvelopes: Array<{ paneId: string; envelope: any }> = [];
	const sentMessages: Array<{ message: any; options: any }> = [];
	const agentGetTargets: string[] = [];
	const startedAgents = new Set<string>();
	let inboxHandler: ((envelope: unknown, envelopeId: string) => Promise<void>) | undefined;
	let activeTools: string[] = [];
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
		setActiveTools(value: string[]) { activeTools = value; },
		appendEntry(type: string, data: any) {
			entries.push({ type, data });
			sessionEntries.push({ type: "custom", customType: type, data });
			timeline.push(`append:${type}`);
		},
		sendMessage(message: any, messageOptions: any) { sentMessages.push({ message, options: messageOptions }); },
		sendUserMessage() {},
		async exec(_command: string, args: string[]) {
			execCalls.push(args);
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
		},
	};
	const ctx: any = {
		mode: "tui",
		cwd: options.contextCwd ?? "/tmp",
		hasUI: false,
		model: { provider: "test", id: "model" },
		signal: new AbortController().signal,
		sessionManager: { getSessionId: () => "session", getBranch: () => options.branch ?? [], getEntries: () => sessionEntries },
		ui: { setStatus() {}, notify() {} },
		isIdle: () => true,
	};
	herdrWorker(pi, {
		disableInbox: true,
		isListening: () => options.listening ?? false,
		writeEnvelope: (paneId, envelope) => {
			if (envelope.type === options.writeEnvelopeErrorForType) throw new Error("mailbox write failed");
			writtenEnvelopes.push({ paneId, envelope });
			timeline.push(`envelope:${envelope.type}`);
		},
		onInboxHandler: (handler) => { inboxHandler = handler; },
	});
	return {
		events, tools, commands, handlers, entries, sessionEntries, timeline, execCalls, agentGetTargets,
		writtenEnvelopes, sentMessages, ctx, pi, herdrWorker, activeTools: () => activeTools,
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
		assert.deepEqual(sessionEntries, [registration, endpoint], item.label);
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
	branch[0].data.workers = [];
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
