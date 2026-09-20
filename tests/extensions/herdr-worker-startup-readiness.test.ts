import assert from "node:assert/strict";
import * as fs from "node:fs";
import * as os from "node:os";
import * as path from "node:path";
import test, { type TestContext } from "node:test";
import { createMailboxTransport, type MailboxCallbacks, type MailboxTransport } from "../../mailbox/transport.js";
import { inboxDir } from "../../mailbox/paths.js";
import { createWorkerRpcClient } from "../../rpc/client.js";
import { createRunQueryClient } from "../../runs/client.js";
import { LIFECYCLE_CHANNELS } from "../../lifecycle/protocol.js";
import { journals } from "../support/mailbox-lifecycle.js";
import { bounded } from "../support/mailbox-fixture.js";
import { FakeIsolatedEventBus } from "../support/fake-isolated-event-bus.js";

process.env.HERDR_ENV = "1";
process.env.HERDR_PANE_ID = "self-pane";
process.env.HERDR_TAB_ID = "tab-1";

function gate() {
	let release!: () => void;
	const promise = new Promise<void>((resolve) => { release = resolve; });
	return { promise, release };
}

async function fixture(t: TestContext) {
	const root = fs.mkdtempSync(path.join(os.tmpdir(), "a04-readiness-"));
	const sessionFile = path.join(root, "session.jsonl");
	const gates: ReturnType<typeof gate>[] = [];
	const hosts: any[] = [];
	const operations: Promise<unknown>[] = [];
	const barrier = () => { const g = gate(); gates.push(g); return g; };
	t.after(async () => {
		for (const g of gates) g.release();
		for (const h of hosts) await h.shutdown();
		await bounded(Promise.allSettled(operations), "fixture operations");
		fs.rmSync(root, { recursive: true, force: true });
	});
	async function host(options: {
		reopen?: boolean;
		liveFresh?: boolean;
		beforeDeliver?: MailboxCallbacks["deliver"];
		splitGate?: ReturnType<typeof gate>;
		splitEntered?: ReturnType<typeof gate>;
		missingPane?: boolean;
		splitError?: boolean;
	} = {}) {
		const { default: extension } = await import("../../extensions/herdr-worker.js");
		const entries: any[] = options.reopen && fs.existsSync(sessionFile)
			? fs.readFileSync(sessionFile, "utf8").trim().split("\n").filter(Boolean).map((line) => JSON.parse(line)) : [];
		const timeline: string[] = [];
		const events = new FakeIsolatedEventBus((channel) => timeline.push(`emit:${channel}`));
		const handlers = new Map<string, any[]>();
		const queued: any[] = [];
		const sent: any[] = [];
		const warnings: string[] = [];
		const calls: string[][] = [];
		const scheduled: Array<{ callback: () => void; disposed: boolean }> = [];
		const agents = [{ pane_id: "self-pane", tab_id: "tab-1", name: "orchestrator", agent: "pi", cwd: root }];
		if (options.liveFresh) agents.push({ pane_id: "fresh-pane", tab_id: "tab-1", name: "agent-fresh", agent: "pi", cwd: root });
		const startEntered = barrier();
		const startReturn = barrier();
		const secondRegistration = barrier();
		let startArgs: string[] = [];
		let startError: string | undefined;
		let persistError = false;
		let journalError = false;
		let stopped = false;
		let scans = 0;
		let transport!: MailboxTransport;
		const write = () => fs.writeFileSync(sessionFile, entries.map((entry) => JSON.stringify(entry) + "\n").join(""));
		const ctx: any = { mode: "tui", cwd: root, hasUI: true, model: { provider: "test", id: "model" },
			signal: new AbortController().signal, isIdle: () => queued.length === 0,
			sessionManager: { getSessionId: () => "session", getBranch: () => entries, getEntries: () => entries },
			ui: { setStatus() {}, notify(message: string) { warnings.push(message); } } };
		const pi: any = {
			events, registerFlag() {}, getFlag() {}, registerTool() {}, registerCommand() {},
			getActiveTools: () => [], setActiveTools() {},
			on(name: string, handler: any) { handlers.set(name, [...(handlers.get(name) ?? []), handler]); },
			appendEntry(customType: string, data: any) {
				if (journalError && customType === "herdr-worker.lifecycle.v1") throw new Error("journal fault");
				entries.push(structuredClone({ type: "custom", customType, data }));
				if (customType === "herdr-worker.run-registration.v1" && entries.filter((e) => e.customType === customType).length === 2) secondRegistration.release();
				timeline.push(`append:${customType}`);
				if (persistError && customType === "herdr-worker") throw new Error("relationship persistence fault");
				write();
			},
			sendMessage(message: any, options: any) { const delivery = structuredClone({ message, options }); queued.push(delivery); sent.push(delivery); timeline.push("inject"); },
			async exec(_command: string, args: string[], execOptions: { signal: AbortSignal }) {
				calls.push(args);
				const ok = (result: unknown) => ({ code: 0, stdout: JSON.stringify({ result }), stderr: "" });
				if (args[0] === "agent" && args[1] === "get") {
					const agent = agents.find((a) => a.name === args[2] || a.pane_id === args[2]);
					return agent ? ok({ agent }) : { code: 1, stdout: "", stderr: "missing" };
				}
				if (args[0] === "agent" && args[1] === "list") return ok({ agents });
				if (args[0] === "pane" && args[1] === "layout") return ok({ layout: { panes: [] } });
				if (args[0] === "pane" && args[1] === "split") {
					options.splitEntered?.release();
					if (options.splitGate) await bounded(options.splitGate.promise, "split barrier");
					if (options.splitError) throw new Error("split failed");
					return ok({ pane: options.missingPane ? {} : { pane_id: "fresh-pane" } });
				}
				if (args[0] === "agent" && args[1] === "start") {
					startArgs = args;
					startEntered.release();
					const abort = () => startReturn.release();
					execOptions.signal.addEventListener("abort", abort, { once: true });
					try { await bounded(startReturn.promise, "mock agent start release"); }
					finally { execOptions.signal.removeEventListener("abort", abort); }
					if (execOptions.signal.aborted) throw new Error("provider lifetime aborted");
					if (startError) throw new Error(startError);
					agents.push({ pane_id: "fresh-pane", tab_id: "tab-1", name: args[2], agent: "pi", cwd: root });
					return ok({});
				}
				if (args[0] === "agent" && args[1] === "prompt") { timeline.push("brief"); return ok({}); }
				if (args[0] === "pane" && args[1] === "report-metadata") return ok({});
				throw new Error(`Unexpected Herdr command ${args.join(" ")}`);
			},
		};
		const schedule = (callback: () => void) => { const s = { callback, disposed: false }; scheduled.push(s); return () => { s.disposed = true; }; };
		extension(pi, { isListening: () => true, createMailbox(callbacks, defaults) {
			transport = createMailboxTransport({ ...callbacks, async deliver(env, id) {
				await options.beforeDeliver?.(env, id);
				return callbacks.deliver(env, id);
			} }, { ...defaults, root,
				watch: (_dir, callback) => ({ close: schedule(callback) }), schedulePoll: (callback) => ({ dispose: schedule(callback) }),
				fileSystem: { readdir(dir) { scans++; return fs.readdirSync(dir); } },
				observeBoundary(boundary) { if (boundary.name === "after-rename" && boundary.paneId === "fresh-pane") timeline.push("brief"); },
			});
			return transport;
		} });
		const hook = async (name: string) => { for (const handler of handlers.get(name) ?? []) await handler({ reason: "startup" }, ctx); };
		const h = {
			entries, timeline, events, queued, sent, warnings, calls, agents, scheduled, ctx, write, hook,
			get transport() { return transport; }, get scans() { return scans; }, startEntered, startReturn, secondRegistration,
			get runId() { return startArgs[startArgs.indexOf("--worker-run-id") + 1]; },
			failStart(error?: string) { startError = error; }, failPersist() { persistError = true; },
			failJournal(value: boolean) { journalError = value; },
			async spawn(initialPrompt?: string, signal?: AbortSignal) {
				const client = createWorkerRpcClient({ events });
				const provider = await client.probe({ timeoutMs: 2000 });
				return client.spawn({ name: "fresh", ...(initialPrompt ? { initialPrompt } : {}) }, provider, { timeoutMs: 6000, signal });
			},
			async ack() {
				while (queued.length) entries.push({ type: "custom_message", ...queued.shift().message });
				write();
				await hook("context");
			},
			async query(runId = startArgs[startArgs.indexOf("--worker-run-id") + 1]) {
				const client = createRunQueryClient({ events });
				const provider = await client.probe({ timeoutMs: 2000 });
				return { record: await client.get({ runId }, provider, { timeoutMs: 2000 }),
					replay: await client.replay({ runId, afterAcceptedSequence: 0 }, provider, { timeoutMs: 2000 }) };
			},
			async shutdown() { if (!stopped) { stopped = true; await hook("session_shutdown"); assert.ok(scheduled.every((s) => s.disposed)); } },
		};
		hosts.push(h);
		await bounded(hook("session_start"), "host startup");
		return h;
	}
	return { root, sessionFile, host, barrier,
		track<T>(operation: Promise<T>) { operations.push(operation); void operation.catch(() => {}); return operation; },
		publish(h: Awaited<ReturnType<typeof host>>, envelope = ready(h.runId), filename = "000000000000001-ready.json") {
			const full = path.join(inboxDir(root, "self-pane"), filename);
			const publisher = createMailboxTransport({ selfId: () => "fixture-writer", isInteractive: () => false, isStopped: () => false,
				async deliver() {}, getAcknowledgedEntries: () => [], handleAcknowledged: () => false, warn(error) { throw error; },
			}, { root, paneId: "fixture-writer", makeFilename: () => filename });
			publisher.writeEnvelope("self-pane", envelope);
			return full;
		},
	};
}

function ready(runId: string): any {
	return { type: "lifecycle", ts: 1, from: { id: "agent-fresh", paneId: "fresh-pane", role: "worker" },
		report: { protocol: 2, runId, eventId: "original-ready", sourceInstanceId: "fresh-source", sourceSequence: 1,
			observedAt: 1, status: "started", evidence: { kind: "worker_ready", readiness: "confirmed" } } };
}

for (const initialPrompt of [undefined, "Your initial brief"]) {
	test(`early fresh readiness survives startup and is accepted once (${initialPrompt ? "with brief" : "no prompt"})`, { timeout: 10000 }, async (t) => {
		const f = await fixture(t);
		const h = await f.host();
		const canonical: any[] = [];
		const projection: any[] = [];
		const off = h.events.on(LIFECYCLE_CHANNELS.lifecycle, (event) => canonical.push(event));
		const offProjection = h.events.on(LIFECYCLE_CHANNELS.started, (event) => projection.push(event));
		t.after(() => { off(); offProjection(); });
		const spawning = f.track(h.spawn(initialPrompt));
		await bounded(h.startEntered.promise, "start entered");
		const full = f.publish(h);
		const bytes = fs.readFileSync(full, "utf8");
		await bounded(h.transport.drainInbox(), "early drain");
		assert.equal(fs.existsSync(full), true, "authorized early readiness file must survive the startup drain");
		await bounded(h.transport.drainInbox(), "repeated early drain");
		assert.equal(fs.readFileSync(full, "utf8"), bytes);
		assert.equal(h.sent.length, 0);
		assert.equal(journals(h.entries).length, 0);
		assert.deepEqual(h.warnings, []);
		h.startReturn.release();
		const result = await bounded(spawning, "spawn result without readiness acknowledgement");
		assert.equal(result.runId, h.runId);
		assert.equal(h.sent.length, 1);
		assert.deepEqual(h.sent[0].message.details.report, ready(h.runId).report);
		assert.deepEqual(h.sent[0].options, { triggerTurn: true, deliverAs: "followUp" });
		assert.equal(journals(h.entries).length, 1);
		assert.equal((await h.query()).record.lifecycle?.readiness, "unconfirmed");
		if (initialPrompt) assert.ok(h.timeline.indexOf("inject") < h.timeline.indexOf("brief"));
		await bounded(h.transport.drainInbox(), "in-flight drain");
		assert.equal(h.sent.length, 1);
		await h.ack();
		assert.equal(fs.existsSync(full), false);
		const { record, replay } = await h.query();
		assert.equal(record.lifecycle?.readiness, "confirmed");
		assert.deepEqual(replay.events.map((event) => [event.evidence.kind, event.acceptedSequence]), [["agent_start_returned", 1], ["worker_ready", 2]]);
		assert.equal(replay.events[1].sourceSequence, 1);
		assert.equal(replay.events[1].eventId, "original-ready");
		assert.deepEqual(canonical, replay.events);
		assert.deepEqual(projection, canonical);
		assert.equal(journals(h.entries).length, 2);
		const reopened = fs.readFileSync(f.sessionFile, "utf8").trim().split("\n").map((line) => JSON.parse(line));
		assert.equal(reopened.filter((entry) => entry.type === "custom_message").length, 1);
		assert.deepEqual(journals(reopened).map((entry) => entry.event), replay.events);
		const publications = h.timeline.filter((entry) => entry === `emit:${LIFECYCLE_CHANNELS.lifecycle}` || entry === `emit:${LIFECYCLE_CHANNELS.started}`);
		assert.deepEqual(publications, [LIFECYCLE_CHANNELS.lifecycle, LIFECYCLE_CHANNELS.started, LIFECYCLE_CHANNELS.lifecycle, LIFECYCLE_CHANNELS.started].map((channel) => `emit:${channel}`));
		assert.ok(h.timeline.indexOf("append:herdr-worker.lifecycle.v1") < h.timeline.indexOf(`emit:${LIFECYCLE_CHANNELS.lifecycle}`));
		f.publish(h, ready(h.runId), "000000000000002-duplicate.json");
		await bounded(h.transport.drainInbox(), "duplicate filename drain");
		await h.ack();
		assert.equal(h.sent.length, 2, "a new filename can inject a duplicate");
		assert.equal(journals(h.entries).length, 2, "event identity deduplicates acceptance");
		off(); offProjection();
	});
}

for (const status of ["completed", "failed"] as const) {
	test(`late readiness and ${status} preserve confirmed readiness`, { timeout: 10000 }, async (t) => {
		const f = await fixture(t);
		const h = await f.host();
		h.startReturn.release();
		await bounded(f.track(h.spawn()), "successful spawn without readiness");
		assert.equal(h.sent.length, 0);
		assert.equal((await h.query()).record.lifecycle?.readiness, "unconfirmed");
		f.publish(h);
		await bounded(h.transport.drainInbox(), "late readiness");
		await h.ack();
		const terminal = ready(h.runId);
		terminal.report = { ...terminal.report, eventId: `terminal-${status}`, sourceSequence: 2, status,
			evidence: status === "completed" ? { kind: "worker_completed_v2", result: "finished" } : { kind: "worker_failed", error: "failure detail" } };
		f.publish(h, terminal, "000000000000002-terminal.json");
		await bounded(h.transport.drainInbox(), "terminal drain");
		await h.ack();
		const { record, replay } = await h.query();
		assert.equal(record.lifecycle?.status, status);
		assert.equal(record.lifecycle?.readiness, "confirmed");
		assert.equal(replay.events.length, 3);
	});
}

test("startup retention rejects wrong identities and early non-readiness without blocking established traffic", { timeout: 10000 }, async (t) => {
	const f = await fixture(t);
	const h = await f.host();
	const spawning = f.track(h.spawn());
	await bounded(h.startEntered.promise, "start held");
	const good = f.publish(h);
	const queued = f.track(h.spawn());
	// RPC registration occurs before serialized provider creation. Wait for it explicitly.
	await bounded(h.secondRegistration.promise, "queued run registration");
	const registrations = h.entries.filter((entry) => entry.customType === "herdr-worker.run-registration.v1");
	const queuedRunId = registrations.at(-1)!.data.runId;
	assert.notEqual(queuedRunId, h.runId);
	const wrongRun = ready("unknown-run");
	const wrongPane = ready(h.runId); wrongPane.from.paneId = "wrong-pane";
	const wrongProtocol = ready(h.runId); wrongProtocol.report.protocol = 1;
	const malformed = ready(h.runId); delete malformed.report.sourceSequence;
	const badSender = ready(h.runId); badSender.from.id = 42;
	const terminal = ready(h.runId); terminal.report.status = "completed"; terminal.report.evidence = { kind: "worker_completed_v2", result: "too early" };
	const ordinary = { type: "message", ts: 1, from: ready(h.runId).from, message: "too early", priority: false };
	const invalid = [wrongRun, wrongPane, wrongProtocol, malformed, badSender, terminal, ordinary, ready(queuedRunId)];
	const files = invalid.map((env, index) => f.publish(h, env, `000000000000002-invalid-${index}.json`));
	// Seed an established relationship via the same restoration hook used for tree changes.
	h.agents.push({ pane_id: "established-pane", tab_id: "tab-1", name: "agent-established", agent: "pi", cwd: f.root });
	h.entries.push({ type: "custom", customType: "herdr-worker", data: { version: 1, sessionId: "session", workers: ["agent-established"] } });
	await h.hook("session_tree");
	f.publish(h, { ...ordinary, from: { id: "agent-established", paneId: "established-pane", role: "worker" } }, "000000000000003-established.json");
	await bounded(h.transport.drainInbox(), "mixed snapshot");
	assert.equal(fs.existsSync(good), true);
	assert.ok(files.every((file) => !fs.existsSync(file)));
	assert.equal(h.sent.length, 1);
	assert.equal(h.sent[0].message.customType, "herdr-worker.message");
	assert.equal(journals(h.entries).length, 0);
	h.startReturn.release();
	await bounded(spawning, "first spawn");
	// The next fresh spawn uses the existing worker name suffix, not the retained first run.
	await bounded(queued, "queued spawn");
	assert.equal(h.calls.filter((args) => args[0] === "agent" && args[1] === "start").length, 2);
});

for (const error of ["start rejected", "provider timeout"]) {
	test(`${error} closes startup retention and preserves uncertainty; queue continues`, { timeout: 10000 }, async (t) => {
		const f = await fixture(t);
		const h = await f.host();
		h.failStart(error);
		const spawning = f.track(h.spawn());
		await bounded(h.startEntered.promise, "held failing start");
		const full = f.publish(h);
		await bounded(h.transport.drainInbox(), "early failure drain");
		assert.equal(fs.existsSync(full), true);
		const next = f.track(h.spawn());
		await bounded(h.secondRegistration.promise, "next run queued behind failed start");
		h.startReturn.release();
		await assert.rejects(bounded(spawning, "failed spawn result"), /The worker operation failed/);
		await bounded(h.transport.drainInbox(), "post-failure drain");
		assert.equal(fs.existsSync(full), false);
		assert.equal(h.sent.length, 0);
		const { record, replay } = await h.query();
		assert.equal(record.lifecycle?.status, "uncertain");
		assert.deepEqual(replay.events.map((event) => [event.status, event.evidence.kind === "uncertain" ? event.evidence.scope : undefined]), [["uncertain", "agent_start"]]);
		assert.ok(h.calls.every((args) => !["close", "kill", "stop"].includes(args[1])));
		h.failStart();
		await bounded(next, "next queued spawn after failure");
	});
}

test("relationship persistence failure keeps its error and removes retention permission despite memory mutation", { timeout: 10000 }, async (t) => {
	const f = await fixture(t);
	const h = await f.host();
	const spawning = f.track(h.spawn());
	await bounded(h.startEntered.promise, "held start");
	const full = f.publish(h);
	await bounded(h.transport.drainInbox(), "early persistence-fault drain");
	assert.equal(fs.existsSync(full), true);
	h.failPersist();
	h.startReturn.release();
	await assert.rejects(bounded(spawning, "persistence failure"), /The worker operation failed/);
	assert.ok(h.entries.some((entry) => entry.customType === "herdr-worker" && entry.data.workers.includes("agent-fresh")));
	assert.equal((await h.query()).record.lifecycle?.status, "uncertain");
	await bounded(h.transport.drainInbox(), "normal delivery under surviving memory relationship");
	assert.equal(h.sent.length, 1, "normal peer and uncertainty binding permit actual readiness, not retry authority");
	await h.ack();
	assert.equal(fs.existsSync(full), false);
	assert.equal((await h.query()).record.lifecycle?.readiness, "confirmed");
	assert.deepEqual(journals(h.entries).map((entry) => entry.event.evidence.kind), ["uncertain", "worker_ready"]);
});

test("provider journal failure after binding does not fail spawn or bypass acknowledgement", { timeout: 10000 }, async (t) => {
	const f = await fixture(t);
	const h = await f.host();
	const spawning = f.track(h.spawn());
	await bounded(h.startEntered.promise, "held start");
	const full = f.publish(h);
	await bounded(h.transport.drainInbox(), "early journal-fault drain");
	h.failJournal(true);
	h.startReturn.release();
	await bounded(spawning, "spawn despite journal failure");
	assert.equal(h.sent.length, 1);
	assert.equal(journals(h.entries).length, 0);
	assert.equal(fs.existsSync(full), true);
	h.failJournal(false);
	await h.ack();
	assert.equal(journals(h.entries).length, 1);
	assert.equal((await h.query()).record.lifecycle?.readiness, "confirmed");
});

for (const movement of ["live peer moved", "sender alias and mismatched bound pane"]) {
	test(`retained readiness rechecks authority: ${movement}`, { timeout: 10000 }, async (t) => {
		const f = await fixture(t);
		const entered = f.barrier();
		const release = f.barrier();
		let postBinding = false;
		const h = await f.host({ beforeDeliver: async () => { if (postBinding) { entered.release(); await bounded(release.promise, "authority change"); } } });
		const spawning = f.track(h.spawn());
		await bounded(h.startEntered.promise, "held start");
		const full = f.publish(h);
		await bounded(h.transport.drainInbox(), "retention scan");
		assert.equal(fs.existsSync(full), true);
		postBinding = true;
		h.startReturn.release();
		await bounded(entered.promise, "post-binding retry");
		if (movement === "live peer moved") h.agents.find((agent) => agent.name === "agent-fresh")!.pane_id = "moved-pane";
		else {
			// Keep the original bound pane a known peer through a different name.
			h.agents.find((agent) => agent.name === "agent-fresh")!.pane_id = "moved-pane";
			h.agents.push({ pane_id: "fresh-pane", tab_id: "tab-1", name: "agent-established", agent: "pi", cwd: f.root });
			h.entries.push({ type: "custom", customType: "herdr-worker", data: { version: 1, sessionId: "session", workers: ["agent-fresh", "agent-established"] } });
			await h.hook("session_tree");
			// Bind mismatch is separately exercised after this old retained file is accepted normally.
		}
		release.release();
		await bounded(spawning, "spawn after authority change");
		if (movement === "live peer moved") {
			assert.equal(fs.existsSync(full), false);
			assert.equal(h.sent.length, 0);
		} else {
			assert.equal(h.sent.length, 1, "sender ID aliases remain unchanged when the exact bound pane is still a known peer");
			await h.ack();
			const wrong = ready(h.runId); wrong.from.paneId = "moved-pane";
			const wrongFile = f.publish(h, wrong, "000000000000002-wrong-pane.json");
			await bounded(h.transport.drainInbox(), "live but mismatched bound pane");
			assert.equal(fs.existsSync(wrongFile), false);
			assert.equal(h.sent.length, 1);
		}
	});
}

for (const shutdown of [false, true]) {
	test(`startup post-current scan survives an active drain${shutdown ? " and cancels on shutdown" : " before sending the brief"}`, { timeout: 10000 }, async (t) => {
		const f = await fixture(t);
		const blocked = f.barrier();
		const release = f.barrier();
		const h = await f.host({ beforeDeliver: async (_env, id) => {
			if (id.endsWith("blocker.json")) { blocked.release(); await bounded(release.promise, "blocked scan release"); }
		} });
		const spawning = f.track(h.spawn("brief"));
		await bounded(h.startEntered.promise, "start entered");
		const full = f.publish(h);
		await bounded(h.transport.drainInbox(), "early retention");
		const blocker = f.publish(h, { type: "invalid", from: { id: "unknown", paneId: "unknown" } }, "000000000000002-blocker.json");
		const active = h.transport.drainInbox();
		await bounded(blocked.promise, "active snapshot blocker");
		const before = h.scans;
		const providerStarted = f.barrier();
		const off = h.events.on(LIFECYCLE_CHANNELS.started, () => providerStarted.release());
		h.startReturn.release();
		await bounded(providerStarted.promise, "provider bound before retry request");
		off();
		const retry1 = h.transport.drainInboxAfterCurrent();
		const retry2 = h.transport.drainInboxAfterCurrent();
		assert.equal(retry1, retry2, "concurrent post-current requests coalesce");
		for (const schedule of h.scheduled) schedule.callback();
		await bounded(h.transport.drainInbox(), "ordinary overlapping call");
		assert.equal(h.scans, before);
		assert.equal(h.sent.length, 0);
		if (shutdown) await h.shutdown();
		release.release();
		await bounded(Promise.all([active, retry1, retry2]), "active and requested scans settle");
		if (shutdown) {
			await assert.rejects(bounded(spawning, "shutdown RPC"));
			assert.equal(h.scans, before);
			assert.equal(h.sent.length, 0);
			assert.equal(fs.existsSync(full), true);
			assert.equal(fs.existsSync(blocker), true);
			assert.deepEqual(journals(h.entries).map((j) => j.event.status), ["started", "uncertain"]);
		} else {
			await bounded(spawning, "spawn after queued retry");
			assert.equal(h.scans, before + 1);
			assert.equal(h.sent.length, 1);
			assert.ok(h.timeline.indexOf("inject") < h.timeline.indexOf("brief"));
			await h.ack();
			assert.equal((await h.query()).record.lifecycle?.readiness, "confirmed");
		}
	});
}

for (const outcome of ["retry", "throw", "stop", "non-interactive"] as const) {
	test(`transport post-current coalescing after ${outcome}`, { timeout: 5000 }, async (t) => {
		const f = await fixture(t);
		const entered = f.barrier();
		const release = f.barrier();
		let interactive = true;
		let scans = 0;
		let deliveries = 0;
		const warnings: unknown[] = [];
		const transport = createMailboxTransport({
			selfId: () => "self", isInteractive: () => interactive, isStopped: () => false,
			getAcknowledgedEntries: () => [], handleAcknowledged: () => false, warn: (error) => { warnings.push(error); },
			async deliver() {
				deliveries++;
				if (deliveries === 1) {
					entered.release(); await bounded(release.promise, "raw transport barrier");
					if (outcome === "throw") throw new Error("one-shot delivery failure");
					return "retry";
				}
			},
		}, { root: f.root, paneId: "self", makeFilename: () => "000000000000001-raw.json",
			fileSystem: { readdir(dir) { scans++; return fs.readdirSync(dir); } },
			watch: () => ({ close() {} }), schedulePoll: () => ({ dispose() {} }),
		});
		t.after(() => transport.stopListening());
		transport.writeEnvelope("self", { ts: 1 });
		const file = path.join(inboxDir(f.root, "self"), "000000000000001-raw.json");
		const active = transport.drainInbox();
		await bounded(entered.promise, "raw active scan");
		const queued = transport.drainInboxAfterCurrent();
		assert.equal(queued, transport.drainInboxAfterCurrent());
		await bounded(transport.drainInbox(), "suppressed raw overlap");
		assert.equal(scans, 1);
		if (outcome === "stop") transport.stopListening();
		if (outcome === "non-interactive") interactive = false;
		release.release();
		await bounded(Promise.all([active, queued]), "raw scans");
		const cancelled = outcome === "stop" || outcome === "non-interactive";
		assert.equal(scans, cancelled ? 1 : 2);
		assert.equal(deliveries, cancelled ? 1 : 2);
		assert.equal(fs.existsSync(file), cancelled);
		assert.equal(warnings.length, outcome === "throw" ? 1 : 0);
	});
}

for (const mode of ["retained file", "persisted custom entry", "accepted journal", "old unconfirmed completion"] as const) {
	test(`restart readiness from ${mode}`, { timeout: 10000 }, async (t) => {
		const f = await fixture(t);
		const h = await f.host();
		h.startReturn.release();
		await bounded(f.track(h.spawn()), "bound spawn");
		const runId = h.runId;
		const env = ready(runId);
		if (mode === "old unconfirmed completion") {
			env.report.status = "completed";
			env.report.evidence = { kind: "worker_completed_v2", result: "without readiness" };
		}
		const full = f.publish(h, env);
		if (mode !== "retained file") {
			await bounded(h.transport.drainInbox(), "pre-restart injection");
			if (mode === "persisted custom entry") {
				h.entries.push({ type: "custom_message", ...h.queued.shift().message }); h.write();
			} else await h.ack();
		}
		await h.shutdown();
		const restored = await f.host({ reopen: true, liveFresh: true });
		await bounded(restored.transport.drainInboxAfterCurrent(), "restored scan");
		if (mode === "retained file") {
			assert.equal(restored.sent.length, 1);
			await restored.ack();
		} else assert.equal(restored.sent.length, 0, "surviving entry or journal needs no reinjection");
		const { record, replay } = await restored.query(runId);
		assert.equal(record.lifecycle?.readiness, mode === "old unconfirmed completion" ? "unconfirmed" : "confirmed");
		assert.equal(replay.events.filter((e) => e.evidence.kind === "worker_ready").length, mode === "old unconfirmed completion" ? 0 : 1);
		assert.equal(fs.existsSync(full), false);
		const emitted = restored.timeline.filter((item) => item === `emit:${LIFECYCLE_CHANNELS.lifecycle}`).length;
		assert.equal(emitted, mode === "retained file" || mode === "persisted custom entry" ? 1 : 0, "journals do not republish history");
	});
}

test("interrupted pre-binding startup restores endpoint but never transient retention authority", { timeout: 10000 }, async (t) => {
	const f = await fixture(t);
	const h = await f.host();
	const spawning = f.track(h.spawn());
	await bounded(h.startEntered.promise, "pre-binding start");
	const runId = h.runId;
	const full = f.publish(h);
	await bounded(h.transport.drainInbox(), "pre-binding retention");
	assert.equal(fs.existsSync(full), true);
	// Preserve the exact disk cut before graceful shutdown. No crash or uncertainty is fabricated.
	const interruptedStorage = fs.readFileSync(f.sessionFile);
	await h.shutdown();
	await assert.rejects(bounded(spawning, "interrupted caller"));
	fs.writeFileSync(f.sessionFile, interruptedStorage);
	const restored = await f.host({ reopen: true, liveFresh: true });
	await bounded(restored.transport.drainInboxAfterCurrent(), "unbound restart scan");
	assert.equal(fs.existsSync(full), false);
	assert.equal(restored.sent.length, 0);
	const { record, replay } = await restored.query(runId);
	assert.ok(!("legacy" in record));
	assert.equal(record.endpoint?.paneId, "fresh-pane");
	assert.deepEqual(record.lifecycle, { status: "registered", acceptedSequence: 0, orchestrationGradeCompletion: false });
	assert.deepEqual(replay.events, []);
});

for (const abort of ["before creation", "caller only", "shutdown"] as const) {
	test(`startup abort boundary: ${abort}`, { timeout: 10000 }, async (t) => {
		const f = await fixture(t);
		const h = await f.host();
		const controller = new AbortController();
		if (abort === "before creation") controller.abort();
		const spawning = f.track(h.spawn(undefined, controller.signal));
		if (abort === "before creation") {
			await assert.rejects(bounded(spawning, "pre-aborted request"));
			const full = f.publish(h, ready("never-created"));
			await bounded(h.transport.drainInbox(), "pre-abort unauthorized file");
			assert.equal(fs.existsSync(full), false);
			assert.ok(h.calls.every((args) => args[0] !== "pane" || args[1] !== "split"));
			assert.equal(journals(h.entries).length, 0);
			return;
		}
		await bounded(h.startEntered.promise, "start before abort");
		const full = f.publish(h);
		await bounded(h.transport.drainInbox(), "retention before abort");
		assert.equal(fs.existsSync(full), true);
		if (abort === "shutdown") {
			await h.shutdown();
			await assert.rejects(bounded(spawning, "shutdown reply"));
			await bounded(h.transport.drainInboxAfterCurrent(), "stopped scan");
			assert.equal(fs.existsSync(full), true);
			assert.equal(h.sent.length, 0);
			assert.deepEqual(journals(h.entries).map((j) => [j.event.status, j.event.evidence.kind]), [["uncertain", "uncertain"]]);
		} else {
			controller.abort();
			await assert.rejects(bounded(spawning, "abandoned caller"));
			const started = f.barrier();
			const off = h.events.on(LIFECYCLE_CHANNELS.started, () => started.release());
			h.startReturn.release();
			await bounded(started.promise, "provider continues despite caller abort");
			off();
			await bounded(h.transport.drainInboxAfterCurrent(), "eventual post-binding retry");
			assert.equal(h.sent.length, 1);
			await h.ack();
			assert.equal((await h.query()).record.lifecycle?.readiness, "confirmed");
		}
		assert.ok(h.calls.every((args) => !["close", "kill", "stop"].includes(args[1])));
	});
}

for (const missingPane of [true, false]) {
	test(`pane creation ${missingPane ? "missing identity" : "failure"} grants no readiness permission`, { timeout: 5000 }, async (t) => {
		const f = await fixture(t);
		const splitGate = f.barrier();
		const splitEntered = f.barrier();
		const h = await f.host({ splitGate, splitEntered, missingPane, splitError: !missingPane });
		const spawning = f.track(h.spawn());
		await bounded(splitEntered.promise, "held split");
		const runId = h.entries.find((entry) => entry.customType === "herdr-worker.run-registration.v1").data.runId;
		const full = f.publish(h, ready(runId));
		await bounded(h.transport.drainInbox(), "pane-creation unauthorized readiness");
		assert.equal(fs.existsSync(full), false);
		splitGate.release();
		await assert.rejects(bounded(spawning, "split failure reply"));
		assert.equal(h.sent.length, 0);
		assert.ok(h.calls.every((args) => args[0] !== "agent" || args[1] !== "start"));
		assert.deepEqual(journals(h.entries).map((entry) => entry.event.evidence.kind), ["uncertain"]);
	});
}

test("ordinary drains suppress synchronous reentry from delivery", { timeout: 5000 }, async (t) => {
	const f = await fixture(t);
	let deliveries = 0;
	let reentered: Promise<void> | undefined;
	const transport = createMailboxTransport({ selfId: () => "self", isInteractive: () => true, isStopped: () => false,
		getAcknowledgedEntries: () => [], handleAcknowledged: () => false, warn(error) { throw error; },
		async deliver() { deliveries++; if (deliveries === 1) reentered = transport.drainInbox(); },
	}, { root: f.root, paneId: "self" });
	transport.writeEnvelope("self", { ts: 1 });
	await bounded(transport.drainInbox(), "synchronously reentered drain");
	await bounded(reentered!, "suppressed reentry");
	assert.equal(deliveries, 1);
});
