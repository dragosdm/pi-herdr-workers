import assert from "node:assert/strict";
import { spawn } from "node:child_process";
import * as fs from "node:fs";
import * as path from "node:path";
import test, { type TestContext } from "node:test";
import { createWorkerRpcClient } from "../../rpc/client.js";
import { inboxDir, mailboxDir } from "../../mailbox/paths.js";
import { LIFECYCLE_JOURNAL_ENTRY } from "../../lifecycle/acceptor.js";
import type { ControlledPiHost } from "../support/controlled-pi-host.js";
import { allMailboxCases, mailboxCaseCells, mailboxCaseName, mailboxCases, type MailboxCase, type MailboxCaseId } from "../support/mailbox-cases.js";
import { bounded, mailboxFixture, oneShotRemoveFault } from "../support/mailbox-fixture.js";
import { lifecycleMailboxCases, type LifecycleMailboxCaseId } from "../support/mailbox-cases.js";
import { journals, lifecycleEnvelope, queryLifecycle, recoveryRunId, seedLifecycleAuthority, selectedTeamEntry, teamState } from "../support/mailbox-lifecycle.js";

process.env.HERDR_ENV = "1";
process.env.HERDR_PANE_ID = "self-pane";
process.env.HERDR_TAB_ID = "tab-1";

const envelopeId = "000000000000001-ordinary.json";
const ordinary = {
	type: "message",
	from: { id: "agent-scout", paneId: "worker-pane", name: "agent-scout", role: "worker" },
	message: "Report from the mailbox",
	priority: false,
	ts: 1,
};

const lifecycle = {
	type: "lifecycle", ts: 1, from: ordinary.from,
	report: { protocol: 2, runId: "mailbox-run", eventId: "mailbox-event", sourceInstanceId: "worker-source",
		sourceSequence: 1, observedAt: 1, status: "completed", evidence: { kind: "worker_completed_v2", result: "Finished" } },
};

function saveRunMetadata(h: ControlledPiHost) {
	h.append({ type: "custom", customType: "herdr-worker", data: {
		version: 1, sessionId: "session", workers: ["agent-scout"],
		meta: { "agent-scout": { runId: lifecycle.report.runId, paneId: "worker-pane", lifecycleProtocol: 2 } },
	} });
	h.write();
}

function messageIds(h: ControlledPiHost) {
	return h.sentMessages.map((delivery) => (delivery.message.details as { envelopeId: string }).envelopeId);
}

function assertNoLifecycle(h: ControlledPiHost) {
	assert.equal(h.entries.filter((entry) => entry.customType === LIFECYCLE_JOURNAL_ENTRY).length, 0);
	assert.equal(h.timeline.filter((event) => event === "emit:herdr-workers:lifecycle").length, 0);
}

function recordOrdinary(h: ControlledPiHost, id: string) {
	return h.append({ type: "custom_message", customType: "herdr-worker.message", content: "Recorded message", display: true,
		details: { envelopeId: id, from: ordinary.from } });
}

function assertInside(root: string, file: string) {
	const relative = path.relative(root, file);
	assert.ok(relative && relative !== ".." && !relative.startsWith(`..${path.sep}`) && !path.isAbsolute(relative), file);
}

async function realWatchDelivery(t: TestContext) {
	const f = mailboxFixture(t);
	const sent = f.gate();
	const ready = f.gate();
	const readinessName = ".watch-ready";
	let watchAttempts = 0;
	const watchEvents: Array<{ event: string; filename: string | null; published: boolean }> = [];
	const watchErrors: string[] = [];
	let rejectWatch!: (error: Error) => void;
	const failed = new Promise<never>((_resolve, reject) => { rejectWatch = reject; });
	void failed.catch(() => {});
	let published = false;
	let readinessWritten = false;
	let readinessSeen = false;
	let deliveryNotifications = 0;
	let pollCalls = 0;
	const snapshots: string[][] = [];
	function onWatchError(error: Error) { watchErrors.push(String(error)); rejectWatch(error); }
	const h = await f.host({ transport: {
		makeFilename: () => envelopeId,
		fileSystem: { readdir(dir) {
			const files = fs.readdirSync(dir);
			snapshots.push(files);
			return files;
		} },
		watch(dir, callback) {
			watchAttempts++;
			const { watcher } = f.nativeWatch(dir, (event, filename) => {
				watchEvents.push({ event, filename, published });
				if (readinessWritten && filename === readinessName) { readinessSeen = true; ready.release(); }
				// Only the final envelope's own notification may drive the asserted delivery.
				if (published && filename === envelopeId) { deliveryNotifications++; callback(); }
			});
			watcher.on("error", onWatchError);
			return watcher;
		},
		schedulePoll: () => ({ dispose() {} }),
	}, prepare(host) { host.observeSend(() => sent.release()); }, beforeDeliver: async () => {
		assert.equal(readinessSeen, true);
		assert.ok(deliveryNotifications > 0);
	} });
	try {
		assert.equal(watchAttempts, 1);
		assert.equal(h.transport.isStarted(), true);
		assert.equal(h.transport.isListening("self-pane"), true);
		assert.deepEqual(snapshots, [[]]);
		const poll = f.scheduled.find((record) => record.kind === "poll")!;
		poll.callback = () => { pollCalls++; assert.fail("Watcher delivery must not invoke polling"); };
		// Darwin libuv close synchronizes a reschedule containing the still-open primary watcher.
		// A single marker write without this boundary has the same registration race as startup delivery.
		const auxiliary = f.nativeWatch(f.inbox(), () => {});
		auxiliary.watcher.on("error", onWatchError);
		auxiliary.watcher.close();
		await bounded(Promise.race([auxiliary.closed, failed]), "auxiliary watcher closure");
		readinessWritten = true;
		fs.writeFileSync(path.join(f.inbox(), readinessName), "ready");
		await bounded(Promise.race([ready.promise, failed]), "named primary watcher readiness event");
		assert.equal(h.sentMessages.length, 0);
		assert.deepEqual(snapshots, [[]], "readiness events must not drive mailbox scans");
		h.transport.writeEnvelope("self-pane", ordinary);
		published = true;
		await bounded(Promise.race([sent.promise, failed]), "final-envelope watcher delivery without polling");
	} catch (error) {
		t.diagnostic(JSON.stringify({ watchAttempts, watchEvents, watchErrors, readinessSeen, deliveryNotifications, pollCalls,
			files: fs.readdirSync(f.inbox()), snapshots, timeline: h.timeline, warnings: h.warnings }));
		throw error;
	}
	assert.equal(pollCalls, 0);
	assert.deepEqual(watchErrors, []);
	assert.ok(deliveryNotifications > 0);
	assert.deepEqual(messageIds(h), [envelopeId]);
	assert.equal(h.queued.length, 1);
	assert.deepEqual(JSON.parse(fs.readFileSync(path.join(f.inbox(), envelopeId), "utf8")), ordinary);
	assert.equal(fs.readFileSync(path.join(f.inbox(), readinessName), "utf8"), "ready", "no readiness cleanup stimulus before delivery");
	await f.dispose();
	assert.equal(fs.existsSync(f.marker()), false);
	assert.ok(f.scheduled.every((record) => record.disposed));
}

async function acknowledgementCase(t: TestContext, hook: "context" | "agent_settled") {
	const f = mailboxFixture(t);
	const h = await f.host({ transport: { makeFilename: () => envelopeId } });
	const full = path.join(f.inbox(), envelopeId);
	h.transport.writeEnvelope("self-pane", ordinary);
	await h.transport.drainInbox();
	assert.equal(h.sentMessages.length, 1);
	await h.hook(hook);
	assert.equal(fs.existsSync(full), true, "a queued message is not an acknowledgement");
	const delivery = h.consume();
	await h.hook("message_end", { message: delivery.message });
	assert.equal(fs.existsSync(full), true);
	assert.equal(h.entries.filter((entry) => entry.type === "custom_message").length, 0);
	const entry = h.appendMessage(delivery);
	assert.deepEqual(h.reopen(), [], "memory append does not populate the test file");
	h.write();
	assert.deepEqual(h.reopen().filter((entry) => entry.type === "custom_message"), [entry]);
	assert.deepEqual(entry.details, { envelopeId, from: ordinary.from });
	assert.equal(fs.existsSync(full), true, "the file remains until the acknowledgement hook");
	await h.transport.drainInbox();
	assert.equal(fs.existsSync(full), true, "in-flight lookup precedes acknowledged-entry lookup");
	await h.hook(hook);
	assert.equal(fs.existsSync(full), false);
	await h.hook(hook);
	await h.transport.drainInbox();
	assert.equal(h.sentMessages.length, 1);
	assert.equal(h.queued.length, 0);
	assert.deepEqual(h.reopen().filter((entry) => entry.type === "custom_message"), [entry]);
	assert.deepEqual(f.boundaries.map((boundary) => boundary.name), [
		"before-write", "after-temp-write", "after-rename", "after-parse", "before-delete", "after-delete",
	]);
	assert.ok(h.timeline.indexOf("write:session") < h.timeline.indexOf("mailbox:before-delete"));
	assert.equal(h.warnings.length, 0);
}

const bodies: Record<MailboxCaseId, (t: TestContext) => Promise<void>> = {
	async "ordinary-publication-receipt"(t) {
		const f = mailboxFixture(t);
		f.listen("worker-pane");
		let receiptReturned = false;
		let temporaryContent: string | undefined;
		const h = await f.host({ transport: { observeBoundary(boundary) {
			assert.equal(receiptReturned, false, "publication must finish before the send receipt");
			assert.equal(boundary.paneId, "worker-pane");
			assert.match(boundary.envelopeId, /^\d{15}-[a-z0-9]{0,6}\.json$/);
			assert.equal(boundary.temporaryPath, path.join(f.inbox("worker-pane"), `.${boundary.envelopeId}.tmp`));
			if (boundary.name === "before-write") {
				assert.equal(fs.existsSync(boundary.fullPath), false);
				assert.equal(fs.existsSync(boundary.temporaryPath!), false);
			} else if (boundary.name === "after-temp-write") {
				assert.equal(fs.existsSync(boundary.fullPath), false);
				temporaryContent = fs.readFileSync(boundary.temporaryPath!, "utf8");
				assert.equal(fs.statSync(boundary.temporaryPath!).mode & 0o777, 0o600);
				assert.deepEqual(fs.readdirSync(f.inbox("worker-pane")), [`.${boundary.envelopeId}.tmp`]);
			} else if (boundary.name === "after-rename") {
				assert.equal(fs.existsSync(boundary.temporaryPath!), false);
				assert.equal(fs.readFileSync(boundary.fullPath, "utf8"), temporaryContent);
			}
		} } });
		const client = createWorkerRpcClient({ events: h.events });
		const provider = await client.probe({ timeoutMs: 1000 });
		assert.equal(provider.available, true);
		const receipt = await client.send({ target: "agent-scout", message: "Published through the real service", mode: "follow-up" }, provider, { timeoutMs: 1000 });
		receiptReturned = true;
		h.timeline.push("receipt:send");
		assert.deepEqual(receipt, {
			target: "agent-scout", paneId: "worker-pane", kind: "pi", status: "idle",
			transport: "inbox", requestedMode: "follow-up", priorityApplied: false,
		});
		assert.ok(temporaryContent);
		const envelope = JSON.parse(temporaryContent);
		assert.equal(typeof envelope.ts, "number");
		assert.deepEqual(envelope, {
			type: "message", from: { id: "orchestrator", paneId: "self-pane", name: "orchestrator", role: "orchestrator" },
			message: "Published through the real service", priority: false, ts: envelope.ts,
		});
		assert.deepEqual(f.boundaries.map((boundary) => boundary.name), ["before-write", "after-temp-write", "after-rename"]);
		assert.deepEqual(fs.readdirSync(f.inbox("worker-pane")), [f.boundaries[0].envelopeId]);
		assert.ok(h.timeline.indexOf("mailbox:after-rename") < h.timeline.indexOf("receipt:send"));
		assert.equal(h.sentMessages.length, 0);
		assert.equal(h.events.listenerCount(), 11, "RPC reply subscriptions are disposed");
		assert.ok(h.execCalls.every((args) => args[0] === "agent" && args[1] === "get"), "Herdr only resolves identities");
	},
	async "ordinary-queued-retention"(t) {
		const f = mailboxFixture(t);
		const h = await f.host({ transport: { makeFilename: () => envelopeId } });
		h.transport.writeEnvelope("self-pane", ordinary);
		for (let i = 0; i < 3; i++) {
			await h.transport.drainInbox();
			await h.hook("context");
			await h.hook("agent_settled");
		}
		assert.equal(h.sentMessages.length, 1);
		assert.equal(h.queued.length, 1);
		assert.equal(h.sentMessages[0].message.customType, "herdr-worker.message");
		assert.equal(h.sentMessages[0].message.display, true);
		assert.match(h.sentMessages[0].message.content, /Worker "agent-scout" in pane worker-pane/);
		assert.ok(h.sentMessages[0].message.content.includes(ordinary.message));
		assert.deepEqual(h.sentMessages[0].message.details, { envelopeId, from: ordinary.from });
		assert.deepEqual(h.sentMessages[0].options, { triggerTurn: true, deliverAs: "followUp" });
		assert.equal(h.entries.filter((entry) => entry.type === "custom_message").length, 0);
		assert.deepEqual(h.reopen(), []);
		assert.deepEqual(JSON.parse(fs.readFileSync(path.join(f.inbox(), envelopeId), "utf8")), ordinary);
		assert.equal(f.boundaries.filter((boundary) => boundary.name === "after-parse").length, 1);
		assert.equal(f.boundaries.some((boundary) => boundary.name === "before-delete"), false);
	},
	"ordinary-context-ack": (t) => acknowledgementCase(t, "context"),
	"ordinary-agent-settled-ack": (t) => acknowledgementCase(t, "agent_settled"),
	async "ordinary-fresh-receiver-ack"(t) {
		const f = mailboxFixture(t);
		const first = await f.host({ transport: { makeFilename: () => envelopeId } });
		first.transport.writeEnvelope("self-pane", ordinary);
		await first.transport.drainInbox();
		const entry = first.appendMessage(first.consume());
		first.write();
		assert.equal(first.sentMessages.length, 1);
		await first.shutdown();
		const full = path.join(f.inbox(), envelopeId);
		assert.equal(fs.existsSync(full), true);
		// An acknowledged filename does not require its current payload to be readable JSON.
		fs.writeFileSync(full, "not JSON");
		const reads: string[] = [];
		const boundaryCount = f.boundaries.length;
		const replacement = await f.host({ reopen: true, transport: { fileSystem: {
			read(file) { reads.push(file); return fs.readFileSync(file, "utf8"); },
		} } });
		await replacement.transport.drainInbox();
		assert.deepEqual(replacement.entries.filter((entry) => entry.type === "custom_message"), [entry]);
		assert.deepEqual(replacement.reopen().filter((entry) => entry.type === "custom_message"), [entry]);
		assert.notEqual(replacement.entries, first.entries);
		assert.equal(replacement.sentMessages.length, 0);
		assert.equal(replacement.queued.length, 0);
		assert.equal(reads.includes(full), false);
		assert.equal(fs.existsSync(full), false);
		assert.deepEqual(f.boundaries.slice(boundaryCount).map((boundary) => boundary.name), ["before-delete", "after-delete"]);
	},
	async "ordinary-headless-listener-ownership"(t) {
		const f = mailboxFixture(t);
		const owner = await f.host();
		const marker = fs.readFileSync(f.marker(), "utf8");
		assert.equal(JSON.parse(marker).pid, process.pid);
		assert.equal(owner.transport.isListening("self-pane"), true);
		assert.deepEqual(f.scheduled.map((record) => record.kind), ["watch", "poll"]);
		const headless = await f.host({ mode: "rpc" });
		assert.equal(headless.transport.isStarted(), false);
		await headless.shutdown();
		assert.equal(fs.readFileSync(f.marker(), "utf8"), marker);
		assert.equal(owner.transport.isStarted(), true);
		assert.equal(f.scheduled.length, 2);
		assert.ok(f.scheduled.every((record) => !record.disposed));
		await f.dispose();
		assert.equal(owner.events.listenerCount(), 0);
		assert.equal(headless.events.listenerCount(), 0);
		assert.ok(f.scheduled.every((record) => record.disposed));
		assert.equal(fs.existsSync(f.marker()), false);
		assert.equal(fs.existsSync(f.root), false);
	},
	async "snapshot-order-overlap"(t) {
		const f = mailboxFixture(t);
		const entered = f.gate();
		const release = f.gate();
		const ids = ["000000000000001-a.json", "000000000000001-b.json", "000000000000002-a.json"];
		const late = "000000000000000-late.json";
		const filenames = [ids[2], ids[1], ids[0], late];
		let snapshots = 0;
		const reached: string[] = [];
		const h = await f.host({ beforeDeliver: async (_envelope, id) => {
			reached.push(id);
			if (id === ids[0]) { entered.release(); await release.promise; }
		}, transport: {
			makeFilename: () => filenames.shift()!,
			fileSystem: { readdir(dir) { snapshots++; return fs.readdirSync(dir); } },
		} });
		snapshots = 0;
		for (let i = 0; i < 3; i++) h.transport.writeEnvelope("self-pane", ordinary);
		const draining = h.transport.drainInbox();
		await bounded(entered.promise, "first blocked delivery");
		for (const record of f.scheduled) record.callback();
		h.transport.writeEnvelope("self-pane", ordinary);
		for (const record of f.scheduled) record.callback();
		assert.equal(snapshots, 1, "overlapping callbacks must not start another snapshot");
		assert.deepEqual(reached, [ids[0]]);
		release.release();
		await bounded(draining, "sorted snapshot drain");
		assert.deepEqual(messageIds(h), ids);
		assert.deepEqual(reached, ids);
		assert.equal(snapshots, 1);
		await h.transport.drainInbox();
		assert.equal(snapshots, 2);
		assert.deepEqual(messageIds(h), [...ids, late]);
		assert.equal(h.queued.length, 4);
		assert.deepEqual(fs.readdirSync(f.inbox()).sort(), [late, ...ids]);
	},
	"real-watch-delivery": realWatchDelivery,
	async "startup-scan-without-notifications"(t) {
		const f = mailboxFixture(t);
		const sent = f.gate();
		fs.mkdirSync(f.inbox(), { recursive: true });
		fs.writeFileSync(path.join(f.inbox(), envelopeId), JSON.stringify(ordinary));
		const h = await f.host({ prepare(host) { host.observeSend(() => sent.release()); } });
		await bounded(sent.promise, "startup scan with no watch or poll callback");
		assert.deepEqual(messageIds(h), [envelopeId]);
		assert.equal(h.queued.length, 1);
		assert.deepEqual(fs.readdirSync(f.inbox()), [envelopeId]);
		assert.deepEqual(f.scheduled.map((record) => record.kind), ["watch", "poll"]);
	},
	async "watch-failure-poll-fallback"(t) {
		const f = mailboxFixture(t);
		const sent = f.gate();
		let watchAttempts = 0;
		const h = await f.host({ transport: { makeFilename: () => envelopeId,
			watch() { watchAttempts++; throw new Error("Injected watch creation failure"); },
		}, prepare(host) { host.observeSend(() => sent.release()); } });
		assert.equal(watchAttempts, 1);
		assert.equal(h.transport.isStarted(), true);
		assert.equal(h.transport.isListening("self-pane"), true);
		h.transport.writeEnvelope("self-pane", ordinary);
		assert.equal(h.sentMessages.length, 0);
		assert.deepEqual(f.scheduled.map((record) => record.kind), ["poll"]);
		f.scheduled[0].callback();
		await bounded(sent.promise, "poll fallback delivery");
		assert.deepEqual(messageIds(h), [envelopeId]);
		assert.equal(h.queued.length, 1);
		assert.deepEqual(JSON.parse(fs.readFileSync(path.join(f.inbox(), envelopeId), "utf8")), ordinary);
		await f.dispose();
		assert.equal(fs.existsSync(f.marker()), false);
		assert.ok(f.scheduled.every((record) => record.disposed));
	},
	async "dead-listener-prompt-fallback"(t) {
		const f = mailboxFixture(t);
		const listener = f.listen("worker-pane");
		const h = await f.host();
		const marker = JSON.parse(fs.readFileSync(f.marker("worker-pane"), "utf8"));
		assert.equal(marker.pid, process.pid);
		assert.equal(marker.id, "worker-pane");
		assert.equal(typeof marker.ts, "number");
		assert.equal(h.transport.isListening("worker-pane"), true);
		const child = spawn(process.execPath, ["-e", "process.exit(0)"], { stdio: "ignore" });
		const closed = new Promise<{ code: number | null; signal: string | null }>((resolve, reject) => {
			child.once("error", reject);
			child.once("close", (code, signal) => resolve({ code, signal }));
		});
		t.after(async () => {
			if (child.exitCode === null && child.signalCode === null) child.kill("SIGKILL");
			await bounded(closed, "owned liveness child cleanup");
		});
		assert.deepEqual(await bounded(closed, "owned liveness child exit"), { code: 0, signal: null });
		assert.ok(child.pid && child.pid > 0);
		fs.writeFileSync(f.marker("worker-pane"), JSON.stringify({ ...marker, pid: child.pid }));
		assert.equal(h.transport.isListening("worker-pane"), false);
		const client = createWorkerRpcClient({ events: h.events });
		const provider = await client.probe({ timeoutMs: 1000 });
		const receipt = await client.send({ target: "agent-scout", message: "Fallback payload", mode: "steer" }, provider, { timeoutMs: 1000 });
		assert.equal(receipt.transport, "herdr-prompt");
		assert.equal(receipt.requestedMode, "steer");
		assert.equal(receipt.priorityApplied, false);
		const prompts = h.execCalls.filter((args) => args[1] === "prompt");
		assert.equal(prompts.length, 1);
		assert.equal(prompts[0][2], "worker-pane");
		assert.ok(prompts[0][3].includes("Fallback payload"));
		assert.deepEqual(fs.readdirSync(f.inbox("worker-pane")), []);
		assert.equal(f.boundaries.length, 0);
		listener.stopListening();
		assert.equal(fs.existsSync(f.marker("worker-pane")), false);
	},
	async "partial-json-and-hidden-temp"(t) {
		const f = mailboxFixture(t);
		const reads: string[] = [];
		const h = await f.host({ transport: { fileSystem: { read(file) { reads.push(file); return fs.readFileSync(file, "utf8"); } } } });
		const final = path.join(f.inbox(), envelopeId);
		const temporary = path.join(f.inbox(), `.${envelopeId}.tmp`);
		const bytes = JSON.stringify(ordinary);
		fs.writeFileSync(final, bytes.slice(0, -1));
		fs.writeFileSync(temporary, bytes);
		for (let i = 0; i < 2; i++) await h.transport.drainInbox();
		assert.equal(h.sentMessages.length, 0);
		assert.equal(fs.readFileSync(final, "utf8"), bytes.slice(0, -1));
		assert.equal(reads.filter((file) => file === final).length, 2);
		assert.equal(reads.includes(temporary), false);
		fs.writeFileSync(final, bytes);
		await h.transport.drainInbox();
		await h.transport.drainInbox();
		assert.deepEqual(messageIds(h), [envelopeId]);
		assert.equal(fs.readFileSync(temporary, "utf8"), bytes);
		assert.equal(fs.readFileSync(final, "utf8"), bytes);
	},
	async "parsed-invalid-cleanup"(t) {
		const f = mailboxFixture(t);
		const reached: string[] = [];
		const h = await f.host({ beforeDeliver: async (_envelope, id) => { reached.push(id); } });
		const invalid = [null, false, 0, "", [], {}, "truthy", { ...ordinary, from: null },
			{ ...ordinary, from: { ...ordinary.from, id: 1 } }, { ...ordinary, from: { ...ordinary.from, paneId: 1 } },
			{ ...ordinary, type: "invalid" }, { ...ordinary, message: 1 }, { ...ordinary, priority: "true" },
			{ ...lifecycle, report: { ...lifecycle.report, sourceSequence: 0 } }];
		invalid.forEach((value, i) => fs.writeFileSync(path.join(f.inbox(), `${i}.json`), JSON.stringify(value)));
		await h.transport.drainInbox();
		assert.deepEqual(fs.readdirSync(f.inbox()), []);
		assert.equal(h.sentMessages.length, 0);
		assert.equal(reached.length, invalid.filter(Boolean).length, "falsy JSON bypasses delivery entirely");
		assert.equal(f.boundaries.filter((boundary) => boundary.name === "after-delete").length, invalid.length);
		assertNoLifecycle(h);
	},
	async "ordinary-priority-routing"(t) {
		const f = mailboxFixture(t);
		const h = await f.host({ transport: { makeFilename: (ts) => `${ts}.json` } });
		h.transport.writeEnvelope("self-pane", { ...ordinary, ts: 1, priority: false });
		h.transport.writeEnvelope("self-pane", { ...ordinary, ts: 2, priority: true });
		await h.transport.drainInbox();
		assert.deepEqual(h.sentMessages.map((delivery) => delivery.options), [
			{ triggerTurn: true, deliverAs: "followUp" }, { triggerTurn: true, deliverAs: "steer" },
		]);
		assert.ok(h.sentMessages.every((delivery) => delivery.message.display && delivery.message.customType === "herdr-worker.message"));
		assert.deepEqual(fs.readdirSync(f.inbox()), ["1.json", "2.json"]);
		assert.equal(h.entries.filter((entry) => entry.type === "custom_message").length, 0);
	},
	async "lifecycle-follow-up-routing"(t) {
		const f = mailboxFixture(t);
		const h = await f.host({ prepare: saveRunMetadata, transport: { makeFilename: () => envelopeId } });
		assert.deepEqual(h.reopen(), h.entries, "saved metadata, not an unbound acceptance shortcut");
		h.transport.writeEnvelope("self-pane", lifecycle);
		await h.transport.drainInbox();
		assert.equal(h.sentMessages.length, 1);
		assert.deepEqual(h.sentMessages[0].options, { triggerTurn: true, deliverAs: "followUp" });
		assert.equal(h.sentMessages[0].message.customType, "herdr-worker.lifecycle-report");
		assert.equal(h.sentMessages[0].message.display, true);
		assert.deepEqual(h.sentMessages[0].message.details, { envelopeId, from: lifecycle.from, report: lifecycle.report });
		await h.hook("context");
		assert.deepEqual(fs.readdirSync(f.inbox()), [envelopeId]);
		assertNoLifecycle(h);
		h.appendMessage(h.consume());
		h.write();
		await h.hook("context");
		await h.hook("agent_settled");
		const journal = h.reopen().filter((entry) => entry.type === "custom" && entry.customType === LIFECYCLE_JOURNAL_ENTRY);
		assert.deepEqual(journal, [{ type: "custom", customType: LIFECYCLE_JOURNAL_ENTRY, data: {
			version: 1, sessionId: "session", event: { ...lifecycle.report, worker: { name: "agent-scout", paneId: "worker-pane" }, source: "worker", acceptedSequence: 1 },
		} }]);
		assert.equal(h.timeline.filter((event) => event === "emit:herdr-workers:lifecycle").length, 1);
		assert.equal(h.timeline.filter((event) => event === "emit:herdr-workers:completed").length, 1);
		assert.deepEqual(fs.readdirSync(f.inbox()), []);
		assert.equal(h.sentMessages.length, 1);
	},
	async "ordinary-peer-rejection"(t) {
		const f = mailboxFixture(t);
		const h = await f.host({ transport: { makeFilename: (ts) => `${ts}.json` } });
		h.transport.writeEnvelope("self-pane", { ...ordinary, from: { ...ordinary.from, paneId: "unknown-pane" } });
		await h.transport.drainInbox();
		assert.deepEqual(fs.readdirSync(f.inbox()), []);
		assert.equal(h.sentMessages.length, 0);
		h.transport.writeEnvelope("self-pane", { ...ordinary, ts: 2 });
		h.setPeerPane("new-worker-pane");
		await h.transport.drainInbox();
		assert.deepEqual(fs.readdirSync(f.inbox()), []);
		assert.equal(h.sentMessages.length, 0);
		h.transport.writeEnvelope("self-pane", { ...ordinary, ts: 3, from: { ...ordinary.from, paneId: "new-worker-pane" } });
		await h.transport.drainInbox();
		assert.deepEqual(messageIds(h), ["3.json"]);
	},
	async "lifecycle-peer-rejection"(t) {
		const f = mailboxFixture(t);
		const h = await f.host({ prepare: saveRunMetadata, transport: { makeFilename: (ts) => `${ts}.json` } });
		h.transport.writeEnvelope("self-pane", { ...lifecycle, from: { ...lifecycle.from, paneId: "unknown-pane" } });
		await h.transport.drainInbox();
		assert.deepEqual(fs.readdirSync(f.inbox()), []);
		h.transport.writeEnvelope("self-pane", { ...lifecycle, ts: 2 });
		h.setPeerPane("new-worker-pane");
		await h.transport.drainInbox();
		assert.deepEqual(fs.readdirSync(f.inbox()), []);
		h.transport.writeEnvelope("self-pane", { ...lifecycle, ts: 3, from: { ...lifecycle.from, paneId: "new-worker-pane" } });
		await h.transport.drainInbox();
		assert.deepEqual(fs.readdirSync(f.inbox()), []);
		assert.equal(h.sentMessages.length, 0);
		assertNoLifecycle(h);
	},
	async "lifecycle-incoming-unbound"(t) {
		const f = mailboxFixture(t);
		const h = await f.host({ transport: { makeFilename: () => envelopeId } });
		h.transport.writeEnvelope("self-pane", lifecycle);
		await h.transport.drainInbox();
		assert.equal(h.sentMessages.length, 0);
		assert.deepEqual(fs.readdirSync(f.inbox()), []);
		assertNoLifecycle(h);
	},
	async "lifecycle-recorded-unbound-pending"(t) {
		const f = mailboxFixture(t);
		const h = await f.host({ transport: { makeFilename: () => envelopeId } });
		h.transport.writeEnvelope("self-pane", lifecycle);
		h.transport.markInFlight(envelopeId);
		h.append({ type: "custom_message", customType: "herdr-worker.lifecycle-report", content: "Recorded unbound report", display: true,
			details: { envelopeId, from: lifecycle.from, report: lifecycle.report } });
		for (let i = 0; i < 2; i++) {
			await h.hook("context");
			await h.hook("agent_settled");
			await h.transport.drainInbox();
		}
		assert.deepEqual(fs.readdirSync(f.inbox()), [envelopeId]);
		assert.equal(h.sentMessages.length, 0);
		assertNoLifecycle(h);
		assert.equal(f.boundaries.some((boundary) => boundary.name === "before-delete"), false);
	},
	async "delivery-reject-before-mark"(t) {
		const f = mailboxFixture(t);
		const reached: string[] = [];
		let fail = true;
		const h = await f.host({ beforeDeliver: async (_envelope, id) => {
			reached.push(id);
			if (fail) { fail = false; throw new Error("Injected pre-mark rejection"); }
		}, transport: { makeFilename: (ts) => `${ts}.json` } });
		h.transport.writeEnvelope("self-pane", ordinary);
		h.transport.writeEnvelope("self-pane", { ...ordinary, ts: 2 });
		await h.transport.drainInbox();
		assert.deepEqual(reached, ["1.json"]);
		assert.equal(h.sentMessages.length, 0);
		assert.deepEqual(fs.readdirSync(f.inbox()), ["1.json", "2.json"]);
		await h.transport.drainInbox();
		assert.deepEqual(reached, ["1.json", "1.json", "2.json"]);
		assert.deepEqual(messageIds(h), ["1.json", "2.json"]);
	},
	async "send-throw-after-mark"(t) {
		const f = mailboxFixture(t);
		const h = await f.host({ transport: { makeFilename: () => envelopeId } });
		h.failNextSend(new Error("Model-only synchronous send failure"));
		h.transport.writeEnvelope("self-pane", ordinary);
		await h.transport.drainInbox();
		assert.deepEqual(messageIds(h), [envelopeId]);
		assert.equal(h.queued.length, 0);
		for (let i = 0; i < 2; i++) {
			await h.hook("context");
			await h.hook("agent_settled");
			await h.transport.drainInbox();
		}
		assert.deepEqual(messageIds(h), [envelopeId]);
		assert.equal(h.entries.filter((entry) => entry.type === "custom_message").length, 0);
		assert.deepEqual(JSON.parse(fs.readFileSync(path.join(f.inbox(), envelopeId), "utf8")), ordinary);
		await h.shutdown();
		const sent = f.gate();
		const replacement = await f.host({ prepare(host) { host.observeSend(() => sent.release()); } });
		await bounded(sent.promise, "fresh receiver retry after synchronous failure");
		assert.deepEqual(messageIds(replacement), [envelopeId]);
		assert.equal(replacement.queued.length, 1);
		assert.deepEqual(replacement.reopen(), []);
	},
	async "hook-unlink-retry"(t) {
		const f = mailboxFixture(t);
		const full = path.join(f.inbox(), envelopeId);
		const fault = oneShotRemoveFault(full);
		const h = await f.host({ transport: { makeFilename: () => envelopeId, fileSystem: { remove: fault.remove } } });
		h.transport.writeEnvelope("self-pane", ordinary);
		await h.transport.drainInbox();
		const entry = h.appendMessage(h.consume());
		h.write();
		assert.deepEqual(h.reopen().filter((entry) => entry.type === "custom_message"), [entry]);
		await h.hook("context");
		assert.equal(fs.existsSync(full), true);
		assert.deepEqual(fault.attempts, [full]);
		await h.hook("context");
		assert.deepEqual(fault.attempts, [full], "hook already cleared the in-flight ID");
		await h.transport.drainInbox();
		assert.deepEqual(fault.attempts, [full, full]);
		assert.equal(fs.existsSync(full), false);
		assert.deepEqual(messageIds(h), [envelopeId]);
		assert.equal(f.boundaries.filter((boundary) => boundary.name === "after-parse").length, 1);
	},
	async "drain-unlink-retry"(t) {
		const f = mailboxFixture(t);
		const full = path.join(f.inbox(), "1.json");
		const second = path.join(f.inbox(), "2.json");
		const fault = oneShotRemoveFault(full);
		const h = await f.host({ transport: { makeFilename: (ts) => `${ts}.json`, fileSystem: { remove: fault.remove } } });
		for (const ts of [1, 2]) {
			h.transport.writeEnvelope("self-pane", { ...ordinary, ts });
			recordOrdinary(h, `${ts}.json`);
		}
		h.write();
		assert.deepEqual(h.reopen(), h.entries);
		await h.transport.drainInbox();
		assert.deepEqual(fault.attempts, [full]);
		assert.deepEqual(fs.readdirSync(f.inbox()), ["1.json", "2.json"]);
		await h.transport.drainInbox();
		assert.deepEqual(fault.attempts, [full, full, second]);
		assert.deepEqual(fs.readdirSync(f.inbox()), []);
		assert.equal(h.sentMessages.length, 0);
		assert.equal(f.boundaries.some((boundary) => boundary.name === "after-parse"), false);
	},
	async "lexical-pane-containment"(t) {
		const f = mailboxFixture(t);
		const h = await f.host();
		const sentinel = path.join(f.root, "outside-mailbox-sentinel");
		fs.writeFileSync(sentinel, "untouched");
		const panes = ["../outside-mailbox-sentinel", path.join(f.root, "absolute-pane"), "a/b", "a\\b", "a\u0000b\n\u007f"];
		for (const pane of panes) {
			h.transport.writeEnvelope(pane, ordinary);
			assertInside(f.mailboxRoot, inboxDir(f.mailboxRoot, pane));
			const publication = f.boundaries.at(-1)!;
			assert.equal(publication.name, "after-rename");
			assertInside(f.mailboxRoot, publication.fullPath);
			assertInside(f.mailboxRoot, publication.temporaryPath!);
			assert.match(publication.envelopeId, /^\d{15}-[a-z0-9]{0,6}\.json$/);
			assert.equal(fs.existsSync(publication.temporaryPath!), false);
			assert.deepEqual(JSON.parse(fs.readFileSync(publication.fullPath, "utf8")), ordinary);
		}
		assert.equal(fs.readFileSync(sentinel, "utf8"), "untouched");
		assert.equal(fs.existsSync(path.join(f.root, "absolute-pane")), false);
	},
	async "acknowledgement-allowlist"(t) {
		const f = mailboxFixture(t);
		const removed: string[] = [];
		const h = await f.host({ transport: { fileSystem: { remove(file) { removed.push(file); fs.rmSync(file, { force: true }); } } } });
		const sentinel = path.join(f.root, "outside-mailbox-sentinel.json");
		fs.writeFileSync(sentinel, "untouched");
		const malformed = [path.relative(f.inbox(), sentinel), sentinel, "../escape.json", "a/b.json", "a\\b.json",
			"bad\u0000.json", "bad\n.json", "bad\u007f.json", "bad.json.tmp", "bad.JSON", "", "bad name.json"];
		for (const id of malformed) { h.transport.markInFlight(id); recordOrdinary(h, id); }
		await h.hook("context");
		await h.hook("agent_settled");
		assert.deepEqual(removed, [], "marked malformed IDs must reach and fail the allowlist before unlink");
		assert.equal(fs.readFileSync(sentinel, "utf8"), "untouched");
		for (const id of ["..json", "...json", "a..b.json", ".hidden.json", "valid-_.json"]) {
			const file = path.join(f.inbox(), id);
			fs.writeFileSync(file, "acknowledged payload");
			h.transport.markInFlight(id);
			recordOrdinary(h, id);
			await h.hook("context");
			assert.equal(fs.existsSync(file), false, `existing allowlist accepts ${id}`);
			assert.equal(removed.at(-1), file);
			assertInside(f.inbox(), file);
		}
		assert.equal(removed.length, 5);
	},
	async "suffix-reverses-producer-order"(t) {
		const f = mailboxFixture(t);
		const names = ["000000000000001-z.json", "000000000000001-a.json"];
		let index = 0;
		const h = await f.host({ transport: { makeFilename: () => names[index++] } });
		h.transport.writeEnvelope("self-pane", { ...ordinary, message: "First producer call" });
		h.transport.writeEnvelope("self-pane", { ...ordinary, message: "Second producer call" });
		await h.transport.drainInbox();
		assert.deepEqual(messageIds(h), [...names].reverse());
		assert.ok(h.sentMessages[0].message.content.includes("Second producer call"));
		assert.ok(h.sentMessages[1].message.content.includes("First producer call"));
	},
	async "filename-collision-overwrite"(t) {
		const f = mailboxFixture(t);
		const h = await f.host({ transport: { makeFilename: () => envelopeId } });
		h.transport.writeEnvelope("self-pane", { ...ordinary, message: "Overwritten first payload" });
		h.transport.writeEnvelope("self-pane", { ...ordinary, message: "Surviving second payload" });
		assert.deepEqual(fs.readdirSync(f.inbox()), [envelopeId]);
		assert.equal(JSON.parse(fs.readFileSync(path.join(f.inbox(), envelopeId), "utf8")).message, "Surviving second payload");
		await h.transport.drainInbox();
		assert.deepEqual(messageIds(h), [envelopeId]);
		assert.ok(h.sentMessages[0].message.content.includes("Surviving second payload"));
		assert.equal(h.sentMessages[0].message.content.includes("Overwritten first payload"), false);
	},
	async "two-receivers-duplicate"(t) {
		const f = mailboxFixture(t);
		const both = f.gate();
		const release = f.gate();
		let reached = 0;
		const beforeDeliver = async () => { if (++reached === 2) both.release(); await release.promise; };
		const first = await f.host({ beforeDeliver, transport: { makeFilename: () => envelopeId } });
		const second = await f.host({ beforeDeliver });
		first.transport.writeEnvelope("self-pane", ordinary);
		const drains = Promise.all([first.transport.drainInbox(), second.transport.drainInbox()]);
		await bounded(both.promise, "both receiver delivery callbacks");
		assert.equal(first.sentMessages.length + second.sentMessages.length, 0);
		assert.deepEqual(fs.readdirSync(f.inbox()), [envelopeId]);
		release.release();
		await bounded(drains, "both receiver drains");
		assert.deepEqual(messageIds(first), [envelopeId]);
		assert.deepEqual(messageIds(second), [envelopeId]);
		assert.equal(first.queued.length, 1);
		assert.equal(second.queued.length, 1);
		first.appendMessage(first.consume());
		first.write();
		await first.hook("context");
		assert.deepEqual(fs.readdirSync(f.inbox()), []);
		assert.equal(second.queued.length, 1, "first acknowledgement cannot retract the second injection");
	},
	async "bind-assignment-reordered"(t) {
		const f = mailboxFixture(t);
		const names = ["000000000000001-z-bind.json", "000000000000001-a-assignment.json"];
		let index = 0;
		const h = await f.host({ transport: { makeFilename: () => names[index++] } });
		const binding = { protocol: 2, runId: "assignment-run" };
		let activeAtSend: unknown = "not observed";
		h.observeSend(() => {
			const state = h.entries.filter((entry) => entry.type === "custom" && entry.customType === "herdr-worker").at(-1)!;
			activeAtSend = (state as { data: { activeRun?: unknown } }).data.activeRun;
		});
		h.transport.writeEnvelope("self-pane", { type: "control", ts: 1, from: ordinary.from, action: "bind-run", binding });
		h.transport.writeEnvelope("self-pane", { ...ordinary, runId: binding.runId, message: "Start the assignment" });
		await h.transport.drainInbox();
		assert.deepEqual(messageIds(h), [names[1]]);
		assert.equal(activeAtSend, undefined, "assignment injection occurs before binding");
		const saved = h.reopen().filter((entry) => entry.type === "custom" && entry.customType === "herdr-worker").at(-1)!;
		const state = (saved as { data: { activeRun: { runId: string; protocol: number; sourceInstanceId: string } } }).data;
		assert.equal(state.activeRun.runId, binding.runId);
		assert.equal(state.activeRun.protocol, 2);
		assert.equal(typeof state.activeRun.sourceInstanceId, "string");
		assert.deepEqual(f.boundaries.filter((boundary) => boundary.name === "after-rename").map((boundary) => boundary.envelopeId), names);
		assert.deepEqual(f.boundaries.filter((boundary) => boundary.name === "after-parse").map((boundary) => boundary.envelopeId), [...names].reverse());
		assert.deepEqual(fs.readdirSync(f.inbox()), [names[1]]);
	},
	async "pane-sanitization-alias"(t) {
		const f = mailboxFixture(t);
		const h = await f.host({ transport: { makeFilename: (ts) => `${ts}.json` } });
		assert.equal(f.inbox("a/b"), f.inbox("a_b"));
		h.transport.writeEnvelope("a/b", ordinary);
		h.transport.writeEnvelope("a_b", { ...ordinary, ts: 2 });
		assert.deepEqual(fs.readdirSync(f.inbox("a/b")), ["1.json", "2.json"]);
		assert.deepEqual(fs.readdirSync(f.inbox("a_b")), ["1.json", "2.json"]);
	},
	async "symlink-entry-read"(t) {
		const f = mailboxFixture(t);
		const h = await f.host();
		const sentinel = path.join(f.root, "outside-mailbox-sentinel.json");
		const bytes = JSON.stringify({ ...ordinary, message: "Read outside the mailbox subtree" });
		fs.writeFileSync(sentinel, bytes);
		const link = path.join(f.inbox(), envelopeId);
		fs.symlinkSync(sentinel, link);
		assertInside(f.root, sentinel);
		assert.ok(path.relative(f.mailboxRoot, fs.realpathSync(link)).startsWith(".."));
		await h.transport.drainInbox();
		assert.deepEqual(messageIds(h), [envelopeId]);
		assert.ok(h.sentMessages[0].message.content.includes("Read outside the mailbox subtree"));
		h.appendMessage(h.consume());
		await h.hook("context");
		assert.equal(fs.existsSync(link), false);
		assert.equal(fs.readFileSync(sentinel, "utf8"), bytes);
	},
	async "symlink-ancestor-write"(t) {
		const f = mailboxFixture(t);
		const h = await f.host({ transport: { makeFilename: () => envelopeId } });
		const outside = path.join(f.root, "outside-mailbox-directory");
		fs.mkdirSync(outside);
		fs.symlinkSync(outside, mailboxDir(f.mailboxRoot, "redirected-pane"), "dir");
		h.transport.writeEnvelope("redirected-pane", ordinary);
		const redirected = path.join(outside, "inbox", envelopeId);
		assertInside(f.root, redirected);
		assert.equal(fs.realpathSync(path.join(f.inbox("redirected-pane"), envelopeId)), fs.realpathSync(redirected));
		assert.deepEqual(JSON.parse(fs.readFileSync(redirected, "utf8")), ordinary);
		assert.deepEqual(fs.readdirSync(path.join(outside, "inbox")), [envelopeId]);
	},
	async "known-pane-impersonation"(t) {
		const f = mailboxFixture(t);
		const h = await f.host();
		const forged = { ...ordinary, from: { ...ordinary.from, id: "not-a-team-member", name: "Forged writer", role: "orchestrator" }, message: "Local forgery" };
		fs.writeFileSync(path.join(f.inbox(), envelopeId), JSON.stringify(forged));
		await h.transport.drainInbox();
		assert.deepEqual(messageIds(h), [envelopeId]);
		assert.deepEqual(h.sentMessages[0].message.details, { envelopeId, from: forged.from });
		assert.ok(h.sentMessages[0].message.content.includes("Local forgery"));
		assert.equal(h.queued.length, 1);
	},
};

for (const row of mailboxCases) test(mailboxCaseName(row), { timeout: 10_000 }, bodies[row.id]);

async function publishLifecycle(h: Awaited<ReturnType<ReturnType<typeof mailboxFixture>["host"]>>) {
	h.transport.writeEnvelope("self-pane", lifecycleEnvelope);
	await h.transport.drainInbox();
	assert.equal(h.sentMessages.length, 1);
	assert.equal(h.queued.length, 1);
	const entry = h.appendMessage(h.consume());
	assert.deepEqual(h.reopen().filter((entry) => entry.type === "custom_message"), [entry]);
	return entry;
}

async function lifecycleAppendFailure(t: TestContext, afterMemory: boolean) {
	const f = mailboxFixture(t);
	const h = await f.host({ persistenceMode: "file-backed", prepare: seedLifecycleAuthority, branchEntry: selectedTeamEntry,
		transport: { makeFilename: () => envelopeId } });
	await publishLifecycle(h);
	const bytes = fs.readFileSync(f.sessionFile, "utf8");
	h.setAppendFault({ customType: LIFECYCLE_JOURNAL_ENTRY, afterMemory });
	await h.hook("context");
	assert.equal(journals(h.entries).length, Number(afterMemory));
	assert.equal(journals(h.reopen()).length, 0);
	assert.equal(fs.readFileSync(f.sessionFile, "utf8"), bytes);
	assert.deepEqual(fs.readdirSync(f.inbox()), [envelopeId]);
	assert.equal(h.events.emissions.filter((event) => event.channel === "herdr-workers:lifecycle").length, 0);
	const failed = await queryLifecycle(h.events);
	assert.equal(failed.record.lifecycle.acceptedSequence, 0);
	assert.deepEqual(failed.replay.events, []);
	h.setAppendFault();
	await h.hook("agent_settled");
	const accepted = await queryLifecycle(h.events);
	assert.equal(accepted.record.lifecycle.acceptedSequence, 1);
	assert.equal(journals(h.entries).length, afterMemory ? 2 : 1);
	assert.deepEqual(journals(h.reopen()), journals(h.entries), "full-snapshot model writes raw candidates on explicit successful retry");
	assert.deepEqual(accepted.replay.events, [journals(h.reopen())[0].event]);
	assert.deepEqual(fs.readdirSync(f.inbox()), []);
	assert.equal(h.sentMessages.length, 1);
	const publications = h.events.emissions.filter((event) => ["herdr-workers:lifecycle", "herdr-workers:completed"].includes(event.channel));
	assert.deepEqual(publications.map((event) => event.channel), ["herdr-workers:lifecycle", "herdr-workers:completed"]);
	assert.equal(publications[0].payload, publications[1].payload);
	assert.deepEqual(publications[0].payload, accepted.replay.events[0]);
	assert.ok(h.timeline.lastIndexOf("write:session") < h.timeline.indexOf("emit:herdr-workers:lifecycle"));
	assert.ok(h.timeline.indexOf("emit:herdr-workers:completed") < h.timeline.indexOf("mailbox:before-delete"));
	await h.reload();
	assert.deepEqual((await queryLifecycle(h.events)).replay, accepted.replay);
	assert.equal(h.events.emissions.filter((event) => event.channel === "herdr-workers:lifecycle").length, 1);
}

async function handledRejection(t: TestContext, reason: string) {
	const f = mailboxFixture(t);
	const rejectedId = "000000000000002-rejected.json";
	const h = await f.host({ persistenceMode: "file-backed", prepare: seedLifecycleAuthority, branchEntry: selectedTeamEntry,
		transport: { makeFilename: (ts) => ts === 1 ? envelopeId : rejectedId } });
	const baselineReport = { ...lifecycleEnvelope.report, eventId: "baseline", sourceSequence: 2,
		...(reason === "terminal-conflict" ? { status: "failed", evidence: { kind: "worker_failed", error: "Prior failure" } }
			: { status: "message", evidence: { kind: "worker_message", message: "Prior progress" } }) };
	h.transport.writeEnvelope("self-pane", { ...lifecycleEnvelope, report: baselineReport });
	await h.transport.drainInbox();
	h.appendMessage(h.consume());
	await h.hook("context");
	const before = await queryLifecycle(h.events);
	assert.equal(before.record.lifecycle.acceptedSequence, 1);
	const report = { ...lifecycleEnvelope.report, sourceSequence: 3,
		...(reason === "duplicate-event" ? { eventId: "baseline" } : {}),
		...(reason === "stale-source" ? { sourceSequence: 1 } : {}),
		...(reason === "protocol-mismatch" ? { protocol: 1, evidence: { kind: "worker_completed", result: "Wrong contract" } } : {}) };
	const details = { envelopeId: rejectedId, report, from: { ...lifecycleEnvelope.from, ...(reason === "pane-mismatch" ? { paneId: "other-pane" } : {}) } };
	h.transport.writeEnvelope("self-pane", { ...lifecycleEnvelope, ts: 2 });
	h.transport.markInFlight(rejectedId);
	h.appendEntry("test-recorded-rejection", { reason });
	h.append({ type: "custom_message", customType: "herdr-worker.lifecycle-report", display: true, content: "Already recorded report",
		details: reason === "malformed-details" ? { envelopeId: rejectedId, report: {}, from: null } : details });
	h.write();
	await h.hook("context");
	await h.hook("agent_settled");
	await h.transport.drainInbox();
	assert.deepEqual(fs.readdirSync(f.inbox()), []);
	assert.equal(h.sentMessages.length, 1, "recorded handled report must not be reinjected");
	assert.deepEqual(await queryLifecycle(h.events), before);
	assert.equal(journals(h.reopen()).length, 1);
	assert.equal(h.events.emissions.filter((event) => event.channel === "herdr-workers:lifecycle").length, 1);
	assert.equal(h.events.emissions.filter((event) => event.channel === "herdr-workers:completed").length, 0);
	if (reason === "terminal-conflict") {
		const rejections = h.reopen().filter((entry) => entry.type === "custom" && entry.customType === "herdr-worker.lifecycle-rejection.v1");
		assert.ok(rejections.length > 0);
		assert.ok(rejections.every((entry) => (entry as { data: { reason: string } }).data.reason === "terminal_conflict"));
	}
}

async function controlAuthority(t: TestContext, action: "orchestrated-by" | "bind-run" | "released") {
	const scenarios = [
		{ assigned: false, known: false, current: false, valid: true },
		{ assigned: true, known: false, current: true, valid: true },
		{ assigned: true, known: true, current: false, valid: true },
		{ assigned: true, known: true, current: true, valid: false },
		{ assigned: true, known: true, current: true, valid: true },
	];
	for (const spec of scenarios) {
		const f = mailboxFixture(t);
		const initial = { version: 1, sessionId: "session", workers: ["agent-scout"],
			...(spec.assigned ? { orchestratedBy: "agent-scout", activeRun: { protocol: 2, runId: "old-run", sourceInstanceId: "old-source", sourceSequence: 0 } } : {}) };
		const h = await f.host({ persistenceMode: "file-backed", prepare(host) { host.appendEntry("herdr-worker", initial); }, transport: { makeFilename: () => envelopeId } });
		const from = { ...lifecycleEnvelope.from, id: spec.current ? "agent-scout" : "claimed-other", paneId: spec.known ? "worker-pane" : "unknown-pane" };
		const allowed = action === "orchestrated-by" ? !spec.assigned || spec.known
			: action === "bind-run" ? spec.valid && (!spec.assigned || (spec.known && spec.current)) : spec.assigned && spec.known && spec.current;
		const count = h.entries.length;
		h.transport.writeEnvelope("self-pane", { type: "control", ts: 1, from, action,
			binding: spec.valid ? { protocol: 2, runId: recoveryRunId } : { protocol: 999, runId: recoveryRunId } });
		await h.transport.drainInbox();
		assert.equal(h.entries.length, count + Number(allowed), JSON.stringify({ action, spec }));
		assert.deepEqual(h.reopen(), JSON.parse(JSON.stringify(h.entries)), "JSON omits undefined cleared state fields");
		assert.equal(h.sentMessages.length, 0);
		assert.deepEqual(fs.readdirSync(f.inbox()), []);
		const state = teamState(h.reopen());
		if (!allowed) assert.deepEqual(state, initial);
		else {
			assert.equal(state.orchestratedBy, action === "released" ? undefined : from.id);
			if (action === "bind-run") {
				assert.equal(state.activeRun.runId, recoveryRunId);
				assert.notEqual(state.activeRun.sourceInstanceId, "old-source");
			} else assert.deepEqual(state.activeRun, action === "orchestrated-by" && spec.current ? initial.activeRun : undefined);
		}
		assert.equal(journals(h.entries).length, 0);
		await f.dispose();
	}
}

const lifecycleBodies: Record<LifecycleMailboxCaseId, (t: TestContext) => Promise<void>> = {
	"lifecycle-journal-append-failed": (t) => lifecycleAppendFailure(t, false),
	"lifecycle-memory-journal-error": (t) => lifecycleAppendFailure(t, true),
	"lifecycle-handled-malformed-details": (t) => handledRejection(t, "malformed-details"),
	"lifecycle-handled-pane-mismatch": (t) => handledRejection(t, "pane-mismatch"),
	"lifecycle-handled-duplicate-event": (t) => handledRejection(t, "duplicate-event"),
	"lifecycle-handled-stale-source": (t) => handledRejection(t, "stale-source"),
	"lifecycle-handled-protocol-mismatch": (t) => handledRejection(t, "protocol-mismatch"),
	"lifecycle-handled-terminal-conflict": (t) => handledRejection(t, "terminal-conflict"),
	"control-orchestrated-by-authority": (t) => controlAuthority(t, "orchestrated-by"),
	"control-bind-run-authority": (t) => controlAuthority(t, "bind-run"),
	"control-released-authority": (t) => controlAuthority(t, "released"),
	async "lifecycle-recorded-unbound"(t) {
		const f = mailboxFixture(t);
		const h = await f.host({ persistenceMode: "file-backed", branchEntry: selectedTeamEntry,
			prepare: (host) => seedLifecycleAuthority(host, false), transport: { makeFilename: () => envelopeId } });
		h.transport.writeEnvelope("self-pane", lifecycleEnvelope);
		h.append({ type: "custom_message", customType: "herdr-worker.lifecycle-report", content: "Recorded unbound report", display: true,
			details: { envelopeId, from: lifecycleEnvelope.from, report: lifecycleEnvelope.report } });
		h.write();
		await h.hook("context");
		await h.hook("agent_settled");
		await h.transport.drainInbox();
		assert.equal((await queryLifecycle(h.events)).record.lifecycle.acceptedSequence, 0);
		assert.equal(h.sentMessages.length, 0);
		assert.equal(journals(h.reopen()).length, 0);
		assert.equal(h.events.emissions.filter((event) => event.channel === "herdr-workers:lifecycle").length, 0);
		assert.deepEqual(fs.readdirSync(f.inbox()), [envelopeId]);
		await h.shutdown();
		const next = await f.host({ reopen: true, persistenceMode: "file-backed", branchEntry: selectedTeamEntry, prepare: seedLifecycleAuthority });
		await next.transport.drainInbox();
		await next.hook("context");
		assert.equal(next.sentMessages.length, 0);
		const accepted = await queryLifecycle(next.events);
		assert.equal(accepted.record.lifecycle.acceptedSequence, 1);
		assert.deepEqual(accepted.replay.events, journals(next.reopen()).map((entry) => entry.event));
		assert.equal(journals(next.reopen()).length, 1);
		assert.equal(next.events.emissions.filter((event) => event.channel === "herdr-workers:lifecycle").length, 1);
		assert.deepEqual(fs.readdirSync(f.inbox()), []);
	},
	async "control-bind-run-redelivery-ready"(t) {
		const f = mailboxFixture(t);
		f.listen("worker-pane");
		const ready = [f.gate(), f.gate()];
		const readiness: any[] = [];
		const fault = oneShotRemoveFault(path.join(f.inbox(), envelopeId));
		let filename = 0;
		const h = await f.host({ persistenceMode: "file-backed", prepare(host) {
			host.appendEntry("herdr-worker", { version: 1, sessionId: "session", workers: [], orchestratedBy: "agent-scout" });
		}, transport: { makeFilename: () => filename++ === 0 ? envelopeId : `ready-${filename}.json`, fileSystem: { remove: fault.remove },
			observeBoundary(boundary) {
				if (boundary.name === "after-rename" && boundary.paneId === "worker-pane") {
					readiness.push(JSON.parse(fs.readFileSync(boundary.fullPath, "utf8")));
					ready[readiness.length - 1].release();
				}
			} } });
		h.transport.writeEnvelope("self-pane", { type: "control", ts: 1, from: lifecycleEnvelope.from, action: "bind-run", binding: { protocol: 2, runId: recoveryRunId } });
		await h.transport.drainInbox();
		await bounded(ready[0].promise, "first completed readiness publication");
		const first = structuredClone(teamState(h.reopen()));
		assert.deepEqual(fs.readdirSync(f.inbox()), [envelopeId]);
		await h.transport.drainInbox();
		await bounded(ready[1].promise, "second completed readiness publication");
		const second = teamState(h.reopen());
		assert.notEqual(second.activeRun.sourceInstanceId, first.activeRun.sourceInstanceId);
		assert.equal(first.activeRun.sourceSequence, 1);
		assert.equal(second.activeRun.sourceSequence, 1);
		assert.equal(readiness.length, 2);
		assert.notEqual(readiness[0].report.eventId, readiness[1].report.eventId);
		assert.deepEqual(readiness.map((envelope) => envelope.report.sourceInstanceId), [first.activeRun.sourceInstanceId, second.activeRun.sourceInstanceId]);
		assert.ok(readiness.every((envelope) => envelope.type === "lifecycle" && envelope.report.status === "started" && envelope.report.evidence.kind === "worker_ready"));
		assert.equal(fs.readdirSync(f.inbox("worker-pane")).length, 2);
		assert.deepEqual(fs.readdirSync(f.inbox()), []);
		assert.equal(h.sentMessages.length, 0);
	},
};
assert.deepEqual(Object.keys(lifecycleBodies).sort(), lifecycleMailboxCases.map((row) => row.id).sort());
for (const row of lifecycleMailboxCases) test(mailboxCaseName(row), { timeout: 10_000 }, lifecycleBodies[row.id]);

function verifyMatrix(rows: readonly MailboxCase[], doc: string) {
	const ids = rows.map((row) => row.id);
	assert.equal(new Set(ids).size, ids.length, "case IDs must be unique");
	for (const row of rows) {
		assert.ok(["Supported guarantee", "Known contract gap"].includes(row.category));
		for (const field of [row.id, row.kind, row.boundary, row.mode, row.assertion, row.assumptions]) assert.ok(field?.trim());
		if (row.category === "Known contract gap") assert.ok(row.obsoleteCondition?.trim());
	}
	const documentedRows = doc.split("\n").filter((line) => line.startsWith("| ")).slice(1)
		.map((line) => line.split("|").slice(1, -1).map((cell) => cell.trim()));
	assert.deepEqual(documentedRows, rows.map(mailboxCaseCells));
}

test("mailbox guarantee matrix matches the executing cases", () => {
	assert.deepEqual(Object.keys(bodies).sort(), mailboxCases.map((row) => row.id).sort(), "each mailbox row registers a test body");
	const doc = fs.readFileSync(new URL("../../docs/mailbox-guarantees.md", import.meta.url), "utf8");
	verifyMatrix(allMailboxCases, doc);
	assert.throws(() => verifyMatrix(allMailboxCases.slice(1), doc), "missing case IDs must fail");
	assert.throws(() => verifyMatrix([...allMailboxCases, allMailboxCases[0]], doc), "duplicate case IDs must fail");
	for (const field of ["id", "boundary", "mode", "assumptions"] as const) {
		const rows = structuredClone(allMailboxCases);
		rows[0][field] = "";
		assert.throws(() => verifyMatrix(rows, doc), `missing ${field} must fail`);
	}
	const rows = structuredClone(allMailboxCases);
	rows[0].category = "Known contract gap";
	rows[0].obsoleteCondition = "Mutation probe";
	assert.throws(() => verifyMatrix(rows, doc), "category mismatches must fail even when fields are valid");
	const gapRows = structuredClone(allMailboxCases);
	delete gapRows.find((row) => row.category === "Known contract gap")!.obsoleteCondition;
	assert.throws(() => verifyMatrix(gapRows, doc), "gap removal condition is required");
});
