import assert from "node:assert/strict";
import * as fs from "node:fs";
import * as path from "node:path";
import test, { type TestContext } from "node:test";
import { inboxDir, listeningFile } from "../../mailbox/paths.js";
import type { ChildConfig, ChildSnapshot } from "../support/mailbox-child.js";
import { mailboxCaseName, recoveryMailboxCases, type RecoveryMailboxCaseId } from "../support/mailbox-cases.js";
import { bounded, mailboxFixture, mailboxProcessFixture } from "../support/mailbox-fixture.js";

process.env.HERDR_ENV = "1";
process.env.HERDR_PANE_ID = "self-pane";
process.env.HERDR_TAB_ID = "tab-1";

const envelopeId = "000000000000001-recovery.json";
const temporaryId = `.${envelopeId}.tmp`;
const ordinary = {
	type: "message", ts: 1, priority: false, message: "Ordinary process recovery payload",
	from: { id: "agent-scout", paneId: "worker-pane", name: "agent-scout", role: "worker" },
};

interface CrashCase {
	role: "sender" | "receiver";
	boundary: string;
	mode?: ChildConfig["mode"];
	file: "none" | "partial" | "temporary" | "final";
	oldInjection: number;
	memory: number;
	recovered: number;
	retry: boolean;
}

type CrashId = Exclude<RecoveryMailboxCaseId, "ordinary-reload-visible" | "ordinary-reload-queued">;
const crashes: Record<CrashId, CrashCase> = {
	"ordinary-before-create": { role: "sender", boundary: "before-write", file: "none", oldInjection: 0, memory: 0, recovered: 0, retry: false },
	"ordinary-partial-temp": { role: "sender", boundary: "partial-temp", file: "partial", oldInjection: 0, memory: 0, recovered: 0, retry: false },
	"ordinary-before-rename": { role: "sender", boundary: "after-temp-write", file: "temporary", oldInjection: 0, memory: 0, recovered: 0, retry: false },
	"ordinary-after-rename": { role: "sender", boundary: "after-rename", file: "final", oldInjection: 0, memory: 0, recovered: 0, retry: true },
	"ordinary-after-read": { role: "receiver", boundary: "after-parse", file: "final", oldInjection: 0, memory: 0, recovered: 0, retry: true },
	"ordinary-after-send": { role: "receiver", boundary: "after-send", file: "final", oldInjection: 1, memory: 0, recovered: 0, retry: true },
	"ordinary-memory-before-write": { role: "receiver", boundary: "memory-before-write", file: "final", oldInjection: 1, memory: 1, recovered: 0, retry: true },
	"ordinary-ack-before-write-deferred-first-write": { role: "receiver", boundary: "ack-before-write", mode: "deferred-first-write", file: "none", oldInjection: 1, memory: 1, recovered: 0, retry: false },
	"ordinary-ack-before-write-disabled": { role: "receiver", boundary: "ack-before-write", mode: "disabled", file: "none", oldInjection: 1, memory: 1, recovered: 0, retry: false },
	"ordinary-ack-before-write-write-failed": { role: "receiver", boundary: "ack-before-write", mode: "write-failed", file: "none", oldInjection: 1, memory: 1, recovered: 0, retry: false },
	"ordinary-file-before-ack": { role: "receiver", boundary: "file-before-ack", file: "final", oldInjection: 1, memory: 1, recovered: 1, retry: false },
	"ordinary-before-delete": { role: "receiver", boundary: "before-delete", file: "final", oldInjection: 1, memory: 1, recovered: 1, retry: false },
	"ordinary-delete-failed": { role: "receiver", boundary: "delete-failed", file: "final", oldInjection: 1, memory: 1, recovered: 1, retry: false },
	"ordinary-after-delete": { role: "receiver", boundary: "after-delete", file: "none", oldInjection: 1, memory: 1, recovered: 1, retry: false },
};

function assertPayload(snapshot: ChildSnapshot) {
	for (const { message, options } of snapshot.sent) {
		assert.equal(message.customType, "herdr-worker.message");
		assert.equal(message.display, true);
		assert.ok(message.content.includes(ordinary.message));
		assert.deepEqual(message.details, { envelopeId, from: ordinary.from });
		assert.deepEqual(options, { triggerTurn: true, deliverAs: "followUp" });
	}
	if (snapshot.memory.length && snapshot.sent.length) {
		assert.deepEqual(snapshot.memory, snapshot.sent.map(({ message }) => ({ type: "custom_message", ...message })));
	}
	assert.equal(snapshot.lifecycleEvents, 0, "ordinary recovery does not accept lifecycle evidence");
}

async function crashCase(t: TestContext, id: CrashId) {
	const spec = crashes[id];
	const f = mailboxProcessFixture(t);
	const first = f.start({ scenario: id, role: spec.role, boundary: spec.boundary, mode: spec.mode ?? "file-backed" });
	const checkpoint = await first.record("checkpoint");
	assert.equal(checkpoint.boundary, spec.boundary, "wrong checkpoint must fail before SIGKILL");
	const old = checkpoint.snapshot!;
	assert.ok(old);
	assert.equal(old.sent.length, spec.oldInjection);
	assert.equal(old.memory.length, spec.memory);
	assert.equal(old.recovered.length, spec.recovered);
	assert.equal(old.sessionFileExists, spec.mode !== "deferred-first-write" && spec.mode !== "disabled");
	assert.equal(old.queued, spec.boundary === "after-send" ? 1 : 0);
	assert.equal(old.receiptReturned, false);
	assertPayload(old);
	if (spec.memory) assert.deepEqual(old.memory, [{ type: "custom_message", ...old.sent[0].message }]);
	if (spec.recovered) assert.deepEqual(old.recovered, old.memory);
	if (spec.boundary === "after-parse") assert.equal(old.boundaries.at(-1), "after-parse");
	if (["memory-before-write", "file-before-ack"].includes(spec.boundary)) {
		assert.equal(old.timeline.includes("hook:context"), false);
		assert.equal(old.boundaries.includes("before-delete"), false);
	}
	if (spec.boundary === "memory-before-write") assert.equal(old.timeline.at(-1), "memory:herdr-worker.message");
	if (spec.boundary === "file-before-ack") assert.equal(old.timeline.at(-1), "write:session");
	if (spec.boundary === "before-delete") {
		assert.equal(old.timeline.includes("hook:context"), true);
		assert.equal(old.boundaries.includes("after-delete"), false);
	}
	assert.equal(old.removeFailures, spec.boundary === "delete-failed" ? 1 : 0);
	if (spec.boundary === "delete-failed") {
		assert.equal(old.boundaries.at(-1), "before-delete");
		assert.equal(old.boundaries.includes("after-delete"), false);
	}
	if (spec.boundary === "ack-before-write") {
		const writeEvent = spec.mode === "write-failed" ? "write:failed" : `write:${spec.mode}`;
		assert.ok(old.timeline.includes(writeEvent));
		assert.equal(old.boundaries.at(-1), "after-delete");
		assert.ok(old.timeline.indexOf(writeEvent) < old.timeline.indexOf("mailbox:after-delete"));
	}
	const names = spec.file === "final" ? [envelopeId] : ["partial", "temporary"].includes(spec.file) ? [temporaryId] : [];
	assert.deepEqual(Object.keys(old.files), names);
	if (spec.file === "partial") {
		assert.equal(old.files[temporaryId].length, 17);
		assert.throws(() => JSON.parse(old.files[temporaryId]));
	}
	if (spec.file === "temporary" || spec.file === "final") {
		const envelope = JSON.parse(old.files[names[0]]);
		assert.deepEqual(envelope, { ...ordinary, ts: envelope.ts });
		assert.equal(typeof envelope.ts, "number");
	}
	await first.kill();
	const inbox = inboxDir(path.join(f.root, "mailbox"), "self-pane");
	assert.deepEqual(Object.fromEntries(fs.readdirSync(inbox).sort().map((name) => [name, fs.readFileSync(path.join(inbox, name), "utf8")])), old.files);
	const marker = JSON.parse(fs.readFileSync(listeningFile(path.join(f.root, "mailbox"), "self-pane"), "utf8"));
	assert.equal(marker.pid, first.child.pid, "SIGKILL leaves the old listener marker without graceful cleanup");
	// Only paths and scenario controls cross into the replacement, never checkpoint entries or queued payloads.
	const replacement = f.start({ scenario: id, role: "replacement", boundary: "none", mode: "file-backed" });
	assert.notEqual(replacement.child.pid, first.child.pid);
	const result = (await replacement.result()).snapshot!;
	assert.ok(result);
	assert.equal(result.initialQueued, 0);
	assert.equal(result.initialInjections, 0);
	assert.equal(result.sessionFileExists, old.sessionFileExists);
	assert.deepEqual(result.initialRecovered, old.recovered, "new host reconstructs only surviving session entries");
	assert.equal(result.sent.length, spec.retry ? 1 : 0);
	assert.equal(result.queued, 0);
	assert.equal(result.memory.length, spec.recovered + Number(spec.retry));
	assert.deepEqual(result.recovered, result.memory, "replacement writes are verified by reopening the test JSONL");
	assertPayload(result);
	if (spec.recovered) assert.deepEqual(result.recovered, old.recovered, "recovered custom payload and envelope ID remain exact");
	assert.equal(result.boundaries.filter((name) => name === "after-parse").length, Number(spec.retry));
	assert.equal(result.boundaries.filter((name) => name === "after-delete").length, Number(spec.file === "final"));
	assert.deepEqual(result.files, ["partial", "temporary"].includes(spec.file) ? old.files : {});
	await f.dispose();
	assert.equal(fs.existsSync(f.root), false);
}

async function reloadCase(t: TestContext, visible: boolean) {
	const f = mailboxFixture(t);
	const h = await f.host({ persistenceMode: "disabled", transport: { makeFilename: () => envelopeId } });
	const oldTransport = h.transport;
	h.transport.writeEnvelope("self-pane", ordinary);
	await h.transport.drainInbox();
	assert.equal(h.sentMessages.length, 1);
	if (visible) h.appendMessage(h.consume());
	assert.equal(h.queued.length, visible ? 0 : 1);
	assert.deepEqual(h.reopen(), []);
	const memory = h.entries;
	const queue = h.queued;
	const oldDelivery = h.sentMessages[0];
	const sent = f.gate();
	h.observeSend(() => sent.release());
	await h.reload();
	if (!visible) await bounded(sent.promise, "retained queue reload reinjection");
	assert.notEqual(h.transport, oldTransport);
	assert.equal(oldTransport.isStarted(), false);
	assert.ok(f.scheduled.slice(0, 2).every((item) => item.disposed));
	assert.equal(h.transport.isStarted(), true);
	assert.equal(h.entries, memory);
	assert.equal(h.queued, queue);
	assert.equal(h.sentMessages.length, visible ? 1 : 2);
	assert.equal(h.queued.length, visible ? 0 : 2);
	if (!visible) {
		assert.equal(h.queued[0], oldDelivery, "the old queued object survives extension replacement");
		assert.deepEqual(h.queued[1], oldDelivery);
		assert.deepEqual(fs.readdirSync(f.inbox()), [envelopeId]);
		assert.equal(h.entries.filter((entry) => entry.type === "custom_message").length, 0);
		while (h.queued.length) h.appendMessage(h.consume());
		await h.hook("context");
	}
	for (let i = 0; i < 3; i++) { await h.transport.drainInbox(); await h.hook("agent_settled"); }
	assert.equal(h.sentMessages.length, visible ? 1 : 2);
	assert.deepEqual(h.entries.filter((entry) => entry.type === "custom_message"),
		Array.from({ length: visible ? 1 : 2 }, () => ({ type: "custom_message", ...oldDelivery.message })));
	assert.deepEqual(h.reopen(), [], "retained memory is not file recovery");
	assert.equal(fs.existsSync(f.sessionFile), false);
	assert.deepEqual(fs.readdirSync(f.inbox()), []);
	assert.equal(f.boundaries.filter((item) => item.name === "after-parse").length, visible ? 1 : 2);
	assert.equal(f.boundaries.filter((item) => item.name === "after-delete").length, 1);
}

const bodies: Record<RecoveryMailboxCaseId, (t: TestContext) => Promise<void>> = {
	...Object.fromEntries(Object.keys(crashes).map((id) => [id, (t: TestContext) => crashCase(t, id as CrashId)])) as Record<CrashId, (t: TestContext) => Promise<void>>,
	"ordinary-reload-visible": (t) => reloadCase(t, true),
	"ordinary-reload-queued": (t) => reloadCase(t, false),
};
assert.deepEqual(Object.keys(bodies).sort(), recoveryMailboxCases.map((row) => row.id).sort());
for (const row of recoveryMailboxCases) test(mailboxCaseName(row), { timeout: 30_000 }, bodies[row.id]);

test("recovery fixture reaps a child and removes its root after an unreached checkpoint", { timeout: 15_000 }, async (t) => {
	const f = mailboxProcessFixture(t);
	const child = f.start({ scenario: "fixture-failure", role: "receiver", boundary: "never-reached", mode: "file-backed" });
	try {
		await child.record("ready");
		await assert.rejects(child.record("checkpoint", 100), /Timed out waiting for fixture-failure checkpoint/);
	} finally { await f.dispose(); }
	assert.equal(child.child.signalCode, "SIGKILL");
	assert.equal(child.records.some((record) => record.kind === "checkpoint" || record.kind === "shutdown"), false);
	assert.equal(fs.existsSync(f.root), false);
});
