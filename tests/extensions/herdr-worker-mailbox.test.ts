import assert from "node:assert/strict";
import * as fs from "node:fs";
import * as path from "node:path";
import test, { type TestContext } from "node:test";
import { createWorkerRpcClient } from "../../rpc/client.js";
import { allMailboxCases, mailboxCaseCells, mailboxCaseName, mailboxCases, type MailboxCaseId } from "../support/mailbox-cases.js";
import { mailboxFixture } from "../support/mailbox-fixture.js";

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
};

for (const row of mailboxCases) test(mailboxCaseName(row), { timeout: 10_000 }, bodies[row.id]);

test("mailbox guarantee matrix matches the executing cases", () => {
	const rows = allMailboxCases;
	const ids = rows.map((row) => row.id);
	assert.equal(new Set(ids).size, ids.length, "case IDs must be unique");
	assert.deepEqual(Object.keys(bodies).sort(), mailboxCases.map((row) => row.id).sort(), "each mailbox row registers a test body");
	for (const row of rows) {
		assert.ok(["Supported guarantee", "Known contract gap"].includes(row.category));
		for (const field of [row.kind, row.boundary, row.mode, row.assertion, row.assumptions]) assert.ok(field.trim());
		if (row.category === "Known contract gap") assert.ok(row.obsoleteCondition?.trim());
	}
	const doc = fs.readFileSync(new URL("../../docs/mailbox-guarantees.md", import.meta.url), "utf8");
	const documentedRows = doc.split("\n").filter((line) => line.startsWith("| ")).slice(1)
		.map((line) => line.split("|").slice(1, -1).map((cell) => cell.trim()));
	assert.deepEqual(documentedRows, rows.map(mailboxCaseCells));
});
