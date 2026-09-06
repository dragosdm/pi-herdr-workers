import assert from "node:assert/strict";
import * as fs from "node:fs";
import test, { type TestContext } from "node:test";
import { mailboxCaseName, piMailboxCases, type PiMailboxCaseId } from "../support/mailbox-cases.js";
import { customEntries, piCompatFixture } from "../support/pi-compat-fixture.js";
import { mailboxFixture } from "../support/mailbox-fixture.js";
import { SessionManager, type CustomEntry } from "@earendil-works/pi-coding-agent";
import { LIFECYCLE_JOURNAL_ENTRY, type LifecycleJournalEntry } from "../../lifecycle/acceptor.js";
import { lifecyclePiCases } from "../support/mailbox-cases.js";
import { queryLifecycle } from "../support/mailbox-lifecycle.js";

process.env.HERDR_ENV = "1";
process.env.HERDR_PANE_ID = "self-pane";
process.env.HERDR_TAB_ID = "tab-1";

type Fixture = Awaited<ReturnType<typeof piCompatFixture>>;

async function held(f: Fixture, count: number) {
	await f.waitFor(() => f.streams.length === count, `assistant stream ${count}`);
	assert.equal(f.streams[count - 1].released, false, f.diagnostic);
}

async function flushFirstAssistant(f: Fixture) {
	f.startPrompt();
	await held(f, 1);
	f.streams[0].release();
	await f.bounded(f.session.waitForIdle(), "first assistant flush");
	assert.ok(fs.existsSync(f.manager.getSessionFile()!), f.diagnostic);
}

function assertMessageEndBeforeAppend(f: Fixture, envelopeId: string) {
	const ended = f.observations.filter((event) => event.name === "message_end" && event.envelopeId === envelopeId);
	assert.equal(ended.length, 1, f.diagnostic);
	assert.ok(ended[0].files.includes(envelopeId), f.diagnostic);
	assert.ok(!ended[0].entries.some((entry) => (entry.details as { envelopeId: string }).envelopeId === envelopeId),
		`${f.diagnostic}: extension message_end precedes SessionManager append`);
}

async function idleAcknowledgement(t: TestContext, disabled: boolean) {
	const f = await piCompatFixture(t, { disabled });
	assert.equal(f.manager.isPersisted(), !disabled, f.diagnostic);
	if (disabled) assert.equal(f.manager.getSessionFile(), undefined, f.diagnostic);
	else {
		assert.equal(typeof f.manager.getSessionFile(), "string", f.diagnostic);
		assert.equal(fs.existsSync(f.manager.getSessionFile()!), false, f.diagnostic);
	}
	const { envelopeId, envelope } = await f.publish();
	await held(f, 1);
	assert.deepEqual(f.deliveries, [envelopeId], f.diagnostic);
	assertMessageEndBeforeAppend(f, envelopeId);
	const before = f.observations.find((event) => event.name === "context:before")!;
	const after = f.observations.find((event) => event.name === "context:after")!;
	assert.deepEqual(before.files, [envelopeId], f.diagnostic);
	assert.deepEqual(after.files, [], `${f.diagnostic}: context deleted the only mailbox copy`);
	assert.equal(before.entries.length, 1, f.diagnostic);
	assert.deepEqual(before.entries[0].details, { envelopeId, from: envelope.from }, f.diagnostic);
	assert.ok(String(before.entries[0].content).includes(envelope.message), f.diagnostic);
	assert.equal(before.sessionFileExists, false, f.diagnostic);
	assert.equal(after.sessionFileExists, false, f.diagnostic);
	assert.deepEqual(f.reopen(), [], `${f.diagnostic}: no recovered entry at the acknowledgement boundary`);
	assert.equal(fs.readdirSync(f.inbox).length, 0, f.diagnostic);
	if (disabled) {
		f.streams[0].release();
		await f.bounded(f.session.waitForIdle(), "disabled-persistence assistant completion");
		assert.ok(f.manager.getEntries().some((entry) => entry.type === "message" && entry.message.role === "assistant"), f.diagnostic);
		assert.equal(f.manager.isPersisted(), false, f.diagnostic);
		assert.equal(f.manager.getSessionFile(), undefined, f.diagnostic);
		assert.deepEqual(fs.readdirSync(f.sessionDir), [], f.diagnostic);
		assert.deepEqual(f.reopen(), [], f.diagnostic);
	}
	// The deferred case ends at the loss boundary. Cleanup may append an aborted assistant later.
}

const bodies: Record<PiMailboxCaseId, (t: TestContext) => Promise<void>> = {
	async "pi-ordinary-reload-queued"(t) {
		const f = await piCompatFixture(t);
		f.startPrompt();
		await held(f, 1);
		const { envelopeId, envelope } = await f.publish();
		assert.deepEqual(f.deliveries, [envelopeId], f.diagnostic);
		assert.deepEqual(customEntries(f.manager), [], f.diagnostic);
		assert.deepEqual(f.reopen(), [], f.diagnostic);
		assert.deepEqual(fs.readdirSync(f.inbox), [envelopeId], f.diagnostic);
		const oldTransport = f.transport;
		const manager = f.session.sessionManager;
		const agent = f.session.agent;
		assert.equal(f.session.isStreaming, true, f.diagnostic);
		await f.bounded(f.session.reload(), "real session.reload while streaming");
		await f.waitFor(() => f.deliveries.length === 2, "replacement mailbox handoff");
		assert.equal(f.session.agent, agent, `${f.diagnostic}: reload retains the agent that owns the queue`);
		assert.equal(f.session.sessionManager, manager, f.diagnostic);
		assert.notEqual(f.transport, oldTransport, f.diagnostic);
		assert.equal(oldTransport.isStarted(), false, f.diagnostic);
		assert.equal(f.transport.isStarted(), true, f.diagnostic);
		f.assertReloadResources();
		assert.equal(f.observations.filter((event) => event.name === "session_shutdown:reload").length, 1, f.diagnostic);
		assert.equal(f.observations.filter((event) => event.name === "session_start:reload").length, 1, f.diagnostic);
		assert.equal(f.streams.length, 1, f.diagnostic);
		assert.equal(f.streams[0].released, false, f.diagnostic);
		assert.deepEqual(customEntries(f.manager), [], f.diagnostic);
		assert.equal(f.observations.filter((event) => event.name === "message_end").length, 0, f.diagnostic);
		for (let i = 0; i < 3; i++) await f.transport.drainInbox();
		assert.deepEqual(f.deliveries, [envelopeId, envelopeId], `${f.diagnostic}: two handoffs, no consumed entry yet`);
		assert.deepEqual(fs.readdirSync(f.inbox), [envelopeId], f.diagnostic);
		f.streams[0].release();
		await held(f, 2);
		assert.equal(customEntries(f.manager).length, 1, f.diagnostic);
		assert.deepEqual(fs.readdirSync(f.inbox), [], f.diagnostic);
		f.streams[1].release();
		await held(f, 3);
		const expected = structuredClone(customEntries(f.manager));
		assert.equal(expected.length, 2, `${f.diagnostic}: the old queue and replacement injection both consume`);
		assert.notEqual(expected[0].id, expected[1].id, f.diagnostic);
		for (const entry of expected) {
			assert.equal(entry.customType, "herdr-worker.message", f.diagnostic);
			assert.equal(entry.display, true, f.diagnostic);
			assert.ok(String(entry.content).includes(envelope.message), f.diagnostic);
			assert.deepEqual(entry.details, { envelopeId, from: envelope.from }, f.diagnostic);
		}
		assert.equal(expected[0].content, expected[1].content, f.diagnostic);
		assert.deepEqual(f.reopen(), expected, `${f.diagnostic}: two distinct entries recover from actual Pi JSONL`);
		f.streams[2].release();
		await f.bounded(f.session.waitForIdle(), "duplicated reload follow-ups settle");
		for (let i = 0; i < 3; i++) await f.transport.drainInbox();
		assert.deepEqual(f.deliveries, [envelopeId, envelopeId], f.diagnostic);
		assert.equal(f.observations.filter((event) => event.name === "message_end" && event.envelopeId === envelopeId).length, 2, f.diagnostic);
		assert.equal(f.observations.filter((event) => event.name === "mailbox:after-delete").length, 1, f.diagnostic);
		assert.deepEqual(f.reopen(), expected, f.diagnostic);
		assert.deepEqual(fs.readdirSync(f.inbox), [], f.diagnostic);
	},
	async "pi-ordinary-queue-consumption"(t) {
		const f = await piCompatFixture(t);
		f.startPrompt();
		await held(f, 1);
		const followUp = await f.publish(1, false);
		const steer = await f.publish(2, true);
		for (let i = 0; i < 3; i++) await f.transport.drainInbox();
		assert.deepEqual(f.deliveries, [followUp.envelopeId, steer.envelopeId], f.diagnostic);
		assert.deepEqual(customEntries(f.manager), [], `${f.diagnostic}: real queues are not session entries`);
		assert.deepEqual(f.reopen(), [], f.diagnostic);
		assert.deepEqual(fs.readdirSync(f.inbox).sort(), [followUp.envelopeId, steer.envelopeId], f.diagnostic);
		assert.equal(f.observations.filter((event) => event.name === "message_end").length, 0, f.diagnostic);
		f.streams[0].release();
		await held(f, 2);
		assert.deepEqual(customEntries(f.manager).map((entry) => entry.details), [{ envelopeId: steer.envelopeId, from: steer.envelope.from }], f.diagnostic);
		assert.deepEqual(fs.readdirSync(f.inbox), [followUp.envelopeId], `${f.diagnostic}: follow-up remains queued during steering response`);
		assert.deepEqual(f.reopen(), customEntries(f.manager), f.diagnostic);
		f.streams[1].release();
		await held(f, 3);
		assert.deepEqual(customEntries(f.manager).map((entry) => entry.details), [
			{ envelopeId: steer.envelopeId, from: steer.envelope.from },
			{ envelopeId: followUp.envelopeId, from: followUp.envelope.from },
		], f.diagnostic);
		assert.deepEqual(fs.readdirSync(f.inbox), [], f.diagnostic);
		for (const message of [steer, followUp]) {
			assertMessageEndBeforeAppend(f, message.envelopeId);
			const entry = customEntries(f.manager).find((entry) => (entry.details as { envelopeId: string }).envelopeId === message.envelopeId)!;
			assert.equal(entry.customType, "herdr-worker.message", f.diagnostic);
			assert.equal(entry.display, true, f.diagnostic);
			assert.ok(String(entry.content).includes(message.envelope.message), f.diagnostic);
			const before = f.observations.find((event) => event.name === "context:before" && event.entries.some((entry) =>
				(entry.details as { envelopeId: string }).envelopeId === message.envelopeId))!;
			assert.ok(before.files.includes(message.envelopeId), f.diagnostic);
			assert.equal(before.sessionFileExists, true, f.diagnostic);
		}
		const expected = structuredClone(customEntries(f.manager));
		assert.deepEqual(f.reopen(), expected, `${f.diagnostic}: exact custom payloads and envelope IDs recovered from real JSONL`);
		f.streams[2].release();
		await f.bounded(f.session.waitForIdle(), "steer and follow-up settlement");
		assert.deepEqual(f.observations.filter((event) => event.name === "agent_settled:after").map((event) => event.files), [[]], f.diagnostic);
		assert.deepEqual(f.reopen(), expected, f.diagnostic);
		assert.equal(f.streams.length, 3, f.diagnostic);
	},
	async "pi-custom-first-flush-and-append"(t) {
		const f = await piCompatFixture(t);
		assert.equal(f.manager.isPersisted(), true, f.diagnostic);
		const file = f.manager.getSessionFile();
		assert.ok(file, f.diagnostic);
		f.manager.appendCustomMessageEntry("herdr-worker.message", "Buffered exact payload", true, { envelopeId: "000000000000001-buffered.json" });
		const buffered = structuredClone(customEntries(f.manager));
		assert.equal(buffered.length, 1, f.diagnostic);
		assert.equal(fs.existsSync(file), false, f.diagnostic);
		assert.deepEqual(f.reopen(), [], f.diagnostic);
		await flushFirstAssistant(f);
		assert.deepEqual(f.reopen(), buffered, f.diagnostic);
		const initialBytes = fs.readFileSync(file, "utf8");
		f.manager.appendCustomMessageEntry("herdr-worker.message", "Later exact payload", true, { envelopeId: "000000000000002-later.json" });
		assert.equal(customEntries(f.manager).length, 2, f.diagnostic);
		assert.ok(fs.readFileSync(file, "utf8").startsWith(initialBytes), `${f.diagnostic}: later append preserves existing JSONL`);
		assert.deepEqual(f.reopen(), customEntries(f.manager), f.diagnostic);
	},
	async "pi-custom-append-error-memory"(t) {
		const f = await piCompatFixture(t);
		await flushFirstAssistant(f);
		const file = f.manager.getSessionFile()!;
		const bytes = fs.readFileSync(file, "utf8");
		const restore = f.breakSessionDirectory();
		assert.throws(() => f.manager.appendCustomMessageEntry("herdr-worker.message", "Memory after failed append", true,
			{ envelopeId: "000000000000001-failed.json" }), { code: "ENOTDIR" }, f.diagnostic);
		const entries = customEntries(f.manager);
		assert.equal(entries.length, 1, f.diagnostic);
		assert.equal(entries[0].content, "Memory after failed append", f.diagnostic);
		assert.deepEqual(entries[0].details, { envelopeId: "000000000000001-failed.json" }, f.diagnostic);
		assert.equal(f.manager.getLeafId(), entries[0].id, f.diagnostic);
		restore();
		assert.equal(fs.readFileSync(file, "utf8"), bytes, f.diagnostic);
		assert.deepEqual(f.reopen(), [], f.diagnostic);
	},
	"pi-ordinary-ack-before-first-write": (t) => idleAcknowledgement(t, false),
	"pi-ordinary-ack-persistence-disabled": (t) => idleAcknowledgement(t, true),
	async "pi-ordinary-ack-write-failed"(t) {
		const f = await piCompatFixture(t, { expectWriteError: true });
		await flushFirstAssistant(f);
		const file = f.manager.getSessionFile()!;
		const bytes = fs.readFileSync(file, "utf8");
		const restore = f.breakSessionDirectory();
		const { envelopeId, envelope } = await f.publish();
		await f.waitFor(() => f.errors.length > 0, "real failed custom append reported by Pi");
		await f.bounded(f.session.waitForIdle(), "failed custom append settlement");
		assert.deepEqual(f.deliveries, [envelopeId], f.diagnostic);
		assertMessageEndBeforeAppend(f, envelopeId);
		assert.equal(f.errors.length, 1, f.diagnostic);
		assert.equal(f.errors[0].event, "send_message", f.diagnostic);
		assert.match(f.errors[0].error, /ENOTDIR/, f.diagnostic);
		assert.equal(customEntries(f.manager).length, 1, f.diagnostic);
		assert.deepEqual(customEntries(f.manager)[0].details, { envelopeId, from: envelope.from }, f.diagnostic);
		const before = f.observations.filter((event) => event.name === "agent_settled:before").at(-1)!;
		const after = f.observations.filter((event) => event.name === "agent_settled:after").at(-1)!;
		assert.deepEqual(before.files, [envelopeId], f.diagnostic);
		assert.equal(before.entries.length, 1, f.diagnostic);
		assert.deepEqual(after.files, [], `${f.diagnostic}: memory after write failure permits deletion`);
		assert.deepEqual(fs.readdirSync(f.inbox), [], f.diagnostic);
		assert.equal(f.streams.length, 1, `${f.diagnostic}: append failed before another assistant stream`);
		restore();
		assert.equal(fs.readFileSync(file, "utf8"), bytes, f.diagnostic);
		assert.deepEqual(f.reopen(), [], `${f.diagnostic}: no mailbox copy and no recovered custom entry`);
	},
	async "pi-send-rejection-in-flight"(t) {
		const f = await piCompatFixture(t);
		const original = f.session.sendCustomMessage;
		const fault = f.injectSendRejection(new Error("Targeted sendCustomMessage rejection"));
		const { envelopeId, envelope } = await f.publish();
		await f.waitFor(() => f.errors.length === 1, "void bridge extension error callback");
		assert.equal(f.errors[0].event, "send_message", f.diagnostic);
		assert.equal(f.errors[0].error, "Targeted sendCustomMessage rejection", f.diagnostic);
		assert.equal(fault.calls.length, 1, f.diagnostic);
		assert.deepEqual(fault.calls[0][0].details, { envelopeId, from: envelope.from }, f.diagnostic);
		assert.deepEqual(fault.calls[0][1], { triggerTurn: true, deliverAs: "followUp" }, f.diagnostic);
		fault.restore();
		assert.equal(f.session.sendCustomMessage, original, f.diagnostic);
		for (let i = 0; i < 3; i++) {
			await f.session.extensionRunner.emit({ type: "agent_settled" });
			await f.transport.drainInbox();
		}
		assert.deepEqual(f.deliveries, [envelopeId], `${f.diagnostic}: retained in-flight state blocks retries after method restoration`);
		assert.deepEqual(customEntries(f.manager), [], f.diagnostic);
		assert.deepEqual(f.reopen(), [], f.diagnostic);
		assert.equal(f.streams.length, 0, f.diagnostic);
		assert.equal(f.observations.some((event) => event.name === "message_end"), false, f.diagnostic);
		assert.deepEqual(fs.readdirSync(f.inbox), [envelopeId], f.diagnostic);
		assert.deepEqual(JSON.parse(fs.readFileSync(`${f.inbox}/${envelopeId}`, "utf8")), envelope, f.diagnostic);
	},
	async "pi-provider-failure-queued"(t) {
		const f = await piCompatFixture(t);
		assert.equal(f.manager.isPersisted(), true, f.diagnostic);
		assert.ok(f.manager.getSessionFile(), f.diagnostic);
		f.startPrompt();
		await held(f, 1);
		const { envelopeId, envelope } = await f.publish();
		assert.deepEqual(customEntries(f.manager), [], f.diagnostic);
		assert.deepEqual(f.reopen(), [], f.diagnostic);
		assert.deepEqual(fs.readdirSync(f.inbox), [envelopeId], f.diagnostic);
		f.streams[0].fail();
		await held(f, 2);
		assert.equal(f.observations.filter((event) => event.name === "agent_settled:after").length, 0,
			`${f.diagnostic}: continuation starts automatically before settlement, without another prompt`);
		assert.ok(f.manager.getEntries().some((entry) => entry.type === "message" && entry.message.role === "assistant"
			&& entry.message.stopReason === "error" && entry.message.errorMessage === "Injected synthetic provider failure"), f.diagnostic);
		assert.deepEqual(f.errors, [], `${f.diagnostic}: provider failure is not a rejected send`);
		assert.deepEqual(f.deliveries, [envelopeId], f.diagnostic);
		assertMessageEndBeforeAppend(f, envelopeId);
		const expected = structuredClone(customEntries(f.manager));
		assert.equal(expected.length, 1, f.diagnostic);
		assert.equal(expected[0].customType, "herdr-worker.message", f.diagnostic);
		assert.equal(expected[0].display, true, f.diagnostic);
		assert.ok(String(expected[0].content).includes(envelope.message), f.diagnostic);
		assert.deepEqual(expected[0].details, { envelopeId, from: envelope.from }, f.diagnostic);
		assert.ok(fs.existsSync(f.manager.getSessionFile()!), f.diagnostic);
		assert.deepEqual(f.reopen(), expected, `${f.diagnostic}: exact custom entry recovered by opening actual session JSONL`);
		assert.deepEqual(fs.readdirSync(f.inbox), [], f.diagnostic);
		const names = f.observations.map((event) => event.name);
		assert.equal(names.filter((name) => name === "stream:error").length, 1, f.diagnostic);
		assert.equal(names.filter((name) => name === "mailbox:after-delete").length, 1, f.diagnostic);
		assert.ok(names.indexOf("stream:error") < names.indexOf("message_end"), f.diagnostic);
		assert.ok(names.indexOf("mailbox:after-delete") < names.lastIndexOf("stream:held"), f.diagnostic);
		const before = f.observations.find((event) => event.name === "context:before" && event.entries.length === 1)!;
		const after = f.observations.find((event) => event.name === "context:after" && event.entries.length === 1)!;
		assert.deepEqual(before.entries, expected, f.diagnostic);
		assert.deepEqual(before.files, [envelopeId], f.diagnostic);
		assert.equal(before.sessionFileExists, true, f.diagnostic);
		assert.deepEqual(after.entries, expected, f.diagnostic);
		assert.deepEqual(after.files, [], f.diagnostic);
		for (let i = 0; i < 3; i++) await f.transport.drainInbox();
		f.streams[1].release();
		await f.bounded(f.session.waitForIdle(), "automatic continuation settlement");
		for (let i = 0; i < 3; i++) await f.transport.drainInbox();
		const settled = f.observations.filter((event) => event.name === "agent_settled:after");
		assert.equal(settled.length, 1, f.diagnostic);
		assert.deepEqual(settled[0].entries, expected, f.diagnostic);
		assert.deepEqual(settled[0].files, [], f.diagnostic);
		assert.deepEqual(f.deliveries, [envelopeId], f.diagnostic);
		assertMessageEndBeforeAppend(f, envelopeId);
		assert.deepEqual(f.reopen(), expected, `${f.diagnostic}: no duplicate recovered entry after settlement and repeated drains`);
		assert.equal(f.streams.length, 2, f.diagnostic);
	},
};

assert.deepEqual(Object.keys(bodies).sort(), piMailboxCases.map((row) => row.id).sort());
for (const row of piMailboxCases) test(mailboxCaseName(row), { timeout: 20_000 }, bodies[row.id]);

for (const row of lifecyclePiCases) test(mailboxCaseName(row), { timeout: 20_000 }, async (t) => {
	const retry = row.id === "pi-lifecycle-memory-journal-retry";
	const f = await piCompatFixture(t, { lifecycle: true });
	await flushFirstAssistant(f);
	f.startPrompt();
	await held(f, 2);
	const envelopeId = await f.publishLifecycle();
	assert.deepEqual(customEntries(f.manager), [], f.diagnostic);
	assert.deepEqual(fs.readdirSync(f.inbox), [envelopeId], f.diagnostic);
	let restore: (() => void) | undefined;
	let bytes: string | undefined;
	const file = f.manager.getSessionFile()!;
	const journalEntries = (manager: SessionManager) => manager.getEntries().filter((entry): entry is CustomEntry => entry.type === "custom" && entry.customType === LIFECYCLE_JOURNAL_ENTRY);
	const publications: unknown[] = [];
	const unsubscribe = f.events.on("herdr-workers:lifecycle", (event) => publications.push(event));
	t.after(unsubscribe);
	f.beforeContext(() => {
		if (customEntries(f.manager).length && !restore) {
			assert.deepEqual(f.reopen(), customEntries(f.manager), `${f.diagnostic}: custom report is written before journal fault`);
			bytes = fs.readFileSync(file, "utf8");
			restore = f.breakSessionDirectory();
		}
	});
	f.streams[1].release();
	await held(f, 3);
	assert.ok(restore, f.diagnostic);
	f.beforeContext();
	assert.deepEqual(publications, [], f.diagnostic);
	const failedJournal = journalEntries(f.manager);
	assert.equal(failedJournal.length, 1, `${f.diagnostic}: real appendCustomEntry inserted memory before ENOTDIR`);
	assert.equal(f.manager.getLeafId(), failedJournal[0].id, f.diagnostic);
	assert.equal((await queryLifecycle(f.events)).record.lifecycle.acceptedSequence, 0, f.diagnostic);
	assert.deepEqual((await queryLifecycle(f.events)).replay.events, [], f.diagnostic);
	assert.deepEqual(fs.readdirSync(f.inbox), [envelopeId], f.diagnostic);
	assert.deepEqual(f.errors, [], `${f.diagnostic}: worker handled gate catches journal append error`);
	// Observe the underlying filesystem error independently without replacing a Pi persistence method.
	assert.throws(() => f.manager.appendCustomEntry("test-ENOTDIR-probe", {}), { code: "ENOTDIR" }, f.diagnostic);
	if (retry) {
		restore();
		assert.equal(fs.readFileSync(file, "utf8"), bytes, f.diagnostic);
		assert.deepEqual(journalEntries(SessionManager.open(file, f.sessionDir, f.cwd)), [], f.diagnostic);
		await f.session.extensionRunner.emit({ type: "agent_settled" });
		assert.equal(journalEntries(f.manager).length, 2, f.diagnostic);
		assert.equal(journalEntries(SessionManager.open(file, f.sessionDir, f.cwd)).length, 1, f.diagnostic);
		assert.equal(publications.length, 1, f.diagnostic);
	}
	await f.bounded(f.session.reload(), "journal memory restoration through real reload");
	f.assertReloadResources();
	const restored = await queryLifecycle(f.events);
	assert.equal(restored.record.lifecycle.acceptedSequence, 1, f.diagnostic);
	assert.deepEqual(restored.replay.events, [(failedJournal[0].data as LifecycleJournalEntry).event], f.diagnostic);
	assert.equal(publications.length, Number(retry), `${f.diagnostic}: restored journal must not republish history`);
	assert.deepEqual(fs.readdirSync(f.inbox), [], `${f.diagnostic}: retained journal memory permits envelope cleanup after reload`);
	assert.equal(journalEntries(f.manager).length, retry ? 2 : 1, f.diagnostic);
	restore();
	if (!retry) assert.equal(fs.readFileSync(file, "utf8"), bytes, f.diagnostic);
	const reopened = SessionManager.open(file, f.sessionDir, f.cwd);
	assert.deepEqual(customEntries(reopened), customEntries(f.manager), f.diagnostic);
	assert.equal(journalEntries(reopened).length, Number(retry), `${f.diagnostic}: only the successful retry writes a journal`);
	f.streams[2].release();
	await f.bounded(f.session.waitForIdle(), "journal compatibility settlement");
	assert.equal(journalEntries(SessionManager.open(file, f.sessionDir, f.cwd)).length, Number(retry), `${f.diagnostic}: later Pi appends do not backfill the failed journal`);
	assert.equal(publications.length, Number(retry), f.diagnostic);
	assert.deepEqual(f.deliveries, [envelopeId], f.diagnostic);
	unsubscribe();
});

test("controlled host models keep memory and file modes separate", async (t) => {
	for (const persistenceMode of ["file-backed", "deferred-first-write", "disabled", "write-failed"] as const) {
		await t.test(persistenceMode, async (t) => {
			const f = mailboxFixture(t);
			const h = await f.host({ persistenceMode });
			const delivery = { message: { customType: "herdr-worker.message", content: "Storage model payload", display: true,
				details: { envelopeId: "000000000000001-model.json" } },
				options: { triggerTurn: true, deliverAs: "followUp" as const } };
			if (persistenceMode === "write-failed") assert.throws(() => h.appendMessage(delivery), /after memory insertion/);
			else h.appendMessage(delivery);
			const entries = h.entries.filter((entry) => entry.type === "custom_message");
			assert.deepEqual(entries, [{ type: "custom_message", ...delivery.message }]);
			assert.deepEqual(h.reopen().filter((entry) => entry.type === "custom_message"), persistenceMode === "file-backed" ? entries : []);
			if (persistenceMode === "deferred-first-write") {
				h.flushFirstWrite();
				h.appendMessage({ ...delivery, message: { ...delivery.message, content: "Later append" } });
				assert.deepEqual(h.reopen(), h.entries);
			} else if (persistenceMode === "disabled") {
				h.write();
				assert.deepEqual(h.reopen(), []);
				assert.equal(fs.existsSync(f.sessionFile), false);
			} else if (persistenceMode === "write-failed") {
				h.setPersistenceMode("file-backed");
				h.write();
				assert.deepEqual(h.reopen(), h.entries);
			}
		});
	}
});
