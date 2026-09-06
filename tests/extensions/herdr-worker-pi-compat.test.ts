import assert from "node:assert/strict";
import * as fs from "node:fs";
import test, { type TestContext } from "node:test";
import { mailboxCaseName, piMailboxCases, type PiMailboxCaseId } from "../support/mailbox-cases.js";
import { customEntries, piCompatFixture } from "../support/pi-compat-fixture.js";
import { mailboxFixture } from "../support/mailbox-fixture.js";

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
};

assert.deepEqual(Object.keys(bodies).sort(), piMailboxCases.map((row) => row.id).sort());
for (const row of piMailboxCases) test(mailboxCaseName(row), { timeout: 20_000 }, bodies[row.id]);

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
