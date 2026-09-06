import assert from "node:assert/strict";
import * as fs from "node:fs";
import * as path from "node:path";
import { inboxDir, listeningFile } from "../../mailbox/paths.js";
import { createMailboxTransport, type MailboxTransport } from "../../mailbox/transport.js";
import { createWorkerRpcClient } from "../../rpc/client.js";
import { createControlledPiHost, type ControlledEntry, type ControlledPersistenceMode, type QueuedMessage } from "./controlled-pi-host.js";

export interface ChildConfig {
	root: string;
	scenario: string;
	role: "sender" | "receiver" | "replacement";
	mode: ControlledPersistenceMode;
	boundary: string;
}

export interface ChildSnapshot {
	files: Record<string, string>;
	sent: QueuedMessage[];
	queued: number;
	memory: ControlledEntry[];
	recovered: ControlledEntry[];
	initialRecovered: ControlledEntry[];
	initialQueued: number;
	initialInjections: number;
	sessionFileExists: boolean;
	boundaries: string[];
	timeline: string[];
	receiptReturned: boolean;
	removeFailures: number;
	lifecycleEvents: number;
}

export interface ChildRecord {
	kind: "checkpoint" | "ready" | "result" | "shutdown" | "failure";
	scenario: string;
	role: ChildConfig["role"];
	pid: number;
	boundary?: string;
	snapshot?: ChildSnapshot;
	error?: string;
}

const config: ChildConfig = JSON.parse(process.argv[2]);
const envelopeId = "000000000000001-recovery.json";
const ordinary = {
	type: "message", ts: 1, priority: false, message: "Ordinary process recovery payload",
	from: { id: "agent-scout", paneId: "worker-pane", name: "agent-scout", role: "worker" },
};

function send(record: Omit<ChildRecord, "scenario" | "role" | "pid">) {
	const bytes = Buffer.from(JSON.stringify({ ...record, scenario: config.scenario, role: config.role, pid: process.pid }) + "\n");
	assert.equal(fs.writeSync(3, bytes), bytes.length, "dedicated pipe write must be complete");
}

async function main() {
	const mailboxRoot = path.join(config.root, "mailbox");
	const sessionFile = path.join(config.root, "controlled-session.jsonl");
	const inbox = inboxDir(mailboxRoot, "self-pane");
	const schedules: Array<{ disposed: boolean }> = [];
	const boundaries: string[] = [];
	const pending = new Set<Promise<void>>();
	let transport!: MailboxTransport;
	let receiptReturned = false;
	let removeFailures = 0;
	let initialRecovered: ControlledEntry[] = [];
	let initialQueued = 0;
	let initialInjections = 0;
	let receiverMarker: MailboxTransport | undefined;
	const custom = (entries: readonly ControlledEntry[]) => entries.filter((entry) => entry.type === "custom_message");

	function snapshot(): ChildSnapshot {
		return {
			files: Object.fromEntries((fs.existsSync(inbox) ? fs.readdirSync(inbox).sort() : []).map((name) => [name, fs.readFileSync(path.join(inbox, name), "utf8")])),
			sent: h.sentMessages, queued: h.queued.length, memory: custom(h.entries), recovered: custom(h.reopen()),
			initialRecovered, initialQueued, initialInjections, boundaries, timeline: h.timeline, receiptReturned, removeFailures,
			sessionFileExists: fs.existsSync(sessionFile),
			lifecycleEvents: h.timeline.filter((name) => name === "emit:herdr-workers:lifecycle").length,
		};
	}
	function checkpoint(boundary: string) {
		if (config.role === "replacement" || config.boundary !== boundary) return;
		send({ kind: "checkpoint", boundary, snapshot: snapshot() });
		Atomics.wait(new Int32Array(new SharedArrayBuffer(4)), 0, 0, 30_000);
		// Exit directly: a caught observer error could otherwise let publication or deletion continue.
		send({ kind: "failure", error: `Kill barrier expired at ${boundary}` });
		process.exit(1);
	}
	function schedule() {
		const item = { disposed: false };
		schedules.push(item);
		return () => { item.disposed = true; };
	}

	const h = await createControlledPiHost({
		cwd: config.root, sessionFile, reopen: config.role === "replacement", persistenceMode: "file-backed",
		observeHook(name) { if (name === "session_shutdown") send({ kind: "shutdown" }); },
		observeStorage(boundary, entry) {
			if (boundary === "after-memory" && entry?.type === "custom_message") checkpoint("memory-before-write");
			if (boundary === "after-write" && custom(h.entries).length) checkpoint("file-before-ack");
		},
		createMailbox(callbacks, defaults) {
			transport = createMailboxTransport({ ...callbacks,
				deliver(envelope, id) {
					const delivery = callbacks.deliver(envelope, id);
					pending.add(delivery);
					void delivery.then(() => pending.delete(delivery), () => pending.delete(delivery));
					return delivery;
				},
				warn(error) { throw error; },
			}, { ...defaults, root: mailboxRoot, makeFilename: () => envelopeId,
				watch: () => ({ close: schedule() }), schedulePoll: () => ({ dispose: schedule() }),
				observeBoundary(boundary) {
					boundaries.push(boundary.name);
					h.timeline.push(`mailbox:${boundary.name}`);
					checkpoint(boundary.name);
					if (boundary.name === "after-delete") checkpoint("ack-before-write");
				},
				fileSystem: {
					write(file, content, options) {
						if (config.role === "sender" && config.boundary === "partial-temp" && file.endsWith(".tmp")) {
							fs.writeFileSync(file, content.slice(0, 17), options);
							checkpoint("partial-temp");
						}
						fs.writeFileSync(file, content, options);
					},
					remove(file) {
						if (config.role === "receiver" && config.boundary === "delete-failed" && file === path.join(inbox, envelopeId) && !removeFailures) {
							removeFailures++;
							throw new Error("One-shot child unlink failure");
						}
						fs.rmSync(file, { force: true });
					},
				},
			});
			return transport;
		},
	});
	try {
		if (config.role !== "replacement") {
			// Retrying cases need saved authority. Fresh deferred and disabled storage have no file.
			if (config.mode !== "deferred-first-write" && config.mode !== "disabled") h.write();
			if (config.role === "sender") h.append({ type: "custom", customType: "herdr-worker", data: {
				version: 1, sessionId: "session", workers: [], orchestratedBy: "orchestrator",
			} });
		}
		initialRecovered = structuredClone(custom(h.entries));
		initialQueued = h.queued.length;
		initialInjections = h.sentMessages.length;
		h.setPersistenceMode(config.mode);
		await h.start();
		await Promise.all([...pending]);
		await transport.drainInbox();
		if (config.role === "replacement") {
			while (h.queued.length) h.appendMessage(h.consume());
			await h.hook("context");
			await h.hook("agent_settled");
			for (let i = 0; i < 3; i++) await transport.drainInbox();
		} else if (config.boundary === "never-reached") {
			send({ kind: "ready" });
			await new Promise((_resolve, reject) => setTimeout(() => reject(new Error("Unreached checkpoint scenario expired")), 30_000));
		} else if (config.role === "sender") {
			receiverMarker = createMailboxTransport({ selfId: () => "orchestrator", isInteractive: () => true, isStopped: () => false,
				async deliver() { throw new Error("Receiver must not run before sender termination"); },
				getAcknowledgedEntries: () => [], handleAcknowledged: () => false, warn(error) { throw error; },
			}, { root: mailboxRoot, paneId: "self-pane", watch: () => ({ close: schedule() }), schedulePoll: () => ({ dispose: schedule() }) });
			receiverMarker.startListening();
			const client = createWorkerRpcClient({ events: h.events });
			const provider = await client.probe({ timeoutMs: 2000 });
			const receipt = await client.send({ target: "orchestrator", message: ordinary.message, mode: "follow-up" }, provider, { timeoutMs: 2000 });
			receiptReturned = true;
			assert.equal(receipt.transport, "inbox");
		} else {
			transport.writeEnvelope("self-pane", ordinary);
			await transport.drainInbox();
			checkpoint("after-send");
			const delivery = h.consume();
			if (config.mode === "write-failed") assert.throws(() => h.appendMessage(delivery), /after memory insertion/);
			else h.appendMessage(delivery);
			await h.hook("context");
			checkpoint("delete-failed");
		}
		if (config.role !== "replacement") throw new Error(`Requested checkpoint was never reached: ${config.boundary}`);
		const result = snapshot();
		await h.shutdown();
		assert.ok(schedules.every((item) => item.disposed));
		assert.equal(transport.isStarted(), false);
		assert.equal(fs.existsSync(listeningFile(mailboxRoot, "self-pane")), false);
		send({ kind: "result", snapshot: result });
	} finally {
		receiverMarker?.stopListening();
		await h.shutdown();
	}
}

main().catch((error) => { send({ kind: "failure", error: String(error?.stack ?? error) }); process.exitCode = 1; });
