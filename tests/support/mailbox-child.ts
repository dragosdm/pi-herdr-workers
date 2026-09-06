import assert from "node:assert/strict";
import * as fs from "node:fs";
import * as path from "node:path";
import { inboxDir, listeningFile } from "../../mailbox/paths.js";
import { createMailboxTransport, type MailboxTransport } from "../../mailbox/transport.js";
import { createWorkerRpcClient } from "../../rpc/client.js";
import { createControlledPiHost, type ControlledEntry, type ControlledPersistenceMode, type QueuedMessage } from "./controlled-pi-host.js";
import { LIFECYCLE_JOURNAL_ENTRY } from "../../lifecycle/acceptor.js";
import { journals, lifecycleEnvelope, queryLifecycle, recoveryRunId, seedLifecycleAuthority, seedProviderEvidence, selectedTeamEntry } from "./mailbox-lifecycle.js";

export interface ChildConfig {
	root: string;
	scenario: string;
	role: "sender" | "receiver" | "replacement";
	mode: ControlledPersistenceMode;
	boundary: string;
	kind?: "lifecycle" | "control";
	action?: "orchestrated-by" | "bind-run" | "released";
	lossTarget?: "report" | "journal";
	evidence?: "registered" | "started" | "uncertain";
	duplicate?: boolean;
}

export interface ChildSnapshot {
	files: Record<string, string>;
	sent: QueuedMessage[];
	queued: number;
	memory: ControlledEntry[];
	recovered: ControlledEntry[];
	initialRecovered: ControlledEntry[];
	initialEntries: ControlledEntry[];
	initialQueued: number;
	initialInjections: number;
	sessionFileExists: boolean;
	boundaries: string[];
	timeline: string[];
	receiptReturned: boolean;
	removeFailures: number;
	lifecycleEvents: number;
	allMemory: ControlledEntry[];
	allRecovered: ControlledEntry[];
	warnings: string[];
	query?: Awaited<ReturnType<typeof queryLifecycle>>;
}

export interface ChildRecord {
	kind: "checkpoint" | "ready" | "result" | "shutdown" | "reload" | "failure";
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
	let initialEntries: ControlledEntry[] = [];
	let initialQueued = 0;
	let initialInjections = 0;
	let receiverMarker: MailboxTransport | undefined;
	let query: Awaited<ReturnType<typeof queryLifecycle>> | undefined;
	let armed = false;
	const custom = (entries: readonly ControlledEntry[]) => entries.filter((entry) => entry.type === "custom_message");

	function snapshot(): ChildSnapshot {
		return {
			files: Object.fromEntries((fs.existsSync(inbox) ? fs.readdirSync(inbox).sort() : []).map((name) => [name, fs.readFileSync(path.join(inbox, name), "utf8")])),
			sent: h.sentMessages, queued: h.queued.length, memory: custom(h.entries), recovered: custom(h.reopen()),
			initialRecovered, initialQueued, initialInjections, boundaries, timeline: h.timeline, receiptReturned, removeFailures,
			sessionFileExists: fs.existsSync(sessionFile),
			lifecycleEvents: h.timeline.filter((name) => name === "emit:herdr-workers:lifecycle").length,
			allMemory: h.entries, allRecovered: h.reopen(), initialEntries, warnings: h.warnings, query,
		};
	}
	function checkpoint(boundary: string) {
		if (!armed || config.role === "replacement" || config.boundary !== boundary) return;
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
		cwd: config.root, sessionFile: config.kind === "lifecycle" && config.role === "sender" ? path.join(config.root, "sender-session.jsonl") : sessionFile,
		reopen: config.role === "replacement", persistenceMode: "file-backed",
		branchEntry: config.kind === "lifecycle" ? selectedTeamEntry : undefined,
		observeHook(name, event) {
			if (name === "session_shutdown") send({ kind: (event as { reason: string }).reason === "reload" ? "reload" : "shutdown" });
		},
		observeStorage(boundary, entry) {
			if (boundary === "after-memory" && entry?.type === "custom_message") checkpoint("memory-before-write");
			if (boundary === "after-write" && entry?.type === "custom_message") checkpoint("file-before-ack");
			if (boundary === "after-write" && entry?.customType === LIFECYCLE_JOURNAL_ENTRY) checkpoint("journal-before-publication");
		},
		createMailbox(callbacks, defaults) {
			transport = createMailboxTransport({ ...callbacks,
				deliver(envelope, id) {
					const delivery = callbacks.deliver(envelope, id);
					pending.add(delivery);
					void delivery.then(() => pending.delete(delivery), () => pending.delete(delivery));
					return delivery;
				},
				warn(error) { if (!config.kind) throw error; h.warnings.push(String(error)); },
			}, { ...defaults, root: mailboxRoot, makeFilename: () => envelopeId,
				watch: () => ({ close: schedule() }), schedulePoll: () => ({ dispose: schedule() }),
				observeBoundary(boundary) {
					boundaries.push(boundary.name);
					h.timeline.push(`mailbox:${boundary.name}`);
					if (boundary.paneId === "self-pane") {
						checkpoint(boundary.name);
						if (boundary.name === "after-delete") checkpoint("ack-before-write");
					}
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
			if (config.kind === "lifecycle") {
				if (config.role === "sender") {
					const authority = await createControlledPiHost({ cwd: config.root, sessionFile, persistenceMode: "file-backed",
						createMailbox: (callbacks, defaults) => createMailboxTransport(callbacks, { ...defaults, root: mailboxRoot }) });
					try { seedLifecycleAuthority(authority); } finally { await authority.shutdown(); }
					h.appendEntry("herdr-worker", { version: 1, sessionId: "session", workers: [], orchestratedBy: "orchestrator",
						activeRun: { protocol: 2, runId: recoveryRunId, sourceInstanceId: "sender-source", sourceSequence: 0 } });
				} else {
					if (config.lossTarget === "report" && config.mode !== "write-failed") h.setPersistenceMode(config.mode);
					seedLifecycleAuthority(h);
					if (config.evidence) {
						h.appendEntry("herdr-worker.operation.v1", { name: "agent-scout", paneId: "worker-pane", phase: "pane-created", at: 2 });
						if (config.evidence !== "registered") seedProviderEvidence(h, config.evidence);
					}
					// Team selection excludes this branch, while lifecycle restoration still sees every journal.
					h.appendEntry("herdr-worker", { version: 1, sessionId: "session", workers: [], offBranch: true });
				}
			} else if (config.kind === "control") {
				h.appendEntry("herdr-worker", { version: 1, sessionId: "session", workers: [],
					...(config.action === "released" ? { orchestratedBy: "agent-scout", activeRun: {
						protocol: 2, runId: recoveryRunId, sourceInstanceId: "prior-control-source", sourceSequence: 0,
					} } : {}) });
			} else {
				// Retrying cases need saved authority. Fresh deferred and disabled storage have no file.
				if (config.mode !== "deferred-first-write" && config.mode !== "disabled") h.write();
				if (config.role === "sender") h.append({ type: "custom", customType: "herdr-worker", data: {
					version: 1, sessionId: "session", workers: [], orchestratedBy: "orchestrator",
				} });
			}
		}
		initialRecovered = structuredClone(custom(h.entries));
		initialEntries = structuredClone(h.entries);
		initialQueued = h.queued.length;
		initialInjections = h.sentMessages.length;
		h.setPersistenceMode(config.lossTarget === "journal" ? "file-backed" : config.mode);
		await h.start();
		armed = true;
		await Promise.all([...pending]);
		await transport.drainInbox();
		if (config.role === "replacement") {
			while (h.queued.length) h.appendMessage(h.consume());
			await h.hook("context");
			await h.hook("agent_settled");
			for (let i = 0; i < 3; i++) await transport.drainInbox();
			if (config.kind === "lifecycle" && (h.reopen().length || config.lossTarget !== "report")) query = await queryLifecycle(h.events);
			if (config.duplicate) {
				const recorded = h.entries.find((entry) => entry.type === "custom_message")! as ControlledEntry & { details: { report: unknown; from: unknown } };
				fs.writeFileSync(path.join(inbox, "000000000000002-duplicate.json"), JSON.stringify({ type: "lifecycle", ts: 2, ...recorded.details }));
				await transport.drainInbox();
				h.appendMessage(h.consume());
				await h.hook("context");
				query = await queryLifecycle(h.events);
			}
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
			if (config.kind === "lifecycle") {
				await h.tools.get("ReportWorkerRun").execute("report", { status: "completed", result: "Recovery result" });
				receiptReturned = true;
			} else {
				const receipt = await client.send({ target: "orchestrator", message: ordinary.message, mode: "follow-up" }, provider, { timeoutMs: 2000 });
				receiptReturned = true;
				assert.equal(receipt.transport, "inbox");
			}
		} else if (config.kind === "control") {
			transport.writeEnvelope("self-pane", { type: "control", ts: 1, from: lifecycleEnvelope.from, action: config.action,
				...(config.action === "bind-run" ? { binding: { protocol: 2, runId: recoveryRunId } } : {}) });
			await transport.drainInbox();
			checkpoint("control-write-failed");
		} else if (config.evidence) {
			query = await queryLifecycle(h.events);
			checkpoint("evidence-saved");
		} else {
			transport.writeEnvelope("self-pane", config.kind === "lifecycle" ? lifecycleEnvelope : ordinary);
			await transport.drainInbox();
			checkpoint("after-send");
			const delivery = h.consume();
			if (config.mode === "write-failed" && config.lossTarget !== "journal") assert.throws(() => h.appendMessage(delivery), /after memory insertion/);
			else h.appendMessage(delivery);
			if (config.kind === "lifecycle") query = await queryLifecycle(h.events);
			if (config.lossTarget === "journal") h.setPersistenceMode(config.mode);
			await h.hook("context");
			if (config.kind === "lifecycle" && config.mode === "write-failed" && config.lossTarget) {
				assert.equal(journals(h.entries).length, 1);
				assert.equal((await queryLifecycle(h.events)).record.lifecycle.acceptedSequence, 0);
				assert.equal(fs.existsSync(path.join(inbox, envelopeId)), true, "throwing append retains envelope before reload");
				// Reload restores the failed journal from retained memory; this is not file recovery.
				await h.reload();
			}
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
