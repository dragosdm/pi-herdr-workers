import assert from "node:assert/strict";
import * as fs from "node:fs";
import * as os from "node:os";
import * as path from "node:path";
import type { TestContext } from "node:test";
import { inboxDir, listeningFile } from "../../mailbox/paths.js";
import { createMailboxTransport, type MailboxBoundary, type MailboxCallbacks, type MailboxTransport, type MailboxTransportOptions } from "../../mailbox/transport.js";
import { createControlledPiHost, type ControlledPiHost, type ControlledPersistenceMode } from "./controlled-pi-host.js";

interface ScheduledCallback {
	kind: "watch" | "poll";
	callback: () => void;
	disposed: boolean;
}

export async function bounded<T>(promise: Promise<T>, label: string): Promise<T> {
	let timer: ReturnType<typeof setTimeout> | undefined;
	try {
		return await Promise.race([promise, new Promise<never>((_resolve, reject) => {
			timer = setTimeout(() => reject(new Error(`Timed out waiting for ${label}`)), 3000);
		})]);
	} finally { clearTimeout(timer); }
}

export function oneShotRemoveFault(target: string) {
	let failed = false;
	const attempts: string[] = [];
	return {
		attempts,
		remove(file: string) {
			attempts.push(file);
			if (file === target && !failed) {
				failed = true;
				throw new Error("Injected one-shot unlink failure");
			}
			fs.rmSync(file, { force: true });
		},
	};
}

export function mailboxFixture(t: TestContext) {
	const root = fs.mkdtempSync(path.join(os.tmpdir(), "herdr-mailbox-test-"));
	const mailboxRoot = path.join(root, "mailbox");
	const sessionFile = path.join(root, "controlled-session.jsonl");
	const scheduled: ScheduledCallback[] = [];
	const hosts: ControlledPiHost[] = [];
	const listeners: MailboxTransport[] = [];
	const transports: Array<{ transport: MailboxTransport; paneId: string }> = [];
	const boundaries: MailboxBoundary[] = [];
	const releaseGates: Array<() => void> = [];
	const pendingDeliveries = new Set<Promise<void>>();
	let disposed = false;

	function schedule(kind: ScheduledCallback["kind"], callback: () => void) {
		const record = { kind, callback, disposed: false };
		scheduled.push(record);
		return () => { record.disposed = true; };
	}

	async function dispose() {
		if (disposed) return;
		disposed = true;
		const ownedMarkers = transports.filter(({ transport }) => transport.isStarted()).map(({ paneId }) => listeningFile(mailboxRoot, paneId));
		const failures: unknown[] = [];
		try {
			for (const release of releaseGates) release();
			await bounded(Promise.allSettled([...pendingDeliveries]), "pending fixture deliveries");
			for (const host of hosts) {
				try { await host.shutdown(); } catch (error) { failures.push(error); }
			}
			for (const listener of listeners) listener.stopListening();
			try {
				assert.ok(scheduled.every((record) => record.disposed), "all watch and poll callbacks must be disposed");
				assert.ok(transports.every(({ transport }) => !transport.isStarted()), "all listeners must be stopped");
				for (const marker of ownedMarkers) assert.equal(fs.existsSync(marker), false, "owned listener marker must be removed");
				for (const host of hosts) assert.equal(host.events.listenerCount(), 0, "no event subscriptions may remain");
			} catch (error) { failures.push(error); }
		} finally {
			for (const { transport } of transports) if (transport.isStarted()) transport.stopListening();
			fs.rmSync(root, { recursive: true, force: true });
		}
		assert.equal(fs.existsSync(root), false, "fixture root must be removed");
		if (failures.length) throw new AggregateError(failures, "Mailbox fixture teardown failed");
	}
	// Register cleanup before importing or starting the extension so failed setup is covered too.
	t.after(dispose);

	return {
		root, mailboxRoot, sessionFile, scheduled, boundaries, dispose,
		gate() {
			let release!: () => void;
			const promise = new Promise<void>((resolve) => { release = resolve; });
			releaseGates.push(release);
			return { promise, release };
		},
		inbox: (paneId = "self-pane") => inboxDir(mailboxRoot, paneId),
		marker: (paneId = "self-pane") => listeningFile(mailboxRoot, paneId),
		listen(paneId: string) {
			const transport = createMailboxTransport({
				selfId: () => paneId, isInteractive: () => true, isStopped: () => false,
				async deliver() { throw new Error("Receipt fixture must not consume messages"); },
				getAcknowledgedEntries: () => [], handleAcknowledged: () => false,
				warn(error) { throw error; },
			}, {
				root: mailboxRoot, paneId,
				watch: (_dir, callback) => ({ close: schedule("watch", callback) }),
				schedulePoll: (callback) => ({ dispose: schedule("poll", callback) }),
			});
			transports.push({ transport, paneId });
			listeners.push(transport);
			transport.startListening();
			return transport;
		},
		async host(options: {
			mode?: "tui" | "rpc";
			reopen?: boolean;
			persistenceMode?: ControlledPersistenceMode;
			prepare?: (host: ControlledPiHost) => void;
			beforeDeliver?: MailboxCallbacks["deliver"];
			transport?: Omit<MailboxTransportOptions, "root" | "paneId">;
		} = {}) {
			let transport!: MailboxTransport;
			let hostTimeline: string[] | undefined;
			const host = await createControlledPiHost({
				cwd: root, sessionFile, mode: options.mode, reopen: options.reopen, persistenceMode: options.persistenceMode,
				createMailbox(callbacks, defaults) {
					transport = createMailboxTransport({ ...callbacks,
						deliver(envelope, envelopeId) {
							const pending = (async () => {
								await options.beforeDeliver?.(envelope, envelopeId);
								await callbacks.deliver(envelope, envelopeId);
							})();
							pendingDeliveries.add(pending);
							void pending.then(() => pendingDeliveries.delete(pending), () => pendingDeliveries.delete(pending));
							return pending;
						},
					}, {
						...defaults, root: mailboxRoot,
						watch: (dir, callback) => {
							const watcher = options.transport?.watch?.(dir, callback);
							const close = schedule("watch", callback);
							return { close() { watcher?.close(); close(); } };
						},
						schedulePoll: (callback) => {
							const poller = options.transport?.schedulePoll?.(callback);
							const dispose = schedule("poll", callback);
							return { dispose() { poller?.dispose(); dispose(); } };
						},
						makeFilename: options.transport?.makeFilename,
						fileSystem: options.transport?.fileSystem,
						observeBoundary(boundary) {
							boundaries.push(boundary);
							hostTimeline?.push(`mailbox:${boundary.name}`);
							options.transport?.observeBoundary?.(boundary);
						},
					});
					transports.push({ transport, paneId: defaults.paneId });
					return transport;
				},
			});
			hostTimeline = host.timeline;
			hosts.push(host);
			options.prepare?.(host);
			await host.start();
			return { ...host, transport };
		},
	};
}
