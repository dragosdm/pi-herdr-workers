import assert from "node:assert/strict";
import { spawn, type ChildProcess } from "node:child_process";
import { EventEmitter } from "node:events";
import * as fs from "node:fs";
import * as os from "node:os";
import * as path from "node:path";
import type { TestContext } from "node:test";
import { fileURLToPath } from "node:url";
import type { ChildConfig, ChildRecord } from "./mailbox-child.js";
import { inboxDir, listeningFile } from "../../mailbox/paths.js";
import { createMailboxTransport, type MailboxBoundary, type MailboxCallbacks, type MailboxTransport, type MailboxTransportOptions } from "../../mailbox/transport.js";
import { createControlledPiHost, type ControlledPiHost, type ControlledPiHostOptions, type ControlledPersistenceMode } from "./controlled-pi-host.js";

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
	const nativeWatchers: Array<{ watcher: fs.FSWatcher; closed: Promise<void> }> = [];
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
			for (const { watcher } of nativeWatchers) watcher.close();
			await bounded(Promise.all(nativeWatchers.map(({ closed }) => closed)), "native watcher closure before root removal");
			fs.rmSync(root, { recursive: true, force: true });
		}
		assert.equal(fs.existsSync(root), false, "fixture root must be removed");
		if (failures.length) throw new AggregateError(failures, "Mailbox fixture teardown failed");
	}
	// Register cleanup before importing or starting the extension so failed setup is covered too.
	t.after(dispose);

	return {
		root, mailboxRoot, sessionFile, scheduled, boundaries, dispose,
		nativeWatch(dir: string, callback: (event: string, filename: string | null) => void) {
			const watcher = fs.watch(dir, callback);
			const closed = new Promise<void>((resolve) => {
				watcher.once("close", resolve);
				// Node closes the native handle before emitting a watch error, without a close event.
				watcher.once("error", () => resolve());
			});
			nativeWatchers.push({ watcher, closed });
			return { watcher, closed };
		},
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
			observeStorage?: ControlledPiHostOptions["observeStorage"];
			branchEntry?: ControlledPiHostOptions["branchEntry"];
			prepare?: (host: ControlledPiHost) => void;
			beforeDeliver?: MailboxCallbacks["deliver"];
			transport?: Omit<MailboxTransportOptions, "root" | "paneId">;
		} = {}) {
			let transport!: MailboxTransport;
			let hostTimeline: string[] | undefined;
			const host = await createControlledPiHost({
				cwd: root, sessionFile, mode: options.mode, reopen: options.reopen, persistenceMode: options.persistenceMode,
				observeStorage: options.observeStorage, branchEntry: options.branchEntry,
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
			return { ...host, get transport() { return transport; } };
		},
	};
}

interface OwnedMailboxChild {
	child: ChildProcess;
	records: ChildRecord[];
	closed: Promise<{ code: number | null; signal: NodeJS.Signals | null }>;
	record(kind: ChildRecord["kind"], timeoutMs?: number): Promise<ChildRecord>;
	kill(): Promise<void>;
	result(): Promise<ChildRecord>;
	cleanup(): Promise<void>;
}

export function mailboxProcessFixture(t: TestContext) {
	const root = fs.mkdtempSync(path.join(os.tmpdir(), "herdr-mailbox-process-"));
	const children: OwnedMailboxChild[] = [];
	let disposed = false;

	async function wait<T>(promise: Promise<T>, label: string, timeoutMs = 10_000): Promise<T> {
		let timer: ReturnType<typeof setTimeout> | undefined;
		try {
			return await Promise.race([promise, new Promise<never>((_resolve, reject) => {
				timer = setTimeout(() => reject(new Error(`Timed out waiting for ${label}`)), timeoutMs);
			})]);
		} finally { clearTimeout(timer); }
	}

	function start(config: Omit<ChildConfig, "root">): OwnedMailboxChild {
		assert.equal(disposed, false);
		const child = spawn(process.execPath, ["--import", "tsx", fileURLToPath(new URL("./mailbox-child.ts", import.meta.url)),
			JSON.stringify({ ...config, root })], {
			stdio: ["ignore", "pipe", "pipe", "pipe"],
			env: { ...process.env, HERDR_ENV: "1", HERDR_PANE_ID: config.role === "sender" ? "worker-pane" : "self-pane", HERDR_TAB_ID: "tab-1" },
		});
		const changes = new EventEmitter();
		const records: ChildRecord[] = [];
		let bytes = "";
		let stderr = "";
		let stdout = "";
		let failure: Error | undefined;
		let exited = false;
		const closed = new Promise<{ code: number | null; signal: NodeJS.Signals | null }>((resolve) => {
			child.once("error", (error) => { failure = error; changes.emit("change"); });
			child.once("close", (code, signal) => { exited = true; resolve({ code, signal }); changes.emit("change"); });
		});
		child.stdout!.on("data", (chunk) => { stdout += chunk; });
		child.stderr!.on("data", (chunk) => { stderr += chunk; });
		const pipe = child.stdio[3] as NodeJS.ReadableStream;
		pipe.setEncoding("utf8");
		pipe.on("error", (error: Error) => { failure = error; changes.emit("change"); });
		pipe.on("data", (chunk: string) => {
			bytes += chunk;
			let end: number;
			while ((end = bytes.indexOf("\n")) !== -1) {
				const line = bytes.slice(0, end);
				bytes = bytes.slice(end + 1);
				try {
					const record = JSON.parse(line) as ChildRecord;
					assert.equal(record.scenario, config.scenario);
					assert.equal(record.role, config.role);
					assert.equal(record.pid, child.pid);
					assert.ok(["checkpoint", "ready", "result", "shutdown", "reload", "failure"].includes(record.kind));
					records.push(record);
					if (record.kind === "failure") failure = new Error(record.error);
				} catch (error) { failure = error as Error; }
			}
			changes.emit("change");
		});

		async function record(kind: ChildRecord["kind"], timeoutMs?: number) {
			let listener = () => {};
			try {
				return await wait(new Promise<ChildRecord>((resolve, reject) => {
					listener = () => {
						if (failure) return reject(failure);
						const found = records.find((item) => item.kind === kind);
						if (found) return resolve(found);
						if (exited) reject(new Error(`Child exited before ${kind}; trailing=${JSON.stringify(bytes)} stderr=${stderr} stdout=${stdout}`));
					};
					changes.on("change", listener);
					listener();
				}), `${config.scenario} ${kind}`, timeoutMs);
			} finally { changes.off("change", listener); }
		}

		const owned = {
			child, records, closed, record,
			async kill() {
				assert.equal(exited, false, "child must still be blocked at its checkpoint");
				assert.equal(child.kill("SIGKILL"), true);
				assert.deepEqual(await wait(closed, "SIGKILL exit"), { code: null, signal: "SIGKILL" });
				assert.equal(records.some((item) => item.kind === "shutdown"), false, "SIGKILL must bypass graceful shutdown");
				assert.equal(bytes, "", "checkpoint pipe must contain complete JSON lines");
				assert.equal(failure, undefined);
			},
			async result() {
				const result = await record("result");
				assert.deepEqual(await wait(closed, "replacement exit"), { code: 0, signal: null }, stderr);
				assert.equal(bytes, "", "truncated replacement result");
				assert.equal(failure, undefined);
				assert.equal(records.filter((item) => item.kind === "result").length, 1);
				assert.equal(records.filter((item) => item.kind === "shutdown").length, 1);
				return result;
			},
			async cleanup() {
				if (!exited) child.kill("SIGKILL");
				await wait(closed, "owned child cleanup");
				assert.equal(changes.listenerCount("change"), 0);
				changes.removeAllListeners();
			},
		};
		children.push(owned);
		return owned;
	}

	async function dispose() {
		if (disposed) return;
		disposed = true;
		const results = await Promise.allSettled(children.map((child) => child.cleanup()));
		// Never remove files while an owned process might still be using them.
		assert.ok(results.every((result) => result.status === "fulfilled"), JSON.stringify(results));
		fs.rmSync(root, { recursive: true, force: true });
		assert.equal(fs.existsSync(root), false);
		assert.ok(children.every(({ child }) => child.exitCode !== null || child.signalCode !== null));
	}
	t.after(dispose);
	return { root, start, children, dispose };
}
