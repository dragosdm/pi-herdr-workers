import * as fs from "node:fs";
import * as path from "node:path";
import { inboxDir, isAcknowledgementFilename, listeningFile } from "./paths.js";

export interface MailboxTransport {
	writeEnvelope<T extends { ts: number }>(paneId: string, envelope: T): void;
	isListening(paneId: string): boolean;
	startListening(): void;
	stopListening(): void;
	isStarted(): boolean;
	drainInbox(): Promise<void>;
	markInFlight(envelopeId: string): void;
	acknowledgeEntries(): void;
}

export interface AcknowledgedEntry {
	customType: string;
	details?: unknown;
}

export interface MailboxCallbacks {
	selfId(): string;
	isInteractive(): boolean;
	isStopped(): boolean;
	deliver(envelope: unknown, envelopeId: string): Promise<void>;
	getAcknowledgedEntries(): readonly AcknowledgedEntry[];
	handleAcknowledged(entry: AcknowledgedEntry): boolean;
	warn(error: unknown): void;
}

export interface MailboxBoundary {
	name: "before-write" | "after-temp-write" | "after-rename" | "after-parse" | "before-delete" | "after-delete";
	paneId: string;
	envelopeId: string;
	fullPath: string;
	temporaryPath?: string;
}

export interface MailboxFileSystem {
	mkdir(dir: string): void;
	write(file: string, content: string, options?: { mode: number }): void;
	read(file: string): string;
	rename(from: string, to: string): void;
	remove(file: string): void;
	readdir(dir: string): string[];
}

export interface MailboxTransportOptions {
	root: string;
	paneId: string;
	makeFilename?: (ts: number) => string;
	watch?: (dir: string, callback: () => void) => { close(): void };
	schedulePoll?: (callback: () => void) => { dispose(): void };
	fileSystem?: Partial<MailboxFileSystem>;
	observeBoundary?: (boundary: MailboxBoundary) => void;
}

export function createMailboxTransport(callbacks: MailboxCallbacks, options: MailboxTransportOptions): MailboxTransport {
	const { root, paneId, observeBoundary } = options;
	const io: MailboxFileSystem = {
		mkdir: (dir) => { fs.mkdirSync(dir, { recursive: true }); },
		write: (file, content, mode) => fs.writeFileSync(file, content, mode),
		read: (file) => fs.readFileSync(file, "utf8"),
		rename: (from, to) => fs.renameSync(from, to),
		remove: (file) => fs.rmSync(file, { force: true }),
		readdir: (dir) => fs.readdirSync(dir),
		...options.fileSystem,
	};
	const makeFilename = options.makeFilename ?? ((ts: number) => `${String(ts).padStart(15, "0")}-${Math.random().toString(36).slice(2, 8)}.json`);
	const watch = options.watch ?? ((dir, callback) => fs.watch(dir, callback));
	const schedulePoll = options.schedulePoll ?? ((callback) => {
		const timer = setInterval(callback, 3000);
		timer.unref?.();
		return { dispose: () => clearInterval(timer) };
	});
	let watcher: { close(): void } | undefined;
	let poller: { dispose(): void } | undefined;
	let draining = false;
	const inFlightEnvelopes = new Set<string>();

	function removeEnvelope(envelopeId: string, fullPath: string) {
		observeBoundary?.({ name: "before-delete", paneId, envelopeId, fullPath });
		io.remove(fullPath);
		observeBoundary?.({ name: "after-delete", paneId, envelopeId, fullPath });
	}

	async function drainInbox() {
		if (draining || !callbacks.isInteractive()) return;
		draining = true;
		try {
			const dir = inboxDir(root, paneId);
			let files: string[] = [];
			try {
				files = io.readdir(dir).filter((f) => f.endsWith(".json")).sort();
			} catch {
				return;
			}
			for (const f of files) {
				if (callbacks.isStopped()) return;
				if (inFlightEnvelopes.has(f)) continue;
				const full = path.join(dir, f);
				const acknowledgedEntry = callbacks.getAcknowledgedEntries().find((e) => (e.details as { envelopeId?: string })?.envelopeId === f);
				if (acknowledgedEntry) {
					if (callbacks.handleAcknowledged(acknowledgedEntry)) removeEnvelope(f, full);
					continue;
				}
				let env: unknown;
				try {
					env = JSON.parse(io.read(full));
				} catch {
					continue; // probably mid-write; next drain picks it up
				}
				observeBoundary?.({ name: "after-parse", paneId, envelopeId: f, fullPath: full });
				if (env) await callbacks.deliver(env, f);
				if (!callbacks.isStopped() && !inFlightEnvelopes.has(f)) removeEnvelope(f, full);
			}
		} catch (error) {
			if (!callbacks.isStopped()) callbacks.warn(error);
		} finally {
			draining = false;
		}
	}

	return {
		writeEnvelope(targetPane, envelope) {
			const dir = inboxDir(root, targetPane);
			io.mkdir(dir);
			const envelopeId = makeFilename(envelope.ts);
			const temporaryPath = path.join(dir, `.${envelopeId}.tmp`);
			const fullPath = path.join(dir, envelopeId);
			const boundary = { paneId: targetPane, envelopeId, fullPath, temporaryPath };
			observeBoundary?.({ ...boundary, name: "before-write" });
			io.write(temporaryPath, JSON.stringify(envelope), { mode: 0o600 });
			observeBoundary?.({ ...boundary, name: "after-temp-write" });
			io.rename(temporaryPath, fullPath);
			observeBoundary?.({ ...boundary, name: "after-rename" });
		},
		isListening(targetPane) {
			try {
				const j = JSON.parse(io.read(listeningFile(root, targetPane)));
				if (typeof j.pid !== "number") return false;
				process.kill(j.pid, 0);
				return true;
			} catch {
				return false;
			}
		},
		startListening() {
			if (!callbacks.isInteractive() || watcher || poller) return;
			const dir = inboxDir(root, paneId);
			io.mkdir(dir);
			io.write(listeningFile(root, paneId), JSON.stringify({ pid: process.pid, ts: Date.now(), id: callbacks.selfId() }));
			try {
				watcher = watch(dir, () => void drainInbox());
			} catch {}
			poller = schedulePoll(() => void drainInbox());
			void drainInbox();
		},
		stopListening() {
			watcher?.close();
			watcher = undefined;
			poller?.dispose();
			poller = undefined;
			try { io.remove(listeningFile(root, paneId)); } catch {}
		},
		isStarted: () => !!watcher || !!poller,
		drainInbox,
		markInFlight: (envelopeId) => { inFlightEnvelopes.add(envelopeId); },
		acknowledgeEntries() {
			for (const entry of callbacks.getAcknowledgedEntries()) {
				if (!callbacks.handleAcknowledged(entry)) continue;
				const id = (entry.details as { envelopeId?: string })?.envelopeId;
				if (id && inFlightEnvelopes.delete(id) && isAcknowledgementFilename(id)) {
					try { removeEnvelope(id, path.join(inboxDir(root, paneId), id)); } catch {}
				}
			}
		},
	};
}
