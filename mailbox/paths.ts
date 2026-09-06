import * as os from "node:os";
import * as path from "node:path";

export function mailboxRoot(): string {
	const base = process.env.XDG_RUNTIME_DIR || os.tmpdir();
	return path.join(base, "pi-herdr-worker");
}

export function mailboxDir(root: string, paneId: string): string {
	return path.join(root, paneId.replace(/[^a-zA-Z0-9_-]/g, "_"));
}

export function inboxDir(root: string, paneId: string): string {
	return path.join(mailboxDir(root, paneId), "inbox");
}

export function listeningFile(root: string, paneId: string): string {
	return path.join(mailboxDir(root, paneId), "listening.json");
}

export function isAcknowledgementFilename(id: string): boolean {
	return /^[a-zA-Z0-9_.-]+\.json$/.test(id);
}
