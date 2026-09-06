export type MailboxCaseCategory = "Supported guarantee" | "Known contract gap";

export interface MailboxCase {
	id: string;
	file?: string;
	category: MailboxCaseCategory;
	kind: "ordinary" | "lifecycle" | "control";
	boundary: string;
	mode: string;
	assertion: string;
	assumptions: string;
	obsoleteCondition?: string;
}

export const mailboxCaseFile = "tests/extensions/herdr-worker-mailbox.test.ts";

export const mailboxCases = [
	{
		id: "ordinary-publication-receipt",
		category: "Supported guarantee",
		kind: "ordinary",
		boundary: "Temporary write, rename, then RPC receipt",
		mode: "Controlled host, unconsumed mailbox",
		assertion: "Hidden 0600 temporary JSON becomes the exact final envelope before the inbox receipt; no receiver consumption is claimed",
		assumptions: "Private root, live listener PID, valid team target, no filename collision",
	},
	{
		id: "ordinary-queued-retention",
		category: "Supported guarantee",
		kind: "ordinary",
		boundary: "Pi handoff before a matching custom entry",
		mode: "Controlled host, queued only",
		assertion: "Repeated drains and acknowledgement hooks leave the file with one injection and no custom entry in memory or reopened storage",
		assumptions: "One receiver instance, known peer pane, no matching custom entry",
	},
	{
		id: "ordinary-context-ack",
		category: "Supported guarantee",
		kind: "ordinary",
		boundary: "Matching custom entry then context",
		mode: "Controlled host, explicit JSONL write before hook",
		assertion: "One injection, exact custom entry reopened from JSONL, then context deletes the file; a drain alone still skips the in-flight file",
		assumptions: "One receiver instance, known peer pane, successful explicit test-file write",
	},
	{
		id: "ordinary-agent-settled-ack",
		category: "Supported guarantee",
		kind: "ordinary",
		boundary: "Matching custom entry then agent_settled",
		mode: "Controlled host, explicit JSONL write before hook",
		assertion: "One injection, exact custom entry reopened from JSONL, then agent_settled deletes the file; a drain alone still skips the in-flight file",
		assumptions: "One receiver instance, known peer pane, successful explicit test-file write",
	},
	{
		id: "ordinary-fresh-receiver-ack",
		category: "Supported guarantee",
		kind: "ordinary",
		boundary: "Fresh receiver with a matching reopened custom entry",
		mode: "Controlled host, file-only reconstruction",
		assertion: "The replacement cleans the retained filename without reading its payload or injecting another custom message",
		assumptions: "Same pane, root and test session file, matching filename, previous receiver stopped",
	},
	{
		id: "ordinary-headless-listener-ownership",
		category: "Supported guarantee",
		kind: "ordinary",
		boundary: "Headless shutdown beside an interactive listener",
		mode: "Controlled TUI and headless hosts",
		assertion: "Headless shutdown preserves the other listener marker and callbacks; owner teardown removes them, all subscriptions and the private root",
		assumptions: "Headless host never started listening; interactive owner remains alive until teardown",
	},
] as const satisfies readonly MailboxCase[];

export type MailboxCaseId = (typeof mailboxCases)[number]["id"];

const piCaseFile = "tests/extensions/herdr-worker-pi-compat.test.ts";
const piAssumptions = "Project-local Pi coding-agent and resolved agent-core 0.84.4, isolated paths, synthetic assistant streams, no provider requests";

export const piMailboxCases = [
	{
		id: "pi-ordinary-queue-consumption", file: piCaseFile, category: "Supported guarantee", kind: "ordinary",
		boundary: "Held assistant, real steering and follow-up consumption, context and agent_settled",
		mode: "Pi 0.84.4 persisted session, first assistant flush then queued appends",
		assertion: "Both files remain while queued; steering consumes before earlier follow-up; message_end precedes memory append; context deletes only consumed files and reopened JSONL contains exact payloads and IDs",
		assumptions: `${piAssumptions}, one receiver and a known peer pane`,
	},
	{
		id: "pi-custom-first-flush-and-append", file: piCaseFile, category: "Supported guarantee", kind: "ordinary",
		boundary: "Direct custom append before first assistant, first flush, then later custom append",
		mode: "Pi 0.84.4 persisted SessionManager with deferred first write",
		assertion: "Persistence is enabled with a defined but absent file; first assistant flush recovers the exact buffered custom entry and a later append recovers independently",
		assumptions: `${piAssumptions}, public SessionManager append APIs and successful local writes`,
	},
	{
		id: "pi-custom-append-error-memory", file: piCaseFile, category: "Known contract gap", kind: "ordinary",
		boundary: "Real appendCustomMessageEntry filesystem exception after memory insertion",
		mode: "Pi 0.84.4 flushed session, parent directory replaced by a regular file",
		assertion: "ENOTDIR leaves the new custom entry and leaf in memory; restoring the directory recovers unchanged prior bytes and no failed entry",
		assumptions: `${piAssumptions}, fixture-owned parent restored before reopen, no mocked persistence methods`,
		obsoleteCondition: "Pi rolls back memory insertion on append failure or separates committed entries from its memory view",
	},
	{
		id: "pi-ordinary-ack-before-first-write", file: piCaseFile, category: "Known contract gap", kind: "ordinary",
		boundary: "Idle-triggered custom message reaches context before the first assistant completes",
		mode: "Pi 0.84.4 persistence enabled, first write deferred",
		assertion: "Context sees the custom entry and deletes the only mailbox copy while the session path is still absent and no file entry is recoverable",
		assumptions: `${piAssumptions}, brand-new persisted session, known peer, first assistant held at the loss boundary`,
		obsoleteCondition: "Mailbox acknowledgement requires a recoverable session write before deleting the envelope",
	},
	{
		id: "pi-ordinary-ack-persistence-disabled", file: piCaseFile, category: "Known contract gap", kind: "ordinary",
		boundary: "Ordinary context acknowledgement with SessionManager.inMemory",
		mode: "Pi 0.84.4 persistence disabled",
		assertion: "The custom entry permits deletion but no session path or recovered entry exists, even after the assistant completes",
		assumptions: `${piAssumptions}, known peer, inMemory has no restored-entry input`,
		obsoleteCondition: "Mailbox retains envelopes or refuses acknowledgement when session persistence is disabled",
	},
	{
		id: "pi-ordinary-ack-write-failed", file: piCaseFile, category: "Known contract gap", kind: "ordinary",
		boundary: "Idle-triggered custom append fails, then real agent_settled acknowledges memory",
		mode: "Pi 0.84.4 flushed session, parent directory replaced by a regular file",
		assertion: "The real append error leaves a memory entry; agent_settled deletes the mailbox file, and restored prior session bytes contain no recovered custom entry",
		assumptions: `${piAssumptions}, known peer, fixture-owned directory fault remains active through acknowledgement`,
		obsoleteCondition: "Failed session writes cannot supply acknowledged entries or mailbox deletion waits for recoverable storage",
	},
] as const satisfies readonly MailboxCase[];

export type PiMailboxCaseId = (typeof piMailboxCases)[number]["id"];
export const allMailboxCases: readonly MailboxCase[] = [...mailboxCases, ...piMailboxCases];

export function mailboxCaseName(row: MailboxCase): string {
	return `${row.category}: ${row.id}`;
}

export function mailboxCaseCells(row: MailboxCase): string[] {
	return [row.id, row.category, row.file ?? mailboxCaseFile, mailboxCaseName(row), row.kind, row.boundary, row.mode, row.assertion, row.assumptions, row.obsoleteCondition ?? "Not applicable"];
}
