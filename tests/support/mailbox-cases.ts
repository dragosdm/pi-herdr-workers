export type MailboxCaseCategory = "Supported guarantee" | "Known contract gap";

export interface MailboxCase {
	id: string;
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

export function mailboxCaseName(row: MailboxCase): string {
	return `${row.category}: ${row.id}`;
}

export function mailboxCaseCells(row: MailboxCase): string[] {
	return [row.id, row.category, mailboxCaseFile, mailboxCaseName(row), row.kind, row.boundary, row.mode, row.assertion, row.assumptions, row.obsoleteCondition ?? "Not applicable"];
}
