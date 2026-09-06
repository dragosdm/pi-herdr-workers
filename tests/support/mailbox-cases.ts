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

function supported<const Id extends string>(id: Id, kind: MailboxCase["kind"], boundary: string, assertion: string,
	assumptions = "One receiver, known peer, private trusted root", mode = "Controlled host, explicit queue and memory") {
	return { id, category: "Supported guarantee" as const, kind, boundary, assertion, assumptions, mode };
}

function gap<const Id extends string>(id: Id, kind: MailboxCase["kind"], boundary: string, assertion: string,
	assumptions: string, obsoleteCondition: string, mode = "Controlled host, queued only") {
	return { id, category: "Known contract gap" as const, kind, boundary, assertion, assumptions, obsoleteCondition, mode };
}

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
	supported("snapshot-order-overlap", "ordinary", "Blocked first delivery with overlapping watch and poll triggers",
		"One sorted snapshot delivers a, b, then the later timestamp once each; overlapping triggers do not rescan and a late file waits for the next explicit drain"),
	supported("real-watch-delivery", "ordinary", "Publish after listener startup with poll callback disabled",
		"A bounded real fs.watch notification reaches the extension and queues one custom message; the envelope remains", "Private root, live real watcher, no poll invocation", "Controlled host, real fs.watch"),
	supported("watch-failure-poll-fallback", "ordinary", "Watch creation throws before controlled polling",
		"Listener remains started; explicit poll callback delivers the published file once and shutdown removes the marker"),
	supported("dead-listener-prompt-fallback", "ordinary", "Listener marker names an exited fixture-owned child",
		"Current PID is live, dead child PID is not; real RPC send returns prompt fallback without a mailbox publication", "Owned child has exited, known peer, private marker, no arbitrary PID probes", "Controlled Herdr prompt and real PID liveness"),
	supported("partial-json-and-hidden-temp", "ordinary", "Incomplete final JSON is later completed",
		"Partial final JSON remains unreadable across drains; hidden temporary JSON is ignored; completing the final file injects once"),
	supported("parsed-invalid-cleanup", "ordinary", "Successful parse followed by invalid or falsy payload",
		"Invalid shapes and falsy JSON are deleted without custom messages; no parse failure is treated as invalid work"),
	supported("ordinary-priority-routing", "ordinary", "Real extension handoff to modeled Pi queue",
		"Priority true maps to steer and false to followUp; both set display and triggerTurn and retain their files"),
	supported("lifecycle-follow-up-routing", "lifecycle", "Bound report handoff, memory append, then context",
		"Valid report uses followUp, remains while queued, then produces one journal event and cleanup at context", "Saved valid run metadata selects worker pane and contract 2; real lifecycle acceptor", "Controlled host, explicit report write and real journal append"),
	supported("ordinary-peer-rejection", "ordinary", "Unknown pane and formerly known pane after roster movement",
		"Both unauthorized envelopes are deleted without injection; the currently known pane still delivers"),
	supported("lifecycle-peer-rejection", "lifecycle", "Unknown or stale live peer and mismatched bound pane",
		"Valid reports from unknown, old, and live-but-not-bound panes are deleted without injection or accepted events", "Saved contract-2 binding, controlled peer movement, private root"),
	supported("lifecycle-incoming-unbound", "lifecycle", "Known peer report arrives without saved run binding",
		"Unbound incoming report is deleted before custom-message injection or lifecycle acceptance"),
	supported("lifecycle-recorded-unbound-pending", "lifecycle", "Already recorded valid report without run binding",
		"Matching recorded report remains pending through hooks and drain with no injection, deletion, or lifecycle event", "Explicit recorded report and marked filename, no saved binding", "Controlled host, recorded memory entry"),
	supported("delivery-reject-before-mark", "ordinary", "Delivery callback rejects before markInFlight",
		"File and remaining snapshot files survive the ended pass; later explicit drain retries and injects each once", "One-shot callback fault before the real extension handler, one receiver", "Controlled callback fault model"),
	gap("send-throw-after-mark", "ordinary", "Synchronous pi.sendMessage throws after markInFlight",
		"File remains readable but same-instance drains and hooks cannot retry without an entry; a fresh host retries from the file",
		"Model-only synchronous void-API fault, old host stopped before replacement, no retained queue",
		"Failed handoffs clear or reconcile in-flight state so the same receiver can retry", "Controlled synchronous send failure, file-only host replacement"),
	supported("hook-unlink-retry", "ordinary", "Hook unlink throws after clearing in-flight",
		"A second hook does not retry unlink; a later drain uses the written session entry to remove the file without another injection", "One-shot local unlink fault; report already written and reopened", "Controlled host, file-backed acknowledgement"),
	supported("drain-unlink-retry", "ordinary", "Acknowledged-entry drain unlink throws",
		"Outer catch ends the pass before remaining files; next drain retries cleanup without another custom send", "Matching entries, no in-flight IDs, one-shot unlink fault", "Controlled host, written session entries"),
	supported("lexical-pane-containment", "ordinary", "Publication with traversal, absolute-looking, separator and control-character pane IDs",
		"Generated final and temporary paths stay lexically inside the mailbox, contain exact envelopes, and leave outside sentinels unchanged", "Private trusted root without symlinks; extension-style numeric timestamps", "Real filesystem publication"),
	supported("acknowledgement-allowlist", "ordinary", "Malformed acknowledgement IDs deliberately marked in flight",
		"Hooks reject traversal, absolute, separator, control-character and non-json IDs before unlink; accepted dotted names are still removed", "Private trusted root, matching recorded entries and explicit markInFlight", "Controlled host, memory acknowledgement"),
	gap("suffix-reverses-producer-order", "ordinary", "Equal timestamp writes with reverse-sorted suffixes",
		"Second producer call is injected first because the snapshot sorts final filenames", "Fixed valid filenames, same timestamp, one receiver",
		"Publication adds and enforces producer sequencing instead of random suffix order"),
	gap("filename-collision-overwrite", "ordinary", "Two writes use the same final filename",
		"Only the second envelope survives and is injected; the first payload is overwritten", "Forced final-name collision before any drain",
		"Exclusive publication detects collisions and preserves both messages"),
	gap("two-receivers-duplicate", "ordinary", "Two transports reach delivery before either acknowledges",
		"Both instances inject the same filename into separate queues; one later acknowledgement deletes the shared file", "Two hosts share one inbox, independent in-flight sets, explicit delivery barrier",
		"Receiver ownership or inter-process coordination grants exclusive delivery"),
	gap("bind-assignment-reordered", "control", "Same timestamp bind-run publication precedes assignment with earlier suffix",
		"Assignment reaches the worker before active binding is set, then bind-run persists the binding", "Known peer, valid binding, fixed reverse-sorted filenames, no readiness listener",
		"Control and assignment sequencing enforces binding before assignment handoff"),
	gap("pane-sanitization-alias", "ordinary", "Distinct pane IDs sanitize to the same directory",
		"Both claimed recipient IDs publish into the same inbox", "Private root; a/b and a_b are distinct inputs",
		"Pane directory encoding is injective or collisions are rejected", "Real filesystem publication"),
	gap("symlink-entry-read", "ordinary", "Final inbox entry links to a sentinel outside mailbox subtree",
		"Drain reads and injects the outside sentinel payload; acknowledgement removes only the symlink", "All symlink targets stay inside the disposable test root",
		"No-follow file opening rejects symlinked inbox entries"),
	gap("symlink-ancestor-write", "ordinary", "Recipient directory is a symlink to outside mailbox subtree",
		"Publication follows the ancestor and writes the final envelope outside the lexical mailbox root", "Symlink and redirected files remain inside disposable test root",
		"Directory ownership and no-follow containment reject redirected ancestors", "Real filesystem publication"),
	gap("known-pane-impersonation", "ordinary", "Local writer claims a known pane with unrelated sender ID",
		"Roster validation permits the forged message and preserves the claimed sender in details", "Fixture writes the file directly; claimed pane is currently known",
		"Receiver authenticates the writer rather than only checking the claimed pane"),
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
	{
		id: "pi-send-rejection-in-flight", file: piCaseFile, category: "Known contract gap", kind: "ordinary",
		boundary: "Individual session.sendCustomMessage returns an injected rejected promise",
		mode: "Pi 0.84.4 targeted asynchronous send fault, unchanged void extension bridge",
		assertion: "Pi reports one send_message error; no custom entry exists; the readable envelope stays in flight and later drains do not retry even after method restoration",
		assumptions: `${piAssumptions}, injected session method restored at teardown, not a naturally occurring queue failure`,
		obsoleteCondition: "Pi handoff failures can reconcile the mailbox in-flight state and permit same-instance retry",
	},
	{
		id: "pi-provider-failure-queued", file: piCaseFile, category: "Supported guarantee", kind: "ordinary",
		boundary: "Active synthetic assistant fails while a custom follow-up is queued",
		mode: "Pi 0.84.4 persistent session, real queue, synthetic provider error",
		assertion: "Automatic continuation consumes the queued message once, context deletes its envelope, and the next assistant starts without another prompt; actual session-file reopen recovers the exact custom entry before and after settlement with no duplicate delivery",
		assumptions: `${piAssumptions}, persistence enabled with successful local writes, one receiver and known peer, no retry or compaction; excludes disabled or failed storage and other Pi versions`,
	},
	{
		id: "pi-ordinary-reload-queued", file: piCaseFile, category: "Known contract gap", kind: "ordinary",
		boundary: "session.reload while streaming with an unconsumed custom follow-up",
		mode: "Pi 0.84.4 retained agent queue and SessionManager, replacement extension",
		assertion: "Reload retains the first queued delivery; the replacement injects the same filename again; two custom messages are consumed and recovered from actual JSONL",
		assumptions: `${piAssumptions}, direct session.reload API while streaming, not the busy-guarded TUI /reload command; successful local writes and known peer`,
		obsoleteCondition: "Reload shares delivery identities with retained Pi queues or reconciles queued filenames before reinjection",
	},
] as const satisfies readonly MailboxCase[];

export type PiMailboxCaseId = (typeof piMailboxCases)[number]["id"];

const recoveryFile = "tests/extensions/herdr-worker-recovery.test.ts";
const recoveryAssumptions = "SIGKILL at a synchronous pipe barrier, new process, same pane/root/session file, saved team authority when retrying, known peer, one receiver, readable local files; not power-loss durability";
function recoverySupported<const Id extends string>(id: Id, boundary: string, assertion: string, mode = "Controlled host, file-backed message storage") {
	return { ...supported(id, "ordinary", boundary, assertion, recoveryAssumptions, mode), file: recoveryFile };
}
function recoveryGap<const Id extends string>(id: Id, boundary: string, assertion: string, mode: string, obsoleteCondition: string) {
	return { ...gap(id, "ordinary", boundary, assertion, recoveryAssumptions, obsoleteCondition, mode), file: recoveryFile };
}

export const recoveryMailboxCases = [
	recoveryGap("ordinary-before-create", "Sender before envelope write",
		"No send receipt returns and no envelope or custom entry survives; replacement has no pending message to recover",
		"Controlled sender, previously saved receiver authority only", "Durable sender intent records pending work before publication"),
	recoverySupported("ordinary-partial-temp", "Sender writes a real prefix to the temporary file before completing write",
		"Partial temporary bytes survive unchanged but are ignored; no final file, receipt, injection, or recovered custom entry"),
	recoverySupported("ordinary-before-rename", "Sender completes temporary write before rename",
		"Complete temporary JSON survives unchanged but is unpublished; no final file, receipt, injection, or recovered custom entry"),
	recoverySupported("ordinary-after-rename", "Sender after rename before receipt and before receiver startup",
		"Final JSON survives without a returned sender receipt; replacement injects once, removes the envelope after writing, and reopens the exact custom entry"),
	recoverySupported("ordinary-after-read", "Receiver after parse before handoff",
		"The retained envelope is parsed again in a fresh process and injected once; exact written custom entry survives acknowledgement"),
	recoverySupported("ordinary-after-send", "Receiver queued custom payload before append",
		"Old process injected once but its queue is lost; replacement starts with no queue or custom entry and injects once from the retained file, then writes and acknowledges it"),
	recoverySupported("ordinary-memory-before-write", "Receiver memory append before file write and acknowledgement",
		"Memory-only entry does not reopen; retained envelope retries once in a new process and produces a recoverable custom entry"),
	...(["deferred-first-write", "disabled", "write-failed"] as const).map((mode) => recoveryGap(
		`ordinary-ack-before-write-${mode}` as const, "Receiver acknowledgement deletes envelope without a recoverable custom write",
		"Old memory contains one custom entry but the envelope is absent; replacement recovers no custom entry and has no file to retry",
		`Controlled host, ${mode} message storage`,
		"Mailbox deletion requires a recoverable custom entry rather than memory visibility")),
	recoverySupported("ordinary-file-before-ack", "Custom entry written before any acknowledgement hook",
		"Exact custom entry reopens in replacement; matching filename is deleted without parsing the envelope or injecting again"),
	recoverySupported("ordinary-before-delete", "Acknowledgement reached before unlink",
		"Custom entry and envelope survive; replacement reopens the entry and deletes the matching filename without parsing or reinjection"),
	recoverySupported("ordinary-delete-failed", "One-shot hook unlink error after recoverable custom write",
		"Failed unlink leaves the envelope; replacement uses the exact recovered entry to clean it without parsing or reinjection"),
	recoverySupported("ordinary-after-delete", "Envelope removed after recoverable custom write",
		"Custom entry survives alone in reopened storage; replacement performs no mailbox replay or deletion"),
	{ ...supported("ordinary-reload-visible", "ordinary", "Extension replacement with reason reload and visible custom entry",
		"Retained memory suppresses reinjection and permits cleanup despite absent session file; no disk recovery is claimed",
		"Same controlled host, memory entries retained, old extension disposed, known peer", "Controlled host, disabled message storage and retained memory"), file: recoveryFile },
	{ ...gap("ordinary-reload-queued", "ordinary", "Extension replacement with reason reload and queued custom payload but no entry",
		"Old queued delivery survives and replacement injects again; two injections and two consumed memory entries share the same envelope ID",
		"Same controlled host and queue retained, old extension disposed, known peer", "Reload shares or reconciles delivery identities with the retained queue",
		"Controlled host, disabled message storage and retained queue"), file: recoveryFile },
] as const satisfies readonly MailboxCase[];

export type RecoveryMailboxCaseId = (typeof recoveryMailboxCases)[number]["id"];
export const allMailboxCases: readonly MailboxCase[] = [...mailboxCases, ...piMailboxCases, ...recoveryMailboxCases];

export function mailboxCaseName(row: MailboxCase): string {
	return `${row.category}: ${row.id}`;
}

export function mailboxCaseCells(row: MailboxCase): string[] {
	return [row.id, row.category, row.file ?? mailboxCaseFile, mailboxCaseName(row), row.kind, row.boundary, row.mode, row.assertion, row.assumptions, row.obsoleteCondition ?? "Not applicable"];
}
