# Mailbox guarantees

This matrix covers the executing Phase 1 cases, not the planned interruption suite. Run it with:

```sh
node --import tsx --test tests/extensions/herdr-worker-mailbox.test.ts tests/extensions/herdr-worker-adapter.test.ts
```

The production transport publishes JSON through a synchronous temporary-file write and rename. The worker extension validates the claimed sender pane and invokes Pi's custom-message API. The transport retains the file while that filename is in flight. The `context` and `agent_settled` hooks acknowledge matching custom entries visible in SessionManager memory. Lifecycle entries also pass through the extension's existing handled/retry gate before cleanup.

A send receipt proves publication, not receiver acceptance or execution. A matching ordinary entry on a fresh receiver permits cleanup without reading the envelope again. In an existing receiver, a drain skips in-flight filenames before looking for matching entries, so the acknowledgement hooks still matter.

## Evidence limits

The controlled host separates its queue, memory entries, and a test JSONL file. Consumption, memory append, file write, reopen, and hooks are explicit operations. Its JSONL is a test persistence model, not Pi's session-file format. File-only reconstruction here uses a fresh host in the same test process after stopping the previous receiver. It is not a process-kill test or a real Pi compatibility result.

The acknowledgement cases deliberately write and reopen the test file before invoking the hook. That order proves those cases, not a requirement enforced by production. `getEntries()` does not prove a successful disk write. Pi can buffer a fresh session or disable persistence; deletion can therefore precede a recoverable session copy. This phase does not claim universal no-loss, exactly-once delivery, power-loss durability, or assignment completion.

Filesystem operations run under a private temporary root. Watch and poll callbacks are captured, not driven by timing. Each fixture checks listener-marker ownership, callback disposal, event unsubscription, and root removal. These tests need no Herdr panes, credentials, model requests, or global Pi installation. Real Pi compatibility, interruption boundaries, ordering races, and hostile filesystem inputs are outside this initial matrix.

## Executable matrix

The test names include their category and stable ID. The test file checks every cell against the metadata that registers its executing bodies. Categories are exactly `Supported guarantee` and `Known contract gap`. No gap reproduction is included yet. Future gap rows must assert the current failure mechanism and identify the change that would make the assertion obsolete.

| ID | Category | Test file | Test name | Envelope kind | Boundary | Persistence mode | Assertion | Assumptions | Gap removal condition |
|---|---|---|---|---|---|---|---|---|---|
| ordinary-publication-receipt | Supported guarantee | tests/extensions/herdr-worker-mailbox.test.ts | Supported guarantee: ordinary-publication-receipt | ordinary | Temporary write, rename, then RPC receipt | Controlled host, unconsumed mailbox | Hidden 0600 temporary JSON becomes the exact final envelope before the inbox receipt; no receiver consumption is claimed | Private root, live listener PID, valid team target, no filename collision | Not applicable |
| ordinary-queued-retention | Supported guarantee | tests/extensions/herdr-worker-mailbox.test.ts | Supported guarantee: ordinary-queued-retention | ordinary | Pi handoff before a matching custom entry | Controlled host, queued only | Repeated drains and acknowledgement hooks leave the file with one injection and no custom entry in memory or reopened storage | One receiver instance, known peer pane, no matching custom entry | Not applicable |
| ordinary-context-ack | Supported guarantee | tests/extensions/herdr-worker-mailbox.test.ts | Supported guarantee: ordinary-context-ack | ordinary | Matching custom entry then context | Controlled host, explicit JSONL write before hook | One injection, exact custom entry reopened from JSONL, then context deletes the file; a drain alone still skips the in-flight file | One receiver instance, known peer pane, successful explicit test-file write | Not applicable |
| ordinary-agent-settled-ack | Supported guarantee | tests/extensions/herdr-worker-mailbox.test.ts | Supported guarantee: ordinary-agent-settled-ack | ordinary | Matching custom entry then agent_settled | Controlled host, explicit JSONL write before hook | One injection, exact custom entry reopened from JSONL, then agent_settled deletes the file; a drain alone still skips the in-flight file | One receiver instance, known peer pane, successful explicit test-file write | Not applicable |
| ordinary-fresh-receiver-ack | Supported guarantee | tests/extensions/herdr-worker-mailbox.test.ts | Supported guarantee: ordinary-fresh-receiver-ack | ordinary | Fresh receiver with a matching reopened custom entry | Controlled host, file-only reconstruction | The replacement cleans the retained filename without reading its payload or injecting another custom message | Same pane, root and test session file, matching filename, previous receiver stopped | Not applicable |
| ordinary-headless-listener-ownership | Supported guarantee | tests/extensions/herdr-worker-mailbox.test.ts | Supported guarantee: ordinary-headless-listener-ownership | ordinary | Headless shutdown beside an interactive listener | Controlled TUI and headless hosts | Headless shutdown preserves the other listener marker and callbacks; owner teardown removes them, all subscriptions and the private root | Headless host never started listening; interactive owner remains alive until teardown | Not applicable |
