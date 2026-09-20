import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import test, { type TestContext } from "node:test";
import type { ExtensionAPI } from "@earendil-works/pi-coding-agent";
import { HerdrMonitorManager, type MonitorSnapshot } from "../../loop/runtime/herdr-monitor.js";
import { registerMonitorTools } from "../../loop/tools/monitor-tools.js";
import { displayRows } from "../../loop/tools/tool-result.js";

const response = (stdout: string, overrides = {}) => ({ stdout, stderr: "", code: 0, killed: false, ...overrides });
const json = (result: unknown) => response(JSON.stringify({ result }));
type ExecResult = Awaited<ReturnType<ExtensionAPI["exec"]>>;
function deferred<T>() {
  let resolve!: (value: T) => void;
  const promise = new Promise<T>(r => { resolve = r; });
  return { promise, resolve };
}
function fixture(t: TestContext, count = 1) {
  const env = { HERDR_ENV: process.env.HERDR_ENV, HERDR_WORKSPACE_ID: process.env.HERDR_WORKSPACE_ID };
  process.env.HERDR_ENV = "1"; process.env.HERDR_WORKSPACE_ID = "a02-test";
  const calls: { command: string; args: string[]; timeout?: number; signal?: AbortSignal }[] = [];
  const snapshots: MonitorSnapshot[] = [];
  const f = {
    calls, snapshots, output: "AUDIT_SERVER_READY\n", busy: new Set<string>(),
    hook: undefined as undefined | ((args: string[], signal?: AbortSignal) => ExecResult | Promise<ExecResult> | undefined),
    widgets: 0, widgetHook: undefined as undefined | (() => void),
  };
  const manager = new HerdrMonitorManager(async (command, args, options) => {
    calls.push({ command, args, ...options });
    const override = f.hook?.(args, options?.signal);
    if (override !== undefined) return override;
    if (args[1] === "read") return response(f.output);
    if (args[1] === "get") return json({ pane: { label: `mon:key-${args[2]} command` } });
    if (args[1] === "process-info") return json({ process_info: { shell_pid: 1, foreground_processes: [f.busy.has(args[3]) ? { pid: 2, name: "node" } : { pid: 1, name: "zsh" }] } });
    throw new Error(`Unexpected CLI request: ${args.join(" ")}`);
  });
  manager.restore({ nextId: count + 1, monitors: Array.from({ length: count }, (_, i) => ({
    id: String(i + 1), key: `key-pane-${i + 1}`, command: `command-${i + 1}`, cwd: "/tmp/a02",
    paneId: `pane-${i + 1}`, tabId: "tab", status: "running", startedAt: Date.now(), reused: false,
  })) });
  manager.onChange = snapshot => snapshots.push(snapshot);
  const tools = new Map<string, any>();
  registerMonitorTools({ pi: { registerTool: (tool: any) => tools.set(tool.name, tool) } as any, getMonitors: () => manager,
    updateWidget: () => { f.widgets++; f.widgetHook?.(); } });
  t.after(() => {
    manager.dispose();
    for (const [key, value] of Object.entries(env)) { if (value === undefined) delete process.env[key]; else process.env[key] = value; }
  });
  return Object.assign(f, { manager, tools,
    list: (signal?: AbortSignal) => tools.get("MonitorList").execute("a02", {}, signal),
    reads: () => calls.filter(c => c.args[1] === "read"),
  });
}
const tailRows = (result: any): string[] => result.content[0].text.split("\n").filter((line: string) => line.startsWith("  | "));
const capturedMarkers = readFileSync(new URL("./fixtures/a02-herdr-0.8.0-markers.txt", import.meta.url), "utf8");
const markerRows = ["A02_BEGIN_0909", "A02_READY_0909", "A02_END_0909"];

test("A02 live capture: bounded wider window reaches markers above trailing blank screen rows", async t => {
  const f = fixture(t);
  // Only the markers are captured bytes. Padding models terminal rows selected before CLI output cleanup.
  const screen = [...capturedMarkers.trimEnd().split("\n"), ...Array<string>(20).fill("")];
  f.hook = args => args[1] === "read"
    ? response(screen.slice(-Number(args[6])).join("\n").trimEnd()) : undefined;
  assert.deepEqual(await f.manager.readTail("pane-1"), markerRows);
  assert.deepEqual(tailRows(await f.list()), markerRows.map(row => `  | ${row}`));
  assert.equal(f.reads().length, 2, "one capture per read, without retries");
  assert.ok(f.reads().every(call => call.args[6] === "50"));
});

test("wider capture still displays only the last five nonblank rows", async t => {
  const f = fixture(t);
  const screen = [...Array.from({ length: 10 }, (_, i) => `row-${i}`), ...Array<string>(20).fill("")];
  f.hook = args => args[1] === "read" ? response(screen.slice(-Number(args[6])).join("\n")) : undefined;
  const listed = await f.list();
  const expected = ["row-5", "row-6", "row-7", "row-8", "row-9"].map(row => `  | ${row}`);
  assert.deepEqual(tailRows(listed), expected);
  assert.deepEqual(listed.details.expanded.slice(1), expected);
  assert.equal(f.reads().length, 1); assert.equal(f.reads()[0].args[6], "50");
});

test("markers outside the bounded window do not cause retries or scrollback fallback", async t => {
  const f = fixture(t);
  const screen = [...markerRows, ...Array<string>(50).fill("")];
  f.hook = args => args[1] === "read" ? response(screen.slice(-Number(args[6])).join("\n")) : undefined;
  assert.deepEqual(tailRows(await f.list()), ["  | (no output captured)"]);
  assert.equal(f.reads().length, 1); assert.equal(f.reads()[0].args[6], "50");
});

test("explicit internal counts above the capture minimum retain their requested bound", async t => {
  const f = fixture(t); const rows = Array.from({ length: 70 }, (_, i) => `row-${i}`);
  f.output = rows.join("\n");
  assert.deepEqual(await f.manager.readTail("pane-1", 60), rows.slice(-60));
  assert.equal(f.reads().length, 1); assert.equal(f.reads()[0].args[6], "60");
});

test("A02 live capture: exact sanitized CLI marker rows survive manager and list", async t => {
  const f = fixture(t); f.output = capturedMarkers;
  assert.deepEqual(await f.manager.readTail("pane-1"), markerRows);
  const listed = await f.list();
  assert.deepEqual(tailRows(listed), markerRows.map(row => `  | ${row}`));
  assert.deepEqual(listed.details.expanded.slice(1), markerRows.map(row => `  | ${row}`));
});

test("A02: ordinary terminal stdout is returned without JSON parsing", async t => {
  const f = fixture(t); const controller = new AbortController();
  assert.deepEqual(await f.manager.readTail("pane-1", undefined, controller.signal), ["AUDIT_SERVER_READY"]);
  assert.equal(f.calls.length, 1);
  assert.equal(f.calls[0].command, "herdr");
  assert.deepEqual(f.calls[0].args, ["pane", "read", "pane-1", "--source", "recent-unwrapped", "--lines", "50", "--format", "text"]);
  assert.equal(f.calls[0].timeout, 15000);
  assert.ok(f.calls[0].signal instanceof AbortSignal); assert.equal(f.calls[0].signal.aborted, false);
  controller.abort(); assert.equal(f.calls[0].signal.aborted, true);
});

for (const [name, stdout, expected] of [
  ["no final LF", "final line", ["final line"]],
  ["JSON-looking logs", '{}\nnull\n"quoted"\n{"result":{"text":"not an envelope"}}\n', ["{}", "null", '"quoted"', '{"result":{"text":"not an envelope"}}']],
  ["CRLF and CR", "first\r\n\r\n  second  \rthird\r\n", ["first", "  second  ", "third"]],
  ["ANSI, OSC and residual controls", "\x1b[32m緑 🐈\x1b[0m\n\x1b]8;;https://example.test\x07link\x1b]8;;\x1b\\\nA\x00B\x07C\tD\x7fE\x80F\x1b\n", ["緑 🐈", "link", "A B C D E F "]],
] as [string, string, string[]][]) test(`text normalization: ${name}`, async t => {
  const f = fixture(t); f.output = stdout;
  assert.deepEqual(await f.manager.readTail("pane-1"), expected);
  const listed = await f.list(); assert.deepEqual(tailRows(listed), expected.map(row => `  | ${row}`));
  assert.deepEqual(listed.details.expanded.slice(1), expected.map(row => `  | ${row}`));
});

for (const stdout of ["", "\n\r\n", " \t \n  ", "\x1b[32m\x1b[0m", "\x00\x07\x7f\x80\t\x1b"]) test(`empty capture ${JSON.stringify(stdout)} has its own marker`, async t => {
  const f = fixture(t); f.output = stdout;
  assert.deepEqual(await f.manager.readTail("pane-1"), []);
  const listed = await f.list();
  assert.deepEqual(tailRows(listed), ["  | (no output captured)"]);
  assert.deepEqual(listed.details.expanded.slice(1), ["  | (no output captured)"]);
  assert.doesNotMatch(listed.content[0].text, /\{\}|could not read/);
});

for (const [lines, stdout, expected] of [
  [undefined, "1\n2\n\n3\n \n4\n5\n6\n", ["2", "3", "4", "5", "6"]],
  [1, "1\n2\n3\n", ["3"]], [3, "1\n2\n3\n", ["1", "2", "3"]],
  [5, "1\n2", ["1", "2"]], [2, "1\n2\n3\n4", ["3", "4"]],
] as [number | undefined, string, string[]][]) test(`last nonblank rows: count=${lines}, input=${JSON.stringify(stdout)}`, async t => {
  const f = fixture(t); f.output = stdout;
  assert.deepEqual(await f.manager.readTail("pane-1", lines), expected);
  assert.equal(f.calls[0].args[6], String(Math.max(50, lines ?? 5)));
});
for (const lines of [0, -1, 1.5, NaN, Infinity, -Infinity, Number.MAX_SAFE_INTEGER + 1]) test(`invalid line count ${lines} rejects before exec`, async t => {
  const f = fixture(t);
  await assert.rejects(f.manager.readTail("pane-1", lines), { name: "RangeError", message: /positive safe integer/ });
  assert.equal(f.calls.length, 0);
});

test("100+ rows and a 10,000-code-unit row stay bounded in content and details", async t => {
  const f = fixture(t); const long = "🐈".repeat(5000);
  f.output = Array.from({ length: 100 }, (_, i) => `row-${i}`).join("\n") + `\n${long}\n`;
  const expected = ["row-96", "row-97", "row-98", "row-99", long];
  assert.deepEqual(await f.manager.readTail("pane-1"), expected);
  const listed = await f.list(); const displayed = expected.map(row => `  | ${row.slice(0, 100)}`);
  assert.deepEqual(tailRows(listed), displayed);
  assert.deepEqual(listed.details.expanded.slice(1), displayed);
  assert.ok(displayed.every(row => row.length <= 104));
});
test("successful stderr is diagnostic noise, not tail output", async t => {
  const f = fixture(t); f.hook = args => args[1] === "read" ? response("stdout only\n", { stderr: "diagnostic" }) : undefined;
  assert.deepEqual(await f.manager.readTail("pane-1"), ["stdout only"]);
  assert.deepEqual(tailRows(await f.list()), ["  | stdout only"]);
});

for (const kind of ["nonzero", "killed", "rejected"] as const) test(`${kind} read rejects, list continues, next read recovers`, async t => {
  const f = fixture(t, 2); f.busy.add("pane-1");
  f.hook = args => {
    if (args[1] !== "read" || args[2] !== "pane-1") return;
    if (kind === "rejected") return Promise.reject(new Error("pane vanished"));
    return response("PARTIAL_NOT_A_TAIL", { code: kind === "nonzero" ? 1 : 0, killed: kind === "killed", stderr: "pane vanished" });
  };
  await assert.rejects(f.manager.readTail("pane-1"), /pane vanished/);
  const listed = await f.list();
  assert.deepEqual(tailRows(listed), ["  | (could not read pane)", "  | AUDIT_SERVER_READY"]);
  assert.doesNotMatch(listed.content[0].text, /PARTIAL_NOT_A_TAIL/);
  assert.equal(f.manager.get("1")?.status, "running");
  assert.equal(listed.details.summary, "2 monitors · 1 running");
  assert.deepEqual(f.reads().map(c => c.args[2]), ["pane-1", "pane-1", "pane-2"]);
  f.hook = undefined;
  assert.deepEqual(tailRows(await f.list()), ["  | AUDIT_SERVER_READY", "  | AUDIT_SERVER_READY"]);
});
for (const [key, value] of [["HERDR_ENV", undefined], ["HERDR_ENV", "0"], ["HERDR_WORKSPACE_ID", undefined], ["HERDR_WORKSPACE_ID", ""]] as const) test(`environment guard ${key}=${value}`, async t => {
  const f = fixture(t); if (value === undefined) delete process.env[key]; else process.env[key] = value;
  await assert.rejects(f.manager.readTail("pane-1"), /Herdr-managed/); assert.equal(f.calls.length, 0);
});

for (const disposal of [false, true]) {
  test(`${disposal ? "disposal" : "caller abort"} at entry prevents manager and empty/nonempty list publication`, async t => {
    const f = fixture(t); const controller = new AbortController();
    if (disposal) f.manager.dispose(); else controller.abort();
    await assert.rejects(f.manager.readTail("pane-1", 5, controller.signal), { name: "AbortError" });
    await assert.rejects(f.list(controller.signal), { name: "AbortError" });
    f.manager.restore({ nextId: 1, monitors: [] });
    await assert.rejects(f.list(controller.signal), { name: "AbortError" });
    assert.equal(f.calls.length, 0); assert.equal(f.widgets, 0);
  });
  for (const tool of [false, true]) test(`${disposal ? "disposal" : "caller abort"} during pending ${tool ? "list" : "read"} aborts exec and ignores late success`, async t => {
    const f = fixture(t, 2); const entered = deferred<void>(); const release = deferred<ExecResult>(); const controller = new AbortController();
    f.hook = args => { if (args[1] === "read") { entered.resolve(); return release.promise; } };
    const signal = disposal ? undefined : controller.signal;
    const operation = tool ? f.list(signal) : f.manager.readTail("pane-1", 5, signal);
    await entered.promise;
    const rejected = assert.rejects(operation, { name: "AbortError" });
    if (disposal) f.manager.dispose(); else controller.abort();
    assert.equal(f.reads()[0].signal?.aborted, true);
    await rejected;
    release.resolve(response("LATE_SUCCESS\n"));
    await new Promise<void>(resolve => setImmediate(resolve));
    assert.equal(f.widgets, 0); assert.equal(f.reads().length, 1);
    assert.ok(f.calls.every(c => ["get", "process-info", "read"].includes(c.args[1])));
  });
  test(`${disposal ? "disposal" : "caller abort"} synchronous with successful exec resolution cannot publish`, async t => {
    const f = fixture(t, 2); const controller = new AbortController();
    f.hook = args => {
      if (args[1] !== "read") return;
      if (disposal) f.manager.dispose(); else controller.abort();
      return response("LATE_SUCCESS\n");
    };
    await assert.rejects(f.list(disposal ? undefined : controller.signal), { name: "AbortError" });
    assert.equal(f.widgets, 0); assert.equal(f.reads().length, 1);
  });
  test(`${disposal ? "disposal" : "caller abort"} at widget publication prevents successful result`, async t => {
    const f = fixture(t); const controller = new AbortController();
    f.widgetHook = () => { if (disposal) f.manager.dispose(); else controller.abort(); };
    await assert.rejects(f.list(disposal ? undefined : controller.signal), { name: "AbortError" });
    assert.equal(f.widgets, 1);
  });
}

test("overlapping reads resolve in reverse order without shared output or persistence", async t => {
  const f = fixture(t, 2); const first = deferred<ExecResult>(); const second = deferred<ExecResult>();
  f.hook = args => args[2] === "pane-1" ? first.promise : second.promise;
  const one = f.manager.readTail("pane-1"); const two = f.manager.readTail("pane-2");
  assert.equal(f.reads().length, 2);
  second.resolve(response("second\n")); assert.deepEqual(await two, ["second"]);
  first.resolve(response("first\n")); assert.deepEqual(await one, ["first"]);
  assert.equal(f.snapshots.length, 0); assert.equal(f.widgets, 0);
});
test("restore preserves metadata and legacy schema; list refreshes text without relaunch", async t => {
  const f = fixture(t); const before = f.manager.snapshot();
  assert.equal(before.monitors[0].status, "unknown"); assert.equal(f.calls.length, 0);
  assert.deepEqual(tailRows(await f.list()), ["  | AUDIT_SERVER_READY"]);
  const saved = f.manager.snapshot();
  assert.deepEqual(saved, { ...before, monitors: [{ ...before.monitors[0], status: "idle" }] });
  f.manager.restore(saved); assert.equal(f.manager.get("1")?.status, "unknown");
  f.output = "fresh after restore\n";
  assert.deepEqual(tailRows(await f.list()), ["  | fresh after restore"]);
  assert.deepEqual(f.manager.snapshot(), saved);
  assert.ok(f.calls.every(c => ["get", "process-info", "read"].includes(c.args[1])));
  assert.ok(f.snapshots.every(s => !JSON.stringify(s).includes("READY") && !JSON.stringify(s).includes("fresh after")));
});
for (const stage of ["get", "process-info"]) for (const output of ["malformed JSON", ""]) test(`metadata ${stage} ${JSON.stringify(output)} remains JSON interpreted`, async t => {
  const f = fixture(t); f.hook = args => args[1] === stage ? response(output) : undefined;
  assert.equal((await f.manager.refresh(f.manager.get("1")!)).status, "error");
  // Empty metadata is {}, not a parse failure. Both lack verified ownership/process data.
  if (!output) {
    const metadata = await (f.manager as any).herdr(["pane", stage, "pane-1"]);
    assert.deepEqual(metadata, {});
  } else await assert.rejects((f.manager as any).herdr(["pane", stage, "pane-1"]), SyntaxError);
});
test("JSON refresh and successful empty mutation preserve stop flow and pane retention", async t => {
  const f = fixture(t); f.busy.add("pane-1");
  f.hook = args => args[1] === "send-keys" ? response("") : undefined;
  assert.equal((await f.manager.refresh(f.manager.get("1")!)).status, "running");
  assert.equal(await f.manager.stop("1"), true);
  assert.equal(f.manager.get("1")?.status, "stopped");
  assert.deepEqual(f.calls.at(-1)?.args, ["pane", "send-keys", "pane-1", "ctrl+c"]);
  f.busy.clear(); assert.equal((await f.manager.refresh(f.manager.get("1")!)).status, "stopped");
  assert.equal(f.calls.some(c => c.args[1] === "close"), false);
});
test("empty roster and list details, schema, order, status/age headers stay compatible", async t => {
  const f = fixture(t, 5); const roster = f.manager.snapshot();
  f.manager.restore({ nextId: 1, monitors: [] });
  assert.deepEqual(await f.list(), { content: [{ type: "text", text: "No monitors." }], details: {
    kind: "monitor", action: "list", tone: "info", summary: "No monitors", expanded: ["Use MonitorCreate to open a pane in the Monitor tab."], expandable: true,
  } });
  assert.equal(f.calls.length, 0); assert.equal(f.widgets, 0);
  assert.deepEqual(Object.keys(f.tools.get("MonitorList").parameters.properties), []);
  f.manager.restore(roster); f.busy.add("pane-2");
  const listed = await f.list(); const lines = listed.content[0].text.split("\n");
  assert.deepEqual(listed.details, { kind: "monitor", action: "list", tone: "info", summary: "5 monitors · 1 running", expanded: displayRows(lines), expandable: true });
  assert.deepEqual(lines.filter((_: string, i: number) => i % 2 === 0).map((line: string) => line.match(/#(\d+)/)?.[1]), ["1", "2", "3", "4", "5"]);
  assert.match(lines[0], /^ok #1 \[idle\] command-1 · pane pane-1 \(\d+s\)$/);
  assert.match(lines[2], /^> #2 \[running\] command-2 · pane pane-2 \(\d+s\)$/);
  assert.equal(f.widgets, 1);
});
