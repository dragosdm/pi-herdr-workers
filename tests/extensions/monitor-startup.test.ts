import assert from "node:assert/strict";
import test, { type TestContext } from "node:test";
import { HerdrMonitorManager, commandKey, type MonitorSnapshot } from "../../loop/runtime/herdr-monitor.js";
import { registerMonitorTools } from "../../loop/tools/monitor-tools.js";
import loopExtension from "../../loop/index.js";

const ready = { shell_pid: 1, foreground_processes: [{ pid: 1, name: "zsh" }] };
const busy = { shell_pid: 1, foreground_processes: [{ pid: 2, name: "helper" }] };
const response = (result: unknown) => ({ stdout: JSON.stringify({ result }), stderr: "", code: 0, killed: false });
function deferred<T = void>() {
  let resolve!: (value: T) => void;
  const promise = new Promise<T>(r => { resolve = r; });
  return { promise, resolve };
}
function fixture(t: TestContext, mode: "split" | "new-root" | "idle-root" = "split") {
  const env = { HERDR_ENV: process.env.HERDR_ENV, HERDR_WORKSPACE_ID: process.env.HERDR_WORKSPACE_ID };
  process.env.HERDR_ENV = "1"; process.env.HERDR_WORKSPACE_ID = "a01-test";
  const calls: { args: string[]; timeout?: number; signal?: AbortSignal }[] = [];
  const order: string[] = [];
  const snapshots: MonitorSnapshot[] = [];
  let now = 0;
  const timers = new Set<{ at: number; expire: () => void }>();
  const f = {
    calls, order, snapshots, timers,
    tab: mode !== "new-root",
    panes: mode === "new-root" ? [] as any[] : [{ pane_id: "root", tab_id: "tab", label: mode === "idle-root" ? "" : "foreign" }],
    observations: [ready] as any[],
    hook: undefined as undefined | ((args: string[], options: any) => any),
    waitHook: undefined as undefined | ((ms: number, signal: AbortSignal) => Promise<void>),
    persistHook: undefined as undefined | ((snapshot: MonitorSnapshot) => void),
    tick(ms: number) { now += ms; for (const timer of [...timers]) if (timer.at <= now) { timers.delete(timer); timer.expire(); } },
    advanceWithoutTimers(ms: number) { now += ms; },
    count(verb: string) { return calls.filter(c => c.args[1] === verb).length; },
    adopt(state?: "pending" | "submitted" | "uncertain") {
      const key = commandKey("printf canary", "/tmp");
      f.panes = [{ pane_id: "owned", tab_id: "tab", label: `mon:${key} canary` }];
      manager.restore({ nextId: 8, monitors: [{ id: "7", key, command: "printf canary", cwd: "/tmp", paneId: "owned", tabId: "tab", status: "running", startedAt: 1, reused: false, ...(state ? { launchState: state } : {}) }] });
    },
  };
  const exec: any = async (_command: string, args: string[], options: any) => {
    calls.push({ args, ...options }); order.push(`${args[0]} ${args[1]}`);
    const override = await f.hook?.(args, options);
    if (override !== undefined) return override;
    let result: any = {};
    if (args[0] === "tab" && args[1] === "list") result = { tabs: f.tab ? [{ label: "Monitor", tab_id: "tab" }] : [] };
    if (args[0] === "tab" && args[1] === "create") {
      f.tab = true; f.panes = [{ pane_id: "root", tab_id: "tab", label: "" }];
      result = { tab: { tab_id: "tab" }, root_pane: { pane_id: "root" } };
    }
    if (args[0] === "pane" && args[1] === "list") result = { panes: structuredClone(f.panes) };
    if (args[1] === "split") {
      const pane = { pane_id: `split-${f.count("split")}`, tab_id: "tab", label: "" };
      f.panes.push(pane); result = { pane };
    }
    if (args[1] === "rename") f.panes.find(p => p.pane_id === args[2]).label = args[3];
    if (args[1] === "get") result = { pane: f.panes.find(p => p.pane_id === args[2]) };
    if (args[1] === "process-info") result = { process_info: f.observations.length > 1 ? f.observations.shift() : f.observations[0] };
    return response(result);
  };
  const manager = new HerdrMonitorManager(exec, {
    now: () => now,
    wait: async (ms, signal) => { if (f.waitHook) await f.waitHook(ms, signal); else f.tick(ms); },
    deadline: (ms, expire) => { const timer = { at: now + ms, expire }; timers.add(timer); return () => { timers.delete(timer); }; },
  });
  manager.onChange = s => { snapshots.push(s); order.push(`save:${s.monitors.at(-1)?.launchState}`); f.persistHook?.(s); };
  const tools = new Map<string, any>(); let widgets = 0;
  registerMonitorTools({ pi: { registerTool: (tool: any) => tools.set(tool.name, tool) } as any, getMonitors: () => manager, updateWidget: () => { widgets++; } });
  t.after(() => {
    manager.dispose(); assert.equal(timers.size, 0, "readiness deadline cleaned up");
    for (const [key, value] of Object.entries(env)) { if (value === undefined) delete process.env[key]; else process.env[key] = value; }
  });
  return Object.assign(f, { manager, exec, tools, widgets: () => widgets,
    create: (signal?: AbortSignal) => manager.create("printf canary", undefined, "/tmp", signal),
    call: (name: string, input = {}) => tools.get(name).execute("a01", input, undefined, undefined, { cwd: "/tmp" }),
  });
}

for (const mode of ["split", "new-root", "idle-root"] as const) {
  test(`A01: ${mode} waits for helper then submits exactly once`, async t => {
    const f = fixture(t, mode);
    f.observations = mode === "idle-root" ? [ready, busy, ready] : [busy, ready];
    const result = await f.create();
    assert.equal(f.count("run"), 1); assert.equal(result.createAction, "submitted");
    assert.equal(result.launchState, "submitted"); assert.equal(result.reused, false);
    assert.equal(f.count("split"), mode === "split" ? 1 : 0);
    assert.equal(f.count("send-keys") + f.count("close"), 0);
    assert.ok(f.order.indexOf("save:pending") < f.order.indexOf("pane rename"));
    assert.ok(f.order.lastIndexOf("pane process-info") < f.order.indexOf("pane get"));
    assert.ok(f.order.indexOf("pane get") < f.order.indexOf("save:uncertain"));
    assert.ok(f.order.indexOf("save:uncertain") < f.order.indexOf("pane run"));
    assert.ok(f.order.indexOf("pane run") < f.order.indexOf("save:submitted"));
    assert.equal("createAction" in f.manager.snapshot().monitors[0], false);
    assert.equal("createAction" in f.manager.get(result.id)!, false);
    const split = f.calls.find(c => c.args[1] === "split");
    if (split) assert.deepEqual(split.args, ["pane", "split", "--pane", "root", "--direction", "down", "--cwd", "/tmp", "--no-focus"]);
  });
}
test("already-ready new pane submits without an initial delay", async t => {
  const f = fixture(t); f.waitHook = async () => assert.fail("forced delay");
  await f.create(); assert.equal(f.count("process-info"), 1);
  assert.equal(f.calls.find(c => c.args[1] === "process-info")?.timeout, 5000);
  assert.equal(f.calls.find(c => c.args[1] === "run")?.timeout, 15000);
});

test("permanent helper retains pending handle, list and stop do not claim a launch", async t => {
  const f = fixture(t); f.observations = [busy];
  await assert.rejects(f.create(), /^Error: Monitor #1 command not submitted: shell readiness timed out after 5000 ms; inspect pane split-1 before retrying\.$/);
  assert.equal(f.count("process-info"), 50); assert.equal(f.count("run"), 0);
  assert.equal(f.manager.get("1")?.launchState, "pending");
  const listed = await f.call("MonitorList");
  assert.match(listed.content[0].text, /unknown · command not submitted/);
  assert.equal(await f.manager.stop("1"), false); assert.equal(f.count("send-keys"), 0);
  f.observations = [ready];
  const retry = await f.create();
  assert.equal(retry.id, "1"); assert.equal(retry.paneId, "split-1"); assert.equal(retry.reused, true);
  assert.equal(retry.createAction, "submitted"); assert.equal(f.count("run"), 1); assert.equal(f.count("split"), 1);
});

const malformed = [
  undefined, {}, { shell_pid: 1 }, { shell_pid: 1, foreground_processes: [] },
  ...[undefined, 0, -1, 1.5, Number.MAX_SAFE_INTEGER + 1, "1"].map(shell_pid => ({ ...ready, shell_pid })),
  ...[undefined, 0, -1, 1.5, Number.MAX_SAFE_INTEGER + 1, "1"].map(pid => ({ shell_pid: 1, foreground_processes: [{ pid, name: "zsh" }] })),
  { shell_pid: 1, foreground_processes: [null] },
  { shell_pid: 1, foreground_processes: [{ pid: 1 }] },
  { shell_pid: 1, foreground_processes: [{ pid: 1, name: "" }] },
  { shell_pid: 1, foreground_processes: [{ pid: 1, name: 2 }] },
  { shell_pid: 1, foreground_processes: [{ pid: 1, name: "zsh" }, { name: "zsh" }] },
];
for (const [index, observation] of malformed.entries()) {
  test(`malformed process observation ${index} cannot authorize input`, async t => {
    const f = fixture(t); f.observations = [observation];
    await assert.rejects(f.create(), /readiness timed out/); assert.equal(f.count("run"), 0);
    f.observations = [observation, ready];
    assert.equal((await f.create()).createAction, "submitted"); assert.equal(f.count("run"), 1);
  });
}
for (const observation of [
  { shell_pid: 1, foreground_processes: [{ pid: 2, name: "zsh" }] },
  { shell_pid: 1, foreground_processes: [{ pid: 1, name: "tcsh" }] },
]) test("both PID equality and supported shell name are required", async t => {
  const f = fixture(t); f.observations = [observation];
  await assert.rejects(f.create(), /readiness timed out/); assert.equal(f.count("run"), 0);
  f.observations = [observation, ready]; await f.create(); assert.equal(f.count("run"), 1);
});

for (const stage of ["process-info", "get"]) {
  test(`ready ${stage} response at the deadline cannot submit`, async t => {
    const f = fixture(t); f.hook = args => { if (args[1] === stage) f.tick(5000); };
    await assert.rejects(f.create(), /readiness timed out/); assert.equal(f.count("run"), 0);
  });
  test(`hung ${stage} is cancelled by the readiness deadline`, async t => {
    const f = fixture(t); const entered = deferred(); const hung = deferred<any>();
    f.hook = args => { if (args[1] === stage) { entered.resolve(); return hung.promise; } };
    const operation = f.create(); await entered.promise;
    const probe = f.calls.at(-1)!; assert.equal(probe.timeout, 5000);
    f.tick(5000); await assert.rejects(operation, /readiness timed out/);
    assert.equal(probe.signal?.aborted, true); assert.equal(f.count("run"), 0);
    hung.resolve(response(stage === "get" ? { pane: f.panes.at(-1) } : { process_info: ready }));
    f.hook = undefined; await f.create(); assert.equal(f.count("run"), 1);
  });
}
for (const failure of [
  { stdout: "", stderr: "probe failed", code: 1, killed: false },
  { stdout: "", stderr: "request killed", code: 0, killed: true },
  { stdout: "not json", stderr: "", code: 0, killed: false },
]) test(`probe failure is immediate, retained and not retried: ${failure.stderr || "JSON"}`, async t => {
  const f = fixture(t); f.hook = args => args[1] === "process-info" ? failure : undefined;
  await assert.rejects(f.create(), /command not submitted: .*inspect pane split-1/);
  assert.equal(f.count("process-info"), 1); assert.equal(f.count("run"), 0);
  assert.equal(f.manager.get("1")?.launchState, "pending");
});

test("pending snapshot restore does not launch and later create waits rather than attaches", async t => {
  const f = fixture(t); f.adopt("pending"); assert.equal(f.calls.length, 0);
  f.observations = [busy]; await assert.rejects(f.create(), /readiness timed out/);
  assert.equal(f.count("run"), 0); assert.equal(f.manager.get("7")?.status, "unknown");
  f.observations = [busy, ready]; assert.equal((await f.create()).createAction, "submitted");
});
for (const state of [undefined, "submitted", "uncertain"] as const) {
  test(`${state ?? "legacy"} busy monitor attaches without invented launch evidence`, async t => {
    const f = fixture(t); f.adopt(state); f.observations = [busy];
    const result = await f.create(); assert.equal(result.createAction, "attached"); assert.equal(result.launchState, state);
    assert.equal(result.id, "7"); assert.equal(f.count("run") + f.count("rename") + f.count("split") + f.count("send-keys"), 0);
    f.observations = [ready]; assert.equal((await f.create()).createAction, "submitted"); assert.equal(f.count("run"), 1);
  });
}
test("untracked labeled busy pane is adopted observationally", async t => {
  const f = fixture(t); f.adopt(); f.manager.restore({ nextId: 1, monitors: [] }); f.observations = [busy];
  const result = await f.create(); assert.equal(result.createAction, "attached"); assert.equal(result.launchState, undefined);
  assert.equal(f.count("run") + f.count("rename"), 0);
});
test("legacy unknown process information fails immediately without readiness waiting", async t => {
  const f = fixture(t); f.adopt(); f.observations = [{}];
  await assert.rejects(f.create(), /Cannot establish shell readiness/); assert.equal(f.count("process-info"), 1);
});

for (const fail of ["timeout", "abort", "acknowledgement append", "failure append"] as const) {
  test(`attempted submission ${fail} stays uncertain and never retries`, async t => {
    const f = fixture(t); const controller = new AbortController();
    f.hook = args => {
      if (args[1] !== "run") return;
      if (fail === "abort") controller.abort(new Error("caller cancelled"));
      if (fail === "timeout" || fail === "failure append") return { stdout: "", stderr: "run timed out", code: 1, killed: true };
    };
    f.persistHook = snapshot => {
      const state = snapshot.monitors[0].launchState;
      if (fail === "acknowledgement append" && state === "submitted") throw new Error("ack append failed");
      if (fail === "failure append" && f.count("run") > 0) throw new Error("failure append failed");
    };
    await assert.rejects(f.create(controller.signal), error => {
      assert.match(String(error), /submission may have happened; inspect pane split-1 before retrying/);
      if (fail === "failure append") assert.match(String(error), /run timed out; saving failure state also failed: failure append failed/);
      return true;
    });
    assert.equal(f.count("run"), 1); assert.equal(f.manager.get("1")?.launchState, "uncertain");
    const snapshot = f.manager.snapshot(); f.manager.restore(snapshot); assert.equal(f.count("run"), 1);
    assert.equal(f.manager.get("1")?.status, "unknown");
  });
}
for (const failState of ["pending", "uncertain"]) test(`persistence failure at ${failState} prevents run`, async t => {
  const f = fixture(t); f.persistHook = s => { if (s.monitors[0].launchState === failState) throw new Error("disk unavailable"); };
  await assert.rejects(f.create(), /command not submitted: disk unavailable/);
  assert.equal(f.count("run"), 0); assert.equal(f.manager.get("1")?.launchState, "pending");
  f.persistHook = undefined; await f.create(); assert.equal(f.count("run"), 1);
});
test("rename failure retains handle before rename; retry does not claim stale foreign label", async t => {
  const f = fixture(t); f.hook = args => args[1] === "rename" ? { stdout: "", stderr: "rename failed", code: 1, killed: false } : undefined;
  await assert.rejects(f.create(), /command not submitted: rename failed; inspect pane split-1/);
  assert.equal(f.snapshots[0].monitors[0].paneId, "split-1"); assert.equal(f.count("run"), 0);
  f.panes.at(-1).label = "someone else"; f.hook = undefined;
  const retry = await f.create(); assert.equal(retry.id, "2"); assert.equal(retry.paneId, "split-2");
});
test("ambiguous acquisition with no pane ID invents no handle", async t => {
  const f = fixture(t); f.hook = args => args[1] === "split" ? response({}) : undefined;
  await assert.rejects(f.create(), /command not submitted: Herdr split returned no pane ID/);
  assert.equal(f.manager.list().length, 0); assert.equal(f.count("run") + f.count("close"), 0);
});

for (const stage of ["before", "wait", "probe", "pre-run"] as const) {
  for (const dispose of [false, true]) test(`${dispose ? "dispose" : "caller cancellation"} at ${stage} prevents late launch`, async t => {
    const f = fixture(t); const controller = new AbortController(); const entered = deferred();
    const never = deferred<any>(); let persisted = 0;
    const cancel = () => { persisted = f.snapshots.length; if (dispose) f.manager.dispose(); else controller.abort(new Error("cancelled")); };
    if (stage === "before") cancel();
    if (stage === "wait") {
      f.observations = [busy]; f.waitHook = async () => { entered.resolve(); await never.promise; };
    }
    if (stage === "probe") f.hook = args => { if (args[1] === "process-info") { entered.resolve(); return never.promise; } };
    if (stage === "pre-run") f.persistHook = s => { if (s.monitors[0].launchState === "uncertain") cancel(); };
    const operation = f.create(controller.signal);
    if (stage === "wait" || stage === "probe") { await entered.promise; cancel(); }
    await assert.rejects(operation, /command not submitted/);
    assert.equal(f.count("run"), 0);
    if (dispose) assert.equal(f.snapshots.length, persisted, "no post-disposal persistence");
    if (stage !== "before") assert.equal(f.manager.get("1")?.launchState, "pending");
    never.resolve(response({ process_info: ready }));
    if (!dispose) { f.hook = undefined; f.waitHook = undefined; f.persistHook = undefined; f.observations = [ready]; await f.create(); assert.equal(f.count("run"), 1); }
  });
}
test("queued cancellation returns promptly, sends no later input and queue remains usable", async t => {
  const f = fixture(t); const entered = deferred(); const release = deferred();
  f.observations = [busy, ready]; f.waitHook = async () => { entered.resolve(); await release.promise; };
  const first = f.create(); await entered.promise;
  const controller = new AbortController(); const second = f.create(controller.signal); controller.abort();
  await assert.rejects(second, /not submitted.*queued/);
  release.resolve(); await first;
  f.waitHook = undefined; await f.create(); assert.equal(f.count("run"), 2); assert.equal(f.count("split"), 1);
});
for (const finishes of [false, true]) test(`concurrent creates ${finishes ? "may rerun finished command" : "attach to busy command"}`, async t => {
  const f = fixture(t); f.hook = args => { if (args[1] === "run" && !finishes) f.observations = [busy]; };
  const [first, second] = await Promise.all([f.create(), f.create()]);
  assert.equal(first.createAction, "submitted"); assert.equal(second.createAction, finishes ? "submitted" : "attached");
  assert.equal(first.paneId, second.paneId); assert.equal(f.count("split"), 1); assert.equal(f.count("run"), finishes ? 2 : 1);
});
test("list and stop during pending startup neither report running nor interrupt helper", async t => {
  const f = fixture(t); const entered = deferred(); const release = deferred();
  f.observations = [busy, ready]; f.waitHook = async () => { entered.resolve(); await release.promise; };
  const operation = f.create(); await entered.promise;
  const listed = await f.call("MonitorList"); assert.match(listed.content[0].text, /command not submitted/);
  assert.equal(f.manager.get("1")?.status, "unknown"); assert.equal(await f.manager.stop("1"), false);
  release.resolve(); await operation; assert.equal(f.count("send-keys"), 0);
});
test("stale concurrent list result cannot overwrite submitted status or launch evidence", async t => {
  const f = fixture(t); const entered = deferred(); const release = deferred(); const listing = deferred(); const releaseList = deferred<any>();
  f.observations = [busy, ready]; f.waitHook = async () => { entered.resolve(); await release.promise; };
  const operation = f.create(); await entered.promise;
  let gets = 0;
  f.hook = args => { if (args[1] === "get" && ++gets === 1) { listing.resolve(); return releaseList.promise; } };
  const refresh = f.manager.refresh(f.manager.get("1")!); await listing.promise;
  release.resolve(); await operation; releaseList.resolve(response({ pane: f.panes.at(-1) })); await refresh;
  assert.equal(f.manager.get("1")?.status, "running"); assert.equal(f.manager.get("1")?.launchState, "submitted");
});
for (const label of ["foreign", undefined]) test(`ownership loss (${label ?? "missing pane"}) prevents submission`, async t => {
  const f = fixture(t); f.hook = args => args[1] === "get" ? response({ pane: label ? { label } : undefined }) : undefined;
  await assert.rejects(f.create(), /ownership could not be verified.*inspect pane split-1/); assert.equal(f.count("run") + f.count("send-keys"), 0);
});
for (const info of [busy, {}]) test("existing foreign busy or unclassified root is split, never typed into", async t => {
  const f = fixture(t, "idle-root"); f.observations = [info, ready];
  const result = await f.create(); assert.equal(result.paneId, "split-1");
  assert.equal(f.calls.find(c => c.args[1] === "run")?.args[2], "split-1");
});
test("failed existing-root probe is an error, not permission to split or claim", async t => {
  const f = fixture(t, "idle-root"); f.hook = args => args[1] === "process-info" ? { stdout: "", stderr: "failed", code: 1, killed: false } : undefined;
  await assert.rejects(f.create(), /command not submitted: failed/); assert.equal(f.count("split") + f.count("run") + f.count("rename"), 0);
});

test("empty command, unavailable environment and 25-handle cap remain enforced", async t => {
  const f = fixture(t);
  await assert.rejects(f.manager.create(" \n "), /cannot be empty/); assert.equal(f.calls.length, 0);
  delete process.env.HERDR_ENV; await assert.rejects(f.create(), /Herdr-managed/); process.env.HERDR_ENV = "1";
  f.adopt(); const original = f.manager.snapshot().monitors[0];
  f.manager.restore({ nextId: 26, monitors: Array.from({ length: 25 }, (_, i) => ({ ...original, id: String(i + 1), key: `other-${i}`, paneId: `other-${i}` })) });
  f.panes = [{ pane_id: "root", tab_id: "tab", label: "foreign" }];
  await assert.rejects(f.create(), /25 monitors tracked/); assert.equal(f.count("run") + f.count("split"), 0);
});
test("quoting, whitespace command key normalization and cwd separation are preserved", async t => {
  const f = fixture(t); const command = "  printf '%s' 'a b'  "; const cwd = "/tmp/a'b";
  const first = await f.manager.create(command, undefined, cwd);
  assert.equal(f.calls.find(c => c.args[1] === "run")?.args[3], `cd -- '/tmp/a'"'"'b' && ${command}`);
  assert.equal(commandKey(command, cwd), commandKey(command.trim(), cwd));
  const second = await f.manager.create(command.trim(), undefined, cwd); assert.equal(first.paneId, second.paneId);
  const third = await f.manager.create(command, undefined, "/tmp/other"); assert.notEqual(first.paneId, third.paneId);
  assert.equal(f.count("run"), 3);
});
test("snapshot round trip preserves launch states, invalid values become uncertain, IDs stay monotonic", async t => {
  const f = fixture(t); f.adopt("pending");
  for (const state of [undefined, "pending", "submitted", "uncertain", "invalid"]) {
    const snapshot = f.manager.snapshot(); (snapshot.monitors[0] as any).launchState = state;
    f.manager.restore(JSON.parse(JSON.stringify(snapshot)));
    assert.equal(f.manager.get("7")?.launchState, state === "invalid" ? "uncertain" : state);
    assert.equal(f.manager.get("7")?.status, "unknown"); assert.equal(f.calls.length, 0);
  }
  assert.equal((await f.manager.create("different", undefined, "/tmp")).id, "8");
});
test("tool text follows submission action, reports uncertainty and updates failure widget", async t => {
  const f = fixture(t);
  const created = await f.call("MonitorCreate", { command: "printf canary" });
  assert.match(created.content[0].text, /command submitted \(new pane\)/); assert.equal(created.details.tone, "success");
  const rerun = await f.call("MonitorCreate", { command: "printf canary" });
  assert.match(rerun.content[0].text, /command submitted \(reused existing pane\)/);
  f.observations = [busy]; const attached = await f.call("MonitorCreate", { command: "printf canary" });
  assert.match(attached.content[0].text, /attached to existing busy pane; no command submitted/);
  assert.match(attached.content[0].text, /Foreground identity is observational/);
  f.observations = [ready]; f.hook = args => args[1] === "run" ? { stdout: "", stderr: "timeout", code: 1, killed: true } : undefined;
  const before = f.widgets(); await assert.rejects(f.call("MonitorCreate", { command: "printf canary" }), /submission may have happened; inspect pane/);
  assert.equal(f.widgets(), before + 1);
  assert.deepEqual(Object.keys(f.tools.get("MonitorCreate").parameters.properties), ["command", "description"]);
  assert.deepEqual(Object.keys(f.tools.get("MonitorList").parameters.properties), []);
  assert.deepEqual(Object.keys(f.tools.get("MonitorStop").parameters.properties), ["monitorId"]);
});
test("pending tool failure has no started/attached success and still updates widget", async t => {
  const f = fixture(t); f.observations = [busy];
  await assert.rejects(f.call("MonitorCreate", { command: "printf canary" }), /command not submitted: shell readiness timed out/);
  assert.equal(f.widgets(), 1); assert.equal(f.manager.get("1")?.launchState, "pending");
});

test("v1 session envelope restores launch evidence without exec and excludes other sessions/scopes", async t => {
  const f = fixture(t); f.adopt("pending");
  const snapshot = f.manager.snapshot();
  const entries = [{ type: "custom", customType: "herdr-monitors.snapshot.v1", data: {
    sessionId: "owner", scope: `${process.env.HERDR_SOCKET_PATH || ""}:a01-test`, snapshot,
  } }];
  for (const [sessionId, wrongScope] of [["owner", false], ["different", false], ["owner", true]] as const) {
    if (wrongScope) entries[0].data.scope = "different-workspace";
    const handlers = new Map<string, any[]>(); const tools = new Map<string, any>(); const appended: any[] = [];
    const pi: any = {
      exec: f.exec, on: (name: string, callback: any) => handlers.set(name, [...(handlers.get(name) ?? []), callback]),
      registerTool: (tool: any) => tools.set(tool.name, tool), registerCommand() {},
      events: { on: () => () => {}, emit() {} }, appendEntry: (type: string, data: any) => appended.push({ type, data }),
    };
    loopExtension(pi);
    const ctx: any = { cwd: "/tmp", hasUI: false, isIdle: () => true, sessionManager: { getEntries: () => entries, getSessionId: () => sessionId, getBranch: () => [] } };
    await handlers.get("session_start")![0]({}, ctx); assert.equal(f.calls.length, 0);
    const listed = await tools.get("MonitorList").execute("a01", {}, undefined, undefined, ctx);
    assert.match(listed.content[0].text, sessionId === "owner" && !wrongScope ? /command not submitted/ : /No monitors/);
    if (sessionId === "owner" && !wrongScope) {
      await tools.get("MonitorCreate").execute("a01", { command: "printf canary" }, undefined, undefined, ctx);
      const saved = appended.filter(e => e.type === "herdr-monitors.snapshot.v1").at(-1);
      assert.equal(saved.data.sessionId, "owner"); assert.equal(saved.data.snapshot.monitors[0].launchState, "submitted");
      assert.equal("createAction" in saved.data.snapshot.monitors[0], false);
    }
    for (const shutdown of handlers.get("session_shutdown") ?? []) await shutdown({}, ctx);
    f.calls.length = 0;
  }
});

for (const stage of ["process-info", "get", "persist"] as const) test(`monotonic deadline rejects late ${stage} even before timer callback`, async t => {
  const f = fixture(t);
  if (stage === "persist") f.persistHook = s => { if (s.monitors[0].launchState === "uncertain") f.advanceWithoutTimers(5000); };
  else f.hook = args => { if (args[1] === stage) f.advanceWithoutTimers(5000); };
  await assert.rejects(f.create(), /readiness timed out/); assert.equal(f.count("run"), 0);
  assert.equal(f.manager.get("1")?.launchState, "pending");
});
test("acquisition time is outside the readiness budget and each probe uses remaining time", async t => {
  const f = fixture(t); f.observations = [busy, ready];
  f.hook = args => { if (args[1] === "rename") f.tick(20000); };
  await f.create();
  assert.deepEqual(f.calls.filter(c => c.args[1] === "process-info").map(c => c.timeout), [5000, 4900]);
  assert.equal(f.calls.find(c => c.args[1] === "get")?.timeout, 4900);
});
for (const shell of ["bash", "zsh", "fish", "sh"]) test(`supported exact shell ${shell} is ready`, async t => {
  const f = fixture(t); f.observations = [{ shell_pid: 1, foreground_processes: [{ pid: 1, name: shell }] }];
  await f.create(); assert.equal(f.count("process-info"), 1); assert.equal(f.count("run"), 1);
});
test("stop retains ownership and foreground checks for submitted monitors", async t => {
  const f = fixture(t); await f.create(); f.observations = [busy];
  assert.equal(await f.manager.stop("1"), true); assert.equal(f.count("send-keys"), 1);
  assert.deepEqual(f.calls.at(-1)?.args, ["pane", "send-keys", "split-1", "ctrl+c"]);
  f.panes.at(-1).label = "foreign";
  assert.equal(await f.manager.stop("1"), false); assert.equal(f.count("send-keys"), 1);
  assert.equal(f.count("close"), 0);
});
test("dispose cancels an entered run as uncertain and suppresses failure appends", async t => {
  const f = fixture(t); const entered = deferred(); const hung = deferred<any>();
  f.hook = args => { if (args[1] === "run") { entered.resolve(); return hung.promise; } };
  const operation = f.create(); await entered.promise; const saved = f.snapshots.length;
  f.manager.dispose(); await assert.rejects(operation, /submission may have happened; inspect pane/);
  assert.equal(f.manager.get("1")?.launchState, "uncertain"); assert.equal(f.snapshots.length, saved);
  hung.resolve(response({})); assert.equal(f.count("run"), 1);
});
test("default abortable wait cancels promptly without launching later", async t => {
  const f = fixture(t); f.observations = [busy]; const controller = new AbortController();
  const manager = new HerdrMonitorManager(f.exec);
  t.after(() => manager.dispose());
  const entered = deferred(); f.hook = args => { if (args[1] === "process-info") entered.resolve(); };
  const operation = manager.create("printf canary", undefined, "/tmp", controller.signal);
  await entered.promise;
  await new Promise<void>(resolve => setImmediate(resolve));
  controller.abort(); await assert.rejects(operation, /command not submitted/);
  assert.equal(f.count("run"), 0); assert.equal(manager.get("1")?.launchState, "pending");
});
