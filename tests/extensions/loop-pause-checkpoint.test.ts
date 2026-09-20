import assert from "node:assert/strict";
import { mkdirSync, mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test from "node:test";
import { Check } from "typebox/value";
import loops from "../../loop/index.js";
import { LoopStore } from "../../loop/store.js";
import { CronScheduler } from "../../loop/scheduler.js";
import { registerLoopTools } from "../../loop/tools/loop-tools.js";
import type { LoopEntry, LoopStoreData } from "../../loop/types.js";

const CREATED = 1_800_000_000_000;
const PAUSED = CREATED + 10_000;
const checkpoint = { state: "new-state", metrics: "new-metrics", doneCriteria: "new-done", prompt: "new-prompt" };
function seed(store: LoopStore) {
  return store.create({ type: "dynamic" }, "old-prompt", {
    recurring: true, readOnly: true, maxFires: 3,
    dynamic: { goal: "old-goal", state: "old-state", metrics: "old-metrics", doneCriteria: "old-done", iteration: 2,
      awaitingUpdate: true, nextWakeAt: CREATED + 60_000, lastUpdatedAt: CREATED },
  });
}
function expected(entry: LoopEntry) {
  return { status: entry.status, iteration: entry.dynamic!.iteration, updatedAt: entry.updatedAt };
}
function fixture(store = new LoopStore()) {
  const tools = new Map<string, any>();
  const snapshots: LoopStoreData[] = [];
  const audits: unknown[] = [];
  const removed: string[] = [];
  const added: LoopEntry[] = [];
  let failSnapshot = false;
  let failAudit = false;
  store.onChange = snapshot => {
    if (failSnapshot) throw new Error("Loop runtime closed");
    snapshots.push(structuredClone(snapshot));
  };
  registerLoopTools({
    pi: { registerTool: (tool: any) => tools.set(tool.name, tool), appendEntry: (_type: string, data: unknown) => {
      if (failAudit) throw new Error("audit append failed");
      audits.push(structuredClone(data));
    } } as any,
    getStore: () => store,
    getTriggerSystem: () => ({ remove: id => { removed.push(id); }, add: entry => { added.push(entry); } }),
    getScheduler: () => ({ nextFire: () => undefined }), getMonitorManager: () => ({ get: () => undefined }), updateWidget() {},
  });
  return { store, snapshots, audits, removed, added, tools,
    failSnapshot: () => { failSnapshot = true; }, failAudit: () => { failAudit = true; },
    call: (input: object) => tools.get("LoopUpdate").execute("a09", input),
  };
}
function assertCheckpoint(entry: LoopEntry, fields = checkpoint) {
  assert.equal(entry.dynamic?.state, fields.state);
  assert.equal(entry.dynamic?.metrics, fields.metrics);
  assert.equal(entry.dynamic?.doneCriteria, fields.doneCriteria);
  assert.equal(entry.prompt, fields.prompt);
  assert.equal(entry.dynamic?.goal, fields.prompt);
}

test("pause checkpoints save all fields in one final snapshot and preserve scheduling invariants", async t => {
  t.mock.method(Date, "now", () => CREATED);
  const f = fixture(); const entry = seed(f.store);
  f.snapshots.length = 0;
  t.mock.method(Date, "now", () => PAUSED);
  const result = await f.call({ id: entry.id, status: "paused", ...checkpoint });
  const saved = f.store.get(entry.id)!;
  assertCheckpoint(saved);
  assert.deepEqual(saved, { ...entry, prompt: checkpoint.prompt, status: "paused", updatedAt: PAUSED,
    pause: { kind: "administrative", at: PAUSED }, dynamic: { ...entry.dynamic!, state: checkpoint.state, metrics: checkpoint.metrics,
      doneCriteria: checkpoint.doneCriteria, goal: checkpoint.prompt, lastUpdatedAt: PAUSED } });
  assert.equal(f.snapshots.length, 1);
  assert.deepEqual(f.snapshots[0], f.store.snapshot());
  assert.deepEqual(f.removed, [entry.id]);
  assert.equal(f.added.length, 0);
  assert.equal(f.audits.length, 1);
  assert.equal(result.content[0].text, `Dynamic loop #${entry.id} paused`);
  assert.equal(result.details.tone, "warning");
});

for (const field of ["state", "metrics", "doneCriteria", "prompt", "none"] as const) {
  test(`pause merges only supplied ${field}`, async t => {
    t.mock.method(Date, "now", () => CREATED);
    const f = fixture(); const entry = seed(f.store);
    t.mock.method(Date, "now", () => PAUSED);
    await f.call({ id: entry.id, status: "paused", ...(field === "none" ? {} : { [field]: checkpoint[field] }) });
    const saved = f.store.get(entry.id)!;
    const dynamic = { ...entry.dynamic!, lastUpdatedAt: PAUSED };
    if (field === "prompt") dynamic.goal = checkpoint.prompt;
    else if (field !== "none") dynamic[field] = checkpoint[field];
    assert.deepEqual(saved.dynamic, dynamic);
    assert.equal(saved.prompt, field === "prompt" ? checkpoint.prompt : entry.prompt);
  });
}

for (const value of ["", "  Δ checkpoint 🦊\nsecond line\n  "]) {
  test(`pause round-trips ${value === "" ? "empty strings" : "Unicode and multiline whitespace"}`, async t => {
    t.mock.method(Date, "now", () => CREATED);
    const f = fixture(); const entry = seed(f.store);
    const fields = { state: value, metrics: value, doneCriteria: value, prompt: value };
    await f.call({ id: entry.id, status: "paused", ...fields });
    const restored = new LoopStore(); restored.restoreSnapshot(f.snapshots.at(-1)!);
    assertCheckpoint(restored.get(entry.id)!, fields);
  });
}

test("registered LoopUpdate schema rejects invalid field types and statuses", () => {
  const schema = fixture().tools.get("LoopUpdate").parameters;
  for (const field of ["state", "metrics", "doneCriteria", "prompt"]) {
    for (const value of [null, 12, {}]) assert.equal(Check(schema, { id: "1", status: "paused", [field]: value }), false);
    assert.equal(Check(schema, { id: "1", status: "paused", [field]: "" }), true);
  }
  for (const status of ["pause", null, 1, {}]) assert.equal(Check(schema, { id: "1", status }), false);
  assert.equal(Check(schema, { id: "1", status: "paused" }), true);
});

for (const nextInterval of ["1h", "malformed interval"]) {
  test(`pause ignores nextInterval ${nextInterval}`, async t => {
    t.mock.method(Date, "now", () => CREATED);
    const f = fixture(); const entry = seed(f.store);
    await f.call({ id: entry.id, status: "paused", ...checkpoint, nextInterval });
    assert.equal(f.store.get(entry.id)?.dynamic?.nextWakeAt, entry.dynamic?.nextWakeAt);
    assert.deepEqual(f.added, []);
    const scheduler = new CronScheduler(f.store, () => assert.fail("paused wake"));
    scheduler.start(); scheduler.pump(CREATED + 120_000);
    assert.equal(scheduler.nextFire(entry.id), undefined);
    scheduler.stop();
  });
}

test("snapshot rejection rolls back synchronously before trigger removal or audit append", t => {
  t.mock.method(Date, "now", () => CREATED);
  const f = fixture(); const entry = seed(f.store); const before = f.store.snapshot();
  f.failSnapshot();
  assert.throws(() => f.call({ id: entry.id, status: "paused", ...checkpoint }), /Loop runtime closed/);
  assert.deepEqual(f.store.snapshot(), before);
  assert.deepEqual(f.removed, []); assert.deepEqual(f.audits, []);
  // No detached or asynchronous mutation is returned to finish after cancellation.
});

test("supplementary audit rejection leaves the whole authoritative pause committed", t => {
  t.mock.method(Date, "now", () => CREATED);
  const f = fixture(); const entry = seed(f.store); f.snapshots.length = 0;
  f.failAudit();
  assert.throws(() => f.call({ id: entry.id, status: "paused", ...checkpoint }), /audit append failed/);
  assert.equal(f.snapshots.length, 1); assert.deepEqual(f.removed, [entry.id]);
  const restored = new LoopStore(); restored.restoreSnapshot(f.snapshots[0]);
  assert.equal(restored.get(entry.id)?.status, "paused"); assertCheckpoint(restored.get(entry.id)!);
  assert.deepEqual(f.audits, []);
});

test("file reopening preserves the complete pause and shared mirror failure is not rollback", async t => {
  t.mock.method(Date, "now", () => CREATED);
  const dir = mkdtempSync(join(tmpdir(), "a09-file-"));
  try {
    const path = join(dir, "loops.json"); const f = fixture(new LoopStore(path)); const entry = seed(f.store);
    t.mock.method(Date, "now", () => PAUSED);
    await f.call({ id: entry.id, status: "paused", ...checkpoint });
    const reopened = new LoopStore(path);
    assert.deepEqual(reopened.snapshot(), f.store.snapshot()); assertCheckpoint(reopened.get(entry.id)!);
    f.failSnapshot();
    assert.throws(() => f.call({ id: entry.id, status: "paused", state: "fully-committed-after-mirror-error" }), /Loop runtime closed/);
    const afterError = new LoopStore(path).get(entry.id)!;
    assertCheckpoint(afterError, { ...checkpoint, state: "fully-committed-after-mirror-error" });
    assert.equal(afterError.status, "paused");
  } finally { rmSync(dir, { recursive: true, force: true }); }
});

for (const conflict of ["status", "iteration", "updatedAt"] as const) {
  test(`shared writer rejects stale expected ${conflict} without any checkpoint patch`, async t => {
    t.mock.method(Date, "now", () => CREATED);
    const dir = mkdtempSync(join(tmpdir(), "a09-conflict-"));
    try {
      const path = join(dir, "loops.json"); const a = new LoopStore(path); const entry = seed(a);
      const b = new LoopStore(path);
      if (conflict === "updatedAt") t.mock.method(Date, "now", () => PAUSED);
      b.updateDynamic(entry.id, { dynamic: { state: "writer-B-whole-checkpoint",
        ...(conflict === "iteration" ? { iteration: entry.dynamic!.iteration + 1 } : {}) } });
      if (conflict === "status") b.pause(entry.id);
      const latest = new LoopStore(path).snapshot();
      assert.equal(a.stopDynamic(entry.id, "paused", expected(entry), checkpoint), false);
      const f = fixture(a);
      // Model the tool read before B's commit; stopDynamic must still reload under lock.
      t.mock.method(a, "get", () => entry);
      await assert.rejects(async () => f.call({ id: entry.id, status: "paused", ...checkpoint }), /changed while/);
      assert.deepEqual(f.snapshots, []); assert.deepEqual(f.audits, []); assert.deepEqual(f.removed, []);
      assert.deepEqual(new LoopStore(path).snapshot(), latest);
    } finally { rmSync(dir, { recursive: true, force: true }); }
  });
}

test("missing and non-dynamic targets reject without a saved update", async t => {
  t.mock.method(Date, "now", () => CREATED);
  const f = fixture(); const entry = f.store.create({ type: "event", source: "a09" }, "event", { recurring: true });
  const before = f.store.snapshot(); f.snapshots.length = 0;
  for (const id of ["missing", entry.id]) {
    await assert.rejects(async () => f.call({ id, status: "paused", ...checkpoint }), /not found|not a dynamic/);
    assert.equal(f.store.stopDynamic(id, "paused", { status: "active", iteration: 0, updatedAt: CREATED }, checkpoint), false);
  }
  assert.deepEqual(f.store.snapshot(), before); assert.deepEqual(f.snapshots, []);
  assert.deepEqual(f.audits, []); assert.deepEqual(f.removed, []);
});

test("completion deletes, continue preserves omissions and validates intervals, generic pause does not checkpoint", async t => {
  t.mock.method(Date, "now", () => CREATED);
  const f = fixture(); const entry = seed(f.store);
  await assert.rejects(async () => f.call({ id: entry.id, status: "continue", nextInterval: "bad" }), /Invalid nextInterval/);
  await f.call({ id: entry.id, status: "continue", nextInterval: "1h" });
  assert.equal(f.store.get(entry.id)?.dynamic?.state, "old-state");
  assert.equal(f.store.get(entry.id)?.dynamic?.nextWakeAt, CREATED + 3_600_000);
  const dynamic = structuredClone(f.store.get(entry.id)?.dynamic);
  t.mock.method(Date, "now", () => PAUSED);
  f.store.pause(entry.id); assert.deepEqual(f.store.get(entry.id)?.dynamic, dynamic);
  f.snapshots.length = 0;
  await f.call({ id: entry.id, status: "completed", ...checkpoint });
  assert.equal(f.store.get(entry.id), undefined); assert.equal(f.snapshots.length, 1);
  assert.deepEqual(f.snapshots[0].loops, []);
  assert.deepEqual(f.audits.at(-1), { id: entry.id, status: "completed", ...checkpoint, at: PAUSED });
});

test("workflow and orchestration ownership rejection preserves checkpoints", async t => {
  t.mock.method(Date, "now", () => CREATED);
  const f = fixture();
  const workflow = f.store.create({ type: "dynamic" }, "workflow", { recurring: true, workflow: {
    version: 1, initialState: "work", states: { work: { prompt: "work", on: { done: "done" } }, done: { prompt: "done", terminal: "completed" } },
  } });
  const orchestration = f.store.create({ type: "dynamic" }, "orchestration", { recurring: true, orchestration: {
    definition: { goal: "goal", work: [{ prompt: "work" }] }, owner: { sessionId: "a09", runtimeId: "a09-runtime", generation: 0 },
  } });
  const before = f.store.snapshot(); f.snapshots.length = 0;
  for (const entry of [workflow, orchestration]) {
    await assert.rejects(async () => f.call({ id: entry.id, status: "paused", ...checkpoint }), /owned/);
  }
  assert.deepEqual(f.store.snapshot(), before); assert.deepEqual(f.snapshots, []);
  assert.deepEqual(f.removed, []); assert.deepEqual(f.audits, []);
});

for (const limit of ["cap", "deadline"]) {
  test(`pause checkpoint does not renew ${limit}`, async t => {
    t.mock.method(Date, "now", () => CREATED);
    const f = fixture(); const entry = seed(f.store);
    if (limit === "cap") for (let n = 0; n < 3; n++) f.store.fire(entry.id, "dynamic");
    await f.call({ id: entry.id, status: "paused", ...checkpoint });
    if (limit === "deadline") t.mock.method(Date, "now", () => entry.expiresAt);
    assert.equal(f.store.resume(entry.id), undefined);
    assertCheckpoint(f.store.get(entry.id)!);
    assert.equal(f.store.get(entry.id)?.expiresAt, entry.expiresAt);
    assert.equal(f.store.get(entry.id)?.maxFires, 3);
  });
}

// Real extension factory, session hooks, command resume, event dispatch and renderer.
// No pane manager calls are allowed. Every runtime is shut down by its test.
function runtime(entries: any[], cwd: string, sessionId = "a09-session") {
  const tools = new Map<string, any>(); const commands = new Map<string, any>();
  const hooks = new Map<string, Array<(...args: any[]) => any>>();
  const listeners = new Map<string, Array<(data: any) => any>>();
  const messages: any[] = []; const events: any[] = []; const pending: Promise<unknown>[] = [];
  const selections: string[] = [];
  const ui = { setStatus() {}, notify() {}, select: async (_title: string, choices: string[]) => {
    const selection = selections.shift();
    assert.ok(selection && choices.includes(selection), `unexpected selection ${selection}: ${choices}`);
    return selection;
  } };
  const ctx: any = { cwd, mode: "rpc", hasUI: true, ui, isIdle: () => true, hasPendingMessages: () => false,
    sessionManager: { getSessionId: () => sessionId, getEntries: () => entries, getBranch: () => [] },
  };
  loops({
    on: (name: string, callback: any) => hooks.set(name, [...(hooks.get(name) ?? []), callback]),
    registerTool: (tool: any) => tools.set(tool.name, tool),
    registerCommand: (name: string, command: any) => commands.set(name, command),
    appendEntry: (customType: string, data: unknown) => entries.push({ type: "custom", customType, data: structuredClone(data) }),
    sendMessage: (message: any) => { messages.push(structuredClone(message)); },
    exec: async () => assert.fail("A09 must not launch panes or commands"),
    events: {
      on: (name: string, callback: any) => {
        const callbacks = listeners.get(name) ?? []; callbacks.push(callback); listeners.set(name, callbacks);
        return () => { const index = callbacks.indexOf(callback); if (index >= 0) callbacks.splice(index, 1); };
      },
      emit: (name: string, data: any) => {
        events.push({ name, data: structuredClone(data) });
        for (const callback of listeners.get(name) ?? []) pending.push(Promise.resolve(callback(data)));
      },
    },
  } as any);
  const hook = async (name: string) => { for (const callback of hooks.get(name) ?? []) await callback({}, ctx); };
  return { entries, messages, events, hook,
    call: (input: object) => tools.get("LoopUpdate").execute("a09-runtime", input),
    list: () => tools.get("LoopList").execute("a09-list", {}),
    snapshot: () => entries.filter(e => e.customType === "herdr-loops.snapshot.v1").at(-1)?.data.snapshot as LoopStoreData,
    resume: async (entry: LoopEntry) => {
      // Use the command's real menu, rather than LoopUpdate continue's historical implicit resume.
      const label = `- #${entry.id} [paused] ${entry.prompt.slice(0, 50)} (dynamic)`;
      selections.push("View loops", label, "* Resume", "< Back");
      await commands.get("loop").handler("", ctx);
      await Promise.all(pending);
    },
  };
}

function withScope(scope: "session" | "memory", path?: string) {
  const previous = { scope: process.env.PI_LOOP_SCOPE, loop: process.env.PI_LOOP };
  process.env.PI_LOOP_SCOPE = scope;
  if (path) process.env.PI_LOOP = path; else delete process.env.PI_LOOP;
  return () => {
    if (previous.scope === undefined) delete process.env.PI_LOOP_SCOPE; else process.env.PI_LOOP_SCOPE = previous.scope;
    if (previous.loop === undefined) delete process.env.PI_LOOP; else process.env.PI_LOOP = previous.loop;
  };
}
function snapshotRecord(snapshot: LoopStoreData, sessionId = "a09-session") {
  return { type: "custom", customType: "herdr-loops.snapshot.v1", data: { sessionId, snapshot: structuredClone(snapshot) } };
}

for (const audit of ["absent", "conflicting"]) {
  test(`real session restoration and next resumed wake use snapshot with ${audit} update audit`, async t => {
    t.mock.method(Date, "now", () => CREATED);
    const dir = mkdtempSync(join(tmpdir(), "a09-runtime-")); const restoreEnv = withScope("session");
    let first: ReturnType<typeof runtime> | undefined; let restored: ReturnType<typeof runtime> | undefined;
    try {
      const store = new LoopStore(); const entry = seed(store);
      first = runtime([snapshotRecord(store.snapshot())], dir);
      await first.hook("session_start");
      t.mock.method(Date, "now", () => PAUSED);
      const count = first.entries.filter(e => e.customType === "herdr-loops.snapshot.v1").length;
      await first.call({ id: entry.id, status: "paused", ...checkpoint });
      assert.equal(first.entries.filter(e => e.customType === "herdr-loops.snapshot.v1").length, count + 1);
      const saved = first.snapshot(); assertCheckpoint(saved.loops[0]);
      await first.hook("session_shutdown"); first = undefined;
      const history: any[] = [snapshotRecord(store.snapshot()), snapshotRecord(saved)];
      if (audit === "conflicting") history.push({ type: "custom", customType: "herdr-loops.update.v1",
        data: { id: entry.id, status: "continue", state: "bad-audit-state", prompt: "bad-audit-prompt", at: PAUSED + 1 } });
      restored = runtime(history, dir);
      await restored.hook("session_start"); await restored.hook("turn_start"); await restored.hook("agent_settled");
      assert.equal(restored.events.filter(e => e.name === "loop:fire").length, 0);
      assert.equal(restored.messages.length, 0);
      assert.deepEqual(restored.snapshot(), saved);
      await restored.resume(saved.loops[0]);
      const fires = restored.events.filter(e => e.name === "loop:fire"); assert.equal(fires.length, 1);
      assert.equal(restored.messages.length, 1);
      for (const dynamic of [fires[0].data.dynamic, restored.messages[0].details.dynamic]) {
        assert.equal(dynamic.goal, checkpoint.prompt); assert.equal(dynamic.state, checkpoint.state);
        assert.equal(dynamic.metrics, checkpoint.metrics); assert.equal(dynamic.doneCriteria, checkpoint.doneCriteria);
      }
      for (const marker of Object.values(checkpoint)) assert.ok(restored.messages[0].content.includes(marker));
      assert.doesNotMatch(restored.messages[0].content, /old-|bad-audit/);
    } finally {
      await first?.hook("session_shutdown"); await restored?.hook("session_shutdown");
      restoreEnv(); rmSync(dir, { recursive: true, force: true });
    }
  });
}

test("old snapshots and legacy import preserve actual checkpoint without fabricating audit data", async t => {
  t.mock.method(Date, "now", () => CREATED);
  const dir = mkdtempSync(join(tmpdir(), "a09-legacy-")); const restoreEnv = withScope("session");
  let host: ReturnType<typeof runtime> | undefined;
  try {
    const store = new LoopStore(); const entry = seed(store); store.pause(entry.id);
    const legacy = store.snapshot(); delete legacy.loops[0].dynamic!.metrics; delete legacy.loops[0].dynamic!.doneCriteria;
    mkdirSync(join(dir, ".pi", "loops"), { recursive: true });
    writeFileSync(join(dir, ".pi", "loops", "loops-a09-session.json"), JSON.stringify(legacy));
    host = runtime([{ type: "custom", customType: "herdr-loops.update.v1", data: { id: entry.id, ...checkpoint } }], dir);
    await host.hook("session_start");
    assert.deepEqual(host.snapshot(), JSON.parse(JSON.stringify(legacy))); assert.deepEqual(host.messages, []);
    assert.equal(host.entries.filter(e => e.customType === "herdr-loops.snapshot.v1")[0].data.migrated, true);
  } finally { await host?.hook("session_shutdown"); restoreEnv(); rmSync(dir, { recursive: true, force: true }); }
});

for (const scope of ["memory", "other-session", "shared"] as const) {
  test(`${scope} restoration keeps its existing authority boundary`, async t => {
    t.mock.method(Date, "now", () => CREATED);
    const dir = mkdtempSync(join(tmpdir(), "a09-scope-")); const path = join(dir, "shared.json");
    const restoreEnv = withScope(scope === "memory" ? "memory" : "session", scope === "shared" ? path : undefined);
    let host: ReturnType<typeof runtime> | undefined;
    try {
      const store = new LoopStore(scope === "shared" ? path : undefined); const entry = seed(store);
      store.stopDynamic(entry.id, "paused", expected(entry), checkpoint);
      const stale = store.snapshot(); stale.loops[0].dynamic!.state = "stale-session-mirror";
      host = runtime([snapshotRecord(stale)], dir, scope === "other-session" ? "new-session" : "a09-session");
      await host.hook("session_start"); await host.hook("turn_start");
      assert.equal(host.messages.length, 0);
      if (scope === "shared") {
        await host.resume(store.get(entry.id)!);
        assert.equal(host.messages[0].details.dynamic.state, checkpoint.state);
        assertCheckpoint(new LoopStore(path).get(entry.id)!);
      } else {
        assert.match((await host.list()).content[0].text, /No loops/);
        assert.equal(host.entries.length, 1);
      }
    } finally { await host?.hook("session_shutdown"); restoreEnv(); rmSync(dir, { recursive: true, force: true }); }
  });
}
