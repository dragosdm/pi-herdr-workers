import assert from "node:assert/strict";
import { mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test, { beforeEach } from "node:test";
import loopExtension from "../../loop/index.js";
import { LoopStore } from "../../loop/store.js";
import { CronScheduler } from "../../loop/scheduler.js";
import { TriggerSystem } from "../../loop/trigger-system.js";
import { registerLoopTools } from "../../loop/tools/loop-tools.js";
import { registerLoopCommand } from "../../loop/commands/loop-command.js";
import type { LoopEntry, LoopStoreData, Trigger } from "../../loop/types.js";
import { FakeEventBus } from "../support/fake-event-bus.js";

beforeEach((t) => {
  assert.ok("mock" in t);
  t.mock.timers.enable({ apis: ["Date"], now: new Date(2026, 8, 20, 12).getTime() });
});

const impossible = "0 0 31 2 *";
const cron: Trigger = { type: "cron", schedule: "*/5 * * * *" };
const event: Trigger = { type: "event", source: "audit:a03" };
const hybrid: Trigger = { type: "hybrid", cron: "*/5 * * * *", event: { source: "audit:hybrid" }, debounceMs: 0 };
function fixture(store = new LoopStore()) {
  const bus = new FakeEventBus();
  const tools = new Map<string, any>();
  const commands = new Map<string, any>();
  const activations: string[] = [];
  const writes: LoopStoreData[] = [];
  const pi: any = { events: bus, registerTool: (tool: any) => tools.set(tool.name, tool), registerCommand: (name: string, command: any) => commands.set(name, command) };
  store.onChange = (snapshot) => writes.push(snapshot);
  const fire = (entry: LoopEntry) => { store.fire(entry.id, "event"); bus.emit("loop:fire", { loopId: entry.id }); };
  const scheduler = new CronScheduler(store, fire);
  const triggers = new TriggerSystem(pi, scheduler, store, fire);
  const options = { pi, getStore: () => store, getTriggerSystem: () => triggers, updateWidget() {}, onDynamicLoopActivated: (entry: LoopEntry) => { activations.push(entry.id); } };
  registerLoopTools({ ...options, getScheduler: () => scheduler, getMonitorManager: () => ({ get: () => undefined }) });
  registerLoopCommand(options);
  return { store, scheduler, triggers, bus, writes, activations, options, tools, commands,
    call: (name: string, input: any = {}, signal?: AbortSignal) => tools.get(name).execute("a03", input, signal),
  };
}
function legacy(triggers: Trigger[]): LoopStoreData {
  const store = new LoopStore();
  for (const trigger of triggers) {
    const entry = store.create(event, "retained prompt", { recurring: true, maxFires: 5 });
    store.updateMetadata(entry.id, { trigger });
  }
  return store.snapshot();
}
const bad: Trigger = { type: "cron", schedule: impossible };
const badHybrid: Trigger = { ...hybrid, cron: impossible };

test("tool rejects unavailable cron without allocating, persisting, or registering", async () => {
  const f = fixture();
  const before = f.store.snapshot();
  await assert.rejects(f.call("LoopCreate", { trigger: impossible, triggerType: "cron", prompt: "invalid" }), /No matching time.*0 0 31 2/);
  assert.deepEqual(f.store.snapshot(), before);
  assert.equal(f.writes.length, 0);
  assert.equal(f.bus.listenerCount(), 0);
  assert.equal(f.scheduler.nextFire("1"), undefined);
  assert.deepEqual(f.activations, []);
});

test("typed hybrid preflight rejects before mutation", () => {
  const f = fixture(); const before = f.store.snapshot();
  assert.throws(() => f.store.create(badHybrid, "invalid", { recurring: true }), /No matching time/);
  assert.deepEqual(f.store.snapshot(), before);
  assert.equal(f.writes.length, 0);
});

for (const position of [0, 1, 3]) test(`legacy poison in position ${position} is isolated and healthy events actually fire`, () => {
  const f = fixture(); const inputs = [cron, event, hybrid]; inputs.splice(position, 0, bad);
  const snapshot = legacy(inputs); f.store.restoreSnapshot(snapshot);
  try {
    f.triggers.start();
    const poison = f.store.get(String(position + 1))!;
    assert.equal(poison.status, "paused");
    assert.match(poison.pause!.reason!, /^Schedule unavailable: No matching time/);
    assert.ok(poison.pause!.reason!.length <= 512);
    assert.deepEqual({ ...poison, status: "active", pause: undefined, updatedAt: snapshot.loops[position].updatedAt }, { ...snapshot.loops[position], pause: undefined });
    for (const entry of f.store.list()) {
      if (entry.status === "active" && entry.trigger.type !== "event") assert.ok(f.scheduler.nextFire(entry.id));
    }
    f.bus.emit("audit:a03", {}); f.bus.emit("audit:hybrid", {});
    assert.equal(f.bus.emissions.filter(e => e.channel === "loop:fire").length, 2);
    for (const entry of f.store.list().filter(e => e.trigger.type === "event" || e.trigger.type === "hybrid")) assert.equal(entry.fireCount, 1);
    const writes = f.writes.length;
    f.triggers.start(); f.triggers.stop(); f.triggers.start();
    assert.equal(f.writes.length, writes);
    assert.equal(f.bus.listenerCount(), 2);
    f.bus.emit("audit:a03", {});
    assert.equal(f.bus.emissions.filter(e => e.channel === "loop:fire").length, 3);
    const restored = fixture(); restored.store.restoreSnapshot(f.store.snapshot());
    try { restored.triggers.start(); assert.equal(restored.writes.length, 0); assert.deepEqual(restored.store.get(poison.id)?.pause, poison.pause); }
    finally { restored.triggers.stop(); }
  } finally { f.triggers.stop(); }
  assert.equal(f.bus.listenerCount(), 0);
});

test("multiple unavailable schedules including hybrid do not subscribe their event half", () => {
  const f = fixture(); f.store.restoreSnapshot(legacy([bad, { ...badHybrid, event: { source: "audit:a03" } }, event]));
  try {
    f.triggers.start();
    assert.equal(f.bus.listenerCount("audit:a03"), 1);
    assert.deepEqual(f.store.list().map(e => e.status), ["paused", "paused", "active"]);
    assert.equal(f.scheduler.nextFire("2"), undefined);
    f.bus.emit("audit:a03", {});
    assert.equal(f.store.get("3")?.fireCount, 1);
    assert.equal(f.store.get("2")?.fireCount, 0);
  } finally { f.triggers.stop(); }
});

test("quarantine persists through file round trip and rejects resume unchanged", () => {
  const dir = mkdtempSync(join(tmpdir(), "a03-")); const path = join(dir, "loops.json");
  try {
    writeFileSync(path, JSON.stringify(legacy([bad, event])));
    const f = fixture(new LoopStore(path));
    try {
      f.triggers.start(); f.bus.emit("audit:a03", {});
      const restored = new LoopStore(path); const before = restored.snapshot();
      assert.equal(before.loops[0].status, "paused");
      assert.match(before.loops[0].pause!.reason!, /^Schedule unavailable:/);
      assert.equal(before.loops[1].fireCount, 1);
      const raw = readFileSync(path, "utf8");
      assert.throws(() => restored.resume("1"), /No matching time/);
      assert.deepEqual(restored.snapshot(), before);
      assert.equal(readFileSync(path, "utf8"), raw);
    } finally { f.triggers.stop(); }
  } finally { rmSync(dir, { recursive: true, force: true }); }
});

for (const schedule of ["* *", "60 * * * *", "*/0 * * * *"]) test(`malformed cron ${schedule} is rejected without mutation`, async () => {
  const f = fixture(); const before = f.store.snapshot();
  assert.throws(() => f.store.create({ type: "cron", schedule }, "invalid", { recurring: true }), /Invalid cron/);
  await assert.rejects(f.call("LoopCreate", { trigger: schedule, triggerType: "cron", prompt: "invalid" }), /Invalid cron|Cannot parse/);
  assert.deepEqual(f.store.snapshot(), before);
  assert.equal(f.writes.length, 0);
});

test("valid shorthand, full cron, event and idle retain defaults and activation", async () => {
  const f = fixture();
  try {
    for (const trigger of ["5m", "0 9 * * 1-5"]) {
      const result = await f.call("LoopCreate", { trigger, prompt: "healthy" });
      assert.match(result.content[0].text, /created:/);
      const entry = f.store.list().at(-1)!;
      assert.equal(entry.status, "active"); assert.equal(entry.recurring, true); assert.equal(entry.maxFires, 25);
      assert.equal(entry.expiresAt - entry.createdAt, 7 * 86400000);
      assert.ok(f.scheduler.nextFire(entry.id));
    }
    await f.call("LoopCreate", { trigger: "audit:a03", prompt: "event", maxFires: 2, readOnly: true });
    const entry = f.store.list().at(-1)!;
    assert.equal(entry.readOnly, true);
    f.bus.emit("audit:a03", {}); assert.equal(f.store.get(entry.id)?.fireCount, 1);
    f.bus.emit("audit:a03", {}); assert.equal(f.store.get(entry.id), undefined);
    await f.call("LoopCreate", { trigger: "idle", triggerType: "idle", prompt: "dynamic" });
    assert.deepEqual(f.activations, [f.store.list().at(-1)!.id]);
    assert.equal(f.store.list().at(-1)!.dynamic?.iteration, 0);
  } finally { f.triggers.stop(); }
});

test("command invalid creation is zero-mutation and successful defaults remain 25/20", async () => {
  const f = fixture(); const notices: Array<[string, string]> = [];
  const ctx = { hasUI: true, ui: { notify: (message: string, tone: string) => notices.push([message, tone]) } };
  const before = f.store.snapshot();
  await f.commands.get("loop").handler(`${impossible} bad`, ctx);
  await f.commands.get("loop").handler("*/0 * * * * bad", ctx);
  assert.deepEqual(f.store.snapshot(), before); assert.equal(f.writes.length, 0);
  assert.ok(notices.every(n => n[1] === "error"));
  try {
    await f.commands.get("loop").handler("5m healthy", ctx);
    await f.commands.get("loop").handler("event audit:a03 healthy", ctx);
    await f.commands.get("loop").handler("healthy goal", ctx);
    assert.deepEqual(f.store.list().map(e => e.maxFires), [25, 25, 20]);
    assert.deepEqual(f.activations, ["3"]);
  } finally { f.triggers.stop(); }
});

test("event subscription failure after hybrid timer registration rolls back by ID", async () => {
  const f = fixture(); const healthy = f.store.create(event, "healthy", { recurring: true });
  f.triggers.add(healthy); const saved = structuredClone(healthy);
  const primary = new Error("provider subscription failed"); const on = f.bus.on.bind(f.bus);
  f.bus.on = (source, callback) => { if (source === "broken") throw primary; return on(source, callback); };
  const beforeId = f.store.snapshot().nextId;
  try {
    await assert.rejects(f.call("LoopCreate", { trigger: "cron:5m event:broken", triggerType: "hybrid", prompt: "bad" }), error => error === primary);
    assert.equal(f.store.get(String(beforeId)), undefined);
    assert.equal(f.scheduler.nextFire(String(beforeId)), undefined);
    assert.equal(f.bus.listenerCount("broken"), 0);
    assert.deepEqual(f.store.get(healthy.id), saved);
    assert.equal(f.store.snapshot().nextId, beforeId + 1);
    assert.equal(f.writes.at(-2)!.loops.length, 2); assert.equal(f.writes.at(-1)!.loops.length, 1);
    f.bus.emit("audit:a03", {}); assert.equal(f.store.get(healthy.id)?.fireCount, 1);
  } finally { f.triggers.stop(); }
});

test("TriggerSystem.add alone cleans hybrid timer when its event subscription throws", () => {
  const f = fixture(); const entry = f.store.create(hybrid, "hybrid", { recurring: true });
  f.bus.on = () => { throw new Error("subscribe failed"); };
  assert.throws(() => f.triggers.add(entry), /subscribe failed/);
  assert.equal(f.scheduler.nextFire(entry.id), undefined);
  assert.equal(f.bus.listenerCount(), 0);
});

test("rollback attempts deletion after remove failure and reports both errors and ID", async () => {
  const f = fixture(); const primary = new Error("register failed");
  f.triggers.add = () => { throw primary; };
  f.triggers.remove = () => { throw new Error("remove failed"); };
  await assert.rejects(f.call("LoopCreate", { trigger: "audit:a03", prompt: "bad" }), /Loop #1.*register failed.*rollback failed.*remove failed/);
  assert.equal(f.store.get("1"), undefined);
});

test("rollback reports persistence deletion failure and does not claim success", async () => {
  const f = fixture();
  f.triggers.add = () => { throw new Error("register failed"); };
  f.store.delete = () => { throw new Error("delete failed"); };
  await assert.rejects(f.call("LoopCreate", { trigger: "audit:a03", prompt: "bad" }), /Loop #1.*register failed.*rollback failed.*delete failed/);
  assert.ok(f.store.get("1"), "residual entry must not be described as rolled back");
});

test("widget cleanup error cannot replace registration failure", async () => {
  const f = fixture(); const primary = new Error("register failed");
  f.triggers.add = () => { throw primary; };
  registerLoopTools({ ...f.options, updateWidget: () => { throw new Error("widget failed"); }, getScheduler: () => f.scheduler, getMonitorManager: () => ({ get: () => undefined }) });
  await assert.rejects(f.call("LoopCreate", { trigger: "audit:a03", prompt: "bad" }), error => error === primary);
  assert.equal(f.store.list().length, 0);
});

test("abort before creation is mutation-free; abort after synchronous registration does not undo commit", async () => {
  const f = fixture(); const aborted = new AbortController(); aborted.abort();
  const before = f.store.snapshot();
  await assert.rejects(f.call("LoopCreate", { trigger: "idle", triggerType: "idle", prompt: "bad" }, aborted.signal), /abort/i);
  assert.deepEqual(f.store.snapshot(), before); assert.equal(f.writes.length, 0); assert.deepEqual(f.activations, []);
  const later = new AbortController(); const add = f.triggers.add.bind(f.triggers);
  f.triggers.add = (entry) => { add(entry); later.abort(); };
  try {
    await f.call("LoopCreate", { trigger: "idle", triggerType: "idle", prompt: "healthy" }, later.signal);
    assert.equal(f.store.list().length, 1); assert.deepEqual(f.activations, ["1"]);
  } finally { f.triggers.stop(); }
});

test("post-registration dynamic activation failure never deletes dispatched controller", async () => {
  const f = fixture();
  registerLoopTools({ ...f.options, onDynamicLoopActivated: () => { throw new Error("activation failed"); }, getScheduler: () => f.scheduler, getMonitorManager: () => ({ get: () => undefined }) });
  try {
    await assert.rejects(f.call("LoopCreate", { trigger: "idle", triggerType: "idle", prompt: "healthy" }), /activation failed/);
    assert.equal(f.store.get("1")?.status, "active"); assert.ok(f.scheduler.nextFire("1"));
  } finally { f.triggers.stop(); }
});

async function inspectAndResume(f: ReturnType<typeof fixture>) {
  const notices: Array<[string, string]> = []; const titles: string[] = [];
  const selections = ["View loops", "- #1 paused", "* Resume", "< Back"];
  await f.commands.get("loop").handler("", { hasUI: true, ui: {
    select: async (title: string) => { titles.push(title); return selections.shift(); },
    notify: (message: string, tone: string) => notices.push([message, tone]),
  } });
  return { notices, titles };
}

test("inspection exposes reason; manual resume rejects unchanged, corrected explicit resume succeeds", async () => {
  const f = fixture(); f.store.restoreSnapshot(legacy([bad])); f.triggers.start();
  const before = f.store.snapshot(); const writes = f.writes.length;
  const result = await inspectAndResume(f);
  assert.ok(result.titles.some(t => /Pause: administrative.*\nReason: Schedule unavailable:/s.test(t)));
  assert.match(result.notices[0][0], /No matching time/); assert.ok(result.notices.every(n => n[1] === "error"));
  assert.deepEqual(f.store.snapshot(), before); assert.equal(f.writes.length, writes);
  const listing = await f.call("LoopList"); assert.match(listing.content[0].text, /\[pause:administrative\].*Schedule unavailable:/);
  f.store.updateMetadata("1", { trigger: cron });
  try {
    f.triggers.start(); assert.equal(f.store.get("1")?.status, "paused", "no automatic resume after correction");
    const resumed = await inspectAndResume(f);
    assert.match(resumed.notices[0][0], /resumed/); assert.equal(f.store.get("1")?.pause, undefined); assert.ok(f.scheduler.nextFire("1"));
  } finally { f.triggers.stop(); }
});

test("resume registration failure removes timers and administratively pauses existing controller", async () => {
  const f = fixture(); const entry = f.store.create(cron, "healthy", { recurring: true }); f.store.pause(entry.id);
  const add = f.triggers.add.bind(f.triggers); f.triggers.add = (loop) => { add(loop); throw new Error("resume registration failed"); };
  const result = await inspectAndResume(f);
  assert.ok(result.notices.every(n => n[1] === "error"));
  assert.equal(f.store.get(entry.id)?.status, "paused"); assert.match(f.store.get(entry.id)?.pause?.reason ?? "", /^Registration failed:/);
  assert.equal(f.scheduler.nextFire(entry.id), undefined); assert.equal(f.store.snapshot().nextId, 2);
});

test("future occurrence beyond seven days arms expiry, not quarantine", () => {
  const f = fixture(); const entry = f.store.create({ type: "cron", schedule: "0 0 1 12 *" }, "December", { recurring: true });
  f.scheduler.start(); assert.equal(f.store.get(entry.id)?.status, "active"); assert.equal(f.scheduler.nextFire(entry.id), undefined);
  f.scheduler.pump(entry.expiresAt); assert.equal(f.store.get(entry.id), undefined);
  assert.equal(f.bus.emissions.length, 0);
});

test("quarantine persistence errors propagate with bad ID unarmed", () => {
  const f = fixture(); f.store.restoreSnapshot(legacy([bad, event]));
  const before = f.store.snapshot(); f.store.onChange = () => { throw new Error("journal unavailable"); };
  assert.throws(() => f.triggers.start(), /Failed to persist schedule quarantine for loop #1: journal unavailable/);
  assert.equal(f.scheduler.nextFire("1"), undefined); assert.equal(f.bus.listenerCount(), 0);
  assert.deepEqual(f.store.snapshot(), before);
});

test("store listing and arbitrary scheduler errors are not misclassified as unavailable schedules", () => {
  const f = fixture(); f.store.list = () => { throw new Error("storage unavailable"); };
  assert.throws(() => f.scheduler.start(), /storage unavailable/);
  const g = fixture(); g.store.create(event, "healthy", { recurring: true });
  (g.scheduler as any).expiryTimes.set = () => { throw new Error("programming failure"); };
  assert.throws(() => g.scheduler.start(), /programming failure/);
  assert.equal(g.store.get("1")?.status, "active");
});

test("preflight failures consume no slots; quarantine still counts toward the 25-controller cap", () => {
  const f = fixture(); const before = f.store.snapshot();
  for (let i = 0; i < 2; i++) assert.throws(() => f.store.create(bad, "invalid", { recurring: true }), /No matching time/);
  assert.deepEqual(f.store.snapshot(), before);
  f.store.restoreSnapshot(legacy([bad])); f.scheduler.start();
  for (let i = 0; i < 24; i++) f.store.create(event, "healthy", { recurring: true });
  assert.equal(f.store.snapshot().nextId, 26); assert.throws(() => f.store.create(event, "overflow", { recurring: true }), /Maximum of 25/);
});

test("wired session recovery persists quarantine, expires old poison, and emits one healthy loop:fire", async () => {
  const oldEnv = { scope: process.env.PI_LOOP_SCOPE, loop: process.env.PI_LOOP };
  process.env.PI_LOOP_SCOPE = "session"; delete process.env.PI_LOOP;
  const snapshot = legacy([bad, event, bad]); snapshot.loops[2].expiresAt = Date.now() - 1;
  const sessionId = "a03-fixture-session";
  const entries: any[] = [{ type: "custom", customType: "herdr-loops.snapshot.v1", data: { sessionId, snapshot } }];
  const ctx: any = { cwd: tmpdir(), hasUI: false, mode: "print", isIdle: () => false, hasPendingMessages: () => false,
    ui: { setStatus() {} }, sessionManager: { getEntries: () => entries, getBranch: () => [], getSessionId: () => sessionId } };
  function host() {
    const bus = new FakeEventBus(); const hooks = new Map<string, Array<(...args: any[]) => any>>();
    const tools = new Map<string, any>();
    const pi: any = { events: bus, exec: async () => { throw new Error("unexpected exec"); },
      appendEntry: (customType: string, data: unknown) => entries.push({ type: "custom", customType, data }),
      on: (name: string, callback: (...args: any[]) => any) => { const handlers = hooks.get(name) ?? []; handlers.push(callback); hooks.set(name, handlers); },
      registerTool: (tool: any) => tools.set(tool.name, tool), registerCommand() {}, sendMessage() {},
    };
    loopExtension(pi);
    return { bus, tools, run: async (name: string) => { for (const hook of hooks.get(name) ?? []) await hook({}, ctx); } };
  }
  const latest = () => entries.filter(e => e.customType === "herdr-loops.snapshot.v1").at(-1).data.snapshot as LoopStoreData;
  let current: ReturnType<typeof host> | undefined;
  try {
    current = host(); await current.run("session_start");
    assert.equal(latest().loops.find(e => e.id === "1")?.status, "paused");
    assert.match(latest().loops.find(e => e.id === "1")?.pause?.reason ?? "", /^Schedule unavailable:/);
    assert.equal(latest().loops.find(e => e.id === "3"), undefined, "ordinary expired poison is removed before quarantine");
    current.bus.emit("audit:a03", {});
    await Promise.resolve();
    const fires = current.bus.emissions.filter(e => e.channel === "loop:fire");
    assert.equal(fires.length, 1); assert.equal((fires[0].payload as any).loopId, "2");
    assert.equal(latest().loops.find(e => e.id === "2")?.fireCount, 1);
    const paused = structuredClone(latest().loops[0]);
    await current.run("session_shutdown"); assert.equal(current.bus.listenerCount("audit:a03"), 0);
    const count = entries.filter(e => e.customType === "herdr-loops.snapshot.v1").length;
    current = host(); await current.run("session_start");
    assert.deepEqual(latest().loops[0], paused);
    assert.equal(entries.filter(e => e.customType === "herdr-loops.snapshot.v1").length, count);
    assert.equal(current.bus.listenerCount("audit:a03"), 1);
    assert.equal(current.bus.emissions.filter(e => e.channel === "loop:fire").length, 0, "reload does not replay a delivered event");
  } finally {
    await current?.run("session_shutdown");
    if (oldEnv.scope === undefined) delete process.env.PI_LOOP_SCOPE; else process.env.PI_LOOP_SCOPE = oldEnv.scope;
    if (oldEnv.loop === undefined) delete process.env.PI_LOOP; else process.env.PI_LOOP = oldEnv.loop;
  }
});

test("startup retains inactive, workflow, orchestration and awaiting-update skip behavior", () => {
  const f = fixture(); f.store.restoreSnapshot(legacy([bad, bad])); f.store.pause("2", "administrative", "already paused");
  const awaiting = f.store.create({ type: "dynamic" }, "waiting", { recurring: true, dynamic: { goal: "waiting", iteration: 1, awaitingUpdate: true } });
  const workflow = () => f.store.create({ type: "dynamic" }, "workflow", { recurring: true, workflow: {
    version: 1, initialState: "working", states: { working: { prompt: "work", loop: { schedule: "*/5 * * * *" }, on: { done: "done" } }, done: { prompt: "done", terminal: "completed" } },
  } });
  const waiting = workflow(); waiting.workflow!.waitingMonitor = { monitorId: "fixture", stateId: "working", transitionSeq: 0, attachedAt: Date.now() };
  const terminal = workflow(); terminal.workflow!.currentState = "done";
  const unavailable = workflow(); unavailable.workflow!.definition.states.working.loop!.schedule = impossible;
  const owned = f.store.create({ type: "dynamic" }, "orchestration", { recurring: true, orchestration: {
    definition: { goal: "fixture", work: [{ prompt: "work" }] }, owner: { sessionId: "session", runtimeId: "runtime", generation: 0 },
  } });
  const beforePaused = structuredClone(f.store.get("2"));
  try {
    f.triggers.start(); f.scheduler.pump(Date.now() + 1000);
    assert.deepEqual(f.store.get("2"), beforePaused);
    assert.equal(f.store.get(unavailable.id)?.status, "paused", "workflow timer failure is isolated too");
    for (const entry of [waiting, terminal, owned]) assert.equal(f.scheduler.nextFire(entry.id), undefined);
    assert.equal(f.store.get(awaiting.id)?.fireCount, 0); assert.equal(f.store.get(awaiting.id)?.dynamic?.awaitingUpdate, true);
    assert.equal(f.bus.emissions.length, 0);
  } finally { f.triggers.stop(); }
});

test("quarantine reason is bounded and resume retains lifetime and fire-cap guards", () => {
  const f = fixture(); f.store.restoreSnapshot(legacy([{ type: "cron", schedule: "invalid".repeat(100) }]));
  f.scheduler.start(); assert.equal(f.store.get("1")?.pause?.reason?.length, 512);
  const expired = f.store.create(cron, "expired", { recurring: true }); f.store.pause(expired.id); f.store.get(expired.id)!.expiresAt = Date.now() - 1;
  const capped = f.store.create(cron, "capped", { recurring: true, maxFires: 1 }); f.store.fire(capped.id); f.store.pause(capped.id);
  const before = f.store.snapshot(); assert.equal(f.store.resume(expired.id), undefined); assert.equal(f.store.resume(capped.id), undefined);
  assert.deepEqual(f.store.snapshot(), before);
});

test("repeated start preserves hybrid debounce history and event filters", () => {
  const f = fixture(); const entry = f.store.create({ ...hybrid, debounceMs: 30000, event: { source: "audit:hybrid", filter: '{"ok":true}' } }, "hybrid", { recurring: true });
  try {
    f.triggers.start(); f.bus.emit("audit:hybrid", { ok: false }); assert.equal(f.store.get(entry.id)?.fireCount, 0);
    f.bus.emit("audit:hybrid", { ok: true }); assert.equal(f.store.get(entry.id)?.fireCount, 1);
    f.triggers.start(); f.bus.emit("audit:hybrid", { ok: true }); assert.equal(f.store.get(entry.id)?.fireCount, 1);
    assert.equal(f.bus.listenerCount("audit:hybrid"), 1);
  } finally { f.triggers.stop(); }
  assert.equal((f.triggers as any).hybridTimers.size, 0);
});

test("slash cron rollback attempts both cleanups and reports the primary error", async () => {
  const f = fixture(); f.triggers.add = () => { throw new Error("register failed"); }; f.triggers.remove = () => { throw new Error("remove failed"); };
  const notices: string[] = [];
  await f.commands.get("loop").handler("5m healthy", { hasUI: true, ui: { notify: (text: string) => notices.push(text) } });
  assert.equal(f.store.list().length, 0); assert.match(notices[0], /register failed.*rollback failed for loop #1.*remove failed/);
});

test("ID-local rollback preserves a healthy second file-store writer and subsequent tool creation", async () => {
  const dir = mkdtempSync(join(tmpdir(), "a03-writers-")); const path = join(dir, "loops.json");
  try {
    const f = fixture(new LoopStore(path)); const other = new LoopStore(path); const add = f.triggers.add.bind(f.triggers);
    f.triggers.add = () => { other.create(event, "other writer", { recurring: true }); throw new Error("register failed"); };
    await assert.rejects(f.call("LoopCreate", { trigger: "5m", prompt: "failed" }), /register failed/);
    assert.deepEqual(new LoopStore(path).list().map(e => [e.id, e.prompt]), [["2", "other writer"]]);
    f.triggers.add = add;
    await assert.rejects(f.call("LoopCreate", { trigger: impossible, prompt: "invalid" }), /No matching time/);
    try {
      await f.call("LoopCreate", { trigger: "audit:a03", prompt: "valid" });
      assert.deepEqual(f.store.list().map(e => e.id), ["2", "3"]);
    } finally { f.triggers.stop(); }
  } finally { rmSync(dir, { recursive: true, force: true }); }
});
