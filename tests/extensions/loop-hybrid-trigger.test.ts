import assert from "node:assert/strict";
import test, { beforeEach, type TestContext } from "node:test";
import { Check } from "typebox/value";
import { parseHybridTriggerInput } from "../../loop/hybrid-parse.js";
import { LoopStore } from "../../loop/store.js";
import { CronScheduler } from "../../loop/scheduler.js";
import { TriggerSystem } from "../../loop/trigger-system.js";
import { registerLoopTools } from "../../loop/tools/loop-tools.js";
import type { LoopEntry, LoopFireOrigin, LoopStoreData } from "../../loop/types.js";
import { FakeEventBus } from "../support/fake-event-bus.js";

beforeEach(t => {
  assert.ok("mock" in t);
  t.mock.timers.enable({ apis: ["Date", "setTimeout"], now: new Date(2026, 8, 21, 8, 2, 30).getTime() });
});

function fixture(t: TestContext) {
  const tools = new Map<string, any>();
  const bus = new FakeEventBus();
  const store = new LoopStore();
  const writes: LoopStoreData[] = [];
  const adds: string[] = [], widgets: number[] = [], journal: unknown[] = [];
  const fires: Array<{ id: string; origin: LoopFireOrigin }> = [];
  const pi: any = { events: bus, registerTool: (tool: any) => tools.set(tool.name, tool), appendEntry: (...args: unknown[]) => journal.push(args) };
  store.onChange = snapshot => writes.push(snapshot);
  const fire = (entry: LoopEntry, origin: LoopFireOrigin) => {
    fires.push({ id: entry.id, origin });
    store.fire(entry.id, origin);
  };
  const scheduler = new CronScheduler(store, fire);
  const triggers = new TriggerSystem(pi, scheduler, store, fire);
  const add = triggers.add.bind(triggers);
  triggers.add = entry => { adds.push(entry.id); add(entry); };
  registerLoopTools({ pi, getStore: () => store, getTriggerSystem: () => triggers, getScheduler: () => scheduler,
    getMonitorManager: () => ({ get: () => undefined }), updateWidget: () => { widgets.push(1); } });
  t.after(() => triggers.stop());
  const call = (name: string, input: any = {}, signal?: AbortSignal) => tools.get(name).execute("a07", input, signal);
  return { tools, bus, store, scheduler, triggers, writes, adds, widgets, journal, fires, call,
    create: (trigger: string, options: Record<string, unknown> = {}) => call("LoopCreate", { trigger, prompt: "Check audit state", triggerType: "hybrid", ...options }),
  };
}

function unchanged(f: ReturnType<typeof fixture>, before: LoopStoreData) {
  assert.deepEqual(f.store.snapshot(), before);
  assert.equal(f.writes.length, 0);
  assert.equal(f.adds.length, 0);
  assert.equal(f.widgets.length, 0);
  assert.equal(f.journal.length, 0);
  assert.equal(f.bus.listenerCount(), 0);
  assert.equal(f.scheduler.nextFire(String(before.nextId)), undefined);
  assert.deepEqual(f.fires, []);
}

for (const cron of ["*/5 * * * *", "0 9 * * 1-5", "0,30 9-17 * * 1-5", "0-30/5 * * * *"]) {
  test(`complete cron reaches LoopCreate: ${cron}`, async t => {
    const f = fixture(t);
    const result = await f.create(`cron: ${cron} event: audit:test`);
    assert.equal(result.details.tone, "success");
    assert.deepEqual(f.store.get("1")?.trigger, { type: "hybrid", cron, event: { source: "audit:test" }, debounceMs: 30000 });
    assert.equal(f.bus.listenerCount("audit:test"), 1);
    assert.ok(f.scheduler.nextFire("1")! > Date.now());
    assert.ok(result.content[0].text.includes(cron));
    assert.ok(result.content[0].text.includes("audit:test"));
    const listing = await f.call("LoopList");
    assert.ok(listing.content[0].text.includes(cron));
    assert.ok(listing.content[0].text.includes("audit:test"));
  });
}

const aliases = [
  ["cron: 1h event: audit:hybrid", "0 * * * *", "audit:hybrid"],
  ["cron 1h event audit", "0 * * * *", "audit"],
  ["cron:1h event:audit", "0 * * * *", "audit"],
  ["event: audit cron: 1h", "0 * * * *", "audit"],
  ["event audit cron 1h", "0 * * * *", "audit"],
  ["event: audit cron: */5 * * * *", "*/5 * * * *", "audit"],
  ["event:audit:cron:done cron:*/5 * * * *", "*/5 * * * *", "audit:cron:done"],
  [" \tcron:\n0,30  9-17\t*\n* 1-5\tevent: Audit:event:Done \n", "0,30 9-17 * * 1-5", "Audit:event:Done"],
  ["cron: 1 h event: audit", "0 * * * *", "audit"],
  ["1h", "0 * * * *", "tool_execution_start"],
  ["cron: 1h", "0 * * * *", "tool_execution_start"],
  ["cron 1h", "0 * * * *", "tool_execution_start"],
  [" */5\t*  *\n* * ", "*/5 * * * *", "tool_execution_start"],
  ["cron: 1h event: 'audit'", "0 * * * *", "'audit'"],
  ["cron: 1h event: audit\\test", "0 * * * *", "audit\\test"],
  ["cron: 1h event: cron:event:hybrid", "0 * * * *", "cron:event:hybrid"],
] as const;
for (const [input, cron, source] of aliases) test(`compatible grammar: ${JSON.stringify(input)}`, async t => {
  const f = fixture(t);
  await f.create(input);
  assert.deepEqual(f.store.get("1")?.trigger, { type: "hybrid", cron, event: { source }, debounceMs: 30000 });
  assert.equal(f.bus.listenerCount(source), 1);
  assert.ok(f.scheduler.nextFire("1"));
});

test("pure helper extracts complete clauses and leaves schedule validation to the shared parser", () => {
  for (const [input, schedule, eventSource] of aliases) {
    const extracted = parseHybridTriggerInput(input);
    // Shorthand is extracted, not normalized to cron by this helper.
    assert.equal(extracted.eventSource, eventSource);
    if (schedule !== "0 * * * *") assert.equal(extracted.schedule, schedule);
    else assert.ok(["1h", "1 h"].includes(extracted.schedule));
  }
  assert.deepEqual(parseHybridTriggerInput("cron: 61 * * * * event: audit"), { schedule: "61 * * * *", eventSource: "audit" });
  assert.throws(() => parseHybridTriggerInput("cron: 1h event:"), /^Error: Invalid hybrid trigger:/);
});

test("canonical inference and explicit hybrid agree; explicit event remains authoritative", async t => {
  const f = fixture(t);
  await f.create("cron: */5 * * * * event: audit:test");
  await f.create("cron: */5 * * * * event: audit:test", { triggerType: undefined });
  assert.deepEqual(f.store.get("1")?.trigger, f.store.get("2")?.trigger);
  const source = "cron:event:hybrid";
  await f.create(source, { triggerType: "event" });
  assert.deepEqual(f.store.get("3")?.trigger, { type: "event", source });
  assert.equal(f.bus.listenerCount(source), 1);
});

const malformed = ["", "  ", "cron:", "event: audit", "cron: event: audit", "cron: 1h event:",
  "event: cron: 1h", "event: audit cron:", "cron: 1h cron: 2h event: audit",
  "cron: 1h event: audit event: other", "event: audit event: other cron: 1h",
  "event: audit cron: 1h cron: 2h", "junk cron: 1h event: audit", "cron: 1h event: audit junk",
  "hybrid: cron: 1h event: audit", 'cron: 1h event: "two words"', "cron: 1h event: two\\ words",
  "cron1h eventaudit", "cron:1hevent:audit", "Cron: 1h event: audit", "cron: 1h Event: audit",
  "cron: */5 * * * event: audit", "cron: 0 */5 * * * * event: audit", "cron: 0 0 * * * 2026 event: audit",
  "cron: 61 * * * * event: audit", "cron: @hourly event: audit", '{"cron":"1h","event":"audit"}',
  "cron: 0m event: audit", "cron: 2d event: audit"];
for (const input of malformed) test(`reject without side effects: ${JSON.stringify(input)}`, async t => {
  const f = fixture(t); const before = f.store.snapshot();
  await assert.rejects(f.create(input), error => {
    assert.ok(error instanceof Error);
    assert.match(error.message, /^Invalid hybrid trigger:/);
    assert.ok(error.message.includes("cron: */5 * * * * event: audit:test"));
    if (input.includes("61 *")) assert.match(error.message, /Invalid cron expression/);
    if (input.includes("2d")) assert.match(error.message, /Unsupported cron interval/);
    if (input.includes("0m")) assert.match(error.message, /positive safe integer/);
    return true;
  });
  unchanged(f, before);
});

test("impossible hybrid schedule retains A03 preflight without allocating an ID", async t => {
  const f = fixture(t); const before = f.store.snapshot();
  await assert.rejects(f.create("cron: 0 0 31 2 * event: audit"), /No matching time/);
  unchanged(f, before);
});

for (const debounceMs of [undefined, 0, 1234]) test(`defaults and options survive, debounce ${debounceMs}`, async t => {
  const f = fixture(t);
  await f.create("cron: */5 * * * * event: audit", { debounceMs });
  const entry = f.store.get("1")!;
  assert.equal(entry.recurring, true); assert.equal(entry.maxFires, 25); assert.equal(entry.readOnly, undefined);
  assert.equal(entry.expiresAt - entry.createdAt, 7 * 24 * 60 * 60 * 1000);
  assert.deepEqual(entry.trigger, { type: "hybrid", cron: "*/5 * * * *", event: { source: "audit" }, debounceMs: debounceMs ?? 30000 });
  await f.create("cron: 1h event: custom", { recurring: false, maxFires: 3, readOnly: true, debounceMs });
  const custom = f.store.get("2")!;
  assert.equal(custom.recurring, false); assert.equal(custom.maxFires, 3); assert.equal(custom.readOnly, true);
  f.bus.emit("custom", {});
  assert.equal(f.store.get("2"), undefined);
  assert.equal(f.bus.listenerCount("custom"), 0); assert.equal(f.scheduler.nextFire("2"), undefined);
});

test("configured event alone fires and persists event origin/count", async t => {
  const f = fixture(t);
  await f.create("cron: */5 * * * * event: audit:test", { debounceMs: 0 });
  f.bus.emit("unrelated", {}); assert.deepEqual(f.fires, []);
  f.bus.emit("audit:test", {});
  assert.deepEqual(f.fires, [{ id: "1", origin: "event" }]);
  assert.equal(f.store.get("1")?.fireCount, 1);
  assert.equal(f.writes.at(-1)?.loops[0].fireCount, 1);
});

test("timer fires at its returned due time without an event", async t => {
  const f = fixture(t);
  await f.create("cron: */5 * * * * event: audit:test");
  const due = f.scheduler.nextFire("1")!;
  t.mock.timers.setTime(due - 1); f.scheduler.pump(Date.now()); assert.deepEqual(f.fires, []);
  t.mock.timers.setTime(due); f.scheduler.pump(Date.now());
  assert.deepEqual(f.fires, [{ id: "1", origin: "scheduler" }]);
  assert.equal(f.store.get("1")?.fireCount, 1);
  assert.ok(f.scheduler.nextFire("1")! > due);
});

for (const first of ["event", "scheduler"] as const) test(`cap one, ${first} first never dispatches twice`, async t => {
  const f = fixture(t);
  await f.create("cron: */5 * * * * event: audit:test", { maxFires: 1, debounceMs: 0 });
  const due = f.scheduler.nextFire("1")!;
  const pump = () => { t.mock.timers.setTime(due); f.scheduler.pump(Date.now()); };
  const emit = () => f.bus.emit("audit:test", {});
  if (first === "event") { emit(); pump(); } else { pump(); emit(); }
  assert.deepEqual(f.fires, [{ id: "1", origin: first }]);
  assert.equal(f.store.get("1"), undefined);
  assert.equal(f.scheduler.nextFire("1"), undefined); assert.equal(f.bus.listenerCount(), 0);
});

for (const action of ["pause", "delete"] as const) test(`cancel before first wake: ${action}`, async t => {
  const f = fixture(t);
  await f.create("cron: */5 * * * * event: audit:test");
  const due = f.scheduler.nextFire("1")!;
  await f.call("LoopDelete", { id: "1", action });
  assert.equal(f.bus.listenerCount(), 0); assert.equal(f.scheduler.nextFire("1"), undefined);
  t.mock.timers.setTime(due); f.scheduler.pump(Date.now()); f.bus.emit("audit:test", {});
  assert.deepEqual(f.fires, []);
});

for (const action of ["pause", "delete", "stop"] as const) test(`pending event debounce is canceled by ${action}`, async t => {
  const f = fixture(t);
  await f.create("cron: */5 * * * * event: audit:test", { debounceMs: 30000, maxFires: 4 });
  f.bus.emit("audit:test", {});
  t.mock.timers.tick(1000); f.bus.emit("audit:test", {});
  assert.equal(f.fires.length, 1);
  if (action === "stop") f.triggers.stop();
  else await f.call("LoopDelete", { id: "1", action });
  assert.equal(f.bus.listenerCount(), 0); assert.equal(f.scheduler.nextFire("1"), undefined);
  t.mock.timers.tick(60000); f.scheduler.pump(Date.now()); f.bus.emit("audit:test", {});
  assert.equal(f.fires.length, 1);
  assert.equal(f.store.get("1")?.status, action === "delete" ? undefined : action === "pause" ? "paused" : "active");
});

test("debounce still dispatches a pending event when not canceled", async t => {
  const f = fixture(t);
  await f.create("cron: */5 * * * * event: audit", { debounceMs: 30000, maxFires: 4 });
  f.bus.emit("audit", {}); t.mock.timers.tick(1000); f.bus.emit("audit", {});
  t.mock.timers.tick(28999); assert.equal(f.fires.length, 1);
  t.mock.timers.tick(1); assert.equal(f.fires.length, 2);
  assert.equal(f.store.get("1")?.fireCount, 2);
});

for (const legacy of [false, true]) test(`healthy restore and repeated start: ${legacy ? "legacy structured shorthand" : "full cron"}`, async t => {
  const old = fixture(t);
  if (legacy) {
    const entry = old.store.create({ type: "hybrid", cron: "0 * * * *", event: { source: "legacy:Audit", filter: '{"ok":true}' }, debounceMs: 0 }, "old shorthand", { recurring: true, maxFires: 5, readOnly: true });
    old.triggers.add(entry);
  } else await old.create("cron: 0,30 9-17 * * 1-5 event: audit:event:Done", { debounceMs: 0, maxFires: 5, readOnly: true });
  const entry = old.store.get("1")!;
  assert.equal(entry.trigger.type, "hybrid");
  if (entry.trigger.type !== "hybrid") throw new Error("Expected hybrid");
  const source = entry.trigger.event.source;
  old.bus.emit(source, { ok: true });
  await old.create("cron: 1h event: paused"); await old.call("LoopDelete", { id: "2", action: "pause" });
  const snapshot = old.store.snapshot(); old.triggers.stop();
  assert.equal(old.bus.listenerCount(), 0); assert.equal(old.scheduler.nextFire("1"), undefined);
  const restored = fixture(t);
  restored.store.restoreSnapshot(snapshot); restored.triggers.start();
  const due = restored.scheduler.nextFire("1")!;
  restored.triggers.start();
  assert.deepEqual(restored.store.snapshot(), snapshot);
  assert.deepEqual(restored.writes, []); assert.deepEqual(restored.fires, []);
  assert.equal(restored.bus.listenerCount(source), 1); assert.equal(restored.bus.listenerCount(), 1);
  assert.equal(restored.scheduler.nextFire("1"), due); assert.equal(restored.scheduler.nextFire("2"), undefined);
  if (legacy) { restored.bus.emit(source, { ok: false }); assert.deepEqual(restored.fires, []); }
  restored.bus.emit(source, { ok: true });
  t.mock.timers.setTime(due); restored.scheduler.pump(Date.now());
  assert.deepEqual(restored.fires, [{ id: "1", origin: "event" }, { id: "1", origin: "scheduler" }]);
  assert.equal(restored.store.get("1")?.fireCount, 3);
  assert.deepEqual(restored.store.get("1")?.trigger, snapshot.loops[0].trigger);
  assert.equal(restored.store.get("1")?.readOnly, true);
  assert.equal(restored.store.get("2")?.status, "paused");
});

test("ordinary cron, shorthand, event and idle routing remain unchanged", async t => {
  const f = fixture(t);
  for (const [trigger, triggerType] of [["*/5 * * * *", undefined], ["1h", undefined], ["audit:test", undefined], ["idle", "idle"]]) {
    await f.call("LoopCreate", { trigger, triggerType, prompt: "ordinary" });
  }
  assert.deepEqual(f.store.list().map(e => e.trigger), [
    { type: "cron", schedule: "*/5 * * * *" }, { type: "cron", schedule: "0 * * * *" },
    { type: "event", source: "audit:test" }, { type: "dynamic" },
  ]);
  await assert.rejects(f.call("LoopCreate", { trigger: "bad", triggerType: "cron", prompt: "bad" }), /^Error: Cannot parse interval/);
});

test("registered schema keeps required fields, property set and strict boundary", t => {
  const f = fixture(t); const schema = f.tools.get("LoopCreate").parameters;
  assert.deepEqual(schema.required, ["trigger", "prompt"]);
  assert.equal(schema.additionalProperties, false);
  assert.deepEqual(Object.keys(schema.properties).sort(), ["trigger", "prompt", "recurring", "triggerType", "debounceMs", "readOnly", "maxFires"].sort());
  for (const [name, type] of Object.entries({ trigger: "string", prompt: "string", recurring: "boolean", triggerType: "string", debounceMs: "number", readOnly: "boolean", maxFires: "integer" })) {
    assert.equal(schema.properties[name].type, type);
  }
  for (const trigger of ["cron: */5 * * * * event: audit:test", "cron: 1h event: audit:hybrid"]) {
    const input = { trigger, triggerType: "hybrid", prompt: "Check audit state", recurring: true, debounceMs: 0, maxFires: 1, readOnly: true };
    assert.equal(Check(schema, input), true);
    assert.equal(Check(schema, { ...input, unknown: "not allowed" }), false);
    assert.equal(Check(schema, { ...input, trigger: 5 }), false);
    assert.equal(Check(schema, { ...input, prompt: undefined }), false);
  }
  assert.equal(Check(schema, { prompt: "missing trigger" }), false);
});

test("pre-aborted creation still performs no parsing or registration", async t => {
  const f = fixture(t); const before = f.store.snapshot();
  await assert.rejects(f.call("LoopCreate", { trigger: "cron: */5 * * * * event: audit", triggerType: "hybrid", prompt: "abort" }, AbortSignal.abort()), /abort/i);
  unchanged(f, before);
});
