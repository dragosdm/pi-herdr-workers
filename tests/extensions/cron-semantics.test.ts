import assert from "node:assert/strict";
import { execFileSync } from "node:child_process";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test, { beforeEach } from "node:test";
import { computeJitter, cronToNextFire, isValidCronExpression, parseInterval } from "../../loop/loop-parse.js";
import { compileCronExpression, searchCron } from "../../loop/cron-search.js";
import { LoopStore } from "../../loop/store.js";
import { CronScheduler } from "../../loop/scheduler.js";
import { TriggerSystem } from "../../loop/trigger-system.js";
import { registerLoopTools } from "../../loop/tools/loop-tools.js";
import { validateWorkflowDefinition } from "../../loop/workflow-definition.js";
import type { LoopEntry, LoopExpiryDisposition, LoopFireOrigin, Trigger, WorkflowDefinition } from "../../loop/types.js";
import { FakeEventBus } from "../support/fake-event-bus.js";

beforeEach(t => {
  assert.ok("mock" in t);
  t.mock.timers.enable({ apis: ["Date"], now: new Date(2026, 8, 20, 12).getTime() });
});

function expectNext(expression: string, start: Date, expected: Date) {
  const original = start.getTime();
  const result = cronToNextFire(expression, start);
  assert.equal(result.getTime(), expected.getTime(), `${expression}: ${result.toISOString()} in ${process.env.TZ}`);
  assert.ok(result.getTime() > original);
  assert.equal(result.getSeconds(), 0);
  assert.equal(result.getMilliseconds(), 0);
  assert.notEqual(result, start);
  assert.equal(start.getTime(), original, "never mutate caller's Date");
}

test("restricted day fields use OR: the next Monday need not be the first", () => {
  expectNext("0 0 1 * 1", new Date(2026, 8, 20, 12), new Date(2026, 8, 21));
});

test("leap day beyond one year remains a valid occurrence", () => {
  expectNext("0 0 29 2 *", new Date(2026, 8, 20, 12), new Date(2028, 1, 29));
});

for (const [expression, start, expected] of [
  ["* * * * *", new Date(2026, 8, 20, 12), new Date(2026, 8, 20, 12, 1)],
  ["* * * * *", new Date(2026, 8, 20, 12, 4, 59, 999), new Date(2026, 8, 20, 12, 5)],
  ["*/5 * * * *", new Date(2026, 8, 20, 12, 5), new Date(2026, 8, 20, 12, 10)],
  ["1,10-20/3 12-14/2 * * *", new Date(2026, 8, 20, 12, 10), new Date(2026, 8, 20, 12, 13)],
  ["1,10-20/3 12-14/2 * * *", new Date(2026, 8, 20, 12, 20), new Date(2026, 8, 20, 14, 1)],
  ["0 0 1 * *", new Date(2026, 8, 30, 23, 59), new Date(2026, 9, 1)],
  ["0 0 1 1 *", new Date(2026, 11, 31, 23, 59), new Date(2027, 0, 1)],
  ["0 0 1 * 1", new Date(2026, 8, 29), new Date(2026, 9, 1)],
  ["0 0 1 * 1", new Date(2027, 0, 31), new Date(2027, 1, 1)],
  ["0 0 1 * 1", new Date(2027, 1, 1), new Date(2027, 1, 8)],
  ["0 0 * * 1", new Date(2026, 8, 21), new Date(2026, 8, 28)],
  ["0 0 1 * *", new Date(2026, 8, 20), new Date(2026, 9, 1)],
  ["0 0 * * *", new Date(2026, 8, 20), new Date(2026, 8, 21)],
  ["0 0 */2 * 1", new Date(2026, 8, 21), new Date(2026, 9, 5)],
  ["0 0 1-31 * 1", new Date(2026, 8, 21), new Date(2026, 8, 22)],
  ["0 0 *,1 * 1", new Date(2026, 8, 21), new Date(2026, 8, 28)],
  ["0 0 1,* * 1", new Date(2026, 8, 21), new Date(2026, 8, 22)],
  ["0 0 1 * *,1", new Date(2026, 8, 21), new Date(2026, 9, 1)],
  ["0 0 1 * 1,*", new Date(2026, 8, 21), new Date(2026, 8, 22)],
  ["0 0 31 2 1", new Date(2026, 8, 20), new Date(2027, 1, 1)],
  ["0 0 29 2 *", new Date(2096, 2, 1), new Date(2104, 1, 29)],
  ["0 0 29 2 */7", new Date(2032, 2, 1), new Date(2060, 1, 29)],
  ["0 0 29 2 *", new Date(2028, 1, 29), new Date(2032, 1, 29)],
] as const) {
  test(`earliest local occurrence: ${expression} after ${start.toISOString()}`, () => expectNext(expression, start, expected));
}

test("compiled membership retains wildcard metadata independently of union contents", () => {
  const stepped = compileCronExpression("0 0 */2 1-12/2 */7")!;
  assert.deepEqual([...stepped[2].values], Array.from({ length: 16 }, (_, i) => 1 + i * 2));
  assert.deepEqual([...stepped[3].values], [1, 3, 5, 7, 9, 11]);
  assert.deepEqual([...stepped[4].values], [0]);
  for (const [token, wildcardBased] of [["*,1", true], ["1,*", false], ["1-31", false]] as const) {
    const field = compileCronExpression(`0 0 ${token} * 1`)![2];
    assert.equal(field.values.size, 31);
    assert.equal(field.wildcardBased, wildcardBased);
  }
});

test("accepted numeric syntax and parseInterval shape remain unchanged", () => {
  for (const expression of ["* * * * *", "00,00,01-05/02 00-23/24 01-31/31 01-12/12 0-6/7", "*/60 */24 */31 */12 */7", "1,* 0 1,* * *,1", "  0\t0  31 2 *  "]) {
    assert.equal(isValidCronExpression(expression), true, expression);
    assert.deepEqual(parseInterval(expression), { cron: expression.trim(), description: `cron: ${expression.trim()}` });
  }
  expectNext("00,00,01-05/02 * * * *", new Date(2026, 8, 20, 12, 1), new Date(2026, 8, 20, 12, 3));
});

test("malformed fields are rejected without broadening the dialect", () => {
  for (const expression of ["", "* * * *", "* * * * * *", "60 * * * *", "* 24 * * *", "* * 0 * *", "* * 32 * *", "* * * 0 *", "* * * 13 *", "* * * * 7", "*/0 * * * *", "*/61 * * * *", "* */25 * * *", "* * */32 * *", "* * * */13 *", "* * * * */8", "5/10 * * * *", "1, * * * *", ",1 * * * *", "1,,2 * * * *", "3-1 * * * *", "1-2-3 * * * *", "*/2/3 * * * *", "-1 * * * *", "+1 * * * *", "1.5 * * * *", "* * * JAN *", "* * * * MON", "@daily", "0 0 L * *", "0 0 1W * *", "0 0 ? * 1#2"]) {
    assert.equal(isValidCronExpression(expression), false, expression);
    assert.throws(() => cronToNextFire(expression), /^Error: Invalid cron expression:/);
  }
});

for (const expression of ["0 0 30 2 *", "0 0 31 2 *", "0 0 31 4 *"]) {
  test(`impossible calendar date is syntax-valid and bounded: ${expression}`, () => {
    const stats = { dates: 0, minutes: 0 };
    assert.equal(isValidCronExpression(expression), true);
    assert.throws(() => searchCron(expression, new Date(), { stats }), /No matching time found within 400 years plus 1 day/);
    assert.ok(stats.dates >= 146097 && stats.dates <= 146100, JSON.stringify(stats));
    assert.equal(stats.minutes, 0);
  });
}

test("invalid start and unrepresentable search horizon have separate errors", () => {
  assert.throws(() => cronToNextFire("* * * * *", new Date(NaN)), /Invalid cron start date/);
  assert.throws(() => cronToNextFire("* * * * *", new Date(8.64e15 - 86400000)), /Unrepresentable cron search range/);
});

test("internal date-jump guard rejects invalid, stationary and backward jumps", () => {
  for (const jump of [NaN, Infinity, 0, -60000]) {
    assert.throws(() => searchCron("0 0 31 2 *", new Date(), { nextDayBoundary: date => date.getTime() + jump }), /date jump.*finite and increasing/);
  }
});

test("internal horizon includes its endpoint and preserves start wall-clock time", () => {
  const start = new Date(2026, 8, 21, 12);
  const endpoint = new Date(2426, 8, 22, 12);
  const result = searchCron("0 12 22 9 *", start, {
    nextDayBoundary: date => date.getTime() < endpoint.getTime() ? endpoint.getTime() : endpoint.getTime() + 86400000,
  });
  assert.equal(result.getTime(), endpoint.getTime());
});

test("defensive date-iteration cap remains effective with a faulty increasing boundary", () => {
  const stats = { dates: 0, minutes: 0 };
  assert.throws(() => searchCron("0 0 31 2 *", new Date(), { stats, nextDayBoundary: date => date.getTime() + 60000 }), /No matching time found within 400 years plus 1 day/);
  assert.deepEqual(stats, { dates: 146100, minutes: 0 });
});

function inZone(zone: string, script: string): string {
  return execFileSync(process.execPath, ["--import", "tsx", "--input-type=module", "-e", `
    import assert from 'node:assert/strict';
    import { cronToNextFire } from './loop/loop-parse.ts';
    import { searchCron } from './loop/cron-search.ts';
    try { ${script} } catch (error) { console.error({ zone: process.env.TZ, node: process.version, tz: process.versions.tz }); throw error; }
  `], { cwd: process.cwd(), env: { ...process.env, TZ: zone }, encoding: "utf8", timeout: 15000 });
}

for (const zone of ["UTC", "Asia/Kathmandu"]) test(`explicit ${zone} subprocess has local cadence and strict boundaries`, () => {
  inZone(zone, `
    const start = new Date(2026, 8, 20, 12, 4, 59, 999);
    const next = cronToNextFire('*/5 * * * *', start);
    assert.deepEqual([next.getHours(), next.getMinutes(), next.getSeconds(), next.getMilliseconds()], [12, 5, 0, 0]);
    assert.equal(next.toISOString(), ${JSON.stringify(zone === "UTC" ? "2026-09-20T12:05:00.000Z" : "2026-09-20T06:20:00.000Z")});
    assert.equal(cronToNextFire('0 0 1 * 1', new Date(2026, 8, 20, 12)).getTime(), new Date(2026, 8, 21).getTime());
    assert.equal(cronToNextFire('0 0 29 2 *', start).getTime(), new Date(2028, 1, 29).getTime());
  `);
});

const transitions: Array<[string, string, string, string]> = [
  ["America/New_York", "30 2 * * *", "2026-03-07T08:00:00Z", "2026-03-09T06:30:00.000Z"],
  ["America/New_York", "30 1 * * *", "2026-11-01T04:00:00Z", "2026-11-01T05:30:00.000Z"],
  ["America/New_York", "30 1 * * *", "2026-11-01T05:30:00Z", "2026-11-01T06:30:00.000Z"],
  ["America/New_York", "30 1 * * *", "2026-11-01T06:15:30Z", "2026-11-01T06:30:00.000Z"],
  ["America/New_York", "* * * * *", "2026-11-01T06:15:30Z", "2026-11-01T06:16:00.000Z"],
  ["Australia/Lord_Howe", "45 1 * * *", "2026-04-04T14:00:00Z", "2026-04-04T14:45:00.000Z"],
  ["Australia/Lord_Howe", "45 1 * * *", "2026-04-04T14:45:00Z", "2026-04-04T15:15:00.000Z"],
  ["Australia/Lord_Howe", "15 2 * * *", "2026-10-02T16:00:00Z", "2026-10-04T15:15:00.000Z"],
  ["Pacific/Apia", "0 0 * * *", "2011-12-29T10:00:00Z", "2011-12-30T10:00:00.000Z"],
  ["Pacific/Apia", "0 0 30 12 *", "2011-12-29T10:00:00Z", "2012-12-29T10:00:00.000Z"],
  // Midnight itself is absent in this modern transition, so do not shift it to 01:00.
  ["America/Sao_Paulo", "0 0 * * *", "2018-11-03T03:00:00Z", "2018-11-05T02:00:00.000Z"],
];
for (const [zone, expression, from, expected] of transitions) test(`${zone}: ${expression} after ${from}`, () => {
  inZone(zone, `assert.equal(cronToNextFire(${JSON.stringify(expression)}, new Date(${JSON.stringify(from)})).toISOString(), ${JSON.stringify(expected)});`);
});

test("25 impossible searches complete under a generous subprocess hang guard", t => {
  const output = inZone("UTC", `
    const start = performance.now();
    const stats = { dates: 0, minutes: 0 };
    for (let i = 0; i < 25; i++) assert.throws(() => searchCron('0 0 31 2 *', new Date('2026-09-20T12:00:00Z'), { stats }), /No matching time found within 400 years plus 1 day/);
    assert.ok(stats.dates <= 25 * 146100); assert.equal(stats.minutes, 0);
    console.log(JSON.stringify({ ...stats, searches: 25, durationMs: performance.now() - start, node: process.version, tz: process.versions.tz }));
  `);
  t.diagnostic(output.trim());
});

const sparse: Trigger = { type: "cron", schedule: "0 0 29 2 *" };
const event: Trigger = { type: "event", source: "a06:event" };
const hybrid: Trigger = { type: "hybrid", cron: "0 0 29 2 *", event: { source: "a06:hybrid" }, debounceMs: 0 };
function fixture(store = new LoopStore()) {
  const bus = new FakeEventBus();
  const tools = new Map<string, any>();
  const fires: Array<[string, LoopFireOrigin]> = [];
  const expiries: Array<[string, LoopExpiryDisposition]> = [];
  let canExpire = true;
  const fire = (entry: LoopEntry, origin: LoopFireOrigin) => { fires.push([entry.id, origin]); store.fire(entry.id, origin); };
  const scheduler = new CronScheduler(store, fire, (entry, disposition) => expiries.push([entry.id, disposition]), () => canExpire);
  const pi: any = { events: bus, registerTool: (tool: any) => tools.set(tool.name, tool) };
  const triggers = new TriggerSystem(pi, scheduler, store, fire);
  registerLoopTools({ pi, getStore: () => store, getTriggerSystem: () => triggers, getScheduler: () => scheduler, getMonitorManager: () => ({ get: () => undefined }), updateWidget() {} });
  return { store, scheduler, triggers, bus, fires, expiries, setCanExpire: (value: boolean) => { canExpire = value; },
    call: (name: string, input: any) => tools.get(name).execute("a06", input),
  };
}

function workflow(schedule: string): WorkflowDefinition {
  return { version: 1, initialState: "working", states: {
    working: { prompt: "work", loop: { schedule }, on: { done: "done" } },
    done: { prompt: "done", terminal: "completed" },
  } };
}

test("LoopCreate accepts distant leap day, preserves options and expires without a cron wake", async () => {
  const f = fixture();
  try {
    const result = await f.call("LoopCreate", { trigger: "0 0 29 2 *", triggerType: "cron", prompt: "leap day", readOnly: true, maxFires: 3 });
    assert.match(result.content[0].text, /created:/);
    assert.equal(f.store.list().length, 1);
    const entry = f.store.list()[0];
    assert.deepEqual([entry.status, entry.maxFires, entry.readOnly, entry.fireCount], ["active", 3, true, 0]);
    assert.equal(entry.expiresAt - entry.createdAt, 7 * 86400000);
    assert.equal(f.scheduler.nextFire(entry.id), undefined);
    f.scheduler.pump(entry.expiresAt - 1);
    assert.equal(f.store.get(entry.id)?.status, "active");
    assert.deepEqual(f.fires, []); assert.deepEqual(f.expiries, []);
    f.scheduler.pump(entry.expiresAt); f.scheduler.pump(entry.expiresAt + 1);
    assert.equal(f.store.get(entry.id), undefined);
    assert.deepEqual(f.expiries, [[entry.id, "deleted"]]);
    assert.deepEqual(f.fires, []);
  } finally { f.triggers.stop(); }
});

for (const edge of ["before", "equal", "jitter-equal", "jitter-crossing"] as const) test(`lifetime edge ${edge} uses the jittered occurrence`, () => {
  const f = fixture();
  const entry = f.store.create({ type: "cron", schedule: "* * * * *" }, "edge", { recurring: true });
  const next = cronToNextFire("* * * * *").getTime();
  const jitter = computeJitter(entry.id, true, 30);
  assert.ok(jitter > 0);
  entry.expiresAt = edge === "before" ? next + jitter + 1 : edge === "equal" ? next : edge === "jitter-equal" ? next + jitter : next + jitter - 1;
  f.scheduler.add(entry);
  assert.equal(f.scheduler.nextFire(entry.id), edge === "before" ? next + jitter : undefined);
  f.scheduler.pump(entry.expiresAt - 1);
  assert.equal(f.fires.length, edge === "before" ? 1 : 0);
  f.scheduler.pump(entry.expiresAt); f.scheduler.pump(entry.expiresAt + 1);
  assert.equal(f.store.get(entry.id), undefined);
  assert.deepEqual(f.expiries, [[entry.id, "deleted"]]);
  f.triggers.stop();
});

test("re-arming a distant schedule removes an earlier fire registration", () => {
  const f = fixture(); const entry = f.store.create({ type: "cron", schedule: "* * * * *" }, "re-arm", { recurring: true });
  f.scheduler.add(entry); const oldFire = f.scheduler.nextFire(entry.id)!;
  assert.ok(oldFire);
  f.store.updateMetadata(entry.id, { trigger: sparse }); f.scheduler.add(f.store.get(entry.id)!);
  assert.equal(f.scheduler.nextFire(entry.id), undefined);
  f.scheduler.pump(oldFire); assert.equal(f.fires.length, 0);
  f.scheduler.pump(entry.expiresAt); assert.deepEqual(f.expiries, [[entry.id, "deleted"]]);
  f.triggers.stop();
});

test("expiry-only registration honors canExpire then notifies once", () => {
  const f = fixture(); const entry = f.store.create(sparse, "sparse", { recurring: true });
  f.scheduler.add(entry); f.setCanExpire(false); f.scheduler.pump(entry.expiresAt);
  assert.equal(f.store.get(entry.id)?.status, "active"); assert.deepEqual(f.expiries, []);
  f.setCanExpire(true); f.scheduler.pump(entry.expiresAt); f.scheduler.pump(entry.expiresAt);
  assert.deepEqual(f.expiries, [[entry.id, "deleted"]]);
});

test("durable restoration preserves sparse, paused, hybrid and ordinary entries without replay", () => {
  const dir = mkdtempSync(join(tmpdir(), "a06-cron-")); const path = join(dir, "loops.json");
  try {
    const store = new LoopStore(path);
    const leap = store.create(sparse, "sparse", { recurring: true, maxFires: 5, readOnly: true });
    store.fire(leap.id);
    const normal = store.create({ type: "cron", schedule: "*/5 * * * *" }, "normal", { recurring: true });
    const corrected = store.create({ type: "cron", schedule: "0 0 1 * 1" }, "corrected", { recurring: true });
    store.fire(corrected.id);
    const ev = store.create(event, "event", { recurring: true, maxFires: 3 });
    const hy = store.create(hybrid, "hybrid", { recurring: true, maxFires: 2 });
    const paused = store.create(sparse, "paused", { recurring: true }); store.pause(paused.id);
    const before = store.snapshot();
    const f = fixture(new LoopStore(path));
    try {
      f.triggers.start(); f.triggers.start(); f.triggers.stop(); f.triggers.start();
      assert.deepEqual(f.store.snapshot(), before, "no counters, timestamps, IDs, expressions or status change on restoration");
      assert.equal(f.fires.length, 0);
      assert.equal(f.scheduler.nextFire(leap.id), undefined);
      assert.equal(f.scheduler.nextFire(hy.id), undefined);
      assert.equal(f.scheduler.nextFire(paused.id), undefined);
      assert.ok(f.scheduler.nextFire(normal.id));
      assert.equal(f.scheduler.nextFire(corrected.id), new Date(2026, 8, 21).getTime() + computeJitter(corrected.id, true, 30));
      assert.equal(f.bus.listenerCount(), 2);
      f.bus.emit("a06:event", {}); f.bus.emit("a06:hybrid", {});
      assert.deepEqual(f.fires, [[ev.id, "event"], [hy.id, "event"]]);
      assert.equal(f.store.get(leap.id)?.fireCount, 1);
      f.bus.emit("a06:hybrid", {}); f.bus.emit("a06:hybrid", {});
      assert.equal(f.fires.filter(([id]) => id === hy.id).length, 2);
      assert.equal(f.store.get(hy.id), undefined); assert.equal(f.bus.listenerCount("a06:hybrid"), 0);
      assert.deepEqual(f.store.get(leap.id), before.loops.find(e => e.id === leap.id));
      f.scheduler.pump(paused.expiresAt);
      assert.equal(f.store.get(paused.id)?.status, "paused");
      assert.ok(!f.expiries.some(([id]) => id === paused.id), "paused sparse entry has no expiry registration");
    } finally { f.triggers.stop(); }
    assert.equal(f.bus.listenerCount(), 0);
  } finally { rmSync(dir, { recursive: true, force: true }); }
});

test("structured hybrid retains its event side while its cron is expiry-only", () => {
  const f = fixture(); const entry = f.store.create(hybrid, "hybrid", { recurring: true, maxFires: 2 });
  try {
    f.triggers.add(entry); assert.equal(f.scheduler.nextFire(entry.id), undefined);
    f.scheduler.pump(Date.now() + 60000); assert.deepEqual(f.fires, []);
    f.bus.emit("a06:hybrid", {}); assert.equal(f.store.get(entry.id)?.fireCount, 1);
    f.bus.emit("a06:hybrid", {}); f.bus.emit("a06:hybrid", {});
    assert.equal(f.fires.length, 2); assert.equal(f.store.get(entry.id), undefined);
    f.scheduler.pump(entry.expiresAt); assert.deepEqual(f.expiries, []);
  } finally { f.triggers.stop(); }
});

test("active workflow schedule shares corrected OR matching and sparse expiry pauses it", () => {
  const f = fixture();
  const weekly = f.store.create({ type: "dynamic" }, "weekly", { recurring: true, workflow: workflow("0 0 1 * 1") });
  const leap = f.store.create({ type: "dynamic" }, "leap", { recurring: true, workflow: workflow("0 0 29 2 *") });
  assert.equal(validateWorkflowDefinition(workflow("0 0 31 2 *")), undefined, "definition validation stays syntax-only");
  f.scheduler.start();
  assert.equal(f.scheduler.nextFire(weekly.id), new Date(2026, 8, 21).getTime() + computeJitter(weekly.id, true, 30));
  assert.equal(f.scheduler.nextFire(leap.id), undefined);
  f.scheduler.pump(leap.expiresAt);
  assert.equal(f.store.get(leap.id)?.status, "paused");
  assert.deepEqual(f.expiries.find(([id]) => id === leap.id), [leap.id, "paused"]);
  assert.deepEqual(f.fires, []); f.scheduler.stop();
});

for (const action of ["remove", "stop", "pause", "delete"] as const) test(`${action} clears or retires stale expiry-only registration without firing`, () => {
  const f = fixture(); const entry = f.store.create(hybrid, "sparse", { recurring: true }); f.triggers.add(entry);
  if (action === "remove") f.triggers.remove(entry.id);
  if (action === "stop") f.triggers.stop();
  if (action === "pause") { f.store.pause(entry.id); f.triggers.remove(entry.id); }
  if (action === "delete") { f.store.delete(entry.id); f.triggers.remove(entry.id); }
  f.scheduler.pump(entry.expiresAt); f.bus.emit("a06:hybrid", {});
  assert.deepEqual(f.fires, []); assert.deepEqual(f.expiries, []);
  assert.equal(f.bus.listenerCount(), 0);
  assert.equal(f.store.get(entry.id)?.status, action === "delete" ? undefined : action === "pause" ? "paused" : "active");
  f.triggers.stop();
});

test("A03 failure safety rejects impossible creation and isolates poison beside sparse restoration", async () => {
  const f = fixture(); const before = f.store.snapshot();
  await assert.rejects(f.call("LoopCreate", { trigger: "0 0 31 2 *", prompt: "bad", triggerType: "cron" }), /No matching time found/);
  assert.deepEqual(f.store.snapshot(), before); assert.equal(f.bus.listenerCount(), 0);
  const bad = f.store.create(event, "legacy invalid hybrid", { recurring: true });
  f.store.updateMetadata(bad.id, { trigger: { ...hybrid, cron: "0 0 31 2 *" } });
  const leap = f.store.create(sparse, "valid sparse", { recurring: true });
  const ev = f.store.create(event, "healthy", { recurring: true });
  const restored = fixture(); restored.store.restoreSnapshot(f.store.snapshot());
  try {
    restored.triggers.start();
    assert.equal(restored.store.get(bad.id)?.status, "paused");
    assert.match(restored.store.get(bad.id)?.pause?.reason ?? "", /^Schedule unavailable: No matching time found/);
    assert.equal(restored.bus.listenerCount("a06:hybrid"), 0);
    assert.equal(restored.store.get(leap.id)?.status, "active"); assert.equal(restored.scheduler.nextFire(leap.id), undefined);
    restored.bus.emit("a06:event", {}); assert.deepEqual(restored.fires, [[ev.id, "event"]]);
  } finally { restored.triggers.stop(); }
});

test("ordinary defaults, one-shot retirement and dynamic scheduling remain unchanged", async () => {
  const f = fixture();
  try {
    await f.call("LoopCreate", { trigger: "5m", prompt: "defaults" });
    const cron = f.store.list()[0];
    assert.equal(cron.maxFires, 25); assert.equal(cron.expiresAt - cron.createdAt, 7 * 86400000);
    assert.equal(f.scheduler.nextFire(cron.id), cronToNextFire("*/5 * * * *").getTime() + computeJitter(cron.id, true, 5));
    const once = f.store.create({ type: "cron", schedule: "* * * * *" }, "once", { recurring: false }); f.scheduler.add(once);
    const wake = f.scheduler.nextFire(once.id)!;
    assert.equal(wake, cronToNextFire("* * * * *").getTime() + computeJitter(once.id, false, 30));
    f.scheduler.pump(wake); assert.equal(f.store.get(once.id), undefined);
    const dynamic = f.store.create({ type: "dynamic" }, "dynamic", { recurring: true, dynamic: { goal: "dynamic", iteration: 0, nextWakeAt: Date.now() + 12345 } });
    f.scheduler.add(dynamic); assert.equal(f.scheduler.nextFire(dynamic.id), Date.now() + 12345);
  } finally { f.triggers.stop(); }
});
