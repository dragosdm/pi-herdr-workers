import assert from "node:assert/strict";
import test, { beforeEach } from "node:test";
import * as intervals from "../../loop/loop-parse.js";
import { LoopStore } from "../../loop/store.js";
import { CronScheduler } from "../../loop/scheduler.js";
import { TriggerSystem } from "../../loop/trigger-system.js";
import { registerLoopTools } from "../../loop/tools/loop-tools.js";
import { registerLoopCommand } from "../../loop/commands/loop-command.js";
import type { LoopEntry, LoopStoreData } from "../../loop/types.js";
import { FakeEventBus } from "../support/fake-event-bus.js";

const { parseInterval, cronToNextFire, computeJitter } = intervals;
const timing = "Timing: local wall-clock cron with scheduler jitter, not an elapsed-time interval.";
const huge = "9".repeat(400) + "d";
beforeEach((t) => {
  assert.ok("mock" in t);
  t.mock.timers.enable({ apis: ["Date"], now: new Date(2026, 8, 20, 12, 2, 30).getTime() });
});

function fixture() {
  const tools = new Map<string, any>();
  const commands = new Map<string, any>();
  const bus = new FakeEventBus();
  const store = new LoopStore();
  const writes: LoopStoreData[] = [];
  const adds: string[] = [], widgets: number[] = [], activations: string[] = [];
  const notices: Array<[string, string]> = [];
  const pi: any = { events: bus, appendEntry() {}, registerTool: (tool: any) => tools.set(tool.name, tool), registerCommand: (name: string, command: any) => commands.set(name, command) };
  store.onChange = snapshot => writes.push(snapshot);
  const fire = (entry: LoopEntry) => { store.fire(entry.id, "scheduler"); };
  const scheduler = new CronScheduler(store, fire);
  const triggers = new TriggerSystem(pi, scheduler, store, fire);
  const add = triggers.add.bind(triggers);
  triggers.add = entry => { adds.push(entry.id); add(entry); };
  const options = { pi, getStore: () => store, getTriggerSystem: () => triggers, updateWidget: () => { widgets.push(1); }, onDynamicLoopActivated: (entry: LoopEntry) => { activations.push(entry.id); } };
  registerLoopTools({ ...options, getScheduler: () => scheduler, getMonitorManager: () => ({ get: () => undefined }) });
  registerLoopCommand(options);
  return { store, bus, scheduler, triggers, writes, adds, widgets, activations, notices, tools,
    call: (name: string, input: any) => tools.get(name).execute("a05", input),
    command: (input: string, ui: any = {}) => commands.get("loop").handler(input, { hasUI: true, ui: { notify: (message: string, tone: string) => notices.push([message, tone]), ...ui } }),
  };
}
function unchanged(f: ReturnType<typeof fixture>, before: LoopStoreData) {
  assert.deepEqual(f.store.snapshot(), before);
  assert.equal(f.writes.length, 0);
  assert.equal(f.adds.length, 0);
  assert.equal(f.widgets.length, 0);
  assert.equal(f.activations.length, 0);
  assert.equal(f.bus.listenerCount(), 0);
  assert.equal(f.scheduler.nextFire(String(before.nextId)), undefined);
}

for (const input of ["0m", "2d"]) test(`primary regression: ${input} rejects rather than rounding`, () => {
  assert.throws(() => parseInterval(input), /positive safe integer|Unsupported cron interval/);
});

const mappings = [
  ["1m", "*/1 * * * *", "1 minute"], ["2m", "*/2 * * * *", "2 minutes"],
  ["5m", "*/5 * * * *", "5 minutes"], ["10m", "*/10 * * * *", "10 minutes"],
  ["15m", "*/15 * * * *", "15 minutes"], ["30m", "*/30 * * * *", "30 minutes"],
  ["1h", "0 * * * *", "1 hour"], ["2h", "0 */2 * * *", "2 hours"],
  ["3h", "0 */3 * * *", "3 hours"], ["4h", "0 */4 * * *", "4 hours"],
  ["6h", "0 */6 * * *", "6 hours"], ["8h", "0 */8 * * *", "8 hours"],
  ["12h", "0 */12 * * *", "12 hours"], ["1d", "0 0 * * *", "1 day"],
] as const;
for (const [input, cron, description] of mappings) test(`exact table mapping: ${input}`, () => {
  assert.deepEqual(parseInterval(input), { cron, description });
});
for (const [input, equivalent] of [["60s", "1m"], ["120s", "2m"], ["60m", "1h"], ["24h", "1d"], ["86400s", "1d"], ["  005 M  ", "5m"]]) {
  test(`equivalent integer syntax: ${input}`, () => assert.deepEqual(parseInterval(input), parseInterval(equivalent)));
}
const unsafeAmounts = [Number.MAX_SAFE_INTEGER, Number.MAX_SAFE_INTEGER + 1]
  .flatMap(amount => ["s", "m", "h", "d"].map(unit => `${amount}${unit}`));
for (const input of ["0s", "0h", "0d", "00m", ...unsafeAmounts, huge]) {
  test(`numeric safety: ${input.slice(0, 30)}`, () => {
    assert.throws(() => parseInterval(input), error => {
      assert.ok(error instanceof Error);
      assert.match(error.message, /positive safe integer|Unsupported cron interval/);
      assert.ok(error.message.length < 400);
      if (input.length > 120) assert.ok(error.message.includes(input.slice(0, 120) + "…"));
      return true;
    });
  });
}
for (const input of ["1s", "59s", "61s", "86399s", "86401s", "30s", "90s", "3m", "7m", "90m", "23h", "25h", "7d"]) {
  test(`non-table duration rejected: ${input}`, () => assert.throws(() => parseInterval(input), /Unsupported cron interval.*No rounding/));
}
for (const input of ["", " ", "-1m", "+1m", "1.0h", ".5h", "1e3m", "Infinitym", "NaNm", "5", "1w", "1m trailing"]) {
  test(`malformed parser input: ${JSON.stringify(input)}`, () => assert.throws(() => parseInterval(input), /Cannot parse interval/));
}
test("prefix recognition is narrow and preserves full matched text", () => {
  for (const input of ["-1m", "+1m", "1.5h", ".5h", "1e+3m", "Infinitym", "-NaNh", "005 M"]) {
    assert.deepEqual(intervals.matchIntervalPrefix(`  ${input}  check  `), { interval: input, rest: "check" });
    assert.deepEqual(intervals.matchIntervalPrefix(input), { interval: input, rest: "" });
  }
  for (const input of ["audit:1m", "1month", "1w", "review 3m timeout"]) assert.equal(intervals.matchIntervalPrefix(input), undefined);
});

for (const trigger of ["0m", "2d", "-1m", "+1m", "1.5h", "1e3m", "Infinitym", "NaNm", `${Number.MAX_SAFE_INTEGER}d`, huge]) {
  for (const triggerType of [undefined, "cron"]) test(`tool ${triggerType ?? "inferred"} rejects ${trigger.slice(0, 25)} before mutation`, async () => {
    const f = fixture(); const before = f.store.snapshot();
    try {
      await assert.rejects(f.call("LoopCreate", { trigger, triggerType, prompt: "check" }), /positive safe integer|Unsupported cron interval|Cannot parse interval/);
      unchanged(f, before);
    } finally { f.triggers.stop(); }
  });
}
for (const input of ["0m", "2d", "-1m", "+1m", "1.5h", "1e3m", "Infinitym", "NaNm"]) test(`command ${input} rejects without dynamic activation`, async () => {
  const f = fixture(); const before = f.store.snapshot();
  try {
    await f.command(`${input} check`); unchanged(f, before);
    assert.equal(f.notices.length, 1); assert.equal(f.notices[0][1], "error");
    assert.match(f.notices[0][0], /positive safe integer|Unsupported cron interval|Cannot parse interval/);
  } finally { f.triggers.stop(); }
});
for (const input of ["5m", "0m", "-1m"]) test(`missing shorthand prompt: ${input}`, async () => {
  const f = fixture(); const before = f.store.snapshot();
  try {
    await f.command(input); unchanged(f, before);
    assert.match(f.notices[0][0], /Provide a prompt/); assert.equal(f.notices[0][1], "warning");
  } finally { f.triggers.stop(); }
});
test("menu shares validation and cancellation remains a no-op", async () => {
  for (const inputs of [["check", "2d"], [undefined], ["check", undefined]]) {
    const f = fixture(); const before = f.store.snapshot(); const queue = [...inputs];
    try {
      await f.command("", { select: async () => "Create scheduled loop", input: async () => queue.shift() });
      unchanged(f, before);
      if (inputs[1] === "2d") { assert.equal(f.notices[0][1], "error"); assert.match(f.notices[0][0], /Unsupported cron interval/); }
      else assert.equal(f.notices.length, 0);
    } finally { f.triggers.stop(); }
  }
});
test("successful tool, command and menu cron creation retain descriptions and explain timing", async () => {
  const f = fixture();
  try {
    const result = await f.call("LoopCreate", { trigger: "5m", prompt: "check" });
    assert.ok(result.content[0].text.includes(timing)); assert.ok(result.details.expanded.includes(timing));
    assert.match(result.content[0].text, /Recurring: true/);
    assert.match(f.tools.get("LoopCreate").parameters.properties.trigger.description, /supported cron shorthand/);
    await f.command("5m check");
    const queue = ["check", "5m"];
    await f.command("", { select: async () => "Create scheduled loop", input: async () => queue.shift() });
    assert.equal(f.store.list().length, 3);
    for (const entry of f.store.list()) assert.deepEqual(entry.trigger, { type: "cron", schedule: "*/5 * * * *" });
    for (const [message, tone] of f.notices) { assert.ok(message.includes(timing)); assert.match(message, /5 minutes/); assert.equal(tone, "info"); }
  } finally { f.triggers.stop(); }
});
test("hybrid shorthand validates before subscribing and explains timing", async () => {
  const f = fixture(); const before = f.store.snapshot();
  try {
    for (const input of ["2d", "0m"]) {
      await assert.rejects(f.call("LoopCreate", { trigger: `cron: ${input} event: audit:a05`, triggerType: "hybrid", prompt: "check" }), /positive safe integer|Unsupported cron interval/);
      unchanged(f, before);
    }
    const result = await f.call("LoopCreate", { trigger: "cron: 1h event: audit:a05", prompt: "check" });
    assert.ok(result.content[0].text.includes(timing)); assert.ok(result.details.expanded.includes(timing));
    assert.equal(f.bus.listenerCount("audit:a05"), 1); assert.deepEqual(f.adds, ["1"]);
    assert.equal(f.store.list()[0].trigger.type, "hybrid");
  } finally { f.triggers.stop(); }
});
test("explicit event intent, ordinary event inference and dynamic goals are preserved", async () => {
  const f = fixture();
  try {
    for (const [trigger, triggerType] of [["-1m", "event"], ["audit:1m", undefined], ["1month", undefined], ["1w", undefined], ["idle", "idle"]]) {
      const result = await f.call("LoopCreate", { trigger, triggerType, prompt: "check" });
      assert.ok(!result.content[0].text.includes(timing)); assert.ok(!result.details.expanded.includes(timing));
    }
    assert.deepEqual(f.store.list().map(e => e.trigger.type), ["event", "event", "event", "event", "dynamic"]);
    await f.command("event -1m check"); await f.command("review 3m timeout"); await f.command("1month");
    assert.deepEqual(f.store.list().slice(5).map(e => e.trigger.type), ["event", "dynamic", "dynamic"]);
    assert.equal(f.bus.listenerCount("-1m"), 2); assert.equal(f.activations.length, 3);
    assert.ok(f.notices.every(([message]) => !message.includes(timing)));
    await assert.rejects(f.call("LoopCreate", { trigger: "1w", triggerType: "cron", prompt: "check" }), /Cannot parse interval/);
  } finally { f.triggers.stop(); }
});
test("explicit non-table cron remains legal and malformed cron fails unchanged", async () => {
  const f = fixture();
  try {
    for (const cron of ["*/3 * * * *", "0 9 * * 1-5"]) {
      assert.deepEqual(parseInterval(cron), { cron, description: `cron: ${cron}` });
      await f.call("LoopCreate", { trigger: cron, prompt: "check" });
      await f.command(`${cron} check`);
    }
    const before = f.store.snapshot(); const writes = f.writes.length;
    await assert.rejects(f.call("LoopCreate", { trigger: "61 * * * *", prompt: "check" }), /Invalid cron expression/);
    assert.deepEqual(f.store.snapshot(), before); assert.equal(f.writes.length, writes);
  } finally { f.triggers.stop(); }
});
test("5m uses the next wall-clock slot plus existing per-ID jitter", async () => {
  const f = fixture();
  try {
    const base = cronToNextFire(parseInterval("5m").cron, new Date());
    assert.equal(base.getTime(), new Date(2026, 8, 20, 12, 5).getTime());
    await f.call("LoopCreate", { trigger: "5m", prompt: "check" });
    assert.equal(f.scheduler.nextFire("1"), base.getTime() + computeJitter("1", true, 5));
    assert.notEqual(base.getTime(), Date.now() + 300000);
  } finally { f.triggers.stop(); }
});
test("concurrent valid and invalid creates preserve existing subscriptions and allocate only the valid ID", async () => {
  const f = fixture();
  try {
    await f.call("LoopCreate", { trigger: "audit:a05", prompt: "existing" });
    const before = f.store.snapshot(); const writes = f.writes.length, widgets = f.widgets.length, adds = f.adds.length;
    const results = await Promise.allSettled(["5m", "0m", "2d", "-1m", huge].map(trigger => f.call("LoopCreate", { trigger, prompt: "check" })));
    assert.equal(results.filter(r => r.status === "fulfilled").length, 1);
    assert.equal(f.store.snapshot().nextId, before.nextId + 1); assert.equal(f.store.list().length, 2);
    assert.equal(f.writes.length, writes + 1); assert.equal(f.widgets.length, widgets + 1); assert.equal(f.adds.length, adds + 1);
    assert.equal(f.scheduler.nextFire("3"), undefined); assert.equal(f.activations.length, 0);
    assert.equal(f.bus.listenerCount("audit:a05"), 1); f.bus.emit("audit:a05", {});
    assert.equal(f.store.get("1")?.fireCount, 1);
  } finally { f.triggers.stop(); }
});
test("snapshot round trip preserves legacy daily, new shorthand, explicit cron, event and dynamic entries", async () => {
  const f = fixture(); const restored = fixture();
  try {
    // A legacy normalized daily schedule contains no evidence of its original shorthand.
    f.store.create({ type: "cron", schedule: "0 0 * * *" }, "legacy daily", { recurring: true, maxFires: 4, readOnly: true });
    for (const trigger of ["5m", "*/3 * * * *", "audit:a05", "cron: 1h event: audit:hybrid"]) await f.call("LoopCreate", { trigger, prompt: "check", maxFires: 3 });
    await f.call("LoopCreate", { trigger: "idle", triggerType: "idle", prompt: "goal" });
    const snapshot = f.store.snapshot(); const writes = f.writes.length;
    await assert.rejects(f.call("LoopCreate", { trigger: "2d", prompt: "not saved" }), /Unsupported cron interval/);
    assert.deepEqual(f.store.snapshot(), snapshot); assert.equal(f.writes.length, writes);
    restored.store.restoreSnapshot(snapshot); restored.triggers.start(); restored.triggers.start();
    assert.deepEqual(restored.store.snapshot(), snapshot); assert.equal(restored.writes.length, 0);
    assert.equal(restored.bus.listenerCount("audit:a05"), 1); assert.equal(restored.bus.listenerCount("audit:hybrid"), 1);
    restored.bus.emit("audit:a05", {}); assert.equal(restored.store.get("4")?.fireCount, 1);
  } finally { f.triggers.stop(); restored.triggers.stop(); }
});
for (const [nextInterval, delay] of [["30s", 30000], ["3m", 180000], ["2d", 172800000]] as const) test(`dynamic elapsed nextInterval remains exact: ${nextInterval}`, async () => {
  const f = fixture();
  try {
    const entry = f.store.create({ type: "dynamic" }, "goal", { recurring: true, maxFires: 3, dynamic: { goal: "goal", awaitingUpdate: true, iteration: 0 } });
    await f.call("LoopUpdate", { id: entry.id, status: "continue", nextInterval });
    assert.equal(f.store.get(entry.id)?.dynamic?.nextWakeAt, Date.now() + delay);
    assert.equal(f.scheduler.nextFire(entry.id), Date.now() + delay);
  } finally { f.triggers.stop(); }
});
test("pause, resume, delete and one-shot read-only fire cap retain their contracts", async () => {
  const f = fixture();
  try {
    await f.call("LoopCreate", { trigger: "5m", prompt: "check", recurring: false, readOnly: true, maxFires: 1 });
    const entry = f.store.get("1")!;
    assert.equal(entry.recurring, false); assert.equal(entry.readOnly, true); assert.equal(entry.maxFires, 1);
    await f.call("LoopDelete", { id: "1", action: "pause" });
    assert.equal(f.store.get("1")?.status, "paused"); assert.equal(f.scheduler.nextFire("1"), undefined);
    const resumed = f.store.resume("1")!; f.triggers.add(resumed);
    assert.deepEqual(resumed.trigger, entry.trigger);
    f.scheduler.pump(f.scheduler.nextFire("1")!);
    assert.equal(f.store.get("1"), undefined); assert.equal(f.scheduler.nextFire("1"), undefined);
    await f.call("LoopCreate", { trigger: "5m", prompt: "check" });
    await f.call("LoopDelete", { id: "2" });
    assert.equal(f.store.get("2"), undefined); assert.equal(f.scheduler.nextFire("2"), undefined);
  } finally { f.triggers.stop(); }
});
