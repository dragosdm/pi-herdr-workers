import assert from "node:assert/strict";
import test from "node:test";
import type { ExtensionAPI, ExtensionCommandContext, ExtensionUIContext } from "@earendil-works/pi-coding-agent";
import { registerLoopCommand } from "../../loop/commands/loop-command.js";
import { LoopStore } from "../../loop/store.js";
import type { LoopEntry, LoopStoreData } from "../../loop/types.js";

function fixture(options: { selection?: string; notifyThrows?: boolean } = {}) {
  const store = new LoopStore();
  const writes: LoopStoreData[] = [];
  const adds: LoopEntry[] = [], removes: string[] = [], activations: LoopEntry[] = [];
  const notices: Array<{ message: string; severity: string }> = [];
  const calls = { create: 0, widgets: 0, input: 0, select: 0, getStore: 0, getTriggers: 0 };
  store.onChange = snapshot => { writes.push(snapshot); };
  const create = store.create.bind(store);
  store.create = (...args) => { calls.create++; return create(...args); };
  let handler!: (args: string, ctx: ExtensionCommandContext) => Promise<void>;
  const pi = { registerCommand(name: string, command: { handler: typeof handler }) {
    assert.equal(name, "loop");
    handler = command.handler;
  } } as ExtensionAPI;
  registerLoopCommand({
    pi,
    getStore: () => { calls.getStore++; return store; },
    getTriggerSystem: () => {
      calls.getTriggers++;
      return { add: entry => { adds.push(entry); }, remove: id => { removes.push(id); } };
    },
    updateWidget: () => { calls.widgets++; },
    onDynamicLoopActivated: entry => { activations.push(entry); },
  });
  const ui = {
    notify(message: string, severity: string) {
      notices.push({ message, severity });
      if (options.notifyThrows) throw new Error("notification failed");
    },
    async input() { calls.input++; return undefined; },
    async select() { calls.select++; return options.selection; },
  } as unknown as ExtensionUIContext;
  const effects = () => ({ ...calls, writes: writes.length, adds: adds.length, removes: removes.length, activations: activations.length });
  return { store, notices, calls, writes, adds, removes, activations, effects,
    command: (args: string, hasUI = true) => handler(args, { hasUI, ui } as ExtensionCommandContext),
  };
}

type Fixture = ReturnType<typeof fixture>;
const checkpoint = (f: Fixture) => ({ snapshot: f.store.snapshot(), effects: f.effects() });
function unchanged(f: Fixture, before: ReturnType<typeof checkpoint>) {
  assert.deepEqual(f.store.snapshot(), before.snapshot);
  assert.deepEqual(f.effects(), before.effects);
}
function notice(f: Fixture, severity: string, message: RegExp) {
  assert.equal(f.notices.length, 1);
  assert.equal(f.notices[0].severity, severity);
  assert.match(f.notices[0].message, message);
}
const missingPrompt = /Provide a prompt after the interval or cron expression.*\/loop 5m.*\/loop 0 9/;
const eventUsage = /Provide an event source and prompt.*\/loop event tool_execution_end/;

for (const [input, message] of [["0 9 * * 1-5", missingPrompt], ["event audit:test", eventUsage]] as const) {
  test(`primary regression: reject ${input}`, async () => {
    const f = fixture(), before = checkpoint(f);
    await f.command(input);
    unchanged(f, before);
    notice(f, "warning", message);
  });
}

const missingSchedules = [
  " 0 9 * * 1-5 ", "0  9   * * 1-5", "0\t9\t*\t*\t1-5\t", "0 9 * * 1-5\n",
  "* * * * *", "*/5 9-17 * * 1,3,5", "0 0 31 2 *", "5m", "5m   ",
];
for (const input of missingSchedules) test(`missing prompt: ${JSON.stringify(input)}`, async () => {
  const f = fixture(), before = checkpoint(f);
  await f.command(input);
  unchanged(f, before);
  notice(f, "warning", missingPrompt);
});

const invalidCrons = ["60 9 * * 1-5", "0 24 * * *", "*/0 * * * *", "0 9 * * 5-1"];
for (const cron of invalidCrons) for (const suffix of ["", " check the deploy"]) {
  test(`invalid cron precedes missing-prompt handling: ${cron}${suffix}`, async () => {
    const f = fixture(), before = checkpoint(f);
    await f.command(`  ${cron.replaceAll(" ", "\t ")}${suffix}  `);
    unchanged(f, before);
    assert.deepEqual(f.notices, [{ message: `Invalid cron expression: ${cron}`, severity: "error" }]);
  });
}
const incompleteEvents = ["event", "when", "WHEN audit:test", " event \t", "\twhen ready\n", "event audit:test   ", "event audit:test review\nthat tool"];
for (const input of incompleteEvents) test(`incomplete reserved event: ${JSON.stringify(input)}`, async () => {
  const f = fixture(), before = checkpoint(f);
  await f.command(input);
  unchanged(f, before);
  notice(f, "warning", eventUsage);
});

const scheduled = [
  ["0 9 * * 1-5 check the deploy", "0 9 * * 1-5", "check the deploy"],
  [" \t0  9 *\t* 1-5  check\t the  deploy\n", "0 9 * * 1-5", "check the deploy"],
  ["0 9 * * 1-5 2026", "0 9 * * 1-5", "2026"],
  ...["5m", "5 m", "5M"].map(interval => [`${interval} check the deploy`, "*/5 * * * *", "check the deploy"]),
];
for (const [input, schedule, prompt] of scheduled) test(`valid scheduled command: ${JSON.stringify(input)}`, async () => {
  const f = fixture();
  await f.command(input);
  const entries = f.store.list();
  assert.equal(entries.length, 1);
  assert.deepEqual(entries[0].trigger, { type: "cron", schedule });
  assert.equal(entries[0].prompt, prompt);
  assert.equal(entries[0].recurring, true);
  assert.equal(entries[0].maxFires, 25);
  assert.deepEqual(f.adds, entries);
  assert.equal(f.activations.length, 0);
  assert.equal(f.writes.length, 1);
  assert.equal(f.calls.widgets, 1);
  assert.equal(f.calls.create, 1);
  assert.equal(f.calls.input + f.calls.select + f.removes.length, 0);
  notice(f, "info", /Loop #1 created:/);
  assert.match(f.notices[0].message, input.trim().startsWith("5") ? /every 5 minutes/ : /cron: 0 9/);
});
for (const prefix of ["event", "WHEN"]) test(`complete ${prefix} preserves event source and prompt`, async () => {
  const f = fixture();
  await f.command(` ${prefix} Audit:Test review  that tool `);
  const entries = f.store.list();
  assert.equal(entries.length, 1);
  assert.deepEqual(entries[0].trigger, { type: "event", source: "Audit:Test" });
  assert.equal(entries[0].prompt, "review  that tool");
  assert.equal(entries[0].recurring, true);
  assert.equal(entries[0].maxFires, 25);
  assert.deepEqual(f.adds, entries);
  assert.equal(f.activations.length, 0);
  assert.equal(f.writes.length, 1);
  assert.equal(f.calls.widgets, 1);
  assert.equal(f.calls.input + f.calls.select + f.removes.length, 0);
  notice(f, "info", /Event loop #1 created:.*Audit:Test/);
});
for (const goal of ["finish the release", "check the deploy every morning", "eventually finish the release", "whenever ready", "review event audit:test", "0 9 * *", "0 9 * * MON", "@daily"]) {
  test(`dynamic compatibility: ${goal}`, async () => {
    const f = fixture();
    await f.command(`  ${goal}\n`);
    const entries = f.store.list();
    assert.equal(entries.length, 1);
    assert.deepEqual(entries[0].trigger, { type: "dynamic" });
    assert.equal(entries[0].dynamic?.goal, goal);
    assert.equal(entries[0].prompt, goal);
    assert.equal(entries[0].maxFires, 20);
    assert.equal(entries[0].recurring, true);
    assert.deepEqual(f.adds, entries);
    assert.deepEqual(f.activations, entries);
    assert.equal(f.writes.length, 1);
    assert.equal(f.calls.widgets, 1);
    assert.equal(f.calls.input + f.calls.select + f.removes.length, 0);
    notice(f, "info", /Dynamic loop #1 created/);
  });
}
for (const input of ["", " \t\n"]) test(`cancelled menu: ${JSON.stringify(input)}`, async () => {
  const f = fixture(), before = checkpoint(f);
  await f.command(input);
  before.effects.select++;
  unchanged(f, before);
  assert.equal(f.notices.length, 0);
});
for (const selection of ["Create scheduled loop", "Create event-triggered loop"]) test(`cancel first input: ${selection}`, async () => {
  const f = fixture({ selection }), before = checkpoint(f);
  await f.command("");
  before.effects.select++;
  before.effects.input++;
  unchanged(f, before);
  assert.equal(f.notices.length, 0);
});

const rejected = ["0 9 * * 1-5", ...missingSchedules, ...invalidCrons, "event audit:test", ...incompleteEvents];
for (const input of rejected) test(`headless guard: ${JSON.stringify(input)}`, async () => {
  const f = fixture(), before = checkpoint(f);
  await assert.rejects(f.command(input, false), { message: "/loop requires a UI; use LoopCreate in headless mode." });
  unchanged(f, before);
  assert.equal(f.notices.length, 0);
});
for (const input of rejected) test(`existing state and restoration after rejection: ${JSON.stringify(input)}`, async () => {
  const f = fixture();
  f.store.create({ type: "event", source: "audit:active" }, "active", { recurring: true });
  const paused = f.store.create({ type: "dynamic" }, "paused", { recurring: true, dynamic: { goal: "paused", iteration: 2 } });
  f.store.pause(paused.id);
  const before = checkpoint(f);
  await f.command(input);
  unchanged(f, before);
  assert.equal(f.notices.length, 1);
  const restored = new LoopStore();
  restored.restoreSnapshot(f.store.snapshot());
  assert.deepEqual(restored.snapshot(), before.snapshot);
});
for (const input of ["0 9 * * 1-5", "event audit:test"]) test(`repeated concurrent rejection: ${input}`, async () => {
  const f = fixture();
  await Promise.all([f.command(input), f.command(input), f.command("finish the release")]);
  assert.equal(f.notices.filter(n => n.severity === "warning").length, 2);
  assert.equal(f.notices.filter(n => n.severity === "info").length, 1);
  assert.equal(f.store.snapshot().nextId, 2);
  assert.equal(f.store.list().length, 1);
  assert.equal(f.store.list()[0].prompt, "finish the release");
  assert.equal(f.calls.create, 1);
  assert.equal(f.writes.length, 1);
  assert.equal(f.calls.widgets, 1);
  assert.deepEqual(f.adds, f.store.list());
  assert.deepEqual(f.activations, f.store.list());
  assert.equal(f.removes.length + f.calls.input + f.calls.select, 0);
});
test("saved cron-looking dynamic goal restores without migration", () => {
  const f = fixture();
  f.store.create({ type: "dynamic" }, "0 9 * * 1-5", { recurring: true, maxFires: 20, dynamic: { goal: "0 9 * * 1-5", iteration: 0 } });
  const restored = new LoopStore();
  restored.restoreSnapshot(f.store.snapshot());
  assert.deepEqual(restored.snapshot(), f.store.snapshot());
  assert.equal(restored.list()[0].dynamic?.goal, "0 9 * * 1-5");
});
for (const input of ["0 9 * * 1-5", "60 9 * * 1-5", "event audit:test", "5m"]) test(`notification failure cannot activate: ${input}`, async () => {
  const f = fixture({ notifyThrows: true }), before = checkpoint(f);
  await assert.rejects(f.command(input), /notification failed/);
  unchanged(f, before);
  assert.equal(f.notices.length, 1);
});
