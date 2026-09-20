import assert from "node:assert/strict";
import test from "node:test";
import { parseInterval, cronToNextFire } from "../../loop/loop-parse.js";
import { LoopStore } from "../../loop/store.js";
import { CronScheduler } from "../../loop/scheduler.js";
import { TriggerSystem } from "../../loop/trigger-system.js";
import { registerLoopTools } from "../../loop/tools/loop-tools.js";
import { registerLoopCommand } from "../../loop/commands/loop-command.js";
import { HerdrMonitorManager } from "../../loop/runtime/herdr-monitor.js";

// Passing characterizations of audit findings, NOT assertions that these are desirable.
// Invert the relevant expectations when fixing the documented gaps.
function fixture() {
  const tools = new Map<string, any>();
  const commands = new Map<string, any>();
  const subscriptions = new Map<string, Set<(data: unknown) => void>>();
  const pi: any = {
    registerTool: (tool: any) => tools.set(tool.name, tool),
    registerCommand: (name: string, command: any) => commands.set(name, command),
    appendEntry() {},
    events: { on(name: string, callback: (data: unknown) => void) {
      const callbacks = subscriptions.get(name) ?? new Set();
      callbacks.add(callback); subscriptions.set(name, callbacks);
      return () => callbacks.delete(callback);
    } },
  };
  const store = new LoopStore();
  const scheduler = new CronScheduler(store, () => {});
  const triggers = new TriggerSystem(pi, scheduler, store, () => {});
  const options = { pi, getStore: () => store, getTriggerSystem: () => triggers, updateWidget() {} };
  registerLoopTools({ ...options, getScheduler: () => scheduler, getMonitorManager: () => ({ get: () => undefined }) });
  registerLoopCommand(options);
  return { store, scheduler, triggers, subscriptions, commands, call: (name: string, input: any) => tools.get(name).execute("audit", input) };
}

test("Known audit gap: zero duration silently creates a minute cadence", () => {
  assert.equal(parseInterval("0m").cron, "*/1 * * * *");
});
test("Known audit gap: two days silently becomes daily", () => {
  assert.equal(parseInterval("2d").cron, "0 0 * * *");
});
test("Known audit gap: day-of-month and weekday use AND, not conventional cron OR", () => {
  const next = cronToNextFire("0 0 1 * 1", new Date(2026, 8, 20, 12));
  assert.equal(next.getFullYear(), 2027);
  assert.equal(next.getMonth(), 1);
  assert.equal(next.getDate(), 1);
});
test("Known audit gap: valid leap-day schedule beyond 366 days is rejected", () => {
  assert.throws(() => cronToNextFire("0 0 29 2 *", new Date(2026, 8, 20)), /No matching time/);
});
test("Known audit gap: full cron in a hybrid spec is truncated to one field", async () => {
  const f = fixture();
  await assert.rejects(f.call("LoopCreate", { trigger: "cron: */5 * * * * event: audit", triggerType: "hybrid", prompt: "audit", maxFires: 1 }), /Cannot parse interval/);
  assert.equal(f.store.list().length, 0);
});
test("Failed scheduling leaves no saved loop and healthy restoration subscribes", async () => {
  const f = fixture();
  const before = f.store.snapshot();
  await assert.rejects(f.call("LoopCreate", { trigger: "0 0 31 2 *", triggerType: "cron", prompt: "audit", maxFires: 1 }), /No matching time/);
  assert.deepEqual(f.store.snapshot(), before);
  const event = f.store.create({ type: "event", source: "audit" }, "audit", { recurring: true, maxFires: 1 });
  try {
    assert.doesNotThrow(() => f.triggers.start());
    assert.equal(f.subscriptions.get("audit")?.size, 1);
    assert.equal(f.store.get(event.id)?.fireCount, 0);
  } finally { f.triggers.stop(); }
});
test("Command creation rolls back the same impossible cron", async () => {
  const f = fixture(); const notices: string[] = [];
  const before = f.store.snapshot();
  await f.commands.get("loop").handler("0 0 31 2 * audit", { hasUI: true, ui: { notify: (message: string) => notices.push(message) } });
  assert.deepEqual(f.store.snapshot(), before);
  assert.match(notices[0], /No matching time/);
});
test("Known audit gap: cron without a prompt is interpreted as a dynamic goal", async () => {
  const f = fixture();
  await f.commands.get("loop").handler("0 9 * * 1-5", { hasUI: true, ui: { notify() {} } });
  assert.equal(f.store.list()[0].trigger.type, "dynamic");
});
test("Known audit gap: a second continue update succeeds without another wake", async () => {
  const f = fixture();
  const loop = f.store.create({ type: "dynamic" }, "audit", { recurring: true, maxFires: 3, dynamic: { goal: "audit", iteration: 0, awaitingUpdate: true } });
  await f.call("LoopUpdate", { id: loop.id, status: "continue", nextInterval: "1h", state: "first" });
  await f.call("LoopUpdate", { id: loop.id, status: "continue", nextInterval: "1h", state: "second" });
  assert.equal(f.store.get(loop.id)?.dynamic?.iteration, 2);
  assert.equal(f.store.get(loop.id)?.fireCount, 0);
});
test("Known audit gap: pause checkpoints are not applied to dynamic state", async () => {
  const f = fixture();
  const loop = f.store.create({ type: "dynamic" }, "audit", { recurring: true, maxFires: 3, dynamic: { goal: "audit", state: "old", iteration: 0, awaitingUpdate: true } });
  await f.call("LoopUpdate", { id: loop.id, status: "paused", state: "new", metrics: "new-metrics", doneCriteria: "new-done" });
  assert.equal(f.store.get(loop.id)?.status, "paused");
  assert.equal(f.store.get(loop.id)?.dynamic?.state, "old");
  assert.equal(f.store.get(loop.id)?.dynamic?.metrics, undefined);
});
test("Dynamic loops awaiting an update expire without another wake", () => {
  const f = fixture();
  const loop = f.store.create({ type: "dynamic" }, "audit", { recurring: true, maxFires: 3, dynamic: { goal: "audit", iteration: 0, awaitingUpdate: true } });
  f.scheduler.add(loop);
  f.scheduler.pump(loop.expiresAt + 1);
  assert.equal(f.store.get(loop.id), undefined);
});

test("Loop store enforces the 25-controller cap", () => {
  const f = fixture();
  for (let index = 0; index < 25; index++) f.store.create({ type: "event", source: "audit" }, "audit", { recurring: true, maxFires: 1 });
  assert.throws(() => f.store.create({ type: "event", source: "audit" }, "overflow", { recurring: true, maxFires: 1 }), /25/);
});

test("Continue preserves omitted checkpoint fields and rejects renewal beyond the fire cap", async () => {
  const f = fixture();
  const loop = f.store.create({ type: "dynamic" }, "audit", { recurring: true, maxFires: 1, dynamic: { goal: "audit", state: "keep", metrics: "keep", iteration: 0 } });
  await f.call("LoopUpdate", { id: loop.id, status: "continue", nextInterval: "1h" });
  assert.equal(f.store.get(loop.id)?.dynamic?.state, "keep");
  f.store.fire(loop.id, "dynamic");
  await assert.rejects(async () => f.call("LoopUpdate", { id: loop.id, status: "continue" }), /fire cap/);
});

test("Known audit gap: monitor readTail assumes JSON for Herdr's text output", async () => {
  const old = { env: process.env.HERDR_ENV, workspace: process.env.HERDR_WORKSPACE_ID };
  process.env.HERDR_ENV = "1"; process.env.HERDR_WORKSPACE_ID = "audit";
  try {
    const manager = new HerdrMonitorManager(async () => ({ stdout: "AUDIT_SERVER_READY\n", stderr: "", code: 0, killed: false }));
    await assert.rejects(manager.readTail("audit-pane"), SyntaxError);
    manager.dispose();
  } finally {
    if (old.env === undefined) delete process.env.HERDR_ENV; else process.env.HERDR_ENV = old.env;
    if (old.workspace === undefined) delete process.env.HERDR_WORKSPACE_ID; else process.env.HERDR_WORKSPACE_ID = old.workspace;
  }
});

test("Known audit gap: busy shell startup in a new monitor pane reports success without running its command", async () => {
  const old = { env: process.env.HERDR_ENV, workspace: process.env.HERDR_WORKSPACE_ID };
  process.env.HERDR_ENV = "1"; process.env.HERDR_WORKSPACE_ID = "audit";
  const calls: string[][] = [];
  try {
    const manager = new HerdrMonitorManager(async (_command, args) => {
      calls.push(args);
      let result: any = {};
      if (args[0] === "tab") result = { tabs: [{ label: "Monitor", tab_id: "tab" }] };
      if (args[0] === "pane" && args[1] === "list") result = { panes: [{ pane_id: "root", tab_id: "tab", label: "existing" }] };
      if (args[1] === "split") result = { pane: { pane_id: "new" } };
      if (args[1] === "process-info") result = { process_info: { shell_pid: 1, foreground_processes: [{ pid: 2, name: "shell-startup-helper" }] } };
      return { stdout: JSON.stringify({ result }), stderr: "", code: 0, killed: false };
    });
    const monitor = await manager.create("printf never-ran", undefined, "/tmp");
    assert.equal(monitor.status, "running");
    assert.equal(monitor.reused, false);
    assert.equal(calls.some(args => args[1] === "run"), false);
    manager.dispose();
  } finally {
    if (old.env === undefined) delete process.env.HERDR_ENV; else process.env.HERDR_ENV = old.env;
    if (old.workspace === undefined) delete process.env.HERDR_WORKSPACE_ID; else process.env.HERDR_WORKSPACE_ID = old.workspace;
  }
});
