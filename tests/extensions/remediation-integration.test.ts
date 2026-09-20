import assert from "node:assert/strict";
import test from "node:test";
import { LoopStore } from "../../loop/store.js";
import { CronScheduler } from "../../loop/scheduler.js";
import { registerLoopTools } from "../../loop/tools/loop-tools.js";

function fixture() {
  const store = new LoopStore();
  const tools = new Map<string, any>();
  const scheduler = new CronScheduler(store, entry => { store.beginDynamicWake(entry.id); });
  registerLoopTools({ pi: { registerTool: (tool: any) => tools.set(tool.name, tool), appendEntry() {} } as any,
    getStore: () => store, getScheduler: () => scheduler, getTriggerSystem: () => scheduler,
    getMonitorManager: () => ({ get: () => undefined }), updateWidget() {} });
  const create = (recurring = true, maxFires = 3) => store.create({ type: "dynamic" }, "integration", {
    recurring, maxFires, dynamic: { state: "old", metrics: "old metrics" },
  });
  const update = (id: string, wakeId: string, status: string) => tools.get("LoopUpdate").execute("integration", {
    id, wakeId, status, state: "saved checkpoint", metrics: "saved metrics", doneCriteria: "verified",
  });
  return { store, scheduler, create, update };
}

test("combined A08/A11: expiry retires an admitted waiting wake despite a rejecting work filter", async t => {
  let now = Date.UTC(2026, 8, 20, 12);
  t.mock.method(Date, "now", () => now);
  const f = fixture(); t.after(() => f.scheduler.stop());
  const entry = f.create(); f.scheduler.add(entry); f.scheduler.pump(now);
  const wakeId = f.store.get(entry.id)!.dynamic!.pendingWakeId!;
  assert.ok(wakeId); assert.equal(f.store.get(entry.id)?.fireCount, 1);
  now = entry.expiresAt;
  await assert.rejects(async () => f.update(entry.id, wakeId, "paused"), /expired/);
  f.scheduler.pump(now, () => false);
  assert.equal(f.store.get(entry.id), undefined);
  assert.equal(f.scheduler.nextFire(entry.id), undefined);
  await assert.rejects(async () => f.update(entry.id, wakeId, "completed"), /not found/);
});

for (const cause of ["cap", "one-shot"] as const) {
  for (const status of ["paused", "completed"] as const) {
    test(`combined A08/A09/A11: ${cause} final wake permits ${status} before expiry only`, async t => {
      let now = Date.UTC(2026, 8, 20, 12);
      t.mock.method(Date, "now", () => now);
      const f = fixture(); t.after(() => f.scheduler.stop());
      const entry = f.create(cause !== "one-shot", cause === "cap" ? 1 : 3);
      f.scheduler.add(entry); f.scheduler.pump(now);
      const pending = f.store.get(entry.id)!;
      const wakeId = pending.dynamic!.pendingWakeId!;
      assert.equal(pending.status, "paused"); assert.ok(wakeId);
      assert.equal(f.scheduler.nextFire(entry.id), undefined);
      await assert.rejects(async () => f.update(entry.id, wakeId, "continue"), /cap|paused/);
      now = entry.expiresAt - 1;
      await f.update(entry.id, wakeId, status);
      if (status === "paused") {
        const restored = new LoopStore(); restored.restoreSnapshot(f.store.snapshot());
        const saved = restored.get(entry.id)!;
        assert.equal(saved.dynamic!.state, "saved checkpoint");
        assert.equal(saved.dynamic!.metrics, "saved metrics");
        assert.equal(saved.dynamic!.doneCriteria, "verified");
        assert.equal(saved.dynamic!.pendingWakeId, undefined);
        assert.equal(saved.dynamic!.awaitingUpdate, false);
        assert.equal(saved.fireCount, 1); assert.equal(saved.dynamic!.iteration, 0);
        assert.equal(saved.expiresAt, entry.expiresAt);
        assert.equal(restored.expireEntries(entry.expiresAt)[0].disposition, "deleted");
      } else assert.equal(f.store.get(entry.id), undefined);
      await assert.rejects(async () => f.update(entry.id, wakeId, status));

      now = Date.UTC(2026, 8, 20, 12);
      const overdue = f.create(cause !== "one-shot", cause === "cap" ? 1 : 3);
      f.scheduler.add(overdue); f.scheduler.pump(now);
      const lateToken = f.store.get(overdue.id)!.dynamic!.pendingWakeId!;
      now = overdue.expiresAt;
      await assert.rejects(async () => f.update(overdue.id, lateToken, status), /expired/);
      f.store.expireEntries(now);
      assert.equal(f.store.get(overdue.id), undefined);
    });
  }
}
