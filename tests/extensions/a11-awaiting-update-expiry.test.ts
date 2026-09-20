import assert from "node:assert/strict";
import test, { type TestContext } from "node:test";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import type { ExtensionAPI } from "@earendil-works/pi-coding-agent";
import { registerLoopTools } from "../../loop/tools/loop-tools.js";
import { CronScheduler } from "../../loop/scheduler.js";
import { LoopStore } from "../../loop/store.js";
import type { LoopEntry, LoopExpiryDisposition } from "../../loop/types.js";

const NOW = Date.UTC(2026, 8, 20, 12);
const DEADLINE = NOW + 7 * 86400000;

function fixture(t: TestContext, store = new LoopStore()) {
  let now = NOW;
  t.mock.method(Date, "now", () => now);
  const fires: string[] = [];
  const expirations: { id: string; disposition: LoopExpiryDisposition }[] = [];
  let allowed = true;
  const scheduler = new CronScheduler(store, (entry) => fires.push(entry.id), (entry, disposition) => {
    expirations.push({ id: entry.id, disposition });
  }, () => allowed);
  t.after(() => scheduler.stop());
  function waiting(dynamic: Partial<NonNullable<LoopEntry["dynamic"]>> = {}, opts: { taskBacklog?: boolean; maxFires?: number; recurring?: boolean } = {}) {
    return store.create({ type: "dynamic" }, "A11 regression", {
      recurring: true, maxFires: 10, ...opts,
      dynamic: { goal: "finish regression", state: "checkpoint", metrics: "one iteration", doneCriteria: "verified", iteration: 1, awaitingUpdate: true, nextWakeAt: NOW, ...dynamic },
    });
  }
  return { store, scheduler, fires, expirations, waiting, clock: (at: number) => { now = at; }, permission: (value: boolean) => { allowed = value; } };
}

function assertDeleted(f: ReturnType<typeof fixture>, entry: LoopEntry) {
  assert.equal(f.store.get(entry.id), undefined);
  assert.equal(f.scheduler.nextFire(entry.id), undefined);
  assert.deepEqual(f.fires, []);
  assert.deepEqual(f.expirations, [{ id: entry.id, disposition: "deleted" }]);
}

test("A11 primary: awaiting ordinary dynamic loop retires at its deadline without another wake", (t) => {
  const f = fixture(t);
  const entry = f.waiting();
  f.scheduler.add(entry);
  assert.equal(f.scheduler.nextFire(entry.id), NOW);
  f.scheduler.pump(entry.expiresAt);
  assertDeleted(f, entry);
});

for (const offset of [-1, 0, 1]) {
  test(`A11 inclusive boundary: deadline ${offset >= 0 ? "+" : ""}${offset}ms`, (t) => {
    const f = fixture(t);
    const entry = f.waiting();
    f.store.fire(entry.id, "dynamic");
    f.scheduler.add(f.store.get(entry.id)!);
    const before = f.store.snapshot();
    f.scheduler.pump(entry.expiresAt + offset);
    if (offset < 0) {
      assert.deepEqual(f.store.snapshot(), before);
      assert.equal(f.scheduler.nextFire(entry.id), NOW);
      assert.deepEqual(f.fires, []);
      assert.deepEqual(f.expirations, []);
    } else assertDeleted(f, entry);
  });
}

test("A11 actual dispatch marks awaiting, preserves the checkpoint, and expires without repeating work", (t) => {
  const f = fixture(t);
  const entry = f.waiting({ awaitingUpdate: false });
  const scheduler = new CronScheduler(f.store, (current, origin) => {
    f.fires.push(current.id);
    f.store.fire(current.id, origin);
    f.store.updateDynamic(current.id, { dynamic: { awaitingUpdate: true, nextWakeAt: undefined, lastUpdatedAt: Date.now() } });
  }, (current, disposition) => f.expirations.push({ id: current.id, disposition }));
  t.after(() => scheduler.stop());
  scheduler.add(entry);
  scheduler.pump(NOW);
  assert.equal(f.store.get(entry.id)?.fireCount, 1);
  assert.equal(f.store.get(entry.id)?.dynamic?.awaitingUpdate, true);
  assert.equal(f.store.get(entry.id)?.dynamic?.nextWakeAt, undefined);
  const before = f.store.snapshot();
  scheduler.pump(DEADLINE - 1);
  assert.deepEqual(f.store.snapshot(), before);
  scheduler.pump(DEADLINE);
  assert.equal(f.store.get(entry.id), undefined);
  assert.equal(scheduler.nextFire(entry.id), undefined);
  assert.deepEqual(f.fires, [entry.id]);
  assert.deepEqual(f.expirations, [{ id: entry.id, disposition: "deleted" }]);
});

for (const nextWakeAt of [undefined, NOW + 1000]) {
  test(`A11 waiting fire-map wake ${nextWakeAt === undefined ? "omitted" : "explicit"} expires`, (t) => {
    const f = fixture(t);
    const entry = f.waiting({ nextWakeAt });
    f.scheduler.add(entry);
    assert.equal(f.scheduler.nextFire(entry.id), nextWakeAt ?? NOW);
    f.scheduler.pump(DEADLINE);
    assertDeleted(f, entry);
  });
}

for (const nextWakeAt of [DEADLINE, DEADLINE + 60000]) {
  test(`A11 expiry-only map for wake ${nextWakeAt === DEADLINE ? "at" : "beyond"} deadline`, (t) => {
    const f = fixture(t);
    const entry = f.waiting({ nextWakeAt });
    f.scheduler.add(entry);
    assert.equal(f.scheduler.nextFire(entry.id), undefined);
    const before = f.store.snapshot();
    f.scheduler.pump(DEADLINE - 1);
    assert.deepEqual(f.store.snapshot(), before);
    assert.deepEqual(f.expirations, []);
    f.scheduler.pump(DEADLINE);
    assertDeleted(f, entry);
  });
}

for (const awaitingUpdate of [true, false]) {
  test(`A11 overdue expiry bypasses rejecting work filter, awaiting=${awaitingUpdate}`, (t) => {
    const f = fixture(t);
    const entry = f.waiting({ awaitingUpdate });
    f.scheduler.add(entry);
    let filtered = 0;
    f.scheduler.pump(DEADLINE + 1, () => { filtered++; return false; });
    assert.equal(filtered, 0);
    assertDeleted(f, entry);
  });
}

test("A11 rejecting work filter still blocks an unexpired eligible wake", (t) => {
  const f = fixture(t);
  const entry = f.waiting({ awaitingUpdate: false });
  f.scheduler.add(entry);
  const before = f.store.snapshot();
  let filtered = 0;
  f.scheduler.pump(NOW, () => { filtered++; return false; });
  assert.equal(filtered, 1);
  assert.deepEqual(f.store.snapshot(), before);
  assert.equal(f.scheduler.nextFire(entry.id), NOW);
  assert.deepEqual(f.fires, []);
  assert.deepEqual(f.expirations, []);
});

for (const nextWakeAt of [NOW, DEADLINE]) {
  for (const awaitingUpdate of [true, false]) {
    test(`A11 deferred permission retries without overdue work: wake=${nextWakeAt}, awaiting=${awaitingUpdate}`, (t) => {
      const f = fixture(t);
      const entry = f.waiting({ nextWakeAt, awaitingUpdate });
      f.scheduler.add(entry);
      const before = f.store.snapshot();
      const registration = f.scheduler.nextFire(entry.id);
      f.permission(false);
      f.scheduler.pump(DEADLINE);
      assert.deepEqual(f.store.snapshot(), before);
      assert.equal(f.scheduler.nextFire(entry.id), registration);
      assert.deepEqual(f.fires, []);
      assert.deepEqual(f.expirations, []);
      f.permission(true);
      f.scheduler.pump(DEADLINE + 1);
      assertDeleted(f, entry);
    });
  }
}

test("A11 repeated retirement and scheduler restart persist and notify only once", (t) => {
  const f = fixture(t);
  const entry = f.waiting();
  const writes: ReturnType<LoopStore["snapshot"]>[] = [];
  f.store.onChange = snapshot => writes.push(snapshot);
  f.scheduler.add(entry);
  f.scheduler.pump(DEADLINE);
  f.scheduler.pump(DEADLINE + 1);
  f.scheduler.stop();
  f.scheduler.start();
  f.scheduler.pump(DEADLINE + 2);
  assertDeleted(f, entry);
  assert.equal(writes.length, 1);
  assert.equal(writes[0].loops.length, 0);
  const restored = new LoopStore();
  restored.restoreSnapshot(writes[0]);
  assert.equal(restored.get(entry.id), undefined);
});

for (const action of ["delete", "pause", "stop"] as const) {
  test(`A11 explicit ${action} before pump is not reported as expiry`, (t) => {
    const f = fixture(t);
    const entry = f.waiting();
    f.scheduler.add(entry);
    if (action === "stop") f.scheduler.stop();
    else f.store[action](entry.id);
    const before = f.store.snapshot();
    f.scheduler.pump(DEADLINE);
    assert.deepEqual(f.store.snapshot(), before);
    assert.equal(f.scheduler.nextFire(entry.id), undefined);
    assert.deepEqual(f.fires, []);
    assert.deepEqual(f.expirations, []);
    if (action === "stop") {
      const restored = new LoopStore();
      restored.restoreSnapshot(before);
      assert.deepEqual(restored.expireEntries(DEADLINE).map(r => r.disposition), ["deleted"]);
    }
  });
}

test("A11 unexpired snapshot restoration retains waiting checkpoint until original deadline", (t) => {
  const f = fixture(t);
  const entry = f.waiting({ nextWakeAt: undefined });
  f.store.fire(entry.id, "dynamic");
  const saved = f.store.snapshot();
  // Exercise a saved finite deadline rather than mutating a live entry object.
  saved.loops[0].expiresAt = NOW + 120000;
  const restored = new LoopStore();
  restored.restoreSnapshot(saved);
  const fires: string[] = [];
  const expired: string[] = [];
  const scheduler = new CronScheduler(restored, e => fires.push(e.id), e => expired.push(e.id));
  t.after(() => scheduler.stop());
  scheduler.start();
  scheduler.pump(NOW + 119999);
  assert.deepEqual(restored.snapshot(), saved);
  assert.deepEqual(fires, []);
  assert.deepEqual(expired, []);
  scheduler.pump(NOW + 120000);
  assert.equal(restored.get(entry.id), undefined);
  assert.equal(scheduler.nextFire(entry.id), undefined);
  assert.deepEqual(fires, []);
  assert.deepEqual(expired, [entry.id]);
});

test("A11 overdue snapshot recovery expires before scheduler start without a second retirement", (t) => {
  const f = fixture(t);
  const entry = f.waiting();
  const saved = f.store.snapshot();
  saved.loops[0].expiresAt = NOW - 1;
  f.store.restoreSnapshot(saved);
  const writes: ReturnType<LoopStore["snapshot"]>[] = [];
  f.store.onChange = snapshot => writes.push(snapshot);
  const records = f.store.expireEntries(NOW);
  assert.equal(records.length, 1);
  assert.equal(records[0].entry.id, entry.id);
  assert.equal(records[0].disposition, "deleted");
  assert.equal(records[0].reason, "expires_at");
  f.scheduler.start();
  f.scheduler.pump(NOW);
  assert.equal(f.store.get(entry.id), undefined);
  assert.equal(f.scheduler.nextFire(entry.id), undefined);
  assert.equal(writes.length, 1);
  assert.deepEqual(f.expirations, []);
  assert.deepEqual(f.fires, []);
});

test("A11 file-backed retirement survives a new store instance", (t) => {
  const directory = mkdtempSync(join(tmpdir(), "a11-expiry-"));
  try {
    const path = join(directory, "loops.json");
    const f = fixture(t, new LoopStore(path));
    const entry = f.waiting();
    assert.equal(new LoopStore(path).get(entry.id)?.dynamic?.awaitingUpdate, true);
    f.scheduler.add(entry);
    f.scheduler.pump(DEADLINE);
    assertDeleted(f, entry);
    assert.equal(new LoopStore(path).get(entry.id), undefined);
  } finally {
    rmSync(directory, { recursive: true, force: true });
  }
});

test("A11 failed memory append rolls back and retries the same eligible registration", (t) => {
  const f = fixture(t);
  const entry = f.waiting();
  f.scheduler.add(entry);
  const before = f.store.snapshot();
  let fail = true;
  let attempts = 0;
  const writes: ReturnType<LoopStore["snapshot"]>[] = [];
  f.store.onChange = snapshot => {
    attempts++;
    if (fail) throw new Error("A11 injected append failure");
    writes.push(snapshot);
  };
  assert.throws(() => f.scheduler.pump(DEADLINE), /A11 injected append failure/);
  assert.deepEqual(f.store.snapshot(), before);
  assert.equal(f.scheduler.nextFire(entry.id), NOW);
  assert.deepEqual(f.expirations, []);
  assert.deepEqual(f.fires, []);
  fail = false;
  f.scheduler.pump(DEADLINE + 1);
  f.scheduler.pump(DEADLINE + 2);
  assertDeleted(f, entry);
  assert.equal(attempts, 2);
  assert.equal(writes.length, 1);
  assert.equal(writes[0].loops.length, 0);
});

function updateTool(f: ReturnType<typeof fixture>) {
  const tools = new Map<string, any>();
  const pi = { registerTool: (tool: any) => tools.set(tool.name, tool), appendEntry() {} } as unknown as ExtensionAPI;
  registerLoopTools({ pi, getStore: () => f.store, getScheduler: () => f.scheduler,
    getTriggerSystem: () => f.scheduler, getMonitorManager: () => ({ get: () => undefined }), updateWidget() {} });
  return (id: string) => tools.get("LoopUpdate").execute("a11-update", { id, status: "continue", state: "updated checkpoint", nextInterval: "1s" });
}

test("A11 valid continue before deadline preserves lifetime and later expires", async (t) => {
  const f = fixture(t);
  const entry = f.waiting();
  f.scheduler.add(entry);
  f.clock(DEADLINE - 2000);
  const result = await updateTool(f)(entry.id);
  assert.equal(result.details.tone, "success");
  assert.equal(f.store.get(entry.id)?.dynamic?.state, "updated checkpoint");
  assert.equal(f.store.get(entry.id)?.dynamic?.awaitingUpdate, false);
  assert.equal(f.store.get(entry.id)?.expiresAt, DEADLINE);
  f.clock(DEADLINE);
  f.scheduler.pump(Date.now());
  assertDeleted(f, entry);
});

for (const offset of [0, 1]) {
  test(`A11 continue at deadline +${offset} cannot renew before pump or recreate after it`, async (t) => {
    const f = fixture(t);
    const entry = f.waiting();
    f.scheduler.add(entry);
    const update = updateTool(f);
    f.clock(DEADLINE + offset);
    const before = f.store.snapshot();
    await assert.rejects(async () => update(entry.id), /has expired/);
    assert.equal(f.store.continueDynamic(entry.id, { dynamic: { awaitingUpdate: false } }), undefined);
    assert.equal(f.store.resume(entry.id), undefined);
    assert.deepEqual(f.store.snapshot(), before);
    f.scheduler.pump(Date.now());
    const after = f.store.snapshot();
    await assert.rejects(async () => update(entry.id), /not found/);
    assert.deepEqual(f.store.snapshot(), after);
    assertDeleted(f, entry);
  });
}

test("A11 dynamic task-backlog expiry preserves existing pause disposition", (t) => {
  const f = fixture(t);
  const entry = f.waiting({}, { taskBacklog: true });
  const writes: ReturnType<LoopStore["snapshot"]>[] = [];
  f.store.onChange = snapshot => writes.push(snapshot);
  f.scheduler.add(entry);
  f.scheduler.pump(DEADLINE);
  f.scheduler.pump(DEADLINE + 1);
  assert.equal(f.store.get(entry.id)?.status, "paused");
  assert.deepEqual(f.store.get(entry.id)?.pause, { kind: "controller_limit", reason: "loop expiry reached", at: DEADLINE });
  assert.equal(f.scheduler.nextFire(entry.id), undefined);
  assert.deepEqual(f.fires, []);
  assert.deepEqual(f.expirations, [{ id: entry.id, disposition: "paused" }]);
  assert.equal(writes.length, 1);
  assert.equal(writes[0].loops[0].status, "paused");
});

for (const trigger of [{ type: "cron", schedule: "* * * * *" }, { type: "event", source: "a11:event" }] as const) {
  test(`A11 ordinary ${trigger.type} expiry still deletes without work`, (t) => {
    const f = fixture(t);
    const entry = f.store.create(trigger, "A11 other trigger", { recurring: true, maxFires: 10 });
    f.scheduler.add(entry);
    f.scheduler.pump(DEADLINE);
    assertDeleted(f, entry);
  });
}

test("A11 unexpired non-waiting dynamic entry still dispatches", (t) => {
  const f = fixture(t);
  const entry = f.waiting({ awaitingUpdate: false });
  f.scheduler.add(entry);
  f.scheduler.pump(NOW);
  assert.equal(f.store.get(entry.id)?.status, "active");
  assert.deepEqual(f.fires, [entry.id]);
  assert.deepEqual(f.expirations, []);
});

test("A11 fire cap still pauses through cap retirement, not expiry", (t) => {
  const f = fixture(t);
  const entry = f.waiting({ awaitingUpdate: false }, { maxFires: 1 });
  const scheduler = new CronScheduler(f.store, (current, origin) => {
    f.fires.push(current.id);
    f.store.fire(current.id, origin);
  }, (current, disposition) => f.expirations.push({ id: current.id, disposition }));
  t.after(() => scheduler.stop());
  scheduler.add(entry);
  scheduler.pump(NOW);
  assert.equal(f.store.get(entry.id)?.fireCount, 1);
  assert.equal(f.store.get(entry.id)?.status, "paused");
  assert.equal(f.store.get(entry.id)?.pause?.reason, "scheduler fire cap reached");
  assert.equal(scheduler.nextFire(entry.id), undefined);
  assert.deepEqual(f.fires, [entry.id]);
  assert.deepEqual(f.expirations, []);
});

test("A11 mixed timer maps retire waiting entries without skipping valid due work", (t) => {
  const f = fixture(t);
  const first = f.waiting();
  const expiryOnly = f.waiting({ nextWakeAt: DEADLINE });
  f.clock(DEADLINE - 1000);
  const valid = f.waiting({ awaitingUpdate: false, nextWakeAt: DEADLINE });
  const last = f.waiting();
  const saved = f.store.snapshot();
  saved.loops.find(e => e.id === last.id)!.expiresAt = DEADLINE;
  f.store.restoreSnapshot(saved);
  f.scheduler.start();
  f.scheduler.pump(DEADLINE);
  for (const entry of [first, expiryOnly, last]) {
    assert.equal(f.store.get(entry.id), undefined);
    assert.equal(f.scheduler.nextFire(entry.id), undefined);
  }
  assert.equal(f.store.get(valid.id)?.status, "active");
  assert.deepEqual(f.fires, [valid.id]);
  assert.deepEqual(f.expirations.map(e => e.id).sort(), [first.id, expiryOnly.id, last.id].sort());
  assert.ok(f.expirations.every(e => e.disposition === "deleted"));
});

test("A11 already-removed store transition cannot emit a false expiry callback", (t) => {
  const f = fixture(t);
  const entry = f.waiting();
  f.scheduler.add(entry);
  const expireEntry = f.store.expireEntry.bind(f.store);
  // A competing removal between the scheduler read and the locked transition.
  t.mock.method(f.store, "expireEntry", (id: string, now?: number) => {
    f.store.delete(id);
    return expireEntry(id, now);
  });
  f.scheduler.pump(DEADLINE);
  assert.equal(f.store.get(entry.id), undefined);
  assert.equal(f.scheduler.nextFire(entry.id), undefined);
  assert.deepEqual(f.expirations, []);
  assert.deepEqual(f.fires, []);
});

test("A11 expiry callback failure cannot authorize another work wake", (t) => {
  const f = fixture(t);
  const entry = f.waiting();
  let callbacks = 0;
  const scheduler = new CronScheduler(f.store, e => f.fires.push(e.id), () => {
    callbacks++;
    throw new Error("A11 injected callback failure");
  });
  t.after(() => scheduler.stop());
  scheduler.add(entry);
  assert.throws(() => scheduler.pump(DEADLINE), /A11 injected callback failure/);
  assert.equal(f.store.get(entry.id), undefined);
  assert.equal(scheduler.nextFire(entry.id), undefined);
  scheduler.pump(DEADLINE + 1);
  assert.equal(callbacks, 1);
  assert.deepEqual(f.fires, []);
});
