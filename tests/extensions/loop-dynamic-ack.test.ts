import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import { mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { spawn } from "node:child_process";
import test from "node:test";
import { Check } from "typebox/value";
import loops from "../../loop/index.js";
import { LoopStore } from "../../loop/store.js";
import { CronScheduler } from "../../loop/scheduler.js";
import { createNotificationRuntime } from "../../loop/runtime/notification-runtime.js";
import { registerLoopTools } from "../../loop/tools/loop-tools.js";
import type { LoopEntry, LoopStoreData } from "../../loop/types.js";

const NOW = 1_800_000_000_000;
function fixture(store = new LoopStore()) {
  const tools = new Map<string, any>();
  const registrations: unknown[] = []; const audits: any[] = []; const snapshots: LoopStoreData[] = [];
  const failures = { snapshot: false, audit: false, registration: false };
  store.onChange = snapshot => {
    if (failures.snapshot) throw new Error("snapshot append failed");
    snapshots.push(structuredClone(snapshot));
  };
  registerLoopTools({ pi: { registerTool: (tool: any) => tools.set(tool.name, tool), appendEntry: (...args: unknown[]) => {
    if (failures.audit) throw new Error("audit append failed");
    audits.push(structuredClone(args));
  } } as any,
    getStore: () => store, getTriggerSystem: () => ({ add: entry => {
      if (failures.registration) throw new Error("registration failed");
      registrations.push(entry);
    }, remove: id => { registrations.push(id); } }),
    getScheduler: () => ({ nextFire: () => undefined }), getMonitorManager: () => ({ get: () => undefined }), updateWidget() {},
  });
  return { store, tools, registrations, audits, snapshots, failures,
    call: (params: object) => tools.get("LoopUpdate").execute("ack", params),
    list: () => tools.get("LoopList").execute("list", {}),
  };
}
function seed(store: LoopStore, maxFires = 4, recurring = true) {
  return store.create({ type: "dynamic" }, "goal", { recurring, maxFires,
    dynamic: { state: "saved", metrics: "metrics", doneCriteria: "done" },
  });
}
function wake(store: LoopStore, id: string) { return store.beginDynamicWake(id)!.dynamic!.pendingWakeId!; }
function expected(entry: LoopEntry) { return { status: entry.status, iteration: entry.dynamic!.iteration, updatedAt: entry.updatedAt }; }
function unchanged(f: ReturnType<typeof fixture>) { return structuredClone([f.store.snapshot(), f.registrations, f.audits, f.snapshots]); }
async function rejectsUnchanged(f: ReturnType<typeof fixture>, params: object, pattern?: RegExp) {
  const before = unchanged(f);
  await assert.rejects(async () => f.call(params), pattern ?? /./);
  assert.deepEqual(unchanged(f), before);
}

test("a recorded wake accepts one continue, never a second checkpoint without another wake", async () => {
  const f = fixture(); const entry = seed(f.store);
  const wakeId = wake(f.store, entry.id);
  await f.call({ id: entry.id, wakeId, status: "continue", state: "first", nextInterval: "1h" });
  await rejectsUnchanged(f, { id: entry.id, wakeId, status: "continue", state: "second" }, /not awaiting/);
  assert.equal(f.store.get(entry.id)?.dynamic?.iteration, 1);
  assert.equal(f.store.get(entry.id)?.dynamic?.state, "first");
  assert.equal(f.store.get(entry.id)?.fireCount, 1);
  assert.equal(f.store.get(entry.id)?.dynamic?.pendingWakeId, undefined);
  assert.equal(f.audits[0][1].wakeId, wakeId);
});

for (const status of ["continue", "paused", "completed"] as const) {
  test(`stale A cannot ${status} while B is pending; B remains usable`, async t => {
    t.mock.method(Date, "now", () => NOW);
    const f = fixture(); const entry = seed(f.store); const a = wake(f.store, entry.id);
    await f.call({ id: entry.id, wakeId: a, status: "continue" });
    const b = wake(f.store, entry.id); assert.notEqual(a, b);
    await rejectsUnchanged(f, { id: entry.id, wakeId: a, status, state: "obsolete" }, /stale wakeId/);
    await f.call({ id: entry.id, wakeId: b, status });
    await rejectsUnchanged(f, { id: entry.id, wakeId: b, status });
  });
  test(`fresh loop cannot ${status} before any recorded wake`, async () => {
    const f = fixture(); const entry = seed(f.store);
    await rejectsUnchanged(f, { id: entry.id, wakeId: "not-a-wake", status }, /not awaiting/);
  });
}

test("wrong-loop identities reject even with equal timestamps and iterations", async t => {
  t.mock.method(Date, "now", () => NOW);
  const f = fixture(); const a = seed(f.store); const b = seed(f.store);
  const tokenA = wake(f.store, a.id); const tokenB = wake(f.store, b.id);
  assert.notEqual(tokenA, tokenB);
  for (const status of ["continue", "paused", "completed"]) await rejectsUnchanged(f, { id: b.id, wakeId: tokenA, status }, /stale/);
  await f.call({ id: b.id, wakeId: tokenB, status: "paused" });
});

test("schema and direct adapter reject missing, malformed tokens and statuses without mutation", async () => {
  const f = fixture(); const entry = seed(f.store); const wakeId = wake(f.store, entry.id);
  const schema = f.tools.get("LoopUpdate").parameters;
  for (const bad of [undefined, "", " ", "has space", "newline\n", "x".repeat(129), 1, null, {}]) {
    const params = { id: entry.id, wakeId: bad, status: "continue" };
    assert.equal(Check(schema, params), false);
    await rejectsUnchanged(f, params, /wakeId is required/);
  }
  for (const status of ["bad", undefined, null, 3, {}]) {
    const params = { id: entry.id, wakeId, status };
    assert.equal(Check(schema, params), false);
    await rejectsUnchanged(f, params, /Invalid status/);
  }
  assert.equal(Check(schema, { id: entry.id, wakeId: "x".repeat(128), status: "continue" }), true);
  await rejectsUnchanged(f, { id: entry.id, wakeId: "opaque-but-wrong", status: "continue" }, /stale/);
  await f.call({ id: entry.id, wakeId, status: "continue" });
});

test("invalid intervals retain the token for a corrected timed continue and omitted fields survive", async t => {
  t.mock.method(Date, "now", () => NOW);
  const f = fixture(); const entry = seed(f.store); const wakeId = wake(f.store, entry.id);
  for (const nextInterval of ["", "bad", "0s", "8d", "7d"]) {
    await rejectsUnchanged(f, { id: entry.id, wakeId, status: "continue", nextInterval }, /Invalid nextInterval|remaining lifetime/);
  }
  await f.call({ id: entry.id, wakeId, status: "continue", nextInterval: "30s" });
  const saved = f.store.get(entry.id)!;
  assert.equal(saved.dynamic!.nextWakeAt, NOW + 30_000);
  assert.equal(saved.dynamic!.state, "saved"); assert.equal(saved.dynamic!.metrics, "metrics");
  const second = wake(f.store, entry.id);
  await f.call({ id: entry.id, wakeId: second, status: "continue" });
  assert.equal(f.store.get(entry.id)!.dynamic!.nextWakeAt, undefined);
});

test("acceptance checks expiry for all statuses before scheduler cleanup", async t => {
  t.mock.method(Date, "now", () => NOW);
  const f = fixture(); const entry = seed(f.store); const wakeId = wake(f.store, entry.id);
  t.mock.method(Date, "now", () => entry.expiresAt);
  for (const status of ["continue", "paused", "completed"]) await rejectsUnchanged(f, { id: entry.id, wakeId, status }, /expired/);
  assert.equal(f.store.beginDynamicWake(entry.id), undefined);
});

for (const status of ["paused", "completed"] as const) {
  for (const retirement of ["direct cap", "scheduler cap", "one shot"] as const) {
    test(`${retirement} preserves its final token for ${status}, never continue or implicit resume`, async t => {
      t.mock.method(Date, "now", () => NOW);
      const f = fixture(); const entry = seed(f.store, retirement === "one shot" ? 4 : 1, retirement !== "one shot");
      if (retirement === "direct cap") wake(f.store, entry.id);
      else {
        const scheduler = new CronScheduler(f.store, loop => { f.store.beginDynamicWake(loop.id); });
        scheduler.add(entry); scheduler.pump(NOW); scheduler.pump(NOW);
        assert.equal(scheduler.nextFire(entry.id), undefined); scheduler.stop();
      }
      const saved = f.store.get(entry.id)!; const wakeId = saved.dynamic!.pendingWakeId!;
      assert.equal(saved.status, "paused"); assert.equal(saved.fireCount, 1); assert.ok(wakeId);
      assert.match((await f.list()).content[0].text, new RegExp(`awaitingUpdate: true wakeId: ${wakeId}`));
      await rejectsUnchanged(f, { id: entry.id, wakeId, status: "continue" }, /fire cap|paused/);
      if (retirement !== "one shot") assert.equal(f.store.resume(entry.id), undefined);
      await f.call({ id: entry.id, wakeId, status });
      assert.equal(f.store.get(entry.id)?.dynamic?.pendingWakeId, undefined);
      await rejectsUnchanged(f, { id: entry.id, wakeId, status });
    });
  }
}

test("administrative pause/delete cancel pending tokens; explicit resume preserves counters and gets a new wake", async t => {
  t.mock.method(Date, "now", () => NOW);
  const f = fixture(); const entry = seed(f.store); const old = wake(f.store, entry.id);
  await f.tools.get("LoopDelete").execute("cancel", { id: entry.id, action: "pause" });
  const paused = f.store.get(entry.id)!;
  assert.equal(paused.dynamic!.pendingWakeId, undefined); assert.equal(paused.dynamic!.awaitingUpdate, false);
  await rejectsUnchanged(f, { id: entry.id, wakeId: old, status: "continue" }, /paused; use \/loop/);
  f.store.resume(entry.id);
  assert.equal(f.store.get(entry.id)?.fireCount, 1); assert.equal(f.store.get(entry.id)?.dynamic?.iteration, 0);
  assert.equal(f.store.get(entry.id)?.dynamic?.state, "saved");
  const fresh = wake(f.store, entry.id); assert.notEqual(fresh, old);
  await rejectsUnchanged(f, { id: entry.id, wakeId: old, status: "completed" }, /stale/);
  await f.tools.get("LoopDelete").execute("cancel", { id: entry.id });
  await rejectsUnchanged(f, { id: entry.id, wakeId: fresh, status: "completed" }, /not found/);
});

test("explicit administrative pause invalidates even an already capped pending wake", async () => {
  const f = fixture(); const entry = seed(f.store, 1); const wakeId = wake(f.store, entry.id);
  await f.tools.get("LoopDelete").execute("pause-capped", { id: entry.id, action: "pause" });
  assert.equal(f.store.get(entry.id)?.pause?.kind, "administrative");
  assert.equal(f.store.get(entry.id)?.dynamic?.pendingWakeId, undefined);
  await rejectsUnchanged(f, { id: entry.id, wakeId, status: "completed" }, /paused/);
});

test("competing admissions persist one complete snapshot with one UUID and one fire", () => {
  const f = fixture(); const entry = seed(f.store); f.snapshots.length = 0;
  const first = f.store.beginDynamicWake(entry.id)!;
  assert.equal(f.store.beginDynamicWake(entry.id), undefined);
  assert.match(first.dynamic!.pendingWakeId!, /^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/);
  assert.equal(f.snapshots.length, 1); assert.equal(f.snapshots[0].loops[0].fireCount, 1);
  assert.equal(f.snapshots[0].loops[0].dynamic!.pendingWakeId, first.dynamic!.pendingWakeId);
});

for (const stop of ["paused", "completed"] as const) {
  for (const first of ["continue", stop]) {
    test(`competing acknowledgements ${first} wins against ${first === "continue" ? stop : "continue"}`, async () => {
      const f = fixture(); const entry = seed(f.store); const wakeId = wake(f.store, entry.id);
      const results = await Promise.allSettled([first, first === "continue" ? stop : "continue"].map(status =>
        Promise.resolve().then(() => f.call({ id: entry.id, wakeId, status, state: status }))));
      assert.deepEqual(results.map(r => r.status), ["fulfilled", "rejected"]);
      assert.equal(f.audits.length, 1);
      assert.equal(f.registrations.length, first === "continue" ? 2 : 1);
      if (first !== "completed") assert.equal(f.store.get(entry.id)?.dynamic?.state, first);
    });
  }
}

test("two shared-file readers recheck tokens and limits under lock, not their stale preflight", async t => {
  t.mock.method(Date, "now", () => NOW);
  const dir = mkdtempSync(join(tmpdir(), "a08-shared-"));
  try {
    const path = join(dir, "loops.json"); const a = fixture(new LoopStore(path)); const entry = seed(a.store);
    const wakeId = wake(a.store, entry.id); const b = fixture(new LoopStore(path)); const stale = b.store.get(entry.id)!;
    await a.call({ id: entry.id, wakeId, status: "continue", state: "winner" });
    b.store.list(); // Refresh the inspection baseline; the adapter still receives its stale read.
    t.mock.method(b.store, "get", () => stale);
    const raw = readFileSync(path, "utf8");
    for (const status of ["continue", "paused", "completed"]) await rejectsUnchanged(b, { id: entry.id, wakeId, status }, /changed while/);
    assert.equal(readFileSync(path, "utf8"), raw); assert.deepEqual(b.registrations, []);
    // Direct store callers cannot forge the next iteration or bypass time limits.
    const tokenB = wake(a.store, entry.id); const before = readFileSync(path, "utf8");
    for (const nextWakeAt of [NOW, NaN, entry.expiresAt]) assert.equal(a.store.continueDynamic(entry.id, { dynamic: { nextWakeAt } }, tokenB), undefined);
    assert.equal(readFileSync(path, "utf8"), before);
    a.store.continueDynamic(entry.id, { dynamic: { iteration: 999 } }, tokenB);
    assert.equal(a.store.get(entry.id)?.dynamic?.iteration, 2);
  } finally { rmSync(dir, { recursive: true, force: true }); }
});

test("shared locked reload rejects cancellation, expiry and budget changes after preflight", async t => {
  t.mock.method(Date, "now", () => NOW);
  const dir = mkdtempSync(join(tmpdir(), "a08-locked-limits-"));
  try {
    for (const change of ["expiry", "budget", "pause"] as const) {
      const path = join(dir, `${change}.json`); const store = new LoopStore(path); const entry = seed(store);
      const wakeId = wake(store, entry.id); const stale = store.get(entry.id)!;
      const updated = store.snapshot();
      if (change === "expiry") updated.loops[0].expiresAt = NOW;
      if (change === "budget") updated.loops[0].maxFires = 1;
      if (change === "pause") { updated.loops[0].status = "paused"; updated.loops[0].pause = { kind: "administrative", at: NOW }; }
      writeFileSync(path, JSON.stringify(updated));
      const f = fixture(new LoopStore(path)); t.mock.method(f.store, "get", () => stale);
      const before = readFileSync(path, "utf8");
      for (const status of change === "budget" ? ["continue"] : ["continue", "paused", "completed"]) {
        await assert.rejects(async () => f.call({ id: entry.id, wakeId, status }), /changed while/);
        assert.equal(readFileSync(path, "utf8"), before);
        assert.deepEqual([f.registrations, f.audits, f.snapshots], [[], [], []]);
      }
      assert.equal(f.store.beginDynamicWake(entry.id), undefined);
      assert.equal(readFileSync(path, "utf8"), before);
    }
  } finally { rmSync(dir, { recursive: true, force: true }); }
});

test("one-shot administrative resume cancels its token without advancing iteration", async t => {
  t.mock.method(Date, "now", () => NOW);
  const f = fixture(); const entry = seed(f.store, 4, false);
  const scheduler = new CronScheduler(f.store, current => { f.store.beginDynamicWake(current.id); });
  scheduler.add(entry); scheduler.pump(NOW); scheduler.stop();
  const previous = f.store.get(entry.id)!.dynamic!.pendingWakeId!;
  f.store.resume(entry.id);
  assert.equal(f.store.get(entry.id)?.fireCount, 1); assert.equal(f.store.get(entry.id)?.dynamic?.iteration, 0);
  assert.equal(f.store.get(entry.id)?.dynamic?.pendingWakeId, undefined);
  await rejectsUnchanged(f, { id: entry.id, wakeId: previous, status: "continue" }, /not awaiting/);
  const next = wake(f.store, entry.id); assert.notEqual(next, previous);
});

test("expiry deletes the pending identity rather than preserving the final-wake exception", async t => {
  t.mock.method(Date, "now", () => NOW);
  const f = fixture(); const entry = seed(f.store, 1); const wakeId = wake(f.store, entry.id);
  t.mock.method(Date, "now", () => entry.expiresAt);
  assert.equal(f.store.expireEntry(entry.id)?.disposition, "deleted");
  await rejectsUnchanged(f, { id: entry.id, wakeId, status: "completed" }, /not found/);
});

test("concurrent child processes sharing a file accept the token only once", { timeout: 20_000 }, async () => {
  const dir = mkdtempSync(join(tmpdir(), "a08-processes-"));
  const children: ReturnType<typeof spawn>[] = [];
  try {
    const path = join(dir, "loops.json"); const store = new LoopStore(path); const entry = seed(store); const wakeId = wake(store, entry.id);
    const code = `import { LoopStore } from ${JSON.stringify(new URL("../../loop/store.ts", import.meta.url).href)};
      const store = new LoopStore(${JSON.stringify(path)}); store.get(${JSON.stringify(entry.id)});
      process.stdout.write('ready\\n');
      process.stdin.once('data', () => {
        const result = store.continueDynamic(${JSON.stringify(entry.id)}, { dynamic: { state: String(process.pid) } }, ${JSON.stringify(wakeId)});
        process.stdout.write(JSON.stringify(!!result) + '\\n'); process.stdin.destroy();
      });`;
    const runners = [0, 1].map(() => {
      const child = spawn(process.execPath, ["--import", "tsx", "--input-type=module", "-e", code], { stdio: ["pipe", "pipe", "pipe"], timeout: 10_000 }); children.push(child);
      let output = ""; let errors = "";
      const ready = new Promise<void>((resolve, reject) => {
        child.stdout!.on("data", data => { output += data; if (output.includes("ready\n")) resolve(); });
        child.on("error", reject); child.on("exit", () => { if (!output.includes("ready\n")) reject(new Error(errors)); });
      });
      child.stderr!.on("data", data => { errors += data; });
      const done = new Promise<boolean>((resolve, reject) => {
        child.on("error", reject); child.on("exit", code => code === 0 ? resolve(output.includes("\ntrue\n")) : reject(new Error(errors)));
      });
      return { child, ready, done };
    });
    await Promise.all(runners.map(r => r.ready));
    for (const r of runners) r.child.stdin!.end("go\n");
    const results = await Promise.all(runners.map(r => r.done));
    assert.deepEqual(results.sort(), [false, true]);
    assert.equal(new LoopStore(path).get(entry.id)?.dynamic?.iteration, 1);
  } finally { for (const child of children) child.kill(); rmSync(dir, { recursive: true, force: true }); }
});

test("pending snapshot restore retains identity with no replay; accepted restore rejects replay", async t => {
  t.mock.method(Date, "now", () => NOW);
  const store = new LoopStore(); const entry = seed(store); const wakeId = wake(store, entry.id);
  const restored = new LoopStore(); restored.restoreSnapshot(store.snapshot());
  const scheduler = new CronScheduler(restored, () => assert.fail("wake replay")); scheduler.start(); scheduler.pump(NOW + 1000); scheduler.stop();
  assert.equal(restored.get(entry.id)?.dynamic?.pendingWakeId, wakeId);
  const f = fixture(restored); await f.call({ id: entry.id, wakeId, status: "continue" });
  const accepted = new LoopStore(); accepted.restoreSnapshot(restored.snapshot());
  await rejectsUnchanged(fixture(accepted), { id: entry.id, wakeId, status: "continue" }, /not awaiting/);
});

test("legacy migration is deterministic across snapshots and file readers; inspected token works once", async t => {
  t.mock.method(Date, "now", () => NOW);
  const store = new LoopStore(); const entry = seed(store); wake(store, entry.id);
  const legacy = store.snapshot(); delete legacy.loops[0].dynamic!.pendingWakeId;
  const expectedToken = `legacy-${createHash("sha256").update(JSON.stringify([entry.id, NOW, 1, 0, NOW])).digest("hex")}`;
  const dir = mkdtempSync(join(tmpdir(), "a08-legacy-"));
  try {
    const path = join(dir, "loops.json"); writeFileSync(path, JSON.stringify(legacy)); const raw = readFileSync(path, "utf8");
    const restored = new LoopStore(); restored.restoreSnapshot(legacy);
    for (const reader of [restored, new LoopStore(path), new LoopStore(path)]) {
      assert.equal(reader.get(entry.id)?.dynamic?.pendingWakeId, expectedToken);
      assert.match((await fixture(reader).list()).content[0].text, new RegExp(expectedToken));
    }
    assert.equal(readFileSync(path, "utf8"), raw);
    const f = fixture(new LoopStore(path));
    await rejectsUnchanged(f, { id: entry.id, status: "continue" }, /wakeId is required/);
    await f.call({ id: entry.id, wakeId: expectedToken, status: "continue" });
    await rejectsUnchanged(fixture(new LoopStore(path)), { id: entry.id, wakeId: expectedToken, status: "continue" }, /not awaiting/);
    const paused = structuredClone(legacy); paused.loops[0].status = "paused"; paused.loops[0].pause = { kind: "administrative", at: NOW };
    restored.restoreSnapshot(paused);
    assert.equal(restored.get(entry.id)?.dynamic?.awaitingUpdate, false);
    assert.equal(restored.get(entry.id)?.dynamic?.pendingWakeId, undefined);
    for (const cap of [true, false]) {
      const retired = structuredClone(legacy); retired.loops[0].status = "paused";
      retired.loops[0].pause = { kind: "controller_limit", at: NOW, reason: "scheduler fire cap reached" };
      if (cap) retired.loops[0].maxFires = 1; else retired.loops[0].recurring = false;
      restored.restoreSnapshot(retired);
      assert.equal(restored.get(entry.id)?.dynamic?.pendingWakeId, expectedToken);
      await fixture(restored).call({ id: entry.id, wakeId: expectedToken, status: "completed" });
    }
  } finally { rmSync(dir, { recursive: true, force: true }); }
});

for (const metadata of [{ awaitingUpdate: false, pendingWakeId: "orphan" }, { awaitingUpdate: true, pendingWakeId: "bad token" },
  { awaitingUpdate: true, pendingWakeId: 12 }, { awaitingUpdate: "yes", pendingWakeId: "opaque" },
  { awaitingUpdate: null, pendingWakeId: undefined }, { awaitingUpdate: true, pendingWakeId: null }]) {
  test(`malformed restored metadata fails closed locally and pause/resume repairs ${JSON.stringify(metadata)}`, async () => {
    const store = new LoopStore(); const entry = seed(store); const unrelated = store.create({ type: "event", source: "event" }, "unrelated", { recurring: true });
    const snapshot = store.snapshot(); Object.assign(snapshot.loops[0].dynamic!, metadata); store.restoreSnapshot(snapshot);
    const f = fixture(store); const before = store.snapshot();
    assert.equal(store.beginDynamicWake(entry.id), undefined); assert.deepEqual(store.snapshot(), before);
    assert.ok(store.get(unrelated.id));
    await rejectsUnchanged(f, { id: entry.id, wakeId: "opaque", status: "paused" }, /invalid pending/);
    assert.match((await f.list()).content[0].text, /invalid pending/);
    store.pause(entry.id); store.resume(entry.id); const wakeId = wake(store, entry.id);
    await f.call({ id: entry.id, wakeId, status: "paused" });
  });
}

for (const status of ["continue", "paused", "completed"]) test(`memory snapshot failure rolls back admission and ${status}; same token remains retryable`, async () => {
  const f = fixture(); const entry = seed(f.store); const fresh = f.store.snapshot(); f.failures.snapshot = true;
  assert.throws(() => f.store.beginDynamicWake(entry.id), /snapshot append/); assert.deepEqual(f.store.snapshot(), fresh);
  f.failures.snapshot = false; const wakeId = wake(f.store, entry.id); const before = unchanged(f);
  f.failures.snapshot = true;
  assert.throws(() => f.call({ id: entry.id, wakeId, status }), /snapshot append/);
  assert.deepEqual(unchanged(f), before);
  f.failures.snapshot = false; await f.call({ id: entry.id, wakeId, status });
});

for (const failure of ["audit", "registration"] as const) {
  test(`post-commit ${failure} failure never reopens the token`, async () => {
    const f = fixture(); const entry = seed(f.store); const wakeId = wake(f.store, entry.id); f.failures[failure] = true;
    assert.throws(() => f.call({ id: entry.id, wakeId, status: "continue" }), /failed/);
    assert.equal(f.store.get(entry.id)?.dynamic?.iteration, 1); assert.equal(f.store.get(entry.id)?.dynamic?.pendingWakeId, undefined);
    await rejectsUnchanged(f, { id: entry.id, wakeId, status: "continue" }, /not awaiting/);
  });
}

test("shared-file mirror failure leaves the authoritative update consumed", async () => {
  const dir = mkdtempSync(join(tmpdir(), "a08-mirror-"));
  try {
    const path = join(dir, "loops.json"); const f = fixture(new LoopStore(path)); const entry = seed(f.store); const wakeId = wake(f.store, entry.id);
    f.failures.snapshot = true; assert.throws(() => f.call({ id: entry.id, wakeId, status: "continue" }), /snapshot append/);
    const authoritative = fixture(new LoopStore(path)); assert.equal(authoritative.store.get(entry.id)?.dynamic?.iteration, 1);
    await rejectsUnchanged(authoritative, { id: entry.id, wakeId, status: "continue" }, /not awaiting/);
    assert.deepEqual(f.registrations, []);
  } finally { rmSync(dir, { recursive: true, force: true }); }
});

test("cancellation ordering does not undo a committed update or acknowledge cancelled work", async () => {
  const f = fixture(); const entry = seed(f.store); const a = wake(f.store, entry.id); f.store.pause(entry.id);
  await rejectsUnchanged(f, { id: entry.id, wakeId: a, status: "continue" }, /paused/);
  f.store.resume(entry.id); const b = wake(f.store, entry.id);
  await f.call({ id: entry.id, wakeId: b, status: "continue", state: "committed" }); f.store.pause(entry.id);
  assert.equal(f.store.get(entry.id)?.dynamic?.iteration, 1); assert.equal(f.store.get(entry.id)?.dynamic?.state, "committed");
  assert.equal(f.store.get(entry.id)?.fireCount, 2);
});

test("store rejects workflow, orchestration and task-backlog ownership at admission and acceptance", () => {
  for (const owner of ["workflow", "orchestration", "taskBacklog"] as const) {
    const store = new LoopStore(); const entry = seed(store); const wakeId = wake(store, entry.id);
    // Store entries are intentionally live here to avoid validating unrelated owners' definitions.
    Object.assign(store.get(entry.id)!, { [owner]: owner === "taskBacklog" ? true : {} });
    const current = store.get(entry.id)!;
    assert.equal(store.beginDynamicWake(entry.id), undefined);
    assert.equal(store.continueDynamic(entry.id, { dynamic: {} }, wakeId), undefined);
    for (const status of ["paused", "completed"] as const) assert.equal(store.stopDynamic(entry.id, status, wakeId, expected(current)), false);
  }
});

test("notification queue, restore and delivery retain exact token and legacy messages request inspection", async () => {
  const messages: any[] = []; const pending: any[] = [];
  const runtime = createNotificationRuntime({ pi: { sendMessage: (message: any) => messages.push(message) } as any,
    hasPendingTasks: async () => 0, cleanDoneTasks: async () => {}, getHasPendingMessages: () => false,
    onPendingChanged: values => pending.push(structuredClone(values)),
  });
  const store = new LoopStore(); const entry = seed(store); const wakeId = wake(store, entry.id);
  runtime.syncRuntimeState({ agentRunning: true });
  await runtime.queueOrDeliverNotification({ loopId: entry.id, prompt: entry.prompt, trigger: entry.trigger, timestamp: NOW, recurring: true, dynamic: store.get(entry.id)!.dynamic });
  assert.equal(messages.length, 0); const recorded = pending.at(-1);
  assert.equal(recorded[0].dynamic.pendingWakeId, wakeId);
  runtime.restore(recorded); runtime.syncRuntimeState({ agentRunning: false }); await runtime.flushPendingNotifications();
  assert.equal(messages[0].details.dynamic.pendingWakeId, wakeId);
  assert.ok(messages[0].content.includes(`Wake ID: ${wakeId}`));
  assert.ok(messages[0].content.includes(`Call LoopUpdate with id="${entry.id}" and wakeId="${wakeId}" exactly once for this wake.`));
  const legacy = structuredClone(recorded); delete legacy[0].dynamic.pendingWakeId;
  runtime.restore(legacy); runtime.syncRuntimeState({ agentRunning: false }); await runtime.flushPendingNotifications();
  assert.match(messages[1].content, /Inspect LoopList and the saved checkpoint/); assert.doesNotMatch(messages[1].content, /wakeId="undefined"/);
  runtime.clear("session_shutdown");
});

test("real extension tool and command activation persist a token before emitting the first wake", async t => {
  t.mock.method(Date, "now", () => NOW);
  const old = { scope: process.env.PI_LOOP_SCOPE, loop: process.env.PI_LOOP };
  process.env.PI_LOOP_SCOPE = "session"; delete process.env.PI_LOOP;
  const dir = mkdtempSync(join(tmpdir(), "a08-runtime-"));
  const tools = new Map<string, any>(); const commands = new Map<string, any>(); const hooks = new Map<string, any[]>(); const listeners = new Map<string, any[]>();
  const journal: any[] = []; const messages: any[] = []; const fires: any[] = []; const pending: Promise<any>[] = [];
  const snapshot = () => journal.filter(e => e.customType === "herdr-loops.snapshot.v1").at(-1)?.data.snapshot as LoopStoreData;
  const ctx: any = { cwd: dir, mode: "rpc", hasUI: true, ui: { setStatus() {}, notify() {} }, isIdle: () => true, hasPendingMessages: () => false,
    sessionManager: { getSessionId: () => "a08", getEntries: () => journal, getBranch: () => [] } };
  loops({ registerTool: (tool: any) => tools.set(tool.name, tool), registerCommand: (name: string, command: any) => commands.set(name, command),
    on: (name: string, fn: any) => hooks.set(name, [...hooks.get(name) ?? [], fn]),
    appendEntry: (customType: string, data: any) => journal.push({ type: "custom", customType, data: structuredClone(data) }),
    sendMessage: (message: any) => messages.push(structuredClone(message)), exec: async () => assert.fail("no pane calls"),
    events: { on: (name: string, fn: any) => { const callbacks = listeners.get(name) ?? []; callbacks.push(fn); listeners.set(name, callbacks); return () => {}; },
      emit: (name: string, data: any) => {
        if (name === "loop:fire") {
          const saved = snapshot().loops.find(e => e.id === data.loopId)!;
          assert.equal(saved.fireCount, 1); assert.equal(saved.dynamic!.pendingWakeId, data.dynamic.pendingWakeId); fires.push(data);
        }
        for (const fn of listeners.get(name) ?? []) pending.push(Promise.resolve(fn(data)));
      } },
  } as any);
  const hook = async (name: string) => { for (const fn of hooks.get(name) ?? []) await fn({}, ctx); };
  try {
    await hook("session_start");
    await tools.get("LoopCreate").execute("create", { trigger: "idle", triggerType: "idle", prompt: "goal", maxFires: 1, readOnly: true });
    await Promise.all(pending);
    assert.equal(fires.length, 1); assert.equal(messages.length, 1);
    const wakeId = messages[0].details.dynamic.pendingWakeId; assert.ok(wakeId);
    assert.equal(snapshot().loops[0].status, "paused"); assert.match(messages[0].content, /Fire cap reached/);
    for (const gate of hooks.get("tool_call") ?? []) {
      assert.equal(await gate({ toolName: "LoopUpdate" }, ctx), undefined);
      assert.equal((await gate({ toolName: "bash" }, ctx))?.block, true);
    }
    await tools.get("LoopUpdate").execute("pause", { id: fires[0].loopId, wakeId, status: "paused" });
    await hook("agent_settled");
    await commands.get("loop").handler("a command-created goal", ctx); await Promise.all(pending);
    assert.equal(fires.length, 2); assert.notEqual(fires[1].dynamic.pendingWakeId, wakeId);
  } finally {
    await hook("session_shutdown");
    if (old.scope === undefined) delete process.env.PI_LOOP_SCOPE; else process.env.PI_LOOP_SCOPE = old.scope;
    if (old.loop === undefined) delete process.env.PI_LOOP; else process.env.PI_LOOP = old.loop;
    rmSync(dir, { recursive: true, force: true });
  }
});
