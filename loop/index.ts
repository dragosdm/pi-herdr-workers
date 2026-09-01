/**
 * /loop + Herdr MonitorCreate/List/Stop. Loaded as part of pi-herdr-workers.
 */
import { existsSync } from "node:fs";
import type { ExtensionAPI, ExtensionContext } from "@earendil-works/pi-coding-agent";
import { registerLoopCommand } from "./commands/loop-command.js";
import { atMaxFires } from "./loop-reducer.js";
import type { ReducerNotification } from "./notification-reducer.js";
import { buildLoopExpiredPayload } from "./runtime/loop-events.js";
import { createNotificationRuntime, type LoopFireEvent } from "./runtime/notification-runtime.js";
import { resolveLoopStorePath } from "./runtime/scope.js";
import { registerSessionRuntimeHooks } from "./runtime/session-runtime.js";
import { HerdrMonitorManager, type MonitorSnapshot } from "./runtime/herdr-monitor.js";
import { CronScheduler } from "./scheduler.js";
import { LoopStore } from "./store.js";
import { registerLoopTools } from "./tools/loop-tools.js";
import { registerMonitorTools } from "./tools/monitor-tools.js";
import { TriggerSystem } from "./trigger-system.js";
import type { LoopStoreData, LoopEntry, LoopExpiryDisposition, LoopExpiryReason, LoopExpirySource, LoopFireOrigin } from "./types.js";
import { LoopWidget } from "./ui/widget.js";
import { atWorkflowStateFireLimit, getActiveWorkflowStateLoop, isTerminalWorkflowRun } from "./workflow-reducer.js";

const DEBUG = !!process.env.PI_LOOP_DEBUG;
function debug(...args: unknown[]) {
  if (DEBUG) console.error("[pi-herdr-workers]", ...args);
}

export default function (pi: ExtensionAPI) {
  const LOOP_LOG = "herdr-loops.snapshot.v1";
  let running = true;
  const piLoopEnv = process.env.PI_LOOP;
  const piLoopScope = process.env.PI_LOOP_SCOPE as "memory" | "session" | "project" | undefined;
  let loopScope: "memory" | "session" | "project" = piLoopScope ?? "session";
  let sessionGeneration = 0;
  let _latestCtx: ExtensionContext | undefined;
  let _sessionId: string | undefined;

  const getScopeOptions = () => ({ piLoopEnv, loopScope });

  // No file I/O or background work in the extension factory.
  let store = new LoopStore();
  let scheduler: CronScheduler;
  let triggerSystem: TriggerSystem;
  const monitors = new HerdrMonitorManager(pi.exec.bind(pi));
  const widget = new LoopWidget(store, monitors, (id) => scheduler?.nextFire(id));

  function createScheduler(loopStore: LoopStore): CronScheduler {
    return new CronScheduler(
      loopStore,
      (entry, origin) => onLoopFire(entry, origin),
      (entry, disposition) => emitLoopExpired(entry, disposition, "scheduler", "expires_at"),
      isCurrentExtensionContext,
    );
  }

  scheduler = createScheduler(store);
  triggerSystem = new TriggerSystem(pi, scheduler, store, (entry, origin) => onLoopFire(entry, origin));

  const MONITOR_LOG = "herdr-monitors.snapshot.v1";
  const monitorScope = `${process.env.HERDR_SOCKET_PATH || ""}:${process.env.HERDR_WORKSPACE_ID || ""}`;
  const PENDING_LOG = "herdr-loops.pending.v1";
  let readOnlyWake = false;
  const notificationRuntime = createNotificationRuntime({
    pi,
    hasPendingTasks: async () => -1,
    cleanDoneTasks: async () => {},
    getHasPendingMessages: () => _latestCtx?.hasPendingMessages() ?? false,
    debug,
    onPendingChanged: (pending) => {
      if (running && _latestCtx && loopScope !== "memory" && piLoopEnv !== "off") {
        pi.appendEntry(PENDING_LOG, { sessionId: _latestCtx.sessionManager.getSessionId(), pending });
      }
    },
    onLoopNotificationDelivered: (data) => { readOnlyWake = data.readOnly === true; },
  });

  pi.on("session_start", async (_event, ctx) => {
    _latestCtx = ctx;
    const entries = ctx.sessionManager.getEntries();
    const monitorState = entries.filter((e) => e.type === "custom" && e.customType === MONITOR_LOG)
      .map((e) => e.type === "custom" ? e.data as { sessionId: string; scope: string; snapshot: MonitorSnapshot } : undefined)
      .filter((d) => d?.sessionId === ctx.sessionManager.getSessionId() && d.scope === monitorScope).at(-1);
    if (monitorState) monitors.restore(monitorState.snapshot);
    monitors.onChange = (snapshot) => {
      if (!running) return;
      pi.appendEntry(MONITOR_LOG, { sessionId: ctx.sessionManager.getSessionId(), scope: monitorScope, snapshot });
      widget.update();
    };
    const saved = entries.filter((e) => e.type === "custom" && e.customType === PENDING_LOG)
      .map((e) => e.type === "custom" ? e.data as { sessionId: string; pending: ReducerNotification[] } : undefined)
      .filter((d) => d?.sessionId === ctx.sessionManager.getSessionId()).at(-1);
    const delivered = new Set(entries.filter((e) => e.type === "custom_message" && e.customType === "pi-loop")
      .map((e) => e.type === "custom_message" ? (e.details as { deliveryKey?: string })?.deliveryKey : undefined));
    if (saved && Array.isArray(saved.pending)) notificationRuntime.restore(saved.pending.filter((n) => !delivered.has(`${n.key}:${n.timestamp}`)));
    if (!ctx.isIdle()) {
      // Reload during a read-only wake must retain the tool gate.
      const lastWake = ctx.sessionManager.getBranch().filter((e) => e.type === "custom_message" && e.customType === "pi-loop").at(-1);
      readOnlyWake = lastWake?.type === "custom_message" && (lastWake.details as { readOnly?: boolean })?.readOnly === true;
    }
  });
  function acknowledgePersistedWakes(ctx: ExtensionContext) {
    // message_end hooks run BEFORE SessionManager persistence in pi 0.84.
    for (const entry of ctx.sessionManager.getEntries()) {
      if (entry.type !== "custom_message" || entry.customType !== "pi-loop") continue;
      const key = (entry.details as { deliveryKey?: string })?.deliveryKey;
      if (key) notificationRuntime.acknowledge(key);
    }
  }
  pi.on("context", (_event, ctx) => acknowledgePersistedWakes(ctx));
  pi.on("agent_settled", (_event, ctx) => acknowledgePersistedWakes(ctx));
  // Clear before the settled handler below delivers the next queued wake.
  pi.on("agent_settled", (_event, ctx) => { if (ctx.isIdle()) readOnlyWake = false; });
  pi.on("tool_call", (event) => {
    const allowed = new Set(["read", "grep", "find", "ls", "LoopList", "LoopUpdate", "LoopDelete", "MonitorList"]);
    if (readOnlyWake && !allowed.has(event.toolName)) return { block: true, reason: `Read-only loop wake: ${event.toolName} is not allowed.` };
  });

  // Lifecycle hooks and the extension event bus are distinct APIs. Bridge advertised sources.
  pi.on("tool_execution_start", (event) => { if (!event.toolName.startsWith("Loop")) pi.events.emit("tool_execution_start", event); });
  pi.on("tool_execution_end", (event) => { if (!event.toolName.startsWith("Loop")) pi.events.emit("tool_execution_end", event); });
  pi.on("turn_start", (event) => pi.events.emit("turn_start", event));
  pi.on("turn_end", (event) => pi.events.emit("turn_end", event));
  pi.on("agent_settled", (event) => pi.events.emit("agent_settled", event));

  function isCurrentExtensionContext(): boolean {
    return running;
  }

  function configureStore(sessionId: string) {
    const ctx = _latestCtx!;
    const explicitSharedStore = loopScope === "project" || (piLoopEnv && piLoopEnv !== "off");
    if (explicitSharedStore) {
      store = new LoopStore(resolveLoopStorePath({ ...getScopeOptions(), cwd: ctx.cwd }, sessionId));
    } else {
      store = new LoopStore();
      if (loopScope !== "memory" && piLoopEnv !== "off") {
        // Operational facts are session-wide, not rewound by /tree. Forks do not inherit active controllers.
        const saved = ctx.sessionManager.getEntries().filter((entry) => entry.type === "custom" && entry.customType === LOOP_LOG)
          .map((entry) => entry.type === "custom" ? entry.data as { sessionId: string; snapshot: LoopStoreData } : undefined)
          .filter((data) => data?.sessionId === sessionId).at(-1);
        if (saved) store.restoreSnapshot(saved.snapshot);
        else {
          const legacy = resolveLoopStorePath({ ...getScopeOptions(), cwd: ctx.cwd }, sessionId);
          if (legacy && existsSync(legacy)) {
            store.restoreSnapshot(new LoopStore(legacy).snapshot());
            pi.appendEntry(LOOP_LOG, { sessionId, snapshot: store.snapshot(), migrated: true });
          }
        }
      }
    }
    if (loopScope !== "memory" && piLoopEnv !== "off") {
      store.onChange = (snapshot) => {
        if (!running) throw new Error("Loop runtime closed");
        pi.appendEntry(LOOP_LOG, { sessionId, snapshot });
        widget.update();
      };
    }
    widget.setStore(store);
    scheduler = createScheduler(store);
    triggerSystem = new TriggerSystem(pi, scheduler, store, (entry, origin) => onLoopFire(entry, origin));
  }

  pi.on("session_shutdown", () => { running = false; store.onChange = undefined; monitors.dispose(); widget.dispose(); });

  function emitLoopExpired(
    entry: LoopEntry,
    disposition: LoopExpiryDisposition,
    source: LoopExpirySource,
    reason: LoopExpiryReason,
    generation = sessionGeneration,
  ): void {
    if (generation !== sessionGeneration || !isCurrentExtensionContext()) return;
    triggerSystem.remove(entry.id);
    const payload = buildLoopExpiredPayload(entry, disposition, source, reason, Date.now());
    try {
      pi.events.emit("loops:expired", payload);
    } catch (error) {
      debug(`loops:expired #${entry.id} — event listener failed`, error);
    }
    void notificationRuntime.queueOrDeliverLoopExpired({ ...payload, sessionGeneration: generation })
      .catch((error) => debug(`loops:expired #${entry.id} — notification failed`, error));
  }

  function emitLoopFire(entry: LoopEntry): void {
    pi.events.emit("loop:fire", {
      loopId: entry.id,
      prompt: entry.prompt,
      trigger: entry.trigger,
      timestamp: Date.now(),
      readOnly: entry.readOnly,
      recurring: entry.recurring,
      persistent: entry.recurring,
      dynamic: entry.dynamic,
      fireLimitReached: atMaxFires(entry),
      workflowStateFireLimitReached: !!entry.workflow && atWorkflowStateFireLimit(entry.workflow),
      sessionGeneration,
    });
  }

  function onLoopFire(entry: LoopEntry, origin: LoopFireOrigin = "dynamic"): void {
    if (!isCurrentExtensionContext()) return;
    debug(`loop:fire #${entry.id}`, { prompt: entry.prompt.slice(0, 50) });
    const current = store.get(entry.id);
    if (current?.status !== "active" || isTerminalWorkflowRun(current?.workflow)) {
      triggerSystem.remove(entry.id);
      return;
    }
    if (current.workflow?.waitingMonitor) return;

    if (atMaxFires(current)) {
      triggerSystem.remove(current.id);
      if (current.dynamic) store.pause(current.id, "controller_limit", "fire cap reached");
      else store.delete(current.id);
      widget.update();
      return;
    }
    const fired = store.fire(current.id, origin);
    if (!fired) return;

    const firedAt = Date.now();
    const stateLoop = fired.workflow && getActiveWorkflowStateLoop(fired.workflow);
    const updatedEntry = fired.trigger.type === "dynamic" && !stateLoop
      ? store.updateDynamic(fired.id, {
          dynamic: {
            awaitingUpdate: true,
            nextWakeAt: undefined,
            lastUpdatedAt: firedAt,
          },
        }) ?? fired
      : fired;
    const firedEntry = { ...updatedEntry, prompt: entry.prompt };

    if (atMaxFires(firedEntry)) {
      triggerSystem.remove(firedEntry.id);
      if (firedEntry.dynamic) store.pause(firedEntry.id, "controller_limit", "fire cap reached");
      else store.delete(firedEntry.id);
      widget.update();
    }

    emitLoopFire(firedEntry);
  }

  registerSessionRuntimeHooks({
    pi,
    getLoopScope: () => loopScope,
    getPiLoopEnv: () => piLoopEnv,
    getSessionGeneration: () => sessionGeneration,
    advanceSessionGeneration: () => ++sessionGeneration,
    recreateSessionStore: configureStore,
    clearAllLoops: () => {
      store.clearAll();
    },
    getStore: () => store,
    getScheduler: () => scheduler,
    getTriggerSystem: () => triggerSystem,
    setLatestCtx: (ctx) => {
      _latestCtx = ctx;
    },
    setSessionId: (sessionId) => {
      _sessionId = sessionId;
    },
    widget,
    notificationRuntime,
    flushPendingNotifications: notificationRuntime.flushPendingNotifications,
    migrateTaskBacklogLoops: () => 0,
    cleanupTaskBacklogLoops: async () => 0,
    adoptTaskBacklogLoops: async () => 0,
    releaseTaskBacklogWakes: () => {},
    clearWorkflowMonitorWaits: () => {},
    recoverOrchestrations: async () => {},
    pumpOrchestrations: async () => {},
    shutdownOrchestrations: async () => {},
    shutdownMonitors: async () => { monitors.dispose(); },
    hasPendingTasks: async () => -1,
    cleanDoneTasks: async () => {},
    isContextCurrent: isCurrentExtensionContext,
    emitLoopExpired: (entry, disposition, reason, generation) => {
      emitLoopExpired(entry, disposition, "session_recovery", reason, generation);
    },
  });

  pi.events.on("loop:fire", async (event: unknown) => {
    await notificationRuntime.queueOrDeliverNotification(event as LoopFireEvent);
  });

  registerLoopTools({
    pi,
    getStore: () => store,
    getTriggerSystem: () => triggerSystem,
    getScheduler: () => scheduler,
    getMonitorManager: () => ({ get: (id: string) => monitors.get(id) }),
    updateWidget: () => widget.update(),
    onDynamicLoopActivated: (entry) => {
      onLoopFire(entry);
    },
  });

  registerMonitorTools({
    pi,
    getMonitors: () => monitors,
    updateWidget: () => widget.update(),
  });

  pi.on("session_start", async (_event, ctx) => {
    // After runtime restoration, recover queued-but-not-delivered wakes. Never replay delivered ones.
    if (ctx.isIdle() && (ctx.mode === "tui" || ctx.mode === "rpc")) await notificationRuntime.flushPendingNotifications();
  });

  registerLoopCommand({
    pi,
    getStore: () => store,
    getTriggerSystem: () => triggerSystem,
    updateWidget: () => widget.update(),
    onDynamicLoopActivated: (entry) => {
      onLoopFire(entry);
    },
  });
}
