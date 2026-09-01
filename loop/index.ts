/**
 * /loop + Herdr MonitorCreate/List/Stop. Loaded as part of pi-herdr-workers.
 */
import { randomUUID } from "node:crypto";
import type { ExtensionAPI, ExtensionContext } from "@earendil-works/pi-coding-agent";
import { registerLoopCommand } from "./commands/loop-command.js";
import { atMaxFires } from "./loop-reducer.js";
import { buildLoopExpiredPayload } from "./runtime/loop-events.js";
import { createNotificationRuntime, type LoopFireEvent } from "./runtime/notification-runtime.js";
import { resolveLoopStorePath } from "./runtime/scope.js";
import { registerSessionRuntimeHooks } from "./runtime/session-runtime.js";
import { isStaleExtensionContextError } from "./runtime/stale-context.js";
import { HerdrMonitorManager } from "./runtime/herdr-monitor.js";
import { CronScheduler } from "./scheduler.js";
import { LoopStore } from "./store.js";
import { registerLoopTools } from "./tools/loop-tools.js";
import { registerMonitorTools } from "./tools/monitor-tools.js";
import { TriggerSystem } from "./trigger-system.js";
import type { LoopEntry, LoopExpiryDisposition, LoopExpiryReason, LoopExpirySource, LoopFireOrigin } from "./types.js";
import { LoopWidget } from "./ui/widget.js";
import { atWorkflowStateFireLimit, getActiveWorkflowStateLoop, isTerminalWorkflowRun } from "./workflow-reducer.js";

const DEBUG = !!process.env.PI_LOOP_DEBUG;
function debug(...args: unknown[]) {
  if (DEBUG) console.error("[pi-herdr-workers]", ...args);
}

export default function (pi: ExtensionAPI) {
  const runtimeId = randomUUID();
  const runtimeProbeEvent = `pi-loop:runtime-probe:${runtimeId}`;
  const piLoopEnv = process.env.PI_LOOP;
  const piLoopScope = process.env.PI_LOOP_SCOPE as "memory" | "session" | "project" | undefined;
  let loopScope: "memory" | "session" | "project" = piLoopScope ?? "session";
  let sessionGeneration = 0;
  let _latestCtx: ExtensionContext | undefined;
  let _sessionId: string | undefined;

  const getScopeOptions = () => ({ piLoopEnv, loopScope });

  let store = new LoopStore(resolveLoopStorePath(getScopeOptions()));
  const memoryLoopStores = new Map<string, LoopStore>();
  let scheduler: CronScheduler;
  let triggerSystem: TriggerSystem;
  const monitors = new HerdrMonitorManager();
  const widget = new LoopWidget(store, monitors);

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

  const notificationRuntime = createNotificationRuntime({
    pi,
    hasPendingTasks: async () => -1,
    cleanDoneTasks: async () => {},
    getHasPendingMessages: () => _latestCtx?.hasPendingMessages() ?? false,
    debug,
  });

  function isCurrentExtensionContext(): boolean {
    try {
      pi.events.emit(runtimeProbeEvent, { runtimeId, sessionGeneration });
      return true;
    } catch (error) {
      if (!isStaleExtensionContextError(error)) throw error;
      debug("extension context went stale, dropping runtime callback");
      return false;
    }
  }

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
      store.delete(current.id);
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
      store.delete(firedEntry.id);
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
    recreateSessionStore: (sessionId: string) => {
      const path = resolveLoopStorePath(getScopeOptions(), sessionId);
      if (path) store = new LoopStore(path);
      else {
        store = memoryLoopStores.get(sessionId) ?? new LoopStore();
        memoryLoopStores.set(sessionId, store);
      }
      widget.setStore(store);
      scheduler = createScheduler(store);
      triggerSystem = new TriggerSystem(pi, scheduler, store, (entry, origin) => onLoopFire(entry, origin));
    },
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
    shutdownMonitors: async () => {},
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
    maybeBootstrapTaskLoop: async () => false,
    isTaskSystemReady: () => false,
    onDynamicLoopActivated: (entry) => {
      onLoopFire(entry);
    },
  });

  registerMonitorTools({
    pi,
    getMonitors: () => monitors,
    updateWidget: () => widget.update(),
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
