import type { LoopEntry } from "./types.js";

export function isOrdinaryDynamic(entry: LoopEntry): boolean {
  return entry.trigger.type === "dynamic" && !!entry.dynamic && !entry.workflow && !entry.orchestration && !entry.taskBacklog;
}

export function validWakeId(value: unknown): value is string {
  return typeof value === "string" && value.length > 0 && value.length <= 128 && !/\s/.test(value);
}

export function invalidDynamicWakeState(entry: LoopEntry): boolean {
  const dynamic = entry.dynamic!;
  if (dynamic.awaitingUpdate === true) return !validWakeId(dynamic.pendingWakeId);
  return (dynamic.awaitingUpdate !== false && dynamic.awaitingUpdate !== undefined) || dynamic.pendingWakeId !== undefined;
}

/** Legacy retirement reasons are recognized only when their original limit agrees. */
export function isFinalDynamicWake(entry: LoopEntry): boolean {
  if (entry.status !== "paused" || entry.pause?.kind !== "controller_limit") return false;
  const cap = !!entry.maxFires && (entry.fireCount ?? 0) >= entry.maxFires;
  if (entry.pause.retirementCause === "fire_cap") return cap;
  if (entry.pause.retirementCause === "one_shot") return !entry.recurring;
  return entry.pause.retirementCause === undefined
    && ["fire cap reached", "scheduler fire cap reached"].includes(entry.pause.reason ?? "") && (cap || !entry.recurring);
}

export function dynamicAckError(entry: LoopEntry, wakeId: string, status: "continue" | "paused" | "completed", now: number): string | undefined {
  if (!isOrdinaryDynamic(entry)) return "not an ordinary dynamic loop";
  if (now >= entry.expiresAt) return "loop has expired";
  if (entry.status !== "active" && !(status !== "continue" && isFinalDynamicWake(entry))) return "loop is paused; use /loop to resume";
  const dynamic = entry.dynamic!;
  if (invalidDynamicWakeState(entry)) return "invalid pending wake metadata; inspect LoopList and pause/resume through /loop";
  if (!dynamic.awaitingUpdate) return "not awaiting an update";
  if (!validWakeId(wakeId) || dynamic.pendingWakeId !== wakeId) return "stale wakeId";
  if (status === "continue" && entry.maxFires && (entry.fireCount ?? 0) >= entry.maxFires) return "fire cap reached";
  return undefined;
}
