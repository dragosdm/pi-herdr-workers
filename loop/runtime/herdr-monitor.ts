import { createHash } from "node:crypto";
import { setTimeout as delay } from "node:timers/promises";
import type { ExtensionAPI } from "@earendil-works/pi-coding-agent";

export type HerdrMonitorStatus = "running" | "idle" | "stopped" | "error" | "unknown";
export interface HerdrMonitor {
  id: string;
  key: string;
  command: string;
  cwd: string;
  description?: string;
  paneId: string;
  tabId: string;
  status: HerdrMonitorStatus;
  startedAt: number;
  reused: boolean;
  launchState?: "pending" | "submitted" | "uncertain";
}
export interface MonitorSnapshot { nextId: number; monitors: HerdrMonitor[] }
export type MonitorCreateResult = HerdrMonitor & { createAction: "submitted" | "attached" };
interface MonitorTiming {
  now: () => number;
  wait: (ms: number, signal: AbortSignal) => Promise<void>;
  deadline: (ms: number, expire: () => void) => () => void;
}
const defaultTiming: MonitorTiming = {
  now: () => performance.now(),
  wait: async (ms, signal) => { await delay(ms, undefined, { signal }); },
  deadline: (ms, expire) => { const timer = setTimeout(expire, ms); return () => clearTimeout(timer); },
};
const READY_MS = 5000;
export const commandKey = (command: string, cwd: string) => createHash("sha256").update(`${cwd}\0${command.trim()}`).digest("hex").slice(0, 12);
const clean = (text: string) => text.replace(/[\x00-\x1f\x7f-\x9f]/g, " ");
const message = (error: unknown) => error instanceof Error ? error.message : String(error);

// Also bound execution when a CLI implementation is slow to acknowledge cancellation.
function abortable<T>(work: Promise<T>, signal: AbortSignal): Promise<T> {
  return new Promise((resolve, reject) => {
    const abort = () => { cleanup(); reject(signal.reason); };
    const cleanup = () => signal.removeEventListener("abort", abort);
    work.then(value => { cleanup(); resolve(value); }, error => { cleanup(); reject(error); });
    signal.addEventListener("abort", abort, { once: true });
    if (signal.aborted) abort();
  });
}

export class HerdrMonitorManager {
  private byId = new Map<string, HerdrMonitor>();
  private nextId = 1;
  private mutation = Promise.resolve();
  private lifetime = new AbortController();
  private timing: MonitorTiming;
  dispose() { this.lifetime.abort(); this.onChange = undefined; }
  onChange?: (snapshot: MonitorSnapshot) => void;
  constructor(private exec: ExtensionAPI["exec"], timing: Partial<MonitorTiming> = {}) {
    this.timing = { ...defaultTiming, ...timing };
  }

  snapshot(): MonitorSnapshot { return structuredClone({ nextId: this.nextId, monitors: this.list() }); }
  restore(snapshot: MonitorSnapshot) {
    if (!Number.isSafeInteger(snapshot.nextId) || snapshot.nextId < 1 || !Array.isArray(snapshot.monitors)) throw new Error("Invalid monitor snapshot");
    this.nextId = snapshot.nextId;
    // Last observed process state is not proof it is still running or was submitted.
    this.byId = new Map(snapshot.monitors.filter((m) => typeof m.paneId === "string" && typeof m.key === "string")
      .map((m) => [m.id, { ...m, status: "unknown", ...(m.launchState === undefined ? {} : {
        launchState: ["pending", "submitted", "uncertain"].includes(m.launchState) ? m.launchState : "uncertain",
      }) } as HerdrMonitor]));
  }
  list(): HerdrMonitor[] { return [...this.byId.values()]; }
  get(id: string): HerdrMonitor | undefined { return this.byId.get(id); }
  private persist() { if (!this.lifetime.signal.aborted) this.onChange?.(this.snapshot()); }
  private signal(signal?: AbortSignal) { return signal ? AbortSignal.any([signal, this.lifetime.signal]) : this.lifetime.signal; }
  private async herdr(args: string[], signal?: AbortSignal, timeout = 15000): Promise<any> {
    if (process.env.HERDR_ENV !== "1" || !process.env.HERDR_WORKSPACE_ID) throw new Error("Monitors require a Herdr-managed pane.");
    signal = this.signal(signal);
    signal.throwIfAborted();
    const result = await abortable(this.exec("herdr", args, { timeout, signal }), signal);
    signal.throwIfAborted();
    if (result.code !== 0 || result.killed) throw new Error(clean(result.stderr || result.stdout || "Herdr request failed"));
    return result.stdout.trim() ? JSON.parse(result.stdout) : {};
  }
  private serialize<T>(fn: () => Promise<T>, signal?: AbortSignal): Promise<T> {
    const next = this.mutation.then(() => { signal?.throwIfAborted(); return fn(); });
    this.mutation = next.then(() => {}, () => {});
    return signal ? abortable(next, signal) : next;
  }
  create(command: string, description?: string, cwd = process.cwd(), signal?: AbortSignal): Promise<MonitorCreateResult> {
    signal = this.signal(signal);
    // Queue cancellation must not bypass the detailed retained-pane error of an active call.
    let entered = false;
    const queued = this.serialize<MonitorCreateResult>(async () => {
      entered = true;
      let monitor: HerdrMonitor | undefined;
      let attempted = false;
      let priorLaunch: HerdrMonitor["launchState"];
      try {
        signal.throwIfAborted();
        if (!command.trim()) throw new Error("Monitor command cannot be empty.");
        const key = commandKey(command, cwd);
        const tab = await this.ensureMonitorTab(cwd, signal);
        const panes = tab.created ? [] : await this.panes(tab.tabId, signal);
        const existing = panes.find((p: any) => p.label?.startsWith(`mon:${key} `));
        const previous = this.list().find((m) => m.key === key && m.paneId === existing?.pane_id);
        let paneId: string;
        if (existing) paneId = existing.pane_id;
        else {
          if (this.list().filter((m) => m.status === "running" || m.status === "unknown").length >= 25) throw new Error("25 monitors tracked; inspect MonitorList before starting more.");
          const root = panes[0];
          if (tab.created) paneId = tab.rootPaneId;
          else if (panes.length === 1 && !root.label && await this.foreground(root.pane_id, signal) === "ready") paneId = root.pane_id;
          else {
            const from = panes.at(-1)?.pane_id || tab.rootPaneId;
            const split = await this.herdr(["pane", "split", "--pane", from, "--direction", "down", "--cwd", cwd, "--no-focus"], signal);
            paneId = split?.result?.pane?.pane_id;
            if (!paneId) throw new Error("Herdr split returned no pane ID.");
          }
        }
        monitor = previous || {
          id: String(this.nextId++), key, command, cwd, description, paneId, tabId: tab.tabId,
          status: "unknown", startedAt: Date.now(), reused: !!existing,
          ...(!existing ? { launchState: "pending" as const } : {}),
        };
        priorLaunch = monitor.launchState;
        monitor.reused = !!existing;
        this.byId.set(monitor.id, monitor);
        this.persist(); // Save the pane handle even if rename or readiness fails.
        if (!existing) await this.herdr(["pane", "rename", paneId, `mon:${key} ${clean(command).slice(0, 40)}`], signal);
        let deadline: number | undefined;
        if (monitor.launchState === "pending") deadline = await this.waitForShell(monitor, signal);
        else {
          if (await this.paneIsBusy(paneId, signal)) {
            monitor.status = "running";
            this.persist();
            return { ...monitor, createAction: "attached" };
          }
          if (!(await this.ownsPane(monitor, signal))) throw new Error("Pane label changed; ownership could not be verified");
        }
        signal.throwIfAborted();
        if (deadline !== undefined && this.timing.now() >= deadline) throw new Error(this.readinessTimeout());
        monitor.launchState = "uncertain";
        this.persist(); // Append before input; failed persistence must prevent submission.
        signal.throwIfAborted();
        if (deadline !== undefined && this.timing.now() >= deadline) throw new Error(this.readinessTimeout());
        const quote = (s: string) => `'${s.replace(/'/g, `'"'"'`)}'`;
        attempted = true;
        await this.herdr(["pane", "run", paneId, `cd -- ${quote(cwd)} && ${command}`], signal);
        monitor.launchState = "submitted";
        monitor.status = "running";
        monitor.startedAt = Date.now();
        this.persist();
        return { ...monitor, createAction: "submitted" };
      } catch (error) {
        let persistenceError: unknown;
        if (monitor) {
          monitor.launchState = attempted ? "uncertain" : priorLaunch;
          monitor.status = "unknown";
          try { this.persist(); } catch (failure) { persistenceError = failure; }
        }
        const detail = `${message(error)}${persistenceError ? `; saving failure state also failed: ${message(persistenceError)}` : ""}`;
        if (attempted) throw new Error(`Monitor #${monitor!.id} submission may have happened; inspect pane ${monitor!.paneId} before retrying. ${detail}`, { cause: error });
        throw new Error(monitor
          ? `Monitor #${monitor.id} command not submitted: ${detail}; inspect pane ${monitor.paneId} before retrying.`
          : `Monitor command not submitted: ${detail}`, { cause: error });
      }
    });
    // Reject promptly while queued, but let active work report whether input was attempted.
    return new Promise((resolve, reject) => {
      const abort = () => { if (!entered) reject(new Error("Monitor command not submitted: operation cancelled while queued.")); };
      signal.addEventListener("abort", abort, { once: true });
      if (signal.aborted) abort();
      queued.then(resolve, reject).finally(() => signal.removeEventListener("abort", abort));
    });
  }
  private readinessTimeout() { return `shell readiness timed out after ${READY_MS} ms`; }
  private async waitForShell(m: HerdrMonitor, signal: AbortSignal): Promise<number> {
    const deadline = this.timing.now() + READY_MS;
    const timer = new AbortController();
    const cancelTimer = this.timing.deadline(READY_MS, () => timer.abort(new Error(this.readinessTimeout())));
    const bounded = AbortSignal.any([signal, timer.signal]);
    const remaining = () => {
      bounded.throwIfAborted();
      const ms = deadline - this.timing.now();
      if (ms <= 0) throw new Error(this.readinessTimeout());
      return Math.min(15000, ms);
    };
    try {
      while (true) {
        const state = await this.foreground(m.paneId, bounded, remaining());
        remaining(); // A delayed ready observation cannot authorize late input.
        if (state === "ready") {
          if (!(await this.ownsPane(m, bounded, remaining()))) throw new Error("Pane label changed; ownership could not be verified");
          remaining();
          return deadline;
        }
        await abortable(this.timing.wait(Math.min(100, remaining()), bounded), bounded);
      }
    } finally { cancelTimer(); }
  }
  private async ownsPane(m: HerdrMonitor, signal?: AbortSignal, timeout?: number) {
    const got = await this.herdr(["pane", "get", m.paneId], signal, timeout);
    return got?.result?.pane?.label?.startsWith(`mon:${m.key} `) === true;
  }
  async refresh(m: HerdrMonitor, signal?: AbortSignal): Promise<HerdrMonitor> {
    signal = this.signal(signal);
    const old = m.status;
    const launch = m.launchState;
    let status: HerdrMonitorStatus;
    try {
      if (!(await this.ownsPane(m, signal))) status = "error";
      else if (launch === "pending") status = "unknown";
      else status = await this.paneIsBusy(m.paneId, signal) ? "running" : old === "stopped" ? "stopped" : "idle";
    } catch { signal.throwIfAborted(); status = "error"; }
    // A list observation begun before submission must not overwrite its newer result.
    if (m.launchState === launch && m.status === old) {
      m.status = status;
      if (status !== old) this.persist();
    }
    return m;
  }
  async readTail(paneId: string, lines = 5, signal?: AbortSignal): Promise<string[]> {
    const raw = await this.herdr(["pane", "read", paneId, "--source", "recent-unwrapped", "--lines", String(lines)], signal);
    const result = raw.result ?? {};
    const text = result.text ?? result.output ?? JSON.stringify(result);
    return String(text).split("\n").filter(Boolean).slice(-lines);
  }
  stop(id: string, signal?: AbortSignal): Promise<boolean> {
    signal = this.signal(signal);
    if (this.get(id)?.launchState === "pending") return Promise.resolve(false);
    return this.serialize(async () => {
      const m = this.get(id);
      if (!m || m.launchState === "pending" || (await this.refresh(m, signal)).status !== "running") return false;
      await this.herdr(["pane", "send-keys", m.paneId, "ctrl+c"], signal);
      m.status = "stopped";
      this.persist();
      return true;
    }, signal);
  }
  private async panes(tabId: string, signal?: AbortSignal): Promise<any[]> {
    const raw = await this.herdr(["pane", "list", "--workspace", process.env.HERDR_WORKSPACE_ID!], signal);
    return (raw?.result?.panes ?? []).filter((p: any) => p.tab_id === tabId);
  }
  private async ensureMonitorTab(cwd: string, signal?: AbortSignal): Promise<{ tabId: string; rootPaneId: string; created: boolean }> {
    const workspace = process.env.HERDR_WORKSPACE_ID!;
    const raw = await this.herdr(["tab", "list", "--workspace", workspace], signal);
    const tab = raw?.result?.tabs?.find((t: any) => t.label === "Monitor");
    if (tab) {
      const panes = await this.panes(tab.tab_id, signal);
      if (!panes[0]) throw new Error("Monitor tab has no available root pane.");
      return { tabId: tab.tab_id, rootPaneId: panes[0].pane_id, created: false };
    }
    const made = await this.herdr(["tab", "create", "--workspace", workspace, "--label", "Monitor", "--cwd", cwd, "--no-focus"], signal);
    const tabId = made?.result?.tab?.tab_id;
    const rootPaneId = made?.result?.root_pane?.pane_id;
    if (!tabId || !rootPaneId) throw new Error("Herdr tab create returned no IDs.");
    return { tabId, rootPaneId, created: true };
  }
  private async foreground(paneId: string, signal?: AbortSignal, timeout?: number): Promise<"ready" | "busy" | "unknown"> {
    const raw = await this.herdr(["pane", "process-info", "--pane", paneId], signal, timeout);
    const info = raw?.result?.process_info;
    const foreground = info?.foreground_processes;
    const pid = (value: unknown) => Number.isSafeInteger(value) && (value as number) > 0;
    if (!pid(info?.shell_pid) || !Array.isArray(foreground) || !foreground.length ||
      foreground.some((p: any) => !p || !pid(p.pid) || typeof p.name !== "string" || !p.name)) return "unknown";
    return foreground.every((p: any) => p.pid === info.shell_pid && ["bash", "zsh", "fish", "sh"].includes(p.name)) ? "ready" : "busy";
  }
  private async paneIsBusy(paneId: string, signal?: AbortSignal): Promise<boolean> {
    const state = await this.foreground(paneId, signal);
    if (state === "unknown") throw new Error(`Cannot establish shell readiness for ${paneId}`);
    return state === "busy";
  }
}
