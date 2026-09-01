import { createHash } from "node:crypto";
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
}
export interface MonitorSnapshot { nextId: number; monitors: HerdrMonitor[] }
export const commandKey = (command: string, cwd: string) => createHash("sha256").update(`${cwd}\0${command.trim()}`).digest("hex").slice(0, 12);
const clean = (text: string) => text.replace(/[\x00-\x1f\x7f-\x9f]/g, " ");

export class HerdrMonitorManager {
  private byId = new Map<string, HerdrMonitor>();
  private nextId = 1;
  private mutation = Promise.resolve();
  private lifetime = new AbortController();
  dispose() { this.lifetime.abort(); this.onChange = undefined; }
  onChange?: (snapshot: MonitorSnapshot) => void;
  constructor(private exec: ExtensionAPI["exec"]) {}

  snapshot(): MonitorSnapshot { return structuredClone({ nextId: this.nextId, monitors: this.list() }); }
  restore(snapshot: MonitorSnapshot) {
    if (!Number.isSafeInteger(snapshot.nextId) || snapshot.nextId < 1 || !Array.isArray(snapshot.monitors)) throw new Error("Invalid monitor snapshot");
    this.nextId = snapshot.nextId;
    // Last observed process state is not proof it is still running.
    this.byId = new Map(snapshot.monitors.filter((m) => typeof m.paneId === "string" && typeof m.key === "string")
      .map((m) => [m.id, { ...m, status: "unknown" }]));
  }
  list(): HerdrMonitor[] { return [...this.byId.values()]; }
  get(id: string): HerdrMonitor | undefined { return this.byId.get(id); }
  private persist() { this.onChange?.(this.snapshot()); }
  private async herdr(args: string[], signal?: AbortSignal): Promise<any> {
    if (process.env.HERDR_ENV !== "1" || !process.env.HERDR_WORKSPACE_ID) throw new Error("Monitors require a Herdr-managed pane.");
    signal = signal ? AbortSignal.any([signal, this.lifetime.signal]) : this.lifetime.signal;
    signal.throwIfAborted();
    const result = await this.exec("herdr", args, { timeout: 15000, signal });
    signal?.throwIfAborted();
    if (result.code !== 0 || result.killed) throw new Error(clean(result.stderr || result.stdout || "Herdr request failed"));
    return result.stdout.trim() ? JSON.parse(result.stdout) : {};
  }
  private serialize<T>(fn: () => Promise<T>): Promise<T> {
    const next = this.mutation.then(fn);
    this.mutation = next.then(() => {}, () => {});
    return next;
  }
  create(command: string, description?: string, cwd = process.cwd(), signal?: AbortSignal): Promise<HerdrMonitor> {
    return this.serialize(async () => {
      signal?.throwIfAborted();
      if (!command.trim()) throw new Error("Monitor command cannot be empty.");
      const key = commandKey(command, cwd);
      const tab = await this.ensureMonitorTab(cwd, signal);
      const panes = await this.panes(tab.tabId, signal);
      const existing = panes.find((p: any) => p.label?.startsWith(`mon:${key} `));
      const previous = this.list().find((m) => m.key === key && m.paneId === existing?.pane_id);
      let paneId: string;
      if (existing) paneId = existing.pane_id;
      else {
        if (this.list().filter((m) => m.status === "running" || m.status === "unknown").length >= 25) throw new Error("25 monitors tracked; inspect MonitorList before starting more.");
        const root = panes[0];
        if (panes.length === 1 && !root.label && !(await this.paneIsBusy(root.pane_id, signal))) paneId = root.pane_id;
        else {
          const from = panes.at(-1)?.pane_id || tab.rootPaneId;
          const split = await this.herdr(["pane", "split", "--pane", from, "--direction", "down", "--cwd", cwd, "--no-focus"], signal);
          paneId = split?.result?.pane?.pane_id;
          if (!paneId) throw new Error("Herdr split returned no pane ID.");
        }
        await this.herdr(["pane", "rename", paneId, `mon:${key} ${clean(command).slice(0, 40)}`], signal);
      }
      const monitor: HerdrMonitor = previous || {
        id: String(this.nextId++), key, command, cwd, description, paneId, tabId: tab.tabId,
        status: "unknown", startedAt: Date.now(), reused: !!existing,
      };
      monitor.reused = !!existing;
      this.byId.set(monitor.id, monitor);
      this.persist(); // Keep a recoverable handle BEFORE running anything.
      if (await this.paneIsBusy(paneId, signal)) {
        monitor.status = "running";
        this.persist();
        return monitor; // Attach; NEVER interrupt an existing process on create/recovery.
      }
      try {
        const quote = (s: string) => `'${s.replace(/'/g, `'"'"'`)}'`;
        await this.herdr(["pane", "run", paneId, `cd -- ${quote(cwd)} && ${command}`], signal);
        monitor.status = "running";
        monitor.startedAt = Date.now();
        this.persist();
        return monitor;
      } catch (error) {
        monitor.status = "unknown"; // The CLI may have sent input before timing out.
        this.persist();
        throw new Error(`Monitor pane ${paneId} needs inspection before retrying`, { cause: error });
      }
    });
  }
  private async ownsPane(m: HerdrMonitor, signal?: AbortSignal) {
    const got = await this.herdr(["pane", "get", m.paneId], signal);
    return got?.result?.pane?.label?.startsWith(`mon:${m.key} `) === true;
  }
  async refresh(m: HerdrMonitor, signal?: AbortSignal): Promise<HerdrMonitor> {
    const old = m.status;
    try {
      if (!(await this.ownsPane(m, signal))) m.status = "error";
      else m.status = await this.paneIsBusy(m.paneId, signal) ? "running" : old === "stopped" ? "stopped" : "idle";
    } catch { signal?.throwIfAborted(); m.status = "error"; }
    if (m.status !== old) this.persist();
    return m;
  }
  async readTail(paneId: string, lines = 5, signal?: AbortSignal): Promise<string[]> {
    const raw = await this.herdr(["pane", "read", paneId, "--source", "recent-unwrapped", "--lines", String(lines)], signal);
    const result = raw.result ?? {};
    const text = result.text ?? result.output ?? JSON.stringify(result);
    return String(text).split("\n").filter(Boolean).slice(-lines);
  }
  stop(id: string, signal?: AbortSignal): Promise<boolean> {
    return this.serialize(async () => {
      const m = this.get(id);
      if (!m || (await this.refresh(m, signal)).status !== "running") return false;
      await this.herdr(["pane", "send-keys", m.paneId, "ctrl+c"], signal);
      m.status = "stopped";
      this.persist();
      return true;
    });
  }
  private async panes(tabId: string, signal?: AbortSignal): Promise<any[]> {
    const raw = await this.herdr(["pane", "list", "--workspace", process.env.HERDR_WORKSPACE_ID!], signal);
    return (raw?.result?.panes ?? []).filter((p: any) => p.tab_id === tabId);
  }
  private async ensureMonitorTab(cwd: string, signal?: AbortSignal): Promise<{ tabId: string; rootPaneId: string }> {
    const workspace = process.env.HERDR_WORKSPACE_ID!;
    const raw = await this.herdr(["tab", "list", "--workspace", workspace], signal);
    const tab = raw?.result?.tabs?.find((t: any) => t.label === "Monitor");
    if (tab) {
      const panes = await this.panes(tab.tab_id, signal);
      if (!panes[0]) throw new Error("Monitor tab has no available root pane.");
      return { tabId: tab.tab_id, rootPaneId: panes[0].pane_id };
    }
    const made = await this.herdr(["tab", "create", "--workspace", workspace, "--label", "Monitor", "--cwd", cwd, "--no-focus"], signal);
    const tabId = made?.result?.tab?.tab_id;
    const rootPaneId = made?.result?.root_pane?.pane_id;
    if (!tabId || !rootPaneId) throw new Error("Herdr tab create returned no IDs.");
    return { tabId, rootPaneId };
  }
  private async paneIsBusy(paneId: string, signal?: AbortSignal): Promise<boolean> {
    const raw = await this.herdr(["pane", "process-info", "--pane", paneId], signal);
    const info = raw?.result?.process_info;
    const foreground = info?.foreground_processes;
    // Unknown is NOT an available shell. Never type into an unclassified foreground process.
    if (!Array.isArray(foreground) || foreground.length === 0) throw new Error(`Cannot establish shell readiness for ${paneId}`);
    return !foreground.every((p: any) => p.pid === info.shell_pid && ["bash", "zsh", "fish", "sh"].includes(p.name));
  }
}
