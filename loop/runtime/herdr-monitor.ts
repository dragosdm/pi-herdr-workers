import { execFile } from "node:child_process";
import { createHash } from "node:crypto";
import { promisify } from "node:util";

const execFileAsync = promisify(execFile);
const TAB_LABEL = "Monitor";
const MAX_RUNNING = 25;

export type HerdrMonitorStatus = "running" | "idle" | "stopped" | "error";

export interface HerdrMonitor {
  id: string;
  key: string;
  command: string;
  description?: string;
  paneId: string;
  tabId: string;
  status: HerdrMonitorStatus;
  startedAt: number;
  reused: boolean;
}

interface HerdrJson {
  result?: Record<string, unknown>;
}

function commandKey(command: string): string {
  return createHash("sha256").update(command.trim()).digest("hex").slice(0, 8);
}

function paneLabel(key: string, command: string): string {
  const snippet = command.trim().replace(/\s+/g, " ").slice(0, 40);
  return `mon:${key} ${snippet}`;
}

function isHerdr(): boolean {
  return process.env.HERDR_ENV === "1" && Boolean(process.env.HERDR_WORKSPACE_ID);
}

async function herdr(args: string[]): Promise<HerdrJson> {
  const { stdout, stderr } = await execFileAsync("herdr", args, {
    encoding: "utf8",
    maxBuffer: 2_000_000,
    env: process.env,
  });
  const text = stdout.trim() || stderr.trim();
  if (!text) return {};
  try {
    return JSON.parse(text) as HerdrJson;
  } catch {
    return { result: { text } };
  }
}

function asRecord(value: unknown): Record<string, unknown> | undefined {
  return value && typeof value === "object" && !Array.isArray(value) ? value as Record<string, unknown> : undefined;
}

function asArray(value: unknown): unknown[] {
  return Array.isArray(value) ? value : [];
}

export class HerdrMonitorManager {
  private byId = new Map<string, HerdrMonitor>();
  private nextId = 1;

  list(): HerdrMonitor[] {
    return [...this.byId.values()];
  }

  get(id: string): HerdrMonitor | undefined {
    return this.byId.get(id);
  }

  async create(command: string, description?: string): Promise<HerdrMonitor> {
    if (!isHerdr()) {
      throw new Error("Monitors require a Herdr pane (HERDR_ENV=1). This agent is not running inside Herdr.");
    }
    const running = this.list().filter((m) => m.status === "running").length;
    if (running >= MAX_RUNNING) throw new Error(`Maximum of ${MAX_RUNNING} running monitors reached.`);

    const workspaceId = process.env.HERDR_WORKSPACE_ID!;
    const cwd = process.cwd();
    const key = commandKey(command);
    const label = paneLabel(key, command);
    const tab = await this.ensureMonitorTab(workspaceId, cwd);
    const existingPane = await this.findPaneByKey(tab.tabId, key);

    let paneId: string;
    let reused = false;
    if (existingPane) {
      paneId = existingPane;
      reused = true;
      const live = [...this.byId.values()].find((m) => m.key === key);
      if (live && await this.paneIsBusy(paneId)) {
        live.reused = true;
        live.status = "running";
        return live;
      }
    } else {
      paneId = await this.allocatePane(tab.tabId, tab.rootPaneId, cwd);
      await herdr(["pane", "rename", paneId, label]);
    }

    if (await this.paneIsBusy(paneId)) {
      await herdr(["pane", "send-keys", paneId, "ctrl+c"]);
    }
    await herdr(["pane", "run", paneId, command]);

    const monitor: HerdrMonitor = {
      id: String(this.nextId++),
      key,
      command,
      description,
      paneId,
      tabId: tab.tabId,
      status: "running",
      startedAt: Date.now(),
      reused,
    };
    for (const [id, entry] of this.byId) {
      if (entry.key === key) this.byId.delete(id);
    }
    this.byId.set(monitor.id, monitor);
    return monitor;
  }

  async refresh(monitor: HerdrMonitor): Promise<HerdrMonitor> {
    try {
      monitor.status = (await this.paneIsBusy(monitor.paneId)) ? "running" : "idle";
    } catch {
      monitor.status = "error";
    }
    return monitor;
  }

  async readTail(paneId: string, lines = 5): Promise<string[]> {
    const raw = await herdr(["pane", "read", paneId, "--source", "recent-unwrapped", "--lines", String(lines)]);
    const result = raw.result ?? {};
    const text = typeof result.text === "string"
      ? result.text
      : typeof result.output === "string"
        ? result.output
        : JSON.stringify(result);
    return text.split("\n").filter(Boolean).slice(-lines);
  }

  async stop(id: string): Promise<boolean> {
    const monitor = this.byId.get(id);
    if (!monitor) return false;
    if (monitor.status !== "running") return false;
    await herdr(["pane", "send-keys", monitor.paneId, "ctrl+c"]);
    monitor.status = "stopped";
    return true;
  }

  private async ensureMonitorTab(workspaceId: string, cwd: string): Promise<{ tabId: string; rootPaneId: string }> {
    const listed = await herdr(["tab", "list", "--workspace", workspaceId]);
    const tabs = asArray(asRecord(listed.result)?.tabs);
    for (const tab of tabs) {
      const rec = asRecord(tab);
      if (rec?.label === TAB_LABEL && typeof rec.tab_id === "string") {
        const panes = await this.panesInTab(rec.tab_id);
        return { tabId: rec.tab_id, rootPaneId: panes[0] ?? await this.tabRootPane(rec.tab_id) };
      }
    }
    const created = await herdr(["tab", "create", "--workspace", workspaceId, "--label", TAB_LABEL, "--cwd", cwd, "--no-focus"]);
    const result = asRecord(created.result) ?? {};
    const tab = asRecord(result.tab) ?? result;
    const root = asRecord(result.root_pane) ?? asRecord(result.pane);
    const tabId = String(tab.tab_id ?? tab.id ?? "");
    const rootPaneId = String(root?.pane_id ?? root?.id ?? "");
    if (!tabId || !rootPaneId) throw new Error(`herdr tab create did not return IDs: ${JSON.stringify(created)}`);
    return { tabId, rootPaneId };
  }

  private async tabRootPane(tabId: string): Promise<string> {
    const got = await herdr(["tab", "get", tabId]);
    const result = asRecord(got.result) ?? {};
    const tab = asRecord(result.tab) ?? result;
    const root = asRecord(result.root_pane) ?? asRecord(tab.root_pane);
    const paneId = root?.pane_id ?? tab.pane_id;
    if (typeof paneId !== "string") throw new Error(`Monitor tab ${tabId} has no root pane`);
    return paneId;
  }

  private async panesInTab(tabId: string): Promise<string[]> {
    const workspaceId = process.env.HERDR_WORKSPACE_ID!;
    const listed = await herdr(["pane", "list", "--workspace", workspaceId]);
    const panes = asArray(asRecord(listed.result)?.panes);
    const ids: string[] = [];
    for (const pane of panes) {
      const rec = asRecord(pane);
      if (rec?.tab_id === tabId && typeof rec.pane_id === "string") ids.push(rec.pane_id);
    }
    return ids;
  }

  private async findPaneByKey(tabId: string, key: string): Promise<string | undefined> {
    const workspaceId = process.env.HERDR_WORKSPACE_ID!;
    const listed = await herdr(["pane", "list", "--workspace", workspaceId]);
    const panes = asArray(asRecord(listed.result)?.panes);
    const prefix = `mon:${key}`;
    for (const pane of panes) {
      const rec = asRecord(pane);
      if (rec?.tab_id !== tabId) continue;
      const title = String(rec.title ?? rec.terminal_title_stripped ?? "");
      if (title.startsWith(prefix) && typeof rec.pane_id === "string") return rec.pane_id;
    }
    return undefined;
  }

  private async allocatePane(tabId: string, rootPaneId: string, cwd: string): Promise<string> {
    const existing = await this.panesInTab(tabId);
    if (existing.length === 0) return rootPaneId;
    const unused = existing.find((id) => ![...this.byId.values()].some((m) => m.paneId === id));
    // First monitor uses the tab's empty root pane.
    if (existing.length === 1 && unused === existing[0]) {
      const busy = await this.paneIsBusy(existing[0]!);
      if (!busy) return existing[0]!;
    }
    const splitFrom = existing[existing.length - 1] ?? rootPaneId;
    const split = await herdr(["pane", "split", splitFrom, "--direction", "down", "--cwd", cwd, "--no-focus"]);
    const pane = asRecord(asRecord(split.result)?.pane) ?? asRecord(split.result);
    const paneId = pane?.pane_id;
    if (typeof paneId !== "string") throw new Error(`herdr pane split did not return a pane id: ${JSON.stringify(split)}`);
    return paneId;
  }

  private async paneIsBusy(paneId: string): Promise<boolean> {
    const info = await herdr(["pane", "process-info", "--pane", paneId]);
    const processInfo = asRecord(asRecord(info.result)?.process_info) ?? asRecord(info.result);
    const foreground = asArray(processInfo?.foreground_processes);
    return foreground.some((proc) => {
      const rec = asRecord(proc);
      const name = String(rec?.name ?? rec?.cmdline ?? "");
      return name !== "" && name !== "bash" && name !== "zsh" && name !== "fish" && name !== "sh";
    });
  }
}
