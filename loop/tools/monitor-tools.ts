import type { ExtensionAPI } from "@earendil-works/pi-coding-agent";
import { Type } from "typebox";
import type { HerdrMonitor, HerdrMonitorManager } from "../runtime/herdr-monitor.js";
import { renderToolCall, renderToolResult, toolArg } from "../ui/tool-renderer.js";
import { displayRows, textResult } from "./tool-result.js";

export interface MonitorToolsOptions {
  pi: ExtensionAPI;
  getMonitors: () => HerdrMonitorManager;
  updateWidget: () => void;
}

function formatAge(ms: number): string {
  if (ms < 60000) return `${Math.round(ms / 1000)}s`;
  if (ms < 3600000) return `${Math.round(ms / 60000)}m`;
  return `${Math.round(ms / 3600000)}h`;
}

export function registerMonitorTools(options: MonitorToolsOptions): void {
  const { pi, getMonitors, updateWidget } = options;

  pi.registerTool({
    name: "MonitorCreate",
    label: "MonitorCreate",
    renderCall: renderToolCall("Monitor", (args) => `start · ${String(toolArg(args, "description") ?? toolArg(args, "command") ?? "background command").slice(0, 56)}`),
    renderResult: renderToolResult,
    description: "Run a long command in a Herdr Monitor tab pane. Same command reuses the same named pane; panes stay open when the command finishes. Use MonitorList/MonitorStop; do not poll with sleep.",
    parameters: Type.Object({
      command: Type.String({ description: "Shell command to run in a Monitor pane" }),
      description: Type.Optional(Type.String({ description: "Human-readable description" })),
    }),
    async execute(_toolCallId, params) {
      try {
        const entry = await getMonitors().create(params.command, params.description);
        updateWidget();
        const reuse = entry.reused ? "reused existing pane" : "new pane";
        return textResult(
          `Monitor #${entry.id} ${entry.reused ? "attached" : "started"} (${reuse})\n` +
          `Tab: Monitor · pane ${entry.paneId}\n` +
          `Command: ${entry.command}\n` +
          `The pane stays open after the command ends. The same command reuses pane key mon:${entry.key}.`,
          {
            kind: "monitor",
            action: "create",
            tone: "success",
            summary: `Monitor #${entry.id} ${entry.status} · ${params.description ?? entry.command.slice(0, 48)}`,
            expanded: [`Pane: ${entry.paneId}`, `Key: mon:${entry.key}`, reuse],
          },
        );
      } catch (error) {
        const message = error instanceof Error ? error.message : String(error);
        return textResult(message, {
          kind: "monitor", action: "create", tone: "error", summary: "Monitor was not created", expanded: [message],
        });
      }
    },
  });

  pi.registerTool({
    name: "MonitorList",
    label: "MonitorList",
    renderCall: renderToolCall("Monitor", () => "status"),
    renderResult: renderToolResult,
    description: "List Herdr Monitor panes with status, command, pane id, and a short output tail.",
    parameters: Type.Object({}),
    async execute() {
      const manager = getMonitors();
      const monitors = manager.list();
      if (monitors.length === 0) {
        return textResult("No monitors.", {
          kind: "monitor", action: "list", tone: "info", summary: "No monitors", expanded: ["Use MonitorCreate to open a pane in the Monitor tab."],
        });
      }
      const lines: string[] = [];
      for (const raw of monitors) {
        const m: HerdrMonitor = await manager.refresh(raw);
        const icon = m.status === "running" ? ">" : m.status === "idle" ? "ok" : "x";
        lines.push(`${icon} #${m.id} [${m.status}] ${m.command.slice(0, 60)} · pane ${m.paneId} (${formatAge(Date.now() - m.startedAt)})`);
        try {
          const tail = await manager.readTail(m.paneId, 5);
          for (const out of tail) lines.push(`  | ${out.slice(0, 100)}`);
        } catch {
          lines.push("  | (could not read pane)");
        }
      }
      const running = monitors.filter((monitor) => monitor.status === "running").length;
      return textResult(lines.join("\n"), {
        kind: "monitor",
        action: "list",
        tone: "info",
        summary: `${monitors.length} monitor${monitors.length === 1 ? "" : "s"} · ${running} running`,
        expanded: displayRows(lines),
      });
    },
  });

  pi.registerTool({
    name: "MonitorStop",
    label: "MonitorStop",
    renderCall: renderToolCall("Monitor", (args) => `stop · #${String(toolArg(args, "monitorId") ?? "?")}`),
    renderResult: renderToolResult,
    description: "Interrupt a running monitor with ctrl+c. The Herdr pane stays open so the same command can reuse it.",
    parameters: Type.Object({
      monitorId: Type.String({ description: "Monitor ID to stop" }),
    }),
    async execute(_toolCallId, params) {
      const stopped = await getMonitors().stop(params.monitorId);
      updateWidget();
      if (stopped) {
        return textResult(`Monitor #${params.monitorId} interrupted; pane kept for reuse`, {
          kind: "monitor", action: "stop", tone: "success", summary: `Monitor #${params.monitorId} stopped`, expanded: ["Pane was not closed."],
        });
      }
      return textResult(`Monitor #${params.monitorId} not found or not running`, {
        kind: "monitor", action: "stop", tone: "error", summary: `Monitor #${params.monitorId} unavailable`, expanded: ["Use MonitorList to find running monitor IDs."],
      });
    },
  });
}
