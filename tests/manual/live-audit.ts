/** Opt-in live Herdr audit driver. Never auto-loaded by the package.
 * Launch with --no-extensions -e tests/manual/live-audit.ts.
 * HERDR_AUDIT_DIR must name a disposable output directory.
 * /audit accepts JSON: {id, action, name?, input?}.
 * Tool actions invoke the registered adapter directly, not a model tool call.
 */
import * as fs from "node:fs";
import * as path from "node:path";
import worker from "../../extensions/herdr-worker.js";
import splits from "../../extensions/split-handoff.js";
import loops from "../../loop/index.js";
import { createWorkerRpcClient } from "../../rpc/client.js";
import { createRunQueryClient } from "../../runs/client.js";
import { createReconciliationClient } from "../../reconciliation/client.js";
import type { ExtensionAPI } from "@earendil-works/pi-coding-agent";

export default function (pi: ExtensionAPI) {
  const root = process.env.HERDR_AUDIT_DIR;
  if (!root) throw new Error("HERDR_AUDIT_DIR is required for the manual audit driver");
  fs.mkdirSync(root, { recursive: true });
  const pane = (process.env.HERDR_PANE_ID ?? "headless").replace(/[^a-z0-9-]/gi, "_");
  const log = (data: unknown) => fs.appendFileSync(path.join(root, `${pane}.jsonl`), JSON.stringify({ at: new Date().toISOString(), data }) + "\n");
  const tools = new Map<string, any>();
  const commands = new Map<string, any>();
  const wrapped = new Proxy(pi, {
    get(target, key) {
      if (key === "registerTool") return (tool: any) => {
        tools.set(tool.name, tool);
        target.registerTool({ ...tool, async execute(...args: any[]) {
          try { const result = await tool.execute(...args); log({ modelTool: tool.name, input: args[1], result }); return result; }
          catch (error) { log({ modelTool: tool.name, error: String(error) }); throw error; }
        } });
      };
      if (key === "registerCommand") return (name: string, command: any) => { commands.set(name, command); target.registerCommand(name, command); };
      return Reflect.get(target, key);
    },
  });
  worker(wrapped); splits(wrapped); loops(wrapped);
  for (const channel of ["herdr-workers:lifecycle", "loop:fire", "loops:expired"]) pi.events.on(channel, (event) => log({ channel, event }));
  pi.on("session_start", (event, ctx) => log({ session: event.reason, mode: ctx.mode, sessionFile: ctx.sessionManager.getSessionFile(), tools: pi.getActiveTools() }));
  pi.on("agent_settled", (_event, ctx) => log({ settled: true, idle: ctx.isIdle() }));
  pi.registerCommand("audit", {
    description: "Explicit manual test driver, writes results to HERDR_AUDIT_DIR",
    handler: async (args, ctx) => {
      let request: any;
      try {
        request = JSON.parse(args);
        let result: any;
        if (request.action === "tool") {
          const tool = tools.get(request.name);
          if (!tool) throw new Error("Unknown audit tool");
          result = await tool.execute(`audit-${request.id}`, request.input ?? {}, new AbortController().signal, undefined, ctx);
        } else if (request.action === "command") {
          const command = commands.get(request.name);
          if (!command) throw new Error("Unknown audit command");
          const notifications: any[] = [];
          const ui = new Proxy(ctx.ui, { get(target, key) {
            if (key === "notify") return (text: string, level: "info" | "warning" | "error") => { notifications.push({ text, level }); target.notify(text, level); };
            return Reflect.get(target, key);
          } });
          await command.handler(request.input ?? "", new Proxy(ctx, { get(target, key) { return key === "ui" ? ui : Reflect.get(target, key); } }));
          result = notifications;
        } else if (request.action === "rpc" || request.action === "runs" || request.action === "reconcile") {
          const client: any = request.action === "rpc" ? createWorkerRpcClient({ events: pi.events }) : request.action === "runs" ? createRunQueryClient({ events: pi.events }) : createReconciliationClient({ events: pi.events });
          const provider = await client.probe({ timeoutMs: 1000 });
          result = request.name === "probe" ? provider : await client[request.name](request.input ?? {}, provider, { timeoutMs: 120000 });
        } else if (request.action === "emit") {
          pi.events.emit(request.name, request.input ?? {}); result = "emitted";
        } else if (request.action === "snapshot") {
          result = { tools: pi.getActiveTools(), sessionId: ctx.sessionManager.getSessionId(), sessionFile: ctx.sessionManager.getSessionFile(), entries: ctx.sessionManager.getEntries().filter(e => e.type === "custom" || e.type === "custom_message") };
        } else if (request.action === "shutdown") {
          ctx.shutdown(); result = "shutdown requested";
        } else throw new Error("Unknown audit action");
        log({ request, result });
        ctx.ui.notify(`AUDIT_DONE ${request.id}`, "info");
      } catch (error) {
        log({ request, error: String(error), stack: error instanceof Error ? error.stack : undefined });
        ctx.ui.notify(`AUDIT_ERROR ${request?.id}: ${String(error)}`, "error");
      }
    },
  });
}
