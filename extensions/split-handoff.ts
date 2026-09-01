// Named, unfocused Herdr splits. Handoff crystallizes context with a model pass
// before prompting the new pane; fork clones the branch without rewriting it.
import { randomUUID } from "node:crypto";
import { access, mkdir, writeFile } from "node:fs/promises";
import { join } from "node:path";
import { setTimeout as delay } from "node:timers/promises";
import {
  convertToLlm,
  getAgentDir,
  SessionManager,
  serializeConversation,
  sessionEntryToContextMessages,
  type ExtensionAPI,
  type ExtensionCommandContext,
  type ExtensionContext,
  type SessionEntry,
} from "@earendil-works/pi-coding-agent";
import { Text } from "@earendil-works/pi-tui";

const ENTRY = "split-handoff.operation.v1";
const CONTEXT_ENTRY = "split-handoff.context.v1";
const STATUS = "split-handoff";
const NAME_RE = /^[a-z][a-z0-9_-]{0,31}$/;
const TINY_MODEL_ID = "deepseek-v4-flash";
const HANDOFF_SYSTEM = `You are a context transfer assistant. Given a conversation history and the user's goal for a new pane, generate a focused, self-contained handoff prompt.

1. Summarize relevant context (decisions, approaches, key findings)
2. List files that were discussed or modified
3. State the next task from the user's goal
4. Do not include preamble like "Here's the prompt" — output only the prompt

Format:
## Context
...
## Files
...
## Task
...`;

type Mode = "handoff" | "fork";
type Phase = "preparing" | "summarizing" | "pane-created" | "started" | "prompted" | "failed" | "interrupted";
type Operation = {
  id: string; mode: Mode; phase: Phase; at: number; sourceSession: string;
  name?: string; paneId?: string; direction?: "right" | "down";
  sessionFile?: string; documentFile?: string; error?: string;
};
const clean = (s: string) => s.replace(/[\x00-\x1f\x7f-\x9f]/g, " ").trim();

export function slugify(input: string, max = 24): string {
  return input.toLowerCase().replace(/[^a-z0-9]+/g, "-").replace(/^[^a-z]+/, "").slice(0, max).replace(/-+$/, "");
}
export function splitPrefixedName(raw: string, used: Iterable<string>): string {
  const taken = new Set(used);
  const root = `split-${slugify(raw.replace(/^split-/, ""), 25) || "agent"}`;
  for (let i = 1; i < 10000; i++) {
    const suffix = i === 1 ? "" : `-${i}`;
    const name = `${root.slice(0, 32 - suffix.length)}${suffix}`;
    if (!taken.has(name)) return name;
  }
  throw new Error("No free split name");
}
export function extractText(content: unknown): string {
  if (typeof content === "string") return content;
  if (!Array.isArray(content)) return "";
  return content.filter((b) => b?.type === "text" && typeof b.text === "string").map((b) => b.text).join("\n");
}
export function collectTranscript(entries: SessionEntry[]): string {
  const text = entries.flatMap(sessionEntryToContextMessages).flatMap((message) => {
    if (message.role === "compactionSummary" || message.role === "branchSummary") return [`Summary:\n${message.summary}`];
    if (message.role === "user" || message.role === "assistant" || message.role === "custom") {
      const text = extractText(message.content).trim();
      return text ? [`${message.role}:\n${text}`] : [];
    }
    return [];
  }).join("\n\n");
  return text.length <= 60000 ? text : `[Earlier transcript omitted]\n${text.slice(-60000)}`;
}
export function originLine(name: string): string {
  return `You are a pi instance split from Herdr agent \`${clean(name)}\`.`;
}
export function wrapHandoff(document: string, options: { sourceName: string; focus?: string }): string {
  return `<handoff-context>\n${originLine(options.sourceName)}\n\n${document}\n</handoff-context>\n\n${options.focus || "Continue the work using this context."}`;
}
export function wrapFork(options: { sourceName: string; instruction?: string }): string {
  return `${originLine(options.sourceName)}\n\n${options.instruction || "Continue from the forked context."}`;
}

function conversationForHandoff(ctx: ExtensionContext): string {
  try {
    const messages = ctx.sessionManager.buildContextEntries()
      .flatMap(sessionEntryToContextMessages);
    return serializeConversation(convertToLlm(messages as never));
  } catch {
    return collectTranscript(ctx.sessionManager.buildContextEntries());
  }
}

async function crystallizeHandoff(ctx: ExtensionContext, goal: string, signal: AbortSignal): Promise<string | undefined> {
  if (!ctx.model) return undefined;
  const history = conversationForHandoff(ctx);
  if (!history.trim()) return undefined;
  const response = await ctx.modelRegistry.complete(ctx.model, {
    systemPrompt: HANDOFF_SYSTEM,
    messages: [{
      role: "user",
      content: [{ type: "text", text: `## Conversation History\n\n${history}\n\n## User's Goal for New Pane\n\n${goal || "Continue the work."}` }],
      timestamp: Date.now(),
    }],
  }, {
    signal,
    cacheRetention: "none",
    sessionId: randomUUID(),
  });
  if (response.stopReason === "aborted") {
    signal.throwIfAborted();
    return undefined;
  }
  const text = response.content
    .filter((c): c is { type: "text"; text: string } => c.type === "text")
    .map((c) => c.text)
    .join("\n")
    .trim();
  return text || undefined;
}

async function nameByTiny(ctx: ExtensionContext, hint: string, used: Set<string>, signal: AbortSignal): Promise<string> {
  const fallback = splitPrefixedName(hint, used);
  const model = ctx.modelRegistry.find("llm-tail", TINY_MODEL_ID);
  if (!model) return fallback;
  try {
    const response = await ctx.modelRegistry.complete(model, {
      messages: [{ role: "user", content: `Reply only with a short lowercase hyphenated agent name. Topic: ${hint.slice(0, 400)}`, timestamp: Date.now() }],
    }, { maxTokens: 24, temperature: 0.2, cacheRetention: "none", sessionId: randomUUID(),
      signal: AbortSignal.any([signal, AbortSignal.timeout(4000)]) });
    const name = extractText(response.content).trim().replace(/^["'`]+|["'`]+$/g, "");
    return name && NAME_RE.test(name) ? splitPrefixedName(name, used) : fallback;
  } catch {
    signal.throwIfAborted();
    return fallback;
  }
}

export default function (pi: ExtensionAPI) {
  let controller: AbortController | undefined;
  let active: Operation | undefined;
  let disposed = false;
  let ctxRef: ExtensionContext | undefined;

  function history(ctx: ExtensionContext): Operation[] {
    const operations = new Map<string, Operation>();
    for (const entry of ctx.sessionManager.getEntries()) {
      if (entry.type !== "custom" || entry.customType !== ENTRY) continue;
      const data = entry.data as Operation | undefined;
      if (data?.sourceSession === ctx.sessionManager.getSessionId() && typeof data.id === "string") operations.set(data.id, data);
    }
    return [...operations.values()];
  }
  function paint(ctx: ExtensionContext) {
    if (!ctx.hasUI) return;
    if (active) { ctx.ui.setStatus(STATUS, `split · ${active.phase}${active.name ? ` · ${active.name}` : ""}`); return; }
    const records = history(ctx);
    const attention = records.filter((o) => o.phase !== "prompted").length;
    ctx.ui.setStatus(STATUS, attention ? `split · ${attention} need review · /splits` : undefined);
  }
  function save(ctx: ExtensionContext, op: Operation) {
    pi.appendEntry(ENTRY, { ...op });
    paint(ctx);
  }
  async function herdr(args: string[], signal: AbortSignal): Promise<any> {
    signal.throwIfAborted();
    const result = await pi.exec("herdr", args, { signal, timeout: args[1] === "start" ? 45000 : 15000 });
    signal.throwIfAborted();
    if (result.code !== 0 || result.killed) throw new Error(clean(result.stderr || result.stdout || `herdr ${args.slice(0, 2).join(" ")} failed`));
    return result.stdout.trim() ? JSON.parse(result.stdout) : undefined;
  }

  pi.registerEntryRenderer<Operation>(ENTRY, (entry, { expanded }, theme) => {
    const op = entry.data;
    if (!op) return new Text("", 0, 0);
    const tone = op.phase === "prompted" ? "success" : op.phase === "failed" || op.phase === "interrupted" ? "warning" : "muted";
    let text = `split-${op.mode} · ${op.phase} · ${op.name || op.id.slice(0, 8)}`;
    if (expanded) text += `\n${[op.paneId, op.sessionFile, op.documentFile, op.error].filter((s): s is string => typeof s === "string").map(clean).join("\n")}`;
    return new Text(theme.fg(tone, text), 0, 0);
  });
  pi.on("session_start", (_event, ctx) => { disposed = false; ctxRef = ctx; paint(ctx); });
  pi.on("session_tree", (_event, ctx) => { ctxRef = ctx; paint(ctx); });
  pi.on("session_shutdown", () => {
    controller?.abort();
    if (active && ctxRef) {
      active = { ...active, phase: "interrupted", at: Date.now(), error: "Runtime closed; inspect the pane before retrying." };
      save(ctxRef, active);
    }
    disposed = true;
    active = undefined;
    if (ctxRef?.hasUI) ctxRef.ui.setStatus(STATUS, undefined);
    ctxRef = undefined;
  });
  pi.registerCommand("splits", {
    description: "Show durable split history (not live agent status); /splits cancel cancels the current startup",
    handler: async (args, ctx) => {
      if (args.trim() === "cancel") { controller?.abort(); return; }
      const lines = history(ctx).map((o) => `${o.name || o.id.slice(0, 8)} · ${o.phase} · ${o.paneId || "no pane recorded"}${o.error ? `\n  ${clean(o.error)}` : ""}`);
      ctx.ui.notify(lines.join("\n") || "No split history in this session.", "info");
    },
  });

  const handle = (mode: Mode) => async (args: string, ctx: ExtensionCommandContext) => {
    if (ctx.mode !== "tui" || process.env.HERDR_ENV !== "1" || !process.env.HERDR_PANE_ID) {
      ctx.ui.notify("Splits require an interactive pi in a Herdr pane.", "error"); return;
    }
    if (controller || !ctx.isIdle()) { ctx.ui.notify("Wait for the current operation to finish before splitting.", "warning"); return; }
    const sourcePane = process.env.HERDR_PANE_ID;
    const sourceFile = ctx.sessionManager.getSessionFile();
    const leaf = ctx.sessionManager.getLeafId();
    const branch = ctx.sessionManager.getBranch();
    if (!branch.some((e) => e.type === "message" && e.message.role === "assistant")) {
      ctx.ui.notify("No completed conversation to split yet.", "warning"); return;
    }
    controller = new AbortController();
    const signal = controller.signal;
    ctxRef = ctx;
    active = { id: randomUUID(), sourceSession: ctx.sessionManager.getSessionId(), mode, phase: "preparing", at: Date.now() };
    save(ctx, active);
    try {
      if (mode === "fork") {
        if (!sourceFile || !leaf) throw new Error("Fork requires a persisted session.");
        await access(sourceFile);
        signal.throwIfAborted();
        const file = SessionManager.open(sourceFile).createBranchedSession(leaf);
        if (!file) throw new Error("Could not persist the fork.");
        active.sessionFile = file;
        save(ctx, active);
      }
      const used = new Set<string>();
      const listed = await herdr(["agent", "list"], signal);
      for (const agent of listed?.result?.agents ?? []) if (typeof agent.name === "string") used.add(agent.name);
      const lastUser = branch.flatMap((e) => e.type === "message" && e.message.role === "user" ? [extractText(e.message.content)] : []).at(-1);
      const hint = args.trim() || ctx.sessionManager.getSessionName() || lastUser || mode;
      const name = await nameByTiny(ctx, hint, used, signal);
      signal.throwIfAborted();
      active.name = name;
      const source = await herdr(["pane", "get", sourcePane], signal);
      const sourceName = source?.result?.pane?.label || sourcePane;
      let pane = sourcePane;
      let direction: "right" | "down" = "right";
      try {
        const neighbor = await herdr(["pane", "neighbor", "--pane", sourcePane, "--direction", "right"], signal);
        const right = neighbor?.result?.neighbor?.neighbor_pane_id;
        if (right) {
          const got = await herdr(["pane", "get", right], signal);
          if (got?.result?.pane?.label?.startsWith("split-")) { pane = right; direction = "down"; }
        }
      } catch { signal.throwIfAborted(); }
      active.direction = direction;

      let prompt = wrapFork({ sourceName, instruction: args.trim() });
      if (mode === "handoff") {
        active.phase = "summarizing";
        save(ctx, active);
        let document: string;
        try {
          const crystallized = await crystallizeHandoff(ctx, args.trim(), signal);
          document = crystallized
            ? wrapHandoff(crystallized, { sourceName, focus: args.trim() })
            : wrapHandoff(collectTranscript(ctx.sessionManager.buildContextEntries()), { sourceName, focus: args.trim() });
        } catch (error) {
          signal.throwIfAborted();
          debugFail(error);
          document = wrapHandoff(collectTranscript(ctx.sessionManager.buildContextEntries()), { sourceName, focus: args.trim() });
        }
        pi.appendEntry(CONTEXT_ENTRY, { id: active.id, document });
        prompt = document;
        if (document.length > 8000) {
          const dir = join(getAgentDir(), "handoffs");
          await mkdir(dir, { recursive: true, mode: 0o700 });
          signal.throwIfAborted();
          active.documentFile = join(dir, `${active.id}.md`);
          await writeFile(active.documentFile, document, { mode: 0o600, flag: "wx" });
          prompt = `${originLine(sourceName)}\nRead the handoff at ${active.documentFile} and follow its focus instructions.`;
        }
      }
      signal.throwIfAborted();
      save(ctx, active);
      const split = await herdr(["pane", "split", "--pane", pane, "--direction", direction, "--cwd", ctx.cwd, "--no-focus"], signal);
      const paneId = split?.result?.pane?.pane_id;
      if (typeof paneId !== "string") throw new Error("Herdr did not return the created pane ID.");
      active.paneId = paneId;
      active.phase = "pane-created";
      save(ctx, active);
      await herdr(["pane", "rename", paneId, name], signal);
      await delay(1000, undefined, { signal });
      const start = ["agent", "start", name, "--kind", "pi", "--pane", paneId, "--timeout", "30000"];
      if (active.sessionFile) start.push("--", "--session", active.sessionFile);
      await herdr(start, signal);
      active.phase = "started";
      save(ctx, active);
      await herdr(["agent", "prompt", name, prompt], signal);
      active.phase = "prompted";
      save(ctx, active);
      pi.sendMessage({ customType: "split-handoff.inbox", content: `/split-${mode} sent to ${name} (${paneId}).`, display: false }, { triggerTurn: false });
    } catch (error) {
      if (!disposed && active) {
        active.phase = signal.aborted ? "interrupted" : "failed";
        active.error = clean(error instanceof Error ? error.message : String(error));
        save(ctx, active);
        ctx.ui.notify(`Split ${active.phase}: ${active.error}${active.paneId ? ` · inspect ${active.paneId} before retrying` : " · inspect the Herdr layout before retrying"}`, "warning");
      }
    } finally {
      controller = undefined;
      active = undefined;
      if (!disposed) paint(ctx);
    }
  };
  pi.registerCommand("split-handoff", { description: "Summarize this thread and hand off into a named pi split (right, then down)", handler: handle("handoff") });
  pi.registerCommand("split-fork", { description: "Clone the active branch into a named pi split (source stays here)", handler: handle("fork") });
}

function debugFail(error: unknown) {
  if (process.env.PI_LOOP_DEBUG) console.error("[split-handoff] crystallize failed", error);
}
