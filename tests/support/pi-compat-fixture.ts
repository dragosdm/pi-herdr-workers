import assert from "node:assert/strict";
import { EventEmitter } from "node:events";
import * as fs from "node:fs";
import { createRequire } from "node:module";
import * as os from "node:os";
import * as path from "node:path";
import type { TestContext } from "node:test";
import { fileURLToPath } from "node:url";
import {
	DefaultResourceLoader, ModelRuntime, SessionManager, SettingsManager, createAgentSession,
	type AgentSession, type CustomMessageEntry, type ExtensionAPI, type ExtensionError,
} from "@earendil-works/pi-coding-agent";
import { createAssistantMessageEventStream, type AssistantMessage } from "@earendil-works/pi-ai";
import { inboxDir, listeningFile } from "../../mailbox/paths.js";
import { createMailboxTransport, type MailboxTransport } from "../../mailbox/transport.js";
import { lifecycleEnvelope, seedLifecycleAuthority } from "./mailbox-lifecycle.js";

export const piCompatibilityVersion = "0.84.4";
const timeoutMs = 5000;

function packageMetadata(name: string, entry: string) {
	let dir = path.dirname(entry);
	while (true) {
		const file = path.join(dir, "package.json");
		if (fs.existsSync(file)) {
			const metadata = JSON.parse(fs.readFileSync(file, "utf8"));
			if (metadata.name === name) return { ...metadata, entry, root: dir };
		}
		const parent = path.dirname(dir);
		assert.notEqual(parent, dir, `Cannot resolve metadata for ${name} from ${entry}`);
		dir = parent;
	}
}

export function assertPiCompatibility(t: TestContext) {
	const codingAgent = packageMetadata("@earendil-works/pi-coding-agent", fileURLToPath(import.meta.resolve("@earendil-works/pi-coding-agent")));
	const agentCore = packageMetadata("@earendil-works/pi-agent-core", createRequire(codingAgent.entry).resolve("@earendil-works/pi-agent-core/package.json"));
	const ai = packageMetadata("@earendil-works/pi-ai", fileURLToPath(import.meta.resolve("@earendil-works/pi-ai")));
	const localModules = fileURLToPath(new URL("../../node_modules/", import.meta.url));
	const diagnostic = `Pi coding-agent ${codingAgent.version}, resolved agent-core ${agentCore.version}, pi-ai ${ai.version}`;
	t.diagnostic(diagnostic);
	for (const dependency of [codingAgent, agentCore, ai]) {
		assert.equal(dependency.version, piCompatibilityVersion, `${diagnostic}: unexpected ${dependency.name} version`);
		const relative = path.relative(localModules, dependency.root);
		assert.ok(!relative.startsWith("..") && !path.isAbsolute(relative), `${diagnostic}: package must be project-local: ${dependency.root}`);
	}
	return diagnostic;
}

export function customEntries(manager: SessionManager): CustomMessageEntry[] {
	return manager.getEntries().filter((entry) => entry.type === "custom_message");
}

export interface PiObservation {
	name: string;
	envelopeId?: string;
	entries: CustomMessageEntry[];
	files: string[];
	sessionFileExists: boolean;
}

interface HeldStream {
	released: boolean;
	release(): void;
	abort(): void;
	fail(): void;
}

export async function piCompatFixture(t: TestContext, options: { disabled?: boolean; expectWriteError?: boolean; lifecycle?: boolean } = {}) {
	const diagnostic = assertPiCompatibility(t);
	const root = fs.mkdtempSync(path.join(os.tmpdir(), "herdr-pi-compat-"));
	const cwd = path.join(root, "cwd");
	const agentDir = path.join(root, "agent");
	const sessionDir = path.join(root, "sessions");
	const mailboxRoot = path.join(root, "mailbox");
	const inbox = inboxDir(mailboxRoot, "self-pane");
	const changes = new EventEmitter();
	const observations: PiObservation[] = [];
	const streams: HeldStream[] = [];
	const errors: ExtensionError[] = [];
	const failures: unknown[] = [];
	const deliveries: string[] = [];
	const schedules: Array<{ disposed: boolean }> = [];
	const prompts: Promise<void>[] = [];
	let session: AgentSession | undefined;
	let transport: MailboxTransport | undefined;
	let manager: SessionManager;
	let restoreDirectory: (() => void) | undefined;
	let restoreSend: (() => void) | undefined;
	let injectedSendError: Error | undefined;
	let unsubscribe: (() => void) | undefined;
	let disposed = false;
	let workerPi!: ExtensionAPI;
	let beforeContext: (() => void) | undefined;

	async function bounded<T>(promise: Promise<T>, label: string): Promise<T> {
		let timer: ReturnType<typeof setTimeout> | undefined;
		try {
			return await Promise.race([promise, new Promise<never>((_resolve, reject) => {
				timer = setTimeout(() => reject(new Error(`${diagnostic}: timed out waiting for ${label}`)), timeoutMs);
			})]);
		} finally { clearTimeout(timer); }
	}

	async function dispose() {
		if (disposed) return;
		disposed = true;
		try {
			restoreSend?.();
			restoreDirectory?.();
			for (const stream of streams) stream.abort();
			if (session) {
				await bounded(session.abort(), "session abort");
				await bounded(Promise.all(prompts), "prompt cleanup");
				await bounded(session.extensionRunner.emit({ type: "session_shutdown", reason: "quit" }), "extension shutdown");
			}
			assert.ok(streams.every((stream) => stream.released), `${diagnostic}: held streams must be released or aborted`);
			assert.ok(schedules.every((schedule) => schedule.disposed), `${diagnostic}: watch and poll callbacks must be disposed`);
			assert.equal(transport?.isStarted() ?? false, false);
			assert.equal(fs.existsSync(listeningFile(mailboxRoot, "self-pane")), false);
			assert.deepEqual(failures, [], `${diagnostic}: fixture or prompt failures`);
			if (injectedSendError) {
				assert.equal(errors.length, 1, diagnostic);
				assert.equal(errors[0].event, "send_message", diagnostic);
				assert.equal(errors[0].error, injectedSendError.message, diagnostic);
			} else if (!options.expectWriteError) assert.deepEqual(errors, [], `${diagnostic}: unexpected extension errors`);
		} finally {
			restoreSend?.();
			for (const stream of streams) stream.abort();
			unsubscribe?.();
			session?.dispose();
			transport?.stopListening();
			changes.removeAllListeners();
			fs.rmSync(root, { recursive: true, force: true });
			assert.equal(fs.existsSync(root), false);
		}
	}
	t.after(dispose);

	for (const dir of [cwd, agentDir, sessionDir]) fs.mkdirSync(dir, { recursive: true });
	// Discovery must ignore even valid-looking local resources, not just an empty directory.
	fs.mkdirSync(path.join(cwd, ".pi", "extensions"), { recursive: true });
	fs.writeFileSync(path.join(cwd, ".pi", "extensions", "unexpected.ts"), 'throw new Error("Unexpected extension discovery");');
	fs.writeFileSync(path.join(cwd, "AGENTS.md"), "Unexpected project context");
	manager = options.disabled ? SessionManager.inMemory(cwd, { id: "pi-mailbox-compat" })
		: SessionManager.create(cwd, sessionDir, { id: "pi-mailbox-compat" });
	manager.appendCustomEntry("herdr-worker", { version: 1, sessionId: manager.getSessionId(), workers: ["agent-scout"] });
	if (options.lifecycle) seedLifecycleAuthority({ entries: manager.getEntries(), ctx: { cwd, sessionManager: manager },
		appendEntry: (customType, data) => { manager.appendCustomEntry(customType, data); } });
	const settingsManager = SettingsManager.inMemory({ retry: { enabled: false }, compaction: { enabled: false } });
	const { default: workerExtension } = await import("../../extensions/herdr-worker.js");

	function record(name: string, envelopeId?: string) {
		const file = manager.getSessionFile();
		observations.push({
			name, envelopeId, entries: structuredClone(customEntries(manager)),
			files: fs.existsSync(inbox) ? fs.readdirSync(inbox).sort() : [],
			sessionFileExists: !!file && fs.existsSync(file),
		});
		changes.emit("change");
	}

	const agents = [
		{ pane_id: "self-pane", tab_id: "tab-1", name: "orchestrator", agent: "pi", cwd },
		{ pane_id: "worker-pane", tab_id: "tab-1", name: "agent-scout", agent: "pi", agent_status: "idle", cwd },
	];
	const exec: ExtensionAPI["exec"] = async (command, args) => {
		try {
			assert.equal(command, "herdr", `${diagnostic}: unexpected executable`);
			if (args[0] === "agent" && args[1] === "get") {
				assert.equal(args.length, 3, `${diagnostic}: unexpected Herdr arguments`);
				const agent = agents.find((agent) => agent.pane_id === args[2] || agent.name === args[2]);
				assert.ok(agent, `${diagnostic}: unexpected Herdr target ${args[2]}`);
				return { code: 0, stdout: JSON.stringify({ result: { agent } }), stderr: "", killed: false };
			}
			assert.deepEqual(args, ["agent", "list"], `${diagnostic}: unexpected Herdr call`);
			return { code: 0, stdout: JSON.stringify({ result: { agents } }), stderr: "", killed: false };
		} catch (error) {
			failures.push(error);
			throw error;
		}
	};
	function schedule() {
		const item = { disposed: false };
		schedules.push(item);
		return () => { item.disposed = true; };
	}
	const loader = new DefaultResourceLoader({
		cwd, agentDir, settingsManager, noExtensions: true, noSkills: true, noPromptTemplates: true,
		noThemes: true, noContextFiles: true, systemPrompt: "Reply with a synthetic test response.",
		extensionFactories: [{ name: "mailbox-compat", factory(pi) {
			workerPi = pi;
			pi.exec = exec;
			pi.on("session_shutdown", (event) => { record(`session_shutdown:${event.reason}`); });
			pi.on("session_start", (event) => { record(`session_start:${event.reason}`); });
			pi.on("message_end", (event) => {
				if (event.message.role === "custom") record("message_end", (event.message.details as { envelopeId?: string })?.envelopeId);
			});
			pi.on("context", () => { record("context:before"); beforeContext?.(); });
			pi.on("agent_settled", () => { record("agent_settled:before"); });
			pi.on("tool_call", () => {
				failures.push(new Error(`${diagnostic}: synthetic streams must never call tools`));
				return { block: true, reason: "No tool execution in compatibility tests" };
			});
			workerExtension(pi, { createMailbox(callbacks, defaults) {
				transport = createMailboxTransport({ ...callbacks,
					async deliver(envelope, envelopeId) {
						deliveries.push(envelopeId);
						await callbacks.deliver(envelope, envelopeId);
					},
					warn(error) { failures.push(error); record("mailbox:warning"); },
				}, { ...defaults, root: mailboxRoot,
					makeFilename: (ts) => `${String(ts).padStart(15, "0")}-pi.json`,
					watch: () => ({ close: schedule() }), schedulePoll: () => ({ dispose: schedule() }),
					observeBoundary: (boundary) => record(`mailbox:${boundary.name}`, boundary.envelopeId),
				});
				return transport;
			} });
			pi.on("context", () => { record("context:after"); });
			pi.on("agent_settled", () => { record("agent_settled:after"); });
		} }],
	});
	await loader.reload();
	assert.deepEqual(loader.getExtensions().errors, [], diagnostic);
	assert.equal(loader.getExtensions().extensions.length, 1, `${diagnostic}: only the inline extension may load`);
	assert.deepEqual(loader.getSkills(), { skills: [], diagnostics: [] }, diagnostic);
	assert.deepEqual(loader.getPrompts(), { prompts: [], diagnostics: [] }, diagnostic);
	assert.deepEqual(loader.getThemes(), { themes: [], diagnostics: [] }, diagnostic);
	assert.deepEqual(loader.getAgentsFiles(), { agentsFiles: [] }, diagnostic);
	assert.deepEqual(loader.getAppendSystemPrompt(), [], diagnostic);
	const modelRuntime = await ModelRuntime.create({
		authPath: path.join(agentDir, "auth.json"), modelsPath: null,
		modelsStorePath: path.join(agentDir, "models-cache.json"), refreshOnCreate: false, allowModelNetwork: false,
	});
	const model = modelRuntime.getModel("anthropic", "claude-sonnet-4-5");
	assert.ok(model, `${diagnostic}: explicit static model must exist`);
	await modelRuntime.setRuntimeApiKey(model.provider, "mailbox-test-not-a-real-key");
	({ session } = await createAgentSession({ cwd, agentDir, sessionManager: manager, settingsManager,
		modelRuntime, model, resourceLoader: loader, thinkingLevel: "off", tools: [],
	}));
	session.agent.streamFunction = (_model, _context, streamOptions) => {
		const stream = createAssistantMessageEventStream();
		const message: AssistantMessage = {
			role: "assistant", content: [{ type: "text", text: `Synthetic response ${streams.length + 1}` }],
			api: model.api, provider: model.provider, model: model.id, timestamp: 1, stopReason: "stop",
			usage: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, totalTokens: 0,
				cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, total: 0 } },
		};
		const finish = (outcome: "stop" | "aborted" | "error") => {
			if (held.released) return;
			held.released = true;
			clearTimeout(timer);
			streamOptions?.signal?.removeEventListener("abort", onAbort);
			if (outcome !== "stop") stream.push({ type: "error", reason: outcome,
				error: { ...message, stopReason: outcome, errorMessage: outcome === "error" ? "Injected synthetic provider failure" : "Aborted" } });
			else stream.push({ type: "done", reason: "stop", message });
			stream.end();
			record(outcome === "stop" ? "stream:release" : `stream:${outcome}`);
		};
		const held: HeldStream = { released: false, release: () => finish("stop"), abort: () => finish("aborted"), fail: () => finish("error") };
		const onAbort = () => held.abort();
		const timer = setTimeout(() => {
			failures.push(new Error(`${diagnostic}: held assistant stream exceeded ${timeoutMs}ms`));
			held.abort();
		}, timeoutMs);
		streams.push(held);
		streamOptions?.signal?.addEventListener("abort", onAbort, { once: true });
		if (streamOptions?.signal?.aborted) held.abort();
		record("stream:held");
		return stream;
	};
	unsubscribe = session.subscribe((event) => { if (event.type === "agent_settled") record("session:idle"); });
	await session.bindExtensions({ mode: "tui", onError(error) { errors.push(error); record("extension:error"); } });
	assert.ok(transport, `${diagnostic}: worker must create a mailbox`);
	assert.ok(transport.isStarted(), `${diagnostic}: worker must own its isolated mailbox`);
	assert.equal(session.autoRetryEnabled, false, diagnostic);
	assert.equal(session.autoCompactionEnabled, false, diagnostic);

	async function waitFor(predicate: () => boolean, label: string) {
		if (predicate()) return;
		let listener: () => void = () => {};
		try {
			await bounded(new Promise<void>((resolve) => {
				listener = () => { if (predicate()) resolve(); };
				changes.on("change", listener);
				listener();
			}), label);
		} finally { changes.off("change", listener); }
	}

	return {
		root, cwd, sessionDir, inbox, manager, session, get transport() { return transport!; }, observations, streams, deliveries, errors, diagnostic,
		bounded, waitFor, dispose,
		get events() { return workerPi.events; },
		beforeContext(callback?: () => void) { beforeContext = callback; },
		async publishLifecycle() {
			transport!.writeEnvelope("self-pane", lifecycleEnvelope);
			await transport!.drainInbox();
			return "000000000000001-pi.json";
		},
		assertReloadResources() {
			assert.deepEqual(loader.getExtensions().errors, [], diagnostic);
			assert.equal(loader.getExtensions().extensions.length, 1, diagnostic);
			assert.deepEqual(loader.getSkills(), { skills: [], diagnostics: [] }, diagnostic);
			assert.deepEqual(loader.getPrompts(), { prompts: [], diagnostics: [] }, diagnostic);
			assert.deepEqual(loader.getThemes(), { themes: [], diagnostics: [] }, diagnostic);
			assert.deepEqual(loader.getAgentsFiles(), { agentsFiles: [] }, diagnostic);
			assert.deepEqual(loader.getAppendSystemPrompt(), [], diagnostic);
			assert.ok(schedules.slice(0, -2).every((schedule) => schedule.disposed), `${diagnostic}: old extension schedules must be disposed`);
			assert.equal(schedules.length, 4, diagnostic);
		},
		injectSendRejection(error: Error) {
			assert.equal(restoreSend, undefined, "only one targeted send fault per fixture");
			injectedSendError = error;
			const original = session!.sendCustomMessage;
			const calls: Parameters<AgentSession["sendCustomMessage"]>[] = [];
			session!.sendCustomMessage = async (...args) => { calls.push(args); throw error; };
			restoreSend = () => { session!.sendCustomMessage = original; restoreSend = undefined; };
			return { calls, restore: () => restoreSend?.() };
		},
		startPrompt() {
			const promise = session!.prompt("Hold this synthetic assistant response").catch((error) => { failures.push(error); });
			prompts.push(promise);
		},
		async publish(ts = 1, priority = false) {
			const envelope = { type: "message", ts, priority, message: `Mailbox payload ${ts}`,
				from: { id: "agent-scout", paneId: "worker-pane", name: "agent-scout", role: "worker" } };
			transport!.writeEnvelope("self-pane", envelope);
			await transport!.drainInbox();
			return { envelope, envelopeId: `${String(ts).padStart(15, "0")}-pi.json` };
		},
		reopen() {
			const file = manager.getSessionFile();
			if (!file || !fs.existsSync(file)) return [];
			return customEntries(SessionManager.open(file, sessionDir, cwd));
		},
		breakSessionDirectory() {
			assert.ok(manager.getSessionFile());
			assert.ok(fs.existsSync(manager.getSessionFile()!), `${diagnostic}: flush before inducing append failure`);
			const savedDir = path.join(root, "saved-sessions");
			fs.renameSync(sessionDir, savedDir);
			restoreDirectory = () => {
				fs.rmSync(sessionDir, { force: true });
				fs.renameSync(savedDir, sessionDir);
				restoreDirectory = undefined;
			};
			fs.writeFileSync(sessionDir, "Fixture-owned parent is now a regular file");
			return () => restoreDirectory?.();
		},
	};
}
