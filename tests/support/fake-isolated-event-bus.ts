import type { RpcEventBus } from "../../rpc/client.js";

export class FakeIsolatedEventBus implements RpcEventBus {
	private readonly listeners = new Map<string, Set<(payload: unknown) => unknown>>();
	readonly emissions: Array<{ channel: string; payload: unknown }> = [];

	constructor(private readonly onEmission?: (channel: string, payload: unknown) => void) {}

	on(channel: string, listener: (payload: unknown) => unknown): () => void {
		const listeners = this.listeners.get(channel) ?? new Set();
		listeners.add(listener);
		this.listeners.set(channel, listeners);
		let subscribed = true;
		return () => {
			if (!subscribed) return;
			subscribed = false;
			listeners.delete(listener);
			if (listeners.size === 0) this.listeners.delete(channel);
		};
	}

	emit(channel: string, payload: unknown): void {
		this.emissions.push({ channel, payload });
		this.onEmission?.(channel, payload);
		for (const listener of [...(this.listeners.get(channel) ?? [])]) {
			try {
				const result = listener(payload);
				if (result && typeof (result as PromiseLike<unknown>).then === "function") {
					void Promise.resolve(result).catch(() => {});
				}
			} catch {}
		}
	}

	listenerCount(channel?: string): number {
		if (channel !== undefined) return this.listeners.get(channel)?.size ?? 0;
		return [...this.listeners.values()].reduce((total, listeners) => total + listeners.size, 0);
	}
}
