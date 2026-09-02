import type { RpcEventBus } from "../../rpc/client.js";

export class FakeEventBus implements RpcEventBus {
  private readonly listeners = new Map<string, Set<(payload: unknown) => void>>();
  readonly emissions: Array<{ channel: string; payload: unknown }> = [];

  on(channel: string, listener: (payload: unknown) => void): () => void {
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
    for (const listener of [...(this.listeners.get(channel) ?? [])]) listener(payload);
  }

  listenerCount(channel?: string): number {
    if (channel !== undefined) return this.listeners.get(channel)?.size ?? 0;
    return [...this.listeners.values()].reduce((total, listeners) => total + listeners.size, 0);
  }
}
