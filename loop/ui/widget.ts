import type { ExtensionUIContext } from "@earendil-works/pi-coding-agent";
import type { LoopStore } from "../store.js";

/** A compact, composable status rail. Detailed rosters belong in LoopList/MonitorList. */
export class LoopWidget {
  private ui: ExtensionUIContext | undefined;
  private previous: string | undefined;
  constructor(
    private store: LoopStore,
    private monitors: { list(): { status: string }[] },
    private nextFire: (id: string) => number | undefined = () => undefined,
  ) {}
  setUICtx(ui: ExtensionUIContext) { this.ui = ui; }
  setStore(store: LoopStore) { this.store = store; this.previous = undefined; }
  update() {
    if (!this.ui) return;
    const loops = this.store.list();
    const active = loops.filter((l) => l.status === "active");
    const paused = loops.filter((l) => l.status === "paused");
    const parts: string[] = [];
    if (active.length) parts.push(`↻ ${active.length} active`);
    if (paused.length) parts.push(`${paused.length} paused`);
    const next = active.map((l) => this.nextFire(l.id)).filter((n): n is number => n !== undefined);
    if (next.length) {
      const seconds = Math.max(0, Math.ceil((Math.min(...next) - Date.now()) / 1000));
      parts.push(seconds === 0 ? "next idle" : `next ${seconds < 60 ? `${seconds}s` : `${Math.ceil(seconds / 60)}m`}`);
    }
    const awaiting = active.filter((l) => l.dynamic?.awaitingUpdate).length;
    if (awaiting) parts.push(`${awaiting} awaiting update`);
    const monitors = this.monitors.list();
    if (monitors.length) {
      const unknown = monitors.filter((m) => m.status === "unknown").length;
      const failed = monitors.filter((m) => m.status === "error").length;
      parts.push(`${monitors.length} monitor${monitors.length === 1 ? "" : "s"}`);
      if (unknown) parts.push(`${unknown} unverified`);
      if (failed) parts.push(`${failed} unavailable`);
      // No fake output activity: live process status is explicitly refreshed by MonitorList.
    }
    const text = parts.join(" · ") || undefined;
    if (text !== this.previous) { this.previous = text; this.ui.setStatus("loops", text); }
  }
  dispose() {
    this.ui?.setStatus("loops", undefined);
    this.ui = undefined;
    this.previous = undefined;
  }
}
