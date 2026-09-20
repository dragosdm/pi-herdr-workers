import { compileCronExpression, searchCron } from "./cron-search.js";

const UNIT_TO_CRON: Record<string, number> = {
  s: 1,
  m: 60,
  h: 3600,
  d: 86400,
};

const COMMON_INTERVALS: Record<number, string> = {
  60: "*/1 * * * *",
  120: "*/2 * * * *",
  300: "*/5 * * * *",
  600: "*/10 * * * *",
  900: "*/15 * * * *",
  1800: "*/30 * * * *",
  3600: "0 * * * *",
  7200: "0 */2 * * *",
  10800: "0 */3 * * *",
  14400: "0 */4 * * *",
  21600: "0 */6 * * *",
  28800: "0 */8 * * *",
  43200: "0 */12 * * *",
  86400: "0 0 * * *",
};

export const CRON_TIMING_NOTE = "Timing: local wall-clock cron with scheduler jitter, not an elapsed-time interval.";

// Recognition reserves duration-looking input for validation; it does not accept it.
export function matchIntervalPrefix(input: string): { interval: string; rest: string } | undefined {
  const trimmed = input.trim();
  const match = trimmed.match(/^([+-]?(?:\d+(?:\.\d*)?|\.\d+)(?:e[+-]?\d+)?|[+-]?(?:Infinity|NaN))\s*[smhd](?=\s|$)/i);
  if (!match) return undefined;
  return { interval: match[0], rest: trimmed.slice(match[0].length).trim() };
}

function describeInterval(seconds: number): string {
  const mins = seconds / 60;
  let description: string;
  if (mins < 60) {
    description = `${mins} minute${mins !== 1 ? "s" : ""}`;
  } else {
    const hrs = mins / 60;
    if (hrs % 24 === 0) {
      const days = hrs / 24;
      description = `${days} day${days !== 1 ? "s" : ""}`;
    } else {
      description = `${hrs} hour${hrs !== 1 ? "s" : ""}`;
    }
  }

  return description;
}

function isFullCron(expr: string): boolean {
  const parts = expr.trim().split(/\s+/);
  return parts.length === 5;
}

export function isValidCronExpression(expr: string): boolean {
  return compileCronExpression(expr) !== undefined;
}

function boundedIntervalInput(input: string): string {
  return input.length > 120 ? `${input.slice(0, 120)}…` : input;
}

export function parseInterval(input: string): { cron: string; description: string } {
  const trimmed = input.trim();

  if (isFullCron(trimmed)) {
    if (!isValidCronExpression(trimmed)) {
      throw new Error(`Invalid cron expression: ${trimmed}`);
    }
    return { cron: trimmed, description: `cron: ${trimmed}` };
  }

  const match = trimmed.match(/^(\d+)\s*(s|m|h|d)$/i);
  if (match) {
    const value = Number(match[1]);
    const unit = (match[2] ?? "").toLowerCase();
    const totalSec = value * UNIT_TO_CRON[unit]!;
    if (!Number.isSafeInteger(value) || value <= 0 || !Number.isSafeInteger(totalSec) || totalSec <= 0) {
      throw new Error(`Interval must be a positive safe integer duration: "${boundedIntervalInput(input)}".`);
    }
    const cron = COMMON_INTERVALS[totalSec];
    if (!cron) {
      throw new Error(`Unsupported cron interval "${boundedIntervalInput(input)}". Supported durations: 1m, 2m, 5m, 10m, 15m, 30m, 1h, 2h, 3h, 4h, 6h, 8h, 12h, 1d. No rounding is applied; use an explicit five-field cron for a different wall-clock schedule.`);
    }
    return { cron, description: describeInterval(totalSec) };
  }

  throw new Error(
    `Cannot parse interval "${boundedIntervalInput(input)}". Use supported integer-unit cron shorthand (e.g., "5m", "2h", "1d") or an explicit five-field cron expression.`
  );
}

export function cronToNextFire(cronExpr: string, fromDate: Date = new Date()): Date {
  return searchCron(cronExpr, fromDate);
}

export function computeJitter(taskId: string, recurring: boolean, scheduleMinutes: number): number {
  let hash = 0;
  for (let i = 0; i < taskId.length; i++) {
    hash = ((hash << 5) - hash) + taskId.charCodeAt(i);
    hash |= 0;
  }
  const normalized = Math.abs(hash % 10000) / 10000;

  if (recurring && scheduleMinutes <= 30) {
    return Math.floor(normalized * (scheduleMinutes / 2) * 60 * 1000);
  }
  if (recurring) {
    return Math.floor(normalized * 30 * 60 * 1000);
  }
  return Math.floor(normalized * 90 * 1000);
}
