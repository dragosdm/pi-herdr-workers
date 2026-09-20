// Internal implementation shared by syntax validation and next-occurrence resolution.
// Not a timezone database or an unlimited next-occurrence service.
const FIELD_RANGES = [[0, 59], [0, 23], [1, 31], [1, 12], [0, 6]] as const;
const MINUTE_MS = 60_000;
const MAX_DATE_ITERATIONS = 146_100;

interface CronField {
  values: Set<number>;
  wildcardBased: boolean;
}
type CronFields = [CronField, CronField, CronField, CronField, CronField];

function numberInRange(input: string, min: number, max: number): number | undefined {
  if (!/^\d+$/.test(input)) return undefined;
  const value = Number(input);
  return value >= min && value <= max ? value : undefined;
}

function compileField(token: string, min: number, max: number): CronField | undefined {
  const values = new Set<number>();
  for (const item of token.split(",")) {
    const [base = "", stepToken, extra] = item.split("/");
    if (extra !== undefined) return undefined;
    const step = stepToken === undefined ? 1 : numberInRange(stepToken, 1, max - min + 1);
    if (step === undefined) return undefined;
    let start: number | undefined;
    let end: number | undefined;
    if (base === "*") {
      start = min;
      end = max;
    } else {
      const range = base.split("-");
      if (range.length === 1 && stepToken === undefined) {
        start = end = numberInRange(base, min, max);
      } else if (range.length === 2) {
        start = numberInRange(range[0]!, min, max);
        end = numberInRange(range[1]!, min, max);
      }
    }
    if (start === undefined || end === undefined || start > end) return undefined;
    for (let value = start; value <= end; value += step) values.add(value);
  }
  // Token order matters: "*,1" is wildcard-based, "1,*" is not.
  return { values, wildcardBased: token.startsWith("*") };
}

export function compileCronExpression(expression: string): CronFields | undefined {
  const tokens = expression.trim().split(/\s+/);
  if (tokens.length !== 5) return undefined;
  const fields = tokens.map((token, index) => {
    const [min, max] = FIELD_RANGES[index]!;
    return compileField(token, min, max);
  });
  if (fields.some(field => field === undefined)) return undefined;
  return fields as CronFields;
}

function dateMatches(fields: CronFields, date: Date): boolean {
  const [, , dom, month, dow] = fields;
  if (!month.values.has(date.getMonth() + 1)) return false;
  const domMatches = dom.values.has(date.getDate());
  const dowMatches = dow.values.has(date.getDay());
  return dom.wildcardBased || dow.wildcardBased ? domMatches && dowMatches : domMatches || dowMatches;
}

function nextLocalDayBoundary(date: Date): number {
  // Build midnight independently of the cursor's hour, including a repeated hour.
  // setFullYear avoids the Date constructor's special handling of years 0..99.
  const next = new Date(0);
  next.setHours(0, 0, 0, 0);
  next.setFullYear(date.getFullYear(), date.getMonth(), date.getDate() + 1);
  return next.getTime();
}

// Per-call test instrumentation, never a global cache or a change to the public helper.
interface SearchOptions {
  stats?: { dates: number; minutes: number };
  nextDayBoundary?: (date: Date) => number;
}

export function searchCron(expression: string, fromDate: Date, options: SearchOptions = {}): Date {
  const fields = compileCronExpression(expression);
  if (!fields) throw new Error(`Invalid cron expression: ${expression}`);
  const fromMs = fromDate.getTime();
  if (!Number.isFinite(fromMs)) throw new Error("Invalid cron start date");

  const horizon = new Date(fromMs);
  horizon.setFullYear(horizon.getFullYear() + 400, horizon.getMonth(), horizon.getDate() + 1);
  const endMs = horizon.getTime();
  if (!Number.isFinite(endMs) || endMs <= fromMs) {
    throw new Error("Unrepresentable cron search range: 400 years plus 1 day");
  }

  let cursor = Math.floor(fromMs / MINUTE_MS) * MINUTE_MS + MINUTE_MS;
  const nextDay = options.nextDayBoundary ?? nextLocalDayBoundary;
  for (let dates = 0; dates < MAX_DATE_ITERATIONS && cursor <= endMs; dates++) {
    if (options.stats) options.stats.dates++;
    const date = new Date(cursor);
    const boundary = nextDay(date);
    if (!Number.isFinite(boundary) || boundary <= cursor) {
      throw new Error("Invalid cron search date jump: next local day must be finite and increasing");
    }
    if (dateMatches(fields, date)) {
      for (; cursor < boundary && cursor <= endMs; cursor += MINUTE_MS) {
        if (options.stats) options.stats.minutes++;
        const candidate = new Date(cursor);
        // Recheck actual local fields after timezone normalization. Epoch advancement
        // visits both real instants in a fold and never invents a gap occurrence.
        if (dateMatches(fields, candidate)
          && fields[0].values.has(candidate.getMinutes())
          && fields[1].values.has(candidate.getHours())) return candidate;
      }
    }
    cursor = Math.ceil(boundary / MINUTE_MS) * MINUTE_MS;
  }
  throw new Error(`No matching time found within 400 years plus 1 day for cron expression: ${expression}`);
}
