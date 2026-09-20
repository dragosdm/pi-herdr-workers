export const HYBRID_TRIGGER_EXAMPLE = "cron: */5 * * * * event: audit:test";

/** Extract clauses only. The shared interval parser owns schedule validation. */
export function parseHybridTriggerInput(input: string): { schedule: string; eventSource: string } {
  const text = input.trim().replace(/\s+/g, " ");
  const cronFirst = /^cron(?:: *| +)(.+?) +event(?:: *| +)(\S+)$/.exec(text);
  const eventFirst = /^event(?:: *| +)(\S+) +cron(?:: *| +)(.+)$/.exec(text);
  const cronOnly = /^cron(?:: *| +)(.+)$/.exec(text);

  const schedule = (cronFirst?.[1] ?? eventFirst?.[2] ?? cronOnly?.[1] ?? text).trim();
  const eventSource = cronFirst?.[2] ?? eventFirst?.[1] ?? "tool_execution_start";

  // A failed labeled alternative must not silently fall back to a partial schedule.
  // Check whole label tokens, not colons inside the separately captured source.
  if (!schedule || /(?:^| )(?:cron|event)(?=:| |$)/.test(schedule)) {
    throw new Error(`Invalid hybrid trigger: missing schedule or invalid/repeated cron or event clause. Use ${HYBRID_TRIGGER_EXAMPLE}`);
  }

  return { schedule, eventSource };
}
