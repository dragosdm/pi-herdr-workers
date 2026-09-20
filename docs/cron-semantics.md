# Cron semantics

`/loop` and `LoopCreate` accept numeric five-field cron in the host process's local timezone. Cron schedules wall-clock slots, not elapsed intervals. [Supported shorthand](../README.md#loop--re-wake-this-agent) is a separate, fixed table; this dialect does not change it.

## Grammar

Separate five fields with whitespace. Leading/trailing whitespace is allowed.

| Field | Range |
| --- | --- |
| Minute | `0..59` |
| Hour | `0..23` |
| Day of month | `1..31` |
| Month | `1..12` |
| Weekday | `0..6`, Sunday is `0` |

Each field accepts `*`, an unsigned decimal integer, an ascending inclusive range such as `1-5`, or comma-separated unions of those forms. A `/step` suffix is allowed on `*` or a range. Steps must be positive integers no larger than the field's width. They start at the field minimum for `*`, or at the range's lower bound. For example, day-of-month `*/2` selects `1,3,5,...,31`; `3-9/3` selects `3,6,9`. Duplicate items and leading zeroes are accepted.

Weekday `7`, descending ranges, empty list items, scalar steps such as `5/10`, zero or oversized steps, signs, fractions, names, macros, seconds/year fields, `CRON_TZ`, `L`, `W`, `#`, and `?` are not supported. This is a numeric subset, not complete compatibility with every cron implementation.

Syntax validation does not prove calendar feasibility. `0 0 31 2 *` is syntactically valid but has no supported occurrence, so creation rejects it without saving a controller.

## How the day fields combine

A day token is wildcard-based only when its first character is `*`. This includes `*/n` and lists beginning with `*`. Do not infer that flag from the set of selected days.

- If either day token is wildcard-based, both day predicates must match.
- Otherwise, either day predicate may match.
- Minute, hour and month always remain mandatory.

| Expression | Matching local dates at midnight |
| --- | --- |
| `0 0 1 * 1` | Every first of the month and every Monday |
| `0 0 * * 1` | Mondays |
| `0 0 1 * *` | First of the month |
| `0 0 * * *` | Every date |
| `0 0 */2 * 1` | Mondays with odd day-of-month numbers |
| `0 0 1-31 * 1` | Every date; `1-31` is not wildcard-based |
| `0 0 *,1 * 1` | Mondays; the DOM token begins with `*` |
| `0 0 1,* * 1` | Every date; the DOM token begins with `1` |
| `0 0 31 2 1` | Mondays in February |

Token order also matters in weekday lists. `0 0 1 * *,1` matches only the first of the month; `0 0 1 * 1,*` matches every date.

## Time, search bounds and delivery

The next occurrence is strictly later than the supplied start time. On modern dates it is a minute boundary with zero seconds and milliseconds. The search copies the start date rather than mutating it.

Nonexistent spring-forward times are skipped, not shifted to another hour. Both real instants of a repeated fall-back minute are eligible. Searching after the first instance can return the second. Calendar day boundaries account for non-hour transitions, absent midnights and skipped dates. The deterministic tests cover selected modern transitions in New York, Lord Howe, Apia and São Paulo, plus UTC and Kathmandu; they do not establish compatibility with all historical timezone rules.

Search ends at the local calendar date 400 years plus one day after the start, preserving the start's wall-clock time and including that endpoint. A defensive limit of 146,100 visited dates also applies. Ineligible calendar dates are skipped without scanning their minutes. This supports leap-century gaps and sparse weekday intersections such as `0 0 29 2 */7`, which can wait 28 years. It is not an unlimited search service.

Exhaustion throws `No matching time found within 400 years plus 1 day ...`. Invalid start dates and unrepresentable search ranges have separate errors. The synchronous search finishes or throws within its work bounds; it is not preemptible or abort-signal driven.

The scheduler adds stable, nonnegative per-ID jitter to the occurrence. For recurring cron, a `*/N` minute step of at most 30 uses half that step as its jitter bound; larger steps use a 30-minute bound. Other minute fields use the existing step fallback of 30, giving a 15-minute bound. Nonrecurring jitter is below 90 seconds. Busy agents can delay delivery further, and missed occurrences are not replayed. Two eligible fold instants do not guarantee two delivered wakes regardless of jitter or agent availability.

## Lifetime and restoration

A valid occurrence beyond a controller's remaining seven-day lifetime is still accepted. Its schedule does not extend the lifetime. If the next occurrence plus jitter is at or beyond expiry, the scheduler retains only its expiry registration; `nextFire(id)` returns `undefined`. A hybrid controller can still fire from its event side before expiry. Ordinary loops are deleted on expiry; workflow and other retained controller types follow their existing pause rules.

On restoration or re-arm, persisted expressions use the corrected day semantics. Two restricted day fields now use OR and may next wake sooner than under the old AND matcher. There is no legacy mode, stored-expression rewrite or replay. IDs, counters, creation/expiry times and paused status are not reset. Changing the host timezone between sessions recomputes schedules in the new zone; expressions do not pin a timezone.

Truly unavailable restored schedules still follow A03's administrative quarantine behavior. A valid leap-day schedule is expiry-only, not invalid or quarantined.
