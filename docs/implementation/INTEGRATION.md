# Audit remediation: independent review and combined verification

20 September 2026. All eleven implementation assignments have accepted run-aware completion reports, published PRs, independent source/test review and passing independent checks. This document consolidates supervisor acceptance and supersedes the pending-review/live wording in the archived worker reports. No GitHub PR was merged.

The consolidated integration PR targets `main` and replaces the need to merge the original audit, specs and implementation stack separately. Original reviews remain linked below. Planning specs, the raw audit JSON and detailed worker reports are archived at verified commit [`08c77a0`](https://github.com/dragosdm/pi-herdr-workers/tree/08c77a09dedf2d308889e249d8e6418df169f4c5/docs), rather than carried in the maintained documentation. The historical audit, runtime contracts, regression tests and manual verification tools remain in the repository.

## Reviewed implementations

| Finding | PR | Reviewed commit | Independent full-suite pass count |
| --- | --- | --- | ---: |
| A01 monitor startup | [#14](https://github.com/dragosdm/pi-herdr-workers/pull/14) | `0ca320ee85ecc24c0961aac3f65fe37ca050d8bd` | 639 |
| A02 terminal output | [#18](https://github.com/dragosdm/pi-herdr-workers/pull/18) | `e26a38cf066ce3ce074fbaf1b95ef4845470c1db` | 693 |
| A03 creation/recovery | [#13](https://github.com/dragosdm/pi-herdr-workers/pull/13) | `131912d1ca071dfe156c8bc018b4dc04a3a786a0` | 584 |
| A04 worker readiness | [#15](https://github.com/dragosdm/pi-herdr-workers/pull/15) | `408f4154f66ce432f80421d4f644d9aba718d325` | 581 |
| A05 interval validation | [#17](https://github.com/dragosdm/pi-herdr-workers/pull/17) | `6341f9eccfac8a8dd002df013f6dcb1957558487` | 688 |
| A06 cron semantics | [#22](https://github.com/dragosdm/pi-herdr-workers/pull/22) | `30722542c7658bc70a6d81cb75d83e8acb214a6d` | 753 |
| A07 hybrid grammar | [#21](https://github.com/dragosdm/pi-herdr-workers/pull/21) | `84c14a01907b283ec7a0b082e2cdbadd8a1693ad` | 758 |
| A08 wake acknowledgement | [#19](https://github.com/dragosdm/pi-herdr-workers/pull/19) | `2bc62cf02846466a7fbecdc7dcde061b19df3996` | 628 |
| A09 pause checkpoint | [#12](https://github.com/dragosdm/pi-herdr-workers/pull/12) | `7958a37872ec8bbade1f7401089153542ad227ed` | 581 |
| A10 command routing | [#20](https://github.com/dragosdm/pi-herdr-workers/pull/20) | `42748b07553a697d3acad4ae4f6cb340bd949e95` | 785 |
| A11 waiting expiry | [#16](https://github.com/dragosdm/pi-herdr-workers/pull/16) | `08c8f0a899df7f462d422e66c52e6efc52eb2d6b` | 619 |

All listed revisions independently passed `npm run typecheck` and committed-range `git diff --check`. Dedicated positive regressions and each spec's definition of done were reviewed. Individual full suites still contained other findings' old characterizations; those counts alone were not treated as proof of combined correctness.

## Required live acceptance

Herdr 0.8.0, Node 24.15.0, live Pi 0.86.0. The opt-in audit driver directly invokes registered adapters, bypassing model/tool-schema validation. Tests used disposable workspaces, never the user's panes.

- **A01:** one canary execution after the first create; one long-command start; identical busy reuse attached without resubmission; stop returned to the shell. Review also required guarding widget failures so they cannot replace submission/retained-pane evidence. Final source and regressions passed independent review. [Supervisor record](https://github.com/dragosdm/pi-herdr-workers/pull/14#issuecomment-5748743286).
- **A02:** the initial live run failed readability despite successful execution: a five-terminal-row request returned empty output because the bottom rows were blank. The revision requests one bounded window of at least fifty rows, then retains five nonblank display rows. It adds honestly documented capture-derived markers and synthetic trailing-blank coverage. The revised run returned BEGIN/READY/END in direct capture and MonitorList before and after a confirmed session reload, using the same monitor/pane without relaunch. [Supervisor record](https://github.com/dragosdm/pi-herdr-workers/pull/18#issuecomment-5748859535).
- Each disposable driver was shut down, owned panes were positively verified shell-only, and only the recorded disposable workspaces were removed. User panes remained untouched. Private raw sessions are not committed.

No other finding required live acceptance. A04 uses controlled real-transport startup interleaving; A08 includes actual competing file-store subprocesses. Optional live checks were omitted.

## Combined branch and conflict review

Verification branch: `integration/audit-remediation`. Local integration merges contain every exact reviewed commit above; no implementation branch was rewritten. The tested code/test revision is `ec5e996`. Subsequent changes consolidate documentation only. Tests and production code are unchanged.

Resolved shared hunks deliberately:

- Combined A06's local-time numeric wording with A07's complete hybrid grammar.
- Retained both A03 preflight imports and A08 acknowledgement imports, plus both quarantine reasons and wake IDs in inspection.
- Kept A11's expiry-before-awaiting/filter order alongside A08's final-wake retirement provenance.
- Retained A05/A06/A07/A10 documentation, A08/A09 checkpoint/token guidance, and A11's unexpired-recovery qualification.
- Preserved all owned positive audit conversions; no `Known audit gap` cases remain in the combined audit test file.

Initial combined checks exposed expected cross-branch fixture incompatibilities: A05 delay tests and A11 update tests lacked A08's now-required wake identity. A11's tokenless seeded snapshots also triggered legacy normalization during restore/rollback. Only these fixtures were adapted: A05 admits an actual wake; A11's already-dispatched snapshot fixtures contain an identity and retain it for post-expiry rejection calls. Original checkpoint, boundary, no-mutation and persistence assertions remain. No production workaround or deleted coverage was needed.

Five new tests in `tests/extensions/remediation-integration.test.ts` verify:

1. An admitted waiting wake expires despite a rejecting work filter; its token cannot update or recreate it.
2. Final-cap and one-shot wakes each permit pause or completion before expiry, never continue.
3. Pause saves the supplied checkpoint, clears the token, preserves counters/lifetime and restores correctly.
4. Duplicate acknowledgement fails; expiry recovery deletes the retained paused controller.
5. The same final-wake exceptions reject at the inclusive expiry deadline.

## Combined checks

All exited zero, with no failed, skipped, canceled or todo tests:

| Command | Result |
| --- | --- |
| `npm run typecheck` | Passed |
| `npm test` | **1,203 passed** |
| `node --import tsx --test tests/extensions/remediation-integration.test.ts` | 5 passed |
| `TZ=UTC node --import tsx --test tests/extensions/cron-semantics.test.ts tests/extensions/remediation-integration.test.ts` | 70 passed |
| Same targeted command, `TZ=America/New_York` | 70 passed |
| Same targeted command, `TZ=Australia/Lord_Howe` | 70 passed |
| Same targeted command, `TZ=Asia/Kathmandu` | 70 passed |
| Same targeted command, `TZ=Pacific/Apia` | 70 passed |
| `git diff 5654978..HEAD --check` | Passed |

## Remaining boundaries

No required acceptance gate remains unmet. Merge approval is still required. The consolidated PR already includes the reviewed shared-hunk resolutions and fixture adaptations. The original stacked PRs are review history, not additional patches to merge afterward.

Tests do not prove exactly-once external execution, filesystem power-loss durability, exhaustive historical timezone behavior, or compatibility with every Herdr/Pi version. Cron search remains finite; monitor output remains a bounded observation; old shared-file writers must upgrade together for wake tokens. The pre-existing unsupported task-backlog stub still recommends tools inconsistently with ordinary dynamic ownership; this was disclosed and left outside the eleven fixes. See the [archived worker evidence](https://github.com/dragosdm/pi-herdr-workers/tree/08c77a09dedf2d308889e249d8e6418df169f4c5/docs/implementation) and historical audit for other non-remediated limitations.
