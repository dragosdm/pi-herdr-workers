# Audit remediation plan

These eleven specifications address the findings in [the live audit](../audits/2026-09-20-live-extension.md). The coordinator reviewed the documents against production source and accepted run reports. Specification approval authorizes implementation, not acceptance of a fix or permission to merge.

## Implementation and PR order

The common implementation base is this specs branch, `specs/audit-findings`. Its PR targets `audit/live-extension-validation` while audit PR #10 is unmerged. Each finding gets a separate worktree, branch, implementation worker and PR. A dependent worker starts only after its parent implementation has passed coordinator review. No GitHub PR is merged by this workflow.

| Finding | Subject | Implementation parent and PR base |
| --- | --- | --- |
| [A01](A01.md) | Monitor startup and truthful submission results | Common specs base |
| [A02](A02.md) | Raw terminal output tails | Reviewed A01 branch |
| [A03](A03.md) | Creation preflight and isolated recovery | Common specs base |
| [A04](A04.md) | Retain authorized startup readiness | Common specs base |
| [A05](A05.md) | Reject unsupported interval shorthand | Reviewed A03 branch |
| [A06](A06.md) | Cron day semantics and bounded sparse search | Reviewed A05 branch, including A03 |
| [A07](A07.md) | Complete hybrid grammar | Reviewed A05 branch, including A03 |
| [A08](A08.md) | Once-per-wake acknowledgement tokens | Reviewed A09 branch |
| [A09](A09.md) | Atomic pause checkpoint | Common specs base |
| [A10](A10.md) | Reject scheduled commands without prompts | Reviewed A05 branch, including A03 |
| [A11](A11.md) | Expiry while awaiting an update | Reviewed A03 branch |

The first parallel group is A01, A03, A04 and A09. After parent review, A02, A05, A08 and A11 can start as their bases become available. A06, A07 and A10 can then run independently on A05. These edges include shared-file coordination, not only runtime prerequisites.

## Resolved design choices

- A01 precedes A02. A02's raw execution helper must retain A01's timeout override, combined cancellation and additive launch-state field. A02's instruction not to change snapshots means no additional A02 change, not removal of A01 metadata.
- A03 keeps resolver failure separate from persistence failure. Valid occurrences beyond a controller's lifetime remain expiry-only. A06 changes which schedules the resolver can satisfy without replacing A03's transaction or quarantine behavior.
- A05 deliberately rejects non-table shorthand instead of approximating it. Explicit cron and dynamic elapsed `nextInterval` remain separate contracts. A07 consumes the shared parser; A10 preserves A05's duration-prefix routing.
- A06 retains the numeric grammar and explicitly documents leading-star day semantics. Its longer search must skip ineligible calendar dates and have measurable work bounds, not merely a larger minute-scan limit.
- A09 precedes A08 despite A08's local ordering recommendation. The smaller atomic checkpoint fix establishes tested pause semantics first. A08 must preserve that merge inside its token-checked transaction and adapt A09 tests to real wake admission. It must not restore A09's historical broken assertions.
- The required A08 `wakeId` is an intentional input compatibility change. Token admission, tool schema, wake text, inspection, migration and locked acceptance must ship together. Tokenless fallback and implicit resume are prohibited. Ordinary dynamic handling must not take over task-backlog, workflow or orchestration-owned execution.
- A11 can proceed on A03 independently of A08 because it owns pump ordering, not token admission. Combined testing must verify expiry invalidates pending acknowledgements while final-cap and one-shot acknowledgements still work before expiry.
- A04 retains only the exact parent-authorized pending readiness envelope. Retention never bypasses normal peer validation, custom-message persistence or lifecycle acceptance. The live symptom is confirmed; the exact historical arrival order is not.

Where this index resolves an ordering conflict, it takes precedence over tentative ordering in individual specs. Behavioral definitions and acceptance checks remain in those specs.

## Review and evidence gates

Every implementation worker must:

1. Read its spec, this plan and inherited fixes. Implement only its finding.
2. Add positive targeted regressions, demonstrate the primary baseline failure where required, and invert only its owned audit characterizations. Minimal fixture adaptations for inherited API changes must preserve the original assertion's meaning.
3. Run targeted tests, `npm run typecheck`, full `npm test` and `git diff --check` on its final revision. Record actual counts and output, not historical totals.
4. Commit, push and create a focused PR against the exact parent branch above. Report commit, PR URL, changed files and evidence against every definition-of-done item. Disclose missing live evidence and other blockers.
5. Address coordinator review in the same worktree. A worker completion report alone is not acceptance.

A01 and A02 require their bounded disposable live checks before full acceptance. Coordinate those checks with the supervisor; do not use valuable user panes or claim unit tests satisfy a live gate. Other live checks are optional as stated in their specs. Tests must use bounded foreground execution rather than the known-broken monitor path.

After individual review, the coordinator combines branches in a separate integration worktree without merging GitHub PRs. Resolve shared README, tool/store, scheduler and audit-test hunks deliberately. Run all dedicated regressions, typecheck, full suite and whitespace checks on the combined commit, including A06's timezone matrix and combined A08/A09/A11 behavior. Report all eleven PRs, review outcomes and any unmet acceptance criterion.

Historical audit observations and evidence JSON remain unchanged. A dated finding-specific resolution note may link later evidence. No claim of exhaustive regression freedom or crash-proof exactly-once execution is authorized.
