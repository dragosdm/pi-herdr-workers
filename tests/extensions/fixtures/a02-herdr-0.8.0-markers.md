# A02 disposable capture provenance

`a02-herdr-0.8.0-markers.txt` contains the exact three marker rows extracted by the supervisor from a disposable Herdr 0.8.0 pane on 20 September 2026. The extraction retains marker bytes and LF line endings, but omits the echoed command, shell prompt and private filesystem path. It is a sanitized subset, not the entire raw stdout.

- Tested implementation: `e68bd2d43c45f406453c22c523f41b61a9c8d958`.
- Disposable pane: `w8:p2`. The supervisor confirmed the bounded printf executed once and returned to the shell.
- Direct request: `herdr pane read w8:p2 --source recent-unwrapped --lines 5 --format text`. Its stdout was empty, zero bytes.
- Bounded diagnostic request: `herdr pane read w8:p2 --source recent-unwrapped --lines 50 --format text`. Its stdout was 295 UTF-8 bytes across six returned rows, with the marker rows at positions two through four.
- Both initial and post-reload MonitorList returned `(no output captured)` at this revision. The live readability gate failed; command startup was verified separately.
- Source artifacts were `direct-stdout.txt`, `direct-50-stdout.txt` and `sanitized-markers.txt` in the supervisor's private `live-a02` evidence directory. The implementation worker copied only `sanitized-markers.txt` and inspected byte/row counts without retaining private prompt/path content. No driver/session log is committed.

Herdr's requested row window can select only blank terminal rows and return empty stdout. Increasing that bounded window exposed the markers. The returned 50-row capture itself did not retain trailing blank rows. The regression's appended blank screen rows are therefore explicitly synthetic, not claimed as captured bytes or an observed blank-row count. They model capture-window selection separately from text normalization.

The wider-window implementation still needs a supervisor live rerun before readable output across reload is accepted. This fixture does not claim that rerun passed or that cleanup was independently verified by the implementation worker.
