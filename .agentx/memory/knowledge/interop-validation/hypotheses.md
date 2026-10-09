---
agentx:
  kind: hypotheses
  scope: interop-validation
  status: current
  created: "2026-08-09T21:45:00+02:00"
  updated: "2026-10-07T19:00:00+02:00"
  topics:
    - ci
  supersedes: []
  superseded_by: []
---
# Hypotheses

The end-to-end catalog (native, Chromium, Firefox, WebKit, Android emulator, iOS simulator) is a proven CI gate. Two hypotheses are active.

- **Missed callback on Windows CI (active, 2026-10-09).**
  - **Observed:** PR run 37854527851 attempt 1, job 113575403521. In `native/messagepack/callbacks` on `windows-latest`, a `TriggerEntityCallback` reply never reached the registered callback within 5 s, although `register` is synchronous and the trigger went out after it. Nothing was logged at `info`; the service logs were not captured.
  - **Since:** passed in every other run.
  - **Next step:** if it recurs, capture the service log and a `rayx_signalr=debug` trace of the receive path.
- **Not test failures:** attempt 1 of dispatched run 37854530727 also lost iOS and `tls-aws-lc (native, macos-latest)` to the account's Actions spending limit (jobs never started).

- **WebKit Long Polling connect hang on Linux runners (active, 2026-10-09).**
  - **Observed:** in `production-grade-client` S15 CI, `e2e / browser (webkit)` failed only in dispatched `e2e.yml` runs, four of four. Each time a Long Polling cell (`server-close` messagepack/json, `authentication` json) hung in a connect or a manual `reconnect()` until the 60 s cell bound. The same cells passed in all four pull-request runs, ten of ten in an isolated diagnostic run, and locally on Windows WebKit.
  - **Not the cause:** the code is the same for both events; no test code reads GitHub event variables.
  - **Suspects:** cold build caches on branch-ref runs (heavy fixture builds competing with the first cells), or WebKitGTK connection reuse after aborted polls.
  - **Next step:** if it recurs, capture a Playwright trace (network) of the hung cell; the fixture's `handler:` lines log manual reconnect timing.

- **Rare stateful-resume stall (retired 2026-10-07, not reproduced under stress).** Once, in a local `check --epic` run on 2026-10-06, `android/json/stateful-reconnect` timed out after the third stateful resume (a late `DelayedConnectionIdentity` completion, then a `ConnectionIdentity` invocation that never completed on an open transport). With the payload-free sequence trace on (`rayx_signalr::stateful=debug`), the `stateful-resume-stress` scenario of `production-grade-client` S4 (500 server-side transport aborts per cell: five fresh stateful connections x 100, each followed by an invocation and a short stream, 20 s stall bound per operation) passed on native, Chromium and the Android emulator for JSON and MessagePack, and the 120 s `soak-chaos` scenario (random 0.5-5 s aborts under mixed traffic; since 2026-10-09 each connection is spared 3 s after an abort, so a resume completes, and `stateful-resume-fallback` proves an interrupted resume) found no lost, duplicated or reordered message. Since P4 a lost stateful transport is resumed once, transparently (D20), and sends and replies made meanwhile go out with the resend. The stall is retired for lack of reproduction (no cause was confirmed); if it recurs, the stress cell's trace names the lost sequence range.
