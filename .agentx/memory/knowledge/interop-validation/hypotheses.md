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

The end-to-end catalog (native, Chromium, Firefox, WebKit, Android emulator, iOS simulator) is a proven CI gate. No hypothesis is active.

- **Rare stateful-resume stall (retired 2026-10-07, not reproduced under stress).** Once, in a local `check --epic` run on 2026-10-06, `android/json/stateful-reconnect` timed out after the third stateful resume (a late `DelayedConnectionIdentity` completion, then a `ConnectionIdentity` invocation that never completed on an open transport). With the payload-free sequence trace on (`rayx_signalr::stateful=debug`), the `stateful-resume-stress` scenario of `production-grade-client` S4 (500 server-side transport aborts per cell: five fresh stateful connections x 100, each followed by an invocation and a short stream, 20 s stall bound per operation) passed on native, Chromium and the Android emulator for JSON and MessagePack, and the 120 s `soak-chaos` scenario (random 0.5-5 s aborts under mixed traffic) found no lost, duplicated or reordered message. Since P4 a lost stateful transport is resumed once, transparently (D20), and sends and replies made meanwhile go out with the resend. The stall is retired for lack of reproduction (no cause was confirmed); if it recurs, the stress cell's trace names the lost sequence range.
