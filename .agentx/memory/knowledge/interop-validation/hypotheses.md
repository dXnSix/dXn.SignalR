---
agentx:
  kind: hypotheses
  scope: interop-validation
  status: current
  created: "2026-08-09T21:45:00+02:00"
  updated: "2026-10-06T20:00:00+02:00"
  topics:
    - ci
  supersedes: []
  superseded_by: []
---
# Hypotheses

The end-to-end catalog (native, Chromium, Firefox, WebKit, Android emulator, iOS simulator) is a proven CI gate. One hypothesis is active:

- **Rare stateful-resume stall (open, unexplained).** Once, in a local `check --epic` run on 2026-10-06, `android/json/stateful-reconnect` timed out: after the third stateful resume the `DelayedConnectionIdentity` completion arrived about 3.5 s late and the next `ConnectionIdentity` invocation never completed on a transport that stayed open, with no protocol error, server-timeout drop or further reconnect (diagnostics were `artifacts-temp/e2e/android-json-stateful-reconnect.json`). It did not reproduce in 8 isolated reruns, the repeated epic check, or any CI run; the emulator was under unrelated load. Candidates: a stateful sequence mismatch (the server discarding the call as a duplicate) or a stalled server buffer or hub slot. Code review found no sequencing defect against ASP.NET Core's `MessageBuffer`. To confirm or retire it, add debug-level logs of tracked sequence ids, Acks and the resend range and rerun with `RUST_LOG=rayx_signalr=debug` when it recurs.
