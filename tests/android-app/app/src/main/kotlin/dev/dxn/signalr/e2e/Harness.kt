package dev.dxn.signalr.e2e

import android.content.Context

/** The JNI entry points of `tests/android-harness`. */
object Harness {
    init {
        System.loadLibrary("dxn_signalr_android_harness")
    }

    /**
     * Initializes the platform verifier with `dxn_signalr::tls::android::init_with_env`, as an
     * app does before its first secure connection. Returns `ok` or the error.
     */
    @JvmStatic
    external fun initialize(context: Context): String

    /**
     * Runs one shared scenario (`tls`, `connect-negotiate`, `invoke`) with `protocol` (`json` or
     * `messagepack`) and the scenario variables (`KEY=VALUE` lines). Returns `ok` or the failure.
     */
    @JvmStatic
    external fun runScenario(name: String, protocol: String, environment: String): String
}
