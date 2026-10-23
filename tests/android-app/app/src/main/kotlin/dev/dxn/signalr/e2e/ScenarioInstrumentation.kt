package dev.dxn.signalr.e2e

import android.app.Activity
import android.app.Instrumentation
import android.app.admin.DevicePolicyManager
import android.content.ComponentName
import android.content.Context
import android.os.Bundle
import android.util.Base64

/**
 * Runs one shared scenario inside the app process and reports `result` (`ok` or the failure)
 * through [finish]. Arguments: `scenario`, `protocol` and `environment` (base64 of `KEY=VALUE`
 * lines with the scenario variables).
 *
 * The platform verifier trusts the device's CA store (system and user CAs), so the run's test CA
 * (`DXN_SIGNALR_E2E_TLS_CA_PEM_BASE64`) is installed as a user CA for the scenario, as a device
 * policy installs a private CA, and removed afterwards. That needs the app to be device owner,
 * which the runner sets up on the emulator. With `-e cleanup true` the instrumentation only removes
 * every user CA the admin installed (after a cell that was killed).
 */
class ScenarioInstrumentation : Instrumentation() {
    private var arguments = Bundle()

    override fun onCreate(arguments: Bundle?) {
        super.onCreate(arguments)
        this.arguments = arguments ?: Bundle()
        start()
    }

    override fun onStart() {
        super.onStart()
        val outcome = try {
            run()
        } catch (error: Throwable) {
            "exception: $error"
        }
        val results = Bundle()
        results.putString("result", outcome)
        finish(if (outcome == "ok") Activity.RESULT_OK else Activity.RESULT_CANCELED, results)
    }

    private fun run(): String {
        val policy = targetContext.getSystemService(Context.DEVICE_POLICY_SERVICE) as DevicePolicyManager
        val admin = ComponentName(targetContext, TestDeviceAdmin::class.java)
        if (arguments.getString("cleanup") == "true") {
            policy.uninstallAllUserCaCerts(admin)
            return "ok"
        }
        val environment = String(
            Base64.decode(arguments.getString("environment").orEmpty(), Base64.DEFAULT),
            Charsets.UTF_8,
        )
        val variables = environment.lines()
            .mapNotNull { line -> line.split('=', limit = 2).takeIf { it.size == 2 } }
            .associate { (key, value) -> key to value }
        val initialized = Harness.initialize(targetContext)
        if (initialized != "ok") {
            return "initialize failed: $initialized"
        }
        val ca = variables["DXN_SIGNALR_E2E_TLS_CA_PEM_BASE64"]?.let { Base64.decode(it, Base64.DEFAULT) }
        if (ca != null && !policy.installCaCert(admin, ca)) {
            return "the test CA could not be installed as a user CA"
        }
        try {
            return Harness.runScenario(
                arguments.getString("scenario").orEmpty(),
                arguments.getString("protocol").orEmpty(),
                environment,
            )
        } finally {
            if (ca != null) {
                policy.uninstallCaCert(admin, ca)
            }
        }
    }
}
