package dev.dxn.signalr.e2e

import android.app.admin.DeviceAdminReceiver

/**
 * The test-only device admin the runner makes device owner on the emulator
 * (`dpm set-device-owner`), so the instrumentation can install the run's test CA into the
 * device's user CA store and remove it again. The runner removes the admin after the run.
 */
class TestDeviceAdmin : DeviceAdminReceiver()
