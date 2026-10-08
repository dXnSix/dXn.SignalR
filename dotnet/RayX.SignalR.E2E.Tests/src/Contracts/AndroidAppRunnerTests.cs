using System.Text;
using RayX.SignalR.E2E.Tests.Scenarios;

namespace RayX.SignalR.E2E.Tests.Contracts;

/// <summary>
/// Proves the Android app runner (<c>android-app</c>, <c>android-app-release</c>): the harness and
/// Gradle invocations, the device-owner set-up, the instrumentation arguments, result parsing and
/// the classification of a missing toolchain.
/// </summary>
public sealed class AndroidAppRunnerTests
{
  [Fact]
  [Trait("Category", "Catalog")]
  public void TheHarnessBuildsForTheEmulatorIntoItsOwnTargetDirectory()
  {
    Infrastructure.ProcessCommand ring = AndroidAppCellRunner.HarnessBuildCommand("/repo", TlsProvider.Ring);
    Assert.Equal(
      ["ndk", "-t", "x86_64", "-P", "24", "build", "--manifest-path", AndroidAppCellRunner.HarnessManifest],
      ring.Arguments);
    Assert.Equal(Path.Combine("/repo", "target/android-harness"), ring.Environment["CARGO_TARGET_DIR"]);

    Infrastructure.ProcessCommand aws = AndroidAppCellRunner.HarnessBuildCommand("/repo", TlsProvider.AwsLcRs);
    Assert.Equal(["--no-default-features", "--features", "tls-aws-lc-rs"], aws.Arguments.TakeLast(3));
    Assert.Equal(Path.Combine("/repo", "target/android-harness-aws-lc"), aws.Environment["CARGO_TARGET_DIR"]);
  }

  [Theory]
  [Trait("Category", "Catalog")]
  [InlineData(false, ":app:assembleDebug", "debug/app-debug.apk")]
  [InlineData(true, ":app:assembleRelease", "release/app-release.apk")]
  public void GradleAssemblesTheVariantWithTheBuiltLibrary(bool release, string task, string apk)
  {
    Infrastructure.ProcessCommand gradle = AndroidAppCellRunner.GradleCommand("/repo", release, "/repo/jni");
    Assert.EndsWith(OperatingSystem.IsWindows() ? "gradlew.bat" : "gradlew", gradle.FileName, StringComparison.Ordinal);
    Assert.Equal(Path.Combine("/repo", AndroidAppCellRunner.ProjectDirectory), gradle.WorkingDirectory);
    Assert.Equal(["--no-daemon", "--console=plain", "-Prayx.jniLibs=/repo/jni", task], gradle.Arguments);
    Assert.EndsWith(apk.Replace('/', Path.DirectorySeparatorChar), AndroidAppCellRunner.ApkPath(release), StringComparison.Ordinal);
  }

  [Fact]
  [Trait("Category", "Catalog")]
  public void TheAppBecomesDeviceOwnerAndIsRemovedAfterTheRun()
  {
    (string[] setUp, string[] cleanUp) = AndroidAppCellRunner.DeviceOwnerCommands();
    Assert.Equal(["shell", "dpm", "set-device-owner", "dev.rayx.signalr.e2e/.TestDeviceAdmin"], setUp);
    Assert.Equal(["shell", "dpm", "remove-active-admin", "dev.rayx.signalr.e2e/.TestDeviceAdmin"], cleanUp);
    Assert.NotNull(AndroidAppCellRunner.DeviceOwnerUnavailable(
      "java.lang.IllegalStateException: Not allowed to set the device owner because there are already some accounts on the device"));
    Assert.Null(AndroidAppCellRunner.DeviceOwnerUnavailable("Success: Device owner set to package dev.rayx.signalr.e2e"));
  }

  [Fact]
  [Trait("Category", "Catalog")]
  public void InstrumentationArgumentsCarryTheVariablesWithoutShellQuoting()
  {
    string[] arguments = AndroidAppCellRunner.InstrumentArguments(
      "tls",
      "messagepack",
      new Dictionary<string, string>
      {
        ["RAYX_SIGNALR_E2E_TRICKY"] = "it's $HOME; & 'quoted'",
        ["RAYX_SIGNALR_E2E_SERVICE_URL"] = "http://10.0.2.2:5000"
      });
    Assert.Equal(
      ["shell", "am", "instrument", "-w", "-e", "scenario", "tls", "-e", "protocol", "messagepack", "-e", "environment"],
      arguments.Take(12));
    Assert.Equal(AndroidAppCellRunner.Instrumentation, arguments[^1]);
    string encoded = arguments[^2];
    Assert.Matches("^[A-Za-z0-9+/=]+$", encoded);
    Assert.Equal(
      "RAYX_SIGNALR_E2E_SERVICE_URL=http://10.0.2.2:5000\nRAYX_SIGNALR_E2E_TRICKY=it's $HOME; & 'quoted'",
      Encoding.UTF8.GetString(Convert.FromBase64String(encoded)));
  }

  [Fact]
  [Trait("Category", "Catalog")]
  public void InstrumentationResultsAreParsed()
  {
    Assert.True(AndroidAppCellRunner.ParseInstrumentation("INSTRUMENTATION_RESULT: result=ok\r\nINSTRUMENTATION_CODE: -1\r\n").Passed);

    (bool passed, string failure) = AndroidAppCellRunner.ParseInstrumentation(
      "INSTRUMENTATION_RESULT: result=the scenario panicked: UnknownIssuer\nINSTRUMENTATION_CODE: 0\n");
    Assert.False(passed);
    Assert.Contains("UnknownIssuer", failure, StringComparison.Ordinal);

    (passed, failure) = AndroidAppCellRunner.ParseInstrumentation(
      "INSTRUMENTATION_STATUS: shortMsg=Process crashed.\nINSTRUMENTATION_CODE: 0\n");
    Assert.False(passed);
    Assert.Contains("did not report a result", failure, StringComparison.Ordinal);

    Assert.False(AndroidAppCellRunner.ParseInstrumentation("INSTRUMENTATION_RESULT: result=ok\nINSTRUMENTATION_CODE: 0\n").Passed);
  }

  [Theory]
  [Trait("Category", "Catalog")]
  [InlineData("ERROR: JAVA_HOME is not set and no 'java' command could be found in your PATH.")]
  [InlineData("SDK location not found. Define a valid SDK location with an ANDROID_HOME environment variable")]
  [InlineData("> Could not GET 'https://dl.google.com/dl/android/maven2/com/android/tools/build/gradle/9.1.0/gradle-9.1.0.pom'.")]
  public void AMissingToolchainIsAnEnvironmentError(string output)
  {
    Assert.StartsWith("environment error:", AndroidAppCellRunner.MissingToolchain(output), StringComparison.Ordinal);
  }

  [Theory]
  [Trait("Category", "Catalog")]
  [InlineData("e: ScenarioInstrumentation.kt:12:5 Unresolved reference 'x'.")]
  [InlineData("> Could not resolve all files for configuration ':app:debugRuntimeClasspath'.\n   > Could not find org.rustls:rustls-platform-verifier:9.9.9.")]
  public void ABuildErrorIsNotAnEnvironmentError(string output)
  {
    Assert.Null(AndroidAppCellRunner.MissingToolchain(output));
  }

  [Theory]
  [Trait("Category", "Catalog")]
  [InlineData("emulator-5554", "1\r\n", null, true)]
  [InlineData("emulator-5554", "", null, true)]
  [InlineData("R58M123", "1", null, true)]
  [InlineData("R58M123", "", null, false)]
  [InlineData("R58M123", "0", "0", false)]
  [InlineData("R58M123", "", "1", true)]
  public void OnlyAnEmulatorIsChangedUnlessSystemChangesAreAllowed(string serial, string qemu, string? allow, bool expected)
  {
    Assert.Equal(expected, AndroidAppCellRunner.MayChangeDevice(serial, qemu, allow));
  }

  [Fact]
  [Trait("Category", "Catalog")]
  public void TheAppReceivesOnlyTheVariablesItsScenariosRead()
  {
    Dictionary<string, string> app = AndroidAppCellRunner.ForApp(new Dictionary<string, string>
    {
      ["RAYX_SIGNALR_E2E_SERVICE_URL"] = "http://10.0.2.2:5000",
      ["RAYX_SIGNALR_E2E_PROXY_CREDENTIALS"] = "rayx:secret",
      ["RAYX_SIGNALR_E2E_PROXY_URL"] = "http://10.0.2.2:6000",
      ["RAYX_SIGNALR_E2E_TLS_CA_PEM_BASE64"] = "Y2E=",
      ["RUST_LOG"] = "rayx_signalr=info"
    });
    Assert.Equal(
      ["RAYX_SIGNALR_E2E_PROXY_URL", "RAYX_SIGNALR_E2E_SERVICE_URL", "RAYX_SIGNALR_E2E_TLS_CA_PEM_BASE64", "RUST_LOG"],
      app.Keys.Order(StringComparer.Ordinal));
    Assert.DoesNotContain("RAYX_SIGNALR_E2E_PROXY_CREDENTIALS", app.Keys);
  }

  [Fact]
  [Trait("Category", "Catalog")]
  public void AMultiLineFailureIsKeptWhole()
  {
    (bool passed, string failure) = AndroidAppCellRunner.ParseInstrumentation(
      "INSTRUMENTATION_RESULT: result=the scenario panicked: assertion failed\n  left: 1\n right: 2\nINSTRUMENTATION_CODE: 0\n");
    Assert.False(passed);
    Assert.Contains("right: 2", failure, StringComparison.Ordinal);
  }

  [Fact]
  [Trait("Category", "Catalog")]
  public void DiagnosticsReadTheHarnessLogAndCleanUpRemovesTheTestCas()
  {
    Assert.Contains($"{AndroidAppCellRunner.LogTag}:V", AndroidAppCellRunner.LogcatArguments);
    Assert.Contains("AndroidRuntime:E", AndroidAppCellRunner.LogcatArguments);
    Assert.Equal(
      ["shell", "am", "instrument", "-w", "-e", "cleanup", "true", AndroidAppCellRunner.Instrumentation],
      AndroidAppCellRunner.CleanupArguments);
  }

  [Fact]
  [Trait("Category", "Catalog")]
  public void TheAppTargetsRunTheTlsProvingScenarios()
  {
    string[] app = ["android-app", "android-app-release"];
    foreach(string target in app)
    {
      // The debug app also proves the system proxy (java.net.ProxySelector through JNI); both
      // prove the typed errors.
      string[] expected = target == "android-app"
        ? ["connect-negotiate", "error-kinds", "invoke", "os-proxy", "tls"]
        : ["connect-negotiate", "error-kinds", "invoke", "tls"];
      Assert.Equal(TargetKind.Native, ScenarioCatalog.Target(target).Kind);
      Assert.Equal(
        expected,
        ScenarioCatalog.Scenarios.Where(scenario => scenario.Targets.Contains(target)).Select(scenario => scenario.Name).Order(StringComparer.Ordinal));
      Assert.IsType<AndroidAppCellRunner>(E2ECellTests.RunnerFor(target));
    }
  }
}
