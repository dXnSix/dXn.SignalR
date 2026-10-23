using dXn.SignalR.E2E.Tests.Infrastructure;
using dXn.SignalR.E2E.Tests.Scenarios;

namespace dXn.SignalR.E2E.Tests.Contracts;

/// <summary>Proves the Android and iOS cell runners' device selection, quoting and endpoint rewriting.</summary>
public sealed class MobileRunnerTests
{
  [Fact]
  [Trait("Category", "Catalog")]
  public void AndroidUsesTheConfiguredSerialOrTheSingleRunningEmulator()
  {
    const string devices = "List of devices attached\nemulator-5554\tdevice\nR58M123\tdevice\nemulator-5556\toffline\n";
    Assert.Equal("emulator-5554", AndroidCellRunner.SelectSerial(null, devices));
    Assert.Equal("R58M123", AndroidCellRunner.SelectSerial(" R58M123 ", devices));

    InvalidOperationException none = Assert.Throws<InvalidOperationException>(
      () => AndroidCellRunner.SelectSerial(null, "List of devices attached\n"));
    Assert.Contains(AndroidCellRunner.SerialVariable, none.Message, StringComparison.Ordinal);
    Assert.Throws<InvalidOperationException>(
      () => AndroidCellRunner.SelectSerial(null, "emulator-5554\tdevice\nemulator-5556\tdevice\n"));
  }

  [Fact]
  [Trait("Category", "Catalog")]
  public void AndroidShellCommandQuotesEveryVariableAndRunsOneExactTest()
  {
    string command = AndroidCellRunner.ShellCommand(
      "invoke::json",
      new Dictionary<string, string>
      {
        ["DXN_SIGNALR_E2E_SERVICE_URL"] = "http://10.0.2.2:5000",
        ["DXN_SIGNALR_E2E_TRICKY"] = "it's"
      });
    Assert.Equal(
      $"cd {AndroidCellRunner.DeviceDirectory} && env DXN_SIGNALR_E2E_SERVICE_URL='http://10.0.2.2:5000' "
      + "DXN_SIGNALR_E2E_TRICKY='it'\\''s' ./e2e invoke::json --exact --test-threads=1 --nocapture",
      command);
  }

  [Fact]
  [Trait("Category", "Catalog")]
  public void AndroidKillsATimedOutCellByTheLaunchedProcessName()
  {
    string command = AndroidCellRunner.ShellCommand("invoke::json", new Dictionary<string, string>());
    Assert.Contains($" ./{AndroidCellRunner.DeviceProcessName} invoke::json ", command, StringComparison.Ordinal);
    // An exact process-name match never matches the shell that runs the kill command.
    Assert.Equal($"pkill -x {AndroidCellRunner.DeviceProcessName}", AndroidCellRunner.KillCommand);
  }

  [Fact]
  [Trait("Category", "Catalog")]
  public void AndroidBuildsIntoItsOwnTargetDirectory()
  {
    Infrastructure.ProcessCommand command = AndroidCellRunner.BuildCommand("/repo");
    Assert.Equal(Path.Combine("/repo", AndroidCellRunner.TargetDirectory), command.Environment["CARGO_TARGET_DIR"]);
    Assert.Equal(["--features", "messagepack,proxy-pac,http3,tls-webpki-roots"], command.Arguments.Skip(8).Take(2));
    Assert.DoesNotContain("--all-features", command.Arguments);
  }

  [Fact]
  [Trait("Category", "Catalog")]
  public void IosAcceptsAPassedTestWhoseLauncherDidNotExitButNotAnUnfinishedOne()
  {
    Infrastructure.ProcessCommand command = new("xcrun", ["simctl", "spawn"], "/repo");
    const string passed = "running 1 test\ntest invoke::json ... ok\n\ntest result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 43 filtered out; finished in 2.36s\n";
    Assert.True(CellExecution.JudgeLibtestAllowingLauncherHang(command, new(124, true, passed, string.Empty)).Passed);
    Assert.False(CellExecution.JudgeLibtestAllowingLauncherHang(command, new(124, true, "running 1 test\n", string.Empty)).Passed);
    Assert.False(CellExecution.JudgeLibtestAllowingLauncherHang(command, new(101, false, "test result: FAILED. 0 passed; 1 failed;", string.Empty)).Passed);
    Assert.False(CellExecution.JudgeLibtest(command, new(124, true, passed, string.Empty)).Passed);
  }

  [Fact]
  [Trait("Category", "Catalog")]
  public void RunnerNotesAreAppendedRedactedToTheDiagnosticsLog()
  {
    string root = Directory.CreateTempSubdirectory("dxn-runner-notes-").FullName;
    try
    {
      RunnerNotes.Write(root, "ios/json/invoke passed; access_token=secret-value");
      string log = File.ReadAllText(Path.Combine(root, "artifacts-temp", "e2e", "runner-notes.log"));
      Assert.Contains("ios/json/invoke passed", log, StringComparison.Ordinal);
      Assert.DoesNotContain("secret-value", log, StringComparison.Ordinal);
    }
    finally
    {
      Directory.Delete(root, recursive: true);
    }
  }

  [Fact]
  [Trait("Category", "Catalog")]
  public void DiagnosticsRedactRedirectTokensAndKeepCompilerErrors()
  {
    Assert.Equal(
      @"{""url"":""http://x/test"",""accessToken"":""[REDACTED]""}",
      Redaction.Redact(@"{""url"":""http://x/test"",""accessToken"":""redirect-token""}"));
    const string messages = """
      {"reason":"compiler-message","message":{"level":"warning","rendered":"warning: unused"}}
      {"reason":"compiler-message","message":{"level":"error","rendered":"error[E0425]: cannot find value"}}
      {"reason":"build-finished","success":false}
      """;
    string errors = NativeE2EBinary.CompilerErrors(messages);
    Assert.Contains("error[E0425]: cannot find value", errors, StringComparison.Ordinal);
    Assert.DoesNotContain("warning: unused", errors, StringComparison.Ordinal);
  }

  [Fact]
  [Trait("Category", "Catalog")]
  public void DeviceEndpointsRewriteOnlyUrlHosts()
  {
    Dictionary<string, string> rewritten = DeviceEndpoints.Rewrite(
      new Dictionary<string, string>
      {
        ["DXN_SIGNALR_E2E_SERVICE_URL"] = "http://localhost:5100",
        ["DXN_SIGNALR_E2E_TLS_URL"] = "https://127.0.0.1:5101",
        ["DXN_SIGNALR_E2E_PROTOCOL"] = "json",
        ["DXN_SIGNALR_E2E_TLS_CA_PEM_BASE64"] = "aHR0cDovL2xvY2FsaG9zdA=="
      },
      AndroidCellRunner.HostAlias);
    Assert.Equal("http://10.0.2.2:5100", rewritten["DXN_SIGNALR_E2E_SERVICE_URL"]);
    Assert.Equal("https://10.0.2.2:5101", rewritten["DXN_SIGNALR_E2E_TLS_URL"]);
    Assert.Equal("json", rewritten["DXN_SIGNALR_E2E_PROTOCOL"]);
    Assert.Equal("aHR0cDovL2xvY2FsaG9zdA==", rewritten["DXN_SIGNALR_E2E_TLS_CA_PEM_BASE64"]);
  }

  [Fact]
  [Trait("Category", "Catalog")]
  public void AndroidFindsTheNewestTestExecutableWithoutExtension()
  {
    string directory = Directory.CreateTempSubdirectory("dxn-e2e-deps-").FullName;
    try
    {
      string older = Path.Combine(directory, "e2e-0001");
      string newer = Path.Combine(directory, "e2e-0002");
      File.WriteAllText(older, string.Empty);
      File.WriteAllText(newer, string.Empty);
      File.WriteAllText(Path.Combine(directory, "e2e-0003.d"), string.Empty);
      File.SetLastWriteTimeUtc(older, DateTime.UtcNow.AddMinutes(-5));
      File.SetLastWriteTimeUtc(newer, DateTime.UtcNow.AddMinutes(-1));
      Assert.Equal(newer, AndroidCellRunner.NewestTestExecutable(directory));
    }
    finally
    {
      Directory.Delete(directory, recursive: true);
    }

    Assert.Throws<InvalidOperationException>(() => AndroidCellRunner.NewestTestExecutable(directory));
  }

  [Fact]
  [Trait("Category", "Catalog")]
  public void IosPicksTheNewestIphoneOrTheConfiguredSimulator()
  {
    const string listing = """
      {
        "devices": {
          "com.apple.CoreSimulator.SimRuntime.iOS-17-5": [
            { "name": "iPhone 15", "udid": "OLD-IPHONE", "state": "Shutdown", "isAvailable": true }
          ],
          "com.apple.CoreSimulator.SimRuntime.iOS-18-2": [
            { "name": "iPad Air", "udid": "NEW-IPAD", "state": "Shutdown", "isAvailable": true },
            { "name": "iPhone 16", "udid": "NEW-IPHONE", "state": "Booted", "isAvailable": true },
            { "name": "iPhone 16 Pro", "udid": "BROKEN", "state": "Shutdown", "isAvailable": false }
          ],
          "com.apple.CoreSimulator.SimRuntime.watchOS-11-2": [
            { "name": "Apple Watch", "udid": "WATCH", "state": "Shutdown", "isAvailable": true }
          ]
        }
      }
      """;
    Assert.Equal(("NEW-IPHONE", true), IosCellRunner.SelectDevice(null, listing));
    Assert.Equal(("OLD-IPHONE", false), IosCellRunner.SelectDevice("iPhone 15", listing));
    Assert.Equal(("NEW-IPAD", false), IosCellRunner.SelectDevice("NEW-IPAD", listing));
    Assert.Throws<InvalidOperationException>(() => IosCellRunner.SelectDevice("iPhone 99", listing));
  }
}
