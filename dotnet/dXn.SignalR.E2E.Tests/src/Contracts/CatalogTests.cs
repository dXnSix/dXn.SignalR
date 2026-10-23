using System.Text.RegularExpressions;
using dXn.SignalR.E2E.Tests.Infrastructure;
using dXn.SignalR.E2E.Tests.Scenarios;

namespace dXn.SignalR.E2E.Tests.Contracts;

/// <summary>Proves cell selection and that every cataloged scenario is implemented where claimed.</summary>
[Collection(AspireTestGroup.Name)]
public sealed partial class CatalogTests(AspireEnvironment environment)
{
  [Fact]
  [Trait("Category", "Catalog")]
  public void SelectionDefaultsExpandAllAndPreserveCatalogOrder()
  {
    E2ESelection defaults = E2ESelection.Parse(null, null, null);
    Assert.Equal(["native"], defaults.Targets);
    Assert.Equal(ScenarioCatalog.Protocols, defaults.Protocols);
    Assert.Equal(ScenarioCatalog.Scenarios.Select(scenario => scenario.Name), defaults.Scenarios);
    Assert.False(defaults.RequiresBrowserFixture);

    E2ESelection all = E2ESelection.Parse("all", "ALL", " all ");
    Assert.Equal(ScenarioCatalog.Targets.Select(target => target.Name), all.Targets);
    Assert.True(all.RequiresBrowserFixture);
    // Host-gated native cells: Windows-only scenarios, and system-proxy scenarios where the host
    // may not change its settings.
    bool HostRuns(Scenario scenario) =>
      (OperatingSystem.IsWindows() || !scenario.WindowsHostOnly)
      && (SystemProxy.HostAllowed() || !scenario.SystemProxy);
    // HTTP/3 scenarios need a host whose Kestrel serves HTTP/3.
    int expected = ScenarioCatalog.Scenarios
      .Where(scenario => AppHost.QuicSupport.IsSupported || !scenario.QuicHostOnly)
      .Sum(scenario => (scenario.Targets.Count - (scenario.Targets.Contains("native") && !HostRuns(scenario) ? 1 : 0)) * scenario.Protocols.Count);
    Assert.Equal(expected, all.Cells().Count);
  }

  [Fact]
  [Trait("Category", "Catalog")]
  public void SelectionGeneratesOnlySupportedCells()
  {
    IReadOnlyList<E2ECell> cells = E2ESelection
                                   .Parse("native,chromium", "json", "short-stream-chaining,ordinary-reconnect")
                                   .Cells();
    Assert.Equal(
      ["native/json/ordinary-reconnect", "chromium/json/short-stream-chaining", "chromium/json/ordinary-reconnect"],
      cells.Select(cell => cell.Id));
    foreach(E2ECell cell in cells)
    {
      Assert.Equal(cell, E2ECell.Parse(cell.Id));
    }
  }

  [Fact]
  [Trait("Category", "Catalog")]
  public void ProtocolSpecificScenariosRunOnlyWithTheirProtocols()
  {
    Assert.Equal(
      ["native/messagepack/msgpack-fidelity", "chromium/messagepack/msgpack-fidelity"],
      E2ESelection.Parse("native,chromium", "all", "msgpack-fidelity").Cells().Select(cell => cell.Id));
    Assert.Throws<InvalidOperationException>(() => E2ESelection.Parse("native", "json", "msgpack-fidelity").Cells());
  }

  [Fact]
  [Trait("Category", "Catalog")]
  public void InvalidOrEmptySelectionsFail()
  {
    Assert.Throws<ArgumentException>(() => E2ESelection.Parse("native,webdriver", null, null));
    Assert.Throws<ArgumentException>(() => E2ESelection.Parse(" , ", null, null));
    Assert.Throws<ArgumentException>(() => E2ESelection.Parse(null, "xml", null));
    Assert.Throws<ArgumentException>(() => E2ESelection.Parse(null, null, "missing-scenario"));
    InvalidOperationException empty = Assert.Throws<InvalidOperationException>(
      () => E2ESelection.Parse("native", "json", "short-stream-chaining").Cells());
    Assert.Contains("matches no cataloged cell", empty.Message, StringComparison.Ordinal);
  }

  [Fact]
  [Trait("Category", "Catalog")]
  public async Task RustScenarioTestsMatchTheCatalog()
  {
    string executable = await NativeE2EBinary.BuildAsync(environment.RepositoryRoot, CancellationToken.None);
    ProcessResult listing = await ProcessRunner.RunAsync(
                              new ProcessCommand(executable, ["--list", "--format", "terse"], environment.RepositoryRoot),
                              TimeSpan.FromSeconds(60));
    Assert.True(listing.Succeeded, Redaction.SafeTail(listing.CombinedOutput));
    HashSet<string> tests = RustTestLine()
                            .Matches(listing.StandardOutput)
                            .Select(match => match.Groups["name"].Value)
                            .ToHashSet(StringComparer.Ordinal);

    HashSet<string> expected = ScenarioCatalog.Scenarios
                                              .Where(ScenarioCatalog.RunsNatively)
                                              .SelectMany(scenario => scenario.Protocols.Select(scenario.RustTest))
                                              .ToHashSet(StringComparer.Ordinal);
    Assert.Empty(expected.Except(tests));
    Assert.Empty(tests.Except(expected));
  }

  [Fact]
  [Trait("Category", "Catalog")]
  public async Task PlaywrightTitlesMatchTheCatalog()
  {
    foreach(string engine in ScenarioCatalog.Targets
                                            .Where(target => target.Kind == TargetKind.Browser)
                                            .Select(target => target.Name))
    {
      ProcessCommand list = BrowserCellRunner.Command(environment.RepositoryRoot, engine, title: null, list: true);
      ProcessResult listing = await ProcessRunner.RunAsync(list, TimeSpan.FromSeconds(120));
      Assert.True(listing.Succeeded, Redaction.SafeTail(listing.CombinedOutput));
      HashSet<string> titles = PlaywrightListLine()
                               .Matches(listing.StandardOutput)
                               .Where(match => match.Groups["project"].Value == engine)
                               .Select(match => match.Groups["title"].Value.Trim())
                               .ToHashSet(StringComparer.Ordinal);

      HashSet<string> expected = E2ESelection.Parse(engine, "all", "all", "all")
                                             .Cells()
                                             .Select(cell => cell.Scenario.PlaywrightTitle(cell.Protocol, cell.Transport))
                                             .ToHashSet(StringComparer.Ordinal);
      Assert.Empty(expected.Except(titles));
      Assert.Empty(titles.Except(expected));
    }
  }

  [Fact]
  [Trait("Category", "Catalog")]
  public void WindowsOnlyScenariosAreSelectedOnlyOnWindowsHosts()
  {
    E2ESelection selection = E2ESelection.Parse("native", "all", "windows-auth,invoke");
    Assert.Contains(selection.Cells(windowsHost: true), cell => cell.Scenario.Name == "windows-auth");
    Assert.DoesNotContain(selection.Cells(windowsHost: false), cell => cell.Scenario.Name == "windows-auth");
    Assert.Contains(selection.Cells(windowsHost: false), cell => cell.Scenario.Name == "invoke");
    Assert.True(ScenarioCatalog.Scenario("windows-auth").WindowsHostOnly);
    Assert.Contains("Windows", ScenarioCatalog.Scenario("windows-auth").WindowsHostOnlyReason, StringComparison.Ordinal);
    Assert.All(ScenarioCatalog.Scenarios.Where(scenario => scenario.WindowsHostOnly),
               scenario => Assert.False(string.IsNullOrWhiteSpace(scenario.WindowsHostOnlyReason)));
    Assert.Equal(["native"], ScenarioCatalog.Scenario("windows-auth").Targets);
  }

  [Fact]
  [Trait("Category", "Catalog")]
  public void SystemProxyCellsChangeOnlyEmulatorsOrOptedInHosts()
  {
    E2ESelection selection = E2ESelection.Parse("native,android-app", "all", "os-proxy");
    Assert.All(selection.Cells(windowsHost: true, hostSystemChanges: false), cell => Assert.Equal("android-app", cell.Target.Name));
    Assert.Contains(selection.Cells(windowsHost: true, hostSystemChanges: true), cell => cell.Target.Name == "native");
    Assert.True(ScenarioCatalog.Scenario("os-proxy").SystemProxy);
    Assert.False(SystemProxy.HostAllowed(null, windows: true, macOs: false));
    Assert.False(SystemProxy.HostAllowed("1", windows: false, macOs: false), "Linux reads only the environment");
    Assert.True(SystemProxy.HostAllowed("1", windows: true, macOs: false));
    Assert.True(SystemProxy.HostAllowed("1", windows: false, macOs: true));
    Assert.Equal(["shell", "settings", "put", "global", "http_proxy", "10.0.2.2:6100"], SystemProxy.AndroidSetArguments("10.0.2.2", 6100));
    Assert.Equal(["shell", "settings", "get", "global", "http_proxy"], SystemProxy.AndroidGetArguments);
    Assert.Equal(["shell", "settings", "put", "global", "http_proxy", ":0"], SystemProxy.AndroidRestoreArguments("null\n"));
    Assert.Equal(["shell", "settings", "put", "global", "http_proxy", "corp:3128"], SystemProxy.AndroidRestoreArguments("corp:3128\r\n"));
    Assert.Equal((true, "corp", "8080"), SystemProxy.MacWebProxy("Enabled: Yes\nServer: corp\nPort: 8080\nAuthenticated Proxy Enabled: 0\n"));
    Assert.Equal(
      [["-setwebproxy", "Wi-Fi", "corp", "8080"], ["-setwebproxystate", "Wi-Fi", "on"]],
      SystemProxy.MacRestoreArguments("Wi-Fi", (true, "corp", "8080")));
    Assert.Equal([["-setwebproxystate", "Wi-Fi", "off"]], SystemProxy.MacRestoreArguments("Wi-Fi", (false, "", "0")));
    Assert.Equal(["Wi-Fi", "Thunderbolt Bridge"], SystemProxy.MacServices("An asterisk (*) denotes that a network service is disabled.\nWi-Fi\n*USB LAN\nThunderbolt Bridge\n"));
    Assert.Equal([["-setwebproxy", "Wi-Fi", "127.0.0.1", "6100"], ["-setwebproxystate", "Wi-Fi", "on"]], SystemProxy.MacSetArguments("Wi-Fi", 6100));
    Assert.True(SystemProxy.MacWebProxyEnabled("Enabled: Yes\nServer: a\nPort: 1\n"));
    Assert.False(SystemProxy.MacWebProxyEnabled("Enabled: No\n"));
  }

  [GeneratedRegex(@"(?m)^(?<name>[a-z0-9_]+::[a-z0-9_]+): test\s*$")]
  private static partial Regex RustTestLine();

  [GeneratedRegex(@"(?m)^\s*\[(?<project>[^\]]+)\] › [^›]+ › (?<title>[^\r\n]+?)\r?$")]
  private static partial Regex PlaywrightListLine();
}

/// <summary>Proves cells write redacted diagnostics for failures before and during their scenario.</summary>
[Collection(AspireTestGroup.Name)]
public sealed class CellDiagnosticsTests(AspireEnvironment environment)
{
  [Fact]
  [Trait("Category", "Catalog")]
  public async Task FailuresBeforeTheScenarioWriteRedactedDiagnostics()
  {
    E2ECell cell = E2ECell.Parse("native/json/ordinary-reconnect");
    string name = $"catalog-selftest-{Guid.NewGuid():N}";
    CellReport startupFailure = await CellRun.ExecuteAsync(
                                  environment,
                                  cell,
                                  () => throw new InvalidOperationException("AppHost failed with access_token=secret-token"),
                                  () => throw new InvalidOperationException("unreachable"),
                                  name);
    AssertDiagnostics(startupFailure, "AppHost failed");

    CellReport buildFailure = await CellRun.ExecuteAsync(
                                environment,
                                cell,
                                () => Task.CompletedTask,
                                () => throw new InvalidOperationException("cargo build failed: Authorization: Bearer secret-token"),
                                name);
    AssertDiagnostics(buildFailure, "cargo build failed");

    ProcessResult failed = new(101, false, "test result: FAILED. 0 passed; 1 failed;", "Bearer secret-token");
    CellReport scenarioFailure = await CellRun.ExecuteAsync(
                                   environment,
                                   cell,
                                   () => Task.CompletedTask,
                                   () => Task.FromResult(
                                     CellExecution.JudgeLibtest(
                                       new ProcessCommand("e2e", ["ordinary_reconnect::json"], "."),
                                       failed)),
                                   name);
    AssertDiagnostics(scenarioFailure, "exit code 101");
  }

  [Fact]
  [Trait("Category", "Catalog")]
  public void VerdictsRequireExactlyOnePassedScenario()
  {
    ProcessCommand command = new("e2e", [], ".");
    Assert.True(CellExecution.JudgeLibtest(command, new ProcessResult(0, false, "test result: ok. 1 passed; 0 failed; 0 ignored;", "")).Passed);
    Assert.False(CellExecution.JudgeLibtest(command, new ProcessResult(0, false, "test result: ok. 0 passed; 0 failed; 0 ignored;", "")).Passed);
    Assert.False(CellExecution.JudgeLibtest(command, new ProcessResult(124, true, "", "")).Passed);
    Assert.True(CellExecution.JudgePlaywright(command, new ProcessResult(0, false, "  1 passed (3.1s)", "")).Passed);
    Assert.False(CellExecution.JudgePlaywright(command, new ProcessResult(0, false, "  1 passed (3.1s)\n  1 skipped", "")).Passed);
    Assert.Equal(
      "access_token=[REDACTED] Authorization: [REDACTED]",
      Redaction.Redact("access_token=abc Authorization: Bearer xyz"));
  }

  static void AssertDiagnostics(CellReport report, string expected)
  {
    Assert.False(report.Passed);
    Assert.NotNull(report.DiagnosticsPath);
    try
    {
      string written = File.ReadAllText(report.DiagnosticsPath);
      Assert.Contains(expected, written, StringComparison.Ordinal);
      Assert.DoesNotContain("secret-token", written, StringComparison.Ordinal);
      Assert.DoesNotContain("secret-token", report.Message, StringComparison.Ordinal);
    }
    finally
    {
      File.Delete(report.DiagnosticsPath);
    }
  }
}
