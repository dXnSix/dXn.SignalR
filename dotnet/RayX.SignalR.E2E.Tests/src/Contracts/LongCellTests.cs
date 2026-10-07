using RayX.SignalR.E2E.Tests.Scenarios;

namespace RayX.SignalR.E2E.Tests.Contracts;

/// <summary>
/// Proves the long-running cells' bounds and variables: <c>soak-chaos</c> runs for
/// <c>RAYX_SIGNALR_SOAK_SECONDS</c> within that duration plus 60 seconds, <c>stateful-resume-stress</c>
/// within 600 seconds with the stateful sequence trace on, and every target receives both.
/// </summary>
public sealed class LongCellTests
{
  static E2ECell Cell(string target, string scenario) =>
    new(ScenarioCatalog.Target(target), "json", ScenarioCatalog.Scenarios.Single(candidate => candidate.Name == scenario));

  [Fact]
  [Trait("Category", "Catalog")]
  public void SoakAndStressCellsHaveTheirOwnBoundsAndReachEveryTarget()
  {
    string? previous = Environment.GetEnvironmentVariable(CellExecution.SoakSecondsVariable);
    try
    {
      Environment.SetEnvironmentVariable(CellExecution.SoakSecondsVariable, "900");
      E2ECell soak = Cell("android", "soak-chaos");
      E2ECell stress = Cell("native", "stateful-resume-stress");
      E2ECell ordinary = Cell("native", "invoke");
      Assert.Equal(TimeSpan.FromSeconds(960), soak.Scenario.Bound);
      Assert.Equal(TimeSpan.FromSeconds(600), stress.Scenario.Bound);
      Assert.Equal(CellExecution.CellTimeout, ordinary.Scenario.Bound);
      Assert.Equal(TimeSpan.FromSeconds(60), CellExecution.CellTimeout);

      Dictionary<string, string> soakVariables = CellExecution.CellVariables(soak);
      Assert.Equal("900", soakVariables[CellExecution.SoakSecondsVariable]);
      Assert.Equal("rayx_signalr=info", soakVariables["RUST_LOG"]);
      Assert.Equal(
        "rayx_signalr=info,rayx_signalr::stateful=debug",
        CellExecution.CellVariables(stress)["RUST_LOG"]);

      // Android passes them through `adb shell env`, iOS through SIMCTL_CHILD_ variables.
      string android = AndroidCellRunner.ShellCommand(soak.Scenario.RustTest(soak.Protocol), soakVariables);
      Assert.Contains($"{CellExecution.SoakSecondsVariable}='900'", android, StringComparison.Ordinal);
      Dictionary<string, string> ios = IosCellRunner.ChildVariables(soakVariables);
      Assert.Equal("900", ios[$"SIMCTL_CHILD_{CellExecution.SoakSecondsVariable}"]);
    }
    finally
    {
      Environment.SetEnvironmentVariable(CellExecution.SoakSecondsVariable, previous);
    }

    if(previous is null)
    {
      // 120 seconds by default.
      Assert.Equal(TimeSpan.FromSeconds(180), Cell("native", "soak-chaos").Scenario.Bound);
    }
  }
}
