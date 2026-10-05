using RayX.SignalR.E2E.Tests.Infrastructure;

namespace RayX.SignalR.E2E.Tests.Scenarios;

/// <summary>
/// Runs every selected end-to-end cell. Selection comes from <c>RAYX_SIGNALR_E2E_TARGETS</c>,
/// <c>RAYX_SIGNALR_E2E_PROTOCOLS</c> and <c>RAYX_SIGNALR_E2E_SCENARIOS</c>; each row executes exactly
/// one scenario on one target with one protocol.
/// </summary>
[Collection(AspireTestGroup.Name)]
public sealed class E2ECellTests(AspireEnvironment environment)
{
  const string _selectionError = "selection-error: ";

  static readonly Dictionary<TargetKind, ICellRunner> _runners = new()
  {
    [TargetKind.Native] = new NativeCellRunner(),
    [TargetKind.Browser] = new BrowserCellRunner()
  };

  /// <summary>Gets one row per selected cell, or one failing row describing an invalid selection.</summary>
  /// <returns>The rows.</returns>
  public static TheoryData<string> SelectedCells()
  {
    TheoryData<string> rows = [];
    try
    {
      foreach(E2ECell cell in E2ESelection.FromEnvironment().Cells())
      {
        rows.Add(cell.Id);
      }
    }
    catch(Exception exception) when(exception is ArgumentException or InvalidOperationException)
    {
      rows.Add(_selectionError + exception.Message);
    }

    return rows;
  }

  [Theory]
  [Trait("Category", "E2E")]
  [MemberData(nameof(SelectedCells))]
  public async Task Cell(string cell)
  {
    Assert.False(cell.StartsWith(_selectionError, StringComparison.Ordinal), cell);
    E2ECell selected = E2ECell.Parse(cell);
    CellReport report = await CellRun.ExecuteAsync(
                          environment,
                          selected,
                          () => environment.EnsureStartedAsync(E2ESelection.FromEnvironment().RequiresBrowserFixture),
                          () => _runners[selected.Target.Kind].RunAsync(selected, environment, CancellationToken.None));
    Assert.True(report.Passed, report.Message);
  }
}
