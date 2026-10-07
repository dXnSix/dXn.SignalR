using RayX.SignalR.E2E.Tests.Infrastructure;

namespace RayX.SignalR.E2E.Tests.Performance;

/// <summary>
/// Runs the selected performance workloads with the release Rust client and the official .NET
/// client against the same test service, one row per workload and protocol, and writes their
/// results to <c>artifacts-temp/perf/&lt;run&gt;.json</c>. Rows exist only when
/// <c>RAYX_SIGNALR_PERF_WORKLOADS</c> selects workloads.
/// </summary>
[Collection(AspireTestGroup.Name)]
public sealed class PerformanceTests(AspireEnvironment environment)
{
  const string _selectionError = "selection-error: ";

  /// <summary>Gets one row per selected workload and protocol, or one failing row describing an invalid selection.</summary>
  /// <returns>The rows.</returns>
  public static TheoryData<string> SelectedRows()
  {
    TheoryData<string> rows = [];
    try
    {
      foreach(PerformanceRow row in PerformanceSelection.FromEnvironment().Rows())
      {
        rows.Add(row.Id);
      }
    }
    catch(ArgumentException exception)
    {
      rows.Add(_selectionError + exception.Message);
    }

    return rows;
  }

  [Theory]
  [Trait("Category", "Performance")]
  [MemberData(nameof(SelectedRows))]
  public async Task Workload(string row)
  {
    Assert.False(row.StartsWith(_selectionError, StringComparison.Ordinal), row);
    PerformanceRow selected = PerformanceRow.Parse(row);
    string scale = PerformanceSelection.FromEnvironment().Scale;
    IReadOnlyList<PerformanceResult> results = await PerformanceRunCache.GetAsync(environment, selected, scale);

    Assert.Equal([PerformanceRunner.Rust, PerformanceRunner.DotNet], results.Select(result => result.Client));
    foreach(PerformanceResult result in results)
    {
      Assert.Equal("release", result.BuildProfile);
      Assert.Equal(PerformanceRunner.Rounds, result.Rounds.Count);
      Assert.All(
        PerformanceRunner.ExpectedMetrics[selected.Workload],
        metric => Assert.True(result.Metrics[metric] > 0, $"{result.Client} {metric}"));
    }
  }
}
