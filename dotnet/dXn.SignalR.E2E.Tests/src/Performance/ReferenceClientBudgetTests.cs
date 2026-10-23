using dXn.SignalR.E2E.Tests.Infrastructure;

namespace dXn.SignalR.E2E.Tests.Performance;

/// <summary>
/// Evaluates the R2.C4 budgets over this run's full-scale results for every workload and protocol;
/// rows the <c>Performance</c> category has not run yet in this process run here first.
/// </summary>
[Collection(AspireTestGroup.Name)]
public sealed class ReferenceClientBudgetTests(AspireEnvironment environment)
{
  [Fact]
  [Trait("Category", "ReferenceClientBudget")]
  public async Task RustClientMeetsTheReferenceClientBudgets()
  {
    PerformanceSelection selection = PerformanceSelection.FromEnvironment();
    Assert.True(
      selection.Scale == "full" && selection.SelectedWorkloads.Count == PerformanceSelection.Workloads.Count,
      $"The budgets apply to a full run: set {PerformanceSelection.WorkloadsVariable}=all and {PerformanceSelection.ScaleVariable}=full.");

    List<PerformanceResult> results = [];
    foreach(PerformanceRow row in selection.Rows())
    {
      results.AddRange(await PerformanceRunCache.GetAsync(environment, row, selection.Scale));
    }

    IReadOnlyList<ReferenceClientBudget.Comparison> comparisons = ReferenceClientBudget.Compare(results);
    string report = string.Join('\n', comparisons);
    string directory = PerformanceRunFile.Directory(environment.RepositoryRoot);
    Directory.CreateDirectory(directory);
    await File.WriteAllTextAsync(Path.Combine(directory, $"reference-budget-{PerformanceRunFile.Run}.txt"), report + "\n");

    Assert.Equal(
      PerformanceSelection.Protocols.Count * ReferenceClientBudget.Metrics.Values.Sum(metrics => metrics.Length),
      comparisons.Count);
    Assert.True(comparisons.All(comparison => comparison.Holds), report);
  }
}
