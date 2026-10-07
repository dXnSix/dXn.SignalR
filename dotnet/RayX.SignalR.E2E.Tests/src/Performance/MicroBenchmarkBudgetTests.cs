using RayX.SignalR.AppHost;
using RayX.SignalR.E2E.Tests.Infrastructure;

namespace RayX.SignalR.E2E.Tests.Performance;

/// <summary>
/// Runs the hot-path micro-benchmarks against the <c>pre-optimization</c> Criterion baseline saved
/// on this machine before the hot-path work, and applies the R2.C3 budgets to Criterion's change
/// estimates. Needs no AppHost.
/// </summary>
public sealed class MicroBenchmarkBudgetTests
{
  [Fact]
  [Trait("Category", "MicroBenchmarkBudget")]
  public async Task HotPathsMeetTheirBudgetsAgainstThePreOptimizationBaseline()
  {
    string root = RepositoryLayout.FindRoot(AppContext.BaseDirectory);
    string criterion = MicroBenchmarkBudget.CriterionDirectory(root);
    IReadOnlyList<string> missing = MicroBenchmarkBudget.MissingBaselines(criterion);
    Assert.True(
      missing.Count == 0,
      $"The Criterion baseline '{MicroBenchmarkBudget.Baseline}' is missing for {string.Join(", ", missing)}. "
      + "Regenerate it on this machine from the commit before the hot-path work (never estimate it): "
      + $"cargo bench --bench hot_paths --features messagepack,bench-internals -- --save-baseline {MicroBenchmarkBudget.Baseline}");

    DateTime started = DateTime.UtcNow;
    ProcessCommand bench = new("cargo", MicroBenchmarkBudget.BenchArguments, root);
    ProcessResult result = await ProcessRunner.RunAsync(bench, TimeSpan.FromMinutes(60));
    Assert.True(
      result.Succeeded,
      $"{bench.Display} exited {result.ExitCode} (timed out: {result.TimedOut}):\n{Redaction.SafeTail(result.CombinedOutput)}");

    IReadOnlyList<BenchmarkChange> changes = MicroBenchmarkBudget.ReadChanges(criterion, started);
    string table = MicroBenchmarkBudget.Table(changes);
    string report = Path.Combine(PerformanceRunFile.Directory(root), $"micro-budget-{PerformanceRunFile.Run}.txt");
    Directory.CreateDirectory(PerformanceRunFile.Directory(root));
    await File.WriteAllTextAsync(report, table + "\n");

    IReadOnlyList<string> violations = MicroBenchmarkBudget.Violations(changes);
    Assert.True(violations.Count == 0, $"{string.Join('\n', violations)}\n\n{table}");
  }
}
