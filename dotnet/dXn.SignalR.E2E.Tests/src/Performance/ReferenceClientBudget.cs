using System.Collections.Concurrent;
using System.Globalization;
using dXn.SignalR.E2E.Tests.Infrastructure;

namespace dXn.SignalR.E2E.Tests.Performance;

/// <summary>
/// Each performance row's results for this test process: a row runs once, whether a
/// <c>Performance</c> row or the <c>ReferenceClientBudget</c> evaluation asks for it first, and
/// its results join the run report.
/// </summary>
public static class PerformanceRunCache
{
  static readonly ConcurrentDictionary<string, Lazy<Task<IReadOnlyList<PerformanceResult>>>> _rows = new(StringComparer.Ordinal);

  /// <summary>Runs <paramref name="row"/> at <paramref name="scale"/> once and returns its Rust and .NET results.</summary>
  /// <param name="environment">The running AppHost.</param>
  /// <param name="row">The workload and protocol.</param>
  /// <param name="scale">The iteration scale.</param>
  /// <returns>The results.</returns>
  public static Task<IReadOnlyList<PerformanceResult>> GetAsync(
    AspireEnvironment environment,
    PerformanceRow row,
    string scale) =>
    _rows.GetOrAdd(
           $"{row.Id}@{scale}",
           _ => new Lazy<Task<IReadOnlyList<PerformanceResult>>>(() => RunAsync(environment, row, scale)))
         .Value;

  static async Task<IReadOnlyList<PerformanceResult>> RunAsync(AspireEnvironment environment, PerformanceRow row, string scale)
  {
    await environment.EnsureStartedAsync(requireFixture: false);
    await environment.ResetServiceAsync();
    IReadOnlyList<PerformanceResult> results = await PerformanceRunner.RunAsync(environment, row, scale, CancellationToken.None);
    await PerformanceRunFile.AddAsync(environment.RepositoryRoot, results);
    return results;
  }
}

/// <summary>
/// The R2.C4 budgets of the release Rust client against the official .NET client in the same run,
/// for JSON and MessagePack: <c>invoke-latency</c> p50 and p99 at most 1.10x of the .NET client, and
/// <c>stream-throughput</c>, <c>send-throughput</c> and <c>fan-in</c> throughput at least 0.95x of it.
/// </summary>
public static class ReferenceClientBudget
{
  /// <summary>The largest Rust/.NET ratio of invocation latency percentiles.</summary>
  public const double MaximumLatencyRatio = 1.10;

  /// <summary>The smallest Rust/.NET ratio of throughputs.</summary>
  public const double MinimumThroughputRatio = 0.95;

  /// <summary>The budgeted metrics per workload: <see langword="true"/> where lower is better.</summary>
  public static readonly IReadOnlyDictionary<string, (string Metric, bool LowerIsBetter)[]> Metrics =
    new Dictionary<string, (string, bool)[]>(StringComparer.Ordinal)
    {
      ["invoke-latency"] = [("p50_us", true), ("p99_us", true)],
      ["stream-throughput"] = [("items_per_second", false)],
      ["send-throughput"] = [("sends_per_second", false)],
      ["fan-in"] = [("calls_per_second", false)]
    };

  /// <summary>One budgeted comparison.</summary>
  /// <param name="Row">The workload and protocol.</param>
  /// <param name="Metric">The metric.</param>
  /// <param name="Rust">The Rust client's median.</param>
  /// <param name="DotNet">The .NET client's median.</param>
  /// <param name="LowerIsBetter">Whether a smaller value is better (latency).</param>
  public sealed record Comparison(string Row, string Metric, double Rust, double DotNet, bool LowerIsBetter)
  {
    /// <summary>Gets the Rust/.NET ratio.</summary>
    public double Ratio => Rust / DotNet;

    /// <summary>Gets whether the comparison is within its budget.</summary>
    public bool Holds => LowerIsBetter ? Ratio <= MaximumLatencyRatio : Ratio >= MinimumThroughputRatio;

    /// <summary>Gets a report line.</summary>
    public override string ToString() => string.Create(
      CultureInfo.InvariantCulture,
      $"{Row,-30} {Metric,-17} rust {Rust,12:0.##} dotnet {DotNet,12:0.##} ratio {Ratio,5:0.000} "
      + $"({(LowerIsBetter ? "<=" : ">=")} {(LowerIsBetter ? MaximumLatencyRatio : MinimumThroughputRatio):0.00}) {(Holds ? "ok" : "OVER BUDGET")}");
  }

  /// <summary>Compares the Rust and .NET results of every row.</summary>
  /// <param name="results">Both clients' results for every row.</param>
  /// <returns>One comparison per row and budgeted metric.</returns>
  public static IReadOnlyList<Comparison> Compare(IEnumerable<PerformanceResult> results)
  {
    List<Comparison> comparisons = [];
    foreach(IGrouping<(string Workload, string Protocol), PerformanceResult> row in results.GroupBy(result => (result.Workload, result.Protocol)))
    {
      PerformanceResult rust = row.Single(result => result.Client == PerformanceRunner.Rust);
      PerformanceResult dotnet = row.Single(result => result.Client == PerformanceRunner.DotNet);
      foreach((string metric, bool lowerIsBetter) in Metrics[row.Key.Workload])
      {
        comparisons.Add(new Comparison(
          $"{row.Key.Workload}/{row.Key.Protocol}",
          metric,
          rust.Metrics[metric],
          dotnet.Metrics[metric],
          lowerIsBetter));
      }
    }

    return comparisons;
  }
}
