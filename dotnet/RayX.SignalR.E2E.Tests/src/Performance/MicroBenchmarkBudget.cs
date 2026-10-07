using System.Globalization;
using System.Text.Json;

namespace RayX.SignalR.E2E.Tests.Performance;

/// <summary>One hot-path benchmark compared with the <c>pre-optimization</c> Criterion baseline.</summary>
/// <param name="Id">The stable benchmark id (<c>hot_paths/&lt;direction&gt;/&lt;protocol&gt;/&lt;message&gt;/&lt;size&gt;</c>).</param>
/// <param name="MeanChange">Criterion's relative change of the mean time (<c>-0.4</c> is 40 % less time).</param>
public sealed record BenchmarkChange(string Id, double MeanChange)
{
  /// <summary>Gets how many times faster the benchmark runs than the baseline.</summary>
  public double Speedup => 1 / (1 + MeanChange);
}

/// <summary>
/// The R2.C3 micro-benchmark budgets: against the <c>pre-optimization</c> baseline saved on the
/// same machine before the hot-path work, inbound StreamItem and Completion decode-plus-dispatch
/// are at least 1.5x faster at 32 B and 1 KiB for both protocols, outbound invocation encoding is at
/// least 1.2x faster, and no benchmark regresses by more than 5 %.
/// </summary>
public static class MicroBenchmarkBudget
{
  /// <summary>The Criterion baseline the budgets compare against.</summary>
  public const string Baseline = "pre-optimization";

  /// <summary>The cargo arguments that run the benchmarks against <see cref="Baseline"/> without overwriting it.</summary>
  public static readonly string[] BenchArguments =
    ["bench", "--bench", "hot_paths", "--features", "messagepack,bench-internals", "--", "--baseline", Baseline];

  /// <summary>The minimum speedup of inbound StreamItem and Completion decode-plus-dispatch.</summary>
  public const double InboundSpeedup = 1.5;

  /// <summary>The minimum speedup of outbound invocation encoding.</summary>
  public const double OutboundSpeedup = 1.2;

  /// <summary>The largest relative slowdown any benchmark may show.</summary>
  public const double MaximumRegression = 0.05;

  static readonly string[] _protocols = ["json", "messagepack"];
  static readonly string[] _sizes = ["32B", "1KiB", "64KiB"];

  /// <summary>Gets every benchmark id <c>benches/hot_paths.rs</c> defines.</summary>
  public static IReadOnlyList<string> ExpectedIds { get; } =
  [
    .. _protocols.SelectMany(protocol => _sizes.SelectMany(size => new[]
    {
      $"hot_paths/inbound/{protocol}/invocation/{size}",
      $"hot_paths/inbound/{protocol}/completion/{size}",
      $"hot_paths/inbound/{protocol}/stream-item/{size}"
    })),
    .. _protocols.SelectMany(protocol => new[]
    {
      $"hot_paths/inbound/{protocol}/completion-void/0B",
      $"hot_paths/inbound/{protocol}/ping/0B"
    }),
    .. _protocols.SelectMany(protocol => _sizes.Select(size => $"hot_paths/outbound/{protocol}/invocation/{size}")),
    .. _protocols.SelectMany(protocol => _sizes.Select(size => $"hot_paths/framing/{protocol}/split-8/{size}"))
  ];

  /// <summary>Gets the Criterion output directory: <c>$CARGO_TARGET_DIR/criterion</c>, or <c>target/criterion</c>.</summary>
  /// <param name="repositoryRoot">The repository root.</param>
  /// <returns>The directory.</returns>
  public static string CriterionDirectory(string repositoryRoot)
  {
    string? target = Environment.GetEnvironmentVariable("CARGO_TARGET_DIR");
    return Path.Combine(
      string.IsNullOrWhiteSpace(target) ? Path.Combine(repositoryRoot, "target") : Path.GetFullPath(target, repositoryRoot),
      "criterion");
  }

  /// <summary>Gets whether every expected benchmark has the saved baseline.</summary>
  /// <param name="criterionDirectory">The Criterion output directory.</param>
  /// <returns>The benchmark ids that have no baseline.</returns>
  public static IReadOnlyList<string> MissingBaselines(string criterionDirectory)
  {
    HashSet<string> present = [.. BenchmarkDirectories(criterionDirectory)
                                  .Where(directory => Directory.Exists(Path.Combine(directory, Baseline)))
                                  .Select(directory => FullId(Path.Combine(directory, Baseline, "benchmark.json")))
                                  .OfType<string>()];
    return [.. ExpectedIds.Where(id => !present.Contains(id))];
  }

  /// <summary>Reads every benchmark's change against the baseline from a comparison run.</summary>
  /// <param name="criterionDirectory">The Criterion output directory.</param>
  /// <param name="since">Ignores comparisons written before this time (left over from earlier runs).</param>
  /// <returns>The changes, by id.</returns>
  public static IReadOnlyList<BenchmarkChange> ReadChanges(string criterionDirectory, DateTime since)
  {
    List<BenchmarkChange> changes = [];
    foreach(string directory in BenchmarkDirectories(criterionDirectory))
    {
      string estimates = Path.Combine(directory, "change", "estimates.json");
      string? id = FullId(Path.Combine(directory, "new", "benchmark.json"));
      if(id is null || !File.Exists(estimates) || File.GetLastWriteTimeUtc(estimates) < since)
      {
        continue;
      }

      using JsonDocument document = JsonDocument.Parse(File.ReadAllText(estimates));
      double change = document.RootElement.GetProperty("mean").GetProperty("point_estimate").GetDouble();
      changes.Add(new BenchmarkChange(id, change));
    }

    return [.. changes.OrderBy(change => change.Id, StringComparer.Ordinal)];
  }

  /// <summary>Applies the budgets; returns one line per violation (empty when every budget holds).</summary>
  /// <param name="changes">The measured changes.</param>
  /// <returns>The violations.</returns>
  public static IReadOnlyList<string> Violations(IReadOnlyList<BenchmarkChange> changes)
  {
    Dictionary<string, BenchmarkChange> byId = changes.ToDictionary(change => change.Id, StringComparer.Ordinal);
    List<string> violations = [];
    foreach(string id in ExpectedIds)
    {
      if(!byId.TryGetValue(id, out BenchmarkChange? change))
      {
        violations.Add($"{id}: no comparison with the {Baseline} baseline");
        continue;
      }

      if(change.MeanChange > MaximumRegression)
      {
        violations.Add($"{id}: {Percent(change.MeanChange)} slower than the baseline (at most {Percent(MaximumRegression)})");
      }

      double? required = RequiredSpeedup(id);
      if(required is double minimum && change.Speedup < minimum)
      {
        violations.Add(
          string.Create(
            CultureInfo.InvariantCulture,
            $"{id}: {change.Speedup:0.00}x faster than the baseline (at least {minimum:0.0}x)"));
      }
    }

    return violations;
  }

  /// <summary>The speedup a benchmark must reach, if it has a speedup budget.</summary>
  /// <param name="id">The benchmark id.</param>
  /// <returns>The minimum speedup, or <see langword="null"/>.</returns>
  public static double? RequiredSpeedup(string id)
  {
    string[] parts = id.Split('/');
    if(parts.Length != 5)
    {
      return null;
    }

    (string direction, string message, string size) = (parts[1], parts[3], parts[4]);
    return direction switch
    {
      "inbound" when message is "stream-item" or "completion" && size is "32B" or "1KiB" => InboundSpeedup,
      "outbound" when message == "invocation" => OutboundSpeedup,
      _ => null
    };
  }

  /// <summary>Formats the changes as a table for the failure message and the run report.</summary>
  /// <param name="changes">The changes.</param>
  /// <returns>The table.</returns>
  public static string Table(IReadOnlyList<BenchmarkChange> changes) =>
    string.Join(
      '\n',
      changes.Select(change => string.Create(
                       CultureInfo.InvariantCulture,
                       $"{change.Id,-46} {Percent(change.MeanChange),9} {change.Speedup,6:0.00}x")));

  static string Percent(double value) => string.Create(CultureInfo.InvariantCulture, $"{value * 100:+0.0;-0.0}%");

  static IEnumerable<string> BenchmarkDirectories(string criterionDirectory) =>
    Directory.Exists(criterionDirectory)
      ? Directory.EnumerateDirectories(criterionDirectory, "*", SearchOption.AllDirectories)
                 .Where(directory => File.Exists(Path.Combine(directory, "new", "benchmark.json"))
                                     || Directory.Exists(Path.Combine(directory, Baseline)))
      : [];

  static string? FullId(string benchmarkJson)
  {
    if(!File.Exists(benchmarkJson))
    {
      return null;
    }

    using JsonDocument document = JsonDocument.Parse(File.ReadAllText(benchmarkJson));
    return document.RootElement.TryGetProperty("full_id", out JsonElement id) ? id.GetString() : null;
  }
}
