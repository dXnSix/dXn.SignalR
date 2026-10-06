namespace RayX.SignalR.E2E.Tests.Performance;

/// <summary>One workload × protocol row of the <c>Performance</c> category.</summary>
/// <param name="Workload">The workload.</param>
/// <param name="Protocol">The hub protocol.</param>
public sealed record PerformanceRow(string Workload, string Protocol)
{
  /// <summary>Gets the stable row identifier (<c>workload/protocol</c>).</summary>
  public string Id => $"{Workload}/{Protocol}";

  /// <summary>Parses an identifier produced by <see cref="Id"/>.</summary>
  /// <param name="id">The identifier.</param>
  /// <returns>The row.</returns>
  public static PerformanceRow Parse(string id)
  {
    string[] parts = id.Split('/');
    return parts.Length == 2
           && PerformanceSelection.Workloads.Contains(parts[0])
           && PerformanceSelection.Protocols.Contains(parts[1])
      ? new PerformanceRow(parts[0], parts[1])
      : throw new ArgumentException($"Invalid performance row '{id}'.", nameof(id));
  }
}

/// <summary>
/// The workloads and scale selected through <c>RAYX_SIGNALR_PERF_WORKLOADS</c> and
/// <c>RAYX_SIGNALR_PERF_SCALE</c>. Performance rows run only when workloads are selected
/// explicitly; they are never part of the end-to-end selection.
/// </summary>
/// <param name="SelectedWorkloads">The selected workloads.</param>
/// <param name="Scale">The iteration scale: <c>quick</c> for smoke checks, <c>full</c> for budgets.</param>
public sealed record PerformanceSelection(IReadOnlyList<string> SelectedWorkloads, string Scale)
{
  /// <summary>Comma-separated workloads, or <c>all</c>; required.</summary>
  public const string WorkloadsVariable = "RAYX_SIGNALR_PERF_WORKLOADS";

  /// <summary><c>quick</c> or <c>full</c>; defaults to <c>quick</c>.</summary>
  public const string ScaleVariable = "RAYX_SIGNALR_PERF_SCALE";

  /// <summary>Every workload, in run order (identical in the Rust and .NET clients).</summary>
  public static readonly IReadOnlyList<string> Workloads =
    ["invoke-latency", "stream-throughput", "send-throughput", "fan-in"];

  /// <summary>Every protocol, in run order.</summary>
  public static readonly IReadOnlyList<string> Protocols = ["json", "messagepack"];

  /// <summary>Every scale.</summary>
  public static readonly IReadOnlyList<string> Scales = ["quick", "full"];

  /// <summary>Reads the selection from the process environment.</summary>
  /// <returns>The selection.</returns>
  public static PerformanceSelection FromEnvironment() =>
    Parse(Environment.GetEnvironmentVariable(WorkloadsVariable), Environment.GetEnvironmentVariable(ScaleVariable));

  /// <summary>Parses selector values.</summary>
  /// <param name="workloads">The workload selector; required.</param>
  /// <param name="scale">The scale selector; unset means <c>quick</c>.</param>
  /// <returns>The selection.</returns>
  public static PerformanceSelection Parse(string? workloads, string? scale)
  {
    if(workloads is null)
    {
      throw new ArgumentException(
        $"{WorkloadsVariable} is not set; the Performance category runs only selected workloads ({string.Join(", ", Workloads)}, or all).",
        nameof(workloads));
    }

    string[] values = workloads.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries)
                               .Select(value => value.ToLowerInvariant())
                               .ToArray();
    if(values.Length == 0)
    {
      throw new ArgumentException($"{WorkloadsVariable} is empty; use a comma list or 'all'.", nameof(workloads));
    }

    string[] unknown = values.Where(value => value != "all" && !Workloads.Contains(value)).ToArray();
    if(unknown.Length > 0)
    {
      throw new ArgumentException(
        $"{WorkloadsVariable} contains unknown value(s) {string.Join(", ", unknown)}; allowed: {string.Join(", ", Workloads)}, all.",
        nameof(workloads));
    }

    string selectedScale = (scale ?? "quick").Trim().ToLowerInvariant();
    if(!Scales.Contains(selectedScale))
    {
      throw new ArgumentException(
        $"{ScaleVariable} is '{scale}'; allowed: {string.Join(", ", Scales)}.",
        nameof(scale));
    }

    IReadOnlyList<string> selected = values.Contains("all")
      ? Workloads
      : Workloads.Where(values.Contains).ToArray();
    return new PerformanceSelection(selected, selectedScale);
  }

  /// <summary>Expands the selection into rows: every selected workload for every protocol.</summary>
  /// <returns>The rows.</returns>
  public IReadOnlyList<PerformanceRow> Rows() =>
    [.. SelectedWorkloads.SelectMany(workload => Protocols.Select(protocol => new PerformanceRow(workload, protocol)))];
}
