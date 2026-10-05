namespace RayX.SignalR.E2E.Tests.Scenarios;

/// <summary>One executable target × protocol × scenario combination.</summary>
/// <param name="Target">The target.</param>
/// <param name="Protocol">The protocol.</param>
/// <param name="Scenario">The scenario.</param>
public sealed record E2ECell(E2ETarget Target, string Protocol, Scenario Scenario)
{
  /// <summary>Gets the stable cell identifier (<c>target/protocol/scenario</c>).</summary>
  public string Id => $"{Target.Name}/{Protocol}/{Scenario.Name}";

  /// <summary>Gets a file-name-safe form of <see cref="Id"/>.</summary>
  public string FileName => $"{Target.Name}-{Protocol}-{Scenario.Name}";

  /// <summary>Parses an identifier produced by <see cref="Id"/>.</summary>
  /// <param name="id">The identifier.</param>
  /// <returns>The cell.</returns>
  public static E2ECell Parse(string id)
  {
    string[] parts = id.Split('/');
    if(parts.Length != 3)
    {
      throw new ArgumentException($"Invalid cell identifier '{id}'.", nameof(id));
    }

    return new E2ECell(
      ScenarioCatalog.Target(parts[0]),
      ScenarioCatalog.Protocols.Contains(parts[1])
        ? parts[1]
        : throw new ArgumentException($"Unknown protocol '{parts[1]}'.", nameof(id)),
      ScenarioCatalog.Scenario(parts[2]));
  }
}

/// <summary>The cells selected through <c>RAYX_SIGNALR_E2E_*</c> environment variables.</summary>
/// <param name="Targets">Selected target names.</param>
/// <param name="Protocols">Selected protocols.</param>
/// <param name="Scenarios">Selected scenario names.</param>
public sealed record E2ESelection(
  IReadOnlyList<string> Targets,
  IReadOnlyList<string> Protocols,
  IReadOnlyList<string> Scenarios)
{
  /// <summary>Comma-separated targets, or <c>all</c>; defaults to <c>native</c>.</summary>
  public const string TargetsVariable = "RAYX_SIGNALR_E2E_TARGETS";

  /// <summary>Comma-separated protocols, or <c>all</c>; defaults to <c>all</c>.</summary>
  public const string ProtocolsVariable = "RAYX_SIGNALR_E2E_PROTOCOLS";

  /// <summary>Comma-separated scenarios, or <c>all</c>; defaults to <c>all</c>.</summary>
  public const string ScenariosVariable = "RAYX_SIGNALR_E2E_SCENARIOS";

  /// <summary>Gets whether any selected target needs the browser fixture.</summary>
  public bool RequiresBrowserFixture =>
    Targets.Any(target => ScenarioCatalog.Target(target).Kind == TargetKind.Browser);

  /// <summary>Gets whether a selected browser cell needs the threaded, cross-origin-isolated fixture.</summary>
  public bool RequiresThreadedFixture =>
    RequiresBrowserFixture && Scenarios.Contains(ScenarioCatalog.ThreadedScenario);

  /// <summary>Reads the selection from the process environment.</summary>
  /// <returns>The selection.</returns>
  public static E2ESelection FromEnvironment() =>
    Parse(
      Environment.GetEnvironmentVariable(TargetsVariable),
      Environment.GetEnvironmentVariable(ProtocolsVariable),
      Environment.GetEnvironmentVariable(ScenariosVariable));

  /// <summary>Parses selector values; unset values use their defaults.</summary>
  /// <param name="targets">The target selector.</param>
  /// <param name="protocols">The protocol selector.</param>
  /// <param name="scenarios">The scenario selector.</param>
  /// <returns>The selection.</returns>
  public static E2ESelection Parse(string? targets, string? protocols, string? scenarios) =>
    new(
      Expand(TargetsVariable, targets ?? "native", ScenarioCatalog.Targets.Select(target => target.Name).ToArray()),
      Expand(ProtocolsVariable, protocols ?? "all", ScenarioCatalog.Protocols),
      Expand(ScenariosVariable, scenarios ?? "all", ScenarioCatalog.Scenarios.Select(scenario => scenario.Name).ToArray()));

  /// <summary>Expands the selection into the cells the catalog supports.</summary>
  /// <returns>The cells; never empty.</returns>
  /// <exception cref="InvalidOperationException">No catalog cell matches the selection.</exception>
  public IReadOnlyList<E2ECell> Cells()
  {
    List<E2ECell> cells = [];
    foreach(string target in Targets)
    {
      foreach(string protocol in Protocols)
      {
        foreach(string name in Scenarios)
        {
          Scenario scenario = ScenarioCatalog.Scenario(name);
          if(scenario.Targets.Contains(target))
          {
            cells.Add(new E2ECell(ScenarioCatalog.Target(target), protocol, scenario));
          }
        }
      }
    }

    return cells.Count > 0
      ? cells
      : throw new InvalidOperationException(
        $"The end-to-end selection matches no cataloged cell: {TargetsVariable}={string.Join(',', Targets)}, {ProtocolsVariable}={string.Join(',', Protocols)}, {ScenariosVariable}={string.Join(',', Scenarios)}.");
  }

  static string[] Expand(string variable, string value, IReadOnlyList<string> allowed)
  {
    string[] values = value.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries)
                           .Select(item => item.ToLowerInvariant())
                           .ToArray();
    if(values.Length == 0)
    {
      throw new ArgumentException($"{variable} is empty; use a comma list or 'all'.", nameof(value));
    }

    if(values.Contains("all"))
    {
      return [.. allowed];
    }

    string[] unknown = values.Where(item => !allowed.Contains(item)).ToArray();
    return unknown.Length == 0
      ? values.Distinct(StringComparer.Ordinal).ToArray()
      : throw new ArgumentException(
        $"{variable} contains unknown value(s) {string.Join(", ", unknown)}; allowed: {string.Join(", ", allowed)}, all.",
        nameof(value));
  }
}
