namespace RayX.SignalR.E2E.Tests.Scenarios;

/// <summary>One executable target × protocol × scenario × transport combination.</summary>
/// <param name="Target">The target.</param>
/// <param name="Protocol">The protocol.</param>
/// <param name="Scenario">The scenario.</param>
/// <param name="Transport">The transport (WebSockets by default).</param>
public sealed record E2ECell(
  E2ETarget Target,
  string Protocol,
  Scenario Scenario,
  string Transport = ScenarioCatalog.WebSockets)
{
  /// <summary>
  /// Gets the stable cell identifier: <c>target/protocol/scenario</c> on WebSockets (the names CI
  /// history knows), with a <c>/transport</c> suffix on the other transports.
  /// </summary>
  public string Id => Transport == ScenarioCatalog.WebSockets
    ? $"{Target.Name}/{Protocol}/{Scenario.Name}"
    : $"{Target.Name}/{Protocol}/{Scenario.Name}/{Transport}";

  /// <summary>Gets a file-name-safe form of <see cref="Id"/>.</summary>
  public string FileName => Id.Replace('/', '-');

  /// <summary>Parses an identifier produced by <see cref="Id"/>.</summary>
  /// <param name="id">The identifier.</param>
  /// <returns>The cell.</returns>
  public static E2ECell Parse(string id)
  {
    string[] parts = id.Split('/');
    if(parts.Length is not (3 or 4))
    {
      throw new ArgumentException($"Invalid cell identifier '{id}'.", nameof(id));
    }

    string transport = parts.Length == 4 ? parts[3] : ScenarioCatalog.WebSockets;
    return new E2ECell(
      ScenarioCatalog.Target(parts[0]),
      ScenarioCatalog.Protocols.Contains(parts[1])
        ? parts[1]
        : throw new ArgumentException($"Unknown protocol '{parts[1]}'.", nameof(id)),
      ScenarioCatalog.Scenario(parts[2]),
      ScenarioCatalog.Transports.Contains(transport) && (parts.Length == 3 || transport != ScenarioCatalog.WebSockets)
        ? transport
        : throw new ArgumentException($"Unknown transport '{transport}'.", nameof(id)));
  }
}

/// <summary>The cells selected through <c>RAYX_SIGNALR_E2E_*</c> environment variables.</summary>
/// <param name="Targets">Selected target names.</param>
/// <param name="Protocols">Selected protocols.</param>
/// <param name="Scenarios">Selected scenario names.</param>
/// <param name="Transports">Selected transports.</param>
public sealed record E2ESelection(
  IReadOnlyList<string> Targets,
  IReadOnlyList<string> Protocols,
  IReadOnlyList<string> Scenarios,
  IReadOnlyList<string> Transports)
{
  /// <summary>Comma-separated targets, or <c>all</c>; defaults to <c>native</c>.</summary>
  public const string TargetsVariable = "RAYX_SIGNALR_E2E_TARGETS";

  /// <summary>Comma-separated protocols, or <c>all</c>; defaults to <c>all</c>.</summary>
  public const string ProtocolsVariable = "RAYX_SIGNALR_E2E_PROTOCOLS";

  /// <summary>Comma-separated scenarios, or <c>all</c>; defaults to <c>all</c>.</summary>
  public const string ScenariosVariable = "RAYX_SIGNALR_E2E_SCENARIOS";

  /// <summary>
  /// Comma-separated transports (<c>websockets</c>, <c>long-polling</c>, <c>server-sent-events</c>),
  /// or <c>all</c>; defaults to <c>websockets</c>.
  /// </summary>
  public const string TransportsVariable = "RAYX_SIGNALR_E2E_TRANSPORTS";

  /// <summary>Gets whether any selected target needs the browser fixture.</summary>
  public bool RequiresBrowserFixture =>
    Targets.Any(target => ScenarioCatalog.Target(target).Kind == TargetKind.Browser);

  /// <summary>Gets whether a selected cell needs the threaded, cross-origin-isolated fixture.</summary>
  public bool RequiresThreadedFixture =>
    RequiresBrowserFixture
    && Cells().Any(cell => cell.Target.Kind == TargetKind.Browser
                           && cell.Scenario.Name == ScenarioCatalog.ThreadedScenario);

  /// <summary>Reads the selection from the process environment.</summary>
  /// <returns>The selection.</returns>
  public static E2ESelection FromEnvironment() =>
    Parse(
      Environment.GetEnvironmentVariable(TargetsVariable),
      Environment.GetEnvironmentVariable(ProtocolsVariable),
      Environment.GetEnvironmentVariable(ScenariosVariable),
      Environment.GetEnvironmentVariable(TransportsVariable));

  /// <summary>Parses selector values; unset values use their defaults.</summary>
  /// <param name="targets">The target selector.</param>
  /// <param name="protocols">The protocol selector.</param>
  /// <param name="scenarios">The scenario selector.</param>
  /// <param name="transports">The transport selector.</param>
  /// <returns>The selection.</returns>
  public static E2ESelection Parse(
    string? targets,
    string? protocols,
    string? scenarios,
    string? transports = null) =>
    new(
      Expand(TargetsVariable, targets ?? "native", ScenarioCatalog.Targets.Select(target => target.Name).ToArray()),
      Expand(ProtocolsVariable, protocols ?? "all", ScenarioCatalog.Protocols),
      Expand(ScenariosVariable, scenarios ?? "all", ScenarioCatalog.Scenarios.Select(scenario => scenario.Name).ToArray()),
      Expand(TransportsVariable, transports ?? ScenarioCatalog.WebSockets, ScenarioCatalog.Transports));

  /// <summary>Expands the selection into the cells the catalog supports.</summary>
  /// <returns>The cells; never empty.</returns>
  /// <exception cref="InvalidOperationException">No catalog cell matches the selection.</exception>
  public IReadOnlyList<E2ECell> Cells() => Cells(OperatingSystem.IsWindows());

  /// <summary>Gets the selected cells for a host that is, or is not, Windows.</summary>
  /// <param name="windowsHost">Whether the host runs Windows (Windows-only scenarios need it).</param>
  /// <returns>The cells.</returns>
  public IReadOnlyList<E2ECell> Cells(bool windowsHost)
  {
    List<E2ECell> cells = [];
    foreach(string transport in Transports)
    {
      foreach(string target in Targets)
      {
        foreach(string protocol in Protocols)
        {
          foreach(string name in Scenarios)
          {
            Scenario scenario = ScenarioCatalog.Scenario(name);
            if(scenario.Targets.Contains(target)
               && scenario.Protocols.Contains(protocol)
               && scenario.Supports(transport, protocol)
               && (windowsHost || !scenario.WindowsHostOnly))
            {
              cells.Add(new E2ECell(ScenarioCatalog.Target(target), protocol, scenario, transport));
            }
          }
        }
      }
    }

    return cells.Count > 0
      ? cells
      : throw new InvalidOperationException(
        $"The end-to-end selection matches no cataloged cell: {TargetsVariable}={string.Join(',', Targets)}, {ProtocolsVariable}={string.Join(',', Protocols)}, {ScenariosVariable}={string.Join(',', Scenarios)}, {TransportsVariable}={string.Join(',', Transports)}.");
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
