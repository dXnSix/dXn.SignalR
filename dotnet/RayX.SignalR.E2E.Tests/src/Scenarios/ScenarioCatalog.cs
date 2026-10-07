namespace RayX.SignalR.E2E.Tests.Scenarios;

/// <summary>The kinds of runtime a cell executes on.</summary>
public enum TargetKind
{
  /// <summary>The prebuilt Rust <c>e2e</c> test binary on a desktop host, emulator or simulator.</summary>
  Native,

  /// <summary>The browser fixture driven by repository-local Playwright.</summary>
  Browser
}

/// <summary>A runtime target cells can execute on.</summary>
/// <param name="Name">The selection name.</param>
/// <param name="Kind">How the runner executes the target.</param>
public sealed record E2ETarget(string Name, TargetKind Kind);

/// <summary>An end-to-end scenario and the targets that run it.</summary>
/// <param name="Name">The kebab-case scenario name.</param>
/// <param name="Targets">The supported target names.</param>
public sealed record Scenario(string Name, IReadOnlySet<string> Targets)
{
  /// <summary>Gets targets intentionally excluded, with the documented reason.</summary>
  public IReadOnlyDictionary<string, string> Exclusions { get; init; } = new Dictionary<string, string>();

  /// <summary>Gets the protocols the scenario runs with (every protocol unless it is protocol-specific).</summary>
  public IReadOnlyList<string> Protocols { get; init; } = ScenarioCatalog.Protocols;

  /// <summary>Gets whether the scenario runs for the soak duration (<c>RAYX_SIGNALR_SOAK_SECONDS</c>).</summary>
  public bool Soak { get; init; }

  /// <summary>Gets a scenario-specific cell bound that replaces the default one.</summary>
  public TimeSpan? Limit { get; init; }

  /// <summary>Gets the <c>RUST_LOG</c> filter of the scenario's Rust cells.</summary>
  public string LogFilter { get; init; } = "rayx_signalr=info";

  /// <summary>Gets the bound of one cell: the soak duration plus 60 seconds, the scenario's own limit, or the default bound.</summary>
  public TimeSpan Bound => Soak
    ? CellExecution.SoakDuration + TimeSpan.FromSeconds(60)
    : Limit ?? CellExecution.CellTimeout;

  /// <summary>Gets the Rust module path segment (<c>ordinary-reconnect</c> → <c>ordinary_reconnect</c>).</summary>
  public string RustModule => Name.Replace('-', '_');

  /// <summary>Gets the Playwright title prefix (<c>ordinary-reconnect</c> → <c>ordinary reconnect</c>).</summary>
  public string PlaywrightWords => Name.Replace('-', ' ');

  /// <summary>Gets the exact libtest name for a protocol.</summary>
  /// <param name="protocol">The protocol.</param>
  /// <returns>The <c>module::protocol</c> test name.</returns>
  public string RustTest(string protocol) => $"{RustModule}::{protocol}";

  /// <summary>Gets the exact Playwright title for a protocol.</summary>
  /// <param name="protocol">The protocol.</param>
  /// <returns>The title.</returns>
  public string PlaywrightTitle(string protocol) =>
    $"{PlaywrightWords} {ScenarioCatalog.ProtocolTitle(protocol)}";
}

/// <summary>The single source of truth for which targets run which scenarios.</summary>
public static class ScenarioCatalog
{
  const string _native = "native";
  const string _chromium = "chromium";
  const string _firefox = "firefox";
  const string _webkit = "webkit";
  const string _android = "android";
  const string _ios = "ios";

  /// <summary>The scenario that needs the threaded fixture on the isolated endpoint.</summary>
  public const string ThreadedScenario = "threaded-wasm";

  /// <summary>Every known target.</summary>
  public static readonly IReadOnlyList<E2ETarget> Targets =
  [
    new(_native, TargetKind.Native),
    new(_chromium, TargetKind.Browser),
    new(_firefox, TargetKind.Browser),
    new(_webkit, TargetKind.Browser),
    new(_android, TargetKind.Native),
    new(_ios, TargetKind.Native)
  ];

  /// <summary>Every protocol, in run order.</summary>
  public static readonly IReadOnlyList<string> Protocols = ["json", "messagepack"];

  /// <summary>Targets that run the Rust <c>e2e</c> binary: the desktop host, the Android emulator and the iOS simulator.</summary>
  static readonly string[] _nativeTargets = [_native, _android, _ios];

  /// <summary>Playwright engines.</summary>
  static readonly string[] _browsers = [_chromium, _firefox, _webkit];

  /// <summary>Every scenario, in run order.</summary>
  public static readonly IReadOnlyList<Scenario> Scenarios =
  [
    Everywhere("connect-negotiate"),
    Everywhere("authentication"),
    Everywhere("invoke"),
    Everywhere("send"),
    Everywhere("server-stream"),
    Everywhere("callbacks"),
    Everywhere("concurrency-and-clones"),
    Everywhere("large-payload"),
    Everywhere("server-close"),
    Everywhere("reconnect-exhaustion"),
    Everywhere("tls"),
    Everywhere("negotiate-redirect"),
    Everywhere("skip-negotiation"),
    new("executor-independence", Set(_nativeTargets)),
    new("message-burst", Set(_browsers)),
    new("worker-transport", Set(_browsers)),
    new(ThreadedScenario, Set(_browsers)),
    Everywhere("ordinary-reconnect"),
    Everywhere("stateful-reconnect"),
    Everywhere("stream-cancellation"),
    new("short-stream-chaining", Set(_browsers)),
    Everywhere("keepalive-idle"),
    Everywhere("server-timeout"),
    Everywhere("handshake-timeout"),
    Everywhere("pause-resume"),
    Everywhere("pause-resume-stateful"),
    Everywhere("unsent-stream"),
    Everywhere("error-kinds"),
    Everywhere("disconnect-every-handle"),
    Everywhere("msgpack-fidelity") with { Protocols = ["messagepack"] },
    Everywhere("callback-dispatch"),
    Everywhere("client-result-errors"),
    Everywhere("connected-callback"),
    Everywhere("restart-after-stop"),
    Everywhere("lifecycle-events"),
    Everywhere("connection-id"),
    Everywhere("graceful-stop"),
    Everywhere("reconnect-call-wait"),
    Everywhere("request-headers"),
    Everywhere("stateful-transparent-resume"),
    Everywhere("stateful-resume-fallback"),
    new("soak-chaos", Set([_native, _chromium, _android]))
    {
      Soak = true,
      Exclusions = new Dictionary<string, string>
      {
        [_firefox] = "a soak cell runs for minutes; one browser engine (Chromium) proves the browser transport under chaos",
        [_webkit] = "a soak cell runs for minutes; one browser engine (Chromium) proves the browser transport under chaos",
        [_ios] = "a soak cell runs for minutes; the Android emulator proves the mobile native build under chaos"
      }
    },
    new("stateful-resume-stress", Set([_native, _chromium, _android]))
    {
      Limit = TimeSpan.FromSeconds(600),
      // The stateful sequence trace reaches the cell diagnostics.
      LogFilter = "rayx_signalr=info,rayx_signalr::stateful=debug",
      Exclusions = new Dictionary<string, string>
      {
        [_firefox] = "a stress cell runs for minutes; one browser engine (Chromium) proves the browser stateful resume",
        [_webkit] = "a stress cell runs for minutes; one browser engine (Chromium) proves the browser stateful resume",
        [_ios] = "a stress cell runs for minutes; the Android emulator proves the mobile native build"
      }
    }
  ];

  /// <summary>Looks up a target by name.</summary>
  /// <param name="name">The target name.</param>
  /// <returns>The target.</returns>
  public static E2ETarget Target(string name) =>
    Targets.FirstOrDefault(target => target.Name == name)
    ?? throw new ArgumentException($"Unknown end-to-end target '{name}'.", nameof(name));

  /// <summary>Looks up a scenario by name.</summary>
  /// <param name="name">The scenario name.</param>
  /// <returns>The scenario.</returns>
  public static Scenario Scenario(string name) =>
    Scenarios.FirstOrDefault(scenario => scenario.Name == name)
    ?? throw new ArgumentException($"Unknown end-to-end scenario '{name}'.", nameof(name));

  /// <summary>Gets the Playwright title suffix for a protocol.</summary>
  /// <param name="protocol">The protocol.</param>
  /// <returns><c>JSON</c> or <c>MessagePack</c>.</returns>
  public static string ProtocolTitle(string protocol) => protocol switch
  {
    "json" => "JSON",
    "messagepack" => "MessagePack",
    _ => throw new ArgumentException($"Unknown protocol '{protocol}'.", nameof(protocol))
  };

  /// <summary>Gets every native-binary target the catalog claims for a scenario.</summary>
  /// <param name="scenario">The scenario.</param>
  /// <returns>Whether any native-kind target runs the scenario.</returns>
  public static bool RunsNatively(Scenario scenario) =>
    scenario.Targets.Any(target => Target(target).Kind == TargetKind.Native);

  /// <summary>Gets the browser engines the catalog claims for a scenario.</summary>
  /// <param name="scenario">The scenario.</param>
  /// <returns>The browser target names.</returns>
  public static IEnumerable<string> BrowserEngines(Scenario scenario) =>
    scenario.Targets.Where(target => Target(target).Kind == TargetKind.Browser);

  static Scenario Everywhere(string name) => new(name, Set([.. _nativeTargets, .. _browsers]));

  static HashSet<string> Set(params string[] targets) => new(targets, StringComparer.Ordinal);
}
