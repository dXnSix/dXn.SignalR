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

  /// <summary>
  /// Gets whether the scenario belongs to the transport baseline: it runs on every selected
  /// transport, while every other scenario runs on WebSockets only.
  /// </summary>
  public bool TransportBaseline { get; init; }

  /// <summary>Whether the scenario has a cell for <paramref name="transport"/> and <paramref name="protocol"/>.</summary>
  /// <param name="transport">The transport.</param>
  /// <param name="protocol">The protocol.</param>
  /// <returns>Whether the cell exists.</returns>
  public bool Supports(string transport, string protocol) =>
    transport == ScenarioCatalog.WebSockets
    || (TransportBaseline
        && !(transport == ScenarioCatalog.ServerSentEvents && protocol == "messagepack"));

  /// <summary>
  /// Gets whether the runner suspends the scenario process after its readiness line
  /// (<see cref="Suspension"/>); browsers freeze the page from the Playwright spec.
  /// </summary>
  public bool Suspends { get; init; }

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

  /// <summary>
  /// Gets the exact Playwright title for a protocol and transport: <c>invoke JSON</c> on WebSockets
  /// (the names CI history knows), <c>invoke (JSON, Long Polling)</c> on the other transports.
  /// </summary>
  /// <param name="protocol">The protocol.</param>
  /// <param name="transport">The transport.</param>
  /// <returns>The title.</returns>
  public string PlaywrightTitle(string protocol, string transport = ScenarioCatalog.WebSockets) =>
    transport == ScenarioCatalog.WebSockets
      ? $"{PlaywrightWords} {ScenarioCatalog.ProtocolTitle(protocol)}"
      : $"{PlaywrightWords} ({ScenarioCatalog.ProtocolTitle(protocol)}, {ScenarioCatalog.TransportTitle(transport)})";
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
  const string _androidApp = "android-app";
  const string _androidAppRelease = "android-app-release";

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
    new(_ios, TargetKind.Native),
    // The shared scenarios inside the test-only Android app (debug, and minified release).
    new(_androidApp, TargetKind.Native),
    new(_androidAppRelease, TargetKind.Native)
  ];

  /// <summary>Every protocol, in run order.</summary>
  public static readonly IReadOnlyList<string> Protocols = ["json", "messagepack"];

  /// <summary>The WebSockets transport (the default selection).</summary>
  public const string WebSockets = "websockets";

  /// <summary>The Long Polling transport.</summary>
  public const string LongPolling = "long-polling";

  /// <summary>The Server-Sent Events transport (JSON only).</summary>
  public const string ServerSentEvents = "server-sent-events";

  /// <summary>Every transport, in run order.</summary>
  public static readonly IReadOnlyList<string> Transports = [WebSockets, LongPolling, ServerSentEvents];

  /// <summary>Gets the title of a transport in cell names.</summary>
  /// <param name="transport">The transport.</param>
  /// <returns>The title.</returns>
  public static string TransportTitle(string transport) => transport switch
  {
    WebSockets => "WebSockets",
    LongPolling => "Long Polling",
    ServerSentEvents => "Server-Sent Events",
    _ => throw new ArgumentException($"Unknown transport '{transport}'.", nameof(transport))
  };

  /// <summary>Targets that run the Rust <c>e2e</c> binary: the desktop host, the Android emulator and the iOS simulator.</summary>
  static readonly string[] _nativeTargets = [_native, _android, _ios];

  /// <summary>
  /// The Android app targets: they run the platform verifier through the Android trust store, so
  /// only the TLS-proving scenarios run there (R10.C2).
  /// </summary>
  static readonly string[] _appTargets = [_androidApp, _androidAppRelease];

  /// <summary>Playwright engines.</summary>
  static readonly string[] _browsers = [_chromium, _firefox, _webkit];

  /// <summary>Every scenario, in run order.</summary>
  public static readonly IReadOnlyList<Scenario> Scenarios =
  [
    Baseline("connect-negotiate") with { Targets = Set([.. _nativeTargets, .. _browsers, .. _appTargets]) },
    Baseline("authentication"),
    Baseline("invoke") with { Targets = Set([.. _nativeTargets, .. _browsers, .. _appTargets]) },
    Baseline("send"),
    Baseline("server-stream"),
    Baseline("callbacks"),
    Baseline("concurrency-and-clones"),
    Baseline("large-payload"),
    Baseline("server-close"),
    Baseline("reconnect-exhaustion"),
    Everywhere("tls") with { Targets = Set([.. _nativeTargets, .. _browsers, .. _appTargets]) },
    Everywhere("negotiate-redirect"),
    Everywhere("skip-negotiation"),
    new("executor-independence", Set(_nativeTargets)),
    // Browsers use the browser's proxy settings: the crate has no proxy API there. A dozen
    // tunnelled connections take most of a minute on an emulator, so the bound is longer.
    new("proxy", Set(_nativeTargets)) { Limit = TimeSpan.FromSeconds(120) },
    new("message-burst", Set(_browsers)),
    new("worker-transport", Set(_browsers)),
    new(ThreadedScenario, Set(_browsers)),
    Baseline("ordinary-reconnect"),
    Everywhere("stateful-reconnect"),
    Everywhere("stream-cancellation"),
    new("short-stream-chaining", Set(_browsers)),
    // Over Long Polling these idle for 45 s (inherent keep-alive), so they get a longer bound.
    Baseline("keepalive-idle") with { Limit = TimeSpan.FromSeconds(120) },
    Baseline("server-timeout") with { Limit = TimeSpan.FromSeconds(120) },
    Everywhere("handshake-timeout"),
    Baseline("pause-resume"),
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
    Baseline("token-factory"),
    Everywhere("negotiate-timeout"),
    Everywhere("transport-fallback"),
    Everywhere("client-to-server-stream"),
    // The process is suspended (the Chromium page frozen) for 40 s, so the bound is longer.
    new("suspend-resume", Set([_native, _chromium, _android, _ios]))
    {
      Suspends = true,
      Limit = TimeSpan.FromSeconds(120),
      Exclusions = new Dictionary<string, string>
      {
        [_firefox] = "the page is frozen through CDP (Debugger.pause), which Playwright offers only for Chromium",
        [_webkit] = "the page is frozen through CDP (Debugger.pause), which Playwright offers only for Chromium"
      }
    },
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

  /// <summary>A transport-baseline scenario (R6.C2): every target, every selected transport.</summary>
  static Scenario Baseline(string name) => Everywhere(name) with { TransportBaseline = true };

  static HashSet<string> Set(params string[] targets) => new(targets, StringComparer.Ordinal);
}
