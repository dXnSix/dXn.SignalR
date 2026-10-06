using System.Globalization;
using System.Runtime.InteropServices;
using System.Text.Json;
using System.Text.Json.Serialization;
using RayX.SignalR.E2E.Tests.Infrastructure;
using RayX.SignalR.E2E.Tests.Scenarios;

namespace RayX.SignalR.E2E.Tests.Performance;

/// <summary>The machine and toolchains a performance run measured on.</summary>
/// <param name="Os">The operating system description.</param>
/// <param name="Architecture">The process architecture.</param>
/// <param name="ProcessorCount">The logical processor count.</param>
/// <param name="DotNet">The .NET runtime of the reference client.</param>
/// <param name="Rust">The <c>rustc</c> version that built the Rust client.</param>
/// <param name="Commit">The measured commit, with <c>-dirty</c> for uncommitted changes.</param>
public sealed record PerformanceEnvironment(
  string Os,
  string Architecture,
  int ProcessorCount,
  string DotNet,
  string Rust,
  string Commit);

/// <summary>One client's median result for one workload and protocol.</summary>
/// <param name="Client"><c>rust</c> or <c>dotnet</c>.</param>
/// <param name="Protocol">The hub protocol.</param>
/// <param name="Workload">The workload.</param>
/// <param name="Scale">The iteration scale.</param>
/// <param name="BuildProfile">The client's build profile (always <c>release</c> for recorded results).</param>
/// <param name="Metrics">The median of every metric over the rounds.</param>
/// <param name="Rounds">Every round's metrics, in run order.</param>
/// <param name="Environment">The machine and toolchains.</param>
public sealed record PerformanceResult(
  string Client,
  string Protocol,
  string Workload,
  string Scale,
  string BuildProfile,
  IReadOnlyDictionary<string, double> Metrics,
  IReadOnlyList<IReadOnlyDictionary<string, double>> Rounds,
  PerformanceEnvironment Environment);

/// <summary>
/// Runs one workload for the release Rust client and the official .NET client against the same
/// test service: three interleaved rounds (the order alternates each round), median of the rounds.
/// </summary>
public static class PerformanceRunner
{
  /// <summary>The Rust client.</summary>
  public const string Rust = "rust";

  /// <summary>The official .NET client.</summary>
  public const string DotNet = "dotnet";

  /// <summary>Rounds per client and row.</summary>
  public const int Rounds = 3;

  /// <summary>The metrics every client reports per workload.</summary>
  public static readonly IReadOnlyDictionary<string, string[]> ExpectedMetrics = new Dictionary<string, string[]>
  {
    ["invoke-latency"] = ["calls", "p50_us", "p99_us", "max_us", "warmup_calls", "warmup_ms"],
    ["stream-throughput"] = ["items", "elapsed_ms", "items_per_second", "warmup_items", "warmup_ms"],
    ["send-throughput"] = ["sends", "elapsed_ms", "sends_per_second", "warmup_sends", "warmup_ms"],
    ["fan-in"] = ["clients", "calls", "elapsed_ms", "calls_per_second", "warmup_calls", "warmup_ms"]
  };

  /// <summary>Runs every round of <paramref name="row"/> and returns one median result per client.</summary>
  /// <param name="environment">The running AppHost.</param>
  /// <param name="row">The workload and protocol.</param>
  /// <param name="scale">The iteration scale.</param>
  /// <param name="cancellationToken">Cancels the run.</param>
  /// <returns>The Rust and .NET results.</returns>
  public static async Task<IReadOnlyList<PerformanceResult>> RunAsync(
    AspireEnvironment environment,
    PerformanceRow row,
    string scale,
    CancellationToken cancellationToken)
  {
    string rust = await PerformanceBinaries.RustAsync(environment.RepositoryRoot, cancellationToken);
    string reference = await PerformanceBinaries.ReferenceAsync(environment.RepositoryRoot, cancellationToken);
    PerformanceEnvironment machine = await PerformanceBinaries.EnvironmentAsync(environment.RepositoryRoot);
    string[] arguments = [row.Workload, "--protocol", row.Protocol, "--scale", scale];
    Dictionary<string, string> variables = new(StringComparer.Ordinal)
    {
      ["RAYX_SIGNALR_E2E_SERVICE_URL"] = environment.ServiceUrl.GetLeftPart(UriPartial.Authority)
    };
    Dictionary<string, ProcessCommand> commands = new(StringComparer.Ordinal)
    {
      [Rust] = new ProcessCommand(rust, arguments, environment.RepositoryRoot) { Environment = variables },
      [DotNet] = new ProcessCommand("dotnet", [reference, .. arguments], environment.RepositoryRoot) { Environment = variables }
    };
    TimeSpan bound = scale == "full" ? TimeSpan.FromMinutes(10) : TimeSpan.FromMinutes(2);

    Dictionary<string, List<IReadOnlyDictionary<string, double>>> rounds = new(StringComparer.Ordinal)
    {
      [Rust] = [],
      [DotNet] = []
    };
    Dictionary<string, string> profiles = new(StringComparer.Ordinal);
    for(int round = 0; round < Rounds; round++)
    {
      string[] order = round % 2 == 0 ? [Rust, DotNet] : [DotNet, Rust];
      foreach(string client in order)
      {
        (IReadOnlyDictionary<string, double> metrics, string profile) = await RunOnceAsync(
          environment,
          commands[client],
          client,
          row,
          scale,
          bound,
          cancellationToken);
        rounds[client].Add(metrics);
        profiles[client] = profile;
      }
    }

    return
    [
      .. new[] { Rust, DotNet }.Select(client => new PerformanceResult(
        client,
        row.Protocol,
        row.Workload,
        scale,
        profiles[client],
        Median(rounds[client]),
        rounds[client],
        machine))
    ];
  }

  /// <summary>The median of every metric over the rounds.</summary>
  /// <param name="rounds">The rounds' metrics.</param>
  /// <returns>The medians.</returns>
  public static IReadOnlyDictionary<string, double> Median(IReadOnlyList<IReadOnlyDictionary<string, double>> rounds)
  {
    Dictionary<string, double> medians = new(StringComparer.Ordinal);
    foreach(string metric in rounds[0].Keys)
    {
      double[] values = [.. rounds.Select(round => round[metric]).Order()];
      int middle = values.Length / 2;
      medians[metric] = values.Length % 2 == 1 ? values[middle] : (values[middle - 1] + values[middle]) / 2;
    }

    return medians;
  }

  /// <summary>Parses and validates one client's result line.</summary>
  /// <param name="output">The process's standard output.</param>
  /// <param name="client">The expected client.</param>
  /// <param name="row">The expected workload and protocol.</param>
  /// <param name="scale">The expected scale.</param>
  /// <returns>The metrics and the client's build profile.</returns>
  public static (IReadOnlyDictionary<string, double> Metrics, string BuildProfile) ParseResult(
    string output,
    string client,
    PerformanceRow row,
    string scale)
  {
    string line = output.Split('\n', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries)
                        .LastOrDefault(candidate => candidate.StartsWith('{'))
                  ?? throw new InvalidOperationException($"{client} printed no result line.");
    using JsonDocument document = JsonDocument.Parse(line);
    JsonElement root = document.RootElement;
    foreach((string property, string expected) in new[]
            {
              ("client", client), ("protocol", row.Protocol), ("workload", row.Workload), ("scale", scale)
            })
    {
      string? actual = root.GetProperty(property).GetString();
      if(actual != expected)
      {
        throw new InvalidOperationException($"{client} reported {property} '{actual}' instead of '{expected}'.");
      }
    }

    Dictionary<string, double> metrics = new(StringComparer.Ordinal);
    foreach(JsonProperty metric in root.GetProperty("metrics").EnumerateObject())
    {
      metrics[metric.Name] = metric.Value.GetDouble();
    }

    string[] missing = [.. ExpectedMetrics[row.Workload].Where(metric => !metrics.TryGetValue(metric, out double value) || value <= 0)];
    if(missing.Length > 0)
    {
      throw new InvalidOperationException($"{client} reported no positive {string.Join(", ", missing)} for {row.Id}.");
    }

    return (metrics, root.GetProperty("build_profile").GetString() ?? string.Empty);
  }

  static async Task<(IReadOnlyDictionary<string, double> Metrics, string BuildProfile)> RunOnceAsync(
    AspireEnvironment environment,
    ProcessCommand command,
    string client,
    PerformanceRow row,
    string scale,
    TimeSpan bound,
    CancellationToken cancellationToken)
  {
    ProcessResult result = await ProcessRunner.RunAsync(command, bound, cancellationToken);
    string failure = result.TimedOut
      ? $"exceeded its {bound.TotalMinutes:0}-minute bound"
      : result.ExitCode != 0
        ? $"failed with exit code {result.ExitCode}"
        : string.Empty;
    if(failure.Length == 0)
    {
      try
      {
        (IReadOnlyDictionary<string, double> metrics, string profile) = ParseResult(result.StandardOutput, client, row, scale);
        // Debug builds are never measured: a result is recorded only from a release client.
        return profile == "release"
          ? (metrics, profile)
          : throw new InvalidOperationException($"{client} reported build profile '{profile}'; only release builds are measured.");
      }
      catch(Exception exception) when(exception is InvalidOperationException or JsonException or KeyNotFoundException)
      {
        failure = exception.Message;
      }
    }

    string message = $"perf {row.Id} ({client}) {failure}";
    string path = await CellDiagnostics.WriteAsync(
                    environment,
                    $"perf-{row.Workload}-{row.Protocol}-{client}",
                    message,
                    command,
                    result);
    throw new InvalidOperationException(
      $"{message}. Diagnostics: {path}\n{Redaction.Tail(Redaction.Redact(result.CombinedOutput), 8000)}");
  }
}

/// <summary>Builds the release Rust <c>perf</c> binary and the release .NET reference once per run.</summary>
public static class PerformanceBinaries
{
  static readonly SemaphoreSlim _gate = new(1, 1);
  static string? _rust;
  static string? _reference;
  static PerformanceEnvironment? _environment;

  /// <summary>The cargo arguments that build the release <c>perf</c> binary.</summary>
  public static readonly string[] RustBuildArguments =
    ["test", "--test", "perf", "--release", .. CellExecution.E2EFeatures, "--no-run", "--message-format=json"];

  /// <summary>The .NET reference project, relative to the repository root.</summary>
  public const string ReferenceProject = "dotnet/RayX.SignalR.PerfReference/RayX.SignalR.PerfReference.csproj";

  /// <summary>Builds (once) and returns the release Rust <c>perf</c> executable.</summary>
  /// <param name="repositoryRoot">The repository root.</param>
  /// <param name="cancellationToken">Cancels the build.</param>
  /// <returns>The executable path.</returns>
  public static async Task<string> RustAsync(string repositoryRoot, CancellationToken cancellationToken)
  {
    await _gate.WaitAsync(cancellationToken);
    try
    {
      if(_rust is not null && File.Exists(_rust))
      {
        return _rust;
      }

      ProcessCommand build = new("cargo", RustBuildArguments, repositoryRoot);
      ProcessResult result = await ProcessRunner.RunAsync(build, TimeSpan.FromMinutes(30), cancellationToken);
      if(!result.Succeeded)
      {
        throw new InvalidOperationException(
          $"{build.Display} failed with exit code {result.ExitCode}:\n{Redaction.SafeTail(NativeE2EBinary.CompilerErrors(result.StandardOutput) + result.StandardError)}");
      }

      _rust = NativeE2EBinary.ParseExecutable(result.StandardOutput, "perf");
      return _rust;
    }
    finally
    {
      _gate.Release();
    }
  }

  /// <summary>Builds (once) the release .NET reference and returns its assembly path.</summary>
  /// <param name="repositoryRoot">The repository root.</param>
  /// <param name="cancellationToken">Cancels the build.</param>
  /// <returns>The <c>RayX.SignalR.PerfReference.dll</c> path.</returns>
  public static async Task<string> ReferenceAsync(string repositoryRoot, CancellationToken cancellationToken)
  {
    await _gate.WaitAsync(cancellationToken);
    try
    {
      string output = Path.Combine(repositoryRoot, "artifacts-temp", "perf", "reference");
      string assembly = Path.Combine(output, "RayX.SignalR.PerfReference.dll");
      if(_reference is not null && File.Exists(_reference))
      {
        return _reference;
      }

      ProcessCommand build = new(
        "dotnet",
        ["build", Path.Combine(repositoryRoot, ReferenceProject), "-c", "Release", "-o", output, "--nologo"],
        repositoryRoot);
      ProcessResult result = await ProcessRunner.RunAsync(build, TimeSpan.FromMinutes(10), cancellationToken);
      if(!result.Succeeded || !File.Exists(assembly))
      {
        throw new InvalidOperationException(
          $"{build.Display} failed with exit code {result.ExitCode}:\n{Redaction.SafeTail(result.CombinedOutput)}");
      }

      _reference = assembly;
      return _reference;
    }
    finally
    {
      _gate.Release();
    }
  }

  /// <summary>Describes (once) the machine and toolchains of this run.</summary>
  /// <param name="repositoryRoot">The repository root.</param>
  /// <returns>The environment.</returns>
  public static async Task<PerformanceEnvironment> EnvironmentAsync(string repositoryRoot)
  {
    if(_environment is not null)
    {
      return _environment;
    }

    async Task<string> FirstLine(string program, params string[] arguments)
    {
      ProcessResult result = await ProcessRunner.RunAsync(
                               new ProcessCommand(program, arguments, repositoryRoot),
                               TimeSpan.FromSeconds(60));
      return result.Succeeded
        ? result.StandardOutput.Split('\n', StringSplitOptions.TrimEntries).FirstOrDefault() ?? string.Empty
        : "unknown";
    }

    string commit = await FirstLine("git", "rev-parse", "HEAD");
    string status = await FirstLine("git", "status", "--porcelain", "--untracked-files=no");
    _environment = new PerformanceEnvironment(
      RuntimeInformation.OSDescription,
      RuntimeInformation.ProcessArchitecture.ToString(),
      Environment.ProcessorCount,
      RuntimeInformation.FrameworkDescription,
      await FirstLine("rustc", "--version"),
      status.Length > 0 ? $"{commit}-dirty" : commit);
    return _environment;
  }
}

/// <summary>
/// This test process's run report, <c>artifacts-temp/perf/&lt;run&gt;.json</c>: every row's results
/// for both clients, rewritten after each row so a partial run still leaves its results.
/// </summary>
public static class PerformanceRunFile
{
  static readonly SemaphoreSlim _gate = new(1, 1);
  static readonly List<PerformanceResult> _results = [];

  static readonly JsonSerializerOptions _json = new()
  {
    WriteIndented = true,
    PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
    DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull
  };

  /// <summary>Gets this process's run identifier.</summary>
  public static string Run { get; } = string.Create(
    CultureInfo.InvariantCulture,
    $"{DateTime.UtcNow:yyyyMMdd-HHmmss}-{Environment.ProcessId}");

  /// <summary>Gets the report directory.</summary>
  /// <param name="repositoryRoot">The repository root.</param>
  /// <returns>The directory.</returns>
  public static string Directory(string repositoryRoot) => Path.Combine(repositoryRoot, "artifacts-temp", "perf");

  /// <summary>Adds a row's results and rewrites the report.</summary>
  /// <param name="repositoryRoot">The repository root.</param>
  /// <param name="results">The row's results.</param>
  /// <returns>The report path.</returns>
  public static async Task<string> AddAsync(string repositoryRoot, IReadOnlyList<PerformanceResult> results)
  {
    await _gate.WaitAsync();
    try
    {
      _results.RemoveAll(existing => results.Any(result =>
        result.Client == existing.Client && result.Protocol == existing.Protocol && result.Workload == existing.Workload));
      _results.AddRange(results);
      string directory = Directory(repositoryRoot);
      System.IO.Directory.CreateDirectory(directory);
      string path = Path.Combine(directory, $"{Run}.json");
      var report = new
      {
        SchemaVersion = 1,
        Run,
        Results = _results
      };
      await File.WriteAllTextAsync(path, JsonSerializer.Serialize(report, _json));
      return path;
    }
    finally
    {
      _gate.Release();
    }
  }
}
