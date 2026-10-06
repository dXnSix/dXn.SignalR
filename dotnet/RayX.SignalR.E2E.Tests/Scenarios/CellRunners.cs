using System.Text.Json;
using System.Text.RegularExpressions;
using RayX.SignalR.E2E.Tests.Infrastructure;

namespace RayX.SignalR.E2E.Tests.Scenarios;

/// <summary>The result of running one cell.</summary>
/// <param name="Command">The executed command.</param>
/// <param name="Result">The process result.</param>
/// <param name="Passed">Whether exactly one scenario executed and passed within the bound.</param>
/// <param name="Failure">A failure description when <paramref name="Passed"/> is false.</param>
public sealed record CellOutcome(ProcessCommand Command, ProcessResult Result, bool Passed, string Failure);

/// <summary>Executes cells of one target kind.</summary>
public interface ICellRunner
{
  /// <summary>Runs one cell.</summary>
  /// <param name="cell">The cell.</param>
  /// <param name="environment">The running AppHost.</param>
  /// <param name="cancellationToken">Cancels the run.</param>
  /// <returns>The outcome.</returns>
  Task<CellOutcome> RunAsync(E2ECell cell, AspireEnvironment environment, CancellationToken cancellationToken);
}

/// <summary>Shared cell bounds, environment and verdicts.</summary>
public static partial class CellExecution
{
  /// <summary>
  /// The cargo features of the host and iOS <c>e2e</c> binaries: the default TLS stack
  /// (platform verifier with the <c>ring</c> provider, as consumers get it) plus MessagePack.
  /// <c>--all-features</c> would select <c>aws-lc-rs</c> and leave the default provider without
  /// a real handshake.
  /// </summary>
  public static readonly string[] E2EFeatures = ["--features", "messagepack"];

  /// <summary>The hard bound for one cell's process.</summary>
  public static readonly TimeSpan CellTimeout = TimeSpan.FromSeconds(60);

  /// <summary>
  /// The libtest arguments of a Rust cell: one exact test, uncaptured output, so a cell killed at
  /// its bound still leaves its client log in the diagnostics.
  /// </summary>
  /// <param name="test">The exact <c>module::protocol</c> test name.</param>
  /// <returns>The arguments.</returns>
  public static string[] LibtestArguments(string test) => [test, "--exact", "--test-threads=1", "--nocapture"];

  /// <summary>Builds the <c>RAYX_SIGNALR_E2E_*</c> variables every scenario reads.</summary>
  /// <param name="environment">The running AppHost.</param>
  /// <param name="cell">The cell.</param>
  /// <returns>The variables.</returns>
  public static Dictionary<string, string> ScenarioVariables(AspireEnvironment environment, E2ECell cell)
  {
    Dictionary<string, string> variables = new(StringComparer.Ordinal)
    {
      ["RAYX_SIGNALR_E2E_SERVICE_URL"] = Origin(environment.ServiceUrl),
      ["RAYX_SIGNALR_E2E_TLS_URL"] = Origin(environment.TlsServiceUrl),
      ["RAYX_SIGNALR_E2E_TLS_CA_PEM_BASE64"] = Convert.ToBase64String(System.Text.Encoding.ASCII.GetBytes(environment.TlsCaPem)),
      ["RAYX_SIGNALR_E2E_PROTOCOL"] = cell.Protocol,
      // Rust cells log the client's lifecycle (diagnostics keep a bounded, redacted tail).
      ["RUST_LOG"] = "rayx_signalr=info"
    };
    if(environment.FixtureEnabled)
    {
      variables["RAYX_SIGNALR_E2E_FIXTURE_URL"] = Origin(environment.FixtureUrl);
    }

    if(environment.ThreadsEnabled)
    {
      variables["RAYX_SIGNALR_E2E_ISOLATED_URL"] = Origin(environment.IsolatedFixtureUrl);
    }

    return variables;
  }

  /// <summary>Judges a libtest run that must have executed exactly one passing test.</summary>
  /// <param name="command">The command.</param>
  /// <param name="result">The result.</param>
  /// <returns>The outcome.</returns>
  public static CellOutcome JudgeLibtest(ProcessCommand command, ProcessResult result) =>
    Judge(command, result, LibtestOnePassed().IsMatch(result.StandardOutput), "libtest did not report exactly one passed test");

  /// <summary>
  /// Judges a libtest run whose launcher may outlive the test: <c>xcrun simctl spawn</c> sometimes
  /// does not return after the simulator process finished. A run that timed out after libtest
  /// already reported exactly one passed test passes, with a note in
  /// its output; a scenario that did not finish within the bound still fails.
  /// </summary>
  /// <param name="command">The command.</param>
  /// <param name="result">The result.</param>
  /// <returns>The outcome.</returns>
  public static CellOutcome JudgeLibtestAllowingLauncherHang(ProcessCommand command, ProcessResult result)
  {
    if(result.TimedOut && LibtestOnePassed().IsMatch(result.StandardOutput))
    {
      ProcessResult finished = result with
      {
        StandardError = result.StandardError
                        + "\nnote: the scenario passed, but the launcher did not exit within the cell bound and was stopped"
      };
      return new CellOutcome(command, finished, true, string.Empty);
    }

    return JudgeLibtest(command, result);
  }

  /// <summary>Judges a Playwright run that must have executed exactly one passing test.</summary>
  /// <param name="command">The command.</param>
  /// <param name="result">The result.</param>
  /// <returns>The outcome.</returns>
  public static CellOutcome JudgePlaywright(ProcessCommand command, ProcessResult result) =>
    Judge(
      command,
      result,
      PlaywrightOnePassed().IsMatch(result.StandardOutput)
      && !PlaywrightOtherOutcome().IsMatch(result.StandardOutput),
      "Playwright did not report exactly one passed test");

  static CellOutcome Judge(ProcessCommand command, ProcessResult result, bool exactlyOnePassed, string summaryFailure)
  {
    if(result.TimedOut)
    {
      return new CellOutcome(command, result, false, $"exceeded the {CellTimeout.TotalSeconds:0}-second cell bound");
    }

    if(result.ExitCode != 0)
    {
      return new CellOutcome(command, result, false, $"failed with exit code {result.ExitCode}");
    }

    return exactlyOnePassed
      ? new CellOutcome(command, result, true, string.Empty)
      : new CellOutcome(command, result, false, summaryFailure);
  }

  static string Origin(Uri uri) => uri.GetLeftPart(UriPartial.Authority);

  [GeneratedRegex(@"(?m)^test result: ok\. 1 passed; 0 failed;")]
  private static partial Regex LibtestOnePassed();

  [GeneratedRegex(@"(?m)^\s*1 passed\b")]
  private static partial Regex PlaywrightOnePassed();

  [GeneratedRegex(@"(?m)^\s*\d+ (failed|flaky|skipped|did not run|interrupted)\b")]
  private static partial Regex PlaywrightOtherOutcome();
}

/// <summary>Builds the Rust <c>e2e</c> test binary once per run.</summary>
public static class NativeE2EBinary
{
  static readonly SemaphoreSlim _gate = new(1, 1);
  static string? _executable;

  /// <summary>Builds (once) and returns the host <c>e2e</c> test executable.</summary>
  /// <param name="repositoryRoot">The repository root.</param>
  /// <param name="cancellationToken">Cancels the build.</param>
  /// <returns>The executable path.</returns>
  public static async Task<string> BuildAsync(string repositoryRoot, CancellationToken cancellationToken)
  {
    await _gate.WaitAsync(cancellationToken);
    try
    {
      if(_executable is not null && File.Exists(_executable))
      {
        return _executable;
      }

      ProcessCommand build = new(
        "cargo",
        ["test", "--test", "e2e", .. CellExecution.E2EFeatures, "--no-run", "--message-format=json"],
        repositoryRoot);
      ProcessResult result = await ProcessRunner.RunAsync(build, TimeSpan.FromMinutes(20), cancellationToken);
      if(!result.Succeeded)
      {
        throw new InvalidOperationException(
          $"{build.Display} failed with exit code {result.ExitCode}:\n{Redaction.SafeTail(CompilerErrors(result.StandardOutput) + result.StandardError)}");
      }

      _executable = ParseExecutable(result.StandardOutput);
      return _executable;
    }
    finally
    {
      _gate.Release();
    }
  }

  /// <summary>
  /// Extracts the rendered compiler errors from <c>cargo --message-format=json</c> output, which
  /// carries them on standard output instead of standard error.
  /// </summary>
  /// <param name="messages">The JSON lines.</param>
  /// <returns>The rendered errors, one per line block.</returns>
  public static string CompilerErrors(string messages)
  {
    System.Text.StringBuilder errors = new();
    foreach(string line in messages.Split('\n', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries))
    {
      if(!line.StartsWith('{'))
      {
        continue;
      }

      using JsonDocument message = JsonDocument.Parse(line);
      JsonElement root = message.RootElement;
      if(root.TryGetProperty("reason", out JsonElement reason)
         && reason.GetString() == "compiler-message"
         && root.TryGetProperty("message", out JsonElement compiler)
         && compiler.TryGetProperty("level", out JsonElement level)
         && level.GetString() == "error"
         && compiler.TryGetProperty("rendered", out JsonElement rendered)
         && rendered.ValueKind == JsonValueKind.String)
      {
        errors.AppendLine(rendered.GetString());
      }
    }

    return errors.ToString();
  }

  /// <summary>Finds the <c>e2e</c> test executable in <c>cargo --message-format=json</c> output.</summary>
  /// <param name="messages">The JSON lines.</param>
  /// <returns>The executable path.</returns>
  public static string ParseExecutable(string messages)
  {
    foreach(string line in messages.Split('\n', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries))
    {
      if(!line.StartsWith('{'))
      {
        continue;
      }

      using JsonDocument message = JsonDocument.Parse(line);
      JsonElement root = message.RootElement;
      if(root.TryGetProperty("reason", out JsonElement reason)
         && reason.GetString() == "compiler-artifact"
         && root.GetProperty("target").GetProperty("name").GetString() == "e2e"
         && root.TryGetProperty("executable", out JsonElement executable)
         && executable.ValueKind == JsonValueKind.String)
      {
        return executable.GetString()!;
      }
    }

    throw new InvalidOperationException("cargo did not report an executable for the e2e test target.");
  }
}

/// <summary>Runs native cells through the prebuilt <c>e2e</c> binary on the host.</summary>
public sealed class NativeCellRunner : ICellRunner
{
  /// <inheritdoc/>
  public async Task<CellOutcome> RunAsync(
    E2ECell cell,
    AspireEnvironment environment,
    CancellationToken cancellationToken)
  {
    string executable = await NativeE2EBinary.BuildAsync(environment.RepositoryRoot, cancellationToken);
    ProcessCommand command = new(
      executable,
      CellExecution.LibtestArguments(cell.Scenario.RustTest(cell.Protocol)),
      environment.RepositoryRoot)
    {
      Environment = CellExecution.ScenarioVariables(environment, cell)
    };
    ProcessResult result = await ProcessRunner.RunAsync(command, CellExecution.CellTimeout, cancellationToken);
    return CellExecution.JudgeLibtest(command, result);
  }
}

/// <summary>Runs browser cells through repository-local Playwright against <c>fixture-host</c>.</summary>
public sealed class BrowserCellRunner : ICellRunner
{
  /// <summary>Builds the Playwright CLI command for a project and optional exact title.</summary>
  /// <param name="repositoryRoot">The repository root.</param>
  /// <param name="engine">The Playwright project.</param>
  /// <param name="title">The exact title, or <see langword="null"/> with <paramref name="list"/>.</param>
  /// <param name="list">Whether to list instead of run.</param>
  /// <returns>The command.</returns>
  public static ProcessCommand Command(string repositoryRoot, string engine, string? title, bool list = false)
  {
    List<string> arguments =
    [
      Path.Combine(repositoryRoot, "tools-node", "node_modules", "@playwright", "test", "cli.js"),
      "test",
      "--config",
      Path.Combine(repositoryRoot, "tools-node", "playwright.config.ts"),
      $"--project={engine}",
      "--workers=1",
      "--reporter=list"
    ];
    if(list)
    {
      arguments.Add("--list");
    }

    if(title is not null)
    {
      // Playwright matches against "<project> <file> <title>"; anchor on the whole title.
      arguments.Add("--grep");
      arguments.Add($"/ {EscapeJavaScriptRegex(title)}$/");
    }

    return new ProcessCommand("node", arguments, Path.Combine(repositoryRoot, "tools-node"))
    {
      Environment = new Dictionary<string, string>(StringComparer.Ordinal) { ["PLAYWRIGHT_BROWSERS_PATH"] = "0" }
    };
  }

  static string EscapeJavaScriptRegex(string value) =>
    string.Concat(value.Select(character => @"\^$.|?*+()[]{}/".Contains(character) ? $@"\{character}" : $"{character}"));

  /// <inheritdoc/>
  public async Task<CellOutcome> RunAsync(
    E2ECell cell,
    AspireEnvironment environment,
    CancellationToken cancellationToken)
  {
    ProcessCommand template = Command(
      environment.RepositoryRoot,
      cell.Target.Name,
      cell.Scenario.PlaywrightTitle(cell.Protocol));
    Dictionary<string, string> variables = CellExecution.ScenarioVariables(environment, cell);
    foreach((string key, string value) in template.Environment)
    {
      variables[key] = value;
    }

    ProcessCommand command = template with { Environment = variables };
    ProcessResult result = await ProcessRunner.RunAsync(command, CellExecution.CellTimeout, cancellationToken);
    return CellExecution.JudgePlaywright(command, result);
  }
}
