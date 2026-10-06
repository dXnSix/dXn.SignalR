using System.Text;
using System.Text.Json;
using RayX.SignalR.E2E.Tests.Infrastructure;

namespace RayX.SignalR.E2E.Tests.Scenarios;

/// <summary>Builds the Rust <c>e2e</c> test binary once per build command and run.</summary>
public static class E2EBinaryBuilds
{
  static readonly SemaphoreSlim _gate = new(1, 1);
  static readonly Dictionary<string, string> _executables = new(StringComparer.Ordinal);

  /// <summary>Runs <paramref name="build"/> once and returns the <c>e2e</c> executable it produced.</summary>
  /// <param name="build">A <c>cargo … test --test e2e --no-run --message-format=json</c> command.</param>
  /// <param name="cancellationToken">Cancels the build.</param>
  /// <returns>The executable path on the host.</returns>
  public static async Task<string> BuildAsync(ProcessCommand build, CancellationToken cancellationToken)
  {
    await _gate.WaitAsync(cancellationToken);
    try
    {
      if(_executables.TryGetValue(build.Display, out string? cached) && File.Exists(cached))
      {
        return cached;
      }

      ProcessResult result = await ProcessRunner.RunAsync(build, TimeSpan.FromMinutes(30), cancellationToken);
      if(!result.Succeeded)
      {
        throw new InvalidOperationException(
          $"{build.Display} failed with exit code {result.ExitCode}:\n{Redaction.SafeTail(result.StandardError)}");
      }

      string executable = NativeE2EBinary.ParseExecutable(result.StandardOutput);
      _executables[build.Display] = executable;
      return executable;
    }
    finally
    {
      _gate.Release();
    }
  }
}

/// <summary>Rewrites the scenario endpoints for a device that reaches the host through another address.</summary>
public static class DeviceEndpoints
{
  /// <summary>Replaces the host of every <c>RAYX_SIGNALR_E2E_*_URL</c> variable.</summary>
  /// <param name="variables">The scenario variables.</param>
  /// <param name="host">The host the device uses for the development machine.</param>
  /// <returns>The rewritten variables.</returns>
  public static Dictionary<string, string> Rewrite(IReadOnlyDictionary<string, string> variables, string host)
  {
    Dictionary<string, string> rewritten = new(StringComparer.Ordinal);
    foreach((string key, string value) in variables)
    {
      rewritten[key] = key.EndsWith("_URL", StringComparison.Ordinal)
                       && Uri.TryCreate(value, UriKind.Absolute, out Uri? uri)
        ? new UriBuilder(uri) { Host = host }.Uri.GetLeftPart(UriPartial.Authority)
        : value;
    }

    return rewritten;
  }
}

/// <summary>
/// Runs native cells on an Android emulator: the <c>e2e</c> binary is built with <c>cargo ndk</c>
/// for x86_64 (API 24), pushed with <c>adb</c> and run per cell with the service endpoints
/// rewritten to <c>10.0.2.2</c>, the emulator's alias for the host loopback.
/// </summary>
public sealed class AndroidCellRunner : ICellRunner
{
  /// <summary>Selects the device; defaults to the single running emulator.</summary>
  public const string SerialVariable = "RAYX_SIGNALR_E2E_ANDROID_SERIAL";

  /// <summary>The device directory that holds the pushed binary; removed after the run.</summary>
  public const string DeviceDirectory = "/data/local/tmp/rayx-signalr-e2e";

  /// <summary>The emulator's address for the host loopback.</summary>
  public const string HostAlias = "10.0.2.2";

  static readonly SemaphoreSlim _gate = new(1, 1);
  static string? _serial;
  static string? _executable;
  static string? _pushed;

  /// <summary>The Rust target of the emulator binary.</summary>
  public const string RustTarget = "x86_64-linux-android";

  /// <summary>The cargo command that builds the Android <c>e2e</c> binary.</summary>
  /// <param name="repositoryRoot">The repository root.</param>
  /// <returns>The command.</returns>
  public static ProcessCommand BuildCommand(string repositoryRoot) =>
    new(
      "cargo",
      ["ndk", "-t", "x86_64", "-P", "24", "test", "--test", "e2e", "--all-features", "--no-run"],
      repositoryRoot);

  /// <summary>
  /// Builds the Android <c>e2e</c> binary once per run. <c>cargo ndk</c> does not forward cargo's
  /// JSON messages, so the binary is the newest <c>e2e-*</c> executable it left in the target
  /// directory.
  /// </summary>
  /// <param name="repositoryRoot">The repository root.</param>
  /// <param name="cancellationToken">Cancels the build.</param>
  /// <returns>The executable path on the host.</returns>
  public static async Task<string> BuildAsync(string repositoryRoot, CancellationToken cancellationToken)
  {
    ProcessCommand build = BuildCommand(repositoryRoot);
    ProcessResult result = await ProcessRunner.RunAsync(build, TimeSpan.FromMinutes(30), cancellationToken);
    if(!result.Succeeded)
    {
      throw new InvalidOperationException(
        $"{build.Display} failed with exit code {result.ExitCode}:\n{Redaction.SafeTail(result.StandardError)}");
    }

    string targetDirectory = Environment.GetEnvironmentVariable("CARGO_TARGET_DIR")
                             ?? Path.Combine(repositoryRoot, "target");
    return NewestTestExecutable(Path.Combine(targetDirectory, RustTarget, "debug", "deps"));
  }

  /// <summary>Finds the newest <c>e2e-&lt;hash&gt;</c> executable (no extension) in a deps directory.</summary>
  /// <param name="depsDirectory">The <c>target/&lt;triple&gt;/debug/deps</c> directory.</param>
  /// <returns>The executable path.</returns>
  public static string NewestTestExecutable(string depsDirectory) =>
    (Directory.Exists(depsDirectory)
      ? new DirectoryInfo(depsDirectory).EnumerateFiles("e2e-*")
                                        .Where(file => file.Extension.Length == 0)
                                        .OrderByDescending(file => file.LastWriteTimeUtc)
                                        .FirstOrDefault()
      : null)?.FullName
    ?? throw new InvalidOperationException($"cargo ndk produced no e2e test executable in {depsDirectory}.");

  /// <summary>Gets the <c>adb</c> executable from the Android SDK, or <c>adb</c> on the path.</summary>
  /// <returns>The executable.</returns>
  public static string Adb()
  {
    string? sdk = Environment.GetEnvironmentVariable("ANDROID_HOME")
                  ?? Environment.GetEnvironmentVariable("ANDROID_SDK_ROOT");
    if(sdk is not null)
    {
      string adb = Path.Combine(sdk, "platform-tools", OperatingSystem.IsWindows() ? "adb.exe" : "adb");
      if(File.Exists(adb))
      {
        return adb;
      }
    }

    return "adb";
  }

  /// <summary>Picks the device serial from <c>adb devices</c> output.</summary>
  /// <param name="configured">The configured serial, if any.</param>
  /// <param name="devices">The <c>adb devices</c> output.</param>
  /// <returns>The serial.</returns>
  /// <exception cref="InvalidOperationException">No single running emulator is available.</exception>
  public static string SelectSerial(string? configured, string devices)
  {
    if(!string.IsNullOrWhiteSpace(configured))
    {
      return configured.Trim();
    }

    string[] emulators = devices
                         .Split('\n', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries)
                         .Select(line => line.Split('\t', StringSplitOptions.TrimEntries))
                         .Where(parts => parts.Length == 2
                                         && parts[1] == "device"
                                         && parts[0].StartsWith("emulator-", StringComparison.Ordinal))
                         .Select(parts => parts[0])
                         .ToArray();
    return emulators.Length == 1
      ? emulators[0]
      : throw new InvalidOperationException(
        $"The android target needs exactly one running emulator (found {emulators.Length}). Start an x86_64 AVD "
        + $"(for example `emulator -avd Pixel_9_Pro_XL`) or set {SerialVariable} to the device serial.");
  }

  /// <summary>Builds the shell command that runs one cell on the device.</summary>
  /// <param name="test">The exact libtest name.</param>
  /// <param name="variables">The device-side environment.</param>
  /// <returns>The <c>adb shell</c> command line.</returns>
  public static string ShellCommand(string test, IReadOnlyDictionary<string, string> variables)
  {
    StringBuilder command = new($"cd {DeviceDirectory} && env");
    foreach((string key, string value) in variables.OrderBy(pair => pair.Key, StringComparer.Ordinal))
    {
      command.Append(' ').Append(key).Append("='").Append(value.Replace("'", "'\\''", StringComparison.Ordinal)).Append('\'');
    }

    command.Append(" ./e2e ").Append(test).Append(" --exact --test-threads=1");
    return command.ToString();
  }

  /// <inheritdoc/>
  public async Task<CellOutcome> RunAsync(E2ECell cell, AspireEnvironment environment, CancellationToken cancellationToken)
  {
    string serial = await PrepareAsync(environment.RepositoryRoot, cancellationToken);
    Dictionary<string, string> variables =
      DeviceEndpoints.Rewrite(CellExecution.ScenarioVariables(environment, cell), HostAlias);
    ProcessCommand command = new(
      Adb(),
      ["-s", serial, "shell", ShellCommand(cell.Scenario.RustTest(cell.Protocol), variables)],
      environment.RepositoryRoot);
    ProcessResult result = await ProcessRunner.RunAsync(command, CellExecution.CellTimeout, cancellationToken);
    if(result.TimedOut)
    {
      // The local adb process is gone; stop the device-side process too.
      await Device(serial, ["shell", "pkill -f rayx-signalr-e2e/e2e"], TimeSpan.FromSeconds(15));
    }

    CellOutcome outcome = CellExecution.JudgeLibtest(command, result);
    if(outcome.Passed)
    {
      return outcome;
    }

    ProcessResult logcat = await Device(serial, ["logcat", "-d", "-t", "200"], TimeSpan.FromSeconds(30));
    return outcome with
    {
      Failure = $"{outcome.Failure}\nlogcat (last 200 lines):\n{Redaction.SafeTail(logcat.StandardOutput)}"
    };
  }

  /// <summary>Removes the pushed binary from the device.</summary>
  /// <returns>A task that completes when the device is clean.</returns>
  public static async Task CleanupAsync()
  {
    if(_serial is null || _pushed is null)
    {
      return;
    }

    await Device(_serial, ["shell", $"rm -rf {DeviceDirectory}"], TimeSpan.FromSeconds(30));
    _pushed = null;
  }

  static async Task<string> PrepareAsync(string repositoryRoot, CancellationToken cancellationToken)
  {
    await _gate.WaitAsync(cancellationToken);
    try
    {
      if(_serial is null)
      {
        ProcessResult devices = await Device(null, ["devices"], TimeSpan.FromSeconds(30));
        _serial = SelectSerial(Environment.GetEnvironmentVariable(SerialVariable), devices.StandardOutput);
      }

      _executable ??= await BuildAsync(repositoryRoot, cancellationToken);
      string executable = _executable;
      if(_pushed != executable)
      {
        await Require(Device(_serial, ["shell", $"rm -rf {DeviceDirectory} && mkdir -p {DeviceDirectory}"], TimeSpan.FromSeconds(30)));
        await Require(Device(_serial, ["push", executable, $"{DeviceDirectory}/e2e"], TimeSpan.FromMinutes(5)));
        await Require(Device(_serial, ["shell", $"chmod 755 {DeviceDirectory}/e2e"], TimeSpan.FromSeconds(30)));
        _pushed = executable;
      }

      return _serial;
    }
    finally
    {
      _gate.Release();
    }
  }

  static Task<ProcessResult> Device(string? serial, IReadOnlyList<string> arguments, TimeSpan timeout)
  {
    List<string> all = serial is null ? [] : ["-s", serial];
    all.AddRange(arguments);
    return ProcessRunner.RunAsync(new ProcessCommand(Adb(), all, Directory.GetCurrentDirectory()), timeout);
  }

  static async Task Require(Task<ProcessResult> step)
  {
    ProcessResult result = await step;
    if(!result.Succeeded)
    {
      throw new InvalidOperationException(
        $"adb failed with exit code {result.ExitCode}:\n{Redaction.SafeTail(result.CombinedOutput)}");
    }
  }
}

/// <summary>
/// Runs native cells on an iOS simulator (macOS only): the <c>e2e</c> binary is built for
/// <c>aarch64-apple-ios-sim</c> and spawned per cell with <c>xcrun simctl spawn</c>, passing the
/// scenario variables as <c>SIMCTL_CHILD_*</c>; the simulator shares the host loopback.
/// </summary>
public sealed class IosCellRunner : ICellRunner
{
  /// <summary>Selects the simulator by name or UDID; defaults to the newest available iPhone.</summary>
  public const string DeviceVariable = "RAYX_SIGNALR_E2E_IOS_DEVICE";

  /// <summary>The simulator Rust target.</summary>
  public const string RustTarget = "aarch64-apple-ios-sim";

  static readonly SemaphoreSlim _gate = new(1, 1);
  static string? _device;
  static bool _booted;

  /// <summary>The cargo command that builds the simulator <c>e2e</c> binary.</summary>
  /// <param name="repositoryRoot">The repository root.</param>
  /// <returns>The command.</returns>
  public static ProcessCommand BuildCommand(string repositoryRoot) =>
    new(
      "cargo",
      ["test", "--test", "e2e", "--all-features", "--target", RustTarget, "--no-run", "--message-format=json"],
      repositoryRoot);

  /// <summary>Picks the simulator from <c>xcrun simctl list devices available --json</c>.</summary>
  /// <param name="configured">A configured name or UDID, if any.</param>
  /// <param name="listing">The JSON listing.</param>
  /// <returns>The UDID and whether it is booted.</returns>
  /// <exception cref="InvalidOperationException">No matching simulator exists.</exception>
  public static (string Udid, bool Booted) SelectDevice(string? configured, string listing)
  {
    using JsonDocument document = JsonDocument.Parse(listing);
    List<(Version Runtime, string Name, string Udid, bool Booted)> devices = [];
    foreach(JsonProperty runtime in document.RootElement.GetProperty("devices").EnumerateObject())
    {
      // com.apple.CoreSimulator.SimRuntime.iOS-18-2
      string[] marker = runtime.Name.Split(".SimRuntime.iOS-");
      if(marker.Length != 2 || !Version.TryParse(marker[1].Replace('-', '.'), out Version? version))
      {
        continue;
      }

      foreach(JsonElement device in runtime.Value.EnumerateArray())
      {
        if(device.TryGetProperty("isAvailable", out JsonElement available) && !available.GetBoolean())
        {
          continue;
        }

        devices.Add((
          version,
          device.GetProperty("name").GetString() ?? string.Empty,
          device.GetProperty("udid").GetString() ?? string.Empty,
          device.GetProperty("state").GetString() == "Booted"));
      }
    }

    var match = string.IsNullOrWhiteSpace(configured)
      ? devices.Where(device => device.Name.StartsWith("iPhone", StringComparison.Ordinal))
               .OrderBy(device => device.Runtime)
               .ThenBy(device => device.Booted)
               .LastOrDefault()
      : devices.LastOrDefault(device => device.Udid == configured.Trim() || device.Name == configured.Trim());
    return match.Udid is { Length: > 0 }
      ? (match.Udid, match.Booted)
      : throw new InvalidOperationException(
        string.IsNullOrWhiteSpace(configured)
          ? "No available iPhone simulator was found; install an iOS simulator runtime in Xcode."
          : $"No available simulator matches {DeviceVariable}='{configured}'.");
  }

  /// <inheritdoc/>
  public async Task<CellOutcome> RunAsync(E2ECell cell, AspireEnvironment environment, CancellationToken cancellationToken)
  {
    if(!OperatingSystem.IsMacOS())
    {
      throw new PlatformNotSupportedException("The ios target runs only on macOS with Xcode and an iOS simulator runtime.");
    }

    string device = await PrepareAsync(environment.RepositoryRoot, cancellationToken);
    string executable = await E2EBinaryBuilds.BuildAsync(BuildCommand(environment.RepositoryRoot), cancellationToken);
    Dictionary<string, string> variables = CellExecution.ScenarioVariables(environment, cell)
                                                        .ToDictionary(
                                                          pair => $"SIMCTL_CHILD_{pair.Key}",
                                                          pair => pair.Value,
                                                          StringComparer.Ordinal);
    ProcessCommand command = new(
      "xcrun",
      ["simctl", "spawn", device, executable, cell.Scenario.RustTest(cell.Protocol), "--exact", "--test-threads=1"],
      environment.RepositoryRoot)
    {
      Environment = variables
    };
    ProcessResult result = await ProcessRunner.RunAsync(command, CellExecution.CellTimeout, cancellationToken);
    if(result.TimedOut)
    {
      // Simulator processes are host processes; stop the scenario binary if it survived.
      await ProcessRunner.RunAsync(
        new ProcessCommand("pkill", ["-f", executable], environment.RepositoryRoot),
        TimeSpan.FromSeconds(15),
        CancellationToken.None);
    }

    return CellExecution.JudgeLibtest(command, result);
  }

  /// <summary>Shuts down a simulator this run booted.</summary>
  /// <returns>A task that completes when the simulator is shut down.</returns>
  public static async Task CleanupAsync()
  {
    if(_device is null || !_booted)
    {
      return;
    }

    await ProcessRunner.RunAsync(
      new ProcessCommand("xcrun", ["simctl", "shutdown", _device], Directory.GetCurrentDirectory()),
      TimeSpan.FromMinutes(1));
    _booted = false;
  }

  static async Task<string> PrepareAsync(string repositoryRoot, CancellationToken cancellationToken)
  {
    await _gate.WaitAsync(cancellationToken);
    try
    {
      if(_device is not null)
      {
        return _device;
      }

      ProcessResult listing = await ProcessRunner.RunAsync(
        new ProcessCommand("xcrun", ["simctl", "list", "devices", "available", "--json"], repositoryRoot),
        TimeSpan.FromMinutes(1),
        cancellationToken);
      if(!listing.Succeeded)
      {
        throw new InvalidOperationException($"xcrun simctl list failed:\n{Redaction.SafeTail(listing.CombinedOutput)}");
      }

      (string udid, bool booted) = SelectDevice(Environment.GetEnvironmentVariable(DeviceVariable), listing.StandardOutput);
      if(!booted)
      {
        ProcessResult boot = await ProcessRunner.RunAsync(
          new ProcessCommand("xcrun", ["simctl", "boot", udid], repositoryRoot),
          TimeSpan.FromMinutes(3),
          cancellationToken);
        if(!boot.Succeeded)
        {
          throw new InvalidOperationException($"xcrun simctl boot failed:\n{Redaction.SafeTail(boot.CombinedOutput)}");
        }

        _booted = true;
      }

      ProcessResult ready = await ProcessRunner.RunAsync(
        new ProcessCommand("xcrun", ["simctl", "bootstatus", udid, "-b"], repositoryRoot),
        TimeSpan.FromMinutes(5),
        cancellationToken);
      if(!ready.Succeeded)
      {
        throw new InvalidOperationException($"The simulator did not finish booting:\n{Redaction.SafeTail(ready.CombinedOutput)}");
      }

      _device = udid;
      return udid;
    }
    finally
    {
      _gate.Release();
    }
  }
}
