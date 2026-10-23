using System.Text;
using System.Text.RegularExpressions;
using dXn.SignalR.E2E.Tests.Infrastructure;

namespace dXn.SignalR.E2E.Tests.Scenarios;

/// <summary>
/// Runs cells inside the test-only Android app (<c>tests/android-app</c>): the
/// <c>tests/android-harness</c> JNI library is built with <c>cargo ndk</c>, the app is assembled
/// with Gradle (debug, or a minified release APK for <c>android-app-release</c>), installed on the
/// emulator and made device owner, and each cell runs one shared scenario through
/// <c>am instrument</c>. The platform verifier is initialized from the app's <c>Context</c> and
/// verifies through the Android trust store (<c>AndroidCAStore</c>), as in a consumer's app; the
/// instrumentation installs the run's test CA there as a user CA for each scenario and removes it
/// (device policy), because the verifier does not read the app's network security config.
/// </summary>
/// <param name="release">Whether this runner builds the minified release APK.</param>
public sealed partial class AndroidAppCellRunner(bool release) : ICellRunner
{
  /// <summary>The app's package and instrumentation.</summary>
  public const string Package = "dev.dxn.signalr.e2e";

  /// <summary>The instrumentation component <c>am instrument</c> runs.</summary>
  public const string Instrumentation = Package + "/.ScenarioInstrumentation";

  /// <summary>The test-only device admin that installs the test CA as device owner.</summary>
  public const string DeviceAdmin = Package + "/.TestDeviceAdmin";

  /// <summary>The Gradle project, relative to the repository root.</summary>
  public const string ProjectDirectory = "tests/android-app";

  /// <summary>The harness crate's manifest, relative to the repository root.</summary>
  public const string HarnessManifest = "tests/android-harness/Cargo.toml";

  /// <summary>The harness library's file name.</summary>
  public const string HarnessLibrary = "libdxn_signalr_android_harness.so";

  /// <summary>The scenario variable that makes the <c>tls</c> scenario trust the platform.</summary>
  public const string PlatformTrustVariable = "DXN_SIGNALR_E2E_TLS_PLATFORM_TRUST";

  /// <summary>The variable that lets the runner change a device that is not an emulator.</summary>
  public const string AllowSystemChangesVariable = "DXN_SIGNALR_E2E_ALLOW_SYSTEM_CHANGES";

  /// <summary>
  /// The scenario variables the app's scenarios read. Only these reach the instrumentation
  /// argument, which a failing cell's diagnostics show: no credential travels base64-encoded
  /// where redaction cannot see it.
  /// </summary>
  public static readonly IReadOnlyList<string> AppVariables =
  [
    "DXN_SIGNALR_E2E_SERVICE_URL", "DXN_SIGNALR_E2E_TLS_URL", "DXN_SIGNALR_E2E_TLS_CA_PEM_BASE64",
    "DXN_SIGNALR_E2E_UNTRUSTED_TLS_URL", "DXN_SIGNALR_E2E_PROTOCOL", CellExecution.TransportVariable, "RUST_LOG",
    PlatformTrustVariable, "DXN_SIGNALR_E2E_PROXY_URL"
  ];

  /// <summary>Keeps the variables the app scenarios read (<see cref="AppVariables"/>).</summary>
  /// <param name="variables">Every scenario variable of the cell.</param>
  /// <returns>The app's variables.</returns>
  public static Dictionary<string, string> ForApp(IReadOnlyDictionary<string, string> variables) =>
    variables.Where(pair => AppVariables.Contains(pair.Key, StringComparer.Ordinal))
             .ToDictionary(pair => pair.Key, pair => pair.Value, StringComparer.Ordinal);

  /// <summary>
  /// Whether the runner may make the app device owner and install a CA on the device: only an
  /// emulator (<c>ro.kernel.qemu=1</c> or an <c>emulator-</c> serial), unless
  /// <c>DXN_SIGNALR_E2E_ALLOW_SYSTEM_CHANGES=1</c> allows another device.
  /// </summary>
  /// <param name="serial">The device serial.</param>
  /// <param name="qemu">The device's <c>ro.kernel.qemu</c> property.</param>
  /// <param name="allowSystemChanges">The <c>DXN_SIGNALR_E2E_ALLOW_SYSTEM_CHANGES</c> value.</param>
  /// <returns>Whether the device may be changed.</returns>
  public static bool MayChangeDevice(string serial, string qemu, string? allowSystemChanges) =>
    qemu.Trim() == "1" || serial.StartsWith("emulator-", StringComparison.Ordinal) || allowSystemChanges == "1";

  static readonly SemaphoreSlim _gate = new(1, 1);
  static readonly Dictionary<bool, string> _installed = [];
  static string? _serial;

  /// <summary>Gets the Gradle task that assembles the variant.</summary>
  /// <param name="release">Whether the variant is the release build.</param>
  /// <returns>The task path.</returns>
  public static string AssembleTask(bool release) => release ? ":app:assembleRelease" : ":app:assembleDebug";

  /// <summary>Gets the APK a variant produces, relative to the repository root.</summary>
  /// <param name="release">Whether the variant is the release build.</param>
  /// <returns>The APK path.</returns>
  public static string ApkPath(bool release) =>
    Path.Combine(ProjectDirectory, "app", "build", "outputs", "apk", release ? "release" : "debug", release ? "app-release.apk" : "app-debug.apk");

  /// <summary>The cargo command that builds the harness library for the emulator.</summary>
  /// <param name="repositoryRoot">The repository root.</param>
  /// <param name="provider">The TLS provider (<see cref="TlsProvider"/>).</param>
  /// <returns>The command.</returns>
  public static ProcessCommand HarnessBuildCommand(string repositoryRoot, string provider) =>
    new(
      "cargo",
      [
        "ndk", "-t", "x86_64", "-P", "24", "build", "--manifest-path", HarnessManifest,
        .. provider == TlsProvider.Ring ? Array.Empty<string>() : ["--no-default-features", "--features", "tls-aws-lc-rs"]
      ],
      repositoryRoot)
    {
      Environment = new Dictionary<string, string>(StringComparer.Ordinal)
      {
        ["CARGO_TARGET_DIR"] = Path.Combine(repositoryRoot, TlsProvider.TargetDirectory("target/android-harness", provider))
      }
    };

  /// <summary>The Gradle command that assembles a variant with the built harness library.</summary>
  /// <param name="repositoryRoot">The repository root.</param>
  /// <param name="release">Whether to assemble the release APK.</param>
  /// <param name="jniLibs">The directory holding <c>x86_64/</c><see cref="HarnessLibrary"/>.</param>
  /// <returns>The command.</returns>
  public static ProcessCommand GradleCommand(string repositoryRoot, bool release, string jniLibs) =>
    new(
      Path.Combine(repositoryRoot, ProjectDirectory, OperatingSystem.IsWindows() ? "gradlew.bat" : "gradlew"),
      ["--no-daemon", "--console=plain", $"-Pdxn.jniLibs={jniLibs}", AssembleTask(release)],
      Path.Combine(repositoryRoot, ProjectDirectory));

  /// <summary>
  /// The <c>adb shell</c> commands that make the app device owner (so it can install the test CA)
  /// and that remove it again after the run.
  /// </summary>
  /// <returns>The set-up and clean-up commands.</returns>
  public static (string[] SetUp, string[] CleanUp) DeviceOwnerCommands() =>
    (["shell", "dpm", "set-device-owner", DeviceAdmin], ["shell", "dpm", "remove-active-admin", DeviceAdmin]);

  /// <summary>
  /// The <c>adb shell</c> arguments that run one scenario: the scenario variables travel as base64
  /// of <c>KEY=VALUE</c> lines, so no value needs shell quoting.
  /// </summary>
  /// <param name="scenario">The scenario name.</param>
  /// <param name="protocol">The protocol.</param>
  /// <param name="variables">The scenario variables.</param>
  /// <returns>The arguments after <c>adb -s &lt;serial&gt;</c>.</returns>
  public static string[] InstrumentArguments(string scenario, string protocol, IReadOnlyDictionary<string, string> variables)
  {
    string lines = string.Join('\n', variables.OrderBy(pair => pair.Key, StringComparer.Ordinal).Select(pair => $"{pair.Key}={pair.Value}"));
    return
    [
      "shell", "am", "instrument", "-w",
      "-e", "scenario", scenario,
      "-e", "protocol", protocol,
      "-e", "environment", Convert.ToBase64String(Encoding.UTF8.GetBytes(lines)),
      Instrumentation
    ];
  }

  /// <summary>Reads the instrumentation's outcome: <c>ok</c>, the scenario's failure, or a crash.</summary>
  /// <param name="output">The <c>am instrument -w</c> output.</param>
  /// <returns>Whether it passed, and the failure otherwise.</returns>
  public static (bool Passed, string Failure) ParseInstrumentation(string output)
  {
    Match result = ResultLine().Match(output);
    Match code = CodeLine().Match(output);
    if(result.Success && result.Groups[1].Value.Trim() == "ok" && code.Success && code.Groups[1].Value == "-1")
    {
      return (true, string.Empty);
    }

    return result.Success
      ? (false, $"the scenario failed: {result.Groups[1].Value.Trim()}")
      : (false, $"the instrumentation did not report a result (crashed or was killed): {Redaction.SafeTail(output)}");
  }

  /// <summary>Removes the device owner and the app from the device after the run.</summary>
  /// <returns>A task that completes when the device is clean.</returns>
  public static async Task CleanupAsync()
  {
    if(_serial is null || _installed.Count == 0)
    {
      return;
    }

    string adb = AndroidCellRunner.Adb();
    string root = Directory.GetCurrentDirectory();
    await RemoveTestCasAsync(_serial, root);
    await ProcessRunner.RunAsync(new ProcessCommand(adb, ["-s", _serial, .. DeviceOwnerCommands().CleanUp], root), TimeSpan.FromSeconds(30));
    await ProcessRunner.RunAsync(new ProcessCommand(adb, ["-s", _serial, "uninstall", Package], root), TimeSpan.FromSeconds(60));
    _installed.Clear();
  }

  /// <summary>
  /// Recognizes a Gradle failure that comes from the machine rather than the app: no JDK, no
  /// Android SDK, or dependencies that cannot be downloaded.
  /// </summary>
  /// <param name="output">The Gradle output.</param>
  /// <returns>The prerequisite message, or <see langword="null"/>.</returns>
  public static string? MissingToolchain(string output)
  {
    string[] markers =
    [
      "JAVA_HOME is not set", "JAVA_HOME is set to an invalid directory", "SDK location not found",
      "Could not GET", "Could not HEAD", "UnknownHostException", "No route to host", "Unable to locate a Java Runtime",
      "No Android SDK found", "Failed to find target with hash string", "Could not install Gradle distribution"
    ];
    string? marker = markers.FirstOrDefault(candidate => output.Contains(candidate, StringComparison.OrdinalIgnoreCase));
    return marker is null
      ? null
      : $"environment error: the Android app build needs a toolchain this machine lacks ('{marker}'). Prerequisites: "
        + "JDK 17 or later (JAVA_HOME), the Android SDK (ANDROID_HOME) with platform 36, and network access for the "
        + "Gradle distribution, the Android Gradle plugin and the rustls-platform-verifier Maven archive on the first build.";
  }

  /// <inheritdoc/>
  public async Task<CellOutcome> RunAsync(E2ECell cell, AspireEnvironment environment, CancellationToken cancellationToken)
  {
    string serial = await PrepareAsync(environment, cancellationToken);
    Dictionary<string, string> variables =
      ForApp(DeviceEndpoints.Rewrite(CellExecution.ScenarioVariables(environment, cell), AndroidCellRunner.HostAlias));
    variables[PlatformTrustVariable] = "1";
    ProcessCommand command = new(
      AndroidCellRunner.Adb(),
      ["-s", serial, .. InstrumentArguments(cell.Scenario.Name, cell.Protocol, variables)],
      environment.RepositoryRoot);
    ProcessResult result;
    if(cell.Scenario.SystemProxy)
    {
      // The emulator's global HTTP proxy points at the test proxy's credential-free listener for
      // the cell, and is restored to its previous value whatever the outcome.
      int port = await SystemProxy.OpenPortAsync(environment.ProxyUrl, cancellationToken);
      ProcessResult previous = await ProcessRunner.RunAsync(
        new ProcessCommand(AndroidCellRunner.Adb(), ["-s", serial, .. SystemProxy.AndroidGetArguments], environment.RepositoryRoot),
        TimeSpan.FromSeconds(30),
        cancellationToken);
      if(!previous.Succeeded)
      {
        throw new InvalidOperationException($"cannot read the emulator's proxy setting: {Redaction.SafeTail(previous.StandardError)}");
      }

      try
      {
        await ProcessRunner.RunAsync(
          new ProcessCommand(AndroidCellRunner.Adb(), ["-s", serial, .. SystemProxy.AndroidSetArguments(AndroidCellRunner.HostAlias, port)], environment.RepositoryRoot),
          TimeSpan.FromSeconds(30),
          cancellationToken);
        result = await ProcessRunner.RunAsync(command, cell.Scenario.Bound, cancellationToken);
      }
      finally
      {
        await ProcessRunner.RunAsync(
          new ProcessCommand(AndroidCellRunner.Adb(), ["-s", serial, .. SystemProxy.AndroidRestoreArguments(previous.StandardOutput)], environment.RepositoryRoot),
          TimeSpan.FromSeconds(30),
          CancellationToken.None);
      }
    }
    else
    {
      result = await ProcessRunner.RunAsync(command, cell.Scenario.Bound, cancellationToken);
    }
    if(result.TimedOut)
    {
      // The instrumentation could not remove the test CA it installed.
      await ProcessRunner.RunAsync(
        new ProcessCommand(AndroidCellRunner.Adb(), ["-s", serial, "shell", "am", "force-stop", Package], environment.RepositoryRoot),
        TimeSpan.FromSeconds(30),
        CancellationToken.None);
      await RemoveTestCasAsync(serial, environment.RepositoryRoot);
    }

    (bool passed, string failure) = result.TimedOut
      ? (false, $"timed out after {cell.Scenario.Bound}")
      : ParseInstrumentation(result.StandardOutput);
    if(passed)
    {
      return new CellOutcome(command, result, true, string.Empty);
    }

    // The client's log (the harness logs to logcat under its tag) and crashes.
    ProcessResult logcat = await ProcessRunner.RunAsync(
      new ProcessCommand(AndroidCellRunner.Adb(), ["-s", serial, .. LogcatArguments], environment.RepositoryRoot),
      TimeSpan.FromSeconds(30),
      CancellationToken.None);
    return new CellOutcome(
      command,
      result,
      false,
      $"{failure}\nlogcat ({LogTag} and crashes):\n{Redaction.SafeTail(logcat.StandardOutput)}");
  }

  /// <summary>The logcat tag the harness logs the client and the scenario under.</summary>
  public const string LogTag = "dxn-signalr";

  /// <summary>The <c>adb</c> arguments that read the harness log and crashes from logcat.</summary>
  public static readonly string[] LogcatArguments =
    ["logcat", "-d", "-t", "1000", "-v", "time", $"{LogTag}:V", "AndroidRuntime:E", "DEBUG:V", "*:S"];

  /// <summary>
  /// Removes every user CA the test admin installed (a cell that was killed could not), through
  /// the instrumentation's clean-up mode.
  /// </summary>
  static Task<ProcessResult> RemoveTestCasAsync(string serial, string root) =>
    ProcessRunner.RunAsync(
      new ProcessCommand(AndroidCellRunner.Adb(), ["-s", serial, .. CleanupArguments], root),
      TimeSpan.FromSeconds(60),
      CancellationToken.None);

  /// <summary>The <c>adb</c> arguments of the instrumentation's clean-up mode.</summary>
  public static readonly string[] CleanupArguments =
    ["shell", "am", "instrument", "-w", "-e", "cleanup", "true", Instrumentation];

  /// <summary>Builds and installs the variant once per run and test CA.</summary>
  async Task<string> PrepareAsync(AspireEnvironment environment, CancellationToken cancellationToken)
  {
    await _gate.WaitAsync(cancellationToken);
    try
    {
      string root = environment.RepositoryRoot;
      if(_serial is null)
      {
        ProcessResult devices = await ProcessRunner.RunAsync(
          new ProcessCommand(AndroidCellRunner.Adb(), ["devices"], root),
          TimeSpan.FromSeconds(30),
          cancellationToken);
        string serial = AndroidCellRunner.SelectSerial(
          Environment.GetEnvironmentVariable(AndroidCellRunner.SerialVariable),
          devices.StandardOutput);
        ProcessResult qemu = await ProcessRunner.RunAsync(
          new ProcessCommand(AndroidCellRunner.Adb(), ["-s", serial, "shell", "getprop", "ro.kernel.qemu"], root),
          TimeSpan.FromSeconds(30),
          cancellationToken);
        if(!MayChangeDevice(serial, qemu.StandardOutput, Environment.GetEnvironmentVariable(AllowSystemChangesVariable)))
        {
          throw new InvalidOperationException(
            $"environment error: the android-app targets make the test app device owner and install a CA, so they "
            + $"run only on an emulator; {serial} is not one (set {AllowSystemChangesVariable}=1 to allow it).");
        }

        _serial = serial;
      }

      string provider = TlsProvider.Current;
      if(_installed.TryGetValue(release, out string? installed) && installed == provider)
      {
        return _serial;
      }

      string jniLibs = Path.Combine(root, "artifacts-temp", "android-app", "jniLibs");

      ProcessCommand harness = HarnessBuildCommand(root, provider);
      await Require(harness, await ProcessRunner.RunAsync(harness, TimeSpan.FromMinutes(30), cancellationToken), TlsProvider.MissingToolchain);
      string library = Path.Combine(
        harness.Environment["CARGO_TARGET_DIR"], AndroidCellRunner.RustTarget, "debug", HarnessLibrary);
      Directory.CreateDirectory(Path.Combine(jniLibs, "x86_64"));
      File.Copy(library, Path.Combine(jniLibs, "x86_64", HarnessLibrary), overwrite: true);

      ProcessCommand gradle = GradleCommand(root, release, jniLibs);
      await Require(gradle, await ProcessRunner.RunAsync(gradle, TimeSpan.FromMinutes(30), cancellationToken), MissingToolchain);

      ProcessCommand install = new(AndroidCellRunner.Adb(), ["-s", _serial, "install", "-r", "-t", Path.Combine(root, ApkPath(release))], root);
      await Require(install, await ProcessRunner.RunAsync(install, TimeSpan.FromMinutes(5), cancellationToken), _ => null);
      ProcessResult owners = await ProcessRunner.RunAsync(
        new ProcessCommand(AndroidCellRunner.Adb(), ["-s", _serial, "shell", "dpm", "list-owners"], root),
        TimeSpan.FromSeconds(30),
        cancellationToken);
      if(!owners.StandardOutput.Contains(Package, StringComparison.Ordinal))
      {
        ProcessCommand owner = new(AndroidCellRunner.Adb(), ["-s", _serial, .. DeviceOwnerCommands().SetUp], root);
        ProcessResult owned = await ProcessRunner.RunAsync(owner, TimeSpan.FromSeconds(30), cancellationToken);
        if(!owned.Succeeded)
        {
          // The app never stays installed without the run that uses it.
          await ProcessRunner.RunAsync(
            new ProcessCommand(AndroidCellRunner.Adb(), ["-s", _serial, "uninstall", Package], root),
            TimeSpan.FromSeconds(60),
            CancellationToken.None);
        }

        await Require(owner, owned, DeviceOwnerUnavailable);
      }

      _installed.Clear();
      _installed[release] = provider;
      return _serial;
    }
    finally
    {
      _gate.Release();
    }
  }

  /// <summary>
  /// Recognizes an emulator that cannot take a device owner (an account or a second user exists),
  /// which is an environment error.
  /// </summary>
  /// <param name="output">The <c>dpm set-device-owner</c> output.</param>
  /// <returns>The prerequisite message, or <see langword="null"/>.</returns>
  public static string? DeviceOwnerUnavailable(string output) =>
    output.Contains("Not allowed to set the device owner", StringComparison.OrdinalIgnoreCase)
    || output.Contains("already", StringComparison.OrdinalIgnoreCase)
      ? "environment error: the android-app targets make the test app device owner on the emulator to install the "
        + "run's test CA as a user CA; use an emulator without accounts, other users or another device owner."
      : null;

  static Task Require(ProcessCommand command, ProcessResult result, Func<string, string?> environmentError)
  {
    if(result.Succeeded)
    {
      return Task.CompletedTask;
    }

    string failure = $"{command.Display} failed with exit code {result.ExitCode}:\n{Redaction.SafeTail(result.CombinedOutput)}";
    throw new InvalidOperationException(
      environmentError(result.CombinedOutput) is string environment ? $"{environment}\n{failure}" : failure);
  }

  [GeneratedRegex(@"(?ms)^INSTRUMENTATION_RESULT: result=(.*?)\r?\n(?=INSTRUMENTATION_)")]
  private static partial Regex ResultLine();

  [GeneratedRegex(@"(?m)^INSTRUMENTATION_CODE: (-?\d+)\r?$")]
  private static partial Regex CodeLine();
}
