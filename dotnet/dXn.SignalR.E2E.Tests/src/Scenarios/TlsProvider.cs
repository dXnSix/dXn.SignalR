namespace dXn.SignalR.E2E.Tests.Scenarios;

/// <summary>
/// The rustls provider the Rust <c>e2e</c> binaries are built with:
/// <c>DXN_SIGNALR_E2E_TLS_PROVIDER</c> is <c>ring</c> (the default, the crate's default features)
/// or <c>aws-lc-rs</c>. Each provider builds into its own target directory, so switching does not
/// rebuild the other.
/// </summary>
public static class TlsProvider
{
  /// <summary>The variable that selects the provider.</summary>
  public const string Variable = "DXN_SIGNALR_E2E_TLS_PROVIDER";

  /// <summary>The <c>ring</c> provider (the crate's default).</summary>
  public const string Ring = "ring";

  /// <summary>The <c>aws-lc-rs</c> provider.</summary>
  public const string AwsLcRs = "aws-lc-rs";

  /// <summary>Gets the provider this run selected.</summary>
  public static string Current => Parse(Environment.GetEnvironmentVariable(Variable));

  /// <summary>Parses a provider selection; empty means <see cref="Ring"/>.</summary>
  /// <param name="value">The selection.</param>
  /// <returns>The provider.</returns>
  /// <exception cref="ArgumentException">The provider is unknown.</exception>
  public static string Parse(string? value) =>
    string.IsNullOrWhiteSpace(value)
      ? Ring
      : value.Trim() switch
      {
        Ring => Ring,
        AwsLcRs => AwsLcRs,
        string other => throw new ArgumentException(
          $"Unknown {Variable} '{other}'; use '{Ring}' or '{AwsLcRs}'.", nameof(value))
      };

  /// <summary>
  /// The cargo feature arguments of the host and iOS <c>e2e</c> binaries: the default TLS stack
  /// plus MessagePack for <c>ring</c>; the platform verifier with <c>aws-lc-rs</c> and no default
  /// features otherwise, so <c>ring</c> is not built in.
  /// </summary>
  /// <param name="provider">The provider.</param>
  /// <returns>The arguments.</returns>
  public static string[] HostFeatures(string provider) =>
    provider == Ring
      ? CellExecution.E2EFeatures
      : ["--no-default-features", "--features", "messagepack,proxy-pac,http3,tls-platform-verifier,tls-aws-lc-rs"];

  /// <summary>
  /// The cargo feature arguments of the Android <c>e2e</c> binary: as the host's, plus the bundled
  /// roots the untrusted-certificate case needs without an app's JVM.
  /// </summary>
  /// <param name="provider">The provider.</param>
  /// <returns>The arguments.</returns>
  public static string[] AndroidFeatures(string provider) =>
    provider == Ring
      ? ["--features", "messagepack,proxy-pac,http3,tls-webpki-roots"]
      : ["--no-default-features", "--features", "messagepack,proxy-pac,http3,tls-platform-verifier,tls-aws-lc-rs,tls-webpki-roots"];

  /// <summary>
  /// The target directory of a provider's build: <paramref name="ringDirectory"/> for <c>ring</c>,
  /// the same path with an <c>-aws-lc</c> suffix (or <c>target/e2e-aws-lc</c> for the default
  /// <c>target</c>) for <c>aws-lc-rs</c>.
  /// </summary>
  /// <param name="ringDirectory">The <c>ring</c> build's directory, relative to the repository root.</param>
  /// <param name="provider">The provider.</param>
  /// <returns>The directory, relative to the repository root.</returns>
  public static string TargetDirectory(string ringDirectory, string provider) =>
    provider == Ring
      ? ringDirectory
      : ringDirectory == "target" ? "target/e2e-aws-lc" : $"{ringDirectory}-aws-lc";

  /// <summary>
  /// The build environment of a provider: none for <c>ring</c> (cargo's default target directory),
  /// else <c>CARGO_TARGET_DIR</c> set to the provider's directory.
  /// </summary>
  /// <param name="repositoryRoot">The repository root.</param>
  /// <param name="ringDirectory">The <c>ring</c> build's directory, relative to the repository root.</param>
  /// <param name="provider">The provider.</param>
  /// <returns>The variables.</returns>
  public static Dictionary<string, string> TargetEnvironment(string repositoryRoot, string ringDirectory, string provider) =>
    provider == Ring
      ? new Dictionary<string, string>(StringComparer.Ordinal)
      : new Dictionary<string, string>(StringComparer.Ordinal)
      {
        ["CARGO_TARGET_DIR"] = Path.Combine(repositoryRoot, TargetDirectory(ringDirectory, provider))
      };

  /// <summary>
  /// Recognizes a build that failed because the machine lacks a toolchain the provider needs
  /// (CMake, NASM, a C compiler or clang for <c>aws-lc-sys</c>), which is an environment error
  /// rather than a cell failure.
  /// </summary>
  /// <param name="output">The build's output.</param>
  /// <returns>The prerequisite message, or <see langword="null"/> when the failure is not a missing toolchain.</returns>
  public static string? MissingToolchain(string output)
  {
    string[] markers =
    [
      "Missing dependency: cmake", "is `cmake` not installed", "failed to execute command: program not found",
      "Missing dependency: nasm", "NASM command not found", "Unable to find libclang",
      "failed to find tool", "is `clang` installed", "could not find native static library `aws_lc"
    ];
    string? marker = markers.FirstOrDefault(candidate => output.Contains(candidate, StringComparison.OrdinalIgnoreCase));
    return marker is null
      ? null
      : $"environment error: the {AwsLcRs} build needs a toolchain this machine lacks ('{marker}'). Prerequisites: "
        + "a C compiler and CMake on the host (NASM is prebuilt on Windows x86_64); for Android, cargo ndk with an "
        + "Android NDK (its clang and CMake).";
  }
}
