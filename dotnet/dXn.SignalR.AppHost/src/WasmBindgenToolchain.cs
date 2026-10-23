using System.Diagnostics;
using System.Text.Json;

namespace dXn.SignalR.AppHost;

/// <summary>
/// Resolves and locates the exact <c>wasm-bindgen</c> CLI required by the browser fixture.
/// </summary>
/// <remarks>
/// The CLI version must match the fixture's resolved <c>wasm-bindgen</c> crate exactly, and other
/// RayX repositories pin different versions, so the CLI is installed per version under the
/// repository's ignored <c>artifacts-temp/tools</c> directory instead of using the machine-wide tool.
/// </remarks>
public static class WasmBindgenToolchain
{
  /// <summary>The fixture manifest whose resolved <c>wasm-bindgen</c> version selects the CLI.</summary>
  public const string FixtureManifest = "tests/wasm-fixture/Cargo.toml";

  /// <summary>Extracts the resolved <c>wasm-bindgen</c> crate version from <c>cargo metadata</c> output.</summary>
  /// <param name="cargoMetadataJson">The JSON printed by <c>cargo metadata --format-version 1</c>.</param>
  /// <returns>The single resolved version.</returns>
  public static string ParseResolvedVersion(string cargoMetadataJson)
  {
    using JsonDocument document = JsonDocument.Parse(cargoMetadataJson);
    string[] versions = document.RootElement
                                .GetProperty("packages")
                                .EnumerateArray()
                                .Where(package => package.GetProperty("name").GetString() == "wasm-bindgen")
                                .Select(package => package.GetProperty("version").GetString() ?? string.Empty)
                                .Distinct(StringComparer.Ordinal)
                                .ToArray();
    return versions.Length switch
    {
      1 when versions[0].Length > 0 => versions[0],
      0 => throw new InvalidOperationException(
        "The browser fixture's dependency graph does not contain wasm-bindgen."),
      _ => throw new InvalidOperationException(
        $"The browser fixture resolves several wasm-bindgen versions ({string.Join(", ", versions)}); pin exactly one.")
    };
  }

  /// <summary>Resolves the fixture's <c>wasm-bindgen</c> version by running <c>cargo metadata</c>.</summary>
  /// <param name="repositoryRoot">The repository root.</param>
  /// <returns>The resolved version.</returns>
  public static string ResolveFixtureVersion(string repositoryRoot)
  {
    ProcessStartInfo startInfo = new("cargo")
    {
      WorkingDirectory = repositoryRoot,
      RedirectStandardOutput = true,
      RedirectStandardError = true,
      UseShellExecute = false,
      CreateNoWindow = true
    };
    foreach(string argument in new[]
            {
              "metadata", "--format-version", "1", "--manifest-path", FixtureManifest
            })
    {
      startInfo.ArgumentList.Add(argument);
    }

    using Process process = Process.Start(startInfo)
                            ?? throw new InvalidOperationException("Failed to start cargo metadata.");
    Task<string> error = process.StandardError.ReadToEndAsync();
    string output = process.StandardOutput.ReadToEnd();
    process.WaitForExit();
    if(process.ExitCode != 0)
    {
      throw new InvalidOperationException(
        $"cargo metadata for {FixtureManifest} failed with exit code {process.ExitCode}: {error.Result}");
    }

    return ParseResolvedVersion(output);
  }

  /// <summary>Gets the per-version installation root for the CLI.</summary>
  /// <param name="repositoryRoot">The repository root.</param>
  /// <param name="version">The exact CLI version.</param>
  /// <returns>The directory passed to <c>cargo install --root</c>.</returns>
  public static string ToolRoot(string repositoryRoot, string version) =>
    Path.Combine(repositoryRoot, "artifacts-temp", "tools", "wasm-bindgen", version);

  /// <summary>Gets the CLI executable inside a per-version installation root.</summary>
  /// <param name="toolRoot">The installation root.</param>
  /// <returns>The executable path.</returns>
  public static string ExecutablePath(string toolRoot) =>
    Path.Combine(toolRoot, "bin", OperatingSystem.IsWindows() ? "wasm-bindgen.exe" : "wasm-bindgen");

  /// <summary>Builds the <c>cargo install</c> arguments that provision one exact CLI version.</summary>
  /// <param name="version">The exact CLI version.</param>
  /// <param name="toolRoot">The installation root.</param>
  /// <returns>The arguments after <c>cargo</c>.</returns>
  public static string[] InstallArguments(string version, string toolRoot) =>
  [
    "install", "wasm-bindgen-cli", "--version", version, "--locked", "--root", toolRoot
  ];
}
