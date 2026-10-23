using System.Text.Json;
using System.Text.RegularExpressions;
using dXn.SignalR.E2E.Tests.Infrastructure;

namespace dXn.SignalR.E2E.Tests.Contracts;

/// <summary>Runs Cargo queries for the static platform cases.</summary>
public static partial class Cargo
{
  /// <summary>Runs <c>cargo</c> and fails the test with redacted output when it fails.</summary>
  /// <param name="repositoryRoot">The repository root.</param>
  /// <param name="arguments">The cargo arguments.</param>
  /// <param name="environment">Extra environment variables.</param>
  /// <returns>Standard output.</returns>
  public static async Task<string> RunAsync(
    string repositoryRoot,
    IReadOnlyList<string> arguments,
    IReadOnlyDictionary<string, string>? environment = null)
  {
    ProcessResult result = await RunRawAsync(repositoryRoot, arguments, environment);
    Assert.True(
      result.Succeeded,
      $"cargo {string.Join(' ', arguments)} exited {result.ExitCode} (timed out: {result.TimedOut}):\n{Redaction.SafeTail(result.StandardError)}");
    return result.StandardOutput;
  }

  /// <summary>Runs <c>cargo</c> and returns the raw result.</summary>
  /// <param name="repositoryRoot">The repository root.</param>
  /// <param name="arguments">The cargo arguments.</param>
  /// <param name="environment">Extra environment variables.</param>
  /// <returns>The result.</returns>
  public static Task<ProcessResult> RunRawAsync(
    string repositoryRoot,
    IReadOnlyList<string> arguments,
    IReadOnlyDictionary<string, string>? environment = null) =>
    ProcessRunner.RunAsync(
      new ProcessCommand("cargo", arguments, repositoryRoot)
      {
        Environment = environment ?? new Dictionary<string, string>()
      },
      TimeSpan.FromMinutes(20));

  /// <summary>Lists the package names in the library's normal (non-dev) dependency graph.</summary>
  /// <param name="repositoryRoot">The repository root.</param>
  /// <param name="extraArguments">Target and feature arguments.</param>
  /// <returns>The package names.</returns>
  public static async Task<HashSet<string>> NormalGraphAsync(string repositoryRoot, params string[] extraArguments)
  {
    string output = await RunAsync(
                      repositoryRoot,
                      ["tree", "-e", "normal", "--prefix", "none", "--format", "{p}", .. extraArguments]);
    return PackageLine()
           .Matches(output)
           .Select(match => match.Groups["name"].Value)
           .ToHashSet(StringComparer.Ordinal);
  }

  /// <summary>Reads <c>cargo metadata --no-deps</c> for the root package.</summary>
  /// <param name="repositoryRoot">The repository root.</param>
  /// <param name="manifest">The manifest path relative to the root.</param>
  /// <returns>The package element (cloned).</returns>
  public static async Task<JsonElement> PackageAsync(string repositoryRoot, string manifest = "Cargo.toml")
  {
    string output = await RunAsync(
                      repositoryRoot,
                      ["metadata", "--no-deps", "--format-version", "1", "--manifest-path", manifest]);
    using JsonDocument document = JsonDocument.Parse(output);
    return document.RootElement.GetProperty("packages")[0].Clone();
  }

  [GeneratedRegex(@"(?m)^(?<name>[A-Za-z0-9_\-]+) v\d")]
  private static partial Regex PackageLine();
}
