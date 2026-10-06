using System.Text.Json;
using System.Text.RegularExpressions;
using RayX.SignalR.E2E.Tests.Infrastructure;

namespace RayX.SignalR.E2E.Tests.Contracts;

/// <summary>Proves the library's normal dependency graph carries no test, debug or dead crates.</summary>
[Collection(AspireTestGroup.Name)]
public sealed partial class ManifestHygieneTests(AspireEnvironment environment)
{
  static readonly string[] _devOnlyOrDead =
  [
    "test-log", "env_logger", "console_log", "console_error_panic_hook", "wasm-timer", "async-std"
  ];

  static readonly string[] _allowedTokioFeatures = ["rt", "net", "time", "sync", "macros"];

  static readonly string[] _prohibitedTokioFeatures =
    ["full", "fs", "process", "signal", "io-std", "parking_lot", "rt-multi-thread"];

  public static TheoryData<string, string> Graphs() => new()
  {
    { "host", "default" },
    { "host", "all-features" },
    { "wasm32-unknown-unknown", "default" },
    { "wasm32-unknown-unknown", "all-features" }
  };

  [Theory]
  [Trait("Category", "Platform")]
  [Trait("Case", "manifest-hygiene")]
  [MemberData(nameof(Graphs))]
  public async Task NormalGraphHasNoTestDebugOrDeadCrates(string target, string features)
  {
    List<string> arguments = target == "host" ? [] : ["--target", target];

    if(features == "all-features")
    {
      arguments.Add("--all-features");
    }

    HashSet<string> graph = await Cargo.NormalGraphAsync(environment.RepositoryRoot, [.. arguments]);
    Assert.Contains("rayx-signalr", graph);
    string[] present = [.. _devOnlyOrDead.Where(graph.Contains)];
    Assert.Empty(present);
  }

  [Fact]
  [Trait("Category", "Platform")]
  [Trait("Case", "manifest-hygiene")]
  public void NeverCompiledWasmTestsAreDeleted() =>
    Assert.False(
      File.Exists(Path.Combine(environment.RepositoryRoot, "src", "tests", "tests_wasm.rs")),
      "src/tests/tests_wasm.rs must not exist");

  [Fact]
  [Trait("Category", "Platform")]
  [Trait("Case", "manifest-hygiene")]
  public async Task LibraryIsRlibAndTheFixtureIsItsOwnCdylib()
  {
    JsonElement package = await Cargo.PackageAsync(environment.RepositoryRoot);
    JsonElement library = package.GetProperty("targets")
                                 .EnumerateArray()
                                 .Single(target => target.GetProperty("name").GetString() == "rayx_signalr");
    Assert.Equal(["rlib"], library.GetProperty("crate_types").EnumerateArray().Select(type => type.GetString()));

    JsonElement fixture = await Cargo.PackageAsync(environment.RepositoryRoot, "tests/wasm-fixture/Cargo.toml");
    JsonElement fixtureLibrary = fixture.GetProperty("targets")
                                        .EnumerateArray()
                                        .Single(target => target.GetProperty("kind").EnumerateArray().Any(kind => kind.GetString() == "cdylib"));
    Assert.Equal(["cdylib"], fixtureLibrary.GetProperty("crate_types").EnumerateArray().Select(type => type.GetString()));
  }

  [Fact]
  [Trait("Category", "Platform")]
  [Trait("Case", "manifest-hygiene")]
  public async Task TokioDependencyEnablesOnlyRuntimeFeatures()
  {
    JsonElement package = await Cargo.PackageAsync(environment.RepositoryRoot);
    JsonElement[] tokio = package.GetProperty("dependencies")
                                 .EnumerateArray()
                                 .Where(dependency => dependency.GetProperty("name").GetString() == "tokio")
                                 .ToArray();
    JsonElement normal = tokio.Single(dependency => dependency.GetProperty("kind").ValueKind == JsonValueKind.Null);
    Assert.False(normal.GetProperty("uses_default_features").GetBoolean(), "tokio default features must be disabled");
    string[] declared = normal.GetProperty("features").EnumerateArray().Select(feature => feature.GetString()!).ToArray();
    Assert.Empty(declared.Except(_allowedTokioFeatures));
    JsonElement development = tokio.Single(dependency => dependency.GetProperty("kind").GetString() == "dev");
    string[] developmentFeatures =
      [.. development.GetProperty("features").EnumerateArray().Select(feature => feature.GetString()!)];
    Assert.Contains("macros", developmentFeatures);
    Assert.Contains("rt-multi-thread", developmentFeatures);

    // Resolved over normal edges only (dev-dependencies excluded), across all features.
    string features = await Cargo.RunAsync(
                        environment.RepositoryRoot,
                        ["tree", "-e", "normal,features", "-i", "tokio", "--all-features", "--prefix", "none"]);
    HashSet<string> resolved = TokioFeature()
                               .Matches(features)
                               .Select(match => match.Groups["feature"].Value)
                               .ToHashSet(StringComparer.Ordinal);
    string[] prohibited = [.. _prohibitedTokioFeatures.Where(resolved.Contains)];
    Assert.Empty(prohibited);
  }

  [GeneratedRegex(@"(?m)^tokio feature ""(?<feature>[^""]+)""")]
  private static partial Regex TokioFeature();
}
