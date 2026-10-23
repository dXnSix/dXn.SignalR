using dXn.SignalR.E2E.Tests.Infrastructure;

namespace dXn.SignalR.E2E.Tests.Contracts;

/// <summary>Proves the browser transport is event-driven and free of the old polling crates.</summary>
[Collection(AspireTestGroup.Name)]
public sealed class WasmTransportTests(AspireEnvironment environment)
{
  static readonly string[] _replacedCrates = ["wasm-sockets", "wasm-timer", "async-std", "ehttp"];

  [Fact]
  [Trait("Category", "Platform")]
  [Trait("Case", "wasm-transport")]
  public async Task TransportSourcesHaveNoIntervalsOrLeakedClosures()
  {
    IReadOnlyList<string> files = await RepositoryFiles.ListAsync(environment.RepositoryRoot);
    List<string> violations = [];
    foreach(string file in files.Where(file => file.StartsWith("src/communication/", StringComparison.Ordinal)
                                               && file.EndsWith(".rs", StringComparison.Ordinal)))
    {
      string text = await File.ReadAllTextAsync(Path.Combine(environment.RepositoryRoot, file));
      foreach(string forbidden in new[] { "setInterval", "set_interval", "Closure::forget", ".forget()", "window()" })
      {
        if(text.Contains(forbidden, StringComparison.Ordinal))
        {
          violations.Add($"{file}: {forbidden}");
        }
      }
    }

    Assert.Empty(violations);
  }

  [Theory]
  [Trait("Category", "Platform")]
  [Trait("Case", "wasm-transport")]
  [InlineData("default")]
  [InlineData("all-features")]
  public async Task BrowserGraphHasNoPollingTransportCrates(string features)
  {
    List<string> arguments = ["--target", "wasm32-unknown-unknown"];
    if(features == "all-features")
    {
      arguments.Add("--all-features");
    }

    HashSet<string> graph = await Cargo.NormalGraphAsync(environment.RepositoryRoot, [.. arguments]);
    Assert.Contains("gloo-net", graph);
    Assert.Contains("gloo-timers", graph);
    string[] replaced = [.. _replacedCrates.Where(graph.Contains)];
    Assert.Empty(replaced);
  }
}
