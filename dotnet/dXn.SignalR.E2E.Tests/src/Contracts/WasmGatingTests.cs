using dXn.SignalR.E2E.Tests.Infrastructure;

namespace dXn.SignalR.E2E.Tests.Contracts;

/// <summary>Proves non-browser wasm targets are rejected explicitly and the cfg split is uniform.</summary>
[Collection(AspireTestGroup.Name)]
public sealed class WasmGatingTests(AspireEnvironment environment)
{
  const string _browserOnlyMessage =
    "dxn-signalr supports browser WASM (wasm32-unknown-unknown) as its only wasm target";

  [Theory]
  [Trait("Category", "Platform")]
  [Trait("Case", "wasi-rejected")]
  [InlineData("default")]
  [InlineData("all-features")]
  public async Task WasiBuildFailsWithTheBrowserOnlyMessage(string features)
  {
    List<string> arguments = ["check", "--lib", "--target", "wasm32-wasip1", "--message-format=short"];
    if(features == "all-features")
    {
      arguments.Add("--all-features");
    }

    ProcessResult result = await Cargo.RunRawAsync(environment.RepositoryRoot, arguments);
    string output = result.CombinedOutput;
    Assert.False(
      output.Contains("target may not be installed", StringComparison.Ordinal),
      "Install the WASI target first: rustup target add wasm32-wasip1 --toolchain 1.92.0");
    Assert.NotEqual(0, result.ExitCode);
    Assert.Contains(_browserOnlyMessage, output, StringComparison.Ordinal);

    // The guard must be the reason: no dependency fails before the crate's own check.
    string[] failingCrates = output.Split('\n')
                                   .Where(line => line.Contains("error: could not compile", StringComparison.Ordinal))
                                   .ToArray();
    Assert.Single(failingCrates);
    Assert.Contains("`dxn-signalr`", failingCrates[0], StringComparison.Ordinal);
  }

  [Fact]
  [Trait("Category", "Platform")]
  [Trait("Case", "wasi-rejected")]
  public async Task LibraryUsesOneNativeBrowserCfgFamily()
  {
    IReadOnlyList<string> files = await RepositoryFiles.ListAsync(environment.RepositoryRoot);
    string[] mixed = files.Where(file => file.StartsWith("src/", StringComparison.Ordinal) && file.EndsWith(".rs", StringComparison.Ordinal))
                          .Where(file => File.ReadAllText(Path.Combine(environment.RepositoryRoot, file))
                                             .Contains("target_family", StringComparison.Ordinal))
                          .ToArray();
    Assert.Empty(mixed);
  }
}
