using System.Text.RegularExpressions;
using RayX.SignalR.AppHost;
using RayX.SignalR.E2E.Tests.Infrastructure;

namespace RayX.SignalR.E2E.Tests.Contracts;

/// <summary>Proves the AppHost provisions the browser fixture's exact wasm-bindgen CLI.</summary>
[Collection(AspireTestGroup.Name)]
public sealed partial class ToolchainTests(AspireEnvironment environment)
{
  [Fact]
  [Trait("Category", "Toolchain")]
  public void ParsesTheSingleResolvedWasmBindgenVersion()
  {
    const string metadata = """
      {"packages":[
        {"name":"wasm-bindgen","version":"0.2.129"},
        {"name":"wasm-bindgen-macro","version":"0.2.129"},
        {"name":"js-sys","version":"0.3.80"}
      ]}
      """;
    Assert.Equal("0.2.129", WasmBindgenToolchain.ParseResolvedVersion(metadata));
    Assert.Throws<InvalidOperationException>(
      () => WasmBindgenToolchain.ParseResolvedVersion("""{"packages":[{"name":"js-sys","version":"0.3.80"}]}"""));
    Assert.Throws<InvalidOperationException>(
      () => WasmBindgenToolchain.ParseResolvedVersion(
        """{"packages":[{"name":"wasm-bindgen","version":"0.2.126"},{"name":"wasm-bindgen","version":"0.2.129"}]}"""));
    Assert.Equal(
      ["install", "wasm-bindgen-cli", "--version", "0.2.129", "--locked", "--root", "tools"],
      WasmBindgenToolchain.InstallArguments("0.2.129", "tools"));
  }

  [Fact]
  [Trait("Category", "Toolchain")]
  public void ResolvesTheFixturePinFromCargoMetadata()
  {
    string manifest = File.ReadAllText(Path.Combine(environment.RepositoryRoot, WasmBindgenToolchain.FixtureManifest));
    Match pin = WasmBindgenPin().Match(manifest);
    Assert.True(pin.Success, "The fixture must pin wasm-bindgen exactly (=x.y.z).");
    Assert.Equal(pin.Groups["version"].Value, WasmBindgenToolchain.ResolveFixtureVersion(environment.RepositoryRoot));
  }

  [Fact]
  [Trait("Category", "Toolchain")]
  public async Task AppHostProvisionsThePinnedCliUnderArtifactsTemp()
  {
    await environment.EnsureStartedAsync(requireFixture: true);
    using CancellationTokenSource timeout = new(TimeSpan.FromMinutes(1));
    await environment.WaitForSuccessfulCompletionAsync("wasm-bindgen-cli", timeout.Token);

    string version = WasmBindgenToolchain.ResolveFixtureVersion(environment.RepositoryRoot);
    string toolRoot = WasmBindgenToolchain.ToolRoot(environment.RepositoryRoot, version);
    string executable = WasmBindgenToolchain.ExecutablePath(toolRoot);
    Assert.StartsWith(
      Path.Combine(environment.RepositoryRoot, "artifacts-temp", "tools", "wasm-bindgen"),
      executable,
      StringComparison.Ordinal);
    Assert.True(File.Exists(executable), $"The pinned CLI is missing at {executable}.");

    ProcessResult result = await ProcessRunner.RunAsync(
                             new ProcessCommand(executable, ["--version"], environment.RepositoryRoot),
                             TimeSpan.FromSeconds(30));
    Assert.True(result.Succeeded, result.CombinedOutput);
    Assert.Equal($"wasm-bindgen {version}", result.StandardOutput.Trim());
  }

  [GeneratedRegex("""wasm-bindgen\s*=\s*"=(?<version>\d+\.\d+\.\d+)"(?m)""")]
  private static partial Regex WasmBindgenPin();
}
