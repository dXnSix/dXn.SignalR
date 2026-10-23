using dXn.SignalR.AppHost;
using dXn.SignalR.E2E.Tests.Infrastructure;

namespace dXn.SignalR.E2E.Tests.Contracts;

/// <summary>Proves the library builds for RayX's multithreaded (atomics) browser WASM.</summary>
[Collection(AspireTestGroup.Name)]
public sealed class ThreadedWasmBuildTests(AspireEnvironment environment)
{
  [Theory]
  [Trait("Category", "Platform")]
  [Trait("Case", "threaded-wasm-build")]
  [InlineData("default")]
  [InlineData("all-features")]
  public async Task LibraryBuildsWithAtomicsAndBuildStd(string features)
  {
    string target = Path.Combine(environment.RepositoryRoot, "artifacts-temp", "threaded-wasm-target");
    string[] extra = features == "all-features" ? ["--lib", "--all-features"] : ["--lib"];
    ProcessResult result = await Cargo.RunRawAsync(
                             environment.RepositoryRoot,
                             ThreadedWasm.BuildArguments("Cargo.toml", target, extra),
                             new Dictionary<string, string> { ["RUSTFLAGS"] = ThreadedWasm.RustFlags });
    Assert.True(
      result.Succeeded,
      $"The threaded WASM build ({features}) exited {result.ExitCode}. Prerequisites: rustup toolchain install {ThreadedWasm.Toolchain} --profile minimal -c rust-src -t wasm32-unknown-unknown.\n{Redaction.SafeTail(result.StandardError)}");
  }
}
