using Microsoft.Extensions.Configuration;
using RayX.SignalR.AppHost;

// Compose the test-only graph used by every service-dependent RayX.SignalR test. Every endpoint is
// allocated dynamically by Aspire and handed to scenarios explicitly; nothing binds a fixed port.
IDistributedApplicationBuilder builder = DistributedApplication.CreateBuilder(args);
string repositoryRoot = RepositoryLayout.FindRoot(builder.AppHostDirectory);

// The ASP.NET Core SignalR test service. Its launch profile is excluded so Aspire owns the port.
builder
  .AddProject<Projects.SignalRTestService>(
    "signalr-test-service",
    options => options.ExcludeLaunchProfile = true)
  .WithHttpEndpoint(name: "http")
  .WithHttpHealthCheck("/health");

// Native-only runs disable the browser fixture to skip the WASM build.
if(builder.Configuration.GetValue("Fixture:Enabled", true))
{
  // Provision the fixture's exact wasm-bindgen CLI under artifacts-temp/tools, reusing an existing
  // installation and never touching the machine-wide CLI.
  string bindgenVersion = WasmBindgenToolchain.ResolveFixtureVersion(repositoryRoot);
  string bindgenRoot = WasmBindgenToolchain.ToolRoot(repositoryRoot, bindgenVersion);
  string bindgenExecutable = WasmBindgenToolchain.ExecutablePath(bindgenRoot);
  IResourceBuilder<ExecutableResource> bindgenCli = File.Exists(bindgenExecutable)
    ? builder.AddExecutable("wasm-bindgen-cli", bindgenExecutable, repositoryRoot, "--version")
    : builder.AddExecutable(
      "wasm-bindgen-cli",
      "cargo",
      repositoryRoot,
      WasmBindgenToolchain.InstallArguments(bindgenVersion, bindgenRoot));
  bindgenCli.WithEnvironment("RAYX_SIGNALR_WASM_BINDGEN_VERSION", bindgenVersion);

  string fixtureTarget = Path.Combine(repositoryRoot, "artifacts-temp", "wasm-fixture-target");
  string fixtureOutput = Path.Combine(repositoryRoot, "artifacts-temp", "wasm-fixture");
  IResourceBuilder<ExecutableResource> fixtureBuild = builder
    .AddExecutable(
      "wasm-fixture-build",
      "cargo",
      repositoryRoot,
      "build",
      "--manifest-path",
      WasmBindgenToolchain.FixtureManifest,
      "--target",
      "wasm32-unknown-unknown",
      "--target-dir",
      fixtureTarget)
    .WaitForCompletion(bindgenCli);

  IResourceBuilder<ExecutableResource> fixtureBindgen = builder
    .AddExecutable(
      "wasm-fixture-bindgen",
      bindgenExecutable,
      repositoryRoot,
      Path.Combine(fixtureTarget, "wasm32-unknown-unknown", "debug", "rayx_signalr_wasm_fixture.wasm"),
      "--target",
      "web",
      "--out-dir",
      fixtureOutput,
      "--out-name",
      "rayx_signalr_fixture")
    .WaitForCompletion(fixtureBuild);

  // Serve the generated fixture and the host pages from their own origin, distinct from the hub's.
  builder
    .AddProject<Projects.RayX_SignalR_FixtureHost>(
      "fixture-host",
      options => options.ExcludeLaunchProfile = true)
    .WithHttpEndpoint(name: "http")
    .WithHttpHealthCheck("/health")
    .WithEnvironment("FixtureHost__FixtureDirectory", fixtureOutput)
    .WithEnvironment(
      "FixtureHost__HostPagesDirectory",
      Path.Combine(repositoryRoot, "tools-node", "wasm-host"))
    .WaitForCompletion(fixtureBindgen);
}

builder.Build().Run();
