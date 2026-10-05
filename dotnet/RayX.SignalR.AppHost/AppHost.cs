using Microsoft.Extensions.Configuration;
using RayX.SignalR.AppHost;

// Compose the test-only graph used by every service-dependent RayX.SignalR test. Every endpoint is
// allocated dynamically by Aspire and handed to scenarios explicitly; nothing binds a fixed port.
IDistributedApplicationBuilder builder = DistributedApplication.CreateBuilder(args);
string repositoryRoot = RepositoryLayout.FindRoot(builder.AppHostDirectory);

// An ephemeral CA and server certificate for the service's HTTPS endpoint, regenerated per run.
TestCertificateFiles certificates = TestCertificates.Generate(
  builder.Configuration["Tls:Directory"]
  ?? Path.Combine(repositoryRoot, "artifacts-temp", "e2e", "tls", $"{DateTime.UtcNow:yyyyMMdd-HHmmss}-{Environment.ProcessId}"));

// The ASP.NET Core SignalR test service. Its launch profile is excluded so Aspire owns the ports.
builder
  .AddProject<Projects.SignalRTestService>(
    "signalr-test-service",
    options => options.ExcludeLaunchProfile = true)
  .WithHttpEndpoint(name: "http")
  .WithHttpsEndpoint(name: "https")
  .WithEnvironment("TestTls__CertificatePath", certificates.ServerCertificate)
  .WithEnvironment("TestTls__KeyPath", certificates.ServerKey)
  .WithHttpHealthCheck("/health", endpointName: "http");

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
  IResourceBuilder<ProjectResource> fixtureHost = builder
    .AddProject<Projects.RayX_SignalR_FixtureHost>(
      "fixture-host",
      options => options.ExcludeLaunchProfile = true)
    .WithHttpEndpoint(name: "http")
    .WithHttpHealthCheck("/health", endpointName: "http")
    .WithEnvironment("FixtureHost__FixtureDirectory", fixtureOutput)
    .WithEnvironment(
      "FixtureHost__HostPagesDirectory",
      Path.Combine(repositoryRoot, "tools-node", "wasm-host"))
    .WaitForCompletion(fixtureBindgen);

  // Opt-in threaded (atomics) fixture with RayX's flags, served cross-origin isolated (COOP/COEP)
  // on the fixture host's `isolated` endpoint.
  if(builder.Configuration.GetValue("Fixture:Threads", false))
  {
    string threadsTarget = Path.Combine(repositoryRoot, "artifacts-temp", "wasm-fixture-threads-target");
    string threadsOutput = Path.Combine(repositoryRoot, "artifacts-temp", "wasm-fixture-threads");
    IResourceBuilder<ExecutableResource> threadsBuild = builder
      .AddExecutable(
        "wasm-fixture-threads-build",
        "cargo",
        repositoryRoot,
        ThreadedWasm.BuildArguments(WasmBindgenToolchain.FixtureManifest, threadsTarget))
      .WithEnvironment("RUSTFLAGS", ThreadedWasm.RustFlags)
      .WaitForCompletion(bindgenCli);
    IResourceBuilder<ExecutableResource> threadsBindgen = builder
      .AddExecutable(
        "wasm-fixture-threads-bindgen",
        bindgenExecutable,
        repositoryRoot,
        Path.Combine(threadsTarget, "wasm32-unknown-unknown", "debug", "rayx_signalr_wasm_fixture.wasm"),
        "--target",
        "web",
        "--out-dir",
        threadsOutput,
        "--out-name",
        "rayx_signalr_fixture")
      .WaitForCompletion(threadsBuild);
    fixtureHost
      .WithHttpEndpoint(name: "isolated")
      .WithEnvironment("FixtureHost__ThreadedDirectory", threadsOutput)
      .WithEnvironment(
        "FixtureHost__IsolatedPort",
        fixtureHost.GetEndpoint("isolated").Property(EndpointProperty.TargetPort))
      .WaitForCompletion(threadsBindgen);
  }
}

builder.Build().Run();
