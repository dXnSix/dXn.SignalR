using Microsoft.Extensions.Configuration;
using dXn.SignalR.AppHost;

// Compose the test-only graph used by every service-dependent dXn.SignalR test. Every endpoint is
// allocated dynamically by Aspire and handed to scenarios explicitly; nothing binds a fixed port.
IDistributedApplicationBuilder builder = DistributedApplication.CreateBuilder(args);
string repositoryRoot = RepositoryLayout.FindRoot(builder.AppHostDirectory);

// An ephemeral CA and server certificate for the service's HTTPS endpoint, regenerated per run.
TestCertificateFiles certificates = TestCertificates.Generate(
  builder.Configuration["Tls:Directory"]
  ?? Path.Combine(repositoryRoot, "artifacts-temp", "tls", $"{DateTime.UtcNow:yyyyMMdd-HHmmss}-{Environment.ProcessId}"));

// The ASP.NET Core SignalR test service. Its launch profile is excluded so Aspire owns the ports.
IResourceBuilder<ProjectResource> service = builder
  .AddProject<Projects.dXn_SignalR_TestService>(
    "signalr-test-service",
    options => options.ExcludeLaunchProfile = true)
  .WithHttpEndpoint(name: "http")
  .WithHttpsEndpoint(name: "https")
  // HTTP/2 only over plaintext (h2c with prior knowledge) and HTTP/1.1 only over TLS, for the
  // `http2` scenario; the service pins each one's protocols by its target port.
  .WithHttpEndpoint(name: "h2c")
  .WithHttpsEndpoint(name: "https-http1")
  .WithEnvironment("TestTls__CertificatePath", certificates.ServerCertificate)
  .WithEnvironment("TestTls__KeyPath", certificates.ServerKey)
  .WithEnvironment("TestTls__CaPath", certificates.CaCertificate)
  .WithEnvironment("TestTls__UntrustedCertificatePath", certificates.UntrustedServerCertificate)
  .WithEnvironment("TestTls__UntrustedKeyPath", certificates.UntrustedServerKey)
  .WithHttpHealthCheck("/health", endpointName: "http");
service
  .WithEnvironment("TestService__Http2OnlyPort", service.GetEndpoint("h2c").Property(EndpointProperty.TargetPort))
  .WithEnvironment("TestService__Http1OnlyPort", service.GetEndpoint("https-http1").Property(EndpointProperty.TargetPort));
// HTTP/3 (QUIC over UDP) for the `http3` scenario where the host has msquic. Aspire's endpoint
// proxy forwards TCP only, so Kestrel binds this endpoint's (dynamically allocated) port itself.
if(QuicSupport.IsSupported)
{
  service
    .WithHttpsEndpoint(name: "http3", isProxied: false)
    .WithEnvironment("TestService__Http3Port", service.GetEndpoint("http3").Property(EndpointProperty.TargetPort));
}

// The test-only HTTP CONNECT proxy of the `proxy` scenario: tunnels need the run's credentials
// (the test runner passes the password as a secret parameter), and `denied.invalid:80` is on its
// deny list.
IResourceBuilder<ParameterResource> proxyPassword = builder.AddParameter("test-proxy-password", secret: true);
builder
  .AddProject<Projects.dXn_SignalR_TestProxy>(
    "test-proxy",
    options => options.ExcludeLaunchProfile = true)
  .WithHttpEndpoint(name: "http")
  .WithEnvironment("TestProxy__User", "dxn")
  .WithEnvironment("TestProxy__Password", proxyPassword)
  .WithEnvironment("TestProxy__Deny", "denied.invalid:80")
  // The run's test certificate for the TLS listener of `https://` proxies (proxy-https).
  .WithEnvironment("TestProxy__CertificatePath", certificates.ServerCertificate)
  .WithEnvironment("TestProxy__KeyPath", certificates.ServerKey)
  // Tunnels to the service's allocated ports go straight to the ports it listens on (which it
  // reports at /e2e/listening), past Aspire's own endpoint proxy, so the service sees this proxy's
  // tunnels as its peers.
  .WithEnvironment("TestProxy__Service", service.GetEndpoint("http"))
  .WithEnvironment("TestProxy__ServicePorts__http", service.GetEndpoint("http").Property(EndpointProperty.Port))
  .WithEnvironment("TestProxy__ServicePorts__https", service.GetEndpoint("https").Property(EndpointProperty.Port))
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
  bindgenCli.WithEnvironment("DXN_SIGNALR_WASM_BINDGEN_VERSION", bindgenVersion);

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
      Path.Combine(fixtureTarget, "wasm32-unknown-unknown", "debug", "dxn_signalr_wasm_fixture.wasm"),
      "--target",
      "web",
      "--out-dir",
      fixtureOutput,
      "--out-name",
      "dxn_signalr_fixture")
    .WaitForCompletion(fixtureBuild);

  // Serve the generated fixture and the host pages from their own origin, distinct from the hub's.
  IResourceBuilder<ProjectResource> fixtureHost = builder
    .AddProject<Projects.dXn_SignalR_FixtureHost>(
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
        Path.Combine(threadsTarget, "wasm32-unknown-unknown", "debug", "dxn_signalr_wasm_fixture.wasm"),
        "--target",
        "web",
        "--out-dir",
        threadsOutput,
        "--out-name",
        "dxn_signalr_fixture")
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
