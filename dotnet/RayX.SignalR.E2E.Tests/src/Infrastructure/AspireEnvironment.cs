using System.Text;
using Aspire.Hosting;
using Aspire.Hosting.ApplicationModel;
using Aspire.Hosting.Testing;
using Microsoft.Extensions.DependencyInjection;
using RayX.SignalR.AppHost;

namespace RayX.SignalR.E2E.Tests.Infrastructure;

/// <summary>
/// Starts the RayX.SignalR AppHost once per test run and exposes its dynamically allocated
/// endpoints and resource logs to every test in the <see cref="AspireTestGroup"/>.
/// </summary>
/// <remarks>
/// The AppHost starts lazily with the capabilities the first test needs. A later test that needs
/// the browser fixture when it is not running restarts the AppHost once with the fixture enabled.
/// </remarks>
public sealed class AspireEnvironment : IAsyncLifetime, IDisposable
{
  /// <summary>The SignalR test service resource.</summary>
  public const string ServiceResource = "signalr-test-service";

  /// <summary>The browser fixture static host resource.</summary>
  public const string FixtureHostResource = "fixture-host";

  /// <summary>The test-only CONNECT proxy resource.</summary>
  public const string ProxyResource = "test-proxy";

  /// <summary>The user name the test proxy requires.</summary>
  public const string ProxyUser = "rayx";

  /// <summary>The completion-gated resources that produce the browser fixture, in dependency order.</summary>
  public static readonly IReadOnlyList<string> FixtureBuildResources =
    ["wasm-bindgen-cli", "wasm-fixture-build", "wasm-fixture-bindgen"];

  /// <summary>The completion-gated resources that produce the threaded fixture.</summary>
  public static readonly IReadOnlyList<string> ThreadedFixtureBuildResources =
    ["wasm-fixture-threads-build", "wasm-fixture-threads-bindgen"];

  static readonly TimeSpan _startupTimeout = TimeSpan.FromMinutes(20);

  readonly SemaphoreSlim _gate = new(1, 1);
  DistributedApplication? _app;
  bool _fixtureEnabled;
  bool _threadsEnabled;
  Exception? _startupFailure;
  bool _startupFailureWithFixture;
  IReadOnlyDictionary<string, string> _startupLogs = new Dictionary<string, string>();

  /// <summary>Gets the repository root.</summary>
  public string RepositoryRoot { get; } = RepositoryLayout.FindRoot(AppContext.BaseDirectory);

  /// <summary>Gets the running application.</summary>
  public DistributedApplication App =>
    _app ?? throw new InvalidOperationException("The AppHost has not been started.");

  /// <summary>Gets whether the running AppHost includes the browser fixture.</summary>
  public bool FixtureEnabled => _app is not null && _fixtureEnabled;

  /// <summary>Gets the SignalR test service's HTTP endpoint.</summary>
  public Uri ServiceUrl => App.GetEndpoint(ServiceResource, "http");

  /// <summary>Gets the test proxy's endpoint.</summary>
  public Uri ProxyUrl => App.GetEndpoint(ProxyResource, "http");

  /// <summary>Gets the password the test proxy requires, generated for this run.</summary>
  public string ProxyPassword { get; } = Guid.NewGuid().ToString("N");

  /// <summary>Gets the SignalR test service's HTTPS endpoint (served with <see cref="TlsCaPem"/>'s CA).</summary>
  public Uri TlsServiceUrl => App.GetEndpoint(ServiceResource, "https");

  /// <summary>
  /// Gets the directory of this run's generated TLS certificates and key: outside
  /// <c>artifacts-temp/e2e</c>, which CI uploads on failure, and deleted when the run stops.
  /// </summary>
  public string TlsDirectory { get; } = Path.Combine(
    RepositoryLayout.FindRoot(AppContext.BaseDirectory),
    "artifacts-temp",
    "tls",
    $"{DateTime.UtcNow:yyyyMMdd-HHmmss}-{Guid.NewGuid():N}");

  /// <summary>Gets the PEM of the CA that issued the HTTPS endpoint's certificate.</summary>
  public string TlsCaPem => File.ReadAllText(new TestCertificateFiles(TlsDirectory).CaCertificate);

  /// <summary>Gets the fixture host's cross-origin-isolated endpoint serving the threaded fixture.</summary>
  public Uri IsolatedFixtureUrl =>
    _app is not null && _threadsEnabled
      ? App.GetEndpoint(FixtureHostResource, "isolated")
      : throw new InvalidOperationException("The AppHost was started without the threaded fixture.");

  /// <summary>Gets whether the running AppHost includes the threaded fixture.</summary>
  public bool ThreadsEnabled => _app is not null && _threadsEnabled;

  /// <summary>Gets the fixture host's HTTP endpoint.</summary>
  public Uri FixtureUrl =>
    FixtureEnabled
      ? App.GetEndpoint(FixtureHostResource, "http")
      : throw new InvalidOperationException("The AppHost was started without the browser fixture.");

  /// <summary>Starts the AppHost if needed, with the browser fixture when <paramref name="requireFixture"/> is set.</summary>
  /// <param name="requireFixture">Whether the browser fixture resources must run.</param>
  /// <param name="cancellationToken">Cancels startup.</param>
  /// <param name="requireThreads">Whether the threaded fixture and the isolated endpoint must run.</param>
  public async Task EnsureStartedAsync(
    bool requireFixture,
    bool requireThreads = false,
    CancellationToken cancellationToken = default)
  {
    await _gate.WaitAsync(cancellationToken);
    // A failed start is not retried for every cell; later cells fail fast with the same cause.
    if(_startupFailure is not null && (_startupFailureWithFixture || !requireFixture))
    {
      _gate.Release();
      throw new InvalidOperationException("The AppHost failed to start earlier in this run.", _startupFailure);
    }

    try
    {
      requireFixture |= requireThreads;
      if(_app is not null && (_fixtureEnabled || !requireFixture) && (_threadsEnabled || !requireThreads))
      {
        return;
      }

      await StopAsync();
      using CancellationTokenSource startup = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
      startup.CancelAfter(_startupTimeout);
      IDistributedApplicationTestingBuilder builder = await DistributedApplicationTestingBuilder
                                                        .CreateAsync<Projects.RayX_SignalR_AppHost>(
                                                          [$"--Fixture:Enabled={requireFixture}", $"--Fixture:Threads={requireThreads}", $"--Tls:Directory={TlsDirectory}", $"--Parameters:test-proxy-password={ProxyPassword}"],
                                                          startup.Token);
      builder.Configuration["DcpPublisher:WaitForResourceCleanup"] = "false";
      DistributedApplication app = await builder.BuildAsync(startup.Token);
      _app = app;
      _fixtureEnabled = requireFixture;
      _threadsEnabled = requireThreads;
      await app.StartAsync(startup.Token);
      await WaitForHealthyAsync(ServiceResource, startup.Token);
      await WaitForHealthyAsync(ProxyResource, startup.Token);
      if(requireFixture)
      {
        foreach(string resource in requireThreads ? [.. FixtureBuildResources, .. ThreadedFixtureBuildResources] : FixtureBuildResources)
        {
          await WaitForSuccessfulCompletionAsync(resource, startup.Token);
        }

        await WaitForHealthyAsync(FixtureHostResource, startup.Token);
      }
    }
    catch(Exception exception)
    {
      _startupFailure = exception;
      _startupFailureWithFixture = requireFixture;
      _startupLogs = await AllResourceLogsAsync();
      await StopAsync();
      throw;
    }
    finally
    {
      _gate.Release();
    }
  }

  /// <summary>Clears the test service's probes and controls before a cell.</summary>
  public async Task ResetServiceAsync()
  {
    using HttpClient client = new() { Timeout = TimeSpan.FromSeconds(30) };
    using HttpResponseMessage response = await client.PostAsync(new Uri(ServiceUrl, "/e2e/reset"), content: null);
    response.EnsureSuccessStatusCode();
  }

  /// <summary>Waits until a resource is healthy, failing fast when it stops first.</summary>
  /// <param name="resource">The resource name.</param>
  /// <param name="cancellationToken">Cancels the wait.</param>
  public async Task WaitForHealthyAsync(string resource, CancellationToken cancellationToken)
  {
    using CancellationTokenSource race = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
    Task healthy = App.ResourceNotifications.WaitForResourceHealthyAsync(resource, race.Token);
    Task<ResourceEvent> stopped = App.ResourceNotifications.WaitForResourceAsync(
                                    resource,
                                    resourceEvent => KnownResourceStates.TerminalStates.Contains(
                                      resourceEvent.Snapshot.State?.Text),
                                    race.Token);
    Task first = await Task.WhenAny(healthy, stopped);
    await race.CancelAsync();
    if(first == stopped && stopped.IsCompletedSuccessfully)
    {
      CustomResourceSnapshot snapshot = stopped.Result.Snapshot;
      throw new InvalidOperationException(
        $"Resource {resource} stopped in state {snapshot.State?.Text} with exit code {snapshot.ExitCode} before becoming healthy.\n{Redaction.SafeTail(await ResourceLogsAsync(resource))}");
    }

    await healthy;
  }

  /// <summary>Waits until an executable resource finishes and fails unless it exited with code 0.</summary>
  /// <param name="resource">The resource name.</param>
  /// <param name="cancellationToken">Cancels the wait.</param>
  /// <returns>The terminal snapshot.</returns>
  public async Task<CustomResourceSnapshot> WaitForSuccessfulCompletionAsync(
    string resource,
    CancellationToken cancellationToken)
  {
    ResourceEvent terminal = await App.ResourceNotifications.WaitForResourceAsync(
                               resource,
                               resourceEvent => KnownResourceStates.TerminalStates.Contains(
                                 resourceEvent.Snapshot.State?.Text),
                               cancellationToken);
    if(terminal.Snapshot.ExitCode != 0)
    {
      string version = terminal.Snapshot.EnvironmentVariables
                               .FirstOrDefault(variable => variable.Name == "RAYX_SIGNALR_WASM_BINDGEN_VERSION")
                               ?.Value
                       ?? "n/a";
      string logs = Redaction.SafeTail(await ResourceLogsAsync(resource));
      throw new InvalidOperationException(
        $"Resource {resource} ended in state {terminal.Snapshot.State?.Text} with exit code {terminal.Snapshot.ExitCode} (pinned wasm-bindgen {version}).\n{logs}");
    }

    return terminal.Snapshot;
  }

  /// <summary>Collects every log line a resource has written so far.</summary>
  /// <param name="resource">The resource name.</param>
  /// <returns>The logs.</returns>
  public async Task<string> ResourceLogsAsync(string resource)
  {
    if(_app is null)
    {
      return string.Empty;
    }

    ResourceLoggerService logger = _app.Services.GetRequiredService<ResourceLoggerService>();
    StringBuilder logs = new();
    using CancellationTokenSource bound = new(TimeSpan.FromSeconds(10));
    try
    {
      await foreach(IReadOnlyList<LogLine> entries in logger.GetAllAsync(resource).WithCancellation(bound.Token))
      {
        foreach(LogLine entry in entries)
        {
          logs.AppendLine(entry.Content);
        }
      }
    }
    catch(OperationCanceledException)
    {
      logs.AppendLine("[log collection timed out]");
    }

    return logs.ToString();
  }

  /// <summary>Collects redacted, bounded logs from every running resource, or those captured when startup failed.</summary>
  /// <returns>Logs keyed by resource name.</returns>
  public async Task<IReadOnlyDictionary<string, string>> AllResourceLogsAsync()
  {
    Dictionary<string, string> logs = new(StringComparer.Ordinal);
    if(_app is null)
    {
      // After a failed start the app is gone; keep the logs captured before shutdown.
      return _startupLogs;
    }

    List<string> resources = [ServiceResource];
    if(_fixtureEnabled)
    {
      resources.AddRange(FixtureBuildResources);
      if(_threadsEnabled)
      {
        resources.AddRange(ThreadedFixtureBuildResources);
      }

      resources.Add(FixtureHostResource);
    }

    foreach(string resource in resources)
    {
      logs[resource] = Redaction.SafeTail(await ResourceLogsAsync(resource));
    }

    return logs;
  }

  /// <inheritdoc/>
  public Task InitializeAsync() => Task.CompletedTask;

  /// <inheritdoc/>
  public async Task DisposeAsync() => await StopAsync();

  /// <inheritdoc/>
  public void Dispose() => _gate.Dispose();

  async Task StopAsync()
  {
    // Device-side artifacts never outlive the run.
    await Scenarios.AndroidCellRunner.CleanupAsync();
    await Scenarios.IosCellRunner.CleanupAsync();
    DistributedApplication? app = _app;
    try
    {
      if(app is not null && Directory.Exists(TlsDirectory))
      {
        Directory.Delete(TlsDirectory, recursive: true);
      }
    }
    catch(IOException)
    {
      // The service may still hold the key open; the folder is outside the uploaded diagnostics.
    }
    catch(UnauthorizedAccessException)
    {
    }
    _app = null;
    _fixtureEnabled = false;
    _threadsEnabled = false;
    if(app is null)
    {
      return;
    }

    using CancellationTokenSource stop = new(TimeSpan.FromMinutes(1));
    try
    {
      await app.StopAsync(stop.Token);
    }
    finally
    {
      await app.DisposeAsync();
    }
  }
}

/// <summary>The single collection that shares one <see cref="AspireEnvironment"/>.</summary>
[CollectionDefinition(Name)]
public sealed class AspireTestGroup : ICollectionFixture<AspireEnvironment>
{
  /// <summary>The collection name.</summary>
  public const string Name = "aspire";
}
