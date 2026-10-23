using dXn.SignalR.E2E.Tests.Infrastructure;

namespace dXn.SignalR.E2E.Tests.Contracts;

/// <summary>Proves the AppHost graph starts every resource on dynamically allocated ports.</summary>
[Collection(AspireTestGroup.Name)]
public sealed class OrchestrationTests(AspireEnvironment environment)
{
  static readonly int[] _formerFixedPorts = [5220, 7878];

  [Fact]
  [Trait("Category", "Orchestration")]
  public async Task EveryResourceIsHealthyOrCompletedOnDynamicPorts()
  {
    await environment.EnsureStartedAsync(requireFixture: true);

    foreach(string resource in AspireEnvironment.FixtureBuildResources)
    {
      using CancellationTokenSource timeout = new(TimeSpan.FromMinutes(1));
      Assert.Equal(0, (await environment.WaitForSuccessfulCompletionAsync(resource, timeout.Token)).ExitCode);
    }

    Uri service = environment.ServiceUrl;
    Uri fixture = environment.FixtureUrl;
    Assert.DoesNotContain(service.Port, _formerFixedPorts);
    Assert.DoesNotContain(fixture.Port, _formerFixedPorts);
    Assert.NotEqual(service.Port, fixture.Port);

    using HttpClient client = new() { Timeout = TimeSpan.FromSeconds(30) };
    Assert.Contains("ready", await client.GetStringAsync(new Uri(service, "/health")), StringComparison.Ordinal);
    Assert.Contains("ready", await client.GetStringAsync(new Uri(fixture, "/health")), StringComparison.Ordinal);
    string page = await client.GetStringAsync(fixture);
    Assert.Contains("dxn_signalr_fixture.js", page, StringComparison.Ordinal);
    using HttpResponseMessage wasm = await client.GetAsync(new Uri(fixture, "/dxn_signalr_fixture_bg.wasm"));
    wasm.EnsureSuccessStatusCode();
    Assert.Equal("application/wasm", wasm.Content.Headers.ContentType?.MediaType);
  }
}
