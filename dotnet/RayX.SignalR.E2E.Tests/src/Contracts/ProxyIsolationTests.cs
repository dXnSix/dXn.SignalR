using RayX.SignalR.E2E.Tests.Infrastructure;

namespace RayX.SignalR.E2E.Tests.Contracts;

/// <summary>
/// Proves that processes running the client never inherit the machine's proxy settings, that
/// build and tool commands keep them, and that proxy credentials never reach diagnostics.
/// </summary>
public sealed class ProxyIsolationTests
{
  static Dictionary<string, string?> MachineEnvironment() =>
    ProcessRunner.ProxyVariables.ToDictionary(
      name => name,
      string? (_) => "http://machine-proxy.invalid:3128",
      StringComparer.Ordinal);

  [Fact]
  [Trait("Category", "Repository")]
  public void ClientProcessesNeverInheritProxyVariables()
  {
    Dictionary<string, string?> environment = MachineEnvironment();
    ProcessCommand cell = new("e2e", ["proxy::json"], ".")
    {
      Environment = new Dictionary<string, string> { ["RAYX_SIGNALR_E2E_SERVICE_URL"] = "http://127.0.0.1:1" },
      IsolateFromProxy = true
    };

    ProcessRunner.ApplyEnvironment(environment, cell);

    foreach(string name in ProcessRunner.ProxyVariables)
    {
      Assert.False(environment.ContainsKey(name), $"{name} reached the cell");
    }

    Assert.Equal("http://127.0.0.1:1", environment["RAYX_SIGNALR_E2E_SERVICE_URL"]);
  }

  [Fact]
  [Trait("Category", "Repository")]
  public void BuildAndToolCommandsKeepTheMachineProxy()
  {
    Dictionary<string, string?> environment = MachineEnvironment();

    ProcessRunner.ApplyEnvironment(environment, new ProcessCommand("cargo", ["build"], "."));

    foreach(string name in ProcessRunner.ProxyVariables)
    {
      Assert.Equal("http://machine-proxy.invalid:3128", environment[name]);
    }
  }

  [Theory]
  [Trait("Category", "Repository")]
  [InlineData("env RAYX_SIGNALR_E2E_PROXY_CREDENTIALS='rayx:0123abcd' ./e2e")]
  [InlineData("RAYX_SIGNALR_E2E_PROXY_CREDENTIALS=rayx:0123abcd")]
  [InlineData("connecting through http://rayx:0123abcd@127.0.0.1:3128")]
  [InlineData("env RAYX_SIGNALR_E2E_CLIENT_KEY_PEM_BASE64='0123abcd' ./e2e")]
  public void DiagnosticsNeverShowProxyCredentials(string diagnostic)
  {
    string redacted = Redaction.Redact(diagnostic);
    Assert.DoesNotContain("0123abcd", redacted, StringComparison.Ordinal);
    Assert.Contains("[REDACTED]", redacted, StringComparison.Ordinal);
  }
}
