using RayX.SignalR.E2E.Tests.Scenarios;

namespace RayX.SignalR.E2E.Tests.Contracts;

/// <summary>
/// Proves the catalog's transport dimension (R5.C3): <c>RAYX_SIGNALR_E2E_TRANSPORTS</c> selection,
/// cells for the transport baseline on every selected transport and for every other scenario on
/// WebSockets only, never Server-Sent Events with MessagePack, transport-suffixed names, and the
/// transport reaching the runners.
/// </summary>
public sealed class TransportCatalogTests
{
  [Fact]
  [Trait("Category", "Catalog")]
  public void TransportSelectionDefaultsToWebSocketsAndRejectsEmptyOrUnknownValues()
  {
    Assert.Equal([ScenarioCatalog.WebSockets], E2ESelection.Parse(null, null, null).Transports);
    Assert.Equal(ScenarioCatalog.Transports, E2ESelection.Parse(null, null, null, " ALL ").Transports);
    Assert.Equal(
      [ScenarioCatalog.LongPolling, ScenarioCatalog.ServerSentEvents],
      E2ESelection.Parse(null, null, null, "long-polling,server-sent-events").Transports);
    Assert.Throws<ArgumentException>(() => E2ESelection.Parse(null, null, null, " , "));
    Assert.Throws<ArgumentException>(() => E2ESelection.Parse(null, null, null, "webtransport"));
  }

  [Fact]
  [Trait("Category", "Catalog")]
  public void OtherTransportsRunOnlyTheTransportBaseline()
  {
    IReadOnlyList<E2ECell> cells = E2ESelection.Parse("all", "all", "all", "long-polling,server-sent-events").Cells();
    Assert.NotEmpty(cells);
    Assert.All(cells, cell => Assert.True(cell.Scenario.TransportBaseline, cell.Id));
    string[] baseline =
    [
      "connect-negotiate", "authentication", "invoke", "send", "server-stream", "callbacks",
      "concurrency-and-clones", "large-payload", "server-close", "reconnect-exhaustion",
      "ordinary-reconnect", "keepalive-idle", "server-timeout", "pause-resume", "token-factory"
    ];
    Assert.Equal(
      baseline.Order(StringComparer.Ordinal),
      ScenarioCatalog.Scenarios.Where(scenario => scenario.TransportBaseline)
                     .Select(scenario => scenario.Name)
                     .Order(StringComparer.Ordinal));
    Assert.Throws<InvalidOperationException>(
      () => E2ESelection.Parse("native", "all", "stateful-reconnect", "long-polling").Cells());
  }

  [Fact]
  [Trait("Category", "Catalog")]
  public void ServerSentEventsNeverRunWithMessagePack()
  {
    IReadOnlyList<E2ECell> cells = E2ESelection.Parse("all", "all", "all", "server-sent-events").Cells();
    Assert.All(cells, cell => Assert.Equal("json", cell.Protocol));
    Assert.Contains(
      E2ESelection.Parse("native", "all", "invoke", "long-polling").Cells(),
      cell => cell.Protocol == "messagepack");
  }

  [Fact]
  [Trait("Category", "Catalog")]
  public void WebSocketCellsKeepTheirNamesAndOtherTransportsAreSuffixed()
  {
    Scenario invoke = ScenarioCatalog.Scenario("invoke");
    E2ECell webSockets = new(ScenarioCatalog.Target("native"), "json", invoke);
    E2ECell longPolling = new(ScenarioCatalog.Target("native"), "json", invoke, ScenarioCatalog.LongPolling);
    Assert.Equal("native/json/invoke", webSockets.Id);
    Assert.Equal("native/json/invoke/long-polling", longPolling.Id);
    Assert.Equal("native-json-invoke-long-polling", longPolling.FileName);
    Assert.Equal(longPolling, E2ECell.Parse(longPolling.Id));
    Assert.Equal(webSockets, E2ECell.Parse(webSockets.Id));
    Assert.Throws<ArgumentException>(() => E2ECell.Parse("native/json/invoke/websockets"));
    Assert.Equal("invoke JSON", invoke.PlaywrightTitle("json"));
    Assert.Equal("invoke (JSON, Long Polling)", invoke.PlaywrightTitle("json", ScenarioCatalog.LongPolling));
    Assert.Equal(
      "invoke (JSON, Server-Sent Events)",
      invoke.PlaywrightTitle("json", ScenarioCatalog.ServerSentEvents));
  }

  [Fact]
  [Trait("Category", "Catalog")]
  public void TheTransportReachesEveryRunner()
  {
    E2ECell cell = new(
      ScenarioCatalog.Target("android"),
      "json",
      ScenarioCatalog.Scenario("invoke"),
      ScenarioCatalog.LongPolling);
    Dictionary<string, string> variables = CellExecution.CellVariables(cell);
    Assert.Equal(ScenarioCatalog.LongPolling, variables[CellExecution.TransportVariable]);
    Assert.Contains(
      $"{CellExecution.TransportVariable}='long-polling'",
      AndroidCellRunner.ShellCommand(cell.Scenario.RustTest(cell.Protocol), variables),
      StringComparison.Ordinal);
    Assert.Equal(
      ScenarioCatalog.LongPolling,
      IosCellRunner.ChildVariables(variables)[$"SIMCTL_CHILD_{CellExecution.TransportVariable}"]);
  }
}
