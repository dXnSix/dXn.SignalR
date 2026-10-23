using dXn.SignalR.E2E.Tests.Infrastructure;
using dXn.SignalR.E2E.Tests.Scenarios;

namespace dXn.SignalR.E2E.Tests.Contracts;

/// <summary>
/// Proves the runner's suspension controls for the <c>suspend-resume</c> scenario: readiness-line
/// parsing, one suspension after the other with a resume for each, and the catalog entry.
/// </summary>
public sealed class SuspensionTests
{
  [Theory]
  [Trait("Category", "Catalog")]
  [InlineData("DXN_SIGNALR_SUSPEND_READY pid=4242 seconds=20", 4242, 20)]
  [InlineData("test suspend_resume::json ... DXN_SIGNALR_SUSPEND_READY pid=7 seconds=40", 7, 40)]
  [InlineData("DXN_SIGNALR_SUSPEND_READY pid=7 seconds=40\r", 7, 40)]
  public void ReadinessLinesNameTheProcessAndTheDuration(string line, int pid, int seconds)
  {
    Assert.Equal(new SuspensionRequest(pid, TimeSpan.FromSeconds(seconds)), Suspension.Request(line));
  }

  [Theory]
  [Trait("Category", "Catalog")]
  [InlineData("DXN_SIGNALR_SUSPEND_READY pid=7")]
  [InlineData("DXN_SIGNALR_SUSPEND_READY pid=7 seconds=0")]
  [InlineData("DXN_SIGNALR_SUSPEND_READY pid=7 seconds=61")]
  [InlineData("DXN_SIGNALR_SUSPEND_READY pid=-7 seconds=20")]
  [InlineData("connected to the hub")]
  public void OtherLinesAreNotReadinessLines(string line)
  {
    Assert.Null(Suspension.Request(line));
  }

  [Fact]
  [Trait("Category", "Catalog")]
  public async Task EachReadinessLineSuspendsAndResumesInTurn()
  {
    List<string> calls = [];
    SuspensionController controller = new(
      pid =>
      {
        lock(calls)
        {
          calls.Add($"suspend {pid}");
        }

        return Task.CompletedTask;
      },
      pid =>
      {
        lock(calls)
        {
          calls.Add($"resume {pid}");
        }

        return Task.CompletedTask;
      });

    controller.OnOutputLine("starting");
    controller.OnOutputLine("DXN_SIGNALR_SUSPEND_READY pid=11 seconds=1");
    controller.OnOutputLine("DXN_SIGNALR_SUSPEND_READY pid=11 seconds=1");
    controller.OnOutputLine("DXN_SIGNALR_SUSPEND_READY pid=11 seconds=99");
    ProcessResult result = await controller.CompleteAsync(new ProcessResult(0, false, string.Empty, "scenario"));

    Assert.Equal(["suspend 11", "resume 11", "suspend 11", "resume 11"], calls);
    Assert.StartsWith("scenario\n", result.StandardError, StringComparison.Ordinal);
    Assert.Contains("ignored an invalid readiness line", result.StandardError, StringComparison.Ordinal);
    Assert.Equal(2, result.StandardError.Split("resumed process 11").Length - 1);
  }

  [Fact]
  [Trait("Category", "Catalog")]
  public async Task AFailedSuspensionStillResumesAndIsReported()
  {
    List<string> calls = [];
    SuspensionController controller = new(
      _ => throw new InvalidOperationException("no such process"),
      pid =>
      {
        calls.Add($"resume {pid}");
        return Task.CompletedTask;
      });

    controller.OnOutputLine("DXN_SIGNALR_SUSPEND_READY pid=5 seconds=1");
    ProcessResult result = await controller.CompleteAsync(new ProcessResult(1, false, string.Empty, string.Empty));

    Assert.Equal(["resume 5"], calls);
    Assert.Contains("suspending process 5 failed: no such process", result.StandardError, StringComparison.Ordinal);
  }

  [Fact]
  [Trait("Category", "Catalog")]
  public async Task AScenarioThatNeverAskedIsReported()
  {
    SuspensionController controller = new(_ => Task.CompletedTask, _ => Task.CompletedTask);
    ProcessResult result = await controller.CompleteAsync(new ProcessResult(1, false, string.Empty, string.Empty));
    Assert.Contains("never printed a readiness line", result.StandardError, StringComparison.Ordinal);
  }

  [Fact]
  [Trait("Category", "Catalog")]
  public void TheCatalogSuspendsNativeAndChromiumCellsWithALongerBound()
  {
    Scenario scenario = ScenarioCatalog.Scenario("suspend-resume");
    Assert.True(scenario.Suspends);
    Assert.Equal(TimeSpan.FromSeconds(120), scenario.Bound);
    Assert.Equal(["android", "chromium", "ios", "native"], scenario.Targets.Order(StringComparer.Ordinal));
    Assert.Equal(["firefox", "webkit"], scenario.Exclusions.Keys.Order(StringComparer.Ordinal));
    Assert.False(ScenarioCatalog.Scenario("invoke").Suspends);
  }
}
