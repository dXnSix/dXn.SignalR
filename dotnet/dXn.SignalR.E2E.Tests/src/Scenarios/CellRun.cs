using dXn.SignalR.E2E.Tests.Infrastructure;

namespace dXn.SignalR.E2E.Tests.Scenarios;

/// <summary>The verdict of one cell, including where failure diagnostics were written.</summary>
/// <param name="Passed">Whether the cell passed.</param>
/// <param name="Message">The failure message, empty on success.</param>
/// <param name="DiagnosticsPath">The diagnostics file, when one was written.</param>
public sealed record CellReport(bool Passed, string Message, string? DiagnosticsPath);

/// <summary>Runs a cell and writes diagnostics for every failure mode, including failures before the scenario starts.</summary>
public static class CellRun
{
  /// <summary>Starts the environment, runs the cell and records failures.</summary>
  /// <param name="environment">The AppHost environment.</param>
  /// <param name="cell">The cell.</param>
  /// <param name="start">Starts the AppHost with the capabilities the cell needs.</param>
  /// <param name="run">Runs the scenario.</param>
  /// <param name="diagnosticsName">Overrides the diagnostics file name.</param>
  /// <returns>The report.</returns>
  public static async Task<CellReport> ExecuteAsync(
    AspireEnvironment environment,
    E2ECell cell,
    Func<Task> start,
    Func<Task<CellOutcome>> run,
    string? diagnosticsName = null)
  {
    string name = diagnosticsName ?? cell.FileName;
    try
    {
      await start();
      CellOutcome outcome = await run();
      if(outcome.Passed)
      {
        return new CellReport(true, string.Empty, null);
      }

      string message = $"{cell.Id} {outcome.Failure}";
      string path = await CellDiagnostics.WriteAsync(environment, name, message, outcome.Command, outcome.Result);
      return new CellReport(
        false,
        $"{message}. Diagnostics: {path}\n{Redaction.Tail(Redaction.Redact(outcome.Result.CombinedOutput), 8000)}",
        path);
    }
    catch(Exception exception) when(exception is not OutOfMemoryException)
    {
      string message = $"{cell.Id} failed before or while running its scenario: {exception}";
      string path = await CellDiagnostics.WriteAsync(environment, name, message, command: null, result: null);
      return new CellReport(false, $"{Redaction.Redact(message)}. Diagnostics: {path}", path);
    }
  }
}
