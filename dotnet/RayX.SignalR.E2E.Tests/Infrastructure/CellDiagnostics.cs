using System.Text.Json;

namespace RayX.SignalR.E2E.Tests.Infrastructure;

/// <summary>Writes bounded, redacted failure diagnostics under <c>artifacts-temp/e2e</c>.</summary>
public static class CellDiagnostics
{
  static readonly JsonSerializerOptions _json = new() { WriteIndented = true };

  /// <summary>Gets the diagnostics directory.</summary>
  /// <param name="repositoryRoot">The repository root.</param>
  /// <returns>The directory.</returns>
  public static string Directory(string repositoryRoot) => Path.Combine(repositoryRoot, "artifacts-temp", "e2e");

  /// <summary>Writes one failure record and returns its path.</summary>
  /// <param name="environment">The AppHost environment whose resource logs are attached.</param>
  /// <param name="fileName">The file-name-safe cell name.</param>
  /// <param name="message">The failure message.</param>
  /// <param name="command">The executed command, when any.</param>
  /// <param name="result">The process result, when any.</param>
  /// <returns>The written path.</returns>
  public static async Task<string> WriteAsync(
    AspireEnvironment environment,
    string fileName,
    string message,
    ProcessCommand? command,
    ProcessResult? result)
  {
    string directory = Directory(environment.RepositoryRoot);
    System.IO.Directory.CreateDirectory(directory);
    string path = Path.Combine(directory, $"{fileName}.json");
    var record = new
    {
      SchemaVersion = 1,
      Cell = fileName,
      Message = Redaction.Redact(message),
      Command = command?.Display,
      result?.ExitCode,
      result?.TimedOut,
      StdoutTail = Redaction.SafeTail(result?.StandardOutput),
      StderrTail = Redaction.SafeTail(result?.StandardError),
      Resources = await environment.AllResourceLogsAsync()
    };
    await File.WriteAllTextAsync(path, JsonSerializer.Serialize(record, _json));
    return path;
  }
}
