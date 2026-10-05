using System.Diagnostics;
using System.Text;

namespace RayX.SignalR.E2E.Tests.Infrastructure;

/// <summary>The captured outcome of one bounded child process.</summary>
/// <param name="ExitCode">The exit code, or <c>124</c> when the process timed out.</param>
/// <param name="TimedOut">Whether the bound elapsed and the process tree was killed.</param>
/// <param name="StandardOutput">The captured standard output.</param>
/// <param name="StandardError">The captured standard error.</param>
public sealed record ProcessResult(int ExitCode, bool TimedOut, string StandardOutput, string StandardError)
{
  /// <summary>Gets standard output followed by standard error.</summary>
  public string CombinedOutput => $"{StandardOutput}\n{StandardError}";

  /// <summary>Gets whether the process exited successfully within its bound.</summary>
  public bool Succeeded => !TimedOut && ExitCode == 0;
}

/// <summary>A child-process invocation.</summary>
/// <param name="FileName">The executable.</param>
/// <param name="Arguments">The arguments.</param>
/// <param name="WorkingDirectory">The working directory.</param>
public sealed record ProcessCommand(string FileName, IReadOnlyList<string> Arguments, string WorkingDirectory)
{
  /// <summary>Gets additional environment variables for the child.</summary>
  public IReadOnlyDictionary<string, string> Environment { get; init; } = new Dictionary<string, string>();

  /// <summary>Gets a display form with redacted arguments.</summary>
  public string Display => Redaction.Redact($"{FileName} {string.Join(' ', Arguments)}");
}

/// <summary>Runs child processes with a hard bound and process-tree cleanup.</summary>
public static class ProcessRunner
{
  /// <summary>Runs <paramref name="command"/>, killing its process tree when <paramref name="timeout"/> elapses.</summary>
  /// <param name="command">The command.</param>
  /// <param name="timeout">The hard bound.</param>
  /// <param name="cancellationToken">Cancels the run and kills the process tree.</param>
  /// <returns>The captured result.</returns>
  public static async Task<ProcessResult> RunAsync(
    ProcessCommand command,
    TimeSpan timeout,
    CancellationToken cancellationToken = default)
  {
    ProcessStartInfo startInfo = new(command.FileName)
    {
      WorkingDirectory = command.WorkingDirectory,
      UseShellExecute = false,
      CreateNoWindow = true,
      RedirectStandardOutput = true,
      RedirectStandardError = true,
      RedirectStandardInput = false,
      StandardOutputEncoding = Encoding.UTF8,
      StandardErrorEncoding = Encoding.UTF8
    };
    foreach(string argument in command.Arguments)
    {
      startInfo.ArgumentList.Add(argument);
    }

    foreach((string key, string value) in command.Environment)
    {
      startInfo.Environment[key] = value;
    }

    startInfo.Environment["NO_COLOR"] = "1";
    startInfo.Environment["FORCE_COLOR"] = "0";
    // Reused MSBuild nodes and build servers inherit the pipes and would keep them open.
    startInfo.Environment["MSBUILDDISABLENODEREUSE"] = "1";
    startInfo.Environment["DOTNET_CLI_USE_MSBUILD_SERVER"] = "0";

    using Process process = new() { StartInfo = startInfo, EnableRaisingEvents = true };
    TaskCompletionSource exited = new(TaskCreationOptions.RunContinuationsAsynchronously);
    process.Exited += (_, _) => exited.TrySetResult();
    StringBuilder output = new();
    StringBuilder error = new();
    TaskCompletionSource outputClosed = new(TaskCreationOptions.RunContinuationsAsynchronously);
    TaskCompletionSource errorClosed = new(TaskCreationOptions.RunContinuationsAsynchronously);
    process.OutputDataReceived += (_, line) => Append(output, line.Data, outputClosed);
    process.ErrorDataReceived += (_, line) => Append(error, line.Data, errorClosed);
    if(!process.Start())
    {
      throw new InvalidOperationException($"Failed to start {command.Display}");
    }

    process.BeginOutputReadLine();
    process.BeginErrorReadLine();
    using CancellationTokenSource bound = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
    bound.CancelAfter(timeout);
    // Wait for the process itself, not for pipe EOF: descendants may outlive it holding the pipes.
    bool timedOut = false;
    try
    {
      await exited.Task.WaitAsync(bound.Token);
    }
    catch(OperationCanceledException)
    {
      timedOut = true;
      KillTree(process);
      await Task.WhenAny(exited.Task, Task.Delay(TimeSpan.FromSeconds(15), CancellationToken.None));
    }

    // Grandchildren may briefly hold the pipes; never wait for them indefinitely.
    await Task.WhenAny(Task.WhenAll(outputClosed.Task, errorClosed.Task), Task.Delay(TimeSpan.FromSeconds(5), CancellationToken.None));
    lock(output)
    {
      lock(error)
      {
        return new ProcessResult(
          timedOut || !process.HasExited ? 124 : process.ExitCode,
          timedOut,
          output.ToString(),
          error.ToString());
      }
    }
  }

  /// <summary>Kills a process and all of its descendants, ignoring processes that already exited.</summary>
  /// <param name="process">The root process.</param>
  public static void KillTree(Process process)
  {
    try
    {
      if(!process.HasExited)
      {
        process.Kill(entireProcessTree: true);
      }
    }
    catch(InvalidOperationException)
    {
      // Already exited.
    }
    catch(System.ComponentModel.Win32Exception)
    {
      // Already exiting.
    }
  }

  static void Append(StringBuilder target, string? line, TaskCompletionSource closed)
  {
    if(line is null)
    {
      closed.TrySetResult();
      return;
    }

    lock(target)
    {
      target.AppendLine(line);
    }
  }
}
