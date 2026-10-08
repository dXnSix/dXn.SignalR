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

  /// <summary>
  /// Gets whether the child runs the client on the host (native cells, performance binaries)
  /// and so must not inherit the machine's proxy variables. Build and tool commands keep them,
  /// so they still download through the machine's proxy.
  /// </summary>
  public bool IsolateFromProxy { get; init; }

  /// <summary>Gets a display form with redacted arguments.</summary>
  public string Display => Redaction.Redact($"{FileName} {string.Join(' ', Arguments)}");
}

/// <summary>Runs child processes with a hard bound and process-tree cleanup.</summary>
public static class ProcessRunner
{
  /// <summary>
  /// The machine's proxy variables, never passed to a command that runs the client
  /// (<see cref="ProcessCommand.IsolateFromProxy"/>): the client's default system proxy would
  /// otherwise route cells through the developer's proxy, and <c>GATEWAY_INTERFACE</c> changes
  /// how it reads them.
  /// </summary>
  public static readonly IReadOnlyList<string> ProxyVariables =
  [
    "http_proxy", "HTTP_PROXY", "https_proxy", "HTTPS_PROXY", "all_proxy", "ALL_PROXY",
    "no_proxy", "NO_PROXY", "GATEWAY_INTERFACE"
  ];

  /// <summary>The start information of <paramref name="command"/>.</summary>
  /// <param name="command">The command.</param>
  /// <returns>The start information.</returns>
  public static ProcessStartInfo StartInfo(ProcessCommand command)
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

    ApplyEnvironment(startInfo.Environment, command);
    return startInfo;
  }

  /// <summary>
  /// Turns the inherited <paramref name="environment"/> into the child's: without the proxy
  /// variables when the command is isolated from the machine's proxy, then the command's own
  /// variables and plain-output settings.
  /// </summary>
  /// <param name="environment">The inherited environment, changed in place.</param>
  /// <param name="command">The command.</param>
  public static void ApplyEnvironment(IDictionary<string, string?> environment, ProcessCommand command)
  {
    if(command.IsolateFromProxy)
    {
      foreach(string variable in ProxyVariables)
      {
        environment.Remove(variable);
      }
    }

    foreach((string key, string value) in command.Environment)
    {
      environment[key] = value;
    }

    // Plain output for parsing, even when CI sets CARGO_TERM_COLOR=always.
    environment["NO_COLOR"] = "1";
    environment["CARGO_TERM_COLOR"] = "never";
    environment["FORCE_COLOR"] = "0";
    // Reused MSBuild nodes and build servers inherit the pipes and would keep them open.
    environment["MSBUILDDISABLENODEREUSE"] = "1";
    environment["DOTNET_CLI_USE_MSBUILD_SERVER"] = "0";
  }

  /// <summary>Runs <paramref name="command"/>, killing its process tree when <paramref name="timeout"/> elapses.</summary>
  /// <param name="command">The command.</param>
  /// <param name="timeout">The hard bound.</param>
  /// <param name="cancellationToken">Cancels the run and kills the process tree.</param>
  /// <returns>The captured result.</returns>
  public static Task<ProcessResult> RunAsync(
    ProcessCommand command,
    TimeSpan timeout,
    CancellationToken cancellationToken = default) =>
    RunAsync(command, timeout, null, cancellationToken);

  /// <summary>
  /// Runs <paramref name="command"/> as <see cref="RunAsync(ProcessCommand, TimeSpan, CancellationToken)"/>,
  /// showing each standard output line to <paramref name="outputLine"/> as it arrives (for example
  /// a readiness line).
  /// </summary>
  /// <param name="command">The command.</param>
  /// <param name="timeout">The hard bound.</param>
  /// <param name="outputLine">Sees each standard output line as it arrives.</param>
  /// <param name="cancellationToken">Cancels the run and kills the process tree.</param>
  /// <returns>The captured result.</returns>
  public static async Task<ProcessResult> RunAsync(
    ProcessCommand command,
    TimeSpan timeout,
    Action<string>? outputLine,
    CancellationToken cancellationToken)
  {
    ProcessStartInfo startInfo = StartInfo(command);

    using Process process = new() { StartInfo = startInfo, EnableRaisingEvents = true };
    TaskCompletionSource exited = new(TaskCreationOptions.RunContinuationsAsynchronously);
    process.Exited += (_, _) => exited.TrySetResult();
    StringBuilder output = new();
    StringBuilder error = new();
    TaskCompletionSource outputClosed = new(TaskCreationOptions.RunContinuationsAsynchronously);
    TaskCompletionSource errorClosed = new(TaskCreationOptions.RunContinuationsAsynchronously);
    process.OutputDataReceived += (_, line) =>
    {
      Append(output, line.Data, outputClosed);
      if(line.Data is not null)
      {
        outputLine?.Invoke(line.Data);
      }
    };
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
