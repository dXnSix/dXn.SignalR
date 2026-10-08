using System.Diagnostics;
using System.Globalization;
using System.Runtime.InteropServices;
using System.Text.RegularExpressions;
using RayX.SignalR.E2E.Tests.Infrastructure;

namespace RayX.SignalR.E2E.Tests.Scenarios;

/// <summary>A suspension a scenario asked for: the process and how long it stays suspended.</summary>
/// <param name="Process">The process id.</param>
/// <param name="Duration">How long the process stays suspended.</param>
public sealed record SuspensionRequest(int Process, TimeSpan Duration);

/// <summary>
/// The test-only suspension controls of the <c>suspend-resume</c> scenario (R9.C2): each time a
/// scenario process prints a readiness line, the runner suspends the process the line names for
/// the duration it names, then resumes it. Browsers freeze the page from the Playwright spec
/// instead.
/// </summary>
public static partial class Suspension
{
  /// <summary>The marker of the line a scenario prints when it is ready to be suspended.</summary>
  public const string ReadyMarker = "RAYX_SIGNALR_SUSPEND_READY";

  /// <summary>The longest suspension a scenario may ask for (cells are bounded at 120 seconds).</summary>
  public static readonly TimeSpan Longest = TimeSpan.FromSeconds(60);

  /// <summary>
  /// Reads a readiness line, which ends with
  /// <c>RAYX_SIGNALR_SUSPEND_READY pid=&lt;pid&gt; seconds=&lt;n&gt;</c> (libtest may have printed the
  /// test name before it).
  /// </summary>
  /// <param name="line">One output line.</param>
  /// <returns>The request, or <see langword="null"/> when the line is not a valid readiness line.</returns>
  public static SuspensionRequest? Request(string line)
  {
    Match match = ReadyLine().Match(line);
    return match.Success
           && int.TryParse(match.Groups[1].Value, NumberStyles.None, CultureInfo.InvariantCulture, out int pid)
           && int.TryParse(match.Groups[2].Value, NumberStyles.None, CultureInfo.InvariantCulture, out int seconds)
           && seconds > 0
           && TimeSpan.FromSeconds(seconds) <= Longest
      ? new SuspensionRequest(pid, TimeSpan.FromSeconds(seconds))
      : null;
  }

  /// <summary>Suspends host processes: <c>NtSuspendProcess</c> on Windows, <c>SIGSTOP</c>/<c>SIGCONT</c> elsewhere.</summary>
  /// <returns>The controller.</returns>
  public static SuspensionController Host() =>
    OperatingSystem.IsWindows()
      ? new SuspensionController(pid => Windows(pid, NativeMethods.NtSuspendProcess), pid => Windows(pid, NativeMethods.NtResumeProcess))
      : new SuspensionController(pid => Signal("STOP", pid), pid => Signal("CONT", pid));

  /// <summary>Suspends processes on an Android device with <c>kill -STOP</c>/<c>-CONT</c> over <c>adb shell</c>.</summary>
  /// <param name="adb">The <c>adb</c> executable.</param>
  /// <param name="serial">The device serial.</param>
  /// <returns>The controller.</returns>
  public static SuspensionController Android(string adb, string serial) =>
    new(
      pid => Run(new ProcessCommand(adb, ["-s", serial, "shell", $"kill -STOP {pid}"], Directory.GetCurrentDirectory())),
      pid => Run(new ProcessCommand(adb, ["-s", serial, "shell", $"kill -CONT {pid}"], Directory.GetCurrentDirectory())));

  static Task Windows(int pid, Func<IntPtr, int> operation)
  {
    using Process process = Process.GetProcessById(pid);
    int status = operation(process.Handle);
    return status == 0
      ? Task.CompletedTask
      : throw new InvalidOperationException($"ntdll returned NTSTATUS 0x{status:X8} for process {pid}.");
  }

  /// <summary>Sends <c>SIGSTOP</c> or <c>SIGCONT</c> to <paramref name="pid"/> through libc's <c>kill</c>.</summary>
  /// <param name="signal"><c>STOP</c> or <c>CONT</c>.</param>
  /// <param name="pid">The process.</param>
  /// <returns>A completed task.</returns>
  static Task Signal(string signal, int pid)
  {
    // The signal numbers differ: Darwin STOP 17 and CONT 19, Linux STOP 19 and CONT 18.
    int number = (signal, OperatingSystem.IsMacOS() || OperatingSystem.IsIOS()) switch
    {
      ("STOP", true) => 17,
      ("CONT", true) => 19,
      ("STOP", false) => 19,
      ("CONT", false) => 18,
      _ => throw new ArgumentException($"Unsupported signal {signal}.", nameof(signal))
    };
    if(NativeMethods.Kill(pid, number) != 0)
    {
      throw new InvalidOperationException(
        $"kill({pid}, SIG{signal}) failed with errno {Marshal.GetLastPInvokeError()}.");
    }

    return Task.CompletedTask;
  }

  static async Task Run(ProcessCommand command)
  {
    ProcessResult result = await ProcessRunner.RunAsync(command, TimeSpan.FromSeconds(30));
    if(!result.Succeeded)
    {
      throw new InvalidOperationException(
        $"{command.Display} failed with exit code {result.ExitCode}: {Redaction.SafeTail(result.CombinedOutput)}");
    }
  }

  [GeneratedRegex(@"RAYX_SIGNALR_SUSPEND_READY pid=(\d+) seconds=(\d+)\s*$")]
  private static partial Regex ReadyLine();

  static partial class NativeMethods
  {
    [LibraryImport("libc", EntryPoint = "kill", SetLastError = true)]
    internal static partial int Kill(int pid, int signal);

#pragma warning disable SYSLIB1054 // Blittable signatures; LibraryImport would need unsafe code in this test project.
    [DllImport("ntdll.dll")]
    internal static extern int NtSuspendProcess(IntPtr processHandle);

    [DllImport("ntdll.dll")]
    internal static extern int NtResumeProcess(IntPtr processHandle);
#pragma warning restore SYSLIB1054
  }
}

/// <summary>
/// Watches a scenario's output and, at each readiness line, suspends the process the line names
/// for the duration it names and resumes it, one suspension after the other, recording what it
/// did for the cell's diagnostics.
/// </summary>
/// <param name="suspend">Suspends a process.</param>
/// <param name="resume">Resumes a process.</param>
public sealed class SuspensionController(Func<int, Task> suspend, Func<int, Task> resume)
{
  readonly List<string> _notes = [];
  Task _run = Task.CompletedTask;

  /// <summary>Handles one output line of the scenario process.</summary>
  /// <param name="line">The line.</param>
  public void OnOutputLine(string line)
  {
    if(!line.Contains(Suspension.ReadyMarker, StringComparison.Ordinal))
    {
      return;
    }

    if(Suspension.Request(line) is not SuspensionRequest request)
    {
      Note($"runner: ignored an invalid readiness line ({line.Trim()})");
      return;
    }

    lock(_notes)
    {
      Task previous = _run;
      _run = Task.Run(async () =>
      {
        await previous;
        await SuspendAsync(request);
      });
    }
  }

  /// <summary>Waits for the suspensions to end and adds their notes to <paramref name="result"/>.</summary>
  /// <param name="result">The scenario's process result.</param>
  /// <returns>The result with the runner's suspension notes in its standard error.</returns>
  public async Task<ProcessResult> CompleteAsync(ProcessResult result)
  {
    Task run;
    lock(_notes)
    {
      run = _run;
    }

    await run;
    lock(_notes)
    {
      string notes = _notes.Count == 0
        ? "runner: the scenario never printed a readiness line, so it was not suspended"
        : string.Join('\n', _notes);
      return result with { StandardError = $"{result.StandardError}\n{notes}" };
    }
  }

  async Task SuspendAsync(SuspensionRequest request)
  {
    Stopwatch suspended = Stopwatch.StartNew();
    try
    {
      await suspend(request.Process);
      Note($"runner: suspended process {request.Process} for {request.Duration.TotalSeconds:0} s");
      await Task.Delay(request.Duration);
    }
    catch(Exception exception) when (exception is InvalidOperationException or ArgumentException or System.ComponentModel.Win32Exception)
    {
      Note($"runner: suspending process {request.Process} failed: {exception.Message}");
    }
    finally
    {
      try
      {
        await resume(request.Process);
        Note($"runner: resumed process {request.Process} after {suspended.Elapsed.TotalSeconds:0.0} s");
      }
      catch(Exception exception) when (exception is InvalidOperationException or ArgumentException or System.ComponentModel.Win32Exception)
      {
        // The process may have ended (for example killed at the cell bound).
        Note($"runner: resuming process {request.Process} failed: {exception.Message}");
      }
    }
  }

  void Note(string note)
  {
    lock(_notes)
    {
      _notes.Add($"{DateTime.UtcNow:HH:mm:ss.fff} {note}");
    }
  }
}
