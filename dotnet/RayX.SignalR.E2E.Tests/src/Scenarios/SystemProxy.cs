using System.Globalization;
using System.Net.Http.Json;
using System.Runtime.InteropServices;
using System.Text.Json;
using RayX.SignalR.E2E.Tests.Infrastructure;

namespace RayX.SignalR.E2E.Tests.Scenarios;

/// <summary>
/// Points the system proxy at the test proxy's credential-free listener for the <c>os-proxy</c>
/// cells and restores it afterwards: the Android emulator's global HTTP proxy (an emulator only),
/// and the host's Windows (WinINet per-user settings) or macOS (<c>networksetup</c>) proxy only on
/// runners that opt in with <c>RAYX_SIGNALR_E2E_ALLOW_SYSTEM_CHANGES=1</c>, so a developer
/// machine's settings never change.
/// </summary>
public static partial class SystemProxy
{
  /// <summary>Whether this host may change its own proxy settings for a cell.</summary>
  /// <param name="allowSystemChanges">The <c>RAYX_SIGNALR_E2E_ALLOW_SYSTEM_CHANGES</c> value.</param>
  /// <param name="windows">Whether the host runs Windows.</param>
  /// <param name="macOs">Whether the host runs macOS.</param>
  /// <returns>Whether native <c>os-proxy</c> cells may run.</returns>
  public static bool HostAllowed(string? allowSystemChanges, bool windows, bool macOs) =>
    allowSystemChanges == "1" && (windows || macOs);

  /// <summary>Whether this host may change its own proxy settings for a cell, from its environment.</summary>
  public static bool HostAllowed() => HostAllowed(
    Environment.GetEnvironmentVariable(AndroidAppCellRunner.AllowSystemChangesVariable),
    OperatingSystem.IsWindows(),
    OperatingSystem.IsMacOS());

  /// <summary>The test proxy's credential-free listener port, from its <c>/stats</c>.</summary>
  /// <param name="proxyUrl">The test proxy's URL.</param>
  /// <param name="cancellationToken">Cancels the request.</param>
  /// <returns>The port.</returns>
  public static async Task<int> OpenPortAsync(Uri proxyUrl, CancellationToken cancellationToken)
  {
    // Directly: the machine's proxy settings must not apply to this test-only request.
    using HttpClientHandler handler = new() { UseProxy = false };
    using HttpClient client = new(handler);
    JsonElement stats = await client.GetFromJsonAsync<JsonElement>(new Uri(proxyUrl, "/stats"), cancellationToken);
    return stats.GetProperty("openPort").GetInt32();
  }

  /// <summary>The <c>adb</c> arguments that set the emulator's global HTTP proxy.</summary>
  /// <param name="host">The proxy host as the emulator reaches it.</param>
  /// <param name="port">The proxy port.</param>
  /// <returns>The arguments after <c>adb -s &lt;serial&gt;</c>.</returns>
  public static string[] AndroidSetArguments(string host, int port) =>
    ["shell", "settings", "put", "global", "http_proxy", $"{host}:{port.ToString(CultureInfo.InvariantCulture)}"];

  /// <summary>The <c>adb</c> arguments that read the emulator's global HTTP proxy.</summary>
  public static readonly string[] AndroidGetArguments = ["shell", "settings", "get", "global", "http_proxy"];

  /// <summary>
  /// The <c>adb</c> arguments that restore the emulator's global HTTP proxy to
  /// <paramref name="previous"/> (what <see cref="AndroidGetArguments"/> read): its value, or
  /// <c>:0</c> (no proxy) when none was set.
  /// </summary>
  /// <param name="previous">The previous value.</param>
  /// <returns>The arguments after <c>adb -s &lt;serial&gt;</c>.</returns>
  public static string[] AndroidRestoreArguments(string previous)
  {
    string value = previous.Trim();
    return ["shell", "settings", "put", "global", "http_proxy", value is "" or "null" ? ":0" : value];
  }

  /// <summary>Points the host's proxy at <c>127.0.0.1:<paramref name="port"/></c> until disposed.</summary>
  /// <param name="port">The proxy port.</param>
  /// <param name="root">The working directory of the commands.</param>
  /// <returns>Restores the previous settings when disposed.</returns>
  public static async Task<IAsyncDisposable> ApplyToHostAsync(int port, string root)
  {
    // Never on a machine that did not opt in, whatever selected the cell.
    if(!HostAllowed())
    {
      throw new InvalidOperationException(
        $"the host proxy is changed only on Windows or macOS runners with {AndroidAppCellRunner.AllowSystemChangesVariable}=1");
    }

    if(OperatingSystem.IsWindows())
    {
      return WindowsProxy.Apply($"127.0.0.1:{port.ToString(CultureInfo.InvariantCulture)}");
    }

    if(OperatingSystem.IsMacOS())
    {
      return await MacProxy.ApplyAsync(port, root);
    }

    throw new PlatformNotSupportedException("the host proxy is changed only on Windows and macOS");
  }

  /// <summary>The network services <c>networksetup -listallnetworkservices</c> lists (disabled ones, marked <c>*</c>, skipped).</summary>
  /// <param name="output">The command's output.</param>
  /// <returns>The services.</returns>
  public static IReadOnlyList<string> MacServices(string output) =>
    [.. output.Split('\n', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries)
              .Skip(1)
              .Where(line => !line.StartsWith('*'))];

  /// <summary>The <c>networksetup</c> arguments that point a service's web proxy at the test proxy.</summary>
  /// <param name="service">The network service.</param>
  /// <param name="port">The proxy port.</param>
  /// <returns>The commands' arguments (each run with <c>sudo networksetup</c>).</returns>
  public static string[][] MacSetArguments(string service, int port) =>
  [
    ["-setwebproxy", service, "127.0.0.1", port.ToString(CultureInfo.InvariantCulture)],
    ["-setwebproxystate", service, "on"]
  ];

  /// <summary>Whether <c>networksetup -getwebproxy</c> reports the web proxy as enabled.</summary>
  /// <param name="output">The command's output.</param>
  /// <returns>Whether it is enabled.</returns>
  public static bool MacWebProxyEnabled(string output) => MacWebProxy(output).Enabled;

  /// <summary>The web proxy <c>networksetup -getwebproxy</c> reports: enabled, server and port.</summary>
  /// <param name="output">The command's output.</param>
  /// <returns>The settings.</returns>
  public static (bool Enabled, string Server, string Port) MacWebProxy(string output)
  {
    string Value(string name) => output
      .Split('\n')
      .Select(line => line.Split(':', 2))
      .Where(parts => parts.Length == 2 && parts[0].Trim().Equals(name, StringComparison.OrdinalIgnoreCase))
      .Select(parts => parts[1].Trim())
      .FirstOrDefault() ?? string.Empty;
    return (Value("Enabled").Equals("Yes", StringComparison.OrdinalIgnoreCase), Value("Server"), Value("Port"));
  }

  /// <summary>
  /// The <c>networksetup</c> arguments that restore a service's web proxy to what
  /// <see cref="MacWebProxy"/> read: its server and port (when it had one), then its state.
  /// </summary>
  /// <param name="service">The network service.</param>
  /// <param name="previous">The previous settings.</param>
  /// <returns>The commands' arguments (each run with <c>sudo networksetup</c>).</returns>
  public static string[][] MacRestoreArguments(string service, (bool Enabled, string Server, string Port) previous) =>
    previous.Server.Length > 0 && previous.Port.Length > 0
      ? [["-setwebproxy", service, previous.Server, previous.Port], ["-setwebproxystate", service, previous.Enabled ? "on" : "off"]]
      : [["-setwebproxystate", service, previous.Enabled ? "on" : "off"]];

  /// <summary>The macOS web proxy of every enabled network service, restored when disposed.</summary>
  sealed class MacProxy(string root) : IAsyncDisposable
  {
    readonly List<(string Service, (bool, string, string) Previous)> _changed = [];

    public static async Task<MacProxy> ApplyAsync(int port, string root)
    {
      MacProxy proxy = new(root);
      try
      {
        ProcessResult services = await ProcessRunner.RunAsync(
          new ProcessCommand("networksetup", ["-listallnetworkservices"], root),
          TimeSpan.FromSeconds(30));
        foreach(string service in MacServices(services.StandardOutput))
        {
          ProcessResult current = await ProcessRunner.RunAsync(
            new ProcessCommand("networksetup", ["-getwebproxy", service], root),
            TimeSpan.FromSeconds(30));
          // Recorded before the change, so a partial failure is restored too.
          proxy._changed.Add((service, MacWebProxy(current.StandardOutput)));
          foreach(string[] arguments in MacSetArguments(service, port))
          {
            await Require(await ProcessRunner.RunAsync(new ProcessCommand("sudo", ["networksetup", .. arguments], root), TimeSpan.FromSeconds(30)));
          }
        }
      }
      catch
      {
        await proxy.DisposeAsync();
        throw;
      }

      return proxy;
    }

    public async ValueTask DisposeAsync()
    {
      foreach((string service, (bool, string, string) previous) in _changed)
      {
        foreach(string[] arguments in MacRestoreArguments(service, previous))
        {
          await ProcessRunner.RunAsync(new ProcessCommand("sudo", ["networksetup", .. arguments], root), TimeSpan.FromSeconds(30));
        }
      }

      _changed.Clear();
    }

    static Task Require(ProcessResult result) =>
      result.Succeeded
        ? Task.CompletedTask
        : throw new InvalidOperationException($"networksetup failed: {Redaction.SafeTail(result.StandardError)}");
  }

  /// <summary>
  /// The current user's WinINet connection settings (what <c>WinHttpGetIEProxyConfigForCurrentUser</c>
  /// reads), set with <c>InternetSetOption</c> and restored when disposed.
  /// </summary>
  sealed partial class WindowsProxy : IAsyncDisposable
  {
    const int PerConnectionOption = 75;
    const int SettingsChanged = 39;
    const int Refresh = 37;
    const int OptionFlags = 1;
    const int OptionProxyServer = 2;
    const int OptionProxyBypass = 3;
    const int OptionAutoConfigUrl = 4;
    const int ProxyTypeDirect = 1;
    const int ProxyTypeProxy = 2;

    readonly int _flags;
    readonly string? _server;
    readonly string? _bypass;
    readonly string? _autoConfigUrl;

    WindowsProxy(int flags, string? server, string? bypass, string? autoConfigUrl)
    {
      _flags = flags;
      _server = server;
      _bypass = bypass;
      _autoConfigUrl = autoConfigUrl;
    }

    public static WindowsProxy Apply(string server)
    {
      (int flags, string? oldServer, string? bypass, string? autoConfigUrl) = Query();
      Set(ProxyTypeDirect | ProxyTypeProxy, server, string.Empty, null);
      return new WindowsProxy(flags, oldServer, bypass, autoConfigUrl);
    }

    public ValueTask DisposeAsync()
    {
      Set(_flags, _server, _bypass, _autoConfigUrl);
      return ValueTask.CompletedTask;
    }

    [StructLayout(LayoutKind.Sequential)]
    struct OptionList
    {
      public int Size;
      public IntPtr Connection;
      public int OptionCount;
      public int OptionError;
      public IntPtr Options;
    }

    /// <summary><c>INTERNET_PER_CONN_OPTION</c> on 64-bit Windows (the union follows at offset 8).</summary>
    [StructLayout(LayoutKind.Explicit)]
    struct Option
    {
      [FieldOffset(0)]
      public int Kind;

      [FieldOffset(8)]
      public int Number;

      [FieldOffset(8)]
      public IntPtr Text;
    }

    [LibraryImport("wininet.dll", EntryPoint = "InternetSetOptionW", SetLastError = true)]
    [return: MarshalAs(UnmanagedType.Bool)]
    private static partial bool InternetSetOption(IntPtr internet, int option, IntPtr buffer, int length);

    [LibraryImport("wininet.dll", EntryPoint = "InternetQueryOptionW", SetLastError = true)]
    [return: MarshalAs(UnmanagedType.Bool)]
    private static partial bool InternetQueryOption(IntPtr internet, int option, IntPtr buffer, ref int length);

    [LibraryImport("kernel32.dll")]
    private static partial IntPtr GlobalFree(IntPtr memory);

    static (int Flags, string? Server, string? Bypass, string? AutoConfigUrl) Query()
    {
      Option[] options = [new() { Kind = OptionFlags }, new() { Kind = OptionProxyServer }, new() { Kind = OptionProxyBypass }, new() { Kind = OptionAutoConfigUrl }];
      return WithList(options, list =>
      {
        int length = Marshal.SizeOf<OptionList>();
        if(!InternetQueryOption(IntPtr.Zero, PerConnectionOption, list, ref length))
        {
          throw new InvalidOperationException($"InternetQueryOption failed: {Marshal.GetLastPInvokeError()}");
        }

        OptionList read = Marshal.PtrToStructure<OptionList>(list);
        Option[] values = [.. Enumerable.Range(0, read.OptionCount).Select(index => Marshal.PtrToStructure<Option>(read.Options + (index * Marshal.SizeOf<Option>())))];
        string? Text(int index)
        {
          IntPtr text = values[index].Text;
          if(text == IntPtr.Zero)
          {
            return null;
          }

          string value = Marshal.PtrToStringUni(text) ?? string.Empty;
          GlobalFree(text);
          return value;
        }

        return (values[0].Number, Text(1), Text(2), Text(3));
      });
    }

    static void Set(int flags, string? server, string? bypass, string? autoConfigUrl)
    {
      IntPtr[] texts = [Marshal.StringToHGlobalUni(server), Marshal.StringToHGlobalUni(bypass), Marshal.StringToHGlobalUni(autoConfigUrl)];
      try
      {
        Option[] options =
        [
          new() { Kind = OptionFlags, Number = flags },
          new() { Kind = OptionProxyServer, Text = texts[0] },
          new() { Kind = OptionProxyBypass, Text = texts[1] },
          new() { Kind = OptionAutoConfigUrl, Text = texts[2] }
        ];
        WithList(options, list =>
        {
          if(!InternetSetOption(IntPtr.Zero, PerConnectionOption, list, Marshal.SizeOf<OptionList>()))
          {
            throw new InvalidOperationException($"InternetSetOption failed: {Marshal.GetLastPInvokeError()}");
          }

          InternetSetOption(IntPtr.Zero, SettingsChanged, IntPtr.Zero, 0);
          InternetSetOption(IntPtr.Zero, Refresh, IntPtr.Zero, 0);
          return 0;
        });
      }
      finally
      {
        foreach(IntPtr text in texts)
        {
          Marshal.FreeHGlobal(text);
        }
      }
    }

    static T WithList<T>(Option[] options, Func<IntPtr, T> use)
    {
      int optionSize = Marshal.SizeOf<Option>();
      IntPtr array = Marshal.AllocHGlobal(optionSize * options.Length);
      IntPtr list = Marshal.AllocHGlobal(Marshal.SizeOf<OptionList>());
      try
      {
        for(int index = 0; index < options.Length; index++)
        {
          Marshal.StructureToPtr(options[index], array + (index * optionSize), false);
        }

        Marshal.StructureToPtr(
          new OptionList { Size = Marshal.SizeOf<OptionList>(), Connection = IntPtr.Zero, OptionCount = options.Length, Options = array },
          list,
          false);
        return use(list);
      }
      finally
      {
        Marshal.FreeHGlobal(list);
        Marshal.FreeHGlobal(array);
      }
    }
  }
}
