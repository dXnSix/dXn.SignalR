using System.Net.Quic;

namespace RayX.SignalR.AppHost;

/// <summary>
/// Whether the host can serve and reach HTTP/3: Kestrel's QUIC transport needs msquic, which
/// Windows 11 and Windows Server 2022 ship with .NET and Linux gets from <c>libmsquic</c>; macOS
/// has none, so the <c>http3</c> scenario is not selected there.
/// </summary>
public static class QuicSupport
{
  /// <summary>The reason the <c>http3</c> scenario needs a QUIC-capable host.</summary>
  public const string Reason =
    "Kestrel serves HTTP/3 only through msquic (Windows 11 or Windows Server 2022, Linux with libmsquic); macOS hosts have none";

  /// <summary>Gets whether msquic is available on this host.</summary>
  public static bool IsSupported =>
    (OperatingSystem.IsWindows() || OperatingSystem.IsLinux() || OperatingSystem.IsMacOS())
    && QuicListener.IsSupported;
}
