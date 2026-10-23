using Microsoft.AspNetCore.Authentication.Negotiate;
using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.SignalR;

namespace dXn.SignalR.TestService;

/// <summary>
/// A hub behind ASP.NET Core Negotiate authentication (Kerberos or NTLM with the caller's Windows
/// account), for the <c>windows-auth</c> scenario on Windows hosts.
/// </summary>
[Authorize(AuthenticationSchemes = NegotiateDefaults.AuthenticationScheme)]
public sealed class WindowsAuthHub : Hub
{
  /// <summary>The authenticated account (<c>DOMAIN\user</c>).</summary>
  /// <returns>The name.</returns>
  public string Identity() => Context.User?.Identity?.Name ?? string.Empty;

  /// <summary>The scheme that authenticated the caller (<c>Kerberos</c>, <c>NTLM</c> or <c>Negotiate</c>).</summary>
  /// <returns>The authentication type.</returns>
  public string AuthenticationType() => Context.User?.Identity?.AuthenticationType ?? string.Empty;

  /// <summary>Echoes a value.</summary>
  /// <param name="value">The value.</param>
  /// <returns><paramref name="value"/>.</returns>
  public int Echo(int value) => value;
}
