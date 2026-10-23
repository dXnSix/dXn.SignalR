using System.Text.RegularExpressions;

namespace dXn.SignalR.E2E.Tests.Infrastructure;

/// <summary>Removes credentials from diagnostics and bounds their size.</summary>
public static partial class Redaction
{
  /// <summary>The maximum number of characters kept from one diagnostic stream.</summary>
  public const int MaximumCharacters = 32 * 1024;

  /// <summary>
  /// Replaces <c>access_token=</c>, JSON <c>"accessToken"</c> (negotiate redirects),
  /// <c>Authorization</c> and <c>Bearer</c> values, <c>*_CREDENTIALS=</c> and <c>*_KEY_PEM_BASE64=</c> values and the
  /// <c>user:password@</c> of URLs (proxy credentials).
  /// </summary>
  /// <param name="value">The text.</param>
  /// <returns>The redacted text.</returns>
  public static string Redact(string? value)
  {
    if(string.IsNullOrEmpty(value))
    {
      return string.Empty;
    }

    string redacted = AccessToken().Replace(value, "$1[REDACTED]");
    redacted = JsonAccessToken().Replace(redacted, "$1[REDACTED]");
    redacted = AuthorizationHeader().Replace(redacted, "$1[REDACTED]");
    redacted = CredentialsVariable().Replace(redacted, "$1[REDACTED]");
    redacted = UrlUserInfo().Replace(redacted, "$1[REDACTED]@");
    return BearerToken().Replace(redacted, "$1[REDACTED]");
  }

  /// <summary>Keeps the last <paramref name="maximum"/> characters.</summary>
  /// <param name="value">The text.</param>
  /// <param name="maximum">The bound.</param>
  /// <returns>The bounded tail.</returns>
  public static string Tail(string? value, int maximum = MaximumCharacters)
  {
    if(string.IsNullOrEmpty(value))
    {
      return string.Empty;
    }

    return value.Length <= maximum ? value : value[^maximum..];
  }

  /// <summary>Redacts and bounds a diagnostic stream.</summary>
  /// <param name="value">The text.</param>
  /// <returns>The safe tail.</returns>
  public static string SafeTail(string? value) => Tail(Redact(value));

  [GeneratedRegex(@"(?i)(access_token=)[^&\s""']+")]
  private static partial Regex AccessToken();

  [GeneratedRegex(@"(?i)(""access_?token""\s*:\s*"")[^""]*")]
  private static partial Regex JsonAccessToken();

  [GeneratedRegex(@"(?i)(authorization""?\s*[:=]\s*""?)[^\r\n""]+")]
  private static partial Regex AuthorizationHeader();

  [GeneratedRegex(@"(?i)(bearer\s+)[A-Za-z0-9._~+/=-]+")]
  private static partial Regex BearerToken();

  [GeneratedRegex(@"(?i)((?:_CREDENTIALS|_KEY_PEM_BASE64)\s*=\s*['""]?)[^'""\s]+")]
  private static partial Regex CredentialsVariable();

  [GeneratedRegex(@"(?i)([a-z][a-z0-9+.-]*://)[^/\s@:'""]+:[^/\s@'""]+@")]
  private static partial Regex UrlUserInfo();
}
