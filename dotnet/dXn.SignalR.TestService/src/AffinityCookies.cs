using System.Collections.Concurrent;

namespace dXn.SignalR.TestService;

/// <summary>
/// Test-only load-balancer affinity on <c>/test-affinity</c>: every negotiate response sets an
/// affinity cookie, and every other request of the hub (the WebSocket upgrade, Long Polling and
/// Server-Sent Events requests) without an issued cookie is refused with 403, as a load balancer
/// that routes by cookie refuses a request it cannot route.
/// </summary>
public static class AffinityCookies
{
  /// <summary>The cookie's name.</summary>
  public const string Name = "dXnAffinity";

  /// <summary>The hub path the affinity applies to.</summary>
  public const string HubPath = "/test-affinity";

  static readonly ConcurrentDictionary<string, bool> _issued = new(StringComparer.Ordinal);
  static int _rejections;

  /// <summary>Gets how many requests arrived without an issued cookie since the last reset.</summary>
  public static int Rejections => Volatile.Read(ref _rejections);

  /// <summary>Clears the issued cookies and the rejection count (per cell).</summary>
  public static void Reset()
  {
    _issued.Clear();
    Interlocked.Exchange(ref _rejections, 0);
  }

  /// <summary>Adds the affinity gate to the pipeline.</summary>
  /// <param name="app">The application.</param>
  /// <returns>The application.</returns>
  public static WebApplication UseAffinityCookies(this WebApplication app)
  {
    app.Use(async (context, next) =>
    {
      if(!context.Request.Path.StartsWithSegments(HubPath, StringComparison.OrdinalIgnoreCase))
      {
        await next();
        return;
      }

      if(HttpMethods.IsPost(context.Request.Method)
         && context.Request.Path.Value?.EndsWith("/negotiate", StringComparison.OrdinalIgnoreCase) == true)
      {
        string value = Guid.NewGuid().ToString("N");
        _issued[value] = true;
        context.Response.Cookies.Append(Name, value, new CookieOptions { Path = HubPath, HttpOnly = true });
        await next();
        return;
      }

      if(context.Request.Cookies[Name] is not { } cookie || !_issued.ContainsKey(cookie))
      {
        Interlocked.Increment(ref _rejections);
        context.Response.StatusCode = StatusCodes.Status403Forbidden;
        await context.Response.WriteAsync("the affinity cookie is missing");
        return;
      }

      await next();
    });
    return app;
  }
}
