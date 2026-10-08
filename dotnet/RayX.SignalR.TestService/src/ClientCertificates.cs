using System.Security.Cryptography.X509Certificates;

namespace RayX.SignalR.TestService;

/// <summary>
/// Test-only client-certificate authentication: the HTTPS endpoint accepts client certificates the
/// run's test CA issued, and every request of the <c>/test-mtls</c> hub (negotiate, the WebSocket
/// upgrade, Long Polling and Server-Sent Events requests) without one is refused with 403, as a
/// server that requires client certificates refuses a client without one.
/// </summary>
public static class ClientCertificates
{
  /// <summary>The hub path that requires a client certificate.</summary>
  public const string HubPath = "/test-mtls";

  /// <summary>Whether <paramref name="client"/> chains to <paramref name="authority"/> (the run's test CA).</summary>
  /// <param name="client">The presented certificate.</param>
  /// <param name="authority">The test CA.</param>
  /// <returns>Whether the certificate is accepted.</returns>
  public static bool IssuedBy(X509Certificate2 client, X509Certificate2 authority)
  {
    using X509Chain chain = new();
    chain.ChainPolicy.TrustMode = X509ChainTrustMode.CustomRootTrust;
    chain.ChainPolicy.CustomTrustStore.Add(authority);
    chain.ChainPolicy.RevocationMode = X509RevocationMode.NoCheck;
    chain.ChainPolicy.ApplicationPolicy.Add(new System.Security.Cryptography.Oid("1.3.6.1.5.5.7.3.2"));
    return chain.Build(client);
  }

  /// <summary>Refuses <c>/test-mtls</c> requests that presented no client certificate.</summary>
  /// <param name="app">The application.</param>
  /// <returns>The application.</returns>
  public static WebApplication UseRequiredClientCertificates(this WebApplication app)
  {
    app.Use(async (context, next) =>
    {
      if(context.Request.Path.StartsWithSegments(HubPath, StringComparison.OrdinalIgnoreCase)
         && context.Connection.ClientCertificate is null)
      {
        context.Response.StatusCode = StatusCodes.Status403Forbidden;
        await context.Response.WriteAsync("a client certificate is required");
        return;
      }

      await next();
    });
    return app;
  }
}
