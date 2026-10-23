using System.IO.Pipelines;
using System.Net;
using System.Net.Security;
using System.Net.Sockets;
using System.Security.Cryptography.X509Certificates;
using dXn.SignalR.TestProxy;

// Test-only HTTP CONNECT and SOCKS5 proxy (also behind TLS, for `https://` proxies) for the proxy
// end-to-end scenarios:
// every connection to this Aspire-allocated port is handled raw (no ASP.NET Core pipeline), so it
// can open tunnels (a first byte of 5 is a SOCKS5 greeting); it also answers `GET /health` and
// `GET /stats` (the tunnel and SOCKS counters, the local ports of its tunnels, which the service
// then sees as peers, and the address types of the SOCKS requests).
WebApplicationBuilder builder = WebApplication.CreateBuilder(args);
using ProxyState state = ProxyState.From(builder.Configuration);
builder.WebHost.ConfigureKestrel(
  kestrel => kestrel.ConfigureEndpointDefaults(
    listen => listen.Use(_ => connection => ProxyConnection.HandleAsync(connection, state))));
WebApplication app = builder.Build();

// PAC scripts cannot carry credentials, so their routes use a second, credential-free listener
// on a dynamic loopback port (reported in the scripts; the Android emulator reaches it as 10.0.2.2).
using TcpListener open = new(IPAddress.Loopback, 0);
open.Start();
state.OpenPort = ((IPEndPoint)open.LocalEndpoint).Port;
_ = AcceptOpenAsync(open, state, app.Lifetime.ApplicationStopping);

// `https://` proxies: the same proxy (with the run's credentials) behind TLS with the run's test
// certificate, on a dynamic loopback port (the Android emulator reaches it as 10.0.2.2).
using TcpListener secure = new(IPAddress.Loopback, 0);
if(builder.Configuration["TestProxy:CertificatePath"] is { } certificatePath
   && builder.Configuration["TestProxy:KeyPath"] is { } keyPath)
{
  using X509Certificate2 pem = X509Certificate2.CreateFromPemFile(certificatePath, keyPath);
  X509Certificate2 certificate = X509CertificateLoader.LoadPkcs12(pem.Export(X509ContentType.Pkcs12), null);
  secure.Start();
  state.TlsPort = ((IPEndPoint)secure.LocalEndpoint).Port;
  _ = AcceptTlsAsync(secure, certificate, state, app.Lifetime.ApplicationStopping);
}

await app.RunAsync();

static async Task AcceptTlsAsync(TcpListener listener, X509Certificate2 certificate, ProxyState state, CancellationToken stopping)
{
  while(!stopping.IsCancellationRequested)
  {
    TcpClient client;
    try
    {
      client = await listener.AcceptTcpClientAsync(stopping);
    }
    catch(OperationCanceledException)
    {
      return;
    }

    _ = Task.Run(
      async () =>
      {
        using(client)
        {
          try
          {
            await using SslStream tls = new(client.GetStream());
            using CancellationTokenSource handshake = CancellationTokenSource.CreateLinkedTokenSource(stopping);
            handshake.CancelAfter(TimeSpan.FromSeconds(30));
            await tls.AuthenticateAsServerAsync(new SslServerAuthenticationOptions { ServerCertificate = certificate }, handshake.Token);
            state.TlsSession();
            await ProxyConnection.HandleAsync(PipeReader.Create(tls), PipeWriter.Create(tls), state, open: false, stopping);
          }
          catch(Exception exception) when (exception is IOException or System.Security.Authentication.AuthenticationException or OperationCanceledException)
          {
            // A client that rejected the certificate, or a tunnel that ended.
          }
        }
      },
      stopping);
  }
}

static async Task AcceptOpenAsync(TcpListener listener, ProxyState state, CancellationToken stopping)
{
  while(!stopping.IsCancellationRequested)
  {
    TcpClient client;
    try
    {
      client = await listener.AcceptTcpClientAsync(stopping);
    }
    catch(OperationCanceledException)
    {
      return;
    }

    _ = Task.Run(
      async () =>
      {
        using(client)
        {
          NetworkStream stream = client.GetStream();
          await ProxyConnection.HandleAsync(PipeReader.Create(stream), PipeWriter.Create(stream), state, open: true, stopping);
        }
      },
      stopping);
  }
}
