using System.Net;
using System.Net.Security;
using System.Net.Sockets;
using System.Security.Cryptography.X509Certificates;

namespace dXn.SignalR.TestService;

/// <summary>
/// Test-only: accepts TLS connections on a loopback port the operating system chooses and presents
/// a certificate that a second, untrusted CA issued, so a client that trusts only the run's test CA
/// must reject it. Nothing is served after the handshake. The emulator reaches it through its host
/// alias <c>10.0.2.2</c>.
/// </summary>
/// <param name="configuration">The configuration (<c>TestTls:UntrustedCertificatePath</c>, <c>TestTls:UntrustedKeyPath</c>).</param>
public sealed class UntrustedTlsListener(IConfiguration configuration) : BackgroundService
{
  readonly TaskCompletionSource<int> _port = new(TaskCreationOptions.RunContinuationsAsynchronously);

  /// <summary>Waits until the listener is bound and returns its port, or 0 when no untrusted certificate was configured.</summary>
  /// <param name="cancellationToken">Cancels the wait.</param>
  /// <returns>The port.</returns>
  public Task<int> PortAsync(CancellationToken cancellationToken) => _port.Task.WaitAsync(cancellationToken);

  /// <inheritdoc/>
  protected override async Task ExecuteAsync(CancellationToken stoppingToken)
  {
    if(configuration["TestTls:UntrustedCertificatePath"] is not { } certificatePath
       || configuration["TestTls:UntrustedKeyPath"] is not { } keyPath)
    {
      _port.TrySetResult(0);
      return;
    }

    using X509Certificate2 pem = X509Certificate2.CreateFromPemFile(certificatePath, keyPath);
    using X509Certificate2 certificate = X509CertificateLoader.LoadPkcs12(pem.Export(X509ContentType.Pkcs12), null);
    TcpListener listener = new(IPAddress.Loopback, 0);
    listener.Start();
    _port.TrySetResult(((IPEndPoint)listener.LocalEndpoint).Port);
    try
    {
      while(!stoppingToken.IsCancellationRequested)
      {
        TcpClient client = await listener.AcceptTcpClientAsync(stoppingToken);
        _ = HandshakeAsync(client, certificate, stoppingToken);
      }
    }
    catch(OperationCanceledException)
    {
      // Stopping.
    }
    finally
    {
      listener.Stop();
    }
  }

  static async Task HandshakeAsync(TcpClient client, X509Certificate2 certificate, CancellationToken stoppingToken)
  {
    using(client)
    {
      try
      {
        using CancellationTokenSource bound = CancellationTokenSource.CreateLinkedTokenSource(stoppingToken);
        bound.CancelAfter(TimeSpan.FromSeconds(30));
        await using SslStream tls = new(client.GetStream());
        await tls.AuthenticateAsServerAsync(
          new SslServerAuthenticationOptions { ServerCertificate = certificate },
          bound.Token);
      }
      catch(Exception exception) when (exception is IOException or System.Security.Authentication.AuthenticationException or OperationCanceledException)
      {
        // The client rejected the certificate, as it should.
      }
    }
  }
}
