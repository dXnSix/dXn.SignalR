using System.Net;
using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;

namespace dXn.SignalR.AppHost;

/// <summary>The files of one generated test certificate authority and server certificate.</summary>
/// <param name="Directory">The directory holding the files.</param>
public sealed record TestCertificateFiles(string Directory)
{
  /// <summary>Gets the CA certificate (PEM), which clients add as an extra trust root.</summary>
  public string CaCertificate => Path.Combine(Directory, "ca.pem");

  /// <summary>Gets the server certificate (PEM).</summary>
  public string ServerCertificate => Path.Combine(Directory, "server.pem");

  /// <summary>Gets the server private key (PKCS#8 PEM).</summary>
  public string ServerKey => Path.Combine(Directory, "server.key");

  /// <summary>
  /// Gets a server certificate (PEM) for the same names that a second CA issued, which no client
  /// trusts: the <c>tls</c> scenario's rejection case inside the Android app.
  /// </summary>
  public string UntrustedServerCertificate => Path.Combine(Directory, "untrusted-server.pem");

  /// <summary>Gets the untrusted server certificate's private key (PKCS#8 PEM).</summary>
  public string UntrustedServerKey => Path.Combine(Directory, "untrusted-server.key");

  /// <summary>
  /// Gets a client certificate (PEM) the test CA issued, which the service's
  /// <c>/test-mtls</c> hub requires (the <c>client-certificate</c> scenario).
  /// </summary>
  public string ClientCertificate => Path.Combine(Directory, "client.pem");

  /// <summary>Gets the client certificate's private key (PKCS#8 PEM).</summary>
  public string ClientKey => Path.Combine(Directory, "client.key");
}

/// <summary>
/// Generates an ephemeral CA and a server certificate for the SignalR test service's HTTPS
/// endpoint. Nothing is committed; every run writes fresh files under <c>artifacts-temp</c>.
/// </summary>
public static class TestCertificates
{
  /// <summary>Writes a new CA and a server certificate for <c>localhost</c>, the loopback addresses
  /// and the Android emulator's host alias <c>10.0.2.2</c>, and a second, untrusted CA's server
  /// certificate for the same names.</summary>
  /// <param name="directory">The output directory.</param>
  /// <returns>The written files.</returns>
  public static TestCertificateFiles Generate(string directory)
  {
    Directory.CreateDirectory(directory);
    TestCertificateFiles files = new(directory);
    (string ca, string server, string key, string client, string clientKey) = Issue("dXn.SignalR E2E Test CA");
    File.WriteAllText(files.CaCertificate, ca);
    File.WriteAllText(files.ServerCertificate, server);
    File.WriteAllText(files.ServerKey, key);
    File.WriteAllText(files.ClientCertificate, client);
    File.WriteAllText(files.ClientKey, clientKey);
    (_, string untrustedServer, string untrustedKey, _, _) = Issue("dXn.SignalR E2E Untrusted CA");
    File.WriteAllText(files.UntrustedServerCertificate, untrustedServer);
    File.WriteAllText(files.UntrustedServerKey, untrustedKey);
    return files;
  }

  /// <summary>
  /// Creates a CA named <paramref name="authority"/>, a server certificate and a client
  /// certificate it issued.
  /// </summary>
  /// <param name="authority">The CA's common name.</param>
  /// <returns>The CA certificate, the server certificate and key, and the client certificate and key, as PEM.</returns>
  static (string Ca, string Server, string Key, string Client, string ClientKey) Issue(string authority)
  {
    DateTimeOffset now = DateTimeOffset.UtcNow;

    using ECDsa caKey = ECDsa.Create(ECCurve.NamedCurves.nistP256);
    CertificateRequest caRequest = new($"CN={authority}", caKey, HashAlgorithmName.SHA256);
    caRequest.CertificateExtensions.Add(new X509BasicConstraintsExtension(true, false, 0, true));
    caRequest.CertificateExtensions.Add(
      new X509KeyUsageExtension(X509KeyUsageFlags.KeyCertSign | X509KeyUsageFlags.CrlSign, true));
    caRequest.CertificateExtensions.Add(new X509SubjectKeyIdentifierExtension(caRequest.PublicKey, false));
    using X509Certificate2 ca = caRequest.CreateSelfSigned(now.AddDays(-1), now.AddDays(7));

    using ECDsa serverKey = ECDsa.Create(ECCurve.NamedCurves.nistP256);
    CertificateRequest serverRequest = new("CN=localhost", serverKey, HashAlgorithmName.SHA256);
    serverRequest.CertificateExtensions.Add(new X509BasicConstraintsExtension(false, false, 0, true));
    serverRequest.CertificateExtensions.Add(new X509KeyUsageExtension(X509KeyUsageFlags.DigitalSignature, true));
    serverRequest.CertificateExtensions.Add(
      new X509EnhancedKeyUsageExtension([new Oid("1.3.6.1.5.5.7.3.1", "Server Authentication")], false));
    SubjectAlternativeNameBuilder names = new();
    names.AddDnsName("localhost");
    names.AddIpAddress(IPAddress.Loopback);
    names.AddIpAddress(IPAddress.IPv6Loopback);
    names.AddIpAddress(IPAddress.Parse("10.0.2.2"));
    serverRequest.CertificateExtensions.Add(names.Build());
    serverRequest.CertificateExtensions.Add(new X509SubjectKeyIdentifierExtension(serverRequest.PublicKey, false));
    serverRequest.CertificateExtensions.Add(X509AuthorityKeyIdentifierExtension.CreateFromCertificate(ca, true, false));
    byte[] serial = RandomNumberGenerator.GetBytes(16);
    serial[0] &= 0x7F;
    using X509Certificate2 server = serverRequest.Create(ca, now.AddDays(-1), now.AddDays(6), serial);

    using ECDsa clientKey = ECDsa.Create(ECCurve.NamedCurves.nistP256);
    CertificateRequest clientRequest = new("CN=dXn.SignalR E2E Client", clientKey, HashAlgorithmName.SHA256);
    clientRequest.CertificateExtensions.Add(new X509BasicConstraintsExtension(false, false, 0, true));
    clientRequest.CertificateExtensions.Add(new X509KeyUsageExtension(X509KeyUsageFlags.DigitalSignature, true));
    clientRequest.CertificateExtensions.Add(
      new X509EnhancedKeyUsageExtension([new Oid("1.3.6.1.5.5.7.3.2", "Client Authentication")], false));
    clientRequest.CertificateExtensions.Add(X509AuthorityKeyIdentifierExtension.CreateFromCertificate(ca, true, false));
    byte[] clientSerial = RandomNumberGenerator.GetBytes(16);
    clientSerial[0] &= 0x7F;
    using X509Certificate2 client = clientRequest.Create(ca, now.AddDays(-1), now.AddDays(6), clientSerial);

    return (
      ca.ExportCertificatePem(),
      server.ExportCertificatePem(),
      serverKey.ExportPkcs8PrivateKeyPem(),
      client.ExportCertificatePem(),
      clientKey.ExportPkcs8PrivateKeyPem());
  }
}
