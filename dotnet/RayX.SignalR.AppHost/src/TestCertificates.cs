using System.Net;
using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;

namespace RayX.SignalR.AppHost;

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
}

/// <summary>
/// Generates an ephemeral CA and a server certificate for the SignalR test service's HTTPS
/// endpoint. Nothing is committed; every run writes fresh files under <c>artifacts-temp</c>.
/// </summary>
public static class TestCertificates
{
  /// <summary>Writes a new CA and a server certificate for <c>localhost</c>, the loopback addresses
  /// and the Android emulator's host alias <c>10.0.2.2</c>.</summary>
  /// <param name="directory">The output directory.</param>
  /// <returns>The written files.</returns>
  public static TestCertificateFiles Generate(string directory)
  {
    Directory.CreateDirectory(directory);
    DateTimeOffset now = DateTimeOffset.UtcNow;

    using ECDsa caKey = ECDsa.Create(ECCurve.NamedCurves.nistP256);
    CertificateRequest caRequest = new("CN=RayX.SignalR E2E Test CA", caKey, HashAlgorithmName.SHA256);
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

    TestCertificateFiles files = new(directory);
    File.WriteAllText(files.CaCertificate, ca.ExportCertificatePem());
    File.WriteAllText(files.ServerCertificate, server.ExportCertificatePem());
    File.WriteAllText(files.ServerKey, serverKey.ExportPkcs8PrivateKeyPem());
    return files;
  }
}
