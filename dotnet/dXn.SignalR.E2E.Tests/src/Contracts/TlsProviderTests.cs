using dXn.SignalR.E2E.Tests.Scenarios;

namespace dXn.SignalR.E2E.Tests.Contracts;

/// <summary>
/// Proves the <c>DXN_SIGNALR_E2E_TLS_PROVIDER</c> selection: its features, its separate target
/// directories and the classification of a missing toolchain.
/// </summary>
public sealed class TlsProviderTests
{
  [Theory]
  [Trait("Category", "Catalog")]
  [InlineData(null, TlsProvider.Ring)]
  [InlineData("", TlsProvider.Ring)]
  [InlineData(" ring ", TlsProvider.Ring)]
  [InlineData("aws-lc-rs", TlsProvider.AwsLcRs)]
  public void TheSelectionDefaultsToRing(string? value, string expected)
  {
    Assert.Equal(expected, TlsProvider.Parse(value));
  }

  [Fact]
  [Trait("Category", "Catalog")]
  public void AnUnknownProviderIsRejected()
  {
    ArgumentException error = Assert.Throws<ArgumentException>(() => TlsProvider.Parse("openssl"));
    Assert.Contains(TlsProvider.Variable, error.Message, StringComparison.Ordinal);
  }

  [Fact]
  [Trait("Category", "Catalog")]
  public void AwsLcRsBuildsWithoutRingIntoItsOwnTargetDirectories()
  {
    Assert.Equal(
      ["--no-default-features", "--features", "messagepack,proxy-pac,http3,tls-platform-verifier,tls-aws-lc-rs"],
      TlsProvider.HostFeatures(TlsProvider.AwsLcRs));
    Assert.Equal(
      ["--no-default-features", "--features", "messagepack,proxy-pac,http3,tls-platform-verifier,tls-aws-lc-rs,tls-webpki-roots"],
      TlsProvider.AndroidFeatures(TlsProvider.AwsLcRs));

    Infrastructure.ProcessCommand host = NativeE2EBinary.BuildCommand("/repo", TlsProvider.AwsLcRs);
    Assert.Equal(Path.Combine("/repo", "target/e2e-aws-lc"), host.Environment["CARGO_TARGET_DIR"]);
    Infrastructure.ProcessCommand android = AndroidCellRunner.BuildCommand("/repo", TlsProvider.AwsLcRs);
    Assert.Equal(Path.Combine("/repo", "target/android-e2e-aws-lc"), android.Environment["CARGO_TARGET_DIR"]);
    Assert.Contains("tls-aws-lc-rs", string.Join(' ', IosCellRunner.BuildCommand("/repo", TlsProvider.AwsLcRs).Arguments), StringComparison.Ordinal);
  }

  [Fact]
  [Trait("Category", "Catalog")]
  public void RingKeepsTheDefaultFeaturesAndTargetDirectories()
  {
    Assert.Equal(CellExecution.E2EFeatures, TlsProvider.HostFeatures(TlsProvider.Ring));
    Assert.Empty(NativeE2EBinary.BuildCommand("/repo", TlsProvider.Ring).Environment);
    Assert.Equal(
      Path.Combine("/repo", AndroidCellRunner.TargetDirectory),
      AndroidCellRunner.BuildCommand("/repo").Environment["CARGO_TARGET_DIR"]);
  }

  [Theory]
  [Trait("Category", "Catalog")]
  [InlineData("error: failed to run custom build command for `aws-lc-sys v0.30.0`\nMissing dependency: cmake")]
  [InlineData("thread 'main' panicked: Unable to find libclang: \"couldn't find any valid shared libraries\"")]
  public void AMissingToolchainIsAnEnvironmentError(string output)
  {
    string? message = TlsProvider.MissingToolchain(output);
    Assert.NotNull(message);
    Assert.StartsWith("environment error:", message, StringComparison.Ordinal);
  }

  [Fact]
  [Trait("Category", "Catalog")]
  public void ACompileErrorIsNotAnEnvironmentError()
  {
    Assert.Null(TlsProvider.MissingToolchain("error[E0308]: mismatched types"));
  }
}
