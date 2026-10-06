using RayX.SignalR.E2E.Tests.Infrastructure;

namespace RayX.SignalR.E2E.Tests.Contracts;

/// <summary>Proves the native TLS dependency graph per target and the Android build.</summary>
[Collection(AspireTestGroup.Name)]
public sealed class MobileTlsTests(AspireEnvironment environment)
{
  static readonly string[] _forbidden = ["openssl", "openssl-sys", "native-tls", "tokio-native-tls", "ureq"];

  public static TheoryData<string> NativeTargets() =>
  [
    "x86_64-unknown-linux-gnu",
    "x86_64-pc-windows-msvc",
    "aarch64-apple-darwin",
    "aarch64-linux-android",
    "x86_64-linux-android",
    "aarch64-apple-ios"
  ];

  [Theory]
  [Trait("Category", "Platform")]
  [Trait("Case", "tls-graph")]
  [MemberData(nameof(NativeTargets))]
  public async Task DefaultGraphUsesRustlsWithRingAndNoOpenSsl(string target)
  {
    HashSet<string> graph = await Cargo.NormalGraphAsync(
                              environment.RepositoryRoot,
                              "--target", target, "--features", "messagepack");
    string[] forbidden = [.. _forbidden.Where(graph.Contains)];
    Assert.Empty(forbidden);
    Assert.Contains("rustls", graph);
    Assert.Contains("ring", graph);
    Assert.DoesNotContain("aws-lc-rs", graph);
    Assert.Contains("rustls-platform-verifier", graph);
  }

  [Fact]
  [Trait("Category", "Platform")]
  [Trait("Case", "tls-graph")]
  public async Task AwsLcRsProviderReplacesRing()
  {
    HashSet<string> graph = await Cargo.NormalGraphAsync(
                              environment.RepositoryRoot,
                              "--no-default-features", "--features", "tls-aws-lc-rs,tls-platform-verifier");
    Assert.Contains("aws-lc-rs", graph);
    string[] forbidden = [.. _forbidden.Where(graph.Contains)];
    Assert.Empty(forbidden);
  }

  [Fact]
  [Trait("Category", "Platform")]
  [Trait("Case", "tls-graph")]
  public async Task AndroidBundlesWebpkiRootsOnlyOnRequest()
  {
    foreach(string target in new[] { "aarch64-linux-android", "x86_64-linux-android" })
    {
      HashSet<string> defaults = await Cargo.NormalGraphAsync(environment.RepositoryRoot, "--target", target);
      Assert.Contains("rustls-platform-verifier", defaults);
      Assert.DoesNotContain("webpki-roots", defaults);

      HashSet<string> bundled = await Cargo.NormalGraphAsync(
                                  environment.RepositoryRoot,
                                  "--target", target, "--features", "tls-webpki-roots");
      Assert.Contains("webpki-roots", bundled);
    }
  }

  public static TheoryData<string> AndroidFeatureSets() =>
  [
    "--features messagepack",
    "--no-default-features --features tls-webpki-roots,tls-ring,messagepack"
  ];

  [Theory]
  [Trait("Category", "Platform")]
  [Trait("Case", "android-build")]
  [MemberData(nameof(AndroidFeatureSets))]
  public async Task LibraryBuildsForAndroidThroughCargoNdk(string features)
  {
    ProcessResult result = await Cargo.RunRawAsync(
                             environment.RepositoryRoot,
                             ["ndk", "-t", "arm64-v8a", "-t", "x86_64", "-P", "24", "check", "--lib", .. features.Split(' ')],
                             AndroidEnvironment());
    Assert.True(
      result.Succeeded,
      $"cargo ndk check {features} exited {result.ExitCode}. Prerequisites: cargo install cargo-ndk; rustup target add aarch64-linux-android x86_64-linux-android --toolchain 1.92.0; an Android NDK under ANDROID_NDK_HOME or $ANDROID_HOME/ndk.\n{Redaction.SafeTail(result.StandardError)}");
  }

  [Fact]
  [Trait("Category", "Platform")]
  [Trait("Case", "android-build")]
  public async Task AndroidReExportsTheVerifierInitialization()
  {
    string library = await File.ReadAllTextAsync(Path.Combine(environment.RepositoryRoot, "src", "lib.rs"));
    Assert.Contains("pub mod tls", library, StringComparison.Ordinal);
    Assert.Contains("#[cfg(all(target_os = \"android\", feature = \"tls-platform-verifier\"))]", library, StringComparison.Ordinal);
    Assert.Contains(
      "pubuserustls_platform_verifier::android::{init_with_env,init_with_refs,init_with_runtime,};",
      string.Concat(library.Where(character => !char.IsWhiteSpace(character))),
      StringComparison.Ordinal);

    // LibraryBuildsForAndroidThroughCargoNdk compiles this module for Android with the default
    // (platform verifier) features, so the re-exported paths resolve there.
  }

  /// <summary>Points cargo-ndk at an NDK: <c>ANDROID_NDK_HOME</c>, else the newest under the SDK.</summary>
  static Dictionary<string, string> AndroidEnvironment()
  {
    Dictionary<string, string> variables = new(StringComparer.Ordinal);
    if(!string.IsNullOrEmpty(Environment.GetEnvironmentVariable("ANDROID_NDK_HOME")))
    {
      return variables;
    }

    string? sdk = Environment.GetEnvironmentVariable("ANDROID_HOME")
                  ?? Environment.GetEnvironmentVariable("ANDROID_SDK_ROOT");
    string ndkRoot = sdk is null ? string.Empty : Path.Combine(sdk, "ndk");
    string? newest = Directory.Exists(ndkRoot)
                       ? Directory.EnumerateDirectories(ndkRoot).OrderBy(path => Version.TryParse(Path.GetFileName(path), out Version? version) ? version : new Version()).LastOrDefault()
                       : null;
    if(newest is not null)
    {
      variables["ANDROID_NDK_HOME"] = newest;
    }

    return variables;
  }
}
