using System.Text.Json;
using System.Text.RegularExpressions;
using dXn.SignalR.E2E.Tests.Infrastructure;

namespace dXn.SignalR.E2E.Tests.Contracts;

/// <summary>
/// Proves the released version stays on 1.x with a matching changelog section, and that consumer
/// documentation depends on the crate only by release tag.
/// </summary>
[Collection(AspireTestGroup.Name)]
public sealed partial class ReleaseConsumptionTests(AspireEnvironment environment)
{
  string ReadText(string relativePath) =>
    File.ReadAllText(Path.Combine(environment.RepositoryRoot, relativePath)).ReplaceLineEndings("\n");

  [Fact]
  [Trait("Category", "Repository")]
  [Trait("Case", "release-consumption")]
  public async Task ManifestVersionStaysOnOneXWithADatedChangelogSection()
  {
    JsonElement package = await Cargo.PackageAsync(environment.RepositoryRoot);
    string version = package.GetProperty("version").GetString()!;
    Match semver = Regex.Match(version, @"^(?<major>\d+)\.(?<minor>\d+)\.(?<patch>\d+)$");
    Assert.True(semver.Success, $"Cargo.toml version {version} is not a release version MAJOR.MINOR.PATCH");
    // The 1.x policy (docs/releasing.md): no release leaves 1.x while the owner is the only
    // consumer. When the policy ends, change this assertion together with docs/releasing.md.
    Assert.True(
      semver.Groups["major"].Value == "1",
      $"Cargo.toml version {version} left 1.x; a 2.0.0 release pull request means a stray `!` or `BREAKING CHANGE:` commit. "
      + "When the 1.x policy ends (a second consumer), change this assertion together with docs/releasing.md.");

    AssertMatches("CHANGELOG.md", ReadText("CHANGELOG.md"), $@"(?m)^## \[{Regex.Escape(version)}\] - \d{{4}}-\d{{2}}-\d{{2}}$");
  }

  [Fact]
  [Trait("Category", "Repository")]
  [Trait("Case", "release-consumption")]
  public void ChangelogStartsWithTheConfiguredHeader()
  {
    string header = ReleaseAutomationTests.ChangelogHeader(ReadText("release-plz.toml"));
    string changelog = ReadText("CHANGELOG.md");
    Assert.StartsWith(header, changelog, StringComparison.Ordinal);
    // Release sections follow the header directly; the header itself appears once.
    AssertMatches("CHANGELOG.md", changelog[header.Length..], @"^## \[\d+\.\d+\.\d+\] - \d{4}-\d{2}-\d{2}\n");
    Assert.Equal(1, Regex.Count(changelog, "(?m)^# Changelog$"));
  }

  [Fact]
  [Trait("Category", "Repository")]
  [Trait("Case", "release-consumption")]
  public void ConsumerDocumentationDependsOnAReleaseTagOnly()
  {
    string root = environment.RepositoryRoot;
    string[] documents =
    [
      Path.Combine(root, "README.md"),
      .. Directory.EnumerateFiles(Path.Combine(root, "docs"), "*.md"),
      .. Directory.EnumerateFiles(Path.Combine(root, ".claude", "skills", "dxn-signalr"), "*.md", SearchOption.AllDirectories)
    ];

    int snippets = 0;
    foreach(string document in documents)
    {
      string name = Path.GetRelativePath(root, document);
      foreach(Match snippet in DependencySnippet().Matches(File.ReadAllText(document)))
      {
        snippets++;
        AssertMatches(name, snippet.Value, @"\btag = ""v\d+\.\d+\.\d+""");
        AssertMatches(name, snippet.Value, @"\bgit = ""(https://github\.com/dXnSix/dXn\.SignalR|\.\.\.)""");
        AssertNotMatches(name, snippet.Value, @"\b(path|branch|rev) = ");
      }

      AssertNotMatches(name, File.ReadAllText(document), @"path = ""\.\./dXn\.SignalR""");
    }

    // README, getting started (two), TLS (two), the skill, its setup reference and its TLS reference (two).
    Assert.True(snippets >= 9, $"expected the consumer dependency snippets, found {snippets}");

    string readme = ReadText("README.md");
    string installation = Regex.Match(readme, @"(?ms)^## Installation\n.*?(?=^## )").Value;
    Assert.Contains("(docs/releasing.md)", installation, StringComparison.Ordinal);
    Assert.Contains("not on crates.io", installation, StringComparison.Ordinal);
    Assert.Contains("https://github.com/dXnSix/dXn.SignalR/releases", installation, StringComparison.Ordinal);
    AssertMatches(
      "README.md ## Installation",
      installation,
      @"(?m)^dxn-signalr = \{ git = ""https://github\.com/dXnSix/dXn\.SignalR"", tag = ""v\d+\.\d+\.\d+"", features = \[""messagepack""\] \}$");

    string skill = ReadText(Path.Combine(".claude", "skills", "dxn-signalr", "SKILL.md"));
    AssertNotMatches("SKILL.md", skill, @"(?i)\b(git or path|by path)\b");
  }

  static void AssertMatches(string name, string value, string pattern) =>
    Assert.True(Regex.IsMatch(value, pattern), $"{name} is missing required contract pattern: {pattern}");

  static void AssertNotMatches(string name, string value, string pattern)
  {
    Match match = Regex.Match(value, pattern);
    Assert.False(match.Success, $"{name} contains prohibited contract pattern {pattern}: '{match.Value}'");
  }

  /// <summary>A `dxn-signalr = { … }` dependency, at the start of a line or inline in prose.</summary>
  [GeneratedRegex(@"dxn-signalr = \{[^}\n]*\}")]
  private static partial Regex DependencySnippet();
}
