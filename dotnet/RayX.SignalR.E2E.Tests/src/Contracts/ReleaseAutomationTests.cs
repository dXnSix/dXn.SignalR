using System.Text.RegularExpressions;
using RayX.SignalR.E2E.Tests.Infrastructure;

namespace RayX.SignalR.E2E.Tests.Contracts;

/// <summary>
/// Proves release-plz releases the crate by git tag only under the 1.x policy, and that the
/// workflows run on the events the release policy allows: Release-plz and Quality's light jobs on
/// pushes to master, nothing heavy on a release pull request.
/// </summary>
[Collection(AspireTestGroup.Name)]
public sealed partial class ReleaseAutomationTests(AspireEnvironment environment)
{
  /// <summary>The job-level condition that skips a job on a same-repository release pull request.</summary>
  const string _releasePullRequestClause =
    "!(startsWith(github.head_ref, 'release-plz-') && github.event.pull_request.head.repo.full_name == github.repository)";

  const string _releasePullRequestCondition = "if: ${{ " + _releasePullRequestClause + " }}";

  const string _pullRequestOnlyCondition = "if: ${{ github.event_name != 'push' && " + _releasePullRequestClause + " }}";

  string ReleasePlz() => ReadText("release-plz.toml");

  string Workflow(string name) => ReadText(Path.Combine(".github", "workflows", name));

  string ReadText(string relativePath) =>
    File.ReadAllText(Path.Combine(environment.RepositoryRoot, relativePath)).ReplaceLineEndings("\n");

  [Fact]
  [Trait("Category", "Repository")]
  [Trait("Case", "release-automation")]
  public void ReleasePlzReleasesTheCrateByGitTagOnly()
  {
    string configuration = ReleasePlz();
    string workspace = Section(configuration, "[workspace]");
    foreach(string line in new[]
            {
              "git_only = true",
              "publish = false",
              "git_tag_enable = true",
              "git_tag_name = \"v{{ version }}\"",
              "git_release_enable = true",
              "changelog_update = true",
              "pr_branch_prefix = \"release-plz-\"",
              "semver_check = false",
              "release_commits = \"^(feat|fix|perf)\""
            })
    {
      AssertMatches("release-plz.toml [workspace]", workspace, "(?m)^" + Regex.Escape(line) + "$");
    }

    // The 1.x policy is stated where the keys that enforce it live.
    AssertMatches("release-plz.toml", workspace, @"(?m)^#.*1\.x policy.*\n(?:#.*\n)*semver_check = false$");
    AssertMatches("release-plz.toml", workspace, @"(?m)^#.*\n(?:#.*`feat`.*\n)(?:#.*\n)*release_commits = ");
    AssertMatches("release-plz.toml", configuration, @"(?m)^\[\[package\]\]\nname = ""rayx-signalr""$");
  }

  [Fact]
  [Trait("Category", "Repository")]
  [Trait("Case", "release-automation")]
  public void ChangelogTemplateKeepsTheKeepAChangelogFormatAndRendersMigrationFooters()
  {
    string changelog = Section(ReleasePlz(), "[changelog]");
    string header = ChangelogHeader(ReleasePlz());
    Assert.StartsWith("# Changelog\n", header, StringComparison.Ordinal);
    foreach(string fragment in new[]
            {
              "`rayx-signalr`",
              "[Keep a Changelog](https://keepachangelog.com/en/1.1.0/)",
              "[Semantic Versioning](https://semver.org/spec/v2.0.0.html)",
              "Conventional Commits by release-plz"
            })
    {
      Assert.Contains(fragment, header, StringComparison.Ordinal);
    }

    string body = Regex.Match(changelog, "(?ms)^body = \"\"\"\n(?<body>.*?)\"\"\"").Groups["body"].Value;
    foreach(string pattern in new[]
            {
              @"(?m)^## \[\{\{ version \}\}\] - \{\{ timestamp \| date\(format=""%Y-%m-%d""\) \}\}$",
              @"group_by\(attribute=""group""\)",
              @"\*\(\{\{ commit\.scope \}\}\)\*",
              @"\[\*\*breaking\*\*\]",
              @"\{%-? for footer in commit\.footers -?%\}",
              @"footer\.token == ""Migration""",
              @"  - Migration: \{\{ footer\.value \}\}\n"
            })
    {
      AssertMatches("release-plz.toml [changelog] body", body, pattern);
    }

    foreach(string line in new[]
            {
              "trim = true",
              "protect_breaking_commits = true",
              "tag_pattern = \"v[0-9].*\"",
              "sort_commits = \"oldest\""
            })
    {
      AssertMatches("release-plz.toml [changelog]", changelog, "(?m)^" + Regex.Escape(line) + "$");
    }

    foreach(string skipped in new[] { "chore\\\\(agentx\\\\)", "docs\\\\(agentx\\\\)", "chore\\\\(release\\\\)", "chore\\\\(deps\\\\)" })
    {
      AssertMatches(
        "release-plz.toml commit_parsers",
        changelog,
        "(?m)^  \\{ message = \"\\^" + Regex.Escape(skipped) + "\", skip = true \\},$");
    }

    foreach((string prefix, string group) in new[]
            {
              ("feat", "Features"), ("fix", "Fixes"), ("perf", "Performance"), ("refactor", "Refactoring"),
              ("docs", "Documentation"), ("(ci|build)", "Build and CI"), ("test", "Tests"), ("chore", "Maintenance")
            })
    {
      AssertMatches(
        "release-plz.toml commit_parsers",
        changelog,
        "(?m)^  \\{ message = \"\\^" + Regex.Escape(prefix) + "\", group = \"" + group + "\" \\},$");
    }
  }

  [Theory]
  [Trait("Category", "Repository")]
  [Trait("Case", "release-automation")]
  [InlineData("feat: x", true)]
  [InlineData("feat(client): x", true)]
  [InlineData("fix(client): x", true)]
  [InlineData("perf(protocol): x", true)]
  [InlineData("docs: x", false)]
  [InlineData("docs(agentx): x", false)]
  [InlineData("chore(agentx): x", false)]
  [InlineData("chore(release): x", false)]
  [InlineData("test(e2e): x", false)]
  [InlineData("ci: x", false)]
  [InlineData("build(test): x", false)]
  [InlineData("refactor(quality): x", false)]
  [InlineData("chore: release v1.0.1", false)]
  [InlineData("Merge pull request #2 from rayx-ui/x", false)]
  public void OnlyUserFacingCommitsStartARelease(string message, bool releases)
  {
    Match value = ReleaseCommits().Match(Section(ReleasePlz(), "[workspace]"));
    Assert.True(value.Success, "release-plz.toml has no release_commits basic string");
    string pattern = value.Groups["value"].Value.Replace(@"\\", @"\", StringComparison.Ordinal);
    Assert.Equal(releases, Regex.IsMatch(message, pattern));
  }

  [Fact]
  [Trait("Category", "WorkflowContract")]
  [Trait("Case", "release-automation")]
  public void ReleaseWorkflowTagsAndOpensReleasePullRequestsWithOnlyRust()
  {
    string workflow = Workflow("release-plz.yml");
    AssertMatches("release-plz.yml", workflow, @"(?m)^permissions:\n  contents: read\n\n");
    AssertNotMatches("release-plz.yml", workflow, @"(?m)^concurrency:");
    Dictionary<string, string> jobs = WorkflowContractTests.JobBlocks(workflow);
    Assert.Equal(["release", "release-pr"], jobs.Keys.Order(StringComparer.Ordinal));

    foreach((string job, string command) in new[] { ("release", "release"), ("release-pr", "release-pr") })
    {
      string block = jobs[job];
      string name = $"release-plz.yml job {job}";
      foreach(string pattern in new[]
              {
                $@"(?m)^    name: release-plz / {command}$",
                @"(?m)^    if: github\.repository == 'rayx-ui/RayX\.SignalR'$",
                @"(?m)^    runs-on: ubuntu-latest$",
                @"(?m)^      group: release-plz-[a-z]+-\$\{\{ github\.ref \}\}$",
                @"(?m)^      cancel-in-progress: false$",
                @"(?m)^      - uses: actions/checkout@v7\n        with:\n          fetch-depth: 0\n          token: \$\{\{ secrets\.RELEASE_PLZ_TOKEN \}\}$",
                @"(?m)^        run: rustup toolchain install 1\.92\.0 --profile minimal$",
                $@"(?m)^      - uses: release-plz/action@v0\.5\n        with:\n          command: {command}\n        env:\n          GITHUB_TOKEN: \$\{{\{{ secrets\.RELEASE_PLZ_TOKEN \}}\}}$"
              })
      {
        AssertMatches(name, block, pattern);
      }

      string[] grants = [.. Regex.Matches(block, @"(?m)^      ([a-z-]+): (read|write)$").Select(match => match.Value.Trim())];
      Assert.Equal(job == "release" ? ["contents: write"] : ["contents: write", "pull-requests: write"], grants);
    }

    AssertNotMatches("release-plz.yml job release", jobs["release"], @"(?m)^    needs:");
    AssertMatches("release-plz.yml job release-pr", jobs["release-pr"], @"(?m)^    needs: release$");

    // Rust only, no tests, no uploads, and the PAT as the one secret.
    AssertNotMatches("release-plz.yml", workflow, @"(?i)dotnet|aspire|pnpm|node|playwright|cargo\s+test|upload-artifact|publish");
    Assert.All(
      Regex.Matches(workflow, @"secrets\.(?<name>[A-Za-z0-9_]+)").Select(match => match.Groups["name"].Value),
      secret => Assert.Equal("RELEASE_PLZ_TOKEN", secret));
    AssertMatches("release-plz.yml", workflow, @"(?m)^#.*GITHUB_TOKEN start no workflows");
  }

  [Theory]
  [Trait("Category", "WorkflowContract")]
  [Trait("Case", "release-automation")]
  [InlineData("quality.yml", "pull_request:\n    branches: [master]\n    paths-ignore:\n      - \".agentx/**\"\n  push:\n    branches: [master]\n    paths-ignore:\n      - \".agentx/**\"\n")]
  [InlineData("e2e.yml", "pull_request:\n    branches: [master]\n    paths-ignore:\n      - \".agentx/**\"\n  workflow_dispatch:\n")]
  [InlineData("perf.yml", "workflow_dispatch:\n")]
  [InlineData("release-plz.yml", "push:\n    branches: [master]\n  workflow_dispatch:\n")]
  public void EveryWorkflowRunsOnlyOnTheEventsTheReleasePolicyAllows(string name, string triggers)
  {
    string workflow = Workflow(name);
    Match on = Regex.Match(workflow, @"(?ms)^on:\n(?<triggers>.*?)\n(?=\S)");
    Assert.True(on.Success, $"{name} has no on: block");
    string block = Regex.Replace(on.Groups["triggers"].Value.TrimEnd('\n') + "\n", @"(?m)^\s*#.*\n", string.Empty);
    Assert.Equal("  " + triggers, block);
  }

  [Theory]
  [Trait("Category", "WorkflowContract")]
  [Trait("Case", "release-automation")]
  [InlineData("quality.yml")]
  [InlineData("e2e.yml")]
  public void ReleasePullRequestsAndMasterPushesSkipTheHeavyJobs(string name)
  {
    Dictionary<string, string> expected = name == "quality.yml"
      ? new(StringComparer.Ordinal)
      {
        ["rustfmt"] = "",
        ["clippy"] = "",
        ["unit-contract"] = _releasePullRequestCondition,
        ["native"] = _pullRequestOnlyCondition,
        ["wasm"] = _pullRequestOnlyCondition,
        ["android"] = _pullRequestOnlyCondition,
        ["ios"] = _pullRequestOnlyCondition,
        ["threaded-wasm"] = _pullRequestOnlyCondition,
        ["platform"] = _pullRequestOnlyCondition,
        ["rustdoc"] = _pullRequestOnlyCondition,
        ["dependency-security"] = _pullRequestOnlyCondition,
        ["contracts"] = _pullRequestOnlyCondition
      }
      : new(StringComparer.Ordinal)
      {
        ["native"] = _releasePullRequestCondition,
        ["browser"] = _releasePullRequestCondition,
        ["android"] = _releasePullRequestCondition,
        ["android-app"] = _releasePullRequestCondition,
        ["ios"] = _releasePullRequestCondition,
        ["os-proxy"] = _releasePullRequestCondition,
        ["tls-aws-lc-native"] = _releasePullRequestCondition,
        ["tls-aws-lc-android"] = _releasePullRequestCondition,
        ["tls-aws-lc-ios"] = _releasePullRequestCondition,
        // Dispatch-only already; its condition stays as it was.
        ["soak"] = "if: github.event_name == 'workflow_dispatch'"
      };

    Dictionary<string, string> jobs = WorkflowContractTests.JobBlocks(Workflow(name));
    Assert.Equal(expected.Keys.Order(StringComparer.Ordinal), jobs.Keys.Order(StringComparer.Ordinal));
    foreach((string job, string condition) in expected)
    {
      string[] conditions = [.. JobCondition().Matches(jobs[job]).Select(match => match.Groups["condition"].Value)];
      Assert.Equal(condition.Length == 0 ? [] : [condition], conditions);
    }
  }

  [Fact]
  [Trait("Category", "WorkflowContract")]
  [Trait("Case", "release-automation")]
  public void ReleasePullRequestGuardUsesTheConfiguredBranchPrefix()
  {
    string prefix = Regex.Match(ReleasePlz(), @"(?m)^pr_branch_prefix = ""(?<prefix>[^""]+)""$").Groups["prefix"].Value;
    Assert.Equal("release-plz-", prefix);
    Assert.Contains($"startsWith(github.head_ref, '{prefix}')", _releasePullRequestClause, StringComparison.Ordinal);
    foreach(string name in new[] { "quality.yml", "e2e.yml" })
    {
      string[] prefixes = [.. Regex.Matches(Workflow(name), @"startsWith\(github\.head_ref, '(?<prefix>[^']+)'\)").Select(match => match.Groups["prefix"].Value)];
      Assert.NotEmpty(prefixes);
      Assert.All(prefixes, value => Assert.Equal(prefix, value));
    }
  }

  /// <summary>The configured changelog header: the TOML multi-line basic string (a newline right after the opening quotes is not part of it).</summary>
  internal static string ChangelogHeader(string configuration)
  {
    Match header = Regex.Match(configuration, "(?ms)^header = \"\"\"\n?(?<header>.*?)\"\"\"");
    Assert.True(header.Success, "release-plz.toml has no [changelog] header");
    return header.Groups["header"].Value;
  }

  /// <summary>The lines of a TOML table, up to the next table header.</summary>
  static string Section(string configuration, string table)
  {
    int start = configuration.IndexOf("\n" + table + "\n", StringComparison.Ordinal);
    Assert.True(start >= 0, $"release-plz.toml has no {table} table");
    Match next = Regex.Match(configuration[(start + table.Length + 2)..], @"(?m)^\[\[?[A-Za-z0-9_.-]+\]\]?$");
    return next.Success
      ? configuration[(start + 1)..(start + table.Length + 2 + next.Index)]
      : configuration[(start + 1)..];
  }

  static void AssertMatches(string name, string value, string pattern) =>
    Assert.True(Regex.IsMatch(value, pattern), $"{name} is missing required contract pattern: {pattern}");

  static void AssertNotMatches(string name, string value, string pattern)
  {
    Match match = Regex.Match(value, pattern);
    Assert.False(match.Success, $"{name} contains prohibited contract pattern {pattern}: '{match.Value}'");
  }

  [GeneratedRegex(@"(?m)^release_commits = ""(?<value>(?:[^""\\]|\\.)*)""$")]
  private static partial Regex ReleaseCommits();

  [GeneratedRegex(@"(?m)^    (?<condition>if: .*?)\s*$")]
  private static partial Regex JobCondition();
}
