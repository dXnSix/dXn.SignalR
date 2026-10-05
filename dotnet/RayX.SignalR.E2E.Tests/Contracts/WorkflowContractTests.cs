using System.Text.RegularExpressions;
using RayX.SignalR.E2E.Tests.Infrastructure;

namespace RayX.SignalR.E2E.Tests.Contracts;

/// <summary>Proves the GitHub Actions workflows keep their governance and run tests only through Aspire.</summary>
[Collection(AspireTestGroup.Name)]
public sealed partial class WorkflowContractTests(AspireEnvironment environment)
{
  /// <summary>Every action the workflows may use, on its Node 24 major.</summary>
  static readonly string[] _allowedActions =
  [
    "actions/checkout@v7",
    "actions/setup-dotnet@v6",
    "actions/cache@v6",
    "actions/upload-artifact@v7",
    "actions/setup-node@v7",
    "pnpm/action-setup@v6"
  ];

  Dictionary<string, string> Workflows()
  {
    string directory = Path.Combine(environment.RepositoryRoot, ".github", "workflows");
    Dictionary<string, string> workflows = Directory
                                           .EnumerateFiles(directory, "*.yml")
                                           .Concat(Directory.EnumerateFiles(directory, "*.yaml"))
                                           .ToDictionary(
                                             path => Path.GetFileName(path),
                                             File.ReadAllText,
                                             StringComparer.Ordinal);
    Assert.Equal(["e2e.yml", "quality.yml"], workflows.Keys.Order(StringComparer.Ordinal));
    return workflows;
  }

  [Fact]
  [Trait("Category", "WorkflowContract")]
  public void EveryWorkflowKeepsLeastPrivilegeGovernance()
  {
    foreach((string name, string workflow) in Workflows())
    {
      AssertMatches(name, workflow, @"(?ms)^on:\s*.*?pull_request:");
      AssertMatches(name, workflow, @"(?ms)^on:\s*.*?push:");
      AssertMatches(name, workflow, @"(?m)^permissions:\s*\r?\n\s+contents:\s*read\s*$");
      AssertMatches(name, workflow, @"(?ms)^concurrency:\s*.*?cancel-in-progress:\s*true");
      AssertNotMatches(name, workflow, @"(?im)^\s*[A-Za-z-]+:\s*write\s*$");
      AssertNotMatches(name, workflow, @"\$\{\{\s*secrets\.");
      AssertNotMatches(name, workflow, @"(?i)publish|nuget\s+push|cargo\s+publish|pull_request_target");
      AssertNotMatches(name, workflow, @"(?i)webdriver|chromedriver|selenium");
      AssertNotMatches(name, workflow, @"(?i)\.ps1\b|\.sh\b|\.mjs\b|scripts/|shell:\s*pwsh");

      string[] actions = UsesLine().Matches(workflow).Select(match => match.Groups["action"].Value).ToArray();
      Assert.All(actions, action => Assert.Contains(action, _allowedActions));
      int checkouts = actions.Count(action => action.StartsWith("actions/checkout@", StringComparison.Ordinal));
      Assert.Equal(checkouts, PersistCredentialsDisabled().Matches(workflow).Count);

      foreach(Match upload in UploadStep().Matches(workflow))
      {
        Assert.Matches(@"if:\s*failure\(\)", upload.Value);
        Assert.Contains("artifacts-temp/e2e", upload.Value, StringComparison.Ordinal);
        Assert.Contains("retention-days: 7", upload.Value, StringComparison.Ordinal);
      }
    }
  }

  [Fact]
  [Trait("Category", "WorkflowContract")]
  public void QualityWorkflowPreservesTheQualityMatrices()
  {
    string quality = Workflows()["quality.yml"];
    foreach(string pattern in new[]
            {
              "quality / rustfmt",
              @"cargo \+1\.92\.0 fmt --all --check",
              "quality / clippy",
              @"clippy --all-targets --test e2e --all-features -- -D warnings",
              @"check --all-targets --test e2e --all-features",
              @"os: \[ubuntu-latest, windows-latest, macos-latest\]",
              @"toolchain: \[1\.92\.0, stable\]",
              "wasm32-unknown-unknown",
              "mode: default",
              "mode: all-features",
              @"quality / unit-contract",
              "--no-default-features",
              "--all-features",
              "quality / rustdoc",
              "doc --all-features --no-deps",
              "quality / dependency-security",
              "cargo install cargo-audit --locked",
              @"dotnet test dotnet/RayX\.SignalR\.E2E\.Tests --filter Category=DependencySecurity",
              "quality / contracts",
              @"dotnet tool install --global Aspire\.Cli --version 13\.5\.\d+",
              @"--filter ""Category=WorkflowContract\|Category=Repository\|Category=Catalog\|Category=Toolchain"""
            })
    {
      AssertMatches("quality.yml", quality, pattern);
    }
  }

  [Fact]
  [Trait("Category", "WorkflowContract")]
  public void EndToEndWorkflowRunsEveryCatalogCellThroughAspire()
  {
    string e2e = Workflows()["e2e.yml"];
    foreach(string pattern in new[]
            {
              @"(?m)^\s+workflow_dispatch:",
              @"e2e / native \(\$\{\{ matrix\.os \}\}\)",
              @"os: \[ubuntu-latest, windows-latest, macos-latest\]",
              @"e2e / browser \(\$\{\{ matrix\.engine \}\}\)",
              @"engine: \[chromium\]",
              @"dotnet tool install --global Aspire\.Cli --version 13\.5\.\d+",
              @"global-json-file: global\.json",
              @"dotnet test dotnet/RayX\.SignalR\.E2E\.Tests --filter Category=E2E",
              @"RAYX_SIGNALR_E2E_TARGETS: native",
              @"RAYX_SIGNALR_E2E_TARGETS: \$\{\{ matrix\.engine \}\}",
              @"RAYX_SIGNALR_E2E_PROTOCOLS: json,messagepack",
              @"RAYX_SIGNALR_E2E_SCENARIOS: all",
              @"pnpm --dir tools-node install --frozen-lockfile",
              @"rustup toolchain install nightly-2026-06-23 --profile minimal --component rust-src --target wasm32-unknown-unknown",
              @"playwright install --with-deps \$\{\{ matrix\.engine \}\}",
              @"node-version: 24"
            })
    {
      AssertMatches("e2e.yml", e2e, pattern);
    }

    Assert.Equal(2, Regex.Matches(e2e, @"dotnet test dotnet/RayX\.SignalR\.E2E\.Tests --filter Category=E2E").Count);
  }

  static void AssertMatches(string name, string value, string pattern) =>
    Assert.True(Regex.IsMatch(value, pattern), $"{name} is missing required contract pattern: {pattern}");

  static void AssertNotMatches(string name, string value, string pattern)
  {
    Match match = Regex.Match(value, pattern);
    Assert.False(match.Success, $"{name} contains prohibited contract pattern {pattern}: '{match.Value}'");
  }

  [GeneratedRegex(@"(?m)^\s*-?\s*uses:\s*(?<action>\S+)")]
  private static partial Regex UsesLine();

  [GeneratedRegex(@"(?m)^\s+persist-credentials:\s*false\s*$")]
  private static partial Regex PersistCredentialsDisabled();

  [GeneratedRegex(@"(?ms)^\s+- name:[^\n]*\n(?:(?!^\s+- ).)*?uses:\s*actions/upload-artifact@v7.*?(?=^\s+- |\z|^\S)")]
  private static partial Regex UploadStep();
}
