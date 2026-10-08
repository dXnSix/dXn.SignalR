using System.Text.RegularExpressions;
using RayX.SignalR.E2E.Tests.Infrastructure;
using RayX.SignalR.E2E.Tests.Scenarios;

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
    "pnpm/action-setup@v6",
    "reactivecircus/android-emulator-runner@v2"
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
      Assert.Equal(checkouts, PersistCredentialsDisabled().Count(workflow));

      // Every upload step is a recognized, failure-only, bounded diagnostics upload.
      Assert.Equal(
        Regex.Count(workflow, @"uses:\s*actions/upload-artifact@v7"),
        UploadStep().Count(workflow));
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
  public void EveryQualityJobRunsItsOwnGateOnItsPlatform()
  {
    string quality = Workflows()["quality.yml"];
    AssertMatches("quality.yml", quality, @"(?m)^env:\s*\n(?:\s+.*\n)*?\s+RUSTFLAGS: -D warnings");
    AssertMatches("quality.yml", quality, @"(?m)^env:\s*\n(?:\s+.*\n)*?\s+RUSTDOCFLAGS: -D warnings");
    Dictionary<string, string> jobs = JobBlocks(quality);
    Dictionary<string, string[]> required = new(StringComparer.Ordinal)
    {
      ["rustfmt"] = [@"runs-on: ubuntu-latest", @"cargo \+1\.92\.0 fmt --all --check"],
      ["clippy"] = [@"runs-on: ubuntu-latest", @"clippy --all-targets --test e2e --all-features -- -D warnings"],
      ["native"] =
      [
        @"runs-on: \$\{\{ matrix\.os \}\}",
        @"os: \[ubuntu-latest, windows-latest, macos-latest\]",
        @"toolchain: \[1\.92\.0, stable\]",
        @"check --all-targets --test e2e --all-features"
      ],
      ["wasm"] =
      [
        @"runs-on: ubuntu-latest",
        "mode: default",
        "mode: all-features",
        @"cargo \+1\.92\.0 check --target wasm32-unknown-unknown \$\{\{ matrix\.feature_args \}\}"
      ],
      ["unit-contract"] =
      [
        @"runs-on: ubuntu-latest",
        "--no-default-features",
        "--all-features",
        @"cargo \+1\.92\.0 test \$\{\{ matrix\.feature_args \}\}"
      ],
      ["android"] =
      [
        @"runs-on: ubuntu-latest",
        @"cargo install cargo-ndk --version 4\.1\.2 --locked",
        @"cargo \+1\.92\.0 ndk -t arm64-v8a -t x86_64 -P 24 check --lib"
      ],
      ["ios"] =
      [
        @"runs-on: macos-latest",
        @"target: \[aarch64-apple-ios, aarch64-apple-ios-sim\]",
        @"cargo \+1\.92\.0 check --lib --test e2e --all-features --target \$\{\{ matrix\.target \}\}"
      ],
      ["threaded-wasm"] =
      [
        @"runs-on: ubuntu-latest",
        @"-C target-feature=\+atomics,\+bulk-memory,\+mutable-globals",
        @"-Z build-std=std,panic_abort"
      ],
      ["platform"] =
      [
        @"runs-on: ubuntu-latest",
        @"cargo install cargo-ndk --version 4\.1\.2 --locked",
        @"nightly-2026-06-23",
        @"--filter Category=Platform"
      ],
      ["rustdoc"] = [@"runs-on: ubuntu-latest", @"cargo \+1\.92\.0 doc --all-features --no-deps"],
      ["dependency-security"] =
      [
        @"runs-on: ubuntu-latest",
        "cargo install cargo-audit --locked",
        @"--filter Category=DependencySecurity"
      ],
      ["contracts"] =
      [
        @"runs-on: ubuntu-latest",
        @"--filter ""Category=WorkflowContract\|Category=Repository\|Category=Catalog\|Category=Toolchain"""
      ]
    };
    Assert.Equal(required.Keys.Order(StringComparer.Ordinal), jobs.Keys.Order(StringComparer.Ordinal));
    foreach((string job, string[] patterns) in required)
    {
      foreach(string pattern in patterns)
      {
        AssertMatches($"quality.yml job {job}", jobs[job], pattern);
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
              @"quality / android \(cargo ndk\)",
              @"cargo install cargo-ndk --version 4\.1\.2 --locked",
              @"cargo \+1\.92\.0 ndk -t arm64-v8a -t x86_64 -P 24 check --lib",
              @"quality / ios \(\$\{\{ matrix\.target \}\}\)",
              @"target: \[aarch64-apple-ios, aarch64-apple-ios-sim\]",
              @"cargo \+1\.92\.0 check --lib --test e2e --all-features --target \$\{\{ matrix\.target \}\}",
              "quality / threaded-wasm",
              @"-C target-feature=\+atomics,\+bulk-memory,\+mutable-globals",
              @"cargo \+nightly-2026-06-23 check --lib --target wasm32-unknown-unknown --all-features -Z build-std=std,panic_abort",
              "quality / platform",
              @"dotnet test dotnet/RayX\.SignalR\.E2E\.Tests --filter Category=Platform",
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
              @"engine: \[chromium, firefox, webkit\]",
              "e2e / android",
              @"RAYX_SIGNALR_E2E_TARGETS: android",
              @"rustup toolchain install 1\.92\.0 --profile minimal --target x86_64-linux-android",
              @"cargo ndk -t x86_64 -P 24 test --test e2e --features messagepack,proxy-pac,tls-webpki-roots --no-run",
              @"KERNEL==""kvm"", GROUP=""kvm"", MODE=""0666""",
              @"uses: reactivecircus/android-emulator-runner@v2",
              @"api-level: 34",
              @"arch: x86_64",
              @"script: dotnet test dotnet/RayX\.SignalR\.E2E\.Tests --filter Category=E2E",
              "e2e / ios",
              @"RAYX_SIGNALR_E2E_TARGETS: ios",
              @"rustup toolchain install 1\.92\.0 --profile minimal --target aarch64-apple-ios-sim",
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

    Assert.Equal(4, Regex.Count(e2e, @"dotnet test dotnet/RayX\.SignalR\.E2E\.Tests --filter Category=E2E"));
  }

  [Fact]
  [Trait("Category", "WorkflowContract")]
  public void EveryCatalogTargetHasAnEndToEndJobOnItsPlatform()
  {
    string e2e = Workflows()["e2e.yml"];
    Dictionary<string, string> jobs = JobBlocks(e2e);
    foreach(E2ETarget target in ScenarioCatalog.Targets)
    {
      string job = target.Name switch
      {
        "native" => "native",
        "android" or "android-app" or "android-app-release" => "android",
        "ios" => "ios",
        _ => "browser"
      };
      Assert.True(jobs.ContainsKey(job), $"e2e.yml has no {job} job for the {target.Name} target");
      string block = jobs[job];
      if(target.Kind == TargetKind.Browser)
      {
        Assert.Matches($@"engine: \[[^\]]*\b{target.Name}\b", block);
      }
    }

    Dictionary<string, string[]> required = new(StringComparer.Ordinal)
    {
      ["native"] =
      [
        @"os: \[ubuntu-latest, windows-latest, macos-latest\]",
        @"RAYX_SIGNALR_E2E_TARGETS: native"
      ],
      ["browser"] =
      [
        @"runs-on: ubuntu-latest",
        @"RAYX_SIGNALR_E2E_TARGETS: \$\{\{ matrix\.engine \}\}",
        @"playwright install --with-deps \$\{\{ matrix\.engine \}\}",
        @"nightly-2026-06-23"
      ],
      ["android"] =
      [
        @"runs-on: ubuntu-latest",
        @"RAYX_SIGNALR_E2E_TARGETS: android",
        @"cargo install cargo-ndk --version 4\.1\.2 --locked",
        @"CARGO_TARGET_DIR: target/android-e2e\s+run: cargo ndk -t x86_64 -P 24 test --test e2e --features messagepack,proxy-pac,tls-webpki-roots --no-run",
        @"KERNEL==""kvm""",
        @"uses: reactivecircus/android-emulator-runner@v2",
        @"api-level: 34",
        @"arch: x86_64",
        @"disk-size: 2048M",
        @"script: dotnet test dotnet/RayX\.SignalR\.E2E\.Tests --filter Category=E2E"
      ],
      ["ios"] =
      [
        @"runs-on: macos-latest",
        @"RAYX_SIGNALR_E2E_TARGETS: ios",
        @"--target aarch64-apple-ios-sim",
        @"dotnet test dotnet/RayX\.SignalR\.E2E\.Tests --filter Category=E2E"
      ]
    };
    foreach((string job, string[] patterns) in required)
    {
      foreach(string pattern in patterns)
      {
        AssertMatches($"e2e.yml job {job}", jobs[job], pattern);
      }
    }

    Assert.Equal(
      AndroidCellRunner.TargetDirectory,
      Regex.Match(jobs["android"], @"CARGO_TARGET_DIR: (?<directory>\S+)").Groups["directory"].Value);
  }

  /// <summary>Splits a workflow's <c>jobs:</c> section into job blocks by job id.</summary>
  static Dictionary<string, string> JobBlocks(string workflow)
  {
    string jobs = workflow[(workflow.IndexOf("\njobs:", StringComparison.Ordinal) + 1)..];
    Dictionary<string, string> blocks = new(StringComparer.Ordinal);
    MatchCollection headers = JobHeader().Matches(jobs);
    for(int index = 0; index < headers.Count; index++)
    {
      int start = headers[index].Index;
      int end = index + 1 < headers.Count ? headers[index + 1].Index : jobs.Length;
      blocks[headers[index].Groups["id"].Value] = jobs[start..end];
    }

    return blocks;
  }

  static void AssertMatches(string name, string value, string pattern) =>
    Assert.True(Regex.IsMatch(value, pattern), $"{name} is missing required contract pattern: {pattern}");

  static void AssertNotMatches(string name, string value, string pattern)
  {
    Match match = Regex.Match(value, pattern);
    Assert.False(match.Success, $"{name} contains prohibited contract pattern {pattern}: '{match.Value}'");
  }

  [GeneratedRegex(@"(?m)^  (?<id>[A-Za-z0-9_-]+):\s*$")]
  private static partial Regex JobHeader();

  [GeneratedRegex(@"(?m)^\s*-?\s*uses:\s*(?<action>\S+)")]
  private static partial Regex UsesLine();

  [GeneratedRegex(@"(?m)^\s+persist-credentials:\s*false\s*$")]
  private static partial Regex PersistCredentialsDisabled();

  [GeneratedRegex(@"(?ms)^\s+- name:[^\n]*\n(?:(?!^\s+- ).)*?uses:\s*actions/upload-artifact@v7.*?(?=^\s+- |\z|^\S)")]
  private static partial Regex UploadStep();
}
