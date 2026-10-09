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
    Assert.Equal(["e2e.yml", "perf.yml", "quality.yml"], workflows.Keys.Order(StringComparer.Ordinal));
    return workflows;
  }

  [Fact]
  [Trait("Category", "WorkflowContract")]
  public void EveryWorkflowKeepsLeastPrivilegeGovernance()
  {
    foreach((string name, string workflow) in Workflows())
    {
      // The performance comparison runs only on schedule and on demand; every other workflow
      // gates pull requests and pushes.
      if(name == "perf.yml")
      {
        AssertMatches(name, workflow, @"(?ms)^on:\s*.*?schedule:");
        AssertMatches(name, workflow, @"(?ms)^on:\s*.*?workflow_dispatch:");
        AssertNotMatches(name, workflow, @"(?m)^\s+(pull_request|push):");
      }
      else
      {
        AssertMatches(name, workflow, @"(?ms)^on:\s*.*?pull_request:");
        // Pull requests only: a merge to master would re-run what the pull request proved.
        AssertNotMatches(name, workflow, @"(?m)^\s+push:");
      }
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

      // Every upload step is a recognized, bounded upload: failure-only redacted diagnostics, or
      // the performance report (always, 30 days).
      Assert.Equal(
        Regex.Count(workflow, @"uses:\s*actions/upload-artifact@v7"),
        UploadStep().Count(workflow));
      foreach(Match upload in UploadStep().Matches(workflow))
      {
        if(name == "perf.yml")
        {
          Assert.Matches(@"if:\s*always\(\)", upload.Value);
          Assert.Contains("path: artifacts-temp/perf", upload.Value, StringComparison.Ordinal);
          Assert.Contains("retention-days: 30", upload.Value, StringComparison.Ordinal);
          continue;
        }

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
              @"cargo ndk -t x86_64 -P 24 test --test e2e --features messagepack,proxy-pac,http3,tls-webpki-roots --no-run",
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
              @"RAYX_SIGNALR_E2E_TRANSPORTS: all",
              @"(?m)^\s+schedule:",
              @"pnpm --dir tools-node install --frozen-lockfile",
              @"rustup toolchain install nightly-2026-06-23 --profile minimal --component rust-src --target wasm32-unknown-unknown",
              @"playwright install --with-deps \$\{\{ matrix\.engine \}\}",
              @"node-version: 24"
            })
    {
      AssertMatches("e2e.yml", e2e, pattern);
    }

    // native, browser, android, android-app, ios, os-proxy, three tls-aws-lc jobs and soak.
    Assert.Equal(10, Regex.Count(e2e, @"dotnet test dotnet/RayX\.SignalR\.E2E\.Tests --filter Category=E2E"));
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
        "android" => "android",
        "android-app" or "android-app-release" => "android-app",
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
        @"CARGO_TARGET_DIR: target/android-e2e\s+run: cargo ndk -t x86_64 -P 24 test --test e2e --features messagepack,proxy-pac,http3,tls-webpki-roots --no-run",
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

  [Fact]
  [Trait("Category", "WorkflowContract")]
  public void EveryEndToEndJobRunsEveryTransport()
  {
    string e2e = Workflows()["e2e.yml"];
    AssertMatches("e2e.yml", e2e, @"(?m)^env:\s*\n(?:\s+.*\n)*?\s+RAYX_SIGNALR_E2E_TRANSPORTS: all");
    foreach((string job, string block) in JobBlocks(e2e))
    {
      AssertNotMatches($"e2e.yml job {job}", block, @"RAYX_SIGNALR_E2E_TRANSPORTS:");
    }
  }

  [Fact]
  [Trait("Category", "WorkflowContract")]
  public void EveryEndToEndJobRunsTheTypedErrorScenario()
  {
    foreach((string job, string block) in JobBlocks(Workflows()["e2e.yml"]))
    {
      // The cells the job's own selection resolves to (scenarios default to the workflow's
      // `all`), on a Windows host that may change its proxy (the widest host): at least one is an
      // `error-kinds` cell on every target the job runs.
      string Selected(string variable, string fallback)
      {
        Match match = Regex.Match(block, $@"{variable}: (?<value>\S+)");
        return match.Success ? match.Groups["value"].Value.Trim('"') : fallback;
      }

      string targets = Selected("RAYX_SIGNALR_E2E_TARGETS", "native");
      if(targets.Contains("${{", StringComparison.Ordinal))
      {
        targets = string.Join(',', Regex.Match(block, @"engine: \[(?<engines>[^\]]+)\]").Groups["engines"].Value.Split(',', StringSplitOptions.TrimEntries));
      }

      E2ESelection selection = E2ESelection.Parse(targets, "json,messagepack", Selected("RAYX_SIGNALR_E2E_SCENARIOS", "all"), "all");
      IReadOnlyList<E2ECell> cells = selection.Cells(windowsHost: true, hostSystemChanges: true, quicHost: true);
      foreach(string target in targets.Split(','))
      {
        Assert.True(
          cells.Any(cell => cell.Target.Name == target && cell.Scenario.Name == "error-kinds"),
          $"e2e.yml job {job} runs no error-kinds cell on {target}");
      }
    }
  }

  [Fact]
  [Trait("Category", "WorkflowContract")]
  public void AndroidAppJobRunsTheDebugAndReleaseAppsOnAnEmulator()
  {
    string block = JobBlocks(Workflows()["e2e.yml"])["android-app"];
    foreach(string pattern in new[]
            {
              "name: e2e / android-app",
              "runs-on: ubuntu-latest",
              "RAYX_SIGNALR_E2E_TARGETS: android-app,android-app-release",
              @"rustup toolchain install 1\.92\.0 --profile minimal --target x86_64-linux-android",
              @"cargo install cargo-ndk --version 4\.1\.2 --locked",
              @"script: JAVA_HOME=\$JAVA_HOME_17_X64 dotnet test",
              @"KERNEL==""kvm""",
              @"uses: reactivecircus/android-emulator-runner@v2",
              @"api-level: 34",
              @"dotnet test dotnet/RayX\.SignalR\.E2E\.Tests --filter Category=E2E"
            })
    {
      AssertMatches("e2e.yml job android-app", block, pattern);
    }
  }

  [Theory]
  [Trait("Category", "WorkflowContract")]
  [InlineData("tls-aws-lc-native", @"os: \[ubuntu-latest, windows-latest, macos-latest\]", "RAYX_SIGNALR_E2E_TARGETS: native")]
  [InlineData("tls-aws-lc-android", "uses: reactivecircus/android-emulator-runner@v2", "RAYX_SIGNALR_E2E_TARGETS: android")]
  [InlineData("tls-aws-lc-ios", "runs-on: macos-latest", "RAYX_SIGNALR_E2E_TARGETS: ios")]
  public void TlsAwsLcJobsRunTheTlsCellsWithTheAwsLcProvider(string job, string platform, string target)
  {
    string block = JobBlocks(Workflows()["e2e.yml"])[job];
    AssertMatches(job, block, @"name: e2e / tls-aws-lc \(");
    AssertMatches(job, block, platform);
    AssertMatches(job, block, "(?m)" + target + @"\s*$");
    AssertMatches(job, block, "RAYX_SIGNALR_E2E_TLS_PROVIDER: aws-lc-rs");
    AssertMatches(job, block, @"RAYX_SIGNALR_E2E_SCENARIOS: \S*\btls\b");
    AssertMatches(job, block, @"dotnet test dotnet/RayX\.SignalR\.E2E\.Tests --filter Category=E2E");
  }

  [Fact]
  [Trait("Category", "WorkflowContract")]
  public void OsProxyJobsChangeSystemSettingsOnlyOnEphemeralRunners()
  {
    Dictionary<string, string> jobs = JobBlocks(Workflows()["e2e.yml"]);
    string block = jobs["os-proxy"];
    foreach(string pattern in new[]
            {
              @"name: e2e / os-proxy \(\$\{\{ matrix\.os \}\}\)",
              @"os: \[windows-latest, macos-latest\]",
              "RAYX_SIGNALR_E2E_TARGETS: native",
              @"RAYX_SIGNALR_E2E_SCENARIOS: os-proxy,error-kinds",
              @"RAYX_SIGNALR_E2E_ALLOW_SYSTEM_CHANGES: ""1"""
            })
    {
      AssertMatches("e2e.yml job os-proxy", block, pattern);
    }

    // No other job may change the host's settings.
    Assert.All(
      jobs.Where(job => job.Key != "os-proxy"),
      job => AssertNotMatches($"e2e.yml job {job.Key}", job.Value, "RAYX_SIGNALR_E2E_ALLOW_SYSTEM_CHANGES"));
  }

  [Fact]
  [Trait("Category", "WorkflowContract")]
  public void SoakRunsNightlyAndOnDemandFor900Seconds()
  {
    string e2e = Workflows()["e2e.yml"];
    AssertMatches("e2e.yml", e2e, @"(?ms)^on:\s*.*?schedule:\s*\n\s+- cron:");
    // The nightly schedule runs only the soak; every other job skips it.
    Assert.All(
      JobBlocks(e2e).Where(job => job.Key != "soak"),
      job => AssertMatches($"e2e.yml job {job.Key}", job.Value, @"if: github\.event_name != 'schedule'"));
    string block = JobBlocks(e2e)["soak"];
    foreach(string pattern in new[]
            {
              "name: e2e / soak",
              @"if: github\.event_name == 'schedule' \|\| github\.event_name == 'workflow_dispatch'",
              @"RAYX_SIGNALR_E2E_SCENARIOS: soak-chaos,error-kinds",
              @"RAYX_SIGNALR_SOAK_SECONDS: ""900""",
              @"dotnet test dotnet/RayX\.SignalR\.E2E\.Tests --filter Category=E2E"
            })
    {
      AssertMatches("e2e.yml job soak", block, pattern);
    }
  }

  [Theory]
  [Trait("Category", "WorkflowContract")]
  [InlineData("native")]
  [InlineData("android")]
  public void Http3CellsGetLibmsquicOnLinux(string job)
  {
    string block = JobBlocks(Workflows()["e2e.yml"])[job];
    AssertMatches(job, block, @"packages\.microsoft\.com/config/ubuntu/");
    AssertMatches(job, block, @"sudo apt-get install -y libmsquic");
  }

  [Fact]
  [Trait("Category", "WorkflowContract")]
  public void PerformanceWorkflowComparesBothClientsAtFullScale()
  {
    string perf = Workflows()["perf.yml"];
    Dictionary<string, string> jobs = JobBlocks(perf);
    Assert.Equal(["compare"], jobs.Keys);
    foreach(string pattern in new[]
            {
              "name: perf / compare",
              "runs-on: ubuntu-latest",
              @"dotnet tool install --global Aspire\.Cli --version 13\.5\.\d+",
              @"dotnet test dotnet/RayX\.SignalR\.E2E\.Tests --filter ""Category=Performance\|Category=ReferenceClientBudget""",
              @"if: always\(\)",
              "path: artifacts-temp/perf",
              "retention-days: 30"
            })
    {
      AssertMatches("perf.yml job compare", jobs["compare"], pattern);
    }

    AssertMatches("perf.yml", perf, @"(?m)^\s+RAYX_SIGNALR_PERF_SCALE: full\s*$");
    AssertMatches("perf.yml", perf, @"(?m)^\s+RAYX_SIGNALR_PERF_WORKLOADS: all\s*$");
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
