using System.Text.RegularExpressions;
using RayX.SignalR.E2E.Tests.Infrastructure;

namespace RayX.SignalR.E2E.Tests.Contracts;

/// <summary>Proves the repository keeps no test scripts and no production test hooks.</summary>
[Collection(AspireTestGroup.Name)]
public sealed partial class RepositoryTests(AspireEnvironment environment)
{
  static readonly string[] _scriptExtensions = [".ps1", ".sh", ".mjs", ".cmd", ".bat"];

  static readonly string[] _removedHooks =
  [
    "TEST_CLIENT",
    "TEST_SOCKET",
    "force_test_transport_drop",
    "pause_next_test_trackable",
    "wait_for_test_trackable_pause",
    "pause_test_trackable",
    "_test_disconnection_handler"
  ];

  [Fact]
  [Trait("Category", "Repository")]
  public async Task RepositoryContainsNoTestScripts()
  {
    IReadOnlyList<string> files = await RepositoryFiles.ListAsync(environment.RepositoryRoot);
    string[] scripts = files
                       .Where(file => !file.StartsWith("tools-node/node_modules/", StringComparison.Ordinal))
                       .Where(file => _scriptExtensions.Contains(Path.GetExtension(file), StringComparer.OrdinalIgnoreCase))
                       .ToArray();
    Assert.Empty(scripts);
    Assert.False(Directory.Exists(Path.Combine(environment.RepositoryRoot, "scripts")), "scripts/ must not exist.");
    Assert.False(
      Directory.Exists(Path.Combine(environment.RepositoryRoot, "tools-node", "scripts")),
      "tools-node/scripts/ must not exist.");
  }

  [Fact]
  [Trait("Category", "Repository")]
  public async Task LibrarySourcesContainNoRemovedTestHooks()
  {
    IReadOnlyList<string> files = await RepositoryFiles.ListAsync(environment.RepositoryRoot);
    List<string> violations = [];
    foreach(string file in files.Where(file => file.StartsWith("src/", StringComparison.Ordinal) && file.EndsWith(".rs", StringComparison.Ordinal)))
    {
      string text = await File.ReadAllTextAsync(Path.Combine(environment.RepositoryRoot, file));
      violations.AddRange(_removedHooks.Where(hook => text.Contains(hook, StringComparison.Ordinal)).Select(hook => $"{file}: {hook}"));
    }

    Assert.Empty(violations);
    Assert.False(File.Exists(Path.Combine(environment.RepositoryRoot, "src", "tests", "tests_tokio.rs")));
    Assert.False(File.Exists(Path.Combine(environment.RepositoryRoot, "src", "tests", "tests_msgpack.rs")));
  }

  [Fact]
  [Trait("Category", "Repository")]
  public async Task DocumentationDescribesTheAspireTestWorkflow()
  {
    string root = environment.RepositoryRoot;
    string readme = await File.ReadAllTextAsync(Path.Combine(root, "README.md"));
    Assert.Contains("## Platform support", readme, StringComparison.Ordinal);
    Assert.Contains("docs/README.md", readme, StringComparison.Ordinal);

    // Every chapter the documentation index links to exists.
    string index = await File.ReadAllTextAsync(Path.Combine(root, "docs", "README.md"));
    string[] chapters = [.. ChapterLink().Matches(index).Select(match => match.Groups["file"].Value).Distinct()];
    Assert.NotEmpty(chapters);
    foreach(string chapter in chapters)
    {
      Assert.True(File.Exists(Path.Combine(root, "docs", chapter)), $"docs/README.md links to missing docs/{chapter}");
    }

    string testing = await File.ReadAllTextAsync(Path.Combine(root, "docs", "14-testing.md"));
    foreach(string required in new[]
            {
              "## Testing with Aspire",
              "aspire run",
              "dotnet test dotnet/RayX.SignalR.E2E.Tests --filter Category=E2E",
              "RAYX_SIGNALR_E2E_TARGETS",
              "quality / contracts",
              "e2e / native (<os>)",
              "e2e / browser (<engine>)",
              "e2e / android",
              "e2e / ios",
              "quality / platform",
              "RAYX_SIGNALR_E2E_ANDROID_SERIAL",
              "RAYX_SIGNALR_E2E_IOS_DEVICE"
            })
    {
      Assert.Contains(required, testing, StringComparison.Ordinal);
    }

    string documentation = string.Join(
      '\n',
      await Task.WhenAll(chapters.Select(chapter => File.ReadAllTextAsync(Path.Combine(root, "docs", chapter)))));
    foreach(string required in new[]
            {
              "init_with_env",
              "tls-webpki-roots",
              "with_runtime_handle",
              "with_keep_alive_interval",
              "pause()",
              "skip_negotiation()",
              "WASI"
            })
    {
      Assert.Contains(required, documentation, StringComparison.Ordinal);
    }

    string limitations = await File.ReadAllTextAsync(Path.Combine(root, "docs", "12-limitations.md"));
    Assert.Contains("## Not covered by tests", limitations, StringComparison.Ordinal);
    Assert.Contains("Long Polling", limitations, StringComparison.Ordinal);

    foreach(string text in new[] { readme, documentation })
    {
      Assert.DoesNotContain("run-interop.ps1 -Target", text, StringComparison.Ordinal);
      Assert.DoesNotContain("verify-workflows.ps1", text, StringComparison.Ordinal);
    }
  }

  [GeneratedRegex(@"\]\((?<file>\d\d-[a-z0-9-]+\.md)(#[^)]*)?\)")]
  private static partial Regex ChapterLink();

  [Fact]
  [Trait("Category", "Repository")]
  public async Task ServiceDependentTestsUseNoFixedEndpointsOrPlaywrightWebServer()
  {
    string playwright = await File.ReadAllTextAsync(Path.Combine(environment.RepositoryRoot, "tools-node", "playwright.config.ts"));
    Assert.DoesNotContain("webServer", playwright, StringComparison.Ordinal);

    IReadOnlyList<string> files = await RepositoryFiles.ListAsync(environment.RepositoryRoot);
    string[] scenarioRoots = ["tests/e2e/", "tests/wasm-fixture/src/", "tools-node/tests/", "tools-node/wasm-host/", "dotnet/RayX.SignalR."];
    List<string> violations = [];
    foreach(string file in files.Where(file => scenarioRoots.Any(root => file.StartsWith(root, StringComparison.Ordinal))))
    {
      string text = await File.ReadAllTextAsync(Path.Combine(environment.RepositoryRoot, file));
      if(file.EndsWith("RepositoryTests.cs", StringComparison.Ordinal) || file.EndsWith("OrchestrationTests.cs", StringComparison.Ordinal))
      {
        continue;
      }

      foreach(string port in new[] { ":5220", ":7878", "5220)", "7878)" })
      {
        if(text.Contains(port, StringComparison.Ordinal))
        {
          violations.Add($"{file}: {port}");
        }
      }
    }

    Assert.Empty(violations);
  }
}

/// <summary>Lists repository files (tracked and untracked, honoring <c>.gitignore</c>).</summary>
public static class RepositoryFiles
{
  /// <summary>Lists repository-relative paths with forward slashes.</summary>
  /// <param name="repositoryRoot">The repository root.</param>
  /// <returns>The files that exist on disk.</returns>
  public static async Task<IReadOnlyList<string>> ListAsync(string repositoryRoot)
  {
    ProcessResult result = await ProcessRunner.RunAsync(
                             new ProcessCommand(
                               "git",
                               ["ls-files", "--cached", "--others", "--exclude-standard"],
                               repositoryRoot),
                             TimeSpan.FromSeconds(60));
    Assert.True(result.Succeeded, Redaction.SafeTail(result.CombinedOutput));
    return result.StandardOutput
                 .Split('\n', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries)
                 .Where(file => File.Exists(Path.Combine(repositoryRoot, file)))
                 .ToArray();
  }
}
