using System.Text.RegularExpressions;
using dXn.SignalR.E2E.Tests.Infrastructure;

namespace dXn.SignalR.E2E.Tests.Contracts;

/// <summary>Scans the Rust and NuGet dependency graphs for known vulnerabilities.</summary>
[Collection(AspireTestGroup.Name)]
public sealed partial class DependencySecurityTests(AspireEnvironment environment)
{
  [Fact]
  [Trait("Category", "DependencySecurity")]
  public async Task RustDependencyGraphHasNoRustSecAdvisories()
  {
    ProcessResult lockfile = await ProcessRunner.RunAsync(
                               new ProcessCommand("cargo", ["generate-lockfile"], environment.RepositoryRoot),
                               TimeSpan.FromMinutes(5));
    Assert.True(
      lockfile.Succeeded,
      $"cargo generate-lockfile exited {lockfile.ExitCode} (timed out: {lockfile.TimedOut}):\n{Redaction.SafeTail(lockfile.CombinedOutput)}");

    ProcessResult audit = await ProcessRunner.RunAsync(
                            new ProcessCommand("cargo", ["audit"], environment.RepositoryRoot),
                            TimeSpan.FromMinutes(10));
    Assert.True(
      audit.Succeeded,
      $"cargo audit failed (install it with `cargo install cargo-audit --locked`):\n{Redaction.SafeTail(audit.CombinedOutput)}");
  }

  [Fact]
  [Trait("Category", "DependencySecurity")]
  public async Task NuGetDependencyGraphHasNoVulnerablePackages()
  {
    ProcessResult scan = await ProcessRunner.RunAsync(
                           new ProcessCommand(
                             "dotnet",
                             ["list", "dXn.SignalR.slnx", "package", "--vulnerable", "--include-transitive"],
                             environment.RepositoryRoot),
                           TimeSpan.FromMinutes(10));
    string output = scan.CombinedOutput;
    Assert.True(
      scan.Succeeded,
      $"dotnet list package --vulnerable exited {scan.ExitCode} (timed out: {scan.TimedOut}):\n{Redaction.SafeTail(output)}");
    Assert.DoesNotMatch("(?i)has the following vulnerable packages", output);
    string solution = await File.ReadAllTextAsync(Path.Combine(environment.RepositoryRoot, "dXn.SignalR.slnx"));
    Assert.Equal(SolutionProject().Count(solution), NoVulnerablePackages().Count(output));
  }

  [GeneratedRegex("(?i)has no vulnerable packages")]
  private static partial Regex NoVulnerablePackages();

  [GeneratedRegex(@"<Project\s+Path=""[^""]+\.csproj""")]
  private static partial Regex SolutionProject();
}
