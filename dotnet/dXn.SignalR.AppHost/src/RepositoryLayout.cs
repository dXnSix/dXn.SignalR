namespace dXn.SignalR.AppHost;

/// <summary>Locates the dXn.SignalR repository root from any directory inside it.</summary>
public static class RepositoryLayout
{
  /// <summary>The solution file that marks the repository root.</summary>
  public const string SolutionFile = "dXn.SignalR.slnx";

  /// <summary>Walks up from <paramref name="start"/> to the directory containing the solution.</summary>
  /// <param name="start">A directory inside the repository.</param>
  /// <returns>The repository root.</returns>
  public static string FindRoot(string start)
  {
    DirectoryInfo? directory = new(start);
    while(directory is not null)
    {
      if(File.Exists(Path.Combine(directory.FullName, SolutionFile)))
      {
        return directory.FullName;
      }

      directory = directory.Parent;
    }

    throw new DirectoryNotFoundException($"Could not locate {SolutionFile} above {start}.");
  }
}
