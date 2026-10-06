using Microsoft.Extensions.FileProviders;

// Test-only static host for the browser fixture: serves the host pages and the wasm-bindgen output
// on an Aspire-allocated origin that differs from the SignalR test service's origin.
WebApplicationBuilder builder = WebApplication.CreateBuilder(args);
WebApplication app = builder.Build();

string fixtureDirectory = RequiredDirectory(app.Configuration, "FixtureHost:FixtureDirectory");
string hostPagesDirectory = RequiredDirectory(app.Configuration, "FixtureHost:HostPagesDirectory");
CompositeFileProvider files = new(
  new PhysicalFileProvider(hostPagesDirectory),
  new PhysicalFileProvider(fixtureDirectory));

app.MapGet("/health", () => Results.Ok(new { status = "ready" }));

// The optional `isolated` endpoint serves the threaded fixture cross-origin isolated, so
// SharedArrayBuffer and wasm threads are available there.
if(app.Configuration.GetValue<int?>("FixtureHost:IsolatedPort") is { } isolatedPort)
{
  string threadedDirectory = RequiredDirectory(app.Configuration, "FixtureHost:ThreadedDirectory");
  CompositeFileProvider threadedFiles = new(
    new PhysicalFileProvider(hostPagesDirectory),
    new PhysicalFileProvider(threadedDirectory));
  app.UseWhen(
    context => context.Connection.LocalPort == isolatedPort,
    isolated =>
    {
      isolated.Use(async (context, next) =>
      {
        context.Response.Headers["Cross-Origin-Opener-Policy"] = "same-origin";
        context.Response.Headers["Cross-Origin-Embedder-Policy"] = "require-corp";
        await next();
      });
      isolated.UseDefaultFiles(new DefaultFilesOptions { FileProvider = threadedFiles });
      isolated.UseStaticFiles(
        new StaticFileOptions
        {
          FileProvider = threadedFiles,
          OnPrepareResponse = context => context.Context.Response.Headers.CacheControl = "no-store"
        });
    });
}

app.UseDefaultFiles(new DefaultFilesOptions { FileProvider = files });
app.UseStaticFiles(
  new StaticFileOptions
  {
    FileProvider = files,
    OnPrepareResponse = context => context.Context.Response.Headers.CacheControl = "no-store"
  });

app.Run();

static string RequiredDirectory(IConfiguration configuration, string key)
{
  string? path = configuration[key];
  if(string.IsNullOrWhiteSpace(path) || !Directory.Exists(path))
  {
    throw new InvalidOperationException($"{key} must name an existing directory (got '{path}').");
  }

  return Path.GetFullPath(path);
}
