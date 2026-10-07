using System.Security.Cryptography.X509Certificates;
using Microsoft.AspNetCore.Authentication;
using Microsoft.AspNetCore.SignalR;
using Microsoft.AspNetCore.SignalR.Protocol;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Options;
using RayX.SignalR.TestService;

// Test-only ASP.NET Core SignalR service for RayX.SignalR end-to-end scenarios. The Aspire AppHost
// allocates its port; every fault control and probe below exists only in this service.
var builder = WebApplication.CreateBuilder(args);
builder.Logging.AddFilter("Microsoft.AspNetCore", LogLevel.Warning);

// HTTPS endpoints use the run's generated test certificate when the AppHost provides one.
if (builder.Configuration["TestTls:CertificatePath"] is { } certificatePath
    && builder.Configuration["TestTls:KeyPath"] is { } keyPath)
{
    using var pem = X509Certificate2.CreateFromPemFile(certificatePath, keyPath);
    // Re-import through PKCS#12 so SslStream can use the key on every platform.
    var certificate = X509CertificateLoader.LoadPkcs12(pem.Export(X509ContentType.Pkcs12), null);
    builder.WebHost.ConfigureKestrel(kestrel =>
        kestrel.ConfigureHttpsDefaults(https => https.ServerCertificate = certificate));
}

builder.Services.AddCors(cors => cors.AddDefaultPolicy(policy =>
    policy.AllowAnyOrigin().AllowAnyHeader().AllowAnyMethod()));
builder.Services
    .AddAuthentication(TestAuthenticationHandler.SchemeName)
    .AddScheme<AuthenticationSchemeOptions, TestAuthenticationHandler>(TestAuthenticationHandler.SchemeName, null);
builder.Services.AddAuthorization();
builder.Services.AddSingleton(typeof(HubLifetimeManager<>), typeof(TrackingHubLifetimeManager<>));
builder.Services
    .AddSignalR(options => options.EnableDetailedErrors = true)
    .AddHubOptions<TestHub>(options => options.MaximumReceiveMessageSize = 4 * 1024 * 1024)
    .AddLivenessHubOptions()
    .AddMessagePackProtocol();
// Both protocols count the Close messages clients send (see CloseCountingHubProtocol).
builder.Services.RemoveAll<IHubProtocol>();
builder.Services.AddSingleton<IHubProtocol>(services => new CloseCountingHubProtocol(
    new JsonHubProtocol(services.GetRequiredService<IOptions<JsonHubProtocolOptions>>())));
builder.Services.AddSingleton<IHubProtocol>(services => new CloseCountingHubProtocol(
    new MessagePackHubProtocol(services.GetRequiredService<IOptions<MessagePackHubProtocolOptions>>())));

var app = builder.Build();

app.UseCors();
app.UseWebSockets(new WebSocketOptions { KeepAliveInterval = TimeSpan.FromMinutes(2) });

// Count every negotiate per hub path and reject negotiation while a test window is active.
app.Use(async (context, next) =>
{
    if (HttpMethods.IsPost(context.Request.Method)
        && context.Request.Path.Value is { } path
        && path.EndsWith("/negotiate", StringComparison.OrdinalIgnoreCase))
    {
        string hubPath = path[..^"/negotiate".Length];
        TestControls.CountNegotiate(hubPath);
        TestControls.RecordNegotiate(hubPath, context.Request);
        if (TestControls.TakeNegotiationDelay(hubPath) is { } delay)
        {
            await Task.Delay(delay, context.RequestAborted);
        }

        if (TestControls.IsRejected(hubPath))
        {
            context.Response.StatusCode = StatusCodes.Status503ServiceUnavailable;
            await context.Response.WriteAsync("negotiation temporarily rejected by the test control");
            return;
        }
    }

    await next();
});

app.UseAuthentication();

// Remember which user negotiated each /test-auth connection token (see TestAuthenticationHandler).
app.Use(async (context, next) =>
{
    if (!context.Request.Path.Equals("/test-auth/negotiate", StringComparison.OrdinalIgnoreCase)
        || context.User.Identity?.IsAuthenticated != true)
    {
        await next();
        return;
    }

    Stream original = context.Response.Body;
    using MemoryStream buffer = new();
    context.Response.Body = buffer;
    try
    {
        await next();
    }
    finally
    {
        context.Response.Body = original;
    }

    byte[] body = buffer.ToArray();
    if (TestControls.ConnectionToken(body) is { } token)
    {
        TestControls.RememberToken(
            token,
            context.User.Identity.Name ?? string.Empty,
            context.User.Identity.AuthenticationType ?? string.Empty);
    }

    await original.WriteAsync(body);
});

app.UseAuthorization();

app.MapGet("/", () => "Hello World!");
app.MapGet("/health", () => Results.Ok(new { status = "ready" }));
// Test-only negotiate redirect, shaped like Azure SignalR's: the client renegotiates at the
// returned hub URL and uses the returned access token for that negotiate and the transport.
app.MapPost("/redirect/negotiate", (HttpContext context) => Results.Json(new
{
    url = $"{context.Request.Scheme}://{context.Request.Host}/test",
    accessToken = "redirect-token"
}));
// Accepts a negotiate and never answers; the request is held until the client gives up.
app.MapPost("/test-hang/negotiate", async (HttpContext context) =>
{
    try
    {
        await Task.Delay(Timeout.Infinite, context.RequestAborted);
    }
    catch(OperationCanceledException)
    {
        // The client's connect timeout ended the request.
    }
});
app.MapPost("/e2e/reset", () =>
{
    TestControls.Reset();
    return Results.Ok(new { status = "reset" });
});
app.MapHub<TestHub>("/test");
app.MapHub<TestHub>("/test-exhaust");
app.MapHub<TestHub>("/test-tls");
app.MapHub<TestHub>("/test-direct");
app.MapHub<TestHub>("/test-stateful", options => options.AllowStatefulReconnects = true);
app.MapHub<AuthHub>("/test-auth");
app.MapHub<ConnectedHub>("/test-connected");
app.MapHub<PerfHub>("/perf");
app.MapLivenessEndpoints();

app.Run();
