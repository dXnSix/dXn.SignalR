using Microsoft.AspNetCore.SignalR;

namespace dXn.SignalR.TestService
{
    /// <summary>
    /// Mapped at <c>/test-keepalive</c> with a 3 s client timeout and 1 s server pings: a client
    /// that sends no keep-alive Pings is dropped after 3 s of idleness.
    /// </summary>
    public sealed class KeepAliveHub : Hub
    {
        /// <summary>The server connection id, which changes when the client reconnects.</summary>
        public string ConnectionIdentity() => Context.ConnectionId;
    }

    /// <summary>
    /// Mapped at <c>/test-silent</c> with server pings every 10 minutes, so an idle client hears
    /// nothing and its server timeout fires.
    /// </summary>
    public sealed class SilentHub : Hub
    {
        /// <summary>The server connection id, which changes when the client reconnects.</summary>
        public string ConnectionIdentity() => Context.ConnectionId;
    }

    /// <summary>Hub options and the never-answering handshake endpoint for liveness scenarios.</summary>
    public static class LivenessEndpoints
    {
        /// <summary>Applies the liveness hubs' ping and timeout intervals.</summary>
        /// <param name="signalR">The SignalR builder.</param>
        /// <returns>The same builder.</returns>
        public static ISignalRServerBuilder AddLivenessHubOptions(this ISignalRServerBuilder signalR) =>
            signalR
                .AddHubOptions<KeepAliveHub>(options =>
                {
                    options.KeepAliveInterval = TimeSpan.FromSeconds(1);
                    options.ClientTimeoutInterval = TimeSpan.FromSeconds(3);
                })
                .AddHubOptions<SilentHub>(options => options.KeepAliveInterval = TimeSpan.FromMinutes(10));

        /// <summary>
        /// Maps the liveness hubs and <c>/silent-handshake</c>, which accepts the WebSocket and
        /// never answers the SignalR handshake.
        /// </summary>
        /// <param name="app">The application.</param>
        public static void MapLivenessEndpoints(this WebApplication app)
        {
            // A short Long Polling poll timeout, so the idle Long Polling cells cross many empty
            // poll answers instead of none (ASP.NET Core holds a poll 90 s by default).
            app.MapHub<KeepAliveHub>("/test-keepalive", options => options.LongPolling.PollTimeout = TimeSpan.FromSeconds(5));
            app.MapHub<SilentHub>("/test-silent", options => options.LongPolling.PollTimeout = TimeSpan.FromSeconds(5));
            app.Map("/silent-handshake", async context =>
            {
                if(!context.WebSockets.IsWebSocketRequest)
                {
                    context.Response.StatusCode = StatusCodes.Status400BadRequest;
                    return;
                }

                using System.Net.WebSockets.WebSocket socket = await context.WebSockets.AcceptWebSocketAsync();
                try
                {
                    await Task.Delay(Timeout.Infinite, context.RequestAborted);
                }
                catch(OperationCanceledException)
                {
                    // The client gave up on the handshake.
                }
            });
        }
    }
}
