using System.Collections.Concurrent;
using Microsoft.AspNetCore.Http.Connections;
using Microsoft.AspNetCore.Http.Connections.Features;
using Microsoft.AspNetCore.SignalR;

namespace dXn.SignalR.TestService
{
    /// <summary>
    /// Test-only transport faults: severs a connection's physical transport without a SignalR
    /// Close, so the client's reconnection policy reconnects with a fresh negotiate. A WebSocket
    /// is severed by aborting its request. Long Polling has no lasting request, so the service
    /// forgets the connection as a restarted server would: the current poll is aborted and every
    /// later poll with its token is answered 404.
    /// </summary>
    public static class TransportFaults
    {
        static readonly ConcurrentDictionary<string, HttpContext> _polls = new();
        static readonly ConcurrentDictionary<string, byte> _severedTokens = new();

        /// <summary>Clears the pending faults between end-to-end cells.</summary>
        public static void Reset() => _severedTokens.Clear();

        /// <summary>Severs the physical transport of the calling connection.</summary>
        /// <param name="context">The hub caller context.</param>
        public static void Sever(HubCallerContext context)
        {
            var httpContext = context.GetHttpContext()
                ?? throw new InvalidOperationException("SignalR transport has no HTTP context");
            if (context.Features.Get<IHttpTransportFeature>()?.TransportType != HttpTransportType.LongPolling)
            {
                httpContext.Abort();
                return;
            }

            string token = httpContext.Request.Query["id"].ToString();
            _severedTokens[token] = 0;
            if (_polls.TryRemove(token, out HttpContext? poll))
            {
                poll.Abort();
            }
        }

        /// <summary>Tracks Long Polling polls so <see cref="Sever"/> can abort them.</summary>
        /// <param name="app">The application.</param>
        public static void UseLongPollingFaults(this IApplicationBuilder app) =>
            app.Use(async (context, next) =>
            {
                if (!HttpMethods.IsGet(context.Request.Method)
                    || context.WebSockets.IsWebSocketRequest
                    || context.Request.Headers.Accept.ToString().Contains("text/event-stream", StringComparison.Ordinal)
                    || context.Request.Query["id"].ToString() is not { Length: > 0 } token)
                {
                    await next();
                    return;
                }

                if (_severedTokens.ContainsKey(token))
                {
                    context.Response.StatusCode = StatusCodes.Status404NotFound;
                    return;
                }

                _polls[token] = context;
                try
                {
                    await next();
                }
                finally
                {
                    _polls.TryRemove(new KeyValuePair<string, HttpContext>(token, context));
                }
            });
    }
}
