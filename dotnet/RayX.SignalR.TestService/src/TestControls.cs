using System.Collections.Concurrent;
using System.Text.Json;
using Microsoft.AspNetCore.SignalR;
using Microsoft.AspNetCore.SignalR.Protocol;

namespace RayX.SignalR.TestService
{
    /// <summary>
    /// Test-only service state shared by hubs and middleware: probes, negotiate counters,
    /// negotiation rejection windows, captured hub connections and authenticated tokens.
    /// Everything here exists only in the test service; the client exposes no fault hooks.
    /// </summary>
    public static class TestControls
    {
        static readonly ConcurrentDictionary<string, int> _negotiateCounts = new(StringComparer.OrdinalIgnoreCase);
        static readonly ConcurrentDictionary<string, DateTimeOffset> _rejections = new(StringComparer.OrdinalIgnoreCase);
        static readonly ConcurrentDictionary<string, ConcurrentQueue<string>> _recordedSends = new();
        static readonly ConcurrentDictionary<string, int> _disconnects = new();
        static readonly ConcurrentDictionary<string, string> _disconnectErrors = new();
        static readonly ConcurrentDictionary<string, TimeSpan> _negotiationDelays = new(StringComparer.OrdinalIgnoreCase);
        static readonly ConcurrentDictionary<string, string[]> _negotiateHeaders = new(StringComparer.OrdinalIgnoreCase);
        static readonly ConcurrentDictionary<string, byte> _rejectedResumes = new();
        static int _clientCloses;
        static int _resumeRejections;

        /// <summary>The request headers the request-headers scenario inspects.</summary>
        static readonly string[] _describedHeaders =
            ["user-agent", "x-signalr-user-agent", "x-requested-with", "accept", "x-tenant", "x-hook"];
        static readonly ConcurrentDictionary<string, int> _clientResults = new();
        static readonly ConcurrentDictionary<string, string> _clientResultErrors = new();
        static readonly ConcurrentDictionary<string, HubConnectionContext> _connections = new();
        static readonly ConcurrentDictionary<string, (string Name, string Scheme)> _authenticatedTokens = new();

        /// <summary>Clears every probe and control between end-to-end cells.</summary>
        public static void Reset()
        {
            _negotiateCounts.Clear();
            _rejections.Clear();
            _recordedSends.Clear();
            _disconnects.Clear();
            _disconnectErrors.Clear();
            _negotiationDelays.Clear();
            _negotiateHeaders.Clear();
            _rejectedResumes.Clear();
            Interlocked.Exchange(ref _clientCloses, 0);
            Interlocked.Exchange(ref _resumeRejections, 0);
            ChaosControl.ResetAll();
            RotatingTokens.Reset();
            _clientResults.Clear();
            _authenticatedTokens.Clear();
        }

        public static void CountNegotiate(string hubPath) =>
            _negotiateCounts.AddOrUpdate(hubPath, 1, static (_, count) => count + 1);

        public static int NegotiateCount(string hubPath) =>
            _negotiateCounts.TryGetValue(hubPath, out var count) ? count : 0;

        /// <summary>Rejects negotiation for <paramref name="hubPath"/> for a bounded time and restarts its counter.</summary>
        public static void RejectNegotiation(string hubPath, TimeSpan duration)
        {
            _negotiateCounts.TryRemove(hubPath, out _);
            _rejections[hubPath] = DateTimeOffset.UtcNow + duration;
        }

        /// <summary>Holds the next negotiate request of <paramref name="hubPath"/> for <paramref name="delay"/>.</summary>
        public static void DelayNextNegotiation(string hubPath, TimeSpan delay) => _negotiationDelays[hubPath] = delay;

        /// <summary>Returns and clears the pending negotiate delay of <paramref name="hubPath"/>.</summary>
        public static TimeSpan? TakeNegotiationDelay(string hubPath) =>
            _negotiationDelays.TryRemove(hubPath, out var delay) ? delay : null;

        /// <summary>
        /// Describes a request for the request-headers scenario: one <c>name: value</c> line per
        /// inspected header that is present, whether an <c>Authorization</c> header was sent (never
        /// its value) and whether the query carries an <c>access_token</c>.
        /// </summary>
        public static string[] DescribeRequest(HttpRequest request)
        {
            List<string> lines = [];
            foreach (string name in _describedHeaders)
            {
                if (request.Headers.TryGetValue(name, out var value))
                {
                    lines.Add($"{name}: {value}");
                }
            }

            lines.Add($"authorization: {(request.Headers.ContainsKey("authorization") ? "present" : "absent")}");
            lines.Add($"access_token: {(request.Query.ContainsKey("access_token") ? "present" : "absent")}");
            return [.. lines];
        }

        /// <summary>Records the last negotiate request of a hub path.</summary>
        public static void RecordNegotiate(string hubPath, HttpRequest request) =>
            _negotiateHeaders[hubPath] = DescribeRequest(request);

        /// <summary>The last negotiate request of a hub path, described as <see cref="DescribeRequest"/> does.</summary>
        public static string[] NegotiateRequest(string hubPath) =>
            _negotiateHeaders.TryGetValue(hubPath, out var lines) ? lines : [];

        /// <summary>Answers the next stateful resume of <paramref name="connectionToken"/> with 404, as ASP.NET Core answers a resume of a connection it disposed.</summary>
        public static void RejectNextResume(string connectionToken) => _rejectedResumes[connectionToken] = 0;

        /// <summary>Whether a WebSocket request for <paramref name="connectionToken"/> is a resume to reject; counts it.</summary>
        public static bool TakeResumeRejection(string connectionToken)
        {
            if (!_rejectedResumes.TryRemove(connectionToken, out _))
            {
                return false;
            }

            Interlocked.Increment(ref _resumeRejections);
            return true;
        }

        /// <summary>How many resumes were rejected since the last reset.</summary>
        public static int ResumeRejections() => Volatile.Read(ref _resumeRejections);

        /// <summary>Counts a SignalR Close message a client sent.</summary>
        public static void CountClientClose() => Interlocked.Increment(ref _clientCloses);

        /// <summary>How many SignalR Close messages clients sent since the last reset.</summary>
        public static int ClientCloseMessages() => Volatile.Read(ref _clientCloses);

        public static bool IsRejected(string hubPath) =>
            _rejections.TryGetValue(hubPath, out var until) && DateTimeOffset.UtcNow < until;

        public static void Record(string connectionId, string value) =>
            _recordedSends.GetOrAdd(connectionId, static _ => new ConcurrentQueue<string>()).Enqueue(value);

        public static string[] Recorded(string connectionId) =>
            _recordedSends.TryGetValue(connectionId, out var values) ? values.ToArray() : [];

        public static void Connected(HubConnectionContext connection) =>
            _connections[connection.ConnectionId] = connection;

        public static void Disconnected(string connectionId)
        {
            _connections.TryRemove(connectionId, out _);
            _disconnects.AddOrUpdate(connectionId, 1, static (_, count) => count + 1);
        }

        public static int DisconnectCount(string connectionId) =>
            _disconnects.TryGetValue(connectionId, out var count) ? count : 0;

        /// <summary>Records the exception the hub's <c>OnDisconnectedAsync</c> received (empty for none).</summary>
        public static void RecordDisconnectError(string connectionId, Exception? exception) =>
            _disconnectErrors[connectionId] = exception?.Message ?? string.Empty;

        /// <summary>The <c>OnDisconnectedAsync</c> exception text of a connection: empty when it ended gracefully, null while connected.</summary>
        public static string? DisconnectError(string connectionId) =>
            _disconnectErrors.TryGetValue(connectionId, out var message) ? message : null;

        public static void RecordClientResult(string connectionId, int value) => _clientResults[connectionId] = value;

        public static int? ClientResult(string connectionId) =>
            _clientResults.TryGetValue(connectionId, out var value) ? value : null;

        /// <summary>Records the <see cref="HubException"/> text a client-result request observed.</summary>
        public static void RecordClientResultError(string connectionId, string message) =>
            _clientResultErrors[connectionId] = message;

        /// <summary>Returns and clears the last client-result error of a connection.</summary>
        public static string? TakeClientResultError(string connectionId) =>
            _clientResultErrors.TryRemove(connectionId, out var message) ? message : null;

        /// <summary>Sends a SignalR Close message to a connection after a delay and aborts it.</summary>
        public static void ScheduleClose(string connectionId, bool allowReconnect, TimeSpan delay)
        {
            if (!_connections.TryGetValue(connectionId, out var connection))
            {
                throw new HubException($"Connection {connectionId} is not tracked");
            }

            _ = Task.Run(async () =>
            {
                await Task.Delay(delay);
                await connection.WriteAsync(new CloseMessage("Server requested close", allowReconnect));
                connection.Abort();
            });
        }

        public static void RememberToken(string connectionToken, string name, string scheme) =>
            _authenticatedTokens[connectionToken] = (name, scheme);

        public static bool TryGetTokenUser(string connectionToken, out (string Name, string Scheme) user) =>
            _authenticatedTokens.TryGetValue(connectionToken, out user);

        /// <summary>Extracts the connection token from a negotiate response body.</summary>
        public static string? ConnectionToken(byte[] negotiateBody)
        {
            try
            {
                using var document = JsonDocument.Parse(negotiateBody);
                return document.RootElement.TryGetProperty("connectionToken", out var token)
                    ? token.GetString()
                    : null;
            }
            catch (JsonException)
            {
                return null;
            }
        }
    }

    /// <summary>Captures hub connections so the test service can send server-initiated Close messages.</summary>
    public sealed class TrackingHubLifetimeManager<THub>(ILogger<DefaultHubLifetimeManager<THub>> logger)
        : DefaultHubLifetimeManager<THub>(logger) where THub : Hub
    {
        public override Task OnConnectedAsync(HubConnectionContext connection)
        {
            TestControls.Connected(connection);
            return base.OnConnectedAsync(connection);
        }

        public override Task OnDisconnectedAsync(HubConnectionContext connection)
        {
            TestControls.Disconnected(connection.ConnectionId);
            return base.OnDisconnectedAsync(connection);
        }
    }
}
