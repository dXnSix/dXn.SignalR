using System.Collections.Concurrent;
using System.Text.Json;
using Microsoft.AspNetCore.SignalR;
using Microsoft.AspNetCore.SignalR.Protocol;

namespace SignalRTestService
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
        static readonly ConcurrentDictionary<string, int> _clientResults = new();
        static readonly ConcurrentDictionary<string, HubConnectionContext> _connections = new();
        static readonly ConcurrentDictionary<string, (string Name, string Scheme)> _authenticatedTokens = new();

        /// <summary>Clears every probe and control between end-to-end cells.</summary>
        public static void Reset()
        {
            _negotiateCounts.Clear();
            _rejections.Clear();
            _recordedSends.Clear();
            _disconnects.Clear();
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

        public static void RecordClientResult(string connectionId, int value) => _clientResults[connectionId] = value;

        public static int? ClientResult(string connectionId) =>
            _clientResults.TryGetValue(connectionId, out var value) ? value : null;

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
