using Microsoft.AspNetCore.SignalR;
using System.Collections.Concurrent;
using System.Runtime.CompilerServices;

namespace RayX.SignalR.TestService
{
    /// <summary>
    /// Test-only transport chaos for the soak scenario: while running, it aborts the current HTTP
    /// transport of every connection of one chaos hub at seeded random intervals, as a network
    /// that keeps dropping would. A stateful connection stays resumable (only its transport ends);
    /// an ordinary one ends. It also counts the fire-and-forget sends the hub received, to prove
    /// stateful mode neither loses nor duplicates them.
    /// </summary>
    public sealed class ChaosControl
    {
        static readonly ConcurrentDictionary<string, ChaosControl> _hubs = new(StringComparer.Ordinal);

        readonly ConcurrentDictionary<string, HubCallerContext> _connections = new(StringComparer.Ordinal);
        readonly ConcurrentDictionary<int, int> _sends = new();
        readonly ConcurrentDictionary<string, int> _transportAborts = new(StringComparer.Ordinal);
        readonly object _gate = new();
        CancellationTokenSource? _chaos;
        int _aborts;

        /// <summary>The control of the chaos hub <paramref name="hub"/>.</summary>
        public static ChaosControl For(string hub) => _hubs.GetOrAdd(hub, static _ => new ChaosControl());

        /// <summary>Stops every chaos run and clears every count, between end-to-end cells.</summary>
        public static void ResetAll()
        {
            foreach (ChaosControl control in _hubs.Values)
            {
                control.Stop();
                control._connections.Clear();
                control._sends.Clear();
                control._transportAborts.Clear();
                Interlocked.Exchange(ref control._aborts, 0);
            }
        }

        public void Track(HubCallerContext connection) => _connections[connection.ConnectionId] = connection;

        public void Untrack(string connectionId) => _connections.TryRemove(connectionId, out _);

        /// <summary>Starts aborting transports every <paramref name="minMilliseconds"/> to <paramref name="maxMilliseconds"/>.</summary>
        public void Start(int minMilliseconds, int maxMilliseconds, int seed)
        {
            CancellationTokenSource chaos = new();
            lock (_gate)
            {
                _chaos?.Cancel();
                _chaos = chaos;
            }

            Random random = new(seed);
            _ = Task.Run(async () =>
            {
                while (!chaos.IsCancellationRequested)
                {
                    try
                    {
                        await Task.Delay(random.Next(minMilliseconds, maxMilliseconds + 1), chaos.Token);
                    }
                    catch (OperationCanceledException)
                    {
                        return;
                    }

                    foreach (HubCallerContext connection in _connections.Values)
                    {
                        if (connection.GetHttpContext() is { } transport)
                        {
                            Interlocked.Increment(ref _aborts);
                            transport.Abort();
                        }
                    }
                }
            });
        }

        /// <summary>Stops the chaos run; returns how many transports it aborted.</summary>
        public int Stop()
        {
            lock (_gate)
            {
                _chaos?.Cancel();
                _chaos = null;
            }

            return Volatile.Read(ref _aborts);
        }

        /// <summary>Aborts <paramref name="connection"/>'s current HTTP transport right away (counted per connection).</summary>
        public void AbortNow(HubCallerContext connection)
        {
            if (connection.GetHttpContext() is not { } transport)
            {
                return;
            }

            string connectionId = connection.ConnectionId;
            // After the invocation returned, so its completion may or may not reach the client
            // first: both orders must survive the resume.
            _ = Task.Run(() =>
            {
                _transportAborts.AddOrUpdate(connectionId, 1, static (_, count) => count + 1);
                transport.Abort();
            });
        }

        public int TransportAborts(string connectionId) =>
            _transportAborts.TryGetValue(connectionId, out int count) ? count : 0;

        public void RecordSend(int value) => _sends.AddOrUpdate(value, 1, static (_, count) => count + 1);

        /// <summary>How many distinct sends arrived, and how many arrived more than once.</summary>
        public int[] SendStats() => [_sends.Count, _sends.Values.Count(count => count > 1)];
    }

    /// <summary>
    /// The soak scenario's mixed workload (test-only): invocations with results, counted
    /// fire-and-forget sends, ordered server streams, ordered server-to-client callbacks and
    /// client results, plus the chaos controls of its own <see cref="ChaosControl"/>.
    /// </summary>
    public abstract class ChaosHubBase(string name) : Hub
    {
        ChaosControl Control => ChaosControl.For(name);

        public override Task OnConnectedAsync()
        {
            Control.Track(Context);
            return base.OnConnectedAsync();
        }

        public override Task OnDisconnectedAsync(Exception? exception)
        {
            Control.Untrack(Context.ConnectionId);
            return base.OnDisconnectedAsync(exception);
        }

        public void StartChaos(int minMilliseconds, int maxMilliseconds, int seed) =>
            Control.Start(minMilliseconds, maxMilliseconds, seed);

        public int StopChaos() => Control.Stop();

        /// <summary>Aborts the caller's current transport right after returning (stateful-resume-stress).</summary>
        public void AbortTransport() => Control.AbortNow(Context);

        /// <summary>How many transports of the caller's connection <see cref="AbortTransport"/> aborted.</summary>
        public int TransportAborts() => Control.TransportAborts(Context.ConnectionId);

        public int Twice(int value) => value * 2;

        public void Count(int value) => Control.RecordSend(value);

        public int[] SendStats() => Control.SendStats();

        public async IAsyncEnumerable<int> Sequence(
            int count,
            int delayMilliseconds,
            [EnumeratorCancellation] CancellationToken cancellationToken)
        {
            for (int value = 1; value <= count; value++)
            {
                await Task.Delay(delayMilliseconds, cancellationToken);
                yield return value;
            }
        }

        /// <summary>Calls the client's <c>chaosCallback</c> with <paramref name="start"/> and the values after it, in order.</summary>
        public async Task TriggerCallbacks(int start, int count)
        {
            for (int value = start; value < start + count; value++)
            {
                await Clients.Caller.SendAsync("chaosCallback", value);
            }
        }

        /// <summary>Asks the client's <c>chaosResult</c> for a result and returns it.</summary>
        public Task<int> ClientResult(int value) =>
            Clients.Caller.InvokeAsync<int>("chaosResult", value, Context.ConnectionAborted);
    }

    /// <summary>Mapped at <c>/test-chaos</c> (ordinary connections).</summary>
    public sealed class ChaosHub() : ChaosHubBase("chaos");

    /// <summary>Mapped at <c>/test-chaos-stateful</c> with stateful reconnect allowed.</summary>
    public sealed class StatefulChaosHub() : ChaosHubBase("chaos-stateful");

    /// <summary>Hub options of the chaos hubs.</summary>
    public static class ChaosHubOptions
    {
        /// <summary>Lets a client-result call wait for the client while other calls of the same connection run.</summary>
        public static ISignalRServerBuilder AddChaosHubOptions(this ISignalRServerBuilder signalR) =>
            signalR
                .AddHubOptions<ChaosHub>(options => options.MaximumParallelInvocationsPerClient = 8)
                .AddHubOptions<StatefulChaosHub>(options => options.MaximumParallelInvocationsPerClient = 8);
    }
}
