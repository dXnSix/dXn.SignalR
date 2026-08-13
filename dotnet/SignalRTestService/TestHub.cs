using Microsoft.AspNetCore.SignalR;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.Runtime.CompilerServices;

namespace SignalRTestService
{
    public class TestHub : Hub
    {
        static readonly ConcurrentDictionary<string, CancellationProbeState> _cancellationProbes = new();

        public async Task<TestEntity> SingleEntity()
        {
            await Task.CompletedTask;

            return new TestEntity
            {
                Number = 1,
                Text = "test"
            };
        }

        public string ConnectionIdentity() => Context.ConnectionId;

        public bool ResetCancellationProbe()
        {
            _cancellationProbes[Context.ConnectionId] = new CancellationProbeState();
            return true;
        }

        public CancellationProbeSnapshot CancellationProbeStatus()
        {
            return GetCancellationProbe().Snapshot();
        }

        public async IAsyncEnumerable<int> CancellationProbeStream(
            int delayMilliseconds = 5,
            [EnumeratorCancellation] CancellationToken cancellationToken = default)
        {
            CancellationProbeState probe = GetCancellationProbe();
            Interlocked.Increment(ref probe.Started);
            Interlocked.Increment(ref probe.Active);
            int value = 0;
            try
            {
                while (true)
                {
                    await Task.Delay(delayMilliseconds, cancellationToken);
                    cancellationToken.ThrowIfCancellationRequested();
                    yield return ++value;
                }
            }
            finally
            {
                if (cancellationToken.IsCancellationRequested)
                {
                    Interlocked.Increment(ref probe.Canceled);
                }
                Interlocked.Decrement(ref probe.Active);
            }
        }

        public async Task AbortPhysicalTransport()
        {
            var httpContext = Context.GetHttpContext()
                ?? throw new InvalidOperationException("SignalR transport has no HTTP context");

            // Let the stateful protocol acknowledge the test command before severing only the
            // underlying HTTP transport. The logical SignalR connection remains resumable.
            await Task.Delay(1200, Context.ConnectionAborted);
            httpContext.Abort();
        }

        public async IAsyncEnumerable<int> OrderedSequence(int count, int delayMilliseconds = 1)
        {
            for (var value = 1; value <= count; value++)
            {
                if (Context.ConnectionAborted.IsCancellationRequested)
                {
                    yield break;
                }

                if (delayMilliseconds > 0)
                {
                    await Task.Delay(delayMilliseconds, Context.ConnectionAborted);
                }
                yield return value;
            }
        }

        public async Task TriggerOrderedCallbacks(string callback, int count)
        {
            for (var value = 1; value <= count; value++)
            {
                await Clients.Caller.SendAsync(callback, value, Context.ConnectionAborted);
            }
        }

        public async IAsyncEnumerable<TestEntity> HundredEntities()
        {
            for(var i = 0; i < 100; i ++)
            {
                await Task.CompletedTask;

                yield return new TestEntity
                {
                    Number = i + 1,
                    Text = $"test {i + 1}"
                };
            }
        }

        public async IAsyncEnumerable<TestEntity> MillionEntities()
        {
            for (var i = 0; i < 100000; i++)
            {
                await Task.CompletedTask;

                if (Context.ConnectionAborted.IsCancellationRequested)
                {
                    yield break;
                }

                yield return new TestEntity
                {
                    Number = i + 1,
                    Text = $"test {i + 1}"
                };
            }
        }

        public async Task<bool> PushEntity(TestEntity entity)
        {
            await Task.CompletedTask;

            return true;
        }

        public async Task<TestEntity> PushTwoEntities(TestEntity entity1, TestEntity entity2)
        {
            await Task.CompletedTask;

            return new TestEntity
            {
                Text = $"{entity1.Text}_{entity2.Text}",
                Number = entity1.Number + entity2.Number,
            };
        }

        public async Task TriggerCallback(string callback)
        {
            await Clients.Caller.SendAsync(callback);
        }

        public async Task TriggerEntityCallback(string callback)
        {
            await Clients.Caller.SendAsync(callback, new TestEntity
            {
                Text = "callback",
                Number = 1,
            });
        }

        public async Task<bool> TriggerEntityResponse(string callback)
        {
            var ret = await Clients.Caller.InvokeAsync<TestEntity>(callback, new TestEntity
            {
                Text = "requested response",
                Number = 1,
            }, CancellationToken.None);

            if(ret.Number == 1)
            {
                return true;
            }
            else
            {
                return false;
            }
        }

        public override Task OnDisconnectedAsync(Exception? exception)
        {
            Debug.WriteLine($"Client is disconnected: {Context.ConnectionId}");

            _cancellationProbes.TryRemove(Context.ConnectionId, out _);

            return base.OnDisconnectedAsync(exception);
        }

        public override Task OnConnectedAsync()
        {
            Debug.WriteLine($"Client is connecting: {Context.ConnectionId}");

            return base.OnConnectedAsync();
        }

        CancellationProbeState GetCancellationProbe()
        {
            return _cancellationProbes.GetOrAdd(
                Context.ConnectionId,
                static _ => new CancellationProbeState());
        }
    }

    public sealed class CancellationProbeState
    {
        public int Active;
        public int Started;
        public int Canceled;

        public CancellationProbeSnapshot Snapshot()
        {
            return new CancellationProbeSnapshot
            {
                Active = Volatile.Read(ref Active),
                Started = Volatile.Read(ref Started),
                Canceled = Volatile.Read(ref Canceled)
            };
        }
    }

    public sealed class CancellationProbeSnapshot
    {
        public int Active { get; set; }
        public int Started { get; set; }
        public int Canceled { get; set; }
    }

    public class TestEntity
    {
        public int Number { get; set; }
        public string Text { get; set; } = string.Empty;
    }
}
