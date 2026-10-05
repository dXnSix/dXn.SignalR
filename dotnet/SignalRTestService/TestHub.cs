using MessagePack;
using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Connections.Features;
using Microsoft.AspNetCore.SignalR;
using System.Collections.Concurrent;
using System.Text.Json.Serialization;
using System.Diagnostics;
using System.Runtime.CompilerServices;

namespace SignalRTestService
{
    public class TestHub(IHubContext<TestHub> hubContext) : Hub
    {
        static readonly ConcurrentDictionary<string, CancellationProbeState> _cancellationProbes = new();
        static readonly ConcurrentDictionary<string, int> _transportAborts = new();

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

        public TransportDetails TransportDetails() => new()
        {
            ConnectionId = Context.ConnectionId,
            TransportId = Context.GetHttpContext()?.Request.Query["id"].ToString() ?? string.Empty,
            TransferFormat = Context.Features.Get<ITransferFormatFeature>()?.ActiveFormat.ToString() ?? string.Empty
        };

        public int Echo(int value) => value;

        public Task VoidCall()
        {
            TestControls.Record(Context.ConnectionId, "void-call");
            return Task.CompletedTask;
        }

        public int Fail(string message) => throw new InvalidOperationException(message);

        public ComplexEntity EchoComplex(ComplexEntity entity) => new()
        {
            EntityId = entity.EntityId + 1,
            DisplayName = entity.DisplayName.ToUpperInvariant(),
            Tags = [.. entity.Tags, "server"],
            Nested = entity.Nested is null ? null : new TestEntity { Text = entity.Nested.Text, Number = entity.Nested.Number * 2 },
            OptionalNote = entity.OptionalNote
        };

        public string DescribeComplex(ComplexEntity entity) =>
            $"{entity.EntityId}|{entity.DisplayName}|{string.Join(',', entity.Tags)}|{entity.Nested?.Text}:{entity.Nested?.Number}|{entity.OptionalNote ?? "null"}";

        public void Record(string value) => TestControls.Record(Context.ConnectionId, value);

        public void RecordPair(string text, int number) => TestControls.Record(Context.ConnectionId, $"{text}:{number}");

        public string[] RecordedSends() => TestControls.Recorded(Context.ConnectionId);

        public async IAsyncEnumerable<int> FailingSequence(int count, int failAfter)
        {
            for (var value = 1; value <= count; value++)
            {
                if (value > failAfter)
                {
                    throw new InvalidOperationException($"FailingSequence failed after {failAfter} items");
                }

                await Task.Yield();
                yield return value;
            }
        }

        public Task TriggerMultiArgumentCallback(string callback) =>
            Clients.Caller.SendAsync(callback, "alpha", 42, new TestEntity { Text = "multi", Number = 7 });

        /// <summary>
        /// Asks the caller for a client result outside this invocation (the hub processes one
        /// invocation per connection at a time) and records the value for <see cref="ClientResult"/>.
        /// </summary>
        public void RequestClientResult(string callback, int value)
        {
            string connectionId = Context.ConnectionId;
            _ = Task.Run(async () =>
            {
                using CancellationTokenSource timeout = new(TimeSpan.FromSeconds(20));
                int result = await hubContext.Clients.Client(connectionId).InvokeAsync<int>(callback, value, timeout.Token);
                TestControls.RecordClientResult(connectionId, result);
            });
        }

        public int? ClientResult() => TestControls.ClientResult(Context.ConnectionId);

        public int DisconnectCount(string connectionId) => TestControls.DisconnectCount(connectionId);

        public string EchoText(string payload) => payload;

        public int TextLength(string payload) => payload.Length;

        public string LargeText(int length) => new('x', length);

        public void ScheduleServerClose(bool allowReconnect, int delayMilliseconds) =>
            TestControls.ScheduleClose(Context.ConnectionId, allowReconnect, TimeSpan.FromMilliseconds(delayMilliseconds));

        public void RejectNegotiation(string hubPath, int milliseconds) =>
            TestControls.RejectNegotiation(hubPath, TimeSpan.FromMilliseconds(milliseconds));

        public int NegotiateCount(string hubPath) => TestControls.NegotiateCount(hubPath);

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
            probe.Start();
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
                probe.Finish(cancellationToken.IsCancellationRequested);
            }
        }

        public async Task AbortPhysicalTransport()
        {
            var httpContext = Context.GetHttpContext()
                ?? throw new InvalidOperationException("SignalR transport has no HTTP context");

            // Let the stateful protocol acknowledge the test command before severing only the
            // underlying HTTP transport. The logical SignalR connection remains resumable.
            await Task.Delay(1200, Context.ConnectionAborted);
            _transportAborts.AddOrUpdate(Context.ConnectionId, 1, static (_, count) => count + 1);
            httpContext.Abort();
        }

        public int TransportAbortCount() =>
            _transportAborts.TryGetValue(Context.ConnectionId, out var count) ? count : 0;

        public async Task<string> DelayedConnectionIdentity(int delayMilliseconds)
        {
            await Task.Delay(delayMilliseconds, Context.ConnectionAborted);
            return Context.ConnectionId;
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
            _transportAborts.TryRemove(Context.ConnectionId, out _);

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
        int _active;
        int _started;
        int _canceled;

        public void Start()
        {
            Interlocked.Increment(ref _started);
            Interlocked.Increment(ref _active);
        }

        public void Finish(bool canceled)
        {
            if (canceled)
            {
                Interlocked.Increment(ref _canceled);
            }
            Interlocked.Decrement(ref _active);
        }

        public CancellationProbeSnapshot Snapshot()
        {
            return new CancellationProbeSnapshot
            {
                Active = Volatile.Read(ref _active),
                Started = Volatile.Read(ref _started),
                Canceled = Volatile.Read(ref _canceled)
            };
        }
    }

    public sealed class CancellationProbeSnapshot
    {
        public int Active { get; set; }
        public int Started { get; set; }
        public int Canceled { get; set; }
    }

    /// <summary>Requires the test authentication scheme; reports the authenticated identity.</summary>
    [Authorize]
    public class AuthHub : Hub
    {
        public string AuthenticatedIdentity() =>
            $"{Context.User?.Identity?.AuthenticationType}:{Context.User?.Identity?.Name}";

        /// <summary>How the WebSocket request was authenticated: header, access_token or token-binding.</summary>
        public string AuthenticationSource() =>
            Context.User?.FindFirst(TestAuthenticationHandler.SourceClaim)?.Value ?? "none";
    }

    public sealed class TransportDetails
    {
        public string ConnectionId { get; set; } = string.Empty;
        public string TransportId { get; set; } = string.Empty;
        public string TransferFormat { get; set; } = string.Empty;
    }

    /// <summary>A DTO whose wire names are snake_case on both protocols, matching Serde renames.</summary>
    [MessagePackObject]
    public sealed class ComplexEntity
    {
        [Key("entity_id")]
        [JsonPropertyName("entity_id")]
        public int EntityId { get; set; }

        [Key("display_name")]
        [JsonPropertyName("display_name")]
        public string DisplayName { get; set; } = string.Empty;

        [Key("tags")]
        [JsonPropertyName("tags")]
        public List<string> Tags { get; set; } = [];

        [Key("nested")]
        [JsonPropertyName("nested")]
        public TestEntity? Nested { get; set; }

        [Key("optional_note")]
        [JsonPropertyName("optional_note")]
        public string? OptionalNote { get; set; }
    }

    /// <summary>Wire names match the Rust scenarios' Serde names on both protocols.</summary>
    [MessagePackObject]
    public class TestEntity
    {
        [Key("number")]
        public int Number { get; set; }

        [Key("text")]
        public string Text { get; set; } = string.Empty;
    }
}
