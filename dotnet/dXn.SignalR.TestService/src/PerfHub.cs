using System.Runtime.CompilerServices;
using MessagePack;
using Microsoft.AspNetCore.SignalR;

namespace dXn.SignalR.TestService
{
    /// <summary>
    /// The performance workloads' hub (<c>/perf</c>): echo, server streams and counted sends, with
    /// no artificial delays, so the Rust client and the official .NET client measure the same work.
    /// </summary>
    public class PerfHub : Hub
    {
        const string SinkCounter = "perf-sink-count";

        public string Echo(string payload) => payload;

        public async IAsyncEnumerable<PerfItem> Stream(
            int count,
            int size,
            [EnumeratorCancellation] CancellationToken cancellationToken)
        {
            string data = new('x', size);
            for (int sequence = 1; sequence <= count; sequence++)
            {
                cancellationToken.ThrowIfCancellationRequested();
                yield return new PerfItem { Sequence = sequence, Data = data };
            }

            await Task.CompletedTask;
        }

        public void Sink(string payload)
        {
            ArgumentNullException.ThrowIfNull(payload);
            Counter().Increment();
        }

        public long SinkCount() => Counter().Value;

        // A connection's invocations run one at a time, so the lookup needs no lock.
        SinkCount Counter()
        {
            if (Context.Items.TryGetValue(SinkCounter, out object? value) && value is SinkCount count)
            {
                return count;
            }

            SinkCount created = new();
            Context.Items[SinkCounter] = created;
            return created;
        }
    }

    /// <summary>The number of <see cref="PerfHub.Sink"/> calls one connection made.</summary>
    public sealed class SinkCount
    {
        long _value;

        public long Value => Interlocked.Read(ref _value);

        public void Increment() => Interlocked.Increment(ref _value);
    }

    /// <summary>One server-stream item; wire names match the Rust and .NET perf clients.</summary>
    [MessagePackObject]
    public sealed class PerfItem
    {
        [Key("sequence")]
        public int Sequence { get; set; }

        [Key("data")]
        public string Data { get; set; } = string.Empty;
    }
}
