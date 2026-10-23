using System.Diagnostics;
using System.Text.Json;
using MessagePack;
using Microsoft.AspNetCore.Http.Connections;
using Microsoft.AspNetCore.SignalR.Client;
using Microsoft.Extensions.DependencyInjection;

namespace dXn.SignalR.PerfReference;

/// <summary>
/// The official .NET client's side of the performance workloads. It runs exactly what the Rust
/// <c>perf</c> binary (<c>tests/perf</c>) runs, with the same command line:
/// <c>dXn.SignalR.PerfReference &lt;workload&gt; --protocol &lt;json|messagepack&gt; --scale &lt;quick|full&gt;</c>.
/// The service URL comes from <c>DXN_SIGNALR_E2E_SERVICE_URL</c>; the result is one JSON line.
/// Every workload first runs its operation untimed for the scale's warm-up (at least its minimum
/// count and duration), exactly as the Rust binary does, so the tiered JIT (left at its defaults,
/// including dynamic PGO) and first-use initialization settle before the timed window.
/// </summary>
static class Program
{
  const string ServiceUrlVariable = "DXN_SIGNALR_E2E_SERVICE_URL";

  static async Task<int> Main(string[] args)
  {
    Options options;
    try
    {
      options = Options.Parse(args);
    }
    catch(ArgumentException exception)
    {
      await Console.Error.WriteLineAsync(exception.Message);
      return 2;
    }

    string? service = Environment.GetEnvironmentVariable(ServiceUrlVariable);
    if(string.IsNullOrEmpty(service))
    {
      await Console.Error.WriteLineAsync($"{ServiceUrlVariable} is not set; run the workloads through the Aspire Performance category.");
      return 2;
    }

    Workloads workloads = new(new Uri(new Uri(service), "/perf"), options.Protocol, options.Scale);
    Dictionary<string, object> metrics = options.Workload switch
    {
      "invoke-latency" => await workloads.InvokeLatencyAsync(),
      "stream-throughput" => await workloads.StreamThroughputAsync(),
      "send-throughput" => await workloads.SendThroughputAsync(),
      "fan-in" => await workloads.FanInAsync(),
      _ => throw new InvalidOperationException($"unknown workload {options.Workload}")
    };

    Dictionary<string, object> result = new()
    {
      ["client"] = "dotnet",
      ["protocol"] = options.Protocol,
      ["workload"] = options.Workload,
      ["scale"] = options.Scale.Name,
#if DEBUG
      ["build_profile"] = "debug",
#else
      ["build_profile"] = "release",
#endif
      ["metrics"] = metrics
    };
    await Console.Out.WriteLineAsync(JsonSerializer.Serialize(result));
    return 0;
  }
}

/// <summary>The parsed command line.</summary>
sealed record Options(string Workload, string Protocol, Scale Scale)
{
  static readonly string[] _workloads = ["invoke-latency", "stream-throughput", "send-throughput", "fan-in"];

  public static Options Parse(string[] args)
  {
    string? workload = null;
    string protocol = "json";
    string scale = "quick";
    for(int index = 0; index < args.Length; index++)
    {
      switch(args[index])
      {
        case "--protocol" when index + 1 < args.Length:
          protocol = args[++index];
          break;
        case "--scale" when index + 1 < args.Length:
          scale = args[++index];
          break;
        default:
          workload = workload is null
            ? args[index]
            : throw new ArgumentException($"unexpected argument {args[index]}");
          break;
      }
    }

    if(workload is null || !_workloads.Contains(workload))
    {
      throw new ArgumentException($"usage: <{string.Join('|', _workloads)}> --protocol <json|messagepack> --scale <quick|full>");
    }

    if(protocol is not ("json" or "messagepack"))
    {
      throw new ArgumentException($"unknown protocol {protocol}");
    }

    return new Options(workload, protocol, Scale.Parse(scale));
  }
}

/// <summary>Iteration counts and warm-up per scale; identical to the Rust <c>perf</c> binary.</summary>
/// <param name="Name">The scale.</param>
/// <param name="Warmup">The least time every workload's untimed warm-up runs.</param>
/// <param name="WarmupCalls">The least invocations of the <c>invoke-latency</c> warm-up.</param>
/// <param name="WarmupStreamItems">The items of each <c>stream-throughput</c> warm-up stream.</param>
/// <param name="Calls">The timed <c>invoke-latency</c> calls.</param>
/// <param name="StreamItems">The timed stream's items.</param>
/// <param name="Sends">The timed sends.</param>
/// <param name="FanInClients">The concurrent <c>fan-in</c> clients.</param>
/// <param name="FanInCalls">The timed calls per <c>fan-in</c> client.</param>
sealed record Scale(
  string Name,
  TimeSpan Warmup,
  int WarmupCalls,
  int WarmupStreamItems,
  int Calls,
  int StreamItems,
  int Sends,
  int FanInClients,
  int FanInCalls)
{
  /// <summary>The characters of every echoed, sunk and streamed string.</summary>
  public const int PayloadCharacters = 32;

  public static Scale Parse(string name) => name switch
  {
    "quick" => new Scale("quick", TimeSpan.FromMilliseconds(200), 20, 1_000, 200, 10_000, 5_000, 8, 100),
    "full" => new Scale("full", TimeSpan.FromSeconds(2), 200, 10_000, 2_000, 100_000, 50_000, 8, 1_000),
    _ => throw new ArgumentException($"unknown scale {name}")
  };
}

/// <summary>One server-stream item (wire names match the test service and the Rust client).</summary>
[MessagePackObject]
public sealed class PerfItem
{
  [Key("sequence")]
  public int Sequence { get; set; }

  [Key("data")]
  public string Data { get; set; } = string.Empty;
}

/// <summary>The workloads against the test service's <c>/perf</c> hub.</summary>
sealed class Workloads(Uri hub, string protocol, Scale scale)
{
  readonly string _payload = new('x', Scale.PayloadCharacters);

  async Task<HubConnection> ConnectAsync()
  {
    IHubConnectionBuilder builder = new HubConnectionBuilder()
      .WithUrl(hub, HttpTransportType.WebSockets);
    if(protocol == "messagepack")
    {
      builder.AddMessagePackProtocol();
    }

    HubConnection connection = builder.Build();
    await connection.StartAsync();
    return connection;
  }

  /// <summary>Sequential Echo calls after a warm-up; latency percentiles in microseconds.</summary>
  public async Task<Dictionary<string, object>> InvokeLatencyAsync()
  {
    await using HubConnection connection = await ConnectAsync();
    long warmupStarted = Stopwatch.GetTimestamp();
    int warmupCalls = 0;
    while(warmupCalls < scale.WarmupCalls || Stopwatch.GetElapsedTime(warmupStarted) < scale.Warmup)
    {
      await EchoAsync(connection);
      warmupCalls++;
    }

    TimeSpan warmup = Stopwatch.GetElapsedTime(warmupStarted);
    double[] latencies = new double[scale.Calls];
    for(int call = 0; call < scale.Calls; call++)
    {
      long started = Stopwatch.GetTimestamp();
      await EchoAsync(connection);
      latencies[call] = Stopwatch.GetElapsedTime(started).TotalMicroseconds;
    }

    Array.Sort(latencies);
    return new Dictionary<string, object>
    {
      ["calls"] = scale.Calls,
      ["p50_us"] = Percentile(latencies, 0.50),
      ["p99_us"] = Percentile(latencies, 0.99),
      ["max_us"] = latencies[^1],
      ["warmup_calls"] = warmupCalls,
      ["warmup_ms"] = Milliseconds(warmup)
    };
  }

  /// <summary>Consumes one server stream of <paramref name="items"/> items in order.</summary>
  static async Task<int> ConsumeStreamAsync(HubConnection connection, int items)
  {
    int expected = 1;
    await foreach(PerfItem item in connection.StreamAsync<PerfItem>("Stream", items, Scale.PayloadCharacters))
    {
      if(item.Sequence != expected || item.Data.Length != Scale.PayloadCharacters)
      {
        throw new InvalidOperationException($"stream item {item.Sequence} arrived where {expected} was expected");
      }

      expected++;
    }

    if(expected - 1 != items)
    {
      throw new InvalidOperationException($"the stream ended after {expected - 1} of {items} items");
    }

    return items;
  }

  /// <summary>One server stream of <see cref="Scale.StreamItems"/> items, consumed in order, after warm-up streams.</summary>
  public async Task<Dictionary<string, object>> StreamThroughputAsync()
  {
    await using HubConnection connection = await ConnectAsync();
    long warmupStarted = Stopwatch.GetTimestamp();
    int warmupItems = 0;
    while(warmupItems == 0 || Stopwatch.GetElapsedTime(warmupStarted) < scale.Warmup)
    {
      warmupItems += await ConsumeStreamAsync(connection, scale.WarmupStreamItems);
    }

    TimeSpan warmup = Stopwatch.GetElapsedTime(warmupStarted);
    long started = Stopwatch.GetTimestamp();
    await ConsumeStreamAsync(connection, scale.StreamItems);
    TimeSpan elapsed = Stopwatch.GetElapsedTime(started);
    Dictionary<string, object> metrics = Throughput("items", scale.StreamItems, elapsed);
    metrics["warmup_items"] = warmupItems;
    metrics["warmup_ms"] = Milliseconds(warmup);
    return metrics;
  }

  static async Task SinkCountAsync(HubConnection connection, long expected)
  {
    long received = await connection.InvokeAsync<long>("SinkCount");
    if(received != expected)
    {
      throw new InvalidOperationException($"the service received {received} of {expected} sends");
    }
  }

  /// <summary>
  /// Sequential fire-and-forget sends, acknowledged by a final invocation, after warm-up sends that
  /// a <c>SinkCount</c> call confirms.
  /// </summary>
  public async Task<Dictionary<string, object>> SendThroughputAsync()
  {
    await using HubConnection connection = await ConnectAsync();
    long warmupStarted = Stopwatch.GetTimestamp();
    int warmupSends = 0;
    while(warmupSends == 0 || Stopwatch.GetElapsedTime(warmupStarted) < scale.Warmup)
    {
      await connection.SendAsync("Sink", _payload);
      warmupSends++;
    }

    await SinkCountAsync(connection, warmupSends);
    TimeSpan warmup = Stopwatch.GetElapsedTime(warmupStarted);
    long started = Stopwatch.GetTimestamp();
    for(int send = 0; send < scale.Sends; send++)
    {
      await connection.SendAsync("Sink", _payload);
    }

    await SinkCountAsync(connection, (long)warmupSends + scale.Sends);
    TimeSpan elapsed = Stopwatch.GetElapsedTime(started);
    Dictionary<string, object> metrics = Throughput("sends", scale.Sends, elapsed);
    metrics["warmup_sends"] = warmupSends;
    metrics["warmup_ms"] = Milliseconds(warmup);
    return metrics;
  }

  /// <summary>
  /// Runs <paramref name="calls"/> Echo calls (or, without a count, calls until the warm-up elapsed
  /// and at least one ran) on every connection concurrently; returns the total calls.
  /// </summary>
  async Task<int> EchoOnEveryConnectionAsync(HubConnection[] connections, int? calls)
  {
    int[] made = await Task.WhenAll(connections.Select(async connection =>
    {
      long started = Stopwatch.GetTimestamp();
      int count = 0;
      while(calls is { } limit ? count < limit : count == 0 || Stopwatch.GetElapsedTime(started) < scale.Warmup)
      {
        await EchoAsync(connection);
        count++;
      }

      return count;
    }));
    return made.Sum();
  }

  /// <summary>
  /// Concurrent clients each calling Echo sequentially, after every client warmed up; aggregate
  /// calls per second.
  /// </summary>
  public async Task<Dictionary<string, object>> FanInAsync()
  {
    HubConnection[] connections = await Task.WhenAll(Enumerable.Range(0, scale.FanInClients).Select(_ => ConnectAsync()));
    try
    {
      long warmupStarted = Stopwatch.GetTimestamp();
      int warmupCalls = await EchoOnEveryConnectionAsync(connections, calls: null);
      TimeSpan warmup = Stopwatch.GetElapsedTime(warmupStarted);
      long started = Stopwatch.GetTimestamp();
      int calls = await EchoOnEveryConnectionAsync(connections, scale.FanInCalls);
      TimeSpan elapsed = Stopwatch.GetElapsedTime(started);
      Dictionary<string, object> metrics = Throughput("calls", calls, elapsed);
      metrics["clients"] = scale.FanInClients;
      metrics["warmup_calls"] = warmupCalls;
      metrics["warmup_ms"] = Milliseconds(warmup);
      return metrics;
    }
    finally
    {
      foreach(HubConnection connection in connections)
      {
        await connection.DisposeAsync();
      }
    }
  }

  async Task EchoAsync(HubConnection connection)
  {
    string echoed = await connection.InvokeAsync<string>("Echo", _payload);
    if(echoed.Length != _payload.Length)
    {
      throw new InvalidOperationException("Echo returned a different payload");
    }
  }

  /// <summary>Nearest-rank percentile of sorted values (identical in the Rust client).</summary>
  static double Percentile(double[] sorted, double quantile)
  {
    int rank = (int)Math.Ceiling(quantile * sorted.Length);
    return sorted[Math.Clamp(rank - 1, 0, sorted.Length - 1)];
  }

  static double Milliseconds(TimeSpan duration) => Math.Round(duration.TotalMilliseconds, 3);

  static Dictionary<string, object> Throughput(string unit, int count, TimeSpan elapsed) => new()
  {
    [unit] = count,
    ["elapsed_ms"] = Milliseconds(elapsed),
    [$"{unit}_per_second"] = Math.Round(count / elapsed.TotalSeconds, 1)
  };
}
