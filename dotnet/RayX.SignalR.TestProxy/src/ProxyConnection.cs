using System.Buffers;
using System.Collections.Concurrent;
using System.Globalization;
using System.IO.Pipelines;
using System.Net;
using System.Net.Http.Json;
using System.Net.Sockets;
using System.Text;
using System.Text.Json;
using Microsoft.AspNetCore.Connections;

namespace RayX.SignalR.TestProxy;

/// <summary>The proxy's configuration and counters.</summary>
/// <param name="user">The user name tunnels must authenticate with.</param>
/// <param name="password">The password tunnels must authenticate with.</param>
/// <param name="denied">The <c>host:port</c> targets the proxy refuses with 403.</param>
/// <param name="service">
/// The test service's URL: tunnels to its allocated ports go straight to the ports it listens on
/// (past Aspire's own endpoint proxy), so the service sees this proxy's tunnels as its peers.
/// </param>
/// <param name="servicePorts">The service's allocated ports by scheme.</param>
public sealed class ProxyState(
  string user,
  string password,
  IReadOnlySet<string> denied,
  Uri? service,
  IReadOnlyDictionary<string, int> servicePorts) : IDisposable
{
  readonly ConcurrentQueue<int> _tunnelPorts = new();
  readonly SemaphoreSlim _routesGate = new(1, 1);
  IReadOnlyDictionary<int, int>? _routes;
  int _tunnels;
  int _refused;

  /// <summary>
  /// Reads <c>TestProxy:User</c>, <c>TestProxy:Password</c>, <c>TestProxy:Deny</c>,
  /// <c>TestProxy:Service</c> and <c>TestProxy:ServicePorts:&lt;scheme&gt;</c>.
  /// </summary>
  /// <param name="configuration">The configuration.</param>
  /// <returns>The state.</returns>
  public static ProxyState From(IConfiguration configuration) => new(
    configuration["TestProxy:User"] ?? "rayx",
    // The credential check is never off: the test runner always provides a password.
    configuration["TestProxy:Password"] is { Length: > 0 } password
      ? password
      : throw new InvalidOperationException("TestProxy:Password is required."),
    (configuration["TestProxy:Deny"] ?? string.Empty)
      .Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries)
      .ToHashSet(StringComparer.OrdinalIgnoreCase),
    Uri.TryCreate(configuration["TestProxy:Service"], UriKind.Absolute, out Uri? service) ? service : null,
    configuration
      .GetSection("TestProxy:ServicePorts")
      .GetChildren()
      .Where(port => int.TryParse(port.Value, CultureInfo.InvariantCulture, out _))
      .ToDictionary(
        port => port.Key,
        port => int.Parse(port.Value!, CultureInfo.InvariantCulture),
        StringComparer.OrdinalIgnoreCase));

  /// <summary>The local port a tunnel to <paramref name="port"/> on the loopback connects to.</summary>
  /// <param name="port">The requested port.</param>
  /// <param name="cancellationToken">Cancels the lookup.</param>
  /// <returns>The routed port.</returns>
  public async Task<int> RouteAsync(int port, CancellationToken cancellationToken)
  {
    IReadOnlyDictionary<int, int> routes = await RoutesAsync(cancellationToken);
    return routes.TryGetValue(port, out int target) ? target : port;
  }

  /// <summary>Asks the service once which ports it listens on, by scheme.</summary>
  async Task<IReadOnlyDictionary<int, int>> RoutesAsync(CancellationToken cancellationToken)
  {
    if(_routes is not null || service is null)
    {
      return _routes ?? new Dictionary<int, int>();
    }

    await _routesGate.WaitAsync(cancellationToken);
    try
    {
      if(_routes is null)
      {
        // Directly: the machine's proxy settings must not apply to this test-only lookup.
        using HttpClientHandler handler = new() { UseProxy = false };
        using HttpClient client = new(handler);
        Dictionary<string, int> listening = await client.GetFromJsonAsync<Dictionary<string, int>>(
          new Uri(service, "/e2e/listening"),
          cancellationToken) ?? [];
        _routes = servicePorts
          .Where(allocated => listening.ContainsKey(allocated.Key))
          .ToDictionary(allocated => allocated.Value, allocated => listening[allocated.Key]);
      }

      return _routes;
    }
    finally
    {
      _routesGate.Release();
    }
  }

  /// <inheritdoc/>
  public void Dispose() => _routesGate.Dispose();

  /// <summary>Whether <paramref name="authorization"/> carries the required credentials.</summary>
  /// <param name="authorization">The <c>Proxy-Authorization</c> value, if any.</param>
  /// <returns>Whether the tunnel may open.</returns>
  public bool Authorized(string? authorization)
  {
    string expected = "Basic " + Convert.ToBase64String(Encoding.UTF8.GetBytes($"{user}:{password}"));
    return string.Equals(authorization, expected, StringComparison.Ordinal);
  }

  /// <summary>Whether the target is on the deny list.</summary>
  /// <param name="target">The CONNECT target.</param>
  /// <returns>Whether it is refused.</returns>
  public bool Denied(string target) => denied.Contains(target);

  /// <summary>Records an open tunnel and the local port of its upstream connection.</summary>
  /// <param name="localPort">The port.</param>
  public void Opened(int localPort)
  {
    _tunnelPorts.Enqueue(localPort);
    Interlocked.Increment(ref _tunnels);
  }

  /// <summary>Records a refused tunnel.</summary>
  public void Refused() => Interlocked.Increment(ref _refused);

  /// <summary>The counters as JSON.</summary>
  /// <returns>The JSON text.</returns>
  public string Stats() => JsonSerializer.Serialize(new
  {
    tunnels = Volatile.Read(ref _tunnels),
    refused = Volatile.Read(ref _refused),
    ports = _tunnelPorts.ToArray()
  });
}

/// <summary>Handles one raw connection: a CONNECT tunnel, or a small HTTP request.</summary>
public static class ProxyConnection
{
  const int HeadLimit = 16 * 1024;

  /// <summary>Handles <paramref name="connection"/> until it ends.</summary>
  /// <param name="connection">The connection.</param>
  /// <param name="state">The proxy state.</param>
  /// <returns>A task that completes with the connection.</returns>
  public static async Task HandleAsync(ConnectionContext connection, ProxyState state)
  {
    PipeReader input = connection.Transport.Input;
    PipeWriter output = connection.Transport.Output;
    string? head = await ReadHeadAsync(input, connection.ConnectionClosed);
    if(head is null)
    {
      return;
    }

    string[] lines = head.Split("\r\n");
    string[] request = lines[0].Split(' ');
    if(request.Length < 2)
    {
      await RespondAsync(output, 400, "Bad Request");
      return;
    }

    string method = request[0];
    string target = request[1];
    if(method == "CONNECT")
    {
      string? authorization = lines
        .Skip(1)
        .Select(line => line.Split(':', 2))
        .Where(parts => parts.Length == 2 && parts[0].Trim().Equals("Proxy-Authorization", StringComparison.OrdinalIgnoreCase))
        .Select(parts => parts[1].Trim())
        .FirstOrDefault();
      await TunnelAsync(target, authorization, input, output, state, connection.ConnectionClosed);
      return;
    }

    switch((method, target))
    {
      case ("GET", "/health"):
        await RespondAsync(output, 200, "OK", "ready");
        break;
      case ("GET", "/stats"):
        await RespondAsync(output, 200, "OK", state.Stats(), "application/json");
        break;
      default:
        await RespondAsync(output, 404, "Not Found");
        break;
    }
  }

  static async Task TunnelAsync(
    string target,
    string? authorization,
    PipeReader input,
    PipeWriter output,
    ProxyState state,
    CancellationToken closed)
  {
    if(!state.Authorized(authorization))
    {
      state.Refused();
      await WriteAsync(output, "HTTP/1.1 407 Proxy Authentication Required\r\nProxy-Authenticate: Basic realm=\"rayx\"\r\nContent-Length: 0\r\n\r\n");
      return;
    }

    if(state.Denied(target))
    {
      state.Refused();
      await RespondAsync(output, 403, "Forbidden");
      return;
    }

    int separator = target.LastIndexOf(':');
    if(separator <= 0 || !int.TryParse(target[(separator + 1)..], NumberStyles.None, CultureInfo.InvariantCulture, out int port))
    {
      await RespondAsync(output, 400, "Bad Request");
      return;
    }

    string host = target[..separator].Trim('[', ']');
    // The Android emulator reaches the host loopback as 10.0.2.2; this proxy runs on the host.
    if(host == "10.0.2.2")
    {
      host = IPAddress.Loopback.ToString();
    }

    if(host is "localhost" or "127.0.0.1" or "::1")
    {
      host = IPAddress.Loopback.ToString();
      try
      {
        port = await state.RouteAsync(port, closed);
      }
      catch(Exception exception) when (exception is HttpRequestException or System.Text.Json.JsonException or TaskCanceledException)
      {
        await RespondAsync(output, 502, "Bad Gateway");
        return;
      }
    }

    using TcpClient upstream = new();
    try
    {
      await upstream.ConnectAsync(host, port, closed);
    }
    catch(SocketException)
    {
      await RespondAsync(output, 502, "Bad Gateway");
      return;
    }

    state.Opened(((IPEndPoint)upstream.Client.LocalEndPoint!).Port);
    await WriteAsync(output, "HTTP/1.1 200 Connection established\r\n\r\n");
    NetworkStream stream = upstream.GetStream();
    // When either direction ends, the other is canceled and both are awaited before the pipes
    // complete, so nothing writes to a completed pipe.
    using CancellationTokenSource tunnel = CancellationTokenSource.CreateLinkedTokenSource(closed);
    Task toUpstream = input.CopyToAsync(stream, tunnel.Token);
    Task toClient = stream.CopyToAsync(output, tunnel.Token);
    await Task.WhenAny(toUpstream, toClient);
    await tunnel.CancelAsync();
    upstream.Close();
    try
    {
      await Task.WhenAll(toUpstream, toClient);
    }
    catch(Exception exception) when (exception is OperationCanceledException or IOException or ObjectDisposedException or SocketException)
    {
      // The tunnel ended; how each direction stopped does not matter.
    }

    await output.CompleteAsync();
    await input.CompleteAsync();
  }

  static async Task<string?> ReadHeadAsync(PipeReader input, CancellationToken closed)
  {
    while(true)
    {
      ReadResult result;
      try
      {
        result = await input.ReadAsync(closed);
      }
      catch(OperationCanceledException)
      {
        return null;
      }

      ReadOnlySequence<byte> buffer = result.Buffer;
      byte[] bytes = buffer.ToArray();
      int end = IndexOfHeadEnd(bytes);
      if(end >= 0)
      {
        input.AdvanceTo(buffer.GetPosition(end + 4));
        return Encoding.ASCII.GetString(bytes, 0, end);
      }

      if(result.IsCompleted || bytes.Length > HeadLimit)
      {
        return null;
      }

      input.AdvanceTo(buffer.Start, buffer.End);
    }
  }

  static int IndexOfHeadEnd(byte[] bytes)
  {
    for(int index = 0; index + 3 < bytes.Length; index++)
    {
      if(bytes[index] == '\r' && bytes[index + 1] == '\n' && bytes[index + 2] == '\r' && bytes[index + 3] == '\n')
      {
        return index;
      }
    }

    return -1;
  }

  static Task RespondAsync(PipeWriter output, int status, string reason, string body = "", string contentType = "text/plain") =>
    WriteAsync(output, $"HTTP/1.1 {status} {reason}\r\nContent-Type: {contentType}\r\nContent-Length: {Encoding.UTF8.GetByteCount(body)}\r\nConnection: close\r\n\r\n{body}");

  static async Task WriteAsync(PipeWriter output, string text)
  {
    await output.WriteAsync(Encoding.UTF8.GetBytes(text));
    await output.FlushAsync();
  }
}
