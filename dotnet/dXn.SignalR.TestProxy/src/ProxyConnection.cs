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

namespace dXn.SignalR.TestProxy;

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
  readonly ConcurrentQueue<int> _socksAddressTypes = new();
  readonly SemaphoreSlim _routesGate = new(1, 1);
  IReadOnlyDictionary<int, int>? _routes;
  int _tunnels;
  int _refused;
  int _socks;
  int _pacScripts;

  /// <summary>
  /// Gets or sets the loopback port of the listener that needs no credentials, for the routes of
  /// PAC scripts (which cannot carry credentials).
  /// </summary>
  public int OpenPort { get; set; }

  /// <summary>Records a served PAC script.</summary>
  public void PacServed() => Interlocked.Increment(ref _pacScripts);

  /// <summary>Gets or sets the loopback port of the TLS listener (<c>https://</c> proxies).</summary>
  public int TlsPort { get; set; }

  int _tlsSessions;

  /// <summary>Records a TLS session a client opened with the TLS listener.</summary>
  public void TlsSession() => Interlocked.Increment(ref _tlsSessions);

  /// <summary>
  /// Reads <c>TestProxy:User</c>, <c>TestProxy:Password</c>, <c>TestProxy:Deny</c>,
  /// <c>TestProxy:Service</c> and <c>TestProxy:ServicePorts:&lt;scheme&gt;</c>.
  /// </summary>
  /// <param name="configuration">The configuration.</param>
  /// <returns>The state.</returns>
  public static ProxyState From(IConfiguration configuration) => new(
    configuration["TestProxy:User"] ?? "dxn",
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

  /// <summary>Whether a SOCKS5 user name and password are the required credentials.</summary>
  /// <param name="name">The user name.</param>
  /// <param name="secret">The password.</param>
  /// <returns>Whether the tunnel may open.</returns>
  public bool Authorized(string name, string secret) =>
    string.Equals(name, user, StringComparison.Ordinal) && string.Equals(secret, password, StringComparison.Ordinal);

  /// <summary>Records a SOCKS5 request and the address type it named the destination with.</summary>
  /// <param name="addressType">The SOCKS5 address type (1 IPv4, 3 domain name, 4 IPv6).</param>
  public void SocksRequested(int addressType)
  {
    _socksAddressTypes.Enqueue(addressType);
    Interlocked.Increment(ref _socks);
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
    ports = _tunnelPorts.ToArray(),
    socks = Volatile.Read(ref _socks),
    socksAddressTypes = _socksAddressTypes.ToArray(),
    pacScripts = Volatile.Read(ref _pacScripts),
    openPort = OpenPort,
    tlsPort = TlsPort,
    tlsSessions = Volatile.Read(ref _tlsSessions)
  });
}

/// <summary>
/// Handles one raw connection: a SOCKS5 tunnel (recognized by its first byte), a CONNECT tunnel,
/// or a small HTTP request.
/// </summary>
public static class ProxyConnection
{
  const int HeadLimit = 16 * 1024;

  /// <summary>Handles <paramref name="connection"/> until it ends.</summary>
  /// <param name="connection">The connection.</param>
  /// <param name="state">The proxy state.</param>
  /// <returns>A task that completes with the connection.</returns>
  public static Task HandleAsync(ConnectionContext connection, ProxyState state) =>
    HandleAsync(connection.Transport.Input, connection.Transport.Output, state, open: false, connection.ConnectionClosed);

  /// <summary>Handles one connection's pipes until it ends.</summary>
  /// <param name="input">The connection's input.</param>
  /// <param name="output">The connection's output.</param>
  /// <param name="state">The proxy state.</param>
  /// <param name="open">Whether tunnels need no credentials (the PAC routes' listener).</param>
  /// <param name="closed">Signals the connection's end.</param>
  /// <returns>A task that completes with the connection.</returns>
  public static async Task HandleAsync(PipeReader input, PipeWriter output, ProxyState state, bool open, CancellationToken closed)
  {
    if(await PeekAsync(input, closed) == Socks5)
    {
      await Socks5Async(input, output, state, open, closed);
      return;
    }

    string? head = await ReadHeadAsync(input, closed);
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
      await TunnelAsync(target, open ? null : authorization, input, output, state, open, closed);
      return;
    }

    if(method == "GET" && target.StartsWith("/proxy.pac", StringComparison.Ordinal))
    {
      string host = lines
        .Skip(1)
        .Select(line => line.Split(':', 2))
        .Where(parts => parts.Length == 2 && parts[0].Trim().Equals("Host", StringComparison.OrdinalIgnoreCase))
        .Select(parts => parts[1].Trim())
        .FirstOrDefault() ?? "127.0.0.1";
      state.PacServed();
      await RespondAsync(output, 200, "OK", PacScript(target, host, state.OpenPort), "application/x-ns-proxy-autoconfig");
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

  /// <summary>
  /// A PAC script whose <c>FindProxyForURL</c> routes plain (<c>http</c>) and secure
  /// (<c>https</c>) endpoints as the query names: <c>proxy</c> (this proxy's credential-free
  /// listener over HTTP CONNECT), <c>socks5</c> (the same listener over SOCKS5), <c>direct</c>,
  /// <c>dead</c> (an unreachable proxy, then <c>DIRECT</c>), or <c>throw</c> (the script fails).
  /// </summary>
  /// <param name="target">The request target with its query.</param>
  /// <param name="hostHeader">The Host header the client reached this proxy with.</param>
  /// <param name="openPort">The credential-free listener's port.</param>
  /// <returns>The script.</returns>
  public static string PacScript(string target, string hostHeader, int openPort)
  {
    string query = target.Contains('?', StringComparison.Ordinal) ? target[(target.IndexOf('?', StringComparison.Ordinal) + 1)..] : string.Empty;
    Dictionary<string, string> kinds = query
      .Split('&', StringSplitOptions.RemoveEmptyEntries)
      .Select(pair => pair.Split('=', 2))
      .Where(pair => pair.Length == 2)
      .ToDictionary(pair => pair[0], pair => pair[1], StringComparer.Ordinal);
    // The address the client reached this proxy with, as a bracketed IPv6 address or a host.
    string host = hostHeader.StartsWith('[') ? hostHeader[..(hostHeader.IndexOf(']', StringComparison.Ordinal) + 1)] : hostHeader.Split(':')[0];
    // The credential-free listener is on the IPv4 loopback: name it so, rather than make every
    // route try `localhost`'s IPv6 address first.
    if(host.Equals("localhost", StringComparison.OrdinalIgnoreCase))
    {
      host = "127.0.0.1";
    }
    string Route(string scheme) => kinds.GetValueOrDefault(scheme, "direct") switch
    {
      "proxy" => $"PROXY {host}:{openPort}",
      "socks5" => $"SOCKS5 {host}:{openPort}",
      "dead" => $"PROXY {host}:1; DIRECT",
      _ => "DIRECT"
    };
    if(kinds.ContainsValue("throw"))
    {
      return "function FindProxyForURL(url, host) { throw new Error('a failing PAC script'); }";
    }

    return "function FindProxyForURL(url, host) {\n"
           + $"  if (shExpMatch(url, 'https:*')) {{ return '{Route("https")}'; }}\n"
           + $"  return '{Route("http")}';\n"
           + "}\n";
  }

  static async Task TunnelAsync(
    string target,
    string? authorization,
    PipeReader input,
    PipeWriter output,
    ProxyState state,
    bool open,
    CancellationToken closed)
  {
    if(!open && !state.Authorized(authorization))
    {
      state.Refused();
      await WriteAsync(output, "HTTP/1.1 407 Proxy Authentication Required\r\nProxy-Authenticate: Basic realm=\"dxn\"\r\nContent-Length: 0\r\n\r\n");
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

    using TcpClient? upstream = await ConnectUpstreamAsync(target[..separator].Trim('[', ']'), port, state, closed);
    if(upstream is null)
    {
      await RespondAsync(output, 502, "Bad Gateway");
      return;
    }

    state.Opened(((IPEndPoint)upstream.Client.LocalEndPoint!).Port);
    await WriteAsync(output, "HTTP/1.1 200 Connection established\r\n\r\n");
    await RelayAsync(upstream, input, output, closed);
  }

  /// <summary>
  /// Connects to the tunnel's destination: the service's loopback ports go to the ports it listens
  /// on, and the Android emulator's host alias (10.0.2.2) to the loopback.
  /// </summary>
  static async Task<TcpClient?> ConnectUpstreamAsync(string host, int port, ProxyState state, CancellationToken closed)
  {
    // The Android emulator reaches the host loopback as 10.0.2.2; this proxy runs on the host.
    // `dxn-os-proxy.test` resolves only here, to the service, so a connection to it proves the
    // client used this proxy (the os-proxy scenario).
    if(host is "10.0.2.2" or "localhost" or "127.0.0.1" or "::1" or "dxn-os-proxy.test")
    {
      host = IPAddress.Loopback.ToString();
      try
      {
        port = await state.RouteAsync(port, closed);
      }
      catch(Exception exception) when (exception is HttpRequestException or System.Text.Json.JsonException or TaskCanceledException)
      {
        return null;
      }
    }

    TcpClient upstream = new();
    try
    {
      await upstream.ConnectAsync(host, port, closed);
      return upstream;
    }
    catch(SocketException)
    {
      upstream.Dispose();
      return null;
    }
  }

  /// <summary>Relays both directions of an open tunnel until either ends.</summary>
  static async Task RelayAsync(TcpClient upstream, PipeReader input, PipeWriter output, CancellationToken closed)
  {
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

  const byte Socks5 = 5;

  /// <summary>The first byte of the connection, without consuming it (-1 when it ended).</summary>
  static async Task<int> PeekAsync(PipeReader input, CancellationToken closed)
  {
    try
    {
      ReadResult result = await input.ReadAsync(closed);
      int first = result.Buffer.IsEmpty ? -1 : result.Buffer.FirstSpan[0];
      // Nothing consumed or examined: the next read sees the same bytes at once.
      input.AdvanceTo(result.Buffer.Start);
      return first;
    }
    catch(OperationCanceledException)
    {
      return -1;
    }
  }

  /// <summary>Reads exactly <paramref name="count"/> bytes, or <c>null</c> when the connection ends first.</summary>
  static async Task<byte[]?> ReadExactAsync(PipeReader input, int count, CancellationToken closed)
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

      if(result.Buffer.Length >= count)
      {
        byte[] bytes = result.Buffer.Slice(0, count).ToArray();
        input.AdvanceTo(result.Buffer.GetPosition(count));
        return bytes;
      }

      if(result.IsCompleted)
      {
        return null;
      }

      input.AdvanceTo(result.Buffer.Start, result.Buffer.End);
    }
  }

  /// <summary>
  /// A SOCKS5 tunnel (RFC 1928) that requires the run's credentials (RFC 1929): a request without
  /// them gets no acceptable method, wrong ones a failed sub-negotiation, and a denied target reply
  /// 2 (connection not allowed by ruleset); each refusal is counted.
  /// </summary>
  static async Task Socks5Async(PipeReader input, PipeWriter output, ProxyState state, bool open, CancellationToken closed)
  {
    byte[]? greeting = await ReadExactAsync(input, 2, closed);
    byte[]? methods = greeting is null ? null : await ReadExactAsync(input, greeting[1], closed);
    if(methods is null)
    {
      return;
    }

    if(open && methods.Contains((byte)0))
    {
      await output.WriteAsync(new byte[] { Socks5, 0 }, closed);
      await Socks5ConnectAsync(input, output, state, closed);
      return;
    }

    if(!methods.Contains((byte)2))
    {
      state.Refused();
      await output.WriteAsync(new byte[] { Socks5, 0xff }, closed);
      return;
    }

    await output.WriteAsync(new byte[] { Socks5, 2 }, closed);
    byte[]? header = await ReadExactAsync(input, 2, closed);
    byte[]? name = header is null ? null : await ReadExactAsync(input, header[1], closed);
    byte[]? secretLength = name is null ? null : await ReadExactAsync(input, 1, closed);
    byte[]? secret = secretLength is null ? null : await ReadExactAsync(input, secretLength[0], closed);
    if(secret is null)
    {
      return;
    }

    if(!state.Authorized(Encoding.UTF8.GetString(name!), Encoding.UTF8.GetString(secret)))
    {
      state.Refused();
      await output.WriteAsync(new byte[] { 1, 1 }, closed);
      return;
    }

    await output.WriteAsync(new byte[] { 1, 0 }, closed);
    await Socks5ConnectAsync(input, output, state, closed);
  }

  /// <summary>The SOCKS5 CONNECT request after authentication, and the tunnel it opens.</summary>
  static async Task Socks5ConnectAsync(PipeReader input, PipeWriter output, ProxyState state, CancellationToken closed)
  {
    byte[]? request = await ReadExactAsync(input, 4, closed);
    if(request is null)
    {
      return;
    }

    string? host = request[3] switch
    {
      1 => (await ReadExactAsync(input, 4, closed)) is { } v4 ? new IPAddress(v4).ToString() : null,
      4 => (await ReadExactAsync(input, 16, closed)) is { } v6 ? new IPAddress(v6).ToString() : null,
      3 => (await ReadExactAsync(input, 1, closed)) is { } length && (await ReadExactAsync(input, length[0], closed)) is { } domain
        ? Encoding.ASCII.GetString(domain)
        : null,
      _ => null
    };
    byte[]? portBytes = host is null ? null : await ReadExactAsync(input, 2, closed);
    if(portBytes is null)
    {
      await output.WriteAsync(new byte[] { Socks5, 8, 0, 1, 0, 0, 0, 0, 0, 0 }, closed);
      return;
    }

    int port = (portBytes[0] << 8) | portBytes[1];
    state.SocksRequested(request[3]);
    string target = host!.Contains(':', StringComparison.Ordinal) ? $"[{host}]:{port}" : $"{host}:{port}";
    if(state.Denied(target))
    {
      state.Refused();
      await output.WriteAsync(new byte[] { Socks5, 2, 0, 1, 0, 0, 0, 0, 0, 0 }, closed);
      return;
    }

    using TcpClient? upstream = await ConnectUpstreamAsync(host, port, state, closed);
    if(upstream is null)
    {
      await output.WriteAsync(new byte[] { Socks5, 5, 0, 1, 0, 0, 0, 0, 0, 0 }, closed);
      return;
    }

    state.Opened(((IPEndPoint)upstream.Client.LocalEndPoint!).Port);
    await output.WriteAsync(new byte[] { Socks5, 0, 0, 1, 0, 0, 0, 0, 0, 0 }, closed);
    await RelayAsync(upstream, input, output, closed);
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
