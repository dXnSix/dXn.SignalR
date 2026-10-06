# Hub protocols and serialization

SignalR separates the transport (WebSockets here) from the hub protocol that encodes messages. The
client implements both ASP.NET Core hub protocols:

| | JSON (default) | MessagePack (`messagepack` feature) |
|---|---|---|
| Select with | nothing | `with_messagepack_protocol()` |
| Server needs | nothing | `.AddMessagePackProtocol()` |
| WebSocket frames | text, records end with `0x1E` | binary, each message prefixed with a varint length |
| Negotiated transfer format | `Text` | `Binary` |
| Typical use | readability, browser debugging, widest compatibility | smaller frames and faster encoding |

The handshake (`{"protocol":"json"|"messagepack","version":1}`) is JSON on both. The calls, streams,
callbacks, client results, pings, closes and the stateful `Ack`/`Sequence` messages all use the
selected protocol, so the rest of the API is identical.

## Matching types on both sides

Your Rust types travel through Serde. The .NET side uses `System.Text.Json` (JSON) or
MessagePack-CSharp (MessagePack). Field names have to agree.

### JSON

ASP.NET Core writes camelCase property names and reads property names case-insensitively. Give
Rust types camelCase wire names:

```rust
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct OrderLine {
    product_id: u64,  // "productId"
    unit_price: f64,  // "unitPrice"
}
```

```csharp
public sealed class OrderLine
{
    public long ProductId { get; set; }
    public double UnitPrice { get; set; }
}
```

### MessagePack

The client writes Rust structs as MessagePack **maps keyed by their Serde names**
(`rmp_serde::to_vec_named`) and never rewrites keys. MessagePack-CSharp matches map keys exactly, so
annotate the .NET type with string keys equal to the Serde names:

```csharp
[MessagePackObject]
public sealed class OrderLine
{
    [Key("productId")] public long ProductId { get; set; }
    [Key("unitPrice")] public double UnitPrice { get; set; }
}
```

With the same `#[serde(rename_all = "camelCase")]` Rust type, one DTO pair then works on both
protocols. Integer keys (`[Key(0)]`, array layout) do not match the map the client sends.

## How values cross the wire

### Outbound (client to server)

Each argument passed to `argument(value)` is serialized when it is added: to a JSON value, and with
the `messagepack` feature also to named MessagePack. The connection's protocol decides which one is
sent. Client results (`InvocationContext::complete`) are encoded the same way.

### Inbound (server to client)

| Value | JSON | MessagePack |
|---|---|---|
| `invoke` result, stream item | `serde_json` into `T` | `rmp_serde` into `T`; if that fails, retried once with map keys converted from PascalCase to camelCase |
| Callback argument (`ctx.argument::<T>`) | `serde_json` into `T` | converted to a JSON value, then `serde_json` into `T` |

The MessagePack-to-JSON conversion of callback arguments has edge cases:

- binary values (`byte[]`) become base64 strings; deserialize them as `String` and decode;
- extension values, including MessagePack timestamps (`DateTime`), become `null`;
- map entries with non-string keys are dropped.

For values that must survive both protocols, prefer strings (ISO 8601 dates, base64 binary) or
plain numbers (Unix milliseconds) over `DateTime` and `byte[]`.

## Void, `null` and `Option`

- A hub method returning `void`/`Task` completes without a result: invoke it with `T = ()`.
- A hub method returning `null` completes with a `null` result: use `Option<T>`.
- Missing fields fail deserialization unless the Rust field is `Option` or has
  `#[serde(default)]`.

## Choosing a protocol

Use JSON while developing and when payloads are small or human-inspected. Switch to MessagePack
when message volume or size matters; it needs the feature on the client, the package and
`.AddMessagePackProtocol()` on the server, and `[MessagePackObject]` keys on shared DTOs. Both
protocols are covered by the same end-to-end scenarios on every platform.
