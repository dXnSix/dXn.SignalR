# Hub protocols and serialization

SignalR separates the transport (WebSockets, Server-Sent Events or Long Polling) from the hub
protocol that encodes messages. The client implements both ASP.NET Core hub protocols:

| | JSON (default) | MessagePack (`messagepack` feature) |
|---|---|---|
| Select with | nothing | `with_messagepack_protocol()` |
| Server needs | nothing | `.AddMessagePackProtocol()` |
| Messages on the wire | text, records end with `0x1E` | binary, each message prefixed with a varint length |
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
Rust types camelCase wire names. Like the .NET client, this client also reads JSON payloads
case-insensitively: a result, stream item or callback argument that does not decode with the exact
Serde names is decoded again with member names matched ignoring case (so `"OrderId"` fills
`order_id` renamed to `orderId`). Outbound names are always the application's.

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

Each argument passed to `argument(value)` is serialized once, when it is added, in the connection's
protocol: to JSON, or to named MessagePack (`rmp_serde::to_vec_named`). Client results
(`InvocationContext::complete`) are encoded the same way.

### Inbound (server to client)

| Value | JSON | MessagePack |
|---|---|---|
| `invoke` result, stream item | `serde_json` into `T`; if that fails, retried once matching member names case-insensitively | `rmp_serde` into `T`; if that fails, retried once with map keys converted from PascalCase to camelCase |
| Callback argument (`ctx.argument::<T>`) | like results | like results: `rmp_serde` into `T` from the server's original value, with the same PascalCase retry |

MessagePack callback arguments keep the server's encoding, as the .NET binder sees it:

- binary values (`byte[]`) decode into types that accept bytes, such as `serde_bytes::ByteBuf`
  (a `Vec<u8>` expects an array of numbers);
- extension values, including MessagePack timestamps (MessagePack-CSharp writes `DateTime` as
  extension type -1), decode through `rmp_serde`'s extension convention: a newtype struct named
  `_ExtStruct` around `(i8, ByteBuf)` receives the extension type and its bytes;
- maps with integer keys (`Dictionary<int, string>`) decode into maps with integer keys, such as
  `HashMap<i32, String>`.

For values that must look the same in both protocols, prefer strings (ISO 8601 dates, base64
binary) or plain numbers (Unix milliseconds) over `DateTime` and `byte[]`: JSON carries those as
strings.

### Framing

As in the .NET client, a hub message may span several WebSocket messages: the incomplete rest of a
WebSocket message (a JSON record without its separator, or a MessagePack message or its length
prefix) is kept for the next one. There is no size cap by default (natively the WebSocket layer's
own limit is lifted too); `with_maximum_message_size(bytes)` bounds an inbound hub message, and a
larger one ends the transport with `ErrorKind::Protocol` (the reconnection policy runs).

A malformed message, or a WebSocket message of the wrong kind (binary on a JSON connection, text
on a MessagePack one), ends the transport with `ErrorKind::Protocol`, and the reconnection policy
runs, as in the .NET client; a stateful connection then negotiates afresh. Message types the client
does not know are ignored, so a newer server can add them.

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
