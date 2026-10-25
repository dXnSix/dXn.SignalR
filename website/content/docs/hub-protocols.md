---
title: Hub protocols
description: Choose JSON or MessagePack, match Serde types to .NET types on the wire, and handle void, null, dates and binary values.
group: Calling the hub
order: 60
---

SignalR separates the transport (WebSockets, Server-Sent Events or Long Polling) from the hub protocol that encodes messages. The client speaks both ASP.NET Core hub protocols, and the rest of the API is identical on either.

| | JSON (default) | MessagePack |
|---|---|---|
| Select with | nothing | `with_messagepack_protocol()` (feature `messagepack`) |
| Server needs | nothing | `Microsoft.AspNetCore.SignalR.Protocols.MessagePack` and `.AddMessagePackProtocol()` |
| On the wire | text, records end with `0x1E` | binary, each message prefixed with a varint length |
| Transfer format | `Text` | `Binary` |
| Transports | all three | WebSockets and Long Polling (Server-Sent Events carry text only) |

```rust
use dxn_signalr::SignalRClient;

async fn connect() -> Result<SignalRClient, dxn_signalr::Error> {
    SignalRClient::connect_with("localhost", "chat", |c| {
        c.with_messagepack_protocol();
    })
    .await
}
```

The handshake is always JSON. Calls, streams, callbacks, client results, pings, closes and the stateful `Ack` and `Sequence` messages use the selected protocol. Use JSON while developing and for small or human-inspected payloads; switch to MessagePack when message volume or size matters. `HubProtocolKind` (`Json`, `MessagePack`) names the two, with `protocol_name()` and `transfer_format()`.

## Matching types on both sides

Your Rust types travel through Serde, and the .NET side uses `System.Text.Json` (JSON) or MessagePack-CSharp. Field names have to agree.

### JSON

ASP.NET Core writes camelCase property names and reads them case-insensitively. Give Rust types camelCase wire names:

```rust
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct OrderLine {
    product_id: u64, // "productId"
    unit_price: f64, // "unitPrice"
}
```

```csharp
public sealed class OrderLine
{
    public long ProductId { get; set; }
    public double UnitPrice { get; set; }
}
```

Like the .NET client, the client reads JSON payloads case-insensitively: a result, stream item or callback argument that does not decode with the exact Serde names is decoded again with member names matched ignoring case. Outbound names are always the ones your types declare.

### MessagePack

The client writes Rust structs as MessagePack maps keyed by their Serde names (`rmp_serde::to_vec_named`) and never rewrites keys. MessagePack-CSharp matches map keys exactly, so annotate the .NET type with string keys equal to the Serde names:

```csharp
[MessagePackObject]
public sealed class OrderLine
{
    [Key("productId")] public long ProductId { get; set; }
    [Key("unitPrice")] public double UnitPrice { get; set; }
}
```

With the same `#[serde(rename_all = "camelCase")]` Rust type, one DTO pair works on both protocols. Integer keys (`[Key(0)]`, array layout) do not match the map the client sends.

## How values cross the wire

Each argument is serialized once, when it is added, in the connection's protocol. Client results are encoded the same way. Inbound values decode like this:

| Value | JSON | MessagePack |
|---|---|---|
| `invoke` result, stream item | `serde_json` into `T`; if that fails, retried once matching member names case-insensitively | `rmp_serde` into `T`; if that fails, retried once with map keys converted from PascalCase to camelCase |
| Callback argument | like results | `rmp_serde` from the server's original value, with the same PascalCase retry |

MessagePack callback arguments keep the server's encoding:

- Binary values (`byte[]`) decode into types that accept bytes, such as `serde_bytes::ByteBuf`. A `Vec<u8>` expects an array of numbers.
- Extension values, including MessagePack timestamps (MessagePack-CSharp writes `DateTime` as extension type -1), decode through `rmp_serde`'s extension convention: a newtype struct named `_ExtStruct` around `(i8, ByteBuf)` receives the type and its bytes.
- Maps with integer keys (`Dictionary<int, string>`) decode into maps with integer keys, such as `HashMap<i32, String>`.

:::tip Prefer plain values for shared fields
For values that must look the same on both protocols, use strings (ISO 8601 dates, base64 binary) or plain numbers (Unix milliseconds) instead of `DateTime` and `byte[]`. The client has no date type of its own.
:::

## Void, null and Option

- A hub method returning `void` or `Task` completes without a result: invoke it with `T = ()`.
- A hub method that can return `null` completes with a `null` result: use `Option<T>`.
- Missing fields fail deserialization unless the Rust field is an `Option` or has `#[serde(default)]`.

## Framing and size limits

A hub message may span several WebSocket messages, as in the .NET client. There is no size cap by default; `with_maximum_message_size(bytes)` bounds an inbound hub message, and a larger one ends the transport with `ErrorKind::Protocol`, after which the reconnection policy runs.

A malformed message, or a WebSocket message of the wrong kind (binary on a JSON connection, text on a MessagePack one), also ends the transport with `ErrorKind::Protocol`. Message types the client does not know are ignored, so a newer server can add them.

## Next

[Transports](./transports.md) explains how the client picks and falls back between transports. [Server setup](./server-setup.md#messagepack) lists the server side of MessagePack.
