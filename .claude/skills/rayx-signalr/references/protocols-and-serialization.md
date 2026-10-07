# Protocols and serialization

Full chapter: [Hub protocols and serialization](../../../../docs/05-hub-protocols.md).

## Choosing

| | JSON | MessagePack |
|---|---|---|
| Client | default | `messagepack` feature + `with_messagepack_protocol()` |
| Server | default | `Microsoft.AspNetCore.SignalR.Protocols.MessagePack` + `.AddMessagePackProtocol()` |
| Frames | text, `0x1E`-terminated records | binary, varint length prefix |
| Negotiated transfer format | `Text` | `Binary` |

The handshake is always JSON. Every later message (calls, streams, callbacks, client results, Ping,
Close, Ack, Sequence, CancelInvocation) uses the selected protocol. The rest of the API is
identical.

## DTO mapping

Rust:

```rust
#[derive(serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
struct OrderLine { product_id: u64, unit_price: f64 }
```

.NET for both protocols:

```csharp
[MessagePackObject]
public sealed class OrderLine
{
    [Key("productId")] public long ProductId { get; set; }
    [Key("unitPrice")] public double UnitPrice { get; set; }
}
```

- JSON: ASP.NET Core writes camelCase and reads case-insensitively.
- MessagePack outbound: the client writes structs as maps keyed by Serde names
  (`rmp_serde::to_vec_named`) and never rewrites keys. MessagePack-CSharp matches keys exactly, so
  .NET needs string `[Key("...")]` equal to the Serde names. Integer keys (array layout) do not
  match.
- MessagePack inbound `invoke` results and stream items: `rmp_serde` into `T`, retried once with
  PascalCase map keys converted to camelCase.
- MessagePack inbound callback arguments: decoded like results, from the server's original
  values. `byte[]` needs a bytes type (`serde_bytes::ByteBuf`, not `Vec<u8>`); `DateTime` arrives
  as extension type -1 (`#[serde(rename = "_ExtStruct")] struct Ext((i8, ByteBuf))`);
  `Dictionary<int, string>` decodes into `HashMap<i32, String>`. Prefer ISO 8601 strings or Unix
  milliseconds and base64 strings in payloads shared with JSON clients.

## Void and null

- `void`/`Task` hub methods: `invoke::<()>`.
- `null` results: `Option<T>`.
- Missing fields fail unless `Option` or `#[serde(default)]`.

## Rules when changing the protocol code

- Preserve application-defined Serde wire names in outbound MessagePack.
- Keep the handshake JSON for both protocols.
- Encode client results and dropped-stream cancellation with the selected protocol; keep JSON and
  MessagePack frame tests paired.
- Ack is cumulative and Sequence ids are nonzero.
