# RayX.SignalR

This repository is the RayX-maintained fork of
[`signalr-client`](https://github.com/danielleiszen/rust_signalr_client). It
is a hard fork with the owned Cargo package and Rust crate identity
`rayx-signalr` / `rayx_signalr`. See [FORK.md](FORK.md) for provenance, the
upstream-import policy, and the current hardening scope.

The fork targets reusable native Tokio and browser WebAssembly clients, with
both JSON and MessagePack SignalR Hub Protocol support. It is not currently
published to crates.io.

## Local path dependency

```toml
[dependencies]
rayx-signalr = {
    path = "../RayX.SignalR",
    features = ["messagepack"]
}
```

The dependency is then imported as `rayx_signalr` in Rust source.

## Status

![license](https://shields.io/badge/license-MIT-blue)

I made this client because I could not find a client that supported my requirements calling a complex backend written in SignalR from a WASM frontend. This package was designed to overcome this limitation. It supports WebAssembly targets out of the box and both JSON and MessagePack hub protocols.

 Read more about SignalR in the [`offical documentation`](https://learn.microsoft.com/en-us/aspnet/core/signalr/introduction?view=aspnetcore-9.0).

I tried to design this package to be simple and convenient to use without writing boilerpart code. Please be free to comment or post issues if you have recommendations or requirements. Contribution is also welcome.

A .NET test backend is also provided for testing purposes (see the /dotnet folder). The tests in the package should run using that backend. Authorization is not tested with the test scenairo, however it's been tested already by using this package in a much larger project of mine. So, it should also work without issues.

## Production validation

Service-dependent tests run only through .NET Aspire. The AppHost
(`dotnet/RayX.SignalR.AppHost`, selected by the root `aspire.config.json`)
composes the ASP.NET Core SignalR test service, the pinned `wasm-bindgen` CLI
provisioning, the browser fixture build and the `fixture-host` static host, all
on dynamically allocated ports. The end-to-end project
(`dotnet/RayX.SignalR.E2E.Tests`, xUnit through `dotnet test`) starts the same
AppHost through `Aspire.Hosting.Testing` and is the single entry point for those
tests, locally and in GitHub Actions. No test scripts remain.

Prerequisites: .NET SDK 10.0.100 (pinned by `global.json`), the Aspire CLI
(`dotnet tool install --global Aspire.Cli --version 13.5.3`), Rust 1.92.0 with
the `wasm32-unknown-unknown` target, and for browser cells Node 24 with pnpm and
the repository-local Playwright browsers
(`pnpm --dir tools-node install --frozen-lockfile`, then
`pnpm --dir tools-node exec playwright install chromium`). The AppHost installs
the fixture's exact `wasm-bindgen` CLI under `artifacts-temp/tools/wasm-bindgen/<version>`
and never uses the machine-wide CLI.

Start the graph with the Aspire dashboard for interactive debugging:

```shell
aspire run
```

Run end-to-end cells. `RAYX_SIGNALR_E2E_TARGETS` (default `native`),
`RAYX_SIGNALR_E2E_PROTOCOLS` and `RAYX_SIGNALR_E2E_SCENARIOS` (default `all`)
take comma lists or `all`; a selection that matches no cataloged cell fails.
The C# scenario catalog (`dotnet/RayX.SignalR.E2E.Tests/Scenarios/ScenarioCatalog.cs`)
decides which targets run which scenarios.

```shell
dotnet test dotnet/RayX.SignalR.E2E.Tests --filter Category=E2E
RAYX_SIGNALR_E2E_TARGETS=chromium dotnet test dotnet/RayX.SignalR.E2E.Tests --filter Category=E2E
```

Each cell runs exactly one scenario (the Rust `e2e` test binary with
`--exact <scenario>::<protocol>`, or Playwright with one exact title), is bounded
to 60 seconds with process-tree cleanup, and on failure writes bounded,
token-redacted output plus Aspire resource logs to `artifacts-temp/e2e/<cell>.json`.
The Rust scenarios live in `tests/e2e` (`test = false`, so plain `cargo test`
never needs the service).

Static checks are .NET test categories, run the same way locally and in CI:

```shell
dotnet test dotnet/RayX.SignalR.E2E.Tests --filter "Category=WorkflowContract|Category=Repository|Category=Catalog|Category=Toolchain"
dotnet test dotnet/RayX.SignalR.E2E.Tests --filter Category=Orchestration
cargo install cargo-audit --locked
dotnet test dotnet/RayX.SignalR.E2E.Tests --filter Category=DependencySecurity
```

The least-privilege GitHub Actions workflows run on pull requests to `master`
and pushes to `master` (end-to-end can also be started manually). They do not
publish packages, require secrets, or change repository settings.

Stable quality check names are:

- `quality / rustfmt`
- `quality / clippy`
- `quality / native (<os>, Rust <toolchain>)`
- `quality / wasm (<feature mode>)`
- `quality / unit-contract (<protocol>)`
- `quality / rustdoc`
- `quality / dependency-security`
- `quality / contracts`

Stable end-to-end check names are:

- `e2e / native (<os>)`
- `e2e / browser (<engine>)`

RayX.GameX's dependency-conformance gate replaces its former
`scripts/ci/run-interop.ps1` call with:

```shell
RAYX_SIGNALR_E2E_TARGETS=chromium RAYX_SIGNALR_E2E_SCENARIOS=ordinary-reconnect,stateful-reconnect dotnet test dotnet/RayX.SignalR.E2E.Tests --filter Category=E2E
```

## Package Overview
`rayx-signalr` is a Rust library designed to call SignalR hubs from cross-platform Rust applications. It supports Tokio-based native runtimes and browser WASM clients.

## Installation Instructions
To use this package, add the following to your Cargo.toml:

```toml
[dependencies]
rayx-signalr = { git = "https://github.com/rayx-ui/RayX.SignalR.git" }
```

## MessagePack Protocol

The library supports the SignalR MessagePack hub protocol as an optional feature. MessagePack uses a binary encoding format, which results in smaller messages and can improve throughput compared to JSON.

To enable MessagePack support, add the `messagepack` feature:

```toml
[dependencies]
rayx-signalr = { git = "https://github.com/rayx-ui/RayX.SignalR.git", features = ["messagepack"] }
```

Then use the `with_messagepack_protocol()` builder method when connecting:

```rust
let mut client = SignalRClient::connect_with("localhost", "hub", |c| {
    c.with_port(5000);
    c.with_messagepack_protocol();
}).await.unwrap();
```

All other API calls (`invoke`, `enumerate`, `send`, `register`) work exactly the same regardless of protocol. The server must also have MessagePack protocol enabled (e.g., `.AddMessagePackProtocol()` in ASP.NET Core).

## Usage Examples

Here is a complex test scenario demonstrating how to use the `rayx-signalr` package:

The `SignalRClient` can be used to invoke methods on the hub, send messages, and register callbacks.
The client can be cloned and used freely across different parts of your application.

```
// Connect to the SignalR server with custom configuration
let mut client = SignalRClient::connect_with("localhost", "test", |c| {
    c.with_port(5220); // Set the port to 5220
    c.unsecure(); // Use an unsecure (HTTP) connection
}).await.unwrap();

// Invoke the "SingleEntity" method and assert the result
let re = client.invoke::<TestEntity>("SingleEntity".to_string()).await;
assert!(re.is_ok());

// Unwrap the result and assert the entity's text
let entity = re.unwrap();
assert_eq!(entity.text, "test".to_string());

// Log the entity's details
info!("Entity {}, {}", entity.text, entity.number);

// Enumerate "HundredEntities" and log each entity
let mut he = client.enumerate::<TestEntity>("HundredEntities".to_string()).await;
while let Some(item) = he.next().await {
    info!("Entity {}, {}", item.text, item.number);
}

info!("Finished fetching entities, calling pushes");

// Invoke the "PushEntity" method with arguments and assert the result
let push1 = client.invoke_with_args::<bool, _>("PushEntity".to_string(), |c| {
    c.argument(TestEntity {
        text: "push1".to_string(),
        number: 100,
    });
}).await;
assert!(push1.unwrap());

// Clone the client and invoke the "PushTwoEntities" method with arguments
let mut secondclient = client.clone();
let push2 = secondclient.invoke_with_args::<TestEntity, _>("PushTwoEntities".to_string(), |c| {
    c.argument(TestEntity {
        text: "entity1".to_string(),
        number: 200,
    }).argument(TestEntity {
        text: "entity2".to_string(),
        number: 300,
    });
}).await;
assert!(push2.is_ok());

// Unwrap the result and assert the merged entity's number
let entity = push2.unwrap();
assert_eq!(entity.number, 500);
info!("Merged Entity {}, {}", entity.text, entity.number);

// Drop the second client
drop(secondclient);

// Register callbacks for "callback1" and "callback2"
let c1 = client.register("callback1".to_string(), |ctx| {
    let result = ctx.argument::<TestEntity>(0);
    if result.is_ok() {
        let entity = result.unwrap();
        info!("Callback results entity: {}, {}", entity.text, entity.number);
    }
});

let c2 = client.register("callback2".to_string(), |mut ctx| {
    let result = ctx.argument::<TestEntity>(0);
    if result.is_ok() {
        let entity = result.unwrap();
        info!("Callback2 results entity: {}, {}", entity.text, entity.number);
        let e2 = entity.clone();
        spawn(async move {
            info!("Completing callback2");
            let _ = ctx.complete(e2).await;
        });
    }
});

// Trigger the callbacks
info!("Calling callback1");
_ = client.send_with_args("TriggerEntityCallback".to_string(), |c| {
    c.argument("callback1".to_string());
}).await;

info!("Calling callback2");
let succ = client.invoke_with_args::<bool, _>("TriggerEntityResponse".to_string(), |c| {
    c.argument("callback2".to_string());
}).await;
assert!(succ.unwrap());

// Measure the time taken to fetch a million entities
let now = Instant::now();
{
    let mut me = client.enumerate::<TestEntity>("MillionEntities".to_string()).await;
    while let Some(_) = me.next().await {}
}
let elapsed = now.elapsed();
info!("1 million entities fetched in: {:.2?}", elapsed);

// Unregister the callbacks and disconnect the client
c1.unregister();
c2.unregister();
client.disconnect();
```

## Acknowledgements

Special thanks to the [`maintainer of the signalrs package`](https://github.com/szarykott) for his invaluable inspiration and work in the first SignalR client. Their efforts have significantly contributed to the development of this package.

## Contributing
Contributions are welcome! Please fork the repository and submit pull requests along with an issue or some explanation. Ensure your code follows the existing style and includes tests for any new functionality. 

## License
This project is licensed under the MIT License. See the LICENSE file for more details.
