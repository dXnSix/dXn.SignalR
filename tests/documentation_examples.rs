//! The Rust examples of `README.md` and `docs/`, compiled: each example is copied verbatim into
//! a function (statements) or a module (items) below, and the `documentation_examples_` test
//! checks that every ```` ```rust ```` block of the documentation still appears here, line by line
//! (indentation and blank lines aside). Changing an example means changing its copy here too.
//! Blocks fenced as ```` ```rust,ignore ```` (dependencies this crate does not have) are skipped.
//!
//! Statement examples assume an enclosing `async fn` returning
//! `Result<(), Box<dyn std::error::Error>>` and, where they use one, a `client` from an earlier
//! example: the `client` parameter. Feature-gated examples compile with their feature. Nothing
//! here runs; the functions only have to type-check (browser examples with
//! `cargo check --target wasm32-unknown-unknown --test documentation_examples`).

// The copies keep the documentation's text and layout: unused bindings and the documentation's
// style stay as written, and rustfmt leaves the copies alone.
#![allow(dead_code, unused, clippy::all, clippy::pedantic)]

#[cfg(not(target_arch = "wasm32"))]
#[rustfmt::skip]
mod native {
    use dxn_signalr::SignalRClient;

    /// `README.md`, "Connect and invoke".
    async fn readme_connect_and_invoke(mut client: SignalRClient) -> Result<(), Box<dyn std::error::Error>> {
        use dxn_signalr::SignalRClient;
        use serde::Deserialize;

        #[derive(Deserialize)]
        #[serde(rename_all = "camelCase")]
        struct Forecast {
            city: String,
            temperature_c: f64,
        }

        let mut client = SignalRClient::connect_with("localhost", "weather", |c| {
            c.unsecure();
            c.with_port(5000);
        })
        .await?;

        let forecast: Forecast = client
            .invoke_with_args("GetForecast".to_string(), |args| {
                args.argument("Zagreb");
            })
            .await?;
        println!("{}: {} °C", forecast.city, forecast.temperature_c);
        Ok(())
    }

    /// `README.md`, "Send and receive".
    async fn readme_send_and_receive(mut client: SignalRClient) -> Result<(), Box<dyn std::error::Error>> {
        use dxn_signalr::CallbackHandler;

        let on_message = client.register("ReceiveMessage".to_string(), |ctx| {
            let user: String = ctx.argument(0).unwrap_or_default();
            let text: String = ctx.argument(1).unwrap_or_default();
            println!("{user}: {text}");
        });

        client
            .send_with_args("Send".to_string(), |args| {
                args.argument("rust").argument("Hello!");
            })
            .await?;

        on_message.unregister();
        Ok(())
    }

    /// `README.md`, "Stream from the server".
    async fn readme_stream_from_the_server(mut client: SignalRClient) -> Result<(), Box<dyn std::error::Error>> {
        use futures::StreamExt;

        let mut ticks = client.enumerate::<f64>("Prices".to_string()).await;
        while let Some(price) = ticks.next().await {
            println!("{price}");
        }
        if let Some(error) = ticks.error() {
            eprintln!("stream failed: {error}");
        }
        Ok(())
    }

    /// `README.md`, "Handle errors".
    async fn readme_handle_errors(mut client: SignalRClient) -> Result<(), Box<dyn std::error::Error>> {
        use dxn_signalr::ErrorKind;

        match client.invoke::<i32>("Total".to_string()).await {
            Ok(total) => println!("{total}"),
            Err(error) if error.kind() == ErrorKind::Hub => {
                eprintln!("the hub failed: {}", error.server_message().unwrap_or(""));
            }
            Err(error) if error.is_transient() => eprintln!("try again later: {error}"),
            Err(error) => return Err(error.into()),
        }
        Ok(())
    }

    /// `README.md`, "Reconnect and replay".
    #[cfg(feature = "messagepack")]
    async fn readme_reconnect_and_replay(mut client: SignalRClient) -> Result<(), Box<dyn std::error::Error>> {
        use std::sync::Arc;
        use dxn_signalr::{DefaultRetryPolicy, ReconnectionConfig, SignalRClient, StatefulReconnectConfig};

        let stateful = StatefulReconnectConfig::new(256 * 1024)?;
        let client = SignalRClient::connect_with("chat.example.com", "chat", move |c| {
            c.authenticate_bearer("<access token>".to_string());
            c.with_messagepack_protocol();
            c.with_stateful_reconnect(stateful);
            c.with_reconnection_policy(ReconnectionConfig {
                policy: Arc::new(DefaultRetryPolicy),
            });
        })
        .await?;

        // A mobile app going to the background and back:
        client.pause().await?;
        client.resume().await?;
        Ok(())
    }

    /// `README.md`, "Start, observe and stop".
    async fn readme_start_observe_and_stop(mut client: SignalRClient) -> Result<(), Box<dyn std::error::Error>> {
        use dxn_signalr::SignalRClient;

        let mut client = SignalRClient::new("localhost", "chat", |c| {
            c.with_port(5000);
        })?;
        let _welcome = client.register_typed("Welcome", |_ctx, (text,): (String,)| println!("{text}"));
        let _reconnected = client.on_reconnected(|id| println!("reconnected as {id:?}"));
        let _closed = client.on_closed(|error| println!("closed: {error:?}"));

        client.start().await?;
        println!("connection id: {:?}", client.connection_id());

        // A graceful stop; `start` connects again with a fresh negotiation.
        client.stop().await;
        client.start().await?;
        Ok(())
    }

    /// `docs/01-getting-started.md`, "A first native client".
    mod getting_started_a_first_native_client {
        use dxn_signalr::{CallbackHandler, SignalRClient};

        #[tokio::main]
        async fn main() -> Result<(), dxn_signalr::Error> {
            let mut client = SignalRClient::connect_with("localhost", "chat", |c| {
                c.unsecure(); // http/ws; the default is https/wss
                c.with_port(5000);
            })
            .await?;

            let receiver = client.register("ReceiveMessage".to_string(), |ctx| {
                let user: String = ctx.argument(0).unwrap_or_default();
                let text: String = ctx.argument(1).unwrap_or_default();
                println!("{user}: {text}");
            });

            client
                .send_with_args("Send".to_string(), |args| {
                    args.argument("rust").argument("Hello from Rust");
                })
                .await?;

            tokio::time::sleep(std::time::Duration::from_secs(1)).await;

            receiver.unregister();
            client.disconnect();
            Ok(())
        }
    }

    /// `docs/02-connecting.md`, "The entry points".
    async fn connecting_the_entry_points(mut client: SignalRClient) -> Result<(), Box<dyn std::error::Error>> {
        use dxn_signalr::SignalRClient;

        // Build without I/O, register callbacks and hooks, then start (`HubConnectionBuilder.Build()`
        // and `StartAsync` in the official .NET client).
        let client = SignalRClient::new("localhost", "hubs/chat", |c| {
            c.unsecure();
            c.with_port(5000);
        })?;
        client.start().await?;

        // Default configuration: https, default port, JSON, no authentication, no reconnection.
        let client = SignalRClient::connect("api.example.com", "hubs/chat").await?;

        // Custom configuration through a closure over `&mut ConnectionConfiguration`.
        let client = SignalRClient::connect_with("localhost", "hubs/chat", |c| {
            c.unsecure();
            c.with_port(5000);
        })
        .await?;
        Ok(())
    }

    /// `docs/02-connecting.md`, "The entry points".
    async fn connecting_the_entry_points_2(mut client: SignalRClient) -> Result<(), Box<dyn std::error::Error>> {
        use dxn_signalr::{ConnectionConfiguration, SignalRClient};

        fn production(c: &mut ConnectionConfiguration) {
            c.with_port(8443);
            c.authenticate_bearer(std::env::var("API_TOKEN").unwrap_or_default());
        }

        let client = SignalRClient::connect_with("api.example.com", "hubs/chat", production).await?;
        Ok(())
    }

    /// `docs/02-connecting.md`, "Access-token factory".
    async fn connecting_access_token_factory(mut client: SignalRClient) -> Result<(), Box<dyn std::error::Error>> {
        let client = dxn_signalr::SignalRClient::new("localhost", "chat", |c| {
            c.with_access_token_factory(|| async {
                // Fetch or refresh the token here.
                Ok::<_, String>(std::env::var("API_TOKEN").unwrap_or_default())
            });
        })?;
        Ok(())
    }

    /// `docs/02-connecting.md`, "Integrated Windows authentication".
    async fn connecting_integrated_windows_authentication(mut client: SignalRClient) -> Result<(), Box<dyn std::error::Error>> {
        let client = dxn_signalr::SignalRClient::new("intranet.example.com", "chat", |c| {
            // The signed-in Windows account (Kerberos, or NTLM where Kerberos is unavailable).
            c.with_default_credentials();
            // Or explicit credentials: c.with_credentials("user", "password", "CORP");
        })?;
        Ok(())
    }

    /// `docs/02-connecting.md`, "Request headers".
    async fn connecting_request_headers(mut client: SignalRClient) -> Result<(), Box<dyn std::error::Error>> {
        let client = dxn_signalr::SignalRClient::new("localhost", "chat", |c| {
            c.with_header("X-Tenant", "blue");
        })?;
        Ok(())
    }

    /// `docs/02-connecting.md`, "Request hooks".
    async fn connecting_request_hooks(mut client: SignalRClient) -> Result<(), Box<dyn std::error::Error>> {
        let client = dxn_signalr::SignalRClient::new("localhost", "chat", |c| {
            c.with_request_hook(|request| {
                // `transport()` is `None` for negotiate; `method()` and `url()` name the request.
                if request.transport().is_none() {
                    let _ = request.set_header("X-Negotiate-Source", "dxn");
                }
            });
        })?;
        Ok(())
    }

    /// `docs/02-connecting.md`, "Skipping negotiation".
    async fn connecting_skipping_negotiation(mut client: SignalRClient) -> Result<(), Box<dyn std::error::Error>> {
        let client = dxn_signalr::SignalRClient::connect_with("localhost", "chat", |c| {
            c.unsecure();
            c.with_port(5000);
            c.skip_negotiation();
            c.with_transports(dxn_signalr::Transports::WEB_SOCKETS);
        })
        .await?;
        Ok(())
    }

    /// `docs/02-connecting.md`, "Transports".
    async fn connecting_transports(mut client: SignalRClient) -> Result<(), Box<dyn std::error::Error>> {
        let client = dxn_signalr::SignalRClient::new("localhost", "chat", |c| {
            c.with_transports(dxn_signalr::Transports::WEB_SOCKETS | dxn_signalr::Transports::LONG_POLLING);
        })?;
        Ok(())
    }

    /// `docs/02-connecting.md`, "Long Polling".
    async fn connecting_long_polling(mut client: SignalRClient) -> Result<(), Box<dyn std::error::Error>> {
        use std::time::Duration;

        let client = dxn_signalr::SignalRClient::new("localhost", "chat", |c| {
            c.with_transports(dxn_signalr::Transports::LONG_POLLING);
            c.with_long_polling_timeout(Duration::from_secs(60));
        })?;
        Ok(())
    }

    /// `docs/02-connecting.md`, "Server-Sent Events".
    async fn connecting_server_sent_events(mut client: SignalRClient) -> Result<(), Box<dyn std::error::Error>> {
        let client = dxn_signalr::SignalRClient::new("localhost", "chat", |c| {
            c.with_transports(dxn_signalr::Transports::SERVER_SENT_EVENTS);
        })?;
        Ok(())
    }

    /// `docs/02-connecting.md`, "HTTP versions".
    async fn connecting_http_versions(mut client: SignalRClient) -> Result<(), Box<dyn std::error::Error>> {
        let client = dxn_signalr::SignalRClient::new("hub.example.com", "chat", |c| {
            c.with_http_version(dxn_signalr::HttpVersion::Http2);
        })?;
        Ok(())
    }

    /// `docs/02-connecting.md`, "Experimental HTTP/3".
    #[cfg(feature = "http3")]
    async fn connecting_experimental_http_3(mut client: SignalRClient) -> Result<(), Box<dyn std::error::Error>> {
        // Requires the `http3` feature.
        let client = dxn_signalr::SignalRClient::new("hub.example.com", "chat", |c| {
            c.with_http_version(dxn_signalr::HttpVersion::Http3Preferred);
        })?;
        Ok(())
    }

    /// `docs/02-connecting.md`, "Proxies".
    async fn connecting_proxies(mut client: SignalRClient) -> Result<(), Box<dyn std::error::Error>> {
        let client = dxn_signalr::SignalRClient::new("hub.example.com", "chat", |c| {
            // An explicit proxy instead of the system's; credentials are optional.
            c.with_proxy("http://user:password@proxy.example.com:3128");
            // Or a SOCKS proxy: c.with_proxy("socks5://user:password@proxy.example.com:1080");
            // Or: c.without_proxy(); to connect directly, or c.with_system_proxy(); (the default).
        })?;
        Ok(())
    }

    /// `docs/02-connecting.md`, "Proxy auto-config".
    #[cfg(dxn_pac)]
    async fn connecting_proxy_auto_config(mut client: SignalRClient) -> Result<(), Box<dyn std::error::Error>> {
        let client = dxn_signalr::SignalRClient::new("hub.example.com", "chat", |c| {
            c.with_proxy_auto_config("http://wpad.example.com/proxy.pac");
        })?;
        Ok(())
    }

    /// `docs/02-connecting.md`, "Cookies".
    async fn connecting_cookies(mut client: SignalRClient) -> Result<(), Box<dyn std::error::Error>> {
        let jar = dxn_signalr::CookieJar::new();
        jar.set_cookie("https://hub.example.com/chat", "tenant=blue; Path=/");
        let client = dxn_signalr::SignalRClient::new("hub.example.com", "chat", |c| {
            c.with_cookie_jar(jar.clone());
        })?;
        // After connecting: jar.cookie_header("https://hub.example.com/chat")
        Ok(())
    }

    /// `docs/02-connecting.md`, "Hub protocol".
    #[cfg(feature = "messagepack")]
    async fn connecting_hub_protocol(mut client: SignalRClient) -> Result<(), Box<dyn std::error::Error>> {
        let client = dxn_signalr::SignalRClient::connect_with("localhost", "chat", |c| {
            c.with_messagepack_protocol();
        })
        .await?;
        Ok(())
    }

    /// `docs/02-connecting.md`, "Native runtime".
    async fn connecting_native_runtime(runtime: &tokio::runtime::Runtime) -> Result<(), Box<dyn std::error::Error>> {
        // `runtime` is a tokio::runtime::Runtime that the application owns.
        let handle = runtime.handle().clone();
        let client = dxn_signalr::SignalRClient::connect_with("localhost", "chat", move |c| {
            c.with_runtime_handle(handle.clone());
        })
        .await?;
        Ok(())
    }

    /// `docs/03-calling-the-hub.md`, "Arguments".
    async fn calling_the_hub_arguments(mut client: SignalRClient) -> Result<(), Box<dyn std::error::Error>> {
        use serde::Serialize;

        #[derive(Serialize)]
        struct Order {
            id: u64,
            quantity: u32,
        }

        let accepted: bool = client
            .invoke_with_args("PlaceOrder".to_string(), |args| {
                args.argument("customer-17")
                    .argument(Order { id: 9, quantity: 3 })
                    .argument(true);
            })
            .await?;
        Ok(())
    }

    /// `docs/03-calling-the-hub.md`, "Invoking".
    async fn calling_the_hub_invoking(mut client: SignalRClient) -> Result<(), Box<dyn std::error::Error>> {
        use serde::Deserialize;

        #[derive(Deserialize)]
        struct Profile {
            name: String,
            karma: i64,
        }

        let profile: Profile = client.invoke("CurrentProfile".to_string()).await?;
        let sum: i32 = client
            .invoke_with_args("Add".to_string(), |args| {
                args.argument(2).argument(3);
            })
            .await?;
        Ok(())
    }

    /// `docs/03-calling-the-hub.md`, "Sending".
    async fn calling_the_hub_sending(mut client: SignalRClient) -> Result<(), Box<dyn std::error::Error>> {
        client.send("Ping".to_string()).await?;
        client
            .send_with_args("Broadcast".to_string(), |args| {
                args.argument("maintenance at 18:00");
            })
            .await?;
        Ok(())
    }

    /// `docs/03-calling-the-hub.md`, "Invocation headers".
    async fn calling_the_hub_invocation_headers(mut client: SignalRClient) -> Result<(), Box<dyn std::error::Error>> {
        let client = dxn_signalr::SignalRClient::new("localhost", "chat", |c| {
            c.with_invocation_headers(|_target| {
                vec![(
                    "traceparent".to_string(),
                    "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01".to_string(),
                )]
            });
        })?;
        Ok(())
    }

    /// `docs/03-calling-the-hub.md`, "Server streams".
    async fn calling_the_hub_server_streams(mut client: SignalRClient) -> Result<(), Box<dyn std::error::Error>> {
        use futures::StreamExt;
        use serde::Deserialize;

        #[derive(Deserialize)]
        struct Tick {
            symbol: String,
            price: f64,
        }

        let mut ticks = client
            .enumerate_with_args::<Tick, _>("Ticks".to_string(), |args| {
                args.argument("EURUSD");
            })
            .await;

        while let Some(tick) = ticks.next().await {
            println!("{} {}", tick.symbol, tick.price);
        }

        if let Some(error) = ticks.error() {
            eprintln!("the stream ended with an error: {error}");
        }
        Ok(())
    }

    /// `docs/03-calling-the-hub.md`, "Cancelling a stream".
    async fn calling_the_hub_cancelling_a_stream(mut client: SignalRClient) -> Result<(), Box<dyn std::error::Error>> {
        use futures::StreamExt;

        let mut prices = client.enumerate::<f64>("Prices".to_string()).await;
        let first_ten: Vec<f64> = (&mut prices).take(10).collect().await;
        drop(prices); // stops the server-side enumeration
        Ok(())
    }

    /// `docs/03-calling-the-hub.md`, "Client-to-server streams".
    async fn calling_the_hub_client_to_server_streams(mut client: SignalRClient) -> Result<(), Box<dyn std::error::Error>> {
        use futures::stream;

        // public async Task<int> Sum(IAsyncEnumerable<int> numbers)
        let sum: i32 = client
            .invoke_with_args("Sum".to_string(), |args| {
                args.stream(stream::iter(1..=10));
            })
            .await?;
        Ok(())
    }

    /// `docs/03-calling-the-hub.md`, "Concurrency and clones".
    async fn calling_the_hub_concurrency_and_clones(mut client: SignalRClient) -> Result<(), Box<dyn std::error::Error>> {
        let mut a = client.clone();
        let mut b = client.clone();
        let (left, right) = futures::join!(
            a.invoke::<i32>("SlowLeft".to_string()),
            b.invoke::<i32>("SlowRight".to_string()),
        );
        Ok(())
    }

    /// `docs/04-server-callbacks.md`, "Registering a callback".
    async fn server_callbacks_registering_a_callback(mut client: SignalRClient) -> Result<(), Box<dyn std::error::Error>> {
        use dxn_signalr::CallbackHandler;
        use serde::Deserialize;

        #[derive(Deserialize)]
        struct ChatMessage {
            user: String,
            text: String,
        }

        let registration = client.register("ReceiveMessage".to_string(), |ctx| {
            match ctx.argument::<ChatMessage>(0) {
                Ok(message) => println!("{}: {}", message.user, message.text),
                Err(error) => eprintln!("unexpected payload: {error}"),
            }
        });

        // Later, to stop receiving the calls:
        registration.unregister();
        Ok(())
    }

    /// `docs/04-server-callbacks.md`, "Asynchronous callbacks".
    async fn server_callbacks_asynchronous_callbacks(mut client: SignalRClient) -> Result<(), Box<dyn std::error::Error>> {
        use dxn_signalr::CallbackHandler;

        let registration = client.register_async("Refresh".to_string(), |ctx| async move {
            let mut hub = ctx.client.clone();
            if let Ok(latest) = hub.invoke::<String>("Latest".to_string()).await {
                println!("latest: {latest}");
            }
        });
        Ok(())
    }

    /// `docs/04-server-callbacks.md`, "Typed callbacks".
    async fn server_callbacks_typed_callbacks(mut client: SignalRClient) -> Result<(), Box<dyn std::error::Error>> {
        use dxn_signalr::CallbackHandler;

        let registration = client.register_typed("PriceChanged", |_ctx, (symbol, price): (String, f64)| {
            println!("{symbol} = {price}");
        });
        Ok(())
    }

    /// `docs/04-server-callbacks.md`, "Reading arguments".
    async fn server_callbacks_reading_arguments(mut client: SignalRClient) -> Result<(), Box<dyn std::error::Error>> {
        let registration = client.register("PriceChanged".to_string(), |ctx| {
            let symbol: String = match ctx.argument(0) {
                Ok(symbol) => symbol,
                Err(_) => return,
            };
            let price: f64 = ctx.argument(1).unwrap_or(f64::NAN);
            println!("{symbol} = {price}");
        });
        Ok(())
    }

    /// `docs/04-server-callbacks.md`, "Client results".
    async fn server_callbacks_client_results(mut client: SignalRClient) -> Result<(), Box<dyn std::error::Error>> {
        use dxn_signalr::InvocationContext;

        let registration = client.register("ConfirmPurchase".to_string(), |mut ctx| {
            let amount: f64 = ctx.argument(0).unwrap_or_default();
            InvocationContext::spawn(async move {
                let approved = amount < 100.0;
                if let Err(error) = ctx.complete(approved).await {
                    eprintln!("cannot answer the server: {error}");
                }
            });
        });
        Ok(())
    }

    /// `docs/04-server-callbacks.md`, "`InvocationContext`".
    async fn server_callbacks_invocationcontext(mut client: SignalRClient) -> Result<(), Box<dyn std::error::Error>> {
        use dxn_signalr::InvocationContext;

        let registration = client.register("Joined".to_string(), |ctx| {
            let user: String = ctx.argument(0).unwrap_or_default();
            let mut hub = ctx.client.clone();
            InvocationContext::spawn(async move {
                let _ = hub
                    .send_with_args("Welcome".to_string(), |args| {
                        args.argument(&user);
                    })
                    .await;
            });
        });
        Ok(())
    }

    /// `docs/04-server-callbacks.md`, "Registrations do not keep a client alive".
    async fn server_callbacks_registrations_do_not_keep_a_client_alive(mut client: SignalRClient) -> Result<(), Box<dyn std::error::Error>> {
        use dxn_signalr::CallbackHandler;

        let on_message = client.register_typed("ReceiveMessage", |_ctx, (text,): (String,)| {
            println!("{text}");
        });

        // Later, when the application is done with the hub:
        on_message.unregister();
        client.disconnect();
        Ok(())
    }

    /// `docs/05-hub-protocols.md`, "JSON".
    mod hub_protocols_json {
        use serde::{Deserialize, Serialize};

        #[derive(Serialize, Deserialize)]
        #[serde(rename_all = "camelCase")]
        struct OrderLine {
            product_id: u64,  // "productId"
            unit_price: f64,  // "unitPrice"
        }
    }

    /// `docs/06-connection-lifecycle.md`, "Building and starting".
    async fn connection_lifecycle_building_and_starting(mut client: SignalRClient) -> Result<(), Box<dyn std::error::Error>> {
        let mut client = dxn_signalr::SignalRClient::new("localhost", "chat", |c| {
            c.with_port(5000);
        })?;

        // Registered before `start`, so messages the hub sends from `OnConnectedAsync` arrive here.
        let _welcome = client.register_typed("Welcome", |_context, (text,): (String,)| {
            println!("{text}");
        });
        let _closed = client.on_closed(|error| println!("closed: {error:?}"));

        client.start().await?;
        Ok(())
    }

    /// `docs/06-connection-lifecycle.md`, "Lifecycle events".
    async fn connection_lifecycle_lifecycle_events(mut client: SignalRClient) -> Result<(), Box<dyn std::error::Error>> {
        let _reconnecting = client.on_reconnecting(|reason| eprintln!("reconnecting: {reason}"));
        let _reconnected = client.on_reconnected(|connection_id| {
            println!("reconnected as {connection_id:?}");
        });
        let _closed = client.on_closed(|error| match error {
            Some(error) => eprintln!("connection lost: {error}"),
            None => println!("stopped"),
        });
        Ok(())
    }

    /// `docs/06-connection-lifecycle.md`, "Lifecycle events".
    async fn connection_lifecycle_lifecycle_events_2(mut client: SignalRClient) -> Result<(), Box<dyn std::error::Error>> {
        let weak = client.downgrade();
        let _restart = client.on_closed(move |error| {
            // `None`, or a cancellation during a reconnect: stopped by the application.
            let Some(error) = error else { return };
            if error.kind() == dxn_signalr::ErrorKind::Canceled {
                return;
            }
            if let Some(client) = weak.upgrade() {
                tokio::spawn(async move {
                    let _ = client.start().await;
                });
            }
        });
        Ok(())
    }

    /// `docs/06-connection-lifecycle.md`, "Liveness".
    async fn connection_lifecycle_liveness(mut client: SignalRClient) -> Result<(), Box<dyn std::error::Error>> {
        use std::time::Duration;

        let client = dxn_signalr::SignalRClient::new("localhost", "chat", |c| {
            c.with_keep_alive_interval(Duration::from_secs(10));
            c.with_server_timeout(Duration::from_secs(40));
            c.with_handshake_timeout(Duration::from_secs(5));
            c.with_connect_timeout(Duration::from_secs(10));
            c.with_close_timeout(Duration::from_secs(2));
        })?;
        Ok(())
    }

    /// `docs/06-connection-lifecycle.md`, "Pausing and resuming".
    async fn connection_lifecycle_pausing_and_resuming(mut client: SignalRClient) -> Result<(), Box<dyn std::error::Error>> {
        // Going to the background: end the transport, stop keep-alive and automatic reconnects.
        client.pause().await?;
        assert_eq!(client.status(), dxn_signalr::ConnectionStatus::Paused);

        // Back in the foreground: reconnect now, then follow the reconnection policy.
        client.resume().await?;
        Ok(())
    }

    /// `docs/06-connection-lifecycle.md`, "App lifecycle integration".
    mod connection_lifecycle_app_lifecycle_integration {
        /// Call from the platform's lifecycle notifications.
        async fn on_foreground_changed(
            client: &dxn_signalr::SignalRClient,
            foreground: bool,
        ) -> Result<(), dxn_signalr::Error> {
            if foreground {
                client.resume().await
            } else {
                client.pause().await
            }
        }
    }

    /// `docs/07-reconnection.md`, "Automatic reconnection".
    async fn reconnection_automatic_reconnection(mut client: SignalRClient) -> Result<(), Box<dyn std::error::Error>> {
        use std::sync::Arc;
        use dxn_signalr::{DefaultRetryPolicy, ReconnectionConfig, SignalRClient};

        let client = SignalRClient::connect_with("localhost", "chat", |c| {
            c.with_reconnection_policy(ReconnectionConfig {
                policy: Arc::new(DefaultRetryPolicy),
            });
        })
        .await?;
        Ok(())
    }

    /// `docs/07-reconnection.md`, "Built-in policies".
    async fn reconnection_built_in_policies(mut client: SignalRClient) -> Result<(), Box<dyn std::error::Error>> {
        use std::{sync::Arc, time::Duration};
        use dxn_signalr::{ExponentialBackoffPolicy, ReconnectionConfig};

        let config = ReconnectionConfig {
            policy: Arc::new(ExponentialBackoffPolicy::new(
                Duration::from_millis(500), // first delay
                2.0,                        // factor
                Some(Duration::from_secs(30)),
                Some(10),                   // give up after 10 attempts
            )),
        };
        Ok(())
    }

    /// `docs/07-reconnection.md`, "Custom policies".
    mod reconnection_custom_policies {
        use std::time::Duration;
        use dxn_signalr::{ErrorKind, ReconnectionPolicy, RetryContext};

        /// Retries every 5 seconds for at most two minutes, but not after a failed authentication.
        struct ForTwoMinutes;

        impl ReconnectionPolicy for ForTwoMinutes {
            fn next_retry_delay_with(&self, context: &RetryContext) -> Option<Duration> {
                if context.retry_reason.kind() == ErrorKind::Authentication {
                    return None;
                }
                (context.elapsed < Duration::from_secs(120)).then(|| Duration::from_secs(5))
            }
        }
    }

    /// `docs/07-reconnection.md`, "Manual reconnection".
    async fn reconnection_manual_reconnection(mut client: SignalRClient) -> Result<(), Box<dyn std::error::Error>> {
        use std::sync::Arc;
        use dxn_signalr::{
            DefaultRetryPolicy, DisconnectionHandler, ReconnectionConfig, ReconnectionHandler, SignalRClient,
        };

        struct Reconnector;

        impl DisconnectionHandler for Reconnector {
            fn on_disconnected(&self, reconnection: ReconnectionHandler) {
                if !reconnection.server_allows_reconnect() {
                    eprintln!("{} closed the connection for good", reconnection.endpoint());
                    return;
                }
                tokio::spawn(async move {
                    // Uses the configured ReconnectionConfig; the first attempt starts immediately.
                    if let Err(error) = reconnection.reconnect_with_policy().await {
                        eprintln!("giving up: {error}");
                    }
                });
            }
        }

        let client = SignalRClient::connect_with("localhost", "chat", |c| {
            // The policy only paces reconnect_with_policy here; the handler decides when to reconnect.
            c.with_reconnection_policy(ReconnectionConfig {
                policy: Arc::new(DefaultRetryPolicy),
            });
            c.with_disconnection_handler(Reconnector);
        })
        .await?;
        Ok(())
    }

    /// `docs/07-reconnection.md`, "Stateful reconnect".
    async fn reconnection_stateful_reconnect(mut client: SignalRClient) -> Result<(), Box<dyn std::error::Error>> {
        use std::sync::Arc;
        use dxn_signalr::{DefaultRetryPolicy, ReconnectionConfig, SignalRClient, StatefulReconnectConfig};

        let stateful = StatefulReconnectConfig::new(256 * 1024)?; // replay buffer in bytes
        let client = SignalRClient::connect_with("localhost", "chat", move |c| {
            c.with_stateful_reconnect(stateful);
            c.with_reconnection_policy(ReconnectionConfig {
                policy: Arc::new(DefaultRetryPolicy),
            });
        })
        .await?;
        Ok(())
    }

    /// `docs/08-tls.md`, "Verifiers".
    #[cfg(feature = "tls-webpki-roots")]
    async fn tls_verifiers(mut client: SignalRClient) -> Result<(), Box<dyn std::error::Error>> {
        let client = dxn_signalr::SignalRClient::connect_with("api.example.com", "hub", |c| {
            c.use_webpki_roots(); // needs the tls-webpki-roots feature
        })
        .await?;
        Ok(())
    }

    /// `docs/08-tls.md`, "Private certificate authorities".
    async fn tls_private_certificate_authorities(mut client: SignalRClient) -> Result<(), Box<dyn std::error::Error>> {
        let ca = std::fs::read("certs/internal-ca.pem")?;
        let client = dxn_signalr::SignalRClient::connect_with("internal.example.com", "hub", move |c| {
            c.with_additional_root_certificates_pem(ca.clone());
        })
        .await?;
        Ok(())
    }

    /// `docs/08-tls.md`, "Client certificates".
    async fn tls_client_certificates(mut client: SignalRClient) -> Result<(), Box<dyn std::error::Error>> {
        let chain = std::fs::read("certs/client.pem")?; // the certificate, then any intermediates
        let key = std::fs::read("certs/client.key")?; // its private key
        let client = dxn_signalr::SignalRClient::connect_with("internal.example.com", "hub", move |c| {
            c.with_client_certificate(chain.clone(), key.clone());
        })
        .await?;
        Ok(())
    }

    /// `docs/08-tls.md`, "Adjusting the rustls configuration".
    #[cfg(dxn_tls)]
    async fn tls_adjusting_the_rustls_configuration(mut client: SignalRClient) -> Result<(), Box<dyn std::error::Error>> {
        let client = dxn_signalr::SignalRClient::new("internal.example.com", "hub", |c| {
            c.with_tls_config_hook(|config| {
                // For example, servers addressed by a name they do not expect in SNI.
                config.enable_sni = false;
            });
        })?;
        Ok(())
    }

    /// `docs/09-platforms.md`, "Native runtime ownership".
    fn platforms_native_runtime_ownership(mut client: SignalRClient) -> Result<(), Box<dyn std::error::Error>> {
        use futures::executor::block_on;
        use dxn_signalr::SignalRClient;

        // A small runtime that owns the network work.
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .enable_all()
            .build()?;
        let handle = runtime.handle().clone();

        // Awaited from a thread with no Tokio context.
        let mut client = block_on(SignalRClient::connect_with("localhost", "chat", move |c| {
            c.unsecure();
            c.with_port(5000);
            c.with_runtime_handle(handle.clone());
        }))?;
        let answer: String = block_on(client.invoke("Echo".to_string()))?;
        Ok(())
    }

    /// `docs/11-errors-and-logging.md`, "Handling errors".
    mod errors_and_logging_handling_errors {
        use std::time::Duration;

        use dxn_signalr::{CloseReason, Error, ErrorKind, SignalRClient, TimeoutOperation};

        async fn connect(host: &str) -> Result<SignalRClient, Error> {
            let result = SignalRClient::connect_with(host, "chat", |c| {
                c.with_connect_timeout(Duration::from_secs(5));
            })
            .await;
            if let Err(error) = &result {
                match error.kind() {
                    ErrorKind::Authentication => eprintln!("sign in again ({:?})", error.status()),
                    ErrorKind::Negotiate if error.status() == Some(404) => eprintln!("no hub at that path"),
                    ErrorKind::Timeout if error.timeout_operation() == Some(TimeoutOperation::Negotiate) => {
                        eprintln!("the server did not answer negotiate within {:?}", error.timeout())
                    }
                    ErrorKind::Configuration => eprintln!("fix {}", error.setting().unwrap_or("the configuration")),
                    _ if error.is_transient() => eprintln!("try again later: {error}"),
                    _ => eprintln!("cannot connect: {error}"),
                }
            }
            result
        }

        async fn total(client: &mut SignalRClient) -> Option<i32> {
            match client.invoke::<i32>("Total".to_string()).await {
                Ok(total) => Some(total),
                Err(error) => {
                    match (error.kind(), error.close_reason()) {
                        (ErrorKind::Hub, _) => eprintln!("the hub failed: {}", error.server_message().unwrap_or("")),
                        (ErrorKind::Serialization, _) => eprintln!("Total returned something other than an i32"),
                        (ErrorKind::ConnectionClosed, Some(CloseReason::Server)) => {
                            eprintln!("the server ended the connection: {:?}", error.server_message())
                        }
                        _ => eprintln!("Total failed: {error}"),
                    }
                    None
                }
            }
        }
    }

    /// `docs/11-errors-and-logging.md`, "Logging".
    fn errors_and_logging_logging(mut client: SignalRClient) -> Result<(), Box<dyn std::error::Error>> {
        env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("dxn_signalr=info"))
            .init();
        Ok(())
    }
}

#[cfg(target_arch = "wasm32")]
#[rustfmt::skip]
mod browser {
    /// `docs/01-getting-started.md`, "A first browser client".
    mod getting_started_a_first_browser_client {
        use dxn_signalr::SignalRClient;
        use wasm_bindgen::prelude::*;

        #[wasm_bindgen(start)]
        pub fn start() {
            wasm_bindgen_futures::spawn_local(async {
                let mut client = match SignalRClient::connect_with("chat.example.com", "chat", |c| {
                    c.authenticate_bearer("<access token>".to_string());
                })
                .await
                {
                    Ok(client) => client,
                    Err(error) => {
                        log::error!("cannot connect: {error}");
                        return;
                    }
                };

                let _ = client
                    .send_with_args("Send".to_string(), |args| {
                        args.argument("browser").argument("Hello from WebAssembly");
                    })
                    .await;
            });
        }
    }
}

/// Every ```` ```rust ```` block of `README.md` and `docs/*.md`, with its lines trimmed and blank
/// lines dropped, appears as consecutive lines of this file.
#[cfg(not(target_arch = "wasm32"))]
#[test]
fn documentation_examples_match_their_compiled_copies() {
    fn normalized(text: &str) -> String {
        let lines: Vec<&str> = text
            .lines()
            .map(str::trim)
            .filter(|line| !line.is_empty())
            .collect();
        format!("\n{}\n", lines.join("\n"))
    }

    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"));
    let mut documents = vec![root.join("README.md")];
    let mut chapters: Vec<_> = std::fs::read_dir(root.join("docs"))
        .expect("docs/")
        .map(|entry| entry.expect("docs/ entry").path())
        .filter(|path| path.extension().is_some_and(|extension| extension == "md"))
        .collect();
    chapters.sort();
    documents.extend(chapters);

    let copies = normalized(include_str!("documentation_examples.rs"));
    let mut checked = 0;
    let mut missing = Vec::new();
    for document in &documents {
        let text = std::fs::read_to_string(document).expect("documentation file");
        let mut block: Option<(usize, Vec<&str>)> = None;
        for (number, line) in text.lines().enumerate() {
            match &mut block {
                None if line == "```rust" => block = Some((number + 1, Vec::new())),
                Some((start, lines)) if line.starts_with("```") => {
                    checked += 1;
                    if !copies.contains(&normalized(&lines.join("\n"))) {
                        missing.push(format!("{}:{start}", document.display()));
                    }
                    block = None;
                }
                Some((_, lines)) => lines.push(line),
                None => {}
            }
        }
    }
    assert!(checked > 0, "no Rust examples found");
    assert!(
        missing.is_empty(),
        "these documentation examples have no compiled copy in tests/documentation_examples.rs \
         (copy the changed block there):\n{}",
        missing.join("\n")
    );
}
