//! Every public fallible item reports the typed `dxn_signalr::Error`: compile-time assertions
//! over the public signatures (function pointers for synchronous items, future output types for
//! asynchronous ones). The test only has to compile and run once.

use std::future::Future;

use dxn_signalr::{
    ArgumentConfiguration, ConnectionConfiguration, Error, ErrorKind, HttpRequest,
    InvocationContext, ManualStream, ReconnectionHandler, SignalRClient, StatefulReconnectConfig,
};

/// A configuration closure, and `SignalRClient::new` with it.
type Configure = fn(&mut ConnectionConfiguration);
type New = fn(&str, &str, Configure) -> Result<SignalRClient, Error>;

/// Asserts that `future` completes with `Result<T, Error>`.
fn output<T, F: Future<Output = Result<T, Error>>>(future: F) -> F {
    future
}

/// The error type can be shared across tasks and threads, cloned into many pending calls, and
/// used with `?` and error-reporting crates.
fn error_traits<
    E: Clone + Send + Sync + std::fmt::Debug + std::fmt::Display + std::error::Error + 'static,
>() {
}

/// Never called: it only has to type-check.
#[allow(dead_code, unused_must_use, clippy::let_underscore_future)]
async fn asynchronous_signatures(
    mut client: SignalRClient,
    mut context: InvocationContext,
    handler: ReconnectionHandler,
) {
    let _ = output::<SignalRClient, _>(SignalRClient::connect("localhost", "hub"));
    let _ = output::<SignalRClient, _>(SignalRClient::connect_with(
        "localhost",
        "hub",
        |_: &mut ConnectionConfiguration| {},
    ));
    let _ = output::<i32, _>(client.invoke::<i32>("Target".to_string()));
    let _ = output::<i32, _>(
        client.invoke_with_args::<i32, _>("Target".to_string(), |_: &mut ArgumentConfiguration| {}),
    );
    let _ = output::<(), _>(client.send("Target".to_string()));
    let _ = output::<(), _>(
        client.send_with_args("Target".to_string(), |_: &mut ArgumentConfiguration| {}),
    );
    let _ = output::<(), _>(client.start());
    let _ = output::<(), _>(client.pause());
    let _ = output::<(), _>(client.resume());
    let _ = output::<(), _>(context.complete(1));
    let _ = output::<(), _>(context.fail("refused"));
    let _ = output::<(), _>(handler.reconnect());
    let _ = output::<(), _>(handler.reconnect_with_policy());
}

#[test]
fn public_fallible_items_report_the_typed_error() {
    error_traits::<Error>();
    let _: fn(usize) -> Result<StatefulReconnectConfig, Error> = StatefulReconnectConfig::new;
    let _: fn(&InvocationContext, usize) -> Result<i32, Error> = InvocationContext::argument::<i32>;
    let _: for<'a> fn(&'a ManualStream<i32>) -> Option<&'a Error> = ManualStream::<i32>::error;
    let _: for<'a> fn(&'a ReconnectionHandler) -> &'a Error = ReconnectionHandler::reason;
    let _: fn(&Error) -> ErrorKind = Error::kind;
    let _: New = SignalRClient::new::<Configure>;
    // `impl Into<String>` parameters: a closure pins the return type.
    let _ = |request: &mut HttpRequest| -> Result<(), Error> { request.set_header("a", "b") };
    let _ = asynchronous_signatures;
}

/// The Android platform-verifier initialization reports the typed error too.
#[cfg(all(target_os = "android", feature = "tls-platform-verifier"))]
#[test]
fn android_initialization_reports_the_typed_error() {
    fn returns_error<A, B>(_: fn(A, B) -> Result<(), Error>) {}
    returns_error(dxn_signalr::tls::android::init_with_env);
}
