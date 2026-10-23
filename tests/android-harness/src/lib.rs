//! JNI entry points of the test-only Android app (`tests/android-app`, class
//! `dev.dxn.signalr.e2e.Harness`): `initialize` sets up the platform verifier from the app's
//! `Context` exactly as the documentation tells consumers, and `runScenario` runs one shared
//! `tests/e2e` scenario inside the app process. Test-only; built by the Aspire runner.

use std::time::Duration;

use jni::{
    errors::ThrowRuntimeExAndDefault,
    objects::{JClass, JObject, JString},
    EnvUnowned,
};
use dxn_signalr_e2e_scenarios::Protocol;

/// The scenario bound; the runner kills the instrumentation at the cell bound.
const SCENARIO_TIMEOUT: Duration = Duration::from_secs(50);

/// `Harness.initialize(Context): String`: `ok`, or why the platform verifier could not start.
#[no_mangle]
pub extern "system" fn Java_dev_dxn_signalr_e2e_Harness_initialize<'local>(
    mut unowned: EnvUnowned<'local>,
    _class: JClass<'local>,
    context: JObject<'local>,
) -> JString<'local> {
    unowned
        .with_env(|env| -> Result<_, jni::errors::Error> {
            let outcome = match dxn_signalr::tls::android::init_with_env(env, context) {
                Ok(()) => "ok".to_string(),
                Err(error) => error.to_string(),
            };
            JString::from_str(env, outcome)
        })
        .resolve::<ThrowRuntimeExAndDefault>()
}

/// `Harness.runScenario(String, String, String): String`: runs scenario `name` with `protocol`
/// after setting the scenario variables (`KEY=VALUE` lines); returns `ok` or the failure.
#[no_mangle]
pub extern "system" fn Java_dev_dxn_signalr_e2e_Harness_runScenario<'local>(
    mut unowned: EnvUnowned<'local>,
    _class: JClass<'local>,
    name: JString<'local>,
    protocol: JString<'local>,
    environment: JString<'local>,
) -> JString<'local> {
    unowned
        .with_env(|env| -> Result<_, jni::errors::Error> {
            let outcome = run(
                &name.to_string(),
                &protocol.to_string(),
                &environment.to_string(),
            );
            JString::from_str(env, outcome)
        })
        .resolve::<ThrowRuntimeExAndDefault>()
}

/// Runs one scenario on its own thread and current-thread runtime, as the `e2e` binary runs each
/// test, and turns a panic or a timeout into the failure text.
fn run(name: &str, protocol: &str, environment: &str) -> String {
    for line in environment.lines() {
        if let Some((key, value)) = line.split_once('=') {
            std::env::set_var(key, value);
        }
    }
    // As `RUST_LOG` selects for the other Rust cells (the runner's filter for the scenario).
    let filter = std::env::var("RUST_LOG").unwrap_or_else(|_| "dxn_signalr=info".to_string());
    android_logger::init_once(
        android_logger::Config::default()
            .with_tag("dxn-signalr")
            .with_max_level(log::LevelFilter::Trace)
            .with_filter(android_logger::FilterBuilder::new().parse(&filter).build()),
    );
    log::info!("running {name} ({protocol})");
    let protocol = match protocol {
        "json" => Protocol::Json,
        "messagepack" => Protocol::MessagePack,
        other => return format!("unknown protocol {other}"),
    };
    let scenario = name.to_string();
    let worker = std::thread::spawn(move || -> Result<(), String> {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|error| format!("cannot start a runtime: {error}"))?;
        runtime.block_on(async {
            tokio::time::timeout(
                SCENARIO_TIMEOUT,
                dxn_signalr_e2e_scenarios::run(&scenario, protocol),
            )
            .await
            .map_err(|_| format!("{scenario} exceeded {SCENARIO_TIMEOUT:?}"))?
        })
    });
    let outcome = match worker.join() {
        Ok(Ok(())) => "ok".to_string(),
        Ok(Err(error)) => error,
        Err(panic) => format!(
            "the scenario panicked: {}",
            panic
                .downcast_ref::<String>()
                .map(String::as_str)
                .or_else(|| panic.downcast_ref::<&str>().copied())
                .unwrap_or("(no message)")
        ),
    };
    log::info!("{name} ({protocol:?}): {outcome}");
    outcome
}
