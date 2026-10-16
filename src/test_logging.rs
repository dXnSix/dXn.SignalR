//! A capturing logger for unit tests. `log` allows one logger per process, so it is installed
//! once and records only what the calling thread logs while a capture runs; records of other
//! tests (on other threads) are ignored.

use std::{cell::RefCell, sync::Once};

/// One captured log record.
#[derive(Clone, Debug)]
pub(crate) struct CapturedRecord {
    pub(crate) level: log::Level,
    pub(crate) target: String,
    pub(crate) message: String,
}

thread_local! {
    static CAPTURE: RefCell<Option<Vec<CapturedRecord>>> = const { RefCell::new(None) };
}

struct ThreadCapture;

impl log::Log for ThreadCapture {
    fn enabled(&self, _metadata: &log::Metadata<'_>) -> bool {
        CAPTURE.with(|capture| capture.try_borrow().is_ok_and(|records| records.is_some()))
    }

    fn log(&self, record: &log::Record<'_>) {
        CAPTURE.with(|capture| {
            if let Ok(mut capture) = capture.try_borrow_mut() {
                if let Some(records) = capture.as_mut() {
                    records.push(CapturedRecord {
                        level: record.level(),
                        target: record.target().to_string(),
                        message: record.args().to_string(),
                    });
                }
            }
        });
    }

    fn flush(&self) {}
}

static LOGGER: ThreadCapture = ThreadCapture;
static INSTALL: Once = Once::new();

/// Runs `body` and returns its result with every record it logged on this thread, at any level.
pub(crate) fn capture<R>(body: impl FnOnce() -> R) -> (R, Vec<CapturedRecord>) {
    INSTALL.call_once(|| {
        log::set_logger(&LOGGER).expect("no other logger is installed in unit tests");
        log::set_max_level(log::LevelFilter::Trace);
    });
    CAPTURE.with(|capture| *capture.borrow_mut() = Some(Vec::new()));
    let result = body();
    let records = CAPTURE.with(|capture| capture.borrow_mut().take().unwrap_or_default());
    (result, records)
}
