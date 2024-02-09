use std::{fmt, sync::Arc};

use desim::time::Timing;
use once_cell::sync::OnceCell;
use parking_lot::Mutex;
use tracing_subscriber::fmt::{format::Writer, time::FormatTime};

/// SimClock can be plugged into tracing logger to print simulation time.
#[derive(Clone)]
pub struct SimClock {
    clock_ptr: Arc<Mutex<Option<Arc<Timing>>>>,
}

impl Default for SimClock {
    fn default() -> Self {
        SimClock {
            clock_ptr: Arc::new(Mutex::new(None)),
        }
    }
}

impl SimClock {
    pub fn set_clock(&self, clock: Arc<Timing>) {
        *self.clock_ptr.lock() = Some(clock);
    }
}

impl FormatTime for SimClock {
    fn format_time(&self, w: &mut Writer<'_>) -> fmt::Result {
        let clock = self.clock_ptr.lock();

        if let Some(clock) = clock.as_ref() {
            let now = clock.now();
            write!(w, "[{}]", now)
        } else {
            write!(w, "[?]")
        }
    }
}

static LOGGING_DONE: OnceCell<SimClock> = OnceCell::new();

/// Returns ptr to clocks attached to tracing logger to update them when the
/// world is (re)created.
pub fn init_tracing_logger(debug_enabled: bool) -> SimClock {
    LOGGING_DONE
        .get_or_init(|| {
            let clock = SimClock::default();
            let base_logger = tracing_subscriber::fmt()
                .with_target(false)
                // prefix log lines with simulated time timestamp
                .with_timer(clock.clone())
                // .with_ansi(true) TODO
                .with_max_level(match debug_enabled {
                    true => tracing::Level::DEBUG,
                    false => tracing::Level::WARN,
                })
                .with_writer(std::io::stdout);
            base_logger.init();

            // logging::replace_panic_hook_with_tracing_panic_hook().forget();

            if !debug_enabled {
                std::panic::set_hook(Box::new(|_| {}));
            }

            clock
        })
        .clone()
}

pub fn init_logger() -> SimClock {
    // RUST_TRACEBACK envvar controls whether we print all logs or only warnings.
    let debug_enabled = std::env::var("RUST_TRACEBACK").is_ok();

    init_tracing_logger(debug_enabled)
}
