use std::{fmt, sync::Arc};

use once_cell::sync::OnceCell;
use desim::{sync::Mutex, world::World};
use tracing_subscriber::fmt::{format::Writer, time::FormatTime};

#[derive(Clone)]
pub struct SimClock {
    world_ptr: Arc<Mutex<Option<Arc<World>>>>,
}

impl Default for SimClock {
    fn default() -> Self {
        SimClock {
            world_ptr: Arc::new(Mutex::new(None)),
        }
    }
}

impl SimClock {
    pub fn set_world(&self, world: Arc<World>) {
        *self.world_ptr.lock() = Some(world);
    }
}

impl FormatTime for SimClock {
    fn format_time(&self, w: &mut Writer<'_>) -> fmt::Result {
        let world = self.world_ptr.lock().clone();

        if let Some(world) = world {
            let now = world.now();
            write!(w, "[{}]", now)
        } else {
            write!(w, "[?]")
        }
    }
}

static LOGGING_DONE: OnceCell<SimClock> = OnceCell::new();

fn init_tracing_logger(debug_enabled: bool) -> SimClock {
    LOGGING_DONE
        .get_or_init(|| {
            let clock = SimClock::default();
            let base_logger = tracing_subscriber::fmt()
                .with_target(false)
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
    // check env for DEBUG
    let debug_enabled = std::env::var("RUST_TRACEBACK").is_ok();

    init_tracing_logger(debug_enabled)
}
