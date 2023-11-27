use std::{fmt, sync::Arc};

use slowsim::{sync::Mutex, world::World};
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

pub fn init_logger() -> SimClock {
    // check env for DEBUG
    let debug_enabled = std::env::var("RUST_TRACEBACK").is_ok();

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
}
