use std::{sync::Arc, fmt};

use safekeeper::simlib::{world::World, sync::Mutex};
use tracing_subscriber::fmt::{time::FormatTime, format::Writer};
use utils::logging;


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
    let clock = SimClock::default();
    let base_logger = tracing_subscriber::fmt()
        .with_target(false)
        .with_timer(clock.clone())
        .with_ansi(true)
        // .with_max_level(tracing::Level::DEBUG)
        .with_writer(std::io::stdout);
    base_logger.init();

    // logging::replace_panic_hook_with_tracing_panic_hook().forget();
    std::panic::set_hook(Box::new(|_| {}));

    clock
}
