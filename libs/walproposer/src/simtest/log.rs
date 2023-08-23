use std::{sync::Arc, fmt};

use safekeeper::simlib::{world::World, sync::Mutex};
use tracing_subscriber::fmt::{time::FormatTime, format::Writer};


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
        .with_writer(std::io::stdout);
    base_logger.init();

    clock
}
