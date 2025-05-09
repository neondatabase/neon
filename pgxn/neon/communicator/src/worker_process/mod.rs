//! This code runs in the communicator worker process. This provides
//! the glue code to:
//!
//! - launch the 'processor',
//! - receive IO requests from backends and pass them to the processor,
//! - write results back to backends.

mod callbacks;
mod logging;
mod main_loop;
mod metrics_exporter;
mod worker_interface;

mod in_progress_ios;
