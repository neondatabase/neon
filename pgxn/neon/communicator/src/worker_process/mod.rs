//! This code runs in the communicator worker process. This provides
//! the glue code to:
//!
//! - launch the main loop,
//! - receive IO requests from backends and process them,
//! - write results back to backends.

mod callbacks;
mod control_socket;
mod lfc_metrics;
mod logging;
mod main_loop;
mod worker_interface;
