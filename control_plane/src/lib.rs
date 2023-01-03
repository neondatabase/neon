//
// Local control plane.
//
// Can start, configure and stop postgres instances running as a local processes.
//
// Intended to be used in integration tests and in CLI tools for
// local installations.
//

mod background_process;
pub mod broker;
pub mod compute;
pub mod local_env;
pub mod pageserver;
pub mod postgresql_conf;
pub mod safekeeper;
