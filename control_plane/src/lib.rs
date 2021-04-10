//
// Local control plane.
//
// Can start, cofigure and stop postgres instances running as a local processes.
//
// Intended to be used in integration tests and in CLI tools for
// local installations.
//

pub mod local_env;
pub mod compute;
pub mod storage;
