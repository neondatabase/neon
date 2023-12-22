use std::sync::Arc;

use rand::Rng;
use tracing::{info, warn};
use utils::lsn::Lsn;

use crate::walproposer_sim::{
    log::init_logger,
    util::{
        generate_network_opts, generate_schedule, validate_events, Schedule, TestAction, TestConfig,
    },
};

mod walproposer_sim;

#[test]
fn sync_empty_safekeepers() {
    let clock = init_logger();
    let config = TestConfig::new(Some(clock));
    let test = config.start(1337);

    let lsn = test.sync_safekeepers().unwrap();
    assert_eq!(lsn, Lsn(0));
    info!("Sucessfully synced empty safekeepers at 0/0");

    let lsn = test.sync_safekeepers().unwrap();
    assert_eq!(lsn, Lsn(0));
    info!("Sucessfully synced (again) empty safekeepers at 0/0");
}

#[test]
fn run_walproposer_generate_wal() {
    let clock = init_logger();
    let config = TestConfig::new(Some(clock));
    // config.network.timeout = Some(250);
    let test = config.start(1337);

    let lsn = test.sync_safekeepers().unwrap();
    assert_eq!(lsn, Lsn(0));
    info!("Sucessfully synced empty safekeepers at 0/0");

    let mut wp = test.launch_walproposer(lsn);

    test.poll_for_duration(30);

    for _ in 0..100 {
        wp.write_tx(1);
        test.poll_for_duration(5);
    }
}
