use tracing::info;
use utils::lsn::Lsn;

use crate::walproposer_sim::{log::init_logger, simulation::TestConfig};

pub mod walproposer_sim;

// Check that first start of sync_safekeepers() returns 0/0 on empty safekeepers.
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

// Check that there are no panics when we are writing and streaming WAL to safekeepers.
#[test]
fn run_walproposer_generate_wal() {
    let clock = init_logger();
    let config = TestConfig::new(Some(clock));
    let test = config.start(1337);

    let lsn = test.sync_safekeepers().unwrap();
    assert_eq!(lsn, Lsn(0));
    info!("Sucessfully synced empty safekeepers at 0/0");

    let mut wp = test.launch_walproposer(lsn);

    // wait for walproposer to start
    test.poll_for_duration(30);

    // just write some WAL
    for _ in 0..100 {
        wp.write_tx(1);
        test.poll_for_duration(5);
    }
}
