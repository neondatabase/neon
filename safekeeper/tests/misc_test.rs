use std::sync::Arc;

use tracing::{info, warn};
use utils::lsn::Lsn;

use crate::walproposer_sim::{
    log::{init_logger, init_tracing_logger},
    simulation::{generate_network_opts, generate_schedule, Schedule, TestAction, TestConfig},
};

pub mod walproposer_sim;

// Test that simulation supports restarting (crashing) safekeepers.
#[test]
fn crash_safekeeper() {
    let clock = init_logger();
    let config = TestConfig::new(Some(clock));
    let test = config.start(1337);

    let lsn = test.sync_safekeepers().unwrap();
    assert_eq!(lsn, Lsn(0));
    info!("Sucessfully synced empty safekeepers at 0/0");

    let mut wp = test.launch_walproposer(lsn);

    // Write some WAL and crash safekeeper 0 without waiting for replication.
    test.poll_for_duration(30);
    wp.write_tx(3);
    test.servers[0].restart();

    // Wait some time, so that walproposer can reconnect.
    test.poll_for_duration(2000);
}

// Test that walproposer can be crashed (stopped).
#[test]
fn test_simple_restart() {
    let clock = init_logger();
    let config = TestConfig::new(Some(clock));
    let test = config.start(1337);

    let lsn = test.sync_safekeepers().unwrap();
    assert_eq!(lsn, Lsn(0));
    info!("Sucessfully synced empty safekeepers at 0/0");

    let mut wp = test.launch_walproposer(lsn);

    test.poll_for_duration(30);
    wp.write_tx(3);
    test.poll_for_duration(100);

    wp.stop();
    drop(wp);

    let lsn = test.sync_safekeepers().unwrap();
    info!("Sucessfully synced safekeepers at {}", lsn);
}

// Test runnning a simple schedule, restarting everything a several times.
#[test]
fn test_simple_schedule() -> anyhow::Result<()> {
    let clock = init_logger();
    let mut config = TestConfig::new(Some(clock));
    config.network.keepalive_timeout = Some(100);
    let test = config.start(1337);

    let schedule: Schedule = vec![
        (0, TestAction::RestartWalProposer),
        (50, TestAction::WriteTx(5)),
        (100, TestAction::RestartSafekeeper(0)),
        (100, TestAction::WriteTx(5)),
        (110, TestAction::RestartSafekeeper(1)),
        (110, TestAction::WriteTx(5)),
        (120, TestAction::RestartSafekeeper(2)),
        (120, TestAction::WriteTx(5)),
        (201, TestAction::RestartWalProposer),
        (251, TestAction::RestartSafekeeper(0)),
        (251, TestAction::RestartSafekeeper(1)),
        (251, TestAction::RestartSafekeeper(2)),
        (251, TestAction::WriteTx(5)),
        (255, TestAction::WriteTx(5)),
        (1000, TestAction::WriteTx(5)),
    ];

    test.run_schedule(&schedule)?;
    info!("Test finished, stopping all threads");
    test.world.deallocate();

    Ok(())
}

// Test that simulation can process 10^4 transactions.
#[test]
fn test_many_tx() -> anyhow::Result<()> {
    let clock = init_logger();
    let config = TestConfig::new(Some(clock));
    let test = config.start(1337);

    let mut schedule: Schedule = vec![];
    for i in 0..100 {
        schedule.push((i * 10, TestAction::WriteTx(100)));
    }

    test.run_schedule(&schedule)?;
    info!("Test finished, stopping all threads");
    test.world.stop_all();

    let events = test.world.take_events();
    info!("Events: {:?}", events);
    let last_commit_lsn = events
        .iter()
        .filter_map(|event| {
            if event.data.starts_with("commit_lsn;") {
                let lsn: u64 = event.data.split(';').nth(1).unwrap().parse().unwrap();
                return Some(lsn);
            }
            None
        })
        .last()
        .unwrap();

    let initdb_lsn = 21623024;
    let diff = last_commit_lsn - initdb_lsn;
    info!("Last commit lsn: {}, diff: {}", last_commit_lsn, diff);
    // each tx is at least 8 bytes, it's written a 100 times for in a loop for 100 times
    assert!(diff > 100 * 100 * 8);
    Ok(())
}

// Checks that we don't have nasty circular dependencies, preventing Arc from deallocating.
// This test doesn't really assert anything, you need to run it manually to check if there
// is any issue.
#[test]
fn test_res_dealloc() -> anyhow::Result<()> {
    let clock = init_tracing_logger(true);
    let mut config = TestConfig::new(Some(clock));

    let seed = 123456;
    config.network = generate_network_opts(seed);
    let test = config.start(seed);
    warn!("Running test with seed {}", seed);

    let schedule = generate_schedule(seed);
    info!("schedule: {:?}", schedule);
    test.run_schedule(&schedule).unwrap();
    test.world.stop_all();

    let world = test.world.clone();
    drop(test);
    info!("world strong count: {}", Arc::strong_count(&world));
    world.deallocate();
    info!("world strong count: {}", Arc::strong_count(&world));

    Ok(())
}
