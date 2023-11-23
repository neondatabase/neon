


use tracing::info;
use utils::lsn::Lsn;

use crate::walproposer_sim::{log::init_logger, util::TestConfig};

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
    let mut config = TestConfig::new(Some(clock));
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

#[test]
fn crash_safekeeper() {
    let clock = init_logger();
    let mut config = TestConfig::new(Some(clock));
    // config.network.timeout = Some(250);
    let test = config.start(1337);

    let lsn = test.sync_safekeepers().unwrap();
    assert_eq!(lsn, Lsn(0));
    info!("Sucessfully synced empty safekeepers at 0/0");

    let mut wp = test.launch_walproposer(lsn);

    test.poll_for_duration(30);

    wp.write_tx(3);

    test.servers[0].restart();

    test.poll_for_duration(100);
    test.poll_for_duration(1000);
}

#[test]
fn test_simple_restart() {
    let clock = init_logger();
    let mut config = TestConfig::new(Some(clock));
    // config.network.timeout = Some(250);
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

// #[test]
// fn test_simple_schedule() -> anyhow::Result<()> {
//     let clock = init_logger();
//     let mut config = TestConfig::new(Some(clock));
//     config.network.keepalive_timeout = Some(100);
//     let test = config.start(1337);

//     let schedule: Schedule = vec![
//         (0, TestAction::RestartWalProposer),
//         (50, TestAction::WriteTx(5)),
//         (100, TestAction::RestartSafekeeper(0)),
//         (100, TestAction::WriteTx(5)),
//         (110, TestAction::RestartSafekeeper(1)),
//         (110, TestAction::WriteTx(5)),
//         (120, TestAction::RestartSafekeeper(2)),
//         (120, TestAction::WriteTx(5)),
//         (201, TestAction::RestartWalProposer),
//         (251, TestAction::RestartSafekeeper(0)),
//         (251, TestAction::RestartSafekeeper(1)),
//         (251, TestAction::RestartSafekeeper(2)),
//         (251, TestAction::WriteTx(5)),
//         (255, TestAction::WriteTx(5)),
//         (1000, TestAction::WriteTx(5)),
//     ];

//     test.run_schedule(&schedule)?;
//     info!("Test finished, stopping all threads");
//     test.world.deallocate();

//     Ok(())
// }

// #[test]
// fn test_many_tx() -> anyhow::Result<()> {
//     enable_debug();
//     let clock = init_logger();
//     let mut config = TestConfig::new(Some(clock));
//     let test = config.start(1337);

//     let mut schedule: Schedule = vec![];
//     for i in 0..100 {
//         schedule.push((i * 10, TestAction::WriteTx(10)));
//     }

//     test.run_schedule(&schedule)?;
//     info!("Test finished, stopping all threads");
//     test.world.stop_all();

//     let events = test.world.take_events();
//     info!("Events: {:?}", events);
//     let last_commit_lsn = events
//         .iter()
//         .filter_map(|event| {
//             if event.data.starts_with("commit_lsn;") {
//                 let lsn: u64 = event.data.split(';').nth(1).unwrap().parse().unwrap();
//                 return Some(lsn);
//             }
//             None
//         })
//         .last()
//         .unwrap();

//     let initdb_lsn = 21623024;
//     let diff = last_commit_lsn - initdb_lsn;
//     info!("Last commit lsn: {}, diff: {}", last_commit_lsn, diff);
//     assert!(diff > 1000 * 8);
//     Ok(())
// }

// #[test]
// fn test_random_schedules() -> anyhow::Result<()> {
//     let clock = init_logger();
//     let mut config = TestConfig::new(Some(clock));
//     config.network.keepalive_timeout = Some(100);

//     for i in 0..30000 {
//         let seed: u64 = rand::thread_rng().gen();
//         config.network = generate_network_opts(seed);

//         let test = config.start(seed);
//         warn!("Running test with seed {}", seed);

//         let schedule = generate_schedule(seed);
//         test.run_schedule(&schedule).unwrap();
//         validate_events(test.world.take_events());
//         test.world.deallocate();
//     }

//     Ok(())
// }

// #[test]
// fn test_one_schedule() -> anyhow::Result<()> {
//     enable_debug();
//     let clock = init_logger();
//     let mut config = TestConfig::new(Some(clock));
//     config.network.keepalive_timeout = Some(100);

//     // let seed = 6762900106769428342;
//     // let test = config.start(seed);
//     // warn!("Running test with seed {}", seed);

//     // let schedule = generate_schedule(seed);
//     // info!("schedule: {:?}", schedule);
//     // test.run_schedule(&schedule)?;
//     // test.world.deallocate();

//     let seed = 3649773280641776194;
//     config.network = generate_network_opts(seed);
//     info!("network: {:?}", config.network);
//     let test = config.start(seed);
//     warn!("Running test with seed {}", seed);

//     let schedule = generate_schedule(seed);
//     info!("schedule: {:?}", schedule);
//     test.run_schedule(&schedule).unwrap();
//     validate_events(test.world.take_events());
//     test.world.deallocate();

//     Ok(())
// }

// #[test]
// fn test_res_dealloc() -> anyhow::Result<()> {
//     // enable_debug();
//     let clock = init_logger();
//     let mut config = TestConfig::new(Some(clock));

//     // print pid
//     let pid = unsafe { libc::getpid() };
//     info!("pid: {}", pid);

//     let seed = 123456;
//     config.network = generate_network_opts(seed);
//     let test = config.start(seed);
//     warn!("Running test with seed {}", seed);

//     let schedule = generate_schedule(seed);
//     info!("schedule: {:?}", schedule);
//     test.run_schedule(&schedule).unwrap();
//     test.world.stop_all();

//     let world = test.world.clone();
//     drop(test);
//     info!("world strong count: {}", Arc::strong_count(&world));
//     world.deallocate();
//     info!("world strong count: {}", Arc::strong_count(&world));

//     Ok(())
// }
