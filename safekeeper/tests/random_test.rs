use rand::Rng;
use tracing::{info, warn};

use crate::walproposer_sim::{
    log::{init_logger, init_tracing_logger},
    simulation::{generate_network_opts, generate_schedule, TestConfig},
    simulation_logs::validate_events,
};

pub mod walproposer_sim;

// Generates 500 random seeds and runs a schedule for each of them.
// If you see this test fail, please report the last seed to the
// @safekeeper team.
#[test]
fn test_random_schedules() -> anyhow::Result<()> {
    let clock = init_logger();
    let mut config = TestConfig::new(Some(clock));

    for _ in 0..500 {
        let seed: u64 = rand::thread_rng().gen();
        config.network = generate_network_opts(seed);

        let test = config.start(seed);
        warn!("Running test with seed {}", seed);

        let schedule = generate_schedule(seed);
        test.run_schedule(&schedule).unwrap();
        validate_events(test.world.take_events());
        test.world.deallocate();
    }

    Ok(())
}

// After you found a seed that fails, you can insert this seed here
// and run the test to see the full debug output.
#[test]
fn test_one_schedule() -> anyhow::Result<()> {
    let clock = init_tracing_logger(true);
    let mut config = TestConfig::new(Some(clock));

    let seed = 11047466935058776390;
    config.network = generate_network_opts(seed);
    info!("network: {:?}", config.network);
    let test = config.start(seed);
    warn!("Running test with seed {}", seed);

    let schedule = generate_schedule(seed);
    info!("schedule: {:?}", schedule);
    test.run_schedule(&schedule).unwrap();
    validate_events(test.world.take_events());
    test.world.deallocate();

    Ok(())
}
