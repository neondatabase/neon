use rand::Rng;
use tracing::{info, warn};

use crate::walproposer_sim::{
    log::init_logger,
    simulation::{generate_network_opts, generate_schedule, TestConfig},
    simulation_logs::validate_events,
};

pub mod walproposer_sim;

#[test]
fn test_random_schedules() -> anyhow::Result<()> {
    let clock = init_logger();
    let mut config = TestConfig::new(Some(clock));

    for _ in 0..2000 {
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

#[test]
fn test_one_schedule() -> anyhow::Result<()> {
    // enable_debug();
    let clock = init_logger();
    let mut config = TestConfig::new(Some(clock));

    let seed = 2717576027256331644;
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
