use rand::Rng;
use std::sync::Arc;
use std::time::SystemTime;
use std::{thread, time};

use control_plane::compute::{ComputeControlPlane, PostgresNode};

use integration_tests::PostgresNodeExt;
use integration_tests::TestStorageControlPlane;

const DOWNTIME: u64 = 2;

fn start_node_with_wal_proposer(
    timeline: &str,
    compute_cplane: &mut ComputeControlPlane,
    wal_acceptors: &str,
) -> Arc<PostgresNode> {
    let node = compute_cplane.new_test_master_node(timeline);
    let _node = node.append_conf(
        "postgresql.conf",
        &format!("wal_acceptors='{}'\n", wal_acceptors),
    );
    node.start().unwrap();
    node
}

#[test]
fn test_embedded_wal_proposer() {
    let local_env = integration_tests::create_test_env("test_embedded_wal_proposer");

    const REDUNDANCY: usize = 3;
    let storage_cplane = TestStorageControlPlane::fault_tolerant(&local_env, REDUNDANCY);
    let mut compute_cplane = ComputeControlPlane::local(&local_env, &storage_cplane.pageserver);
    let wal_acceptors = storage_cplane.get_wal_acceptor_conn_info();

    // start postgres
    let node = start_node_with_wal_proposer("main", &mut compute_cplane, &wal_acceptors);

    // check basic work with table
    node.safe_psql(
        "postgres",
        "CREATE TABLE t(key int primary key, value text)",
    );
    node.safe_psql(
        "postgres",
        "INSERT INTO t SELECT generate_series(1,100000), 'payload'",
    );
    let count: i64 = node
        .safe_psql("postgres", "SELECT sum(key) FROM t")
        .first()
        .unwrap()
        .get(0);
    println!("sum = {}", count);
    assert_eq!(count, 5000050000);
    // check wal files equality
}

#[test]
fn test_acceptors_normal_work() {
    let local_env = integration_tests::create_test_env("test_acceptors_normal_work");

    const REDUNDANCY: usize = 3;
    let storage_cplane = TestStorageControlPlane::fault_tolerant(&local_env, REDUNDANCY);
    let mut compute_cplane = ComputeControlPlane::local(&local_env, &storage_cplane.pageserver);
    let wal_acceptors = storage_cplane.get_wal_acceptor_conn_info();

    // start postgres
    let node = start_node_with_wal_proposer("main", &mut compute_cplane, &wal_acceptors);

    // check basic work with table
    node.safe_psql(
        "postgres",
        "CREATE TABLE t(key int primary key, value text)",
    );
    node.safe_psql(
        "postgres",
        "INSERT INTO t SELECT generate_series(1,100000), 'payload'",
    );
    let count: i64 = node
        .safe_psql("postgres", "SELECT sum(key) FROM t")
        .first()
        .unwrap()
        .get(0);
    println!("sum = {}", count);
    assert_eq!(count, 5000050000);
    // check wal files equality
}

// Run page server and multiple safekeepers, and multiple compute nodes running
// against different timelines.
#[test]
fn test_many_timelines() {
    // Initialize a new repository, and set up WAL safekeepers and page server.
    const REDUNDANCY: usize = 3;
    const N_TIMELINES: usize = 5;
    let local_env = integration_tests::create_test_env("test_many_timelines");
    let storage_cplane = TestStorageControlPlane::fault_tolerant(&local_env, REDUNDANCY);
    let mut compute_cplane = ComputeControlPlane::local(&local_env, &storage_cplane.pageserver);
    let wal_acceptors = storage_cplane.get_wal_acceptor_conn_info();

    // Create branches
    let mut timelines: Vec<String> = vec!["main".to_string()];

    for i in 1..N_TIMELINES {
        let branchname = format!("experimental{}", i);
        storage_cplane
            .pageserver
            .branch_create(&branchname, "main")
            .unwrap();
        timelines.push(branchname);
    }

    // start postgres on each timeline
    let mut nodes = Vec::new();
    for tli_name in timelines {
        let node = start_node_with_wal_proposer(&tli_name, &mut compute_cplane, &wal_acceptors);
        nodes.push(node.clone());
    }

    // create schema
    for node in &nodes {
        node.safe_psql(
            "postgres",
            "CREATE TABLE t(key int primary key, value text)",
        );
    }

    // Populate data
    for node in &nodes {
        node.safe_psql(
            "postgres",
            "INSERT INTO t SELECT generate_series(1,100000), 'payload'",
        );
    }

    // Check data
    for node in &nodes {
        let count: i64 = node
            .safe_psql("postgres", "SELECT sum(key) FROM t")
            .first()
            .unwrap()
            .get(0);
        println!("sum = {}", count);
        assert_eq!(count, 5000050000);
    }
}

// Majority is always alive
#[test]
fn test_acceptors_restarts() {
    let local_env = integration_tests::create_test_env("test_acceptors_restarts");

    // Start pageserver that reads WAL directly from that postgres
    const REDUNDANCY: usize = 3;
    const FAULT_PROBABILITY: f32 = 0.01;

    let storage_cplane = TestStorageControlPlane::fault_tolerant(&local_env, REDUNDANCY);
    let mut compute_cplane = ComputeControlPlane::local(&local_env, &storage_cplane.pageserver);
    let wal_acceptors = storage_cplane.get_wal_acceptor_conn_info();
    let mut rng = rand::thread_rng();

    // start postgres
    let node = start_node_with_wal_proposer("main", &mut compute_cplane, &wal_acceptors);

    let mut failed_node: Option<usize> = None;

    // check basic work with table
    node.safe_psql(
        "postgres",
        "CREATE TABLE t(key int primary key, value text)",
    );
    let mut psql = node.open_psql("postgres");
    for i in 1..=1000 {
        psql.execute("INSERT INTO t values ($1, 'payload')", &[&i])
            .unwrap();
        let prob: f32 = rng.gen();
        if prob <= FAULT_PROBABILITY {
            if let Some(node) = failed_node {
                storage_cplane.wal_acceptors[node].start();
                failed_node = None;
            } else {
                let node: usize = rng.gen_range(0..REDUNDANCY);
                failed_node = Some(node);
                storage_cplane.wal_acceptors[node].stop().unwrap();
            }
        }
    }
    let count: i64 = node
        .safe_psql("postgres", "SELECT sum(key) FROM t")
        .first()
        .unwrap()
        .get(0);
    println!("sum = {}", count);
    assert_eq!(count, 500500);
}

fn start_acceptor(cplane: &Arc<TestStorageControlPlane>, no: usize) {
    let cp = cplane.clone();
    thread::spawn(move || {
        thread::sleep(time::Duration::from_secs(DOWNTIME));
        cp.wal_acceptors[no].start();
    });
}

// Stop majority of acceptors while compute is under the load. Boot
// them again and check that nothing was losed. Repeat.
// N_CRASHES env var
#[test]
fn test_acceptors_unavailability() {
    let local_env = integration_tests::create_test_env("test_acceptors_unavailability");

    // Start pageserver that reads WAL directly from that postgres
    const REDUNDANCY: usize = 2;

    let storage_cplane = TestStorageControlPlane::fault_tolerant(&local_env, REDUNDANCY);
    let mut compute_cplane = ComputeControlPlane::local(&local_env, &storage_cplane.pageserver);
    let wal_acceptors = storage_cplane.get_wal_acceptor_conn_info();

    // start postgres
    let node = start_node_with_wal_proposer("main", &mut compute_cplane, &wal_acceptors);

    // check basic work with table
    node.safe_psql(
        "postgres",
        "CREATE TABLE t(key int primary key, value text)",
    );
    let mut psql = node.open_psql("postgres");
    psql.execute("INSERT INTO t values (1, 'payload')", &[])
        .unwrap();

    // Shut down all wal acceptors
    storage_cplane.wal_acceptors[0].stop().unwrap();
    let cp = Arc::new(storage_cplane);
    start_acceptor(&cp, 0);
    let now = SystemTime::now();
    psql.execute("INSERT INTO t values (2, 'payload')", &[])
        .unwrap();
    // Here we check that the query above was hanging
    // while wal_acceptor was unavailiable
    assert!(now.elapsed().unwrap().as_secs() >= DOWNTIME);
    psql.execute("INSERT INTO t values (3, 'payload')", &[])
        .unwrap();

    cp.wal_acceptors[1].stop().unwrap();
    start_acceptor(&cp, 1);
    psql.execute("INSERT INTO t values (4, 'payload')", &[])
        .unwrap();
    // Here we check that the query above was hanging
    // while wal_acceptor was unavailiable
    assert!(now.elapsed().unwrap().as_secs() >= 2 * DOWNTIME);

    psql.execute("INSERT INTO t values (5, 'payload')", &[])
        .unwrap();

    let count: i64 = node
        .safe_psql("postgres", "SELECT sum(key) FROM t")
        .first()
        .unwrap()
        .get(0);
    println!("sum = {}", count);
    // Ensure that all inserts succeeded.
    // Including ones that were waiting for wal acceptor restart.
    assert_eq!(count, 15);
}

fn simulate_failures(cplane: Arc<TestStorageControlPlane>) {
    let mut rng = rand::thread_rng();
    let n_acceptors = cplane.wal_acceptors.len();
    let failure_period = time::Duration::from_secs(1);
    while cplane.is_running() {
        thread::sleep(failure_period);
        let mask: u32 = rng.gen_range(0..(1 << n_acceptors));
        for i in 0..n_acceptors {
            if (mask & (1 << i)) != 0 {
                cplane.wal_acceptors[i].stop().unwrap();
            }
        }
        thread::sleep(failure_period);
        for i in 0..n_acceptors {
            if (mask & (1 << i)) != 0 {
                cplane.wal_acceptors[i].start();
            }
        }
    }
}

// Race condition test
#[test]
fn test_race_conditions() {
    let local_env = integration_tests::create_test_env("test_race_conditions");

    // Start pageserver that reads WAL directly from that postgres
    const REDUNDANCY: usize = 3;

    let storage_cplane = Arc::new(TestStorageControlPlane::fault_tolerant(
        &local_env, REDUNDANCY,
    ));
    let mut compute_cplane = ComputeControlPlane::local(&local_env, &storage_cplane.pageserver);
    let wal_acceptors = storage_cplane.get_wal_acceptor_conn_info();

    // start postgres
    let node = start_node_with_wal_proposer("main", &mut compute_cplane, &wal_acceptors);

    // check basic work with table
    node.safe_psql(
        "postgres",
        "CREATE TABLE t(key int primary key, value text)",
    );

    let cp = storage_cplane.clone();
    let failures_thread = thread::spawn(move || {
        simulate_failures(cp);
    });

    let mut psql = node.open_psql("postgres");
    for i in 1..=1000 {
        psql.execute("INSERT INTO t values ($1, 'payload')", &[&i])
            .unwrap();
    }
    let count: i64 = node
        .safe_psql("postgres", "SELECT sum(key) FROM t")
        .first()
        .unwrap()
        .get(0);
    println!("sum = {}", count);
    assert_eq!(count, 500500);

    storage_cplane.stop();
    failures_thread.join().unwrap();
}
