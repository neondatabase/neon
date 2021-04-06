// Restart acceptors one by one while compute is under the load.
#[allow(dead_code)]
mod control_plane;
use control_plane::ComputeControlPlane;
use control_plane::StorageControlPlane;

use rand::Rng;
use std::sync::Arc;
use std::time::SystemTime;
use std::{thread, time};

#[test]
fn test_acceptors_normal_work() {
    // Start pageserver that reads WAL directly from that postgres
    const REDUNDANCY: usize = 3;
    let storage_cplane = StorageControlPlane::fault_tolerant(REDUNDANCY);
    let mut compute_cplane = ComputeControlPlane::local(&storage_cplane);
    let wal_acceptors = storage_cplane.get_wal_acceptor_conn_info();

    // start postgre
    let node = compute_cplane.new_master_node();
    node.start(&storage_cplane);

    // start proxy
    let _proxy = node.start_proxy(wal_acceptors);

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

// Majority is always alive
#[test]
fn test_acceptors_restarts() {
    // Start pageserver that reads WAL directly from that postgres
    const REDUNDANCY: usize = 3;
    const FAULT_PROBABILITY: f32 = 0.01;

    let storage_cplane = StorageControlPlane::fault_tolerant(REDUNDANCY);
    let mut compute_cplane = ComputeControlPlane::local(&storage_cplane);
    let wal_acceptors = storage_cplane.get_wal_acceptor_conn_info();
    let mut rng = rand::thread_rng();

    // start postgre
    let node = compute_cplane.new_master_node();
    node.start(&storage_cplane);

    // start proxy
    let _proxy = node.start_proxy(wal_acceptors);
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
                storage_cplane.wal_acceptors[node].stop();
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

fn start_acceptor(cplane: &Arc<StorageControlPlane>, no: usize) {
    let cp = cplane.clone();
    thread::spawn(move || {
        thread::sleep(time::Duration::from_secs(1));
        cp.wal_acceptors[no].start();
    });
}

// Stop majority of acceptors while compute is under the load. Boot
// them again and check that nothing was losed. Repeat.
// N_CRASHES env var
#[test]
fn test_acceptors_unavalability() {
    // Start pageserver that reads WAL directly from that postgres
    const REDUNDANCY: usize = 2;

    let storage_cplane = StorageControlPlane::fault_tolerant(REDUNDANCY);
    let mut compute_cplane = ComputeControlPlane::local(&storage_cplane);
    let wal_acceptors = storage_cplane.get_wal_acceptor_conn_info();

    // start postgre
    let node = compute_cplane.new_master_node();
    node.start(&storage_cplane);

    // start proxy
    let _proxy = node.start_proxy(wal_acceptors);

    // check basic work with table
    node.safe_psql(
        "postgres",
        "CREATE TABLE t(key int primary key, value text)",
    );
    let mut psql = node.open_psql("postgres");
    psql.execute("INSERT INTO t values (1, 'payload')", &[])
        .unwrap();

    storage_cplane.wal_acceptors[0].stop();
    let ap = Arc::new(storage_cplane);
    start_acceptor(&ap, 0);
    let now = SystemTime::now();
    psql.execute("INSERT INTO t values (2, 'payload')", &[])
        .unwrap();
    assert!(now.elapsed().unwrap().as_secs() > 1);
    psql.execute("INSERT INTO t values (3, 'payload')", &[])
        .unwrap();

    ap.wal_acceptors[1].stop();
    start_acceptor(&ap, 1);
    psql.execute("INSERT INTO t values (4, 'payload')", &[])
        .unwrap();
    assert!(now.elapsed().unwrap().as_secs() > 2);

    psql.execute("INSERT INTO t values (5, 'payload')", &[])
        .unwrap();

    let count: i64 = node
        .safe_psql("postgres", "SELECT sum(key) FROM t")
        .first()
        .unwrap()
        .get(0);
    println!("sum = {}", count);
    assert_eq!(count, 15);
}

fn simulate_failures(cplane: &Arc<StorageControlPlane>) {
    let mut rng = rand::thread_rng();
    let n_acceptors = cplane.wal_acceptors.len();
    let failure_period = time::Duration::from_secs(1);
    loop {
        thread::sleep(failure_period);
        let mask: u32 = rng.gen_range(0..(1 << n_acceptors));
        for i in 0..n_acceptors {
            if (mask & (1 << i)) != 0 {
                cplane.wal_acceptors[i].stop();
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
    // Start pageserver that reads WAL directly from that postgres
    const REDUNDANCY: usize = 3;

    let storage_cplane = StorageControlPlane::fault_tolerant(REDUNDANCY);
    let mut compute_cplane = ComputeControlPlane::local(&storage_cplane);
    let wal_acceptors = storage_cplane.get_wal_acceptor_conn_info();

    // start postgre
    let node = compute_cplane.new_master_node();
    node.start(&storage_cplane);

    // start proxy
    let _proxy = node.start_proxy(wal_acceptors);

    // check basic work with table
    node.safe_psql(
        "postgres",
        "CREATE TABLE t(key int primary key, value text)",
    );
    let cp = Arc::new(storage_cplane);
    thread::spawn(move || {
        simulate_failures(&cp);
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
}
