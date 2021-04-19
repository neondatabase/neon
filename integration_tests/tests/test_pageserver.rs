// mod control_plane;
use control_plane::compute::ComputeControlPlane;
use control_plane::storage::TestStorageControlPlane;

use std::thread::sleep;
use std::time::Duration;

// XXX: force all redo at the end
// -- restart + seqscan won't read deleted stuff
// -- pageserver api endpoint to check all rels

// Handcrafted cases with wal records that are (were) problematic for redo.
#[test]
fn test_redo_cases() {
    // Start pageserver that reads WAL directly from that postgres
    let storage_cplane = TestStorageControlPlane::one_page_server(String::new());
    let mut compute_cplane = ComputeControlPlane::local(&storage_cplane.pageserver);

    // start postgres
    let node = compute_cplane.new_test_node();
    node.start().unwrap();

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

    // check 'create table as'
    node.safe_psql("postgres", "CREATE TABLE t2 AS SELECT * FROM t");
    let count: i64 = node
        .safe_psql("postgres", "SELECT sum(key) FROM t")
        .first()
        .unwrap()
        .get(0);
    println!("sum = {}", count);
    assert_eq!(count, 5000050000);
}

// Runs pg_regress on a compute node
#[test]
#[ignore]
fn test_regress() {
    // Start pageserver that reads WAL directly from that postgres
    let storage_cplane = TestStorageControlPlane::one_page_server(String::new());
    let mut compute_cplane = ComputeControlPlane::local(&storage_cplane.pageserver);

    // start postgres
    let node = compute_cplane.new_test_node();
    node.start().unwrap();

<<<<<<< HEAD
    node.pg_regress();
}

// Runs pg_bench on a compute node
#[test]
fn pgbench() {
    // Start pageserver that reads WAL directly from that postgres
    let storage_cplane = StorageControlPlane::one_page_server();
    let mut compute_cplane = ComputeControlPlane::local(&storage_cplane);

    // start postgres
    let node = compute_cplane.new_node();
    node.start(&storage_cplane);

    node.pg_bench(10, 100);
=======
    control_plane::storage::regress_check(&node);
>>>>>>> main
}

// Run two postgres instances on one pageserver
#[test]
fn test_pageserver_multitenancy() {
    // Start pageserver that reads WAL directly from that postgres
    let storage_cplane = TestStorageControlPlane::one_page_server(String::new());
    let mut compute_cplane = ComputeControlPlane::local(&storage_cplane.pageserver);

    // Allocate postgres instance, but don't start
    let node1 = compute_cplane.new_test_node();
    let node2 = compute_cplane.new_test_node();
    node1.start().unwrap();
    node2.start().unwrap();

    // check node1
    node1.safe_psql(
        "postgres",
        "CREATE TABLE t(key int primary key, value text)",
    );
    node1.safe_psql(
        "postgres",
        "INSERT INTO t SELECT generate_series(1,100000), 'payload'",
    );
    let count: i64 = node1
        .safe_psql("postgres", "SELECT sum(key) FROM t")
        .first()
        .unwrap()
        .get(0);
    println!("sum = {}", count);
    assert_eq!(count, 5000050000);

    // check node2
    node2.safe_psql(
        "postgres",
        "CREATE TABLE t(key int primary key, value text)",
    );
    node2.safe_psql(
        "postgres",
        "INSERT INTO t SELECT generate_series(100000,200000), 'payload'",
    );
    let count: i64 = node2
        .safe_psql("postgres", "SELECT sum(key) FROM t")
        .first()
        .unwrap()
        .get(0);
    println!("sum = {}", count);
    assert_eq!(count, 15000150000);
}

#[test]
fn test_upload_pageserver_local() {
    // Init pageserver that reads WAL directly from that postgres
    // Don't start yet

    let storage_cplane = TestStorageControlPlane::one_page_server_no_start();
    let mut compute_cplane = ComputeControlPlane::local(&storage_cplane.pageserver);

    // init postgres node
    let node = compute_cplane.new_test_node();

    //upload data to pageserver & start it
    &storage_cplane
        .pageserver
        .start_fromdatadir(node.pgdata().to_str().unwrap().to_string())
        .unwrap();

    sleep(Duration::from_secs(10));

    // start postgres node
    node.start().unwrap();

    // check basic work with table
    node.safe_psql(
        "postgres",
        "CREATE TABLE t(key int primary key, value text)",
    );
    node.safe_psql(
        "postgres",
        "INSERT INTO t SELECT generate_series(1,100000), 'payload'",
    );
}
