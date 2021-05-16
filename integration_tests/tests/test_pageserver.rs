use control_plane::compute::ComputeControlPlane;

use integration_tests;
use integration_tests::TestStorageControlPlane;
use integration_tests::PostgresNodeExt;

// XXX: force all redo at the end
// -- restart + seqscan won't read deleted stuff
// -- pageserver api endpoint to check all rels
#[test]
fn test_redo_cases() {
    let local_env = integration_tests::create_test_env("test_redo_cases");

    // Start pageserver that reads WAL directly from that postgres
    let storage_cplane = TestStorageControlPlane::one_page_server(&local_env);
    let mut compute_cplane = ComputeControlPlane::local(&local_env, &storage_cplane.pageserver);

    // start postgres
    let node = compute_cplane.new_test_node("main");
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
fn test_regress() {
    let local_env = integration_tests::create_test_env("test_regress");

    // Start pageserver that reads WAL directly from that postgres
    let storage_cplane = TestStorageControlPlane::one_page_server(&local_env);
    let mut compute_cplane = ComputeControlPlane::local(&local_env, &storage_cplane.pageserver);

    // start postgres
    let node = compute_cplane.new_test_node("main");
    node.start().unwrap();

    let status = node.pg_regress();
    assert!(status.success());
}

// Runs pg_bench on a compute node
#[test]
fn pgbench() {
    let local_env = integration_tests::create_test_env("pgbench");

    // Start pageserver that reads WAL directly from that postgres
    let storage_cplane = TestStorageControlPlane::one_page_server(&local_env);
    let mut compute_cplane = ComputeControlPlane::local(&local_env, &storage_cplane.pageserver);

    // start postgres
    let node = compute_cplane.new_test_node("main");
    node.start().unwrap();

    let status = node.pg_bench(10, 5);
    assert!(status.success());
}

// Run two postgres instances on one pageserver, on different timelines
#[test]
fn test_pageserver_two_timelines() {
    let local_env = integration_tests::create_test_env("test_pageserver_two_timelines");

    // Start pageserver that reads WAL directly from that postgres
    let storage_cplane = TestStorageControlPlane::one_page_server(&local_env);
    let mut compute_cplane = ComputeControlPlane::local(&local_env, &storage_cplane.pageserver);

    // Create new branch at the end of 'main'
    storage_cplane
        .pageserver
        .branch_create("experimental", "main")
        .unwrap();

    // Launch postgres instances on both branches
    let node1 = compute_cplane.new_test_node("main");
    let node2 = compute_cplane.new_test_node("experimental");
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
