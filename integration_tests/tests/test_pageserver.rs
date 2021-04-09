#[allow(dead_code)]
mod control_plane;
use std::thread::sleep;
use std::time::Duration;

use control_plane::ComputeControlPlane;
use control_plane::StorageControlPlane;

// XXX: force all redo at the end
// -- restart + seqscan won't read deleted stuff
// -- pageserver api endpoint to check all rels

//Handcrafted cases with wal records that are (were) problematic for redo.
#[test]
#[ignore]
fn test_redo_cases() {
    // Start pageserver that reads WAL directly from that postgres
    let storage_cplane = StorageControlPlane::one_page_server(false);
    let mut compute_cplane = ComputeControlPlane::local(&storage_cplane);

    // start postgres
    let node = compute_cplane.new_node();
    node.start(&storage_cplane);

    println!("await pageserver connection...");
    sleep(Duration::from_secs(3));

    // check basic work with table
    node.safe_psql(
        "postgres",
        "CREATE TABLE t(key int primary key, value text)",
    );
    node.safe_psql(
        "postgres",
        "INSERT INTO t SELECT generate_series(1,100), 'payload'",
    );
    let count: i64 = node
        .safe_psql("postgres", "SELECT sum(key) FROM t")
        .first()
        .unwrap()
        .get(0);
    println!("sum = {}", count);
    assert_eq!(count, 5050);

    //check 'create table as'
    node.safe_psql("postgres", "CREATE TABLE t2 AS SELECT * FROM t");
    let count: i64 = node
        .safe_psql("postgres", "SELECT sum(key) FROM t")
        .first()
        .unwrap()
        .get(0);
    println!("sum = {}", count);
    assert_eq!(count, 5050);
}

// Runs pg_regress on a compute node
#[test]
#[ignore]
fn test_regress() {
    // Start pageserver that reads WAL directly from that postgres
    let storage_cplane = StorageControlPlane::one_page_server(false);
    let mut compute_cplane = ComputeControlPlane::local(&storage_cplane);

    // start postgres
    let node = compute_cplane.new_node();
    node.start(&storage_cplane);

    println!("await pageserver connection...");
    sleep(Duration::from_secs(3));

    control_plane::regress_check(&node);
}

// Run two postgres instances on one pageserver
#[test]
#[ignore]
fn test_pageserver_multitenancy() {
    // Start pageserver that reads WAL directly from that postgres
    let storage_cplane = StorageControlPlane::one_page_server(false);
    let mut compute_cplane = ComputeControlPlane::local(&storage_cplane);

    // Allocate postgres instance, but don't start
    let node1 = compute_cplane.new_node();
    let node2 = compute_cplane.new_node();
    node1.start(&storage_cplane);
    node2.start(&storage_cplane);

    // XXX: add some extension func to postgres to check walsender conn
    // XXX: or better just drop that
    println!("await pageserver connection...");
    sleep(Duration::from_secs(3));

    // check node1
    node1.safe_psql(
        "postgres",
        "CREATE TABLE t(key int primary key, value text)",
    );
    node1.safe_psql(
        "postgres",
        "INSERT INTO t SELECT generate_series(1,100), 'payload'",
    );
    let count: i64 = node1
        .safe_psql("postgres", "SELECT sum(key) FROM t")
        .first()
        .unwrap()
        .get(0);
    println!("sum = {}", count);
    assert_eq!(count, 5050);

    // check node2
    node2.safe_psql(
        "postgres",
        "CREATE TABLE t(key int primary key, value text)",
    );
    node2.safe_psql(
        "postgres",
        "INSERT INTO t SELECT generate_series(100,200), 'payload'",
    );
    let count: i64 = node2
        .safe_psql("postgres", "SELECT sum(key) FROM t")
        .first()
        .unwrap()
        .get(0);
    println!("sum = {}", count);
    assert_eq!(count, 15150);
}

#[test]
#[ignore]
// Start pageserver using s3 base image
//
// Requires working minio with hardcoded setup:
// .env("S3_ENDPOINT", "https://127.0.0.1:9000")
// .env("S3_REGION", "us-east-1")
// .env("S3_ACCESSKEY", "minioadmin")
// .env("S3_SECRET", "minioadmin")
// .env("S3_BUCKET", "zenith-testbucket")
// TODO use env variables in test
fn test_pageserver_recovery() {
    //This test expects that image is already uploaded to s3
    //To upload it use zenith_push before test (see node.push_to_s3() for details)
    let storage_cplane = StorageControlPlane::one_page_server(true);
    let mut compute_cplane = ComputeControlPlane::local(&storage_cplane);

    //Wait while daemon uploads pages from s3
    sleep(Duration::from_secs(15));

    let node_restored = compute_cplane.new_node_wo_data();

    //TODO 6947041219207877724 is a hardcoded sysid for my cluster. Get it somewhere
    node_restored.setup_compute_node(6947041219207877724, &storage_cplane);

    node_restored.start(&storage_cplane);

    let rows = node_restored.safe_psql("postgres", "SELECT relname from pg_class;");

    assert_eq!(rows.len(), 395);
}

#[test]
#[ignore]
//Scenario for future test. Not implemented yet
fn test_pageserver_node_switch() {
    //Create pageserver
    let storage_cplane = StorageControlPlane::one_page_server(false);
    let mut compute_cplane = ComputeControlPlane::local(&storage_cplane);

    //Create reqular node
    let node = compute_cplane.new_node();
    node.start(&storage_cplane);

    node.safe_psql(
        "postgres",
        "CREATE TABLE t(key int primary key, value text)",
    );
    node.safe_psql(
        "postgres",
        "INSERT INTO t SELECT generate_series(1,100), 'payload'",
    );
    let count: i64 = node
        .safe_psql("postgres", "SELECT sum(key) FROM t")
        .first()
        .unwrap()
        .get(0);
    println!("sum = {}", count);
    assert_eq!(count, 5050);

    //Push all node files to s3
    //TODO upload them directly to pageserver
    node.push_to_s3();
    //Upload data from s3 to pageserver
    //storage_cplane.upload_from_s3() //Not implemented yet

    //Shut down the node
    node.stop();

    //Create new node without files
    let node_restored = compute_cplane.new_node_wo_data();

    // Setup minimal set of files needed to start node and setup pageserver connection
    // TODO 6947041219207877724 is a hardcoded sysid. Get it from node
    node_restored.setup_compute_node(6947041219207877724, &storage_cplane);

    //Start compute node without files
    node_restored.start(&storage_cplane);

    //Ensure that is has table created on initial node
    let rows = node_restored.safe_psql("postgres", "SELECT key from t;");
    assert_eq!(rows.len(), 5050);
}
