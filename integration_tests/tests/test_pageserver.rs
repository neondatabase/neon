#[allow(dead_code)]
mod control_plane;

use control_plane::ComputeControlPlane;
use control_plane::StorageControlPlane;

// XXX: force all redo at the end
// -- restart + seqscan won't read deleted stuff
// -- pageserver api endpoint to check all rels

// Handcrafted cases with wal records that are (were) problematic for redo.
// #[test]
// fn test_redo_cases() {
//     // Start pageserver that reads WAL directly from that postgres
//     let storage_cplane = StorageControlPlane::one_page_server(false);
//     let mut compute_cplane = ComputeControlPlane::local(&storage_cplane);

//     // start postgres
//     let node = compute_cplane.new_node();
//     node.start(&storage_cplane);

//     println!("await pageserver connection...");
//     sleep(Duration::from_secs(3));

//     // check basic work with table
//     node.safe_psql("postgres", "CREATE TABLE t(key int primary key, value text)");
//     node.safe_psql("postgres", "INSERT INTO t SELECT generate_series(1,100000), 'payload'");
//     let count: i64 = node
//         .safe_psql("postgres", "SELECT sum(key) FROM t")
//         .first()
//         .unwrap()
//         .get(0);
//     println!("sum = {}", count);
//     assert_eq!(count, 5000050000);

//     // check 'create table as'
//     node.safe_psql("postgres", "CREATE TABLE t2 AS SELECT * FROM t");
//     let count: i64 = node
//         .safe_psql("postgres", "SELECT sum(key) FROM t")
//         .first()
//         .unwrap()
//         .get(0);
//     println!("sum = {}", count);
//     assert_eq!(count, 5000050000);
// }

// // Runs pg_regress on a compute node
// #[test]
// fn test_regress() {
//     // Start pageserver that reads WAL directly from that postgres
//     let storage_cplane = StorageControlPlane::one_page_server(false);
//     let mut compute_cplane = ComputeControlPlane::local(&storage_cplane);

//     // start postgres
//     let node = compute_cplane.new_node();
//     node.start(&storage_cplane);

//     println!("await pageserver connection...");
//     sleep(Duration::from_secs(3));

//     pageserver::control_plane::regress_check(&node);
// }

// // Run two postgres instances on one pageserver
// #[test]
// fn test_pageserver_multitenancy() {
//     // Start pageserver that reads WAL directly from that postgres
//     let storage_cplane = StorageControlPlane::one_page_server(false);
//     let mut compute_cplane = ComputeControlPlane::local(&storage_cplane);

//     // Allocate postgres instance, but don't start
//     let node1 = compute_cplane.new_node();
//     let node2 = compute_cplane.new_node();
//     node1.start(&storage_cplane);
//     node2.start(&storage_cplane);

//     // XXX: add some extension func to postgres to check walsender conn
//     // XXX: or better just drop that
//     println!("await pageserver connection...");
//     sleep(Duration::from_secs(3));

//     // check node1
//     node1.safe_psql("postgres", "CREATE TABLE t(key int primary key, value text)");
//     node1.safe_psql("postgres", "INSERT INTO t SELECT generate_series(1,100000), 'payload'");
//     let count: i64 = node1
//         .safe_psql("postgres", "SELECT sum(key) FROM t")
//         .first()
//         .unwrap()
//         .get(0);
//     println!("sum = {}", count);
//     assert_eq!(count, 5000050000);

//     // check node2
//     node2.safe_psql("postgres", "CREATE TABLE t(key int primary key, value text)");
//     node2.safe_psql("postgres", "INSERT INTO t SELECT generate_series(100000,200000), 'payload'");
//     let count: i64 = node2
//         .safe_psql("postgres", "SELECT sum(key) FROM t")
//         .first()
//         .unwrap()
//         .get(0);
//     println!("sum = {}", count);
//     assert_eq!(count, 15000150000);
// }

#[test]
// Start pageserver using s3 base image
fn test_pageserver_recovery() {

    let storage_cplane = StorageControlPlane::one_page_server(true);

<<<<<<< HEAD:integration_tests/tests/test_pageserver.rs
    control_plane::regress_check(&node);
}

// Run two postgres instances on one pageserver
#[test]
fn test_pageserver_multitenancy() {
    // Start pageserver that reads WAL directly from that postgres
    let storage_cplane = StorageControlPlane::one_page_server();
    let mut compute_cplane = ComputeControlPlane::local(&storage_cplane);

    // Allocate postgres instance, but don't start
    let node1 = compute_cplane.new_node();
    let node2 = compute_cplane.new_node();
    node1.start(&storage_cplane);
    node2.start(&storage_cplane);

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
=======
    sleep(Duration::from_secs(30));


    //Request page from pageserver
    storage_cplane.page_server_psql("controlfile");
>>>>>>> add test test_pageserver_recovery.:tests/test_pageserver.rs
}
