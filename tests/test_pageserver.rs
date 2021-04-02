use std::thread::sleep;
use std::time::Duration;

use pageserver::control_plane::ComputeControlPlane;
use pageserver::control_plane::StorageControlPlane;

// XXX: force all redo at the end
// -- restart + seqscan won't read deleted stuff
// -- pageserver api endpoint to check all rels

// Handcrafted cases with wal records that are (were) problematic for redo.
#[test]
fn test_redo_cases() {
    // Allocate postgres instance, but don't start
    let mut compute_cplane = ComputeControlPlane::local();
    // Create compute node without files, only datadir structure
    let node = compute_cplane.new_minimal_node();

    // Start pageserver that reads WAL directly from that postgres
    let storage_cplane = StorageControlPlane::one_page_server(node.connstr());
    let pageserver_addr = storage_cplane.page_server_addr();

    // Configure that node to take pages from pageserver
    node.append_conf("postgresql.conf", format!("\
        page_server_connstring = 'host={} port={}'\n\
    ", pageserver_addr.ip(), pageserver_addr.port()).as_str());

    // Request info needed to build control file
    storage_cplane.simple_query_storage("postgres", node.whoami().as_str(), "controlfile");
    // Setup controlfile
    node.setup_controlfile();

    // start postgres
    node.start();

    println!("await pageserver connection...");
    sleep(Duration::from_secs(3));

    // check basic work with table
    node.safe_psql("postgres", "CREATE TABLE t(key int primary key, value text)");
    node.safe_psql("postgres", "INSERT INTO t SELECT generate_series(1,100000), 'payload'");
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
    // Allocate postgres instance, but don't start
    let mut compute_cplane = ComputeControlPlane::local();
    let node = compute_cplane.new_vanilla_node();

    // Start pageserver that reads WAL directly from that postgres
    let storage_cplane = StorageControlPlane::one_page_server(node.connstr());
    let pageserver_addr = storage_cplane.page_server_addr();

    // Configure that node to take pages from pageserver
    node.append_conf("postgresql.conf", format!("\
        page_server_connstring = 'host={} port={}'\n\
    ", pageserver_addr.ip(), pageserver_addr.port()).as_str());

    // start postgres
    node.start();

    println!("await pageserver connection...");
    sleep(Duration::from_secs(3));

    //////////////////////////////////////////////////////////////////

    pageserver::control_plane::regress_check(node);
}

// Runs recovery with minio
#[test]
fn test_pageserver_recovery() {}
