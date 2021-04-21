// mod control_plane;
use control_plane::compute::ComputeControlPlane;
use control_plane::local_env;
use control_plane::local_env::PointInTime;
use control_plane::storage::TestStorageControlPlane;

// XXX: force all redo at the end
// -- restart + seqscan won't read deleted stuff
// -- pageserver api endpoint to check all rels
/*
#[test]
fn test_redo_cases() {
    let local_env = local_env::test_env("test_redo_cases");

    // Start pageserver that reads WAL directly from that postgres
    let storage_cplane = TestStorageControlPlane::one_page_server(&local_env);
    let mut compute_cplane = ComputeControlPlane::local(&local_env, &storage_cplane.pageserver);

    // start postgres
    let maintli = storage_cplane.get_branch_timeline("main");
    let node = compute_cplane.new_test_node(maintli);
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
*/
// Runs pg_regress on a compute node
#[test]
fn test_regress() {
    let local_env = local_env::test_env("test_regress");

    // Start pageserver that reads WAL directly from that postgres
    let storage_cplane = TestStorageControlPlane::one_page_server(&local_env);
    let mut compute_cplane = ComputeControlPlane::local(&local_env, &storage_cplane.pageserver);

    // start postgres
    let maintli = storage_cplane.get_branch_timeline("main");
    let node = compute_cplane.new_test_node(maintli);
    node.start().unwrap();

    node.pg_regress();
}

// Runs pg_bench on a compute node
#[test]
fn pgbench() {
    // Start pageserver that reads WAL directly from that postgres
    let storage_cplane = TestStorageControlPlane::one_page_server(String::new());
    let mut compute_cplane = ComputeControlPlane::local(&storage_cplane.pageserver);

    // start postgres
    let node = compute_cplane.new_test_node();
    node.start().unwrap();

    node.pg_bench(10, 100);
}

// Run two postgres instances on one pageserver, on different timelines
#[test]
fn test_pageserver_two_timelines() {
    let local_env = local_env::test_env("test_pageserver_two_timelines");

    // Start pageserver that reads WAL directly from that postgres
    let storage_cplane = TestStorageControlPlane::one_page_server(&local_env);
    let mut compute_cplane = ComputeControlPlane::local(&local_env, &storage_cplane.pageserver);

    let maintli = storage_cplane.get_branch_timeline("main");

    // Create new branch at the end of 'main'
    let startpoint = local_env::find_end_of_wal(&local_env, maintli).unwrap();
    local_env::create_branch(
        &local_env,
        "experimental",
        PointInTime {
            timelineid: maintli,
            lsn: startpoint,
        },
    )
    .unwrap();
    let experimentaltli = storage_cplane.get_branch_timeline("experimental");

    // Launch postgres instances on both branches
    let node1 = compute_cplane.new_test_node(maintli);
    let node2 = compute_cplane.new_test_node(experimentaltli);
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
