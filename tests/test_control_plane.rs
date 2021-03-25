use pageserver::control_plane::ComputeControlPlane;
use pageserver::control_plane::StorageControlPlane;

#[test]
fn test_actions() {
    let mut cplane = ComputeControlPlane::local();
    let node = cplane.new_vanilla_node();

    node.start();
    node.safe_psql("postgres", "CREATE TABLE t(key int primary key, value text)");
    node.safe_psql("postgres", "INSERT INTO t SELECT generate_series(1,100000), 'payload'");
    
    let count: i64 = node
        .safe_psql("postgres", "SELECT count(*) FROM t")
        .first()
        .unwrap()
        .get(0);

    assert_eq!(count, 100000);
}

#[test]
fn test_regress() {
}