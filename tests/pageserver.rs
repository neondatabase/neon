// XXX: force all redo at the end
// -- restart + seqscan won'r read deleted stuff
// -- pageserver api endpoint to check all rels

// Handcrafted cases with wal records that are (were) problematic for redo.
#[test]
fn test_redo_cases() {}

// Runs pg_regress on a compute node
#[test]
fn test_regress() {}

// Runs pg_regress on a compute node
#[test]
fn test_pageserver_recovery() {}
