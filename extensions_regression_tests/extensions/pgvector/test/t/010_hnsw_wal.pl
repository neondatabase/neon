# Based on postgres/contrib/bloom/t/001_wal.pl

# Test generic xlog record work for hnsw index replication.
use strict;
use warnings;
use PostgresNode;
use TestLib;
use Test::More;

my $dim = 32;

my $node_primary;
my $node_replica;

# Run few queries on both primary and replica and check their results match.
sub test_index_replay
{
	my ($test_name) = @_;

	# Wait for replica to catch up
	my $applname = $node_replica->name;
	my $caughtup_query = "SELECT pg_current_wal_lsn() <= replay_lsn FROM pg_stat_replication WHERE application_name = '$applname';";
	$node_primary->poll_query_until('postgres', $caughtup_query)
	  or die "Timed out while waiting for replica 1 to catch up";

	my @r = ();
	for (1 .. $dim)
	{
		push(@r, rand());
	}
	my $sql = join(",", @r);

	my $queries = qq(
		SET enable_seqscan = off;
		SELECT * FROM tst ORDER BY v <-> '[$sql]' LIMIT 10;
	);

	# Run test queries and compare their result
	my $primary_result = $node_primary->safe_psql("postgres", $queries);
	my $replica_result = $node_replica->safe_psql("postgres", $queries);

	is($primary_result, $replica_result, "$test_name: query result matches");
	return;
}

# Use ARRAY[random(), random(), random(), ...] over
# SELECT array_agg(random()) FROM generate_series(1, $dim)
# to generate different values for each row
my $array_sql = join(",", ('random()') x $dim);

# Initialize primary node
$node_primary = get_new_node('primary');
$node_primary->init(allows_streaming => 1);
if ($dim > 32)
{
	# TODO use wal_keep_segments for Postgres < 13
	$node_primary->append_conf('postgresql.conf', qq(wal_keep_size = 1GB));
}
if ($dim > 1500)
{
	$node_primary->append_conf('postgresql.conf', qq(maintenance_work_mem = 128MB));
}
$node_primary->start;
my $backup_name = 'my_backup';

# Take backup
$node_primary->backup($backup_name);

# Create streaming replica linking to primary
$node_replica = get_new_node('replica');
$node_replica->init_from_backup($node_primary, $backup_name, has_streaming => 1);
$node_replica->start;

# Create hnsw index on primary
$node_primary->safe_psql("postgres", "CREATE EXTENSION vector;");
$node_primary->safe_psql("postgres", "CREATE TABLE tst (i int4, v vector($dim));");
$node_primary->safe_psql("postgres",
	"INSERT INTO tst SELECT i % 10, ARRAY[$array_sql] FROM generate_series(1, 1000) i;"
);
$node_primary->safe_psql("postgres", "CREATE INDEX ON tst USING hnsw (v vector_l2_ops);");

# Test that queries give same result
test_index_replay('initial');

# Run 10 cycles of table modification. Run test queries after each modification.
for my $i (1 .. 10)
{
	$node_primary->safe_psql("postgres", "DELETE FROM tst WHERE i = $i;");
	test_index_replay("delete $i");
	$node_primary->safe_psql("postgres", "VACUUM tst;");
	test_index_replay("vacuum $i");
	my ($start, $end) = (1001 + ($i - 1) * 100, 1000 + $i * 100);
	$node_primary->safe_psql("postgres",
		"INSERT INTO tst SELECT i % 10, ARRAY[$array_sql] FROM generate_series($start, $end) i;"
	);
	test_index_replay("insert $i");
}

done_testing();
