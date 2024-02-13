use strict;
use warnings;
use PostgresNode;
use TestLib;
use Test::More;

my $node;
my @queries = ();
my @expected;
my $limit = 20;

sub test_recall
{
	my ($min, $ef_search, $test_name) = @_;
	my $correct = 0;
	my $total = 0;

	my $explain = $node->safe_psql("postgres", qq(
		SET enable_seqscan = off;
		SET hnsw.ef_search = $ef_search;
		EXPLAIN ANALYZE SELECT i FROM tst ORDER BY v <-> '$queries[0]' LIMIT $limit;
	));
	like($explain, qr/Index Scan/);

	for my $i (0 .. $#queries)
	{
		my $actual = $node->safe_psql("postgres", qq(
			SET enable_seqscan = off;
			SET hnsw.ef_search = $ef_search;
			SELECT i FROM tst ORDER BY v <-> '$queries[$i]' LIMIT $limit;
		));
		my @actual_ids = split("\n", $actual);
		my %actual_set = map { $_ => 1 } @actual_ids;

		my @expected_ids = split("\n", $expected[$i]);

		foreach (@expected_ids)
		{
			if (exists($actual_set{$_}))
			{
				$correct++;
			}
			$total++;
		}
	}

	cmp_ok($correct / $total, ">=", $min, $test_name);
}

# Initialize node
$node = get_new_node('node');
$node->init;
$node->start;

# Create table
$node->safe_psql("postgres", "CREATE EXTENSION vector;");
$node->safe_psql("postgres", "CREATE TABLE tst (i int4, v vector(3));");
$node->safe_psql("postgres", "ALTER TABLE tst SET (autovacuum_enabled = false);");
$node->safe_psql("postgres",
	"INSERT INTO tst SELECT i, ARRAY[random(), random(), random()] FROM generate_series(1, 10000) i;"
);

# Add index
$node->safe_psql("postgres", "CREATE INDEX ON tst USING hnsw (v vector_l2_ops) WITH (m = 4, ef_construction = 8);");

# Delete data
$node->safe_psql("postgres", "DELETE FROM tst WHERE i > 2500;");

# Generate queries
for (1 .. 20)
{
	my $r1 = rand();
	my $r2 = rand();
	my $r3 = rand();
	push(@queries, "[$r1,$r2,$r3]");
}

# Get exact results
@expected = ();
foreach (@queries)
{
	my $res = $node->safe_psql("postgres", qq(
		SET enable_indexscan = off;
		SELECT i FROM tst ORDER BY v <-> '$_' LIMIT $limit;
	));
	push(@expected, $res);
}

test_recall(0.20, $limit, "before vacuum");
test_recall(0.95, 100, "before vacuum");

# TODO Test concurrent inserts with vacuum
$node->safe_psql("postgres", "VACUUM tst;");

test_recall(0.95, $limit, "after vacuum");

done_testing();
