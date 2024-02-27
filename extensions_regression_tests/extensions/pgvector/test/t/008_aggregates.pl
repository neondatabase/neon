use strict;
use warnings;
use PostgresNode;
use TestLib;
use Test::More;

# Initialize node
my $node = get_new_node('node');
$node->init;
$node->start;

# Create table
$node->safe_psql("postgres", "CREATE EXTENSION vector;");
$node->safe_psql("postgres", "CREATE TABLE tst (r1 real, r2 real, r3 real, v vector(3));");
$node->safe_psql("postgres", qq(
	INSERT INTO tst SELECT r1, r2, r3, ARRAY[r1, r2, r3] FROM (
		SELECT random() + 1.01 AS r1, random() + 2.01 AS r2, random() + 3.01 AS r3 FROM generate_series(1, 1000000) t
	) i;
));

sub test_aggregate
{
	my ($agg) = @_;

	# Test value
	my $res = $node->safe_psql("postgres", "SELECT $agg(v) FROM tst;");
	like($res, qr/\[1\.5/);
	like($res, qr/,2\.5/);
	like($res, qr/,3\.5/);

	# Test matches real for avg
	# Cannot test sum since sum(real) varies between calls
	if ($agg eq 'avg')
	{
		my $r1 = $node->safe_psql("postgres", "SELECT $agg(r1)::float4 FROM tst;");
		my $r2 = $node->safe_psql("postgres", "SELECT $agg(r2)::float4 FROM tst;");
		my $r3 = $node->safe_psql("postgres", "SELECT $agg(r3)::float4 FROM tst;");
		is($res, "[$r1,$r2,$r3]");
	}

	# Test explain
	my $explain = $node->safe_psql("postgres", "EXPLAIN SELECT $agg(v) FROM tst;");
	like($explain, qr/Partial Aggregate/);
}

test_aggregate('avg');
test_aggregate('sum');

done_testing();
