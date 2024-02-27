use strict;
use warnings;
use PostgresNode;
use TestLib;
use Test::More;

my $dim = 3;

my $array_sql = join(",", ('random()') x $dim);

# Initialize node
my $node = get_new_node('node');
$node->init;
$node->start;

# Create table and index
$node->safe_psql("postgres", "CREATE EXTENSION vector;");
$node->safe_psql("postgres", "CREATE TABLE tst (i serial, v vector($dim));");
$node->safe_psql("postgres",
	"INSERT INTO tst (v) SELECT ARRAY[$array_sql] FROM generate_series(1, 10000) i;"
);
$node->safe_psql("postgres", "CREATE INDEX ON tst USING ivfflat (v vector_l2_ops);");

# Delete data
$node->safe_psql("postgres", "DELETE FROM tst WHERE i % 100 != 0;");

my $exp = $node->safe_psql("postgres", qq(
	SET enable_indexscan = off;
	SELECT i FROM tst ORDER BY v <-> '[0,0,0]';
));

# Run twice to make sure correct tuples marked as dead
for (1 .. 2)
{
	my $res = $node->safe_psql("postgres", qq(
		SET enable_seqscan = off;
		SET ivfflat.probes = 100;
		SELECT i FROM tst ORDER BY v <-> '[0,0,0]';
	));
	is($res, $exp);
}

done_testing();
