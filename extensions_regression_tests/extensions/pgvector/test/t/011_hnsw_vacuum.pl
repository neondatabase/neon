use strict;
use warnings;
use PostgresNode;
use TestLib;
use Test::More;

my $dim = 3;

my @r = ();
for (1 .. $dim)
{
	my $v = int(rand(1000)) + 1;
	push(@r, "i % $v");
}
my $array_sql = join(", ", @r);

# Initialize node
my $node = get_new_node('node');
$node->init;
$node->start;

# Create table and index
$node->safe_psql("postgres", "CREATE EXTENSION vector;");
$node->safe_psql("postgres", "CREATE TABLE tst (i int4, v vector($dim));");
$node->safe_psql("postgres",
	"INSERT INTO tst SELECT i, ARRAY[$array_sql] FROM generate_series(1, 10000) i;"
);
$node->safe_psql("postgres", "CREATE INDEX ON tst USING hnsw (v vector_l2_ops);");

# Get size
my $size = $node->safe_psql("postgres", "SELECT pg_total_relation_size('tst_v_idx');");

# Delete all, vacuum, and insert same data
$node->safe_psql("postgres", "DELETE FROM tst;");
$node->safe_psql("postgres", "VACUUM tst;");
$node->safe_psql("postgres",
	"INSERT INTO tst SELECT i, ARRAY[$array_sql] FROM generate_series(1, 10000) i;"
);

# Check size
# May increase some due to different levels
my $new_size = $node->safe_psql("postgres", "SELECT pg_total_relation_size('tst_v_idx');");
cmp_ok($new_size, "<=", $size * 1.02, "size does not increase too much");

# Delete all but one
$node->safe_psql("postgres", "DELETE FROM tst WHERE i != 123;");
$node->safe_psql("postgres", "VACUUM tst;");
my $res = $node->safe_psql("postgres", qq(
	SET enable_seqscan = off;
	SELECT i FROM tst ORDER BY v <-> '[0,0,0]' LIMIT 10;
));
is($res, 123);

done_testing();
