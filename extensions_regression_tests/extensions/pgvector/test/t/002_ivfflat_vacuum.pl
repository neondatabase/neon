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
	"INSERT INTO tst SELECT i % 10, ARRAY[$array_sql] FROM generate_series(1, 100000) i;"
);
$node->safe_psql("postgres", "CREATE INDEX ON tst USING ivfflat (v vector_l2_ops);");

# Get size
my $size = $node->safe_psql("postgres", "SELECT pg_total_relation_size('tst_v_idx');");

# Delete all, vacuum, and insert same data
$node->safe_psql("postgres", "DELETE FROM tst;");
$node->safe_psql("postgres", "VACUUM tst;");
$node->safe_psql("postgres",
	"INSERT INTO tst SELECT i % 10, ARRAY[$array_sql] FROM generate_series(1, 100000) i;"
);

# Check size
my $new_size = $node->safe_psql("postgres", "SELECT pg_total_relation_size('tst_v_idx');");
is($size, $new_size, "size does not change");

done_testing();
