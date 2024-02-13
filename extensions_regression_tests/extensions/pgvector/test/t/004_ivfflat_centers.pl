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
$node->safe_psql("postgres", "CREATE TABLE tst (i int4, v vector(3));");
$node->safe_psql("postgres",
	"INSERT INTO tst SELECT i, '[1,2,3]' FROM generate_series(1, 10) i;"
);

sub test_centers
{
	my ($lists, $min) = @_;

	my ($ret, $stdout, $stderr) = $node->psql("postgres", "CREATE INDEX ON tst USING ivfflat (v vector_l2_ops) WITH (lists = $lists);");
	is($ret, 0, $stderr);
}

# Test no error for duplicate centers
test_centers(5);
test_centers(10);

$node->safe_psql("postgres",
	"INSERT INTO tst SELECT i, '[4,5,6]' FROM generate_series(1, 10) i;"
);

# Test no error for duplicate centers
test_centers(10);

done_testing();
