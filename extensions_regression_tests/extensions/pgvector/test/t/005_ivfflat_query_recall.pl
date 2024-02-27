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
$node->safe_psql("postgres", "CREATE TABLE tst (i int4 primary key, v vector(3));");
$node->safe_psql("postgres",
	"INSERT INTO tst SELECT i, ARRAY[random(), random(), random()] FROM generate_series(1, 100000) i;"
);

# Check each index type
my @operators = ("<->", "<#>", "<=>");
my @opclasses = ("vector_l2_ops", "vector_ip_ops", "vector_cosine_ops");

for my $i (0 .. $#operators)
{
	my $operator = $operators[$i];
	my $opclass = $opclasses[$i];

	# Add index
	$node->safe_psql("postgres", "CREATE INDEX ON tst USING ivfflat (v $opclass);");

	# Test 100% recall
	for (1 .. 20)
	{
		my $id = int(rand() * 100000);
		my $query = $node->safe_psql("postgres", "SELECT v FROM tst WHERE i = $id;");
		my $res = $node->safe_psql("postgres", qq(
			SET enable_seqscan = off;
			SELECT v FROM tst ORDER BY v <-> '$query' LIMIT 1;
		));
		is($res, $query);
	}
}

done_testing();
