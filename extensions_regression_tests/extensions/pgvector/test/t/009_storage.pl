use strict;
use warnings;
use PostgresNode;
use TestLib;
use Test::More;

my $dim = 1024;

# Initialize node
my $node = get_new_node('node');
$node->init;
$node->start;

# Create table
$node->safe_psql("postgres", "CREATE EXTENSION vector;");
$node->safe_psql("postgres", "CREATE TABLE tst (v1 vector(1024), v2 vector(1024), v3 vector(1024));");

# Test insert succeeds
$node->safe_psql("postgres",
	"INSERT INTO tst SELECT array_agg(n), array_agg(n), array_agg(n) FROM generate_series(1, $dim) n"
);

# Change storage to PLAIN
$node->safe_psql("postgres", "ALTER TABLE tst ALTER COLUMN v1 SET STORAGE PLAIN");
$node->safe_psql("postgres", "ALTER TABLE tst ALTER COLUMN v2 SET STORAGE PLAIN");
$node->safe_psql("postgres", "ALTER TABLE tst ALTER COLUMN v3 SET STORAGE PLAIN");

# Test insert fails
my ($ret, $stdout, $stderr) = $node->psql("postgres",
	"INSERT INTO tst SELECT array_agg(n), array_agg(n), array_agg(n) FROM generate_series(1, $dim) n"
);
like($stderr, qr/row is too big/);

done_testing();
