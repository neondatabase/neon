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
$node->safe_psql("postgres", "CREATE TABLE tst (v vector(3));");
$node->safe_psql("postgres",
	"INSERT INTO tst SELECT ARRAY[random(), random(), random()] FROM generate_series(1, 100000) i;"
);

$node->safe_psql("postgres", "CREATE INDEX lists50 ON tst USING ivfflat (v vector_l2_ops) WITH (lists = 50);");
$node->safe_psql("postgres", "CREATE INDEX lists100 ON tst USING ivfflat (v vector_l2_ops) WITH (lists = 100);");

# Test prefers more lists
my $res = $node->safe_psql("postgres", "EXPLAIN SELECT v FROM tst ORDER BY v <-> '[0.5,0.5,0.5]' LIMIT 10;");
like($res, qr/lists100/);
unlike($res, qr/lists50/);

# Test errors with too much memory
my ($ret, $stdout, $stderr) = $node->psql("postgres",
	"CREATE INDEX lists10000 ON tst USING ivfflat (v vector_l2_ops) WITH (lists = 10000);"
);
like($stderr, qr/memory required is/);

done_testing();
