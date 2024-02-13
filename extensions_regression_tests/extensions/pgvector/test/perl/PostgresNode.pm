use PostgreSQL::Test::Cluster;

sub get_new_node
{
	return PostgreSQL::Test::Cluster->new(@_);
}

1;
