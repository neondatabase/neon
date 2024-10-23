SELECT setting::int AS max_cluster_size FROM pg_settings WHERE name = 'neon.max_cluster_size';
