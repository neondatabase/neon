{
  collector_name: 'neon_collector_autoscaling',
  metrics: [
    import 'sql_exporter/lfc_approximate_working_set_size_windows.autoscaling.libsonnet',
    import 'sql_exporter/lfc_cache_size_limit.libsonnet',
    import 'sql_exporter/lfc_hits.libsonnet',
    import 'sql_exporter/lfc_misses.libsonnet',
    import 'sql_exporter/lfc_used.libsonnet',
    import 'sql_exporter/lfc_writes.libsonnet',
  ],
}
