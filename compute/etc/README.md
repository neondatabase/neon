# Compute Configuration

These files are the configuration files for various other pieces of software
that will be running in the compute alongside Postgres.

## `sql_exporter`

### Adding a `sql_exporter` Metric

We use `sql_exporter` to export various metrics from Postgres. In order to add
a metric, you will need to create two files: a `libsonnet` and a `sql` file. You
will then import the `libsonnet` file in one of the collector files, and the
`sql` file will be imported in the `libsonnet` file.

In the event your statistic is an LSN, you may want to cast it to a `float8`
because Prometheus only supports floats. It's probably fine because `float8` can
store integers from `-2^53` to `+2^53` exactly.
