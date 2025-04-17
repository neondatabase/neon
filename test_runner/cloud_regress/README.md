# How to run the `pg_regress` tests on a cloud Neon instance.

* Create a Neon project on staging.
* Grant the superuser privileges to the DB user.
* (Optional) create a branch for testing
* Add the following settings to the `pg_settings` section of the default endpoint configuration for the project using the admin interface:
  * `Timeone`: `America/Los_Angeles`
  * `DateStyle`: `Postgres,MDY`
  * `compute_query_id`: `off`
* Add the following section to the project configuration:
```json
"preload_libraries": {
    "use_defaults": false,
    "enabled_libraries": []
  }
```
* Checkout the actual `Neon` sources
* Patch the sql and expected files for the specific PostgreSQL version, e.g. for v17:
```bash
$ cd vendor/postgres-v17
$ patch -p1 <../../compute/patches/cloud_regress_pg17.patch
```
* Set the environment variables (please modify according your configuration):
```bash
$ export DEFAULT_PG_VERSION=17
$ export BUILD_TYPE=release
```
* Build the Neon binaries see [README.md](../../README.md)
* Set the environment variable `BENCHMARK_CONNSTR` to the connection URI of your project.
* Update poetry, run
```bash
$ scripts/pysync
```
* Run 
```bash
$ scripts/pytest -m remote_cluster -k cloud_regress
```