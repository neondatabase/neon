# PostGIS Testing in Neon

This directory contains configuration files and patches for running PostGIS tests in the Neon database environment.

## Overview

PostGIS is a spatial database extension for PostgreSQL that adds support for geographic objects. Testing PostGIS compatibility ensures that Neon's modifications to PostgreSQL don't break compatibility with this critical extension.

## PostGIS Versions

- PostgreSQL v17: PostGIS 3.5.0
- PostgreSQL v14/v15/v16: PostGIS 3.3.3

## Test Configuration

The test setup includes:

- `postgis-no-upgrade-test.patch`: Disables upgrade tests by removing the upgrade test section from regress/runtest.mk
- `postgis-regular-v16.patch`: Version-specific patch for PostgreSQL v16
- `postgis-regular-v17.patch`: Version-specific patch for PostgreSQL v17
- `regular-test.sh`: Script to run PostGIS tests as a regular user
- `neon-test.sh`: Script to handle version-specific test configurations
- `raster_outdb_template.sql`: Template for raster tests with explicit file paths

## Excluded Tests

**Important Note:** The test exclusions listed below are specifically for regular-user tests against staging instances. These exclusions are necessary because staging instances run with limited privileges and cannot perform operations requiring superuser access. Docker-compose based tests are not affected by these exclusions.

### Tests Requiring Superuser Permissions

These tests cannot be run as a regular user:
- `estimatedextent`
- `regress/core/legacy`
- `regress/core/typmod`
- `regress/loader/TestSkipANALYZE`
- `regress/loader/TestANALYZE`

### Tests Requiring Filesystem Access

These tests need direct filesystem access that is only possible for superusers:
- `loader/load_outdb`

### Tests with Flaky Results

These tests have assumptions that don't always hold true:
- `regress/core/computed_columns` - Assumes computed columns always outperform alternatives, which is not consistently true

### Tests Requiring Tunable Parameter Modifications

These tests attempt to modify the `postgis.gdal_enabled_drivers` parameter, which is only accessible to superusers:
- `raster/test/regress/rt_wkb`
- `raster/test/regress/rt_addband`
- `raster/test/regress/rt_setbandpath`
- `raster/test/regress/rt_fromgdalraster`
- `raster/test/regress/rt_asgdalraster`
- `raster/test/regress/rt_astiff`
- `raster/test/regress/rt_asjpeg`
- `raster/test/regress/rt_aspng`
- `raster/test/regress/permitted_gdal_drivers`
- Loader tests: `BasicOutDB`, `Tiled10x10`, `Tiled10x10Copy`, `Tiled8x8`, `TiledAuto`, `TiledAutoSkipNoData`, `TiledAutoCopyn`

### Topology Tests (v17 only)
- `populate_topology_layer`
- `renametopogeometrycolumn`

## Other Modifications

- Binary.sql tests are modified to use explicit file paths
- Server-side SQL COPY commands (which require superuser privileges) are converted to client-side `\copy` commands
- Upgrade tests are disabled
