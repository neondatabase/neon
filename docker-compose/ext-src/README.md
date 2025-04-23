# PostgreSQL Extensions for Testing

This directory contains PostgreSQL extensions used primarily for:
1. Testing extension upgrades between different Compute versions
2. Running regression tests with regular users (mostly for cloud instances)

## Directory Structure

Each extension directory follows a standard structure:

- `extension-name-src/` - Directory containing test files for the extension
  - `test-upgrade.sh` - Script for testing upgrade scenarios
  - `regular-test.sh` - Script for testing with regular users
  - Additional test files depending on the extension

## Available Extensions

This directory includes the following extensions:

- `hll-src` - HyperLogLog, a fixed-size data structure for approximating cardinality
- `hypopg-src` - Extension to create hypothetical indexes
- `ip4r-src` - IPv4/v6 and subnet data types
- `pg_cron-src` - Run periodic jobs in PostgreSQL
- `pg_graphql-src` - GraphQL support for PostgreSQL
- `pg_hint_plan-src` - Execution plan hints
- `pg_ivm-src` - Incremental view maintenance
- `pg_jsonschema-src` - JSON Schema validation
- `pg_repack-src` - Reorganize tables with minimal locks
- `pg_roaringbitmap-src` - Roaring bitmap implementation
- `pg_semver-src` - Semantic version data type
- `pg_session_jwt-src` - JWT authentication for PostgreSQL
- `pg_tiktoken-src` - OpenAI Tiktoken tokenizer
- `pg_uuidv7-src` - UUIDv7 implementation for PostgreSQL
- `pgjwt-src` - JWT tokens for PostgreSQL
- `pgrag-src` - Retrieval Augmented Generation for PostgreSQL
- `pgtap-src` - Unit testing framework for PostgreSQL
- `pgvector-src` - Vector similarity search
- `pgx_ulid-src` - ULID data type
- `plv8-src` - JavaScript language for PostgreSQL stored procedures
- `postgresql-unit-src` - SI units for PostgreSQL
- `prefix-src` - Prefix matching for strings
- `rag_bge_small_en_v15-src` - BGE embedding model for RAG
- `rag_jina_reranker_v1_tiny_en-src` - Jina reranker model for RAG
- `rum-src` - RUM access method for text search

## Usage

### Extension Upgrade Testing

The extensions in this directory are used by the `test_extensions_upgrade.sh` script to test upgrading extensions between different versions of Neon Compute nodes. The script:

1. Creates a database with extensions installed on an old Compute version
2. Creates timelines for each extension
3. Switches to a new Compute version and tests the upgrade process
4. Verifies extension functionality after upgrade

### Regular User Testing

For testing with regular users (particularly for cloud instances), each extension directory typically contains a `regular-test.sh` script that:

1. Creates a test database
2. Installs the extension
3. Runs regression tests

A note about pg_regress: Since pg_regress attempts to set `lc_messages` for the database by default (which can cause issues), we create databases manually and use the `--use-existing` option to bypass this limitation.

### CI Workflows

Two main workflows use these extensions:

1. **Cloud Extensions Test** - Tests extensions on Neon cloud projects
2. **Force Test Upgrading of Extension** - Tests upgrading extensions between different Compute versions

## Adding New Extensions

To add a new extension for testing:

1. Create a directory named `extension-name-src` in this directory
2. Add at minimum:
   - `test-upgrade.sh` for upgrade testing
   - `regular-test.sh` for regular user testing
3. Update the list of extensions in the `test_extensions_upgrade.sh` script if needed
