[![Neon](https://github.com/neondatabase/neon/assets/11527560/f15a17f0-836e-40c5-b35d-030606a6b660)](https://neon.tech)

# Neon

Neon is a serverless open-source alternative to AWS Aurora Postgres. It separates storage and compute
and substitutes the PostgreSQL storage layer by redistributing data across a cluster of nodes.

## Quick Start

Try the [Neon Free Tier](https://neon.tech/github) to create a serverless Postgres instance.
Then connect to it with your preferred Postgres client (psql, dbeaver, etc) or use the online
[SQL Editor](https://neon.tech/docs/get-started-with-neon/query-with-neon-sql-editor/).

See [Connect from any application](https://neon.tech/docs/connect/connect-from-any-app/) for connection instructions.

## Build Instructions

Build instructions (run a local installation) see: [Build Instructions](/docs/build/README.md)

## Architecture Overview

A Neon installation consists of compute nodes and the Neon storage engine.
Compute nodes are stateless PostgreSQL nodes backed by the Neon storage engine.

The Neon storage engine consists of two major components:

| Component   | Description                                                                                           |
|-------------|-------------------------------------------------------------------------------------------------------|
| Pageserver  | Scalable storage backend for the *compute* nodes                                                      |
| Safekeepers | The safekeepers form a redundant WAL service that received WAL from the compute node, and stores it durably until it has been processed by the pageserver and uploaded to cloud storage |

## Documentation

The `/docs` subdir contains [sourcetree.md](/docs/sourcetree.md) which gives you better insight of neon infrastructure.

To view your `rustdoc` documentation in a browser, try running `cargo doc --no-deps --open`
See also README files in some source directories, and `rustdoc` style documentation comments.

### Other Resources

- [SELECT 'Hello, World'](https://neon.tech/blog/hello-world/): Blog post by Nikita Shamgunov on the high level architecture
- [Architecture decisions in Neon](https://neon.tech/blog/architecture-decisions-in-neon/): Blog post by Heikki Linnakangas
- [Neon: Serverless PostgreSQL!](https://www.youtube.com/watch?v=rES0yzeERns): Presentation on storage system by Heikki Linnakangas in the CMU Database Group seminar series

## Postgres-specific Terms

Due to Neon's very close relation with PostgreSQL internals, numerous specific terms are used.
The same applies to certain spelling: i.e. we use MB to denote 1024 * 1024 bytes, while MiB would be technically more
correct, it's inconsistent with what PostgreSQL code and its documentation use.

To get more familiar with this aspect, refer to:

- [Neon glossary](/docs/glossary.md)
- [PostgreSQL glossary](https://www.postgresql.org/docs/14/glossary.html)
- Other PostgreSQL documentation and sources (Neon fork sources can be found [here](https://github.com/neondatabase/postgres))

## Join The Development

- Read [CONTRIBUTING.md](/CONTRIBUTING.md) to learn about project code style and practices.
- To get familiar with a source tree layout, use [sourcetree.md](/docs/sourcetree.md).
- To learn more about PostgreSQL internals, check http://www.interdb.jp/pg/index.html

See developer documentation in [SUMMARY.md](/docs/SUMMARY.md) for more information.
