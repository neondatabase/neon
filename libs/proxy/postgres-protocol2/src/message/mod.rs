//! Postgres message protocol support.
//!
//! See [Postgres's documentation][docs] for more information on message flow.
//!
//! [docs]: https://www.postgresql.org/docs/9.5/static/protocol-flow.html

pub mod backend;
pub mod frontend;
