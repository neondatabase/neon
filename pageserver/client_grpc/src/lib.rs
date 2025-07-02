//! A rich Pageserver gRPC client. This client is more capable than the basic `page_api::Client`
//! gRPC client, and supports:
//!
//! * Sharded tenants across multiple Pageservers.
//! * Pooling of connections, clients, and streams for efficient resource use.
//! * Concurrent use by many callers.
//! * Internal handling of GetPage bidirectional streams.
//! * Automatic retries.
//! * Observability.
//!
//! The client is under development, this package is just a shell.

#[allow(unused)]
mod pool;
