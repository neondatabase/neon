mod client;
mod pool;
mod retry;

pub use client::{PageserverClient, ShardSpec};
pub use pageserver_api::shard::ShardStripeSize; // used in ShardSpec
