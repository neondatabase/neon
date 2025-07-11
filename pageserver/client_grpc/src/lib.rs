mod client;
mod pool;
mod retry;
mod split;

pub use client::{PageserverClient, ShardSpec};
pub use pageserver_api::shard::ShardStripeSize; // used in ShardSpec
