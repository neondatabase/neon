use std::collections::HashMap;

use bytes::Bytes;

use pageserver_api::key::rel_block_to_key;
use pageserver_api::shard::{ShardStripeSize, key_to_shard_number};
use pageserver_page_api as page_api;
use utils::shard::{ShardCount, ShardIndex};

/// Splits GetPageRequests that straddle shard boundaries and assembles the responses.
/// TODO: add tests for this.
pub struct GetPageSplitter {
    /// The original request ID. Used for all shard requests.
    request_id: page_api::RequestID,
    /// Split requests by shard index.
    requests: HashMap<ShardIndex, page_api::GetPageRequest>,
    /// Maps the offset in `GetPageRequest::block_numbers` to the owning shard. Used to assemble
    /// the response pages in the same order as the original request.
    block_shards: Vec<ShardIndex>,
    /// Page responses by shard index. Will be assembled into a single response.
    responses: HashMap<ShardIndex, Vec<Bytes>>,
}

impl GetPageSplitter {
    /// Checks if the given request only touches a single shard, and returns the shard ID. This is
    /// the common case, so we check first in order to avoid unnecessary allocations and overhead.
    /// The caller must ensure that the request has at least one block number, or this will panic.
    pub fn is_single_shard(
        req: &page_api::GetPageRequest,
        count: ShardCount,
        stripe_size: ShardStripeSize,
    ) -> Option<ShardIndex> {
        // Fast path: unsharded tenant.
        if count.is_unsharded() {
            return Some(ShardIndex::unsharded());
        }

        // Find the base shard index for the first page, and compare with the rest.
        let key = rel_block_to_key(req.rel, *req.block_numbers.first().expect("no pages"));
        let shard_number = key_to_shard_number(count, stripe_size, &key);

        req.block_numbers
            .iter()
            .skip(1) // computed above
            .all(|&blkno| {
                let key = rel_block_to_key(req.rel, blkno);
                key_to_shard_number(count, stripe_size, &key) == shard_number
            })
            .then_some(ShardIndex::new(shard_number, count))
    }

    /// Splits the given request.
    pub fn split(
        req: page_api::GetPageRequest,
        count: ShardCount,
        stripe_size: ShardStripeSize,
    ) -> Self {
        // The caller should make sure we don't split requests unnecessarily.
        debug_assert!(
            Self::is_single_shard(&req, count, stripe_size).is_none(),
            "unnecessary request split"
        );

        // Split the requests by shard index.
        let mut requests = HashMap::with_capacity(2); // common case
        let mut block_shards = Vec::with_capacity(req.block_numbers.len());
        for blkno in req.block_numbers {
            let key = rel_block_to_key(req.rel, blkno);
            let shard_number = key_to_shard_number(count, stripe_size, &key);
            let shard_id = ShardIndex::new(shard_number, count);

            let shard_req = requests
                .entry(shard_id)
                .or_insert_with(|| page_api::GetPageRequest {
                    request_id: req.request_id,
                    request_class: req.request_class,
                    rel: req.rel,
                    read_lsn: req.read_lsn,
                    block_numbers: Vec::new(),
                });
            shard_req.block_numbers.push(blkno);
            block_shards.push(shard_id);
        }

        Self {
            request_id: req.request_id,
            responses: HashMap::with_capacity(requests.len()),
            requests,
            block_shards,
        }
    }

    /// Drains the per-shard requests, moving them out of the hashmap to avoid extra allocations.
    pub fn drain_requests(
        &mut self,
    ) -> impl Iterator<Item = (ShardIndex, page_api::GetPageRequest)> {
        self.requests.drain()
    }

    /// Adds a response from the given shard. The response must match the request ID and have an OK
    /// status code. A response must not already exist for the given shard ID.
    #[allow(clippy::result_large_err)]
    pub fn add_response(
        &mut self,
        shard_id: ShardIndex,
        response: page_api::GetPageResponse,
    ) -> tonic::Result<()> {
        // The caller should already have converted status codes into tonic::Status.
        if response.status_code != page_api::GetPageStatusCode::Ok {
            return Err(tonic::Status::internal(format!(
                "unexpected non-OK response for shard {shard_id}: {:?}",
                response.status_code
            )));
        }

        // The stream pool ensures the response matches the request ID.
        if response.request_id != self.request_id {
            return Err(tonic::Status::internal(format!(
                "response ID mismatch for shard {shard_id}: expected {}, got {}",
                self.request_id, response.request_id
            )));
        }

        // We only dispatch one request per shard.
        if self.responses.contains_key(&shard_id) {
            return Err(tonic::Status::internal(format!(
                "duplicate response for shard {shard_id}"
            )));
        }

        // Add the response data to the map.
        self.responses.insert(shard_id, response.page_images);

        Ok(())
    }

    /// Assembles the shard responses into a single response. Responses must be present for all
    /// relevant shards, and the total number of pages must match the original request.
    #[allow(clippy::result_large_err)]
    pub fn assemble_response(self) -> tonic::Result<page_api::GetPageResponse> {
        let mut response = page_api::GetPageResponse {
            request_id: self.request_id,
            status_code: page_api::GetPageStatusCode::Ok,
            reason: None,
            page_images: Vec::with_capacity(self.block_shards.len()),
        };

        // Set up per-shard page iterators we can pull from.
        let mut shard_responses = HashMap::with_capacity(self.responses.len());
        for (shard_id, responses) in self.responses {
            shard_responses.insert(shard_id, responses.into_iter());
        }

        // Reassemble the responses in the same order as the original request.
        for shard_id in &self.block_shards {
            let page = shard_responses
                .get_mut(shard_id)
                .ok_or_else(|| {
                    tonic::Status::internal(format!("missing response for shard {shard_id}"))
                })?
                .next()
                .ok_or_else(|| {
                    tonic::Status::internal(format!("missing page from shard {shard_id}"))
                })?;
            response.page_images.push(page);
        }

        // Make sure there are no additional pages.
        for (shard_id, mut pages) in shard_responses {
            if pages.next().is_some() {
                return Err(tonic::Status::internal(format!(
                    "extra pages returned from shard {shard_id}"
                )));
            }
        }

        Ok(response)
    }
}
