use std::collections::HashMap;

use bytes::Bytes;

use crate::model::*;
use pageserver_api::key::rel_block_to_key;
use pageserver_api::shard::key_to_shard_number;
use utils::shard::{ShardCount, ShardIndex, ShardStripeSize};

/// Splits GetPageRequests that straddle shard boundaries and assembles the responses.
/// TODO: add tests for this.
pub struct GetPageSplitter {
    /// Split requests by shard index.
    requests: HashMap<ShardIndex, GetPageRequest>,
    /// The response being assembled. Preallocated with empty pages, to be filled in.
    response: GetPageResponse,
    /// Maps the offset in `request.block_numbers` and `response.pages` to the owning shard. Used
    /// to assemble the response pages in the same order as the original request.
    block_shards: Vec<ShardIndex>,
}

impl GetPageSplitter {
    /// Checks if the given request only touches a single shard, and returns the shard ID. This is
    /// the common case, so we check first in order to avoid unnecessary allocations and overhead.
    pub fn for_single_shard(
        req: &GetPageRequest,
        count: ShardCount,
        stripe_size: Option<ShardStripeSize>,
    ) -> Result<Option<ShardIndex>, SplitError> {
        // Fast path: unsharded tenant.
        if count.is_unsharded() {
            return Ok(Some(ShardIndex::unsharded()));
        }

        let Some(stripe_size) = stripe_size else {
            return Err("stripe size must be given for sharded tenants".into());
        };

        // Find the first page's shard, for comparison.
        let Some(&first_page) = req.block_numbers.first() else {
            return Err("no block numbers in request".into());
        };
        let key = rel_block_to_key(req.rel, first_page);
        let shard_number = key_to_shard_number(count, stripe_size, &key);

        Ok(req
            .block_numbers
            .iter()
            .skip(1) // computed above
            .all(|&blkno| {
                let key = rel_block_to_key(req.rel, blkno);
                key_to_shard_number(count, stripe_size, &key) == shard_number
            })
            .then_some(ShardIndex::new(shard_number, count)))
    }

    /// Splits the given request.
    pub fn split(
        req: GetPageRequest,
        count: ShardCount,
        stripe_size: Option<ShardStripeSize>,
    ) -> Result<Self, SplitError> {
        // The caller should make sure we don't split requests unnecessarily.
        debug_assert!(
            Self::for_single_shard(&req, count, stripe_size)?.is_none(),
            "unnecessary request split"
        );

        if count.is_unsharded() {
            return Err("unsharded tenant, no point in splitting request".into());
        }
        let Some(stripe_size) = stripe_size else {
            return Err("stripe size must be given for sharded tenants".into());
        };

        // Split the requests by shard index.
        let mut requests = HashMap::with_capacity(2); // common case
        let mut block_shards = Vec::with_capacity(req.block_numbers.len());
        for &blkno in &req.block_numbers {
            let key = rel_block_to_key(req.rel, blkno);
            let shard_number = key_to_shard_number(count, stripe_size, &key);
            let shard_id = ShardIndex::new(shard_number, count);

            requests
                .entry(shard_id)
                .or_insert_with(|| GetPageRequest {
                    request_id: req.request_id,
                    request_class: req.request_class,
                    rel: req.rel,
                    read_lsn: req.read_lsn,
                    block_numbers: Vec::new(),
                })
                .block_numbers
                .push(blkno);
            block_shards.push(shard_id);
        }

        // Construct a response to be populated by shard responses. Preallocate empty page slots
        // with the expected block numbers.
        let response = GetPageResponse {
            request_id: req.request_id,
            status_code: GetPageStatusCode::Ok,
            reason: None,
            rel: req.rel,
            pages: req
                .block_numbers
                .into_iter()
                .map(|block_number| {
                    Page {
                        block_number,
                        image: Bytes::new(), // empty page slot to be filled in
                    }
                })
                .collect(),
        };

        Ok(Self {
            requests,
            response,
            block_shards,
        })
    }

    /// Drains the per-shard requests, moving them out of the splitter to avoid extra allocations.
    pub fn drain_requests(&mut self) -> impl Iterator<Item = (ShardIndex, GetPageRequest)> {
        self.requests.drain()
    }

    /// Adds a response from the given shard. The response must match the request ID and have an OK
    /// status code. A response must not already exist for the given shard ID.
    pub fn add_response(
        &mut self,
        shard_id: ShardIndex,
        response: GetPageResponse,
    ) -> Result<(), SplitError> {
        // The caller should already have converted status codes into tonic::Status.
        if response.status_code != GetPageStatusCode::Ok {
            return Err(SplitError(format!(
                "unexpected non-OK response for shard {shard_id}: {} {}",
                response.status_code,
                response.reason.unwrap_or_default()
            )));
        }

        if response.request_id != self.response.request_id {
            return Err(SplitError(format!(
                "response ID mismatch for shard {shard_id}: expected {}, got {}",
                self.response.request_id, response.request_id
            )));
        }

        if response.request_id != self.response.request_id {
            return Err(SplitError(format!(
                "response ID mismatch for shard {shard_id}: expected {}, got {}",
                self.response.request_id, response.request_id
            )));
        }

        // Place the shard response pages into the assembled response, in request order.
        let mut pages = response.pages.into_iter();

        for (i, &s) in self.block_shards.iter().enumerate() {
            if shard_id != s {
                continue;
            }

            let Some(slot) = self.response.pages.get_mut(i) else {
                return Err(SplitError(format!(
                    "no block_shards slot {i} for shard {shard_id}"
                )));
            };
            let Some(page) = pages.next() else {
                return Err(SplitError(format!(
                    "missing page {} in shard {shard_id} response",
                    slot.block_number
                )));
            };
            if page.block_number != slot.block_number {
                return Err(SplitError(format!(
                    "shard {shard_id} returned wrong page at index {i}, expected {} got {}",
                    slot.block_number, page.block_number
                )));
            }
            if !slot.image.is_empty() {
                return Err(SplitError(format!(
                    "shard {shard_id} returned duplicate page {} at index {i}",
                    slot.block_number
                )));
            }

            *slot = page;
        }

        // Make sure we've consumed all pages from the shard response.
        if let Some(extra_page) = pages.next() {
            return Err(SplitError(format!(
                "shard {shard_id} returned extra page: {}",
                extra_page.block_number
            )));
        }

        Ok(())
    }

    /// Collects the final, assembled response.
    pub fn collect_response(self) -> Result<GetPageResponse, SplitError> {
        // Check that the response is complete.
        for (i, page) in self.response.pages.iter().enumerate() {
            if page.image.is_empty() {
                return Err(SplitError(format!(
                    "missing page {} for shard {}",
                    page.block_number,
                    self.block_shards
                        .get(i)
                        .map(|s| s.to_string())
                        .unwrap_or_else(|| "?".to_string())
                )));
            }
        }

        Ok(self.response)
    }
}

/// A GetPageSplitter error.
#[derive(Debug, thiserror::Error)]
#[error("{0}")]
pub struct SplitError(String);

impl From<&str> for SplitError {
    fn from(err: &str) -> Self {
        SplitError(err.to_string())
    }
}

impl From<String> for SplitError {
    fn from(err: String) -> Self {
        SplitError(err)
    }
}

impl From<SplitError> for tonic::Status {
    fn from(err: SplitError) -> Self {
        tonic::Status::internal(err.0)
    }
}
