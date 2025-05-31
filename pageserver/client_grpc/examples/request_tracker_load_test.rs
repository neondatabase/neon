// examples/request_tracker_load_test.rs

use std::{sync::Arc, time::Duration};
use tokio;
use pageserver_client_grpc::request_tracker::RequestTracker;
use pageserver_client_grpc::ClientCacheOptions;
use pageserver_client_grpc::PageserverClientAggregateMetrics;

use pageserver_page_api::model;

use rand::prelude::*;

use pageserver_api::key::Key;

use utils::lsn::Lsn;
use utils::id::TenantTimelineId;

use pageserver_page_api::model::{GetPageClass, GetPageResponse, GetPageStatus};
#[derive(Clone)]
struct KeyRange {
    timeline: TenantTimelineId,
    timeline_lsn: Lsn,
    start: i128,
    end: i128,
}

impl KeyRange {
    fn len(&self) -> i128 {
        self.end - self.start
    }
}

#[tokio::main]
async fn main() {
    // 1) configure the client‐pool behavior
    let client_cache_options = ClientCacheOptions {
        max_delay_ms:       0,
        drop_rate:          0.0,
        hang_rate:          0.0,
        connect_timeout:    Duration::from_secs(0),
        connect_backoff:    Duration::from_millis(0),
        max_consumers:      1,
        error_threshold:    1,
        max_idle_duration:  Duration::from_secs(60),
    };

    // 2) metrics collector (we assume Default is implemented)
    let metrics = Arc::new(PageserverClientAggregateMetrics::new());

    // 3) build the tracker with a mock stream factory under the hood
    let auth_token: Option<String> = None;
    let mut tracker = RequestTracker::new(
        client_cache_options.clone(),
        "tenant1",
        "timeline1",
        &auth_token,
        metrics.clone(),
        "http://unused-endpoint",
    );

    // 4) fire off 10 000 requests in parallel
    let mut handles = Vec::with_capacity(10000);
    for i in 0..10000 {

        // taken mostly from pagebench
        let req = {
            let mut rng = rand::thread_rng();
            let r = KeyRange {
                timeline: TenantTimelineId::empty(),
                timeline_lsn: Lsn::from(i as u64),
                start: 10,
                end: 20,
            };
            let key: i128 = rng.gen_range(r.start..r.end);
            let key = Key::from_i128(key);
            let (rel_tag, block_no) = key
                .to_rel_block()
                .expect("we filter non-rel-block keys out above");
            pageserver_page_api::model::GetPageRequest {
                request_id: 0, // TODO
                request_class: GetPageClass::Normal,
                read_lsn: pageserver_page_api::model::ReadLsn {
                    request_lsn: if rng.gen_bool(0.5) {
                        Lsn::MAX
                    } else {
                        r.timeline_lsn
                    },
                    not_modified_since_lsn: r.timeline_lsn,
                },
                rel: pageserver_page_api::model::RelTag {
                    spc_oid: rel_tag.spcnode,
                    db_oid: rel_tag.dbnode,
                    rel_number: rel_tag.relnode,
                    fork_number: rel_tag.forknum,
                },
                block_number: vec![block_no],
            }
        };

        // RequestTracker is Clone, so we can share it
        let mut tr = tracker.clone();
        handles.push(tokio::spawn(async move {
            let resp = tr.send_request(req).await;
            // sanity‐check: the mock echo returns the same request_id
            assert!(resp.request_id > 0);
        }));
    }

    // 5) wait for them all
    for h in handles {
        h.await.expect("task panicked");
    }

    println!("✅ All 1000 requests completed successfully");
}
