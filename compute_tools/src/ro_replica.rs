use std::{
    pin::Pin,
    str::FromStr,
    sync::Arc,
    time::{Duration, Instant, SystemTime},
};

use compute_api::spec::PageserverProtocol;
use futures::{StreamExt, stream::FuturesUnordered};
use tokio_util::sync::CancellationToken;
use tracing::{Instrument, error, info, info_span, warn};
use utils::{backoff::retry, id::TimelineId, lsn::Lsn};

use crate::{
    compute::ComputeNode,
    pageserver_client::{ConnectInfo, pageserver_connstrings_for_connect},
};

#[derive(Default)]
pub(crate) struct GlobalState {
    min_inflight_request_lsn: tokio::sync::watch::Sender<Option<Lsn>>,
}

impl GlobalState {
    pub fn update_min_inflight_request_lsn(&self, update: Lsn) {
        self.min_inflight_request_lsn.send_if_modified(|value| {
            let modified = *value != Some(update);
            if let Some(value) = *value && value > update {
                warn!(current=%value, new=%update, "min inflight request lsn moving backwards, this should not happen, bug in communicator");
            }
            *value = Some(update);
            modified
        });
    }
}

pub fn spawn_bg_task(compute: Arc<ComputeNode>) {
    std::thread::spawn(|| {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        rt.block_on(bg_task(compute))
    });
}

#[derive(Clone)]
struct Reservation {
    horizon: Lsn,
    expiration: Instant,
}

async fn bg_task(compute: Arc<ComputeNode>) {
    // Wait until we have the first value.
    // Allows us to simply .unwrap() later because it never transitions back to None.
    let mut min_inflight_request_lsn_changed =
        compute.ro_replica.min_inflight_request_lsn.subscribe();
    min_inflight_request_lsn_changed.mark_changed();
    min_inflight_request_lsn_changed
        .wait_for(|value| value.is_some())
        .await;

    let (connstr_watch_tx, mut connstr_watch_rx) = tokio::sync::watch::channel(());
    std::thread::spawn({
        let compute = Arc::clone(&compute);
        move || {
            loop {
                compute
                    .wait_timeout_while_pageserver_connstr_unchanged(Duration::from_secs(todo!()));
                connstr_watch_tx.send_replace(());
            }
        }
    });

    let mut reservation = Reservation {
        horizon: Lsn(0),
        expiration: Instant::now(),
    };
    loop {
        tokio::select! {
            _  = tokio::time::sleep_until(reservation.expiration.into()) => {
                info!("updating due to expiration");
            }
            _ = connstr_watch_rx.changed() => {
                info!("updating due to changed pageserver_connstr")
            }
            _ = async {
                // debounce TODO make this lower in tests
                tokio::time::sleep(Duration::from_secs(10));
                // every 10 GiB TODO make this tighter in tests?
                let max_horizon_lag = 10 * (1<<30);
                min_inflight_request_lsn_changed.wait_for(|x| x.unwrap().0 > reservation.horizon.0 + max_horizon_lag).await
            } => {
                info!(%reservation.horizon, "updating due to max horizon lag");
            }
        }
        // retry forever
        let compute = Arc::clone(&compute);
        reservation = retry(
            || attempt(&compute),
            |_| false,
            0,
            u32::MAX, // forever
            "update standby_horizon position in pageserver",
            // There is no cancellation story in compute_ctl
            &CancellationToken::new(),
        )
        .await
        .expect("is_permanent returns false, so, retry always returns Some")
        .expect("u32::MAX exceeded");
    }
}

// Returns expiration time
async fn attempt(compute: &Arc<ComputeNode>) -> anyhow::Result<Reservation> {
    let (shards, timeline_id) = {
        let state = compute.state.lock().unwrap();
        let pspec = state.pspec.as_ref().expect("spec must be set");
        (pageserver_connstrings_for_connect(pspec), pspec.timeline_id)
    };

    let lsn = compute
        .ro_replica
        .min_inflight_request_lsn
        .borrow()
        .expect("we only call this function once it has been transitioned to Some");
    let mut futs = FuturesUnordered::new();
    for connect_info in shards {
        let logging_span = info_span!(
            "attempt_one",
            tenant_id=%connect_info.tenant_shard_id.tenant_id,
            shard_id=%connect_info.tenant_shard_id.shard_slug(),
            timeline_id=%timeline_id,
        );
        let logging_wrapper = |fut: Pin<Box<dyn Future<Output = anyhow::Result<Reservation>>>>| {
            async move {
            match fut.await {
                Ok(v) => Ok(v),
                Err(err) => {
                    error!(
                        "failed to advance standby_horizon, communicator reads from this shard may star failing: {err:?}"
                    );
                    Err(())
                }
            }}.instrument(logging_span)
        };
        let fut = match PageserverProtocol::from_connstring(&connect_info.connstring)? {
            PageserverProtocol::Libpq => {
                logging_wrapper(Box::pin(attempt_one_libpq(connect_info, timeline_id, lsn)))
            }
            PageserverProtocol::Grpc => {
                logging_wrapper(Box::pin(attempt_one_grpc(connect_info, timeline_id, lsn)))
            }
        };
        futs.push(fut);
    }
    let mut errors = 0;
    let mut min = None;
    while let Some(res) = futs.next().await {
        match res {
            Ok(reservation) => {
                let Reservation {
                    horizon,
                    expiration,
                } = min.get_or_insert_with(|| reservation.clone());
                *horizon = std::cmp::min(*horizon, reservation.horizon);
                *expiration = std::cmp::min(*expiration, reservation.expiration);
            }
            Err(()) => {
                // the logging wrapper does the logging
            }
        }
    }
    if errors > 0 {
        return Err(anyhow::anyhow!(
            "failed to advance standby_horizon for {errors} shards, check logs for details"
        ));
    }
    match min {
        Some(min) => Ok(min),
        None => Err(anyhow::anyhow!("pageservers connstrings is empty")), // this probably can't happen
    }
}

async fn attempt_one_libpq(
    connect_info: ConnectInfo,
    timeline_id: TimelineId,
    lsn: Lsn,
) -> anyhow::Result<Reservation> {
    let ConnectInfo {
        tenant_shard_id,
        connstring,
        auth,
    } = connect_info;
    tokio_postgres::Config::from_str(&connstring)?;
    todo!()
}

async fn attempt_one_grpc(
    connect_info: ConnectInfo,
    timeline_id: TimelineId,
    lsn: Lsn,
) -> anyhow::Result<Reservation> {
    todo!()
}
