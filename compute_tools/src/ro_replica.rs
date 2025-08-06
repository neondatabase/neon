use std::{str::FromStr, sync::Arc, time::Duration};

use anyhow::Context;
use chrono::Utc;
use compute_api::spec::PageserverProtocol;
use futures::{FutureExt, StreamExt, stream::FuturesUnordered};
use postgres::SimpleQueryMessage;
use tokio_util::sync::CancellationToken;
use tracing::{Instrument, error, info, info_span, instrument, warn};
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

#[instrument(name = "standby_horizon_lease", skip_all, fields(lease_id))]
async fn bg_task(compute: Arc<ComputeNode>) {
    // Use a lease_id that is globally unique to this process to maximize attribution precision & log correlation.
    let lease_id = format!("v1-{}-{}", compute.params.compute_id, std::process::id());
    tracing::Span::current().record("lease_id", tracing::field::display(&lease_id));

    // Wait until we have the first value.
    // Allows us to simply .unwrap() later because it never transitions back to None.
    info!("waiting for first lease lsn to be fetched from postgres");
    let mut min_inflight_request_lsn_changed =
        compute.ro_replica.min_inflight_request_lsn.subscribe();
    min_inflight_request_lsn_changed.mark_changed(); // it could have been set already
    min_inflight_request_lsn_changed
        .wait_for(|value| value.is_some())
        .await
        .expect("we never drop the sender");

    // React to connstring changes. Sadly there is no async API for this yet.
    let (connstr_watch_tx, mut connstr_watch_rx) = tokio::sync::watch::channel(None);
    std::thread::spawn({
        let compute = Arc::clone(&compute);
        move || {
            loop {
                compute.wait_timeout_while_pageserver_connstr_unchanged(Duration::MAX);
                let new = compute
                    .state
                    .lock()
                    .unwrap()
                    .pspec
                    .as_ref()
                    .and_then(|pspec| pspec.spec.pageserver_connstring.clone());
                connstr_watch_tx.send_if_modified(|existing| {
                    if &new != existing {
                        *existing = new;
                        true
                    } else {
                        false
                    }
                });
            }
        }
    });

    let mut obtained = ObtainedLease {
        lsn: Lsn(0),
        nearest_expiration: Utc::now(),
    };
    loop {
        let valid_duration: Duration = obtained
            .nearest_expiration
            .signed_duration_since(Utc::now())
            .to_std()
            // to_std() errors if the duration is less than zero, i.e,. if the lease already expired;
            // try to renew anyway in that case;
            .unwrap_or_default();
        // Sleep for 60 seconds less than the valid duration but no more than half of the valid duration.
        let sleep_duration = valid_duration
            .saturating_sub(Duration::from_secs(60))
            .max(valid_duration / 2);

        tokio::select! {
            _  = tokio::time::sleep(sleep_duration) => {
                info!("updating because lease is going to expire soon");
            }
            _ = connstr_watch_rx.changed() => {
                info!("updating due to changed pageserver_connstr")
            }
            _ = async {
                // debounce; TODO make this lower in tests
                tokio::time::sleep(Duration::from_secs(10)).await;
                // every 10 GiB; TODO make this tighter in tests?
                let max_horizon_lag = 10 * (1<<30);
                min_inflight_request_lsn_changed.wait_for(|x| x.unwrap().0 > obtained.lsn.0 + max_horizon_lag).await
            } => {
                info!(%obtained.lsn, "updating due to max horizon lag");
            }
        }
        // retry forever
        let compute = Arc::clone(&compute);
        let lease_id = lease_id.clone();
        obtained = retry(
            || attempt(lease_id.clone(), &compute),
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

struct ObtainedLease {
    lsn: Lsn,
    nearest_expiration: chrono::DateTime<Utc>,
}

async fn attempt(lease_id: String, compute: &Arc<ComputeNode>) -> anyhow::Result<ObtainedLease> {
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
        let logging_wrapper = async |fut| {
            async move {
                // TODO: timeout?
                match fut.await {
                    Ok(Some(v)) => {
                        info!("lease obtained");
                        Ok(Some(v))
                    }
                    Ok(None) => {
                        error!("pageserver rejected our request");
                        Ok(None)
                    }
                    Err(err) => {
                        error!("communication failure: {err:?}");
                        Err(())
                    }
                }
            }
            .instrument(logging_span)
            .await
        };
        let fut = match PageserverProtocol::from_connstring(&connect_info.connstring)? {
            PageserverProtocol::Libpq => logging_wrapper(
                attempt_one_libpq(connect_info, timeline_id, lease_id.clone(), lsn).boxed(),
            ),
            PageserverProtocol::Grpc => logging_wrapper(Box::pin(
                attempt_one_grpc(connect_info, timeline_id, lease_id.clone(), lsn).boxed(),
            )),
        };
        futs.push(fut);
    }
    let mut errors = 0;
    let mut nearest_expiration = None;
    while let Some(res) = futs.next().await {
        match res {
            Ok(Some(expiration)) => {
                let nearest_expiration = nearest_expiration.get_or_insert(expiration);
                *nearest_expiration = std::cmp::min(*nearest_expiration, expiration);
            }
            Ok(None) | Err(()) => {
                // the logging wrapper does the logging
                errors += 1;
            }
        }
    }
    if errors > 0 {
        return Err(anyhow::anyhow!(
            "failed to advance standby_horizon for {errors} shards, check logs for details"
        ));
    }
    match nearest_expiration {
        Some(nearest_expiration) => Ok(ObtainedLease {
            lsn,
            nearest_expiration,
        }),
        None => Err(anyhow::anyhow!("pageservers connstrings is empty")), // this probably can't happen
    }
}

async fn attempt_one_libpq(
    connect_info: ConnectInfo,
    timeline_id: TimelineId,
    lease_id: String,
    lsn: Lsn,
) -> anyhow::Result<Option<chrono::DateTime<Utc>>> {
    let ConnectInfo {
        tenant_shard_id,
        connstring,
        auth,
    } = connect_info;
    let mut config = tokio_postgres::Config::from_str(&connstring)?;
    if let Some(auth) = auth {
        config.password(auth);
    }
    let (client, conn) = config.connect(postgres::NoTls).await?;
    tokio::spawn(conn);
    let cmd = format!("lease standby_horizon {tenant_shard_id} {timeline_id} {lease_id} {lsn} ");
    let res = client.simple_query(&cmd).await?;
    let msg = match res.first() {
        Some(msg) => msg,
        None => anyhow::bail!("empty response"),
    };
    let row = match msg {
        SimpleQueryMessage::Row(row) => row,
        _ => anyhow::bail!("expected row message type"),
    };

    // Note: this will be NULL (=> None) if a lease is explicitly not granted.
    row.get("expiration")
        .map(|s| {
            chrono::DateTime::<Utc>::from_str(s).with_context(|| format!("parse expiration: {s:?}"))
        })
        .transpose()
}

async fn attempt_one_grpc(
    connect_info: ConnectInfo,
    timeline_id: TimelineId,
    lease_id: String,
    lsn: Lsn,
) -> anyhow::Result<Option<chrono::DateTime<Utc>>> {
    let ConnectInfo {
        tenant_shard_id,
        connstring,
        auth,
    } = connect_info;
    let mut client = pageserver_page_api::Client::connect(
        connstring.to_string(),
        tenant_shard_id.tenant_id,
        timeline_id,
        tenant_shard_id.to_index(),
        auth,
        None,
    )
    .await?;

    let req = pageserver_page_api::LeaseStandbyHorizonRequest { lease_id, lsn };
    match client.lease_standby_horizon(req).await {
        Ok(pageserver_page_api::LeaseStandbyHorizonResponse { expiration }) => Ok(Some(expiration)),
        // Lease couldn't be acquired
        Err(err) if err.code() == tonic::Code::FailedPrecondition => Ok(None),
        Err(err) => Err(err.into()),
    }
}
