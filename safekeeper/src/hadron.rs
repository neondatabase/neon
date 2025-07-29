use once_cell::sync::Lazy;
use pem::Pem;
use safekeeper_api::models::PullTimelineRequest;
use std::{
    collections::HashMap, env::VarError, net::IpAddr, sync::Arc, sync::atomic::AtomicBool,
    time::Duration,
};
use tokio::time::sleep;
use tokio_util::sync::CancellationToken;
use url::Url;
use utils::{backoff, critical_timeline, id::TenantTimelineId, ip_address};

use anyhow::{Result, anyhow};

use pageserver_api::controller_api::{
    AvailabilityZone, NodeRegisterRequest, SafekeeperTimeline, SafekeeperTimelinesResponse,
};

use crate::{
    GlobalTimelines, SafeKeeperConf,
    metrics::{
        SK_RECOVERY_PULL_TIMELINE_ERRORS, SK_RECOVERY_PULL_TIMELINE_OKS,
        SK_RECOVERY_PULL_TIMELINE_SECONDS, SK_RECOVERY_PULL_TIMELINES_SECONDS,
    },
    pull_timeline,
    timelines_global_map::DeleteOrExclude,
};

// Extract information in the SafeKeeperConf to build a NodeRegisterRequest used to register the safekeeper with the HCC.
fn build_node_registeration_request(
    conf: &SafeKeeperConf,
    node_ip_addr: Option<IpAddr>,
) -> Result<NodeRegisterRequest> {
    let advertise_pg_addr_with_port = conf
        .advertise_pg_addr_tenant_only
        .as_deref()
        .expect("advertise_pg_addr_tenant_only is required to register with HCC");

    // Extract host/port from the string.
    let (advertise_host_addr, pg_port_str) = advertise_pg_addr_with_port.split_at(
        advertise_pg_addr_with_port
            .rfind(':')
            .ok_or(anyhow::anyhow!("Invalid advertise_pg_addr"))?,
    );
    // Need the `[1..]` to remove the leading ':'.
    let pg_port = pg_port_str[1..]
        .parse::<u16>()
        .map_err(|e| anyhow::anyhow!("Cannot parse PG port: {}", e))?;

    let (_, http_port_str) = conf.listen_http_addr.split_at(
        conf.listen_http_addr
            .rfind(':')
            .ok_or(anyhow::anyhow!("Invalid listen_http_addr"))?,
    );
    let http_port = http_port_str[1..]
        .parse::<u16>()
        .map_err(|e| anyhow::anyhow!("Cannot parse HTTP port: {}", e))?;

    Ok(NodeRegisterRequest {
        node_id: conf.my_id,
        listen_pg_addr: advertise_host_addr.to_string(),
        listen_pg_port: pg_port,
        listen_http_addr: advertise_host_addr.to_string(),
        listen_http_port: http_port,
        node_ip_addr,
        availability_zone_id: AvailabilityZone("todo".to_string()),
        listen_grpc_addr: None,
        listen_grpc_port: None,
        listen_https_port: None,
    })
}

// Retrieve the JWT token used for authenticating with HCC from the environment variable.
// Returns None if the token cannot be retrieved.
fn get_hcc_auth_token() -> Option<String> {
    match std::env::var("HCC_AUTH_TOKEN") {
        Ok(v) => {
            tracing::info!("Loaded JWT token for authentication with HCC");
            Some(v)
        }
        Err(VarError::NotPresent) => {
            tracing::info!("No JWT token for authentication with HCC detected");
            None
        }
        Err(_) => {
            tracing::info!(
                "Failed to either load to detect non-present HCC_AUTH_TOKEN environment variable"
            );
            None
        }
    }
}

async fn send_safekeeper_register_request(
    request_url: &Url,
    auth_token: &Option<String>,
    request: &NodeRegisterRequest,
) -> Result<()> {
    let client = reqwest::Client::new();
    let mut req_builder = client
        .post(request_url.clone())
        .header("Content-Type", "application/json");
    if let Some(token) = auth_token {
        req_builder = req_builder.bearer_auth(token);
    }
    req_builder
        .json(&request)
        .send()
        .await?
        .error_for_status()?;
    Ok(())
}

/// Registers this safe keeper with the HCC.
pub async fn register(conf: &SafeKeeperConf) -> Result<()> {
    match conf.hcc_base_url.as_ref() {
        None => {
            tracing::info!("HCC base URL is not set, skipping registration");
            Ok(())
        }
        Some(hcc_base_url) => {
            // The following operations acquiring the auth token and the node IP address both read environment
            // variables. It's fine for now as this `register()` function is only called once during startup.
            // If we start to talk to HCC more regularly in the safekeeper we should probably consider
            // refactoring things into a "HadronClusterCoordinatorClient" struct.
            let auth_token = get_hcc_auth_token();
            let node_ip_addr =
                ip_address::read_node_ip_addr_from_env().expect("Error reading node IP address.");

            let request = build_node_registeration_request(conf, node_ip_addr)?;
            let cancel = CancellationToken::new();
            let request_url = hcc_base_url.clone().join("/hadron-internal/v1/sk")?;

            backoff::retry(
                || async {
                    send_safekeeper_register_request(&request_url, &auth_token, &request).await
                },
                |_| false,
                3,
                u32::MAX,
                "Calling the HCC safekeeper register API",
                &cancel,
            )
            .await
            .ok_or(anyhow::anyhow!(
                "Error in forever retry loop. This error should never be surfaced."
            ))?
        }
    }
}

async fn safekeeper_list_timelines_request(
    conf: &SafeKeeperConf,
) -> Result<pageserver_api::controller_api::SafekeeperTimelinesResponse> {
    if conf.hcc_base_url.is_none() {
        tracing::info!("HCC base URL is not set, skipping registration");
        return Err(anyhow::anyhow!("HCC base URL is not set"));
    }

    // The following operations acquiring the auth token and the node IP address both read environment
    // variables. It's fine for now as this `register()` function is only called once during startup.
    // If we start to talk to HCC more regularly in the safekeeper we should probably consider
    // refactoring things into a "HadronClusterCoordinatorClient" struct.
    let auth_token = get_hcc_auth_token();
    let method = format!("/control/v1/safekeeper/{}/timelines", conf.my_id.0);
    let request_url = conf.hcc_base_url.as_ref().unwrap().clone().join(&method)?;

    let client = reqwest::Client::new();
    let mut req_builder = client
        .get(request_url.clone())
        .header("Content-Type", "application/json")
        .query(&[("id", conf.my_id.0)]);
    if let Some(token) = auth_token {
        req_builder = req_builder.bearer_auth(token);
    }
    let response = req_builder
        .send()
        .await?
        .error_for_status()?
        .json::<pageserver_api::controller_api::SafekeeperTimelinesResponse>()
        .await?;
    Ok(response)
}

// Returns true on success, false otherwise.
pub async fn hcc_pull_timeline(
    timeline: SafekeeperTimeline,
    conf: &SafeKeeperConf,
    global_timelines: Arc<GlobalTimelines>,
    nodeid_http: &HashMap<u64, String>,
) -> bool {
    let mut request = PullTimelineRequest {
        tenant_id: timeline.tenant_id,
        timeline_id: timeline.timeline_id,
        http_hosts: Vec::new(),
        mconf: None,
    };
    for host in timeline.peers {
        if host.0 == conf.my_id.0 {
            continue;
        }
        if let Some(http_host) = nodeid_http.get(&host.0) {
            request.http_hosts.push(http_host.clone());
        }
    }

    let ca_certs = match conf
        .ssl_ca_certs
        .iter()
        .map(Pem::contents)
        .map(reqwest::Certificate::from_der)
        .collect::<Result<Vec<_>, _>>()
    {
        Ok(result) => result,
        Err(_) => {
            return false;
        }
    };
    match pull_timeline::handle_request(
        request,
        conf.sk_auth_token.clone(),
        ca_certs,
        global_timelines.clone(),
        true,
    )
    .await
    {
        Ok(resp) => {
            tracing::info!(
                "Completed pulling tenant {} timeline {} from SK {:?}",
                timeline.tenant_id,
                timeline.timeline_id,
                resp.safekeeper_host
            );
            return true;
        }
        Err(e) => {
            tracing::error!(
                "Failed to pull tenant {} timeline {} from SK {}",
                timeline.tenant_id,
                timeline.timeline_id,
                e
            );

            let ttid = TenantTimelineId {
                tenant_id: timeline.tenant_id,
                timeline_id: timeline.timeline_id,
            };
            // Revert the failed timeline pull.
            // Notice that not found timeline returns OK also.
            match global_timelines
                .delete_or_exclude(&ttid, DeleteOrExclude::DeleteLocal)
                .await
            {
                Ok(dr) => {
                    tracing::info!(
                        "Deleted tenant {} timeline {} DirExists: {}",
                        timeline.tenant_id,
                        timeline.timeline_id,
                        dr.dir_existed,
                    );
                }
                Err(e) => {
                    tracing::error!(
                        "Failed to delete tenant {} timeline {} from global_timelines: {}",
                        timeline.tenant_id,
                        timeline.timeline_id,
                        e
                    );
                }
            }
        }
    }
    false
}

pub async fn hcc_pull_timeline_till_success(
    timeline: SafekeeperTimeline,
    conf: &SafeKeeperConf,
    global_timelines: Arc<GlobalTimelines>,
    nodeid_http: &HashMap<u64, String>,
) {
    const MAX_PULL_TIMELINE_RETRIES: u64 = 100;
    for i in 0..MAX_PULL_TIMELINE_RETRIES {
        if hcc_pull_timeline(
            timeline.clone(),
            conf,
            global_timelines.clone(),
            nodeid_http,
        )
        .await
        {
            SK_RECOVERY_PULL_TIMELINE_OKS.inc();
            return;
        }
        tracing::error!(
            "Failed to pull timeline {} from SK peers, retrying {}/{}",
            timeline.timeline_id,
            i + 1,
            MAX_PULL_TIMELINE_RETRIES
        );
        tokio::time::sleep(std::time::Duration::from_secs(1)).await;
    }
    SK_RECOVERY_PULL_TIMELINE_ERRORS.inc();
}

pub async fn hcc_pull_timelines(
    conf: &SafeKeeperConf,
    global_timelines: Arc<GlobalTimelines>,
) -> Result<()> {
    let _timer = SK_RECOVERY_PULL_TIMELINES_SECONDS.start_timer();
    tracing::info!("Start pulling timelines from SK peers");

    let mut response = SafekeeperTimelinesResponse {
        timelines: Vec::new(),
        safekeeper_peers: Vec::new(),
    };
    for i in 0..100 {
        match safekeeper_list_timelines_request(conf).await {
            Ok(timelines) => {
                response = timelines;
            }
            Err(e) => {
                tracing::error!("Failed to list timelines from HCC: {}", e);
                if i == 99 {
                    return Err(e);
                }
            }
        }
        sleep(Duration::from_millis(100)).await;
    }

    let mut nodeid_http = HashMap::new();
    for sk in response.safekeeper_peers {
        nodeid_http.insert(
            sk.node_id.0,
            format!("http://{}:{}", sk.listen_http_addr, sk.http_port),
        );
    }
    tracing::info!("Received {} timelines from HCC", response.timelines.len());
    for timeline in response.timelines {
        let _timer = SK_RECOVERY_PULL_TIMELINE_SECONDS
            .with_label_values(&[
                &timeline.tenant_id.to_string(),
                &timeline.timeline_id.to_string(),
            ])
            .start_timer();
        hcc_pull_timeline_till_success(timeline, conf, global_timelines.clone(), &nodeid_http)
            .await;
    }
    Ok(())
}

/// true if the last background scan found total usage > limit
pub static GLOBAL_DISK_LIMIT_EXCEEDED: Lazy<AtomicBool> = Lazy::new(|| AtomicBool::new(false));

/// Returns filesystem usage in bytes for the filesystem containing the given path.
// Need to suppress the clippy::unnecessary_cast warning because the casts on the block count and the
// block size are required on macOS (they are 32-bit integers on macOS, apparantly).
#[allow(clippy::unnecessary_cast)]
pub fn get_filesystem_usage(path: &std::path::Path) -> u64 {
    // Allow overriding disk usage via failpoint for tests
    fail::fail_point!("sk-global-disk-usage", |val| {
        // val is Option<String>; parse payload if present
        val.and_then(|s| s.parse::<u64>().ok()).unwrap_or(0)
    });

    // Call statvfs(3) for filesystem usage
    use nix::sys::statvfs::statvfs;
    match statvfs(path) {
        Ok(stat) => {
            // fragment size (f_frsize) if non-zero else block size (f_bsize)
            let frsize = stat.fragment_size();
            let blocksz = if frsize > 0 {
                frsize
            } else {
                stat.block_size()
            };
            // used blocks = total blocks - available blocks for unprivileged
            let used_blocks = stat.blocks().saturating_sub(stat.blocks_available());
            used_blocks as u64 * blocksz as u64
        }
        Err(e) => {
            // The global disk usage watcher aren't associated with a tenant or timeline, so we just
            // pass placeholder (all-zero) tenant and timeline IDs to the critical!() macro.
            let placeholder_ttid = TenantTimelineId::empty();
            critical_timeline!(
                placeholder_ttid.tenant_id,
                placeholder_ttid.timeline_id,
                None::<&AtomicBool>,
                "Global disk usage watcher failed to read filesystem usage: {:?}",
                e
            );
            0
        }
    }
}

/// Returns the total capacity of the current working directory's filesystem in bytes.
#[allow(clippy::unnecessary_cast)]
pub fn get_filesystem_capacity(path: &std::path::Path) -> Result<u64> {
    // Call statvfs(3) for filesystem stats
    use nix::sys::statvfs::statvfs;
    match statvfs(path) {
        Ok(stat) => {
            // fragment size (f_frsize) if non-zero else block size (f_bsize)
            let frsize = stat.fragment_size();
            let blocksz = if frsize > 0 {
                frsize
            } else {
                stat.block_size()
            };
            Ok(stat.blocks() as u64 * blocksz as u64)
        }
        Err(e) => Err(anyhow!("Failed to read filesystem capacity: {:?}", e)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use utils::id::NodeId;

    #[test]
    fn test_build_node_registeration_request() {
        // Test that:
        // 1. We always extract the host name and port used to register with the HCC from the
        //    `advertise_pg_addr` if it is set.
        // 2. The correct ports are extracted from `advertise_pg_addr` and `listen_http_addr`.
        let mut conf = SafeKeeperConf::dummy();
        conf.my_id = NodeId(1);
        conf.advertise_pg_addr_tenant_only =
            Some("safe-keeper-1.safe-keeper.hadron.svc.cluster.local:5454".to_string());
        // `listen_pg_addr` and `listen_pg_addr_tenant_only` are not used for node registration. Set them to a different
        // host and port values and make sure that they don't show up in the node registration request.
        conf.listen_pg_addr = "0.0.0.0:5456".to_string();
        conf.listen_pg_addr_tenant_only = Some("0.0.0.0:5456".to_string());
        conf.listen_http_addr = "0.0.0.0:7676".to_string();
        let node_ip_addr: Option<IpAddr> = Some("127.0.0.1".parse().unwrap());

        let request = build_node_registeration_request(&conf, node_ip_addr).unwrap();
        assert_eq!(request.node_id, NodeId(1));
        assert_eq!(
            request.listen_pg_addr,
            "safe-keeper-1.safe-keeper.hadron.svc.cluster.local"
        );
        assert_eq!(request.listen_pg_port, 5454);
        assert_eq!(
            request.listen_http_addr,
            "safe-keeper-1.safe-keeper.hadron.svc.cluster.local"
        );
        assert_eq!(request.listen_http_port, 7676);
        assert_eq!(
            request.node_ip_addr,
            Some(IpAddr::V4("127.0.0.1".parse().unwrap()))
        );
    }
}
