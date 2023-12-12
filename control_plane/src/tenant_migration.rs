//!
//! Functionality for migrating tenants across pageservers: unlike most of neon_local, this code
//! isn't scoped to a particular physical service, as it needs to update compute endpoints to
//! point to the new pageserver.
//!
use crate::local_env::LocalEnv;
use crate::{
    attachment_service::AttachmentService, endpoint::ComputeControlPlane,
    pageserver::PageServerNode,
};
use pageserver_api::models::{
    LocationConfig, LocationConfigMode, LocationConfigSecondary, TenantConfig,
};
use pageserver_api::shard::TenantShardId;
use std::collections::HashMap;
use std::time::Duration;
use utils::{id::TimelineId, lsn::Lsn};

/// Given an attached pageserver, retrieve the LSN for all timelines
async fn get_lsns(
    tenant_shard_id: TenantShardId,
    pageserver: &PageServerNode,
) -> anyhow::Result<HashMap<TimelineId, Lsn>> {
    let timelines = pageserver.timeline_list(&tenant_shard_id).await?;
    Ok(timelines
        .into_iter()
        .map(|t| (t.timeline_id, t.last_record_lsn))
        .collect())
}

/// Wait for the timeline LSNs on `pageserver` to catch up with or overtake
/// `baseline`.
async fn await_lsn(
    tenant_shard_id: TenantShardId,
    pageserver: &PageServerNode,
    baseline: HashMap<TimelineId, Lsn>,
) -> anyhow::Result<()> {
    loop {
        let latest = match get_lsns(tenant_shard_id, pageserver).await {
            Ok(l) => l,
            Err(e) => {
                println!(
                    "🕑 Can't get LSNs on pageserver {} yet, waiting ({e})",
                    pageserver.conf.id
                );
                std::thread::sleep(Duration::from_millis(500));
                continue;
            }
        };

        let mut any_behind: bool = false;
        for (timeline_id, baseline_lsn) in &baseline {
            match latest.get(timeline_id) {
                Some(latest_lsn) => {
                    println!("🕑 LSN origin {baseline_lsn} vs destination {latest_lsn}");
                    if latest_lsn < baseline_lsn {
                        any_behind = true;
                    }
                }
                None => {
                    // Expected timeline isn't yet visible on migration destination.
                    // (IRL we would have to account for timeline deletion, but this
                    //  is just test helper)
                    any_behind = true;
                }
            }
        }

        if !any_behind {
            println!("✅ LSN caught up.  Proceeding...");
            break;
        } else {
            std::thread::sleep(Duration::from_millis(500));
        }
    }

    Ok(())
}

/// This function spans multiple services, to demonstrate live migration of a tenant
/// between pageservers:
///  - Coordinate attach/secondary/detach on pageservers
///  - call into attachment_service for generations
///  - reconfigure compute endpoints to point to new attached pageserver
pub async fn migrate_tenant(
    env: &LocalEnv,
    tenant_shard_id: TenantShardId,
    dest_ps: PageServerNode,
) -> anyhow::Result<()> {
    // Get a new generation
    let attachment_service = AttachmentService::from_env(env);

    fn build_location_config(
        mode: LocationConfigMode,
        generation: Option<u32>,
        secondary_conf: Option<LocationConfigSecondary>,
    ) -> LocationConfig {
        LocationConfig {
            mode,
            generation,
            secondary_conf,
            tenant_conf: TenantConfig::default(),
            shard_number: 0,
            shard_count: 0,
            shard_stripe_size: 0,
        }
    }

    let previous = attachment_service.inspect(tenant_shard_id).await?;
    let mut baseline_lsns = None;
    if let Some((generation, origin_ps_id)) = &previous {
        let origin_ps = PageServerNode::from_env(env, env.get_pageserver_conf(*origin_ps_id)?);

        if origin_ps_id == &dest_ps.conf.id {
            println!("🔁 Already attached to {origin_ps_id}, freshening...");
            let gen = attachment_service
                .attach_hook(tenant_shard_id, dest_ps.conf.id)
                .await?;
            let dest_conf = build_location_config(LocationConfigMode::AttachedSingle, gen, None);
            dest_ps
                .location_config(tenant_shard_id, dest_conf, None)
                .await?;
            println!("✅ Migration complete");
            return Ok(());
        }

        println!("🔁 Switching origin pageserver {origin_ps_id} to stale mode");

        let stale_conf =
            build_location_config(LocationConfigMode::AttachedStale, Some(*generation), None);
        origin_ps
            .location_config(tenant_shard_id, stale_conf, Some(Duration::from_secs(10)))
            .await?;

        baseline_lsns = Some(get_lsns(tenant_shard_id, &origin_ps).await?);
    }

    let gen = attachment_service
        .attach_hook(tenant_shard_id, dest_ps.conf.id)
        .await?;
    let dest_conf = build_location_config(LocationConfigMode::AttachedMulti, gen, None);

    println!("🔁 Attaching to pageserver {}", dest_ps.conf.id);
    dest_ps
        .location_config(tenant_shard_id, dest_conf, None)
        .await?;

    if let Some(baseline) = baseline_lsns {
        println!("🕑 Waiting for LSN to catch up...");
        await_lsn(tenant_shard_id, &dest_ps, baseline).await?;
    }

    let cplane = ComputeControlPlane::load(env.clone())?;
    for (endpoint_name, endpoint) in &cplane.endpoints {
        if endpoint.tenant_id == tenant_shard_id.tenant_id {
            println!(
                "🔁 Reconfiguring endpoint {} to use pageserver {}",
                endpoint_name, dest_ps.conf.id
            );
            endpoint.reconfigure(vec![]).await?;
        }
    }

    for other_ps_conf in &env.pageservers {
        if other_ps_conf.id == dest_ps.conf.id {
            continue;
        }

        let other_ps = PageServerNode::from_env(env, other_ps_conf);
        let other_ps_tenants = other_ps.tenant_list().await?;

        // Check if this tenant is attached
        let found = other_ps_tenants
            .into_iter()
            .map(|t| t.id)
            .any(|i| i == tenant_shard_id);
        if !found {
            continue;
        }

        // Downgrade to a secondary location
        let secondary_conf = build_location_config(
            LocationConfigMode::Secondary,
            None,
            Some(LocationConfigSecondary { warm: true }),
        );

        println!(
            "💤 Switching to secondary mode on pageserver {}",
            other_ps.conf.id
        );
        other_ps
            .location_config(tenant_shard_id, secondary_conf, None)
            .await?;
    }

    println!(
        "🔁 Switching to AttachedSingle mode on pageserver {}",
        dest_ps.conf.id
    );
    let dest_conf = build_location_config(LocationConfigMode::AttachedSingle, gen, None);
    dest_ps
        .location_config(tenant_shard_id, dest_conf, None)
        .await?;

    println!("✅ Migration complete");

    Ok(())
}
