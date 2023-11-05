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
use std::collections::HashMap;
use std::time::Duration;
use utils::{
    generation::Generation,
    id::{TenantId, TimelineId},
    lsn::Lsn,
};

/// Given an attached pageserver, retrieve the LSN for all timelines
fn get_lsns(
    tenant_id: TenantId,
    pageserver: &PageServerNode,
) -> anyhow::Result<HashMap<TimelineId, Lsn>> {
    let timelines = pageserver.timeline_list(&tenant_id)?;
    Ok(timelines
        .into_iter()
        .map(|t| (t.timeline_id, t.last_record_lsn))
        .collect())
}

/// Wait for the timeline LSNs on `pageserver` to catch up with or overtake
/// `baseline`.
fn await_lsn(
    tenant_id: TenantId,
    pageserver: &PageServerNode,
    baseline: HashMap<TimelineId, Lsn>,
) -> anyhow::Result<()> {
    loop {
        let latest = match get_lsns(tenant_id, pageserver) {
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
pub fn migrate_tenant(
    env: &LocalEnv,
    tenant_id: TenantId,
    dest_ps: PageServerNode,
) -> anyhow::Result<()> {
    // Get a new generation
    let attachment_service = AttachmentService::from_env(env);

    let previous = attachment_service.inspect(tenant_id)?;
    let mut baseline_lsns = None;
    if let Some((generation, origin_ps_id)) = &previous {
        let origin_ps = PageServerNode::from_env(env, env.get_pageserver_conf(*origin_ps_id)?);

        if origin_ps_id == &dest_ps.conf.id {
            println!("🔁 Already attached to {origin_ps_id}, freshening...");
            let gen = attachment_service.attach_hook(tenant_id, dest_ps.conf.id)?;
            let dest_conf = LocationConfig {
                shard_count: 0,
                shard_number: 0,
                shard_stripe_size: 0,
                mode: LocationConfigMode::AttachedSingle,
                generation: gen.map(Generation::new),
                secondary_conf: None,
                tenant_conf: TenantConfig::default(),
            };
            dest_ps.location_config(tenant_id, dest_conf)?;
            println!("✅ Migration complete");
            return Ok(());
        }

        println!("🔁 Switching origin pageserver {origin_ps_id} to stale mode");

        let stale_conf = LocationConfig {
            shard_count: 0,
            shard_number: 0,
            shard_stripe_size: 0,
            mode: LocationConfigMode::AttachedStale,
            generation: Some(Generation::new(*generation)),
            secondary_conf: None,
            tenant_conf: TenantConfig::default(),
        };
        origin_ps.location_config(tenant_id, stale_conf)?;

        baseline_lsns = Some(get_lsns(tenant_id, &origin_ps)?);
    }

    let gen = attachment_service.attach_hook(tenant_id, dest_ps.conf.id)?;
    let dest_conf = LocationConfig {
        shard_count: 0,
        shard_number: 0,
        shard_stripe_size: 0,
        mode: LocationConfigMode::AttachedMulti,
        generation: gen.map(Generation::new),
        secondary_conf: None,
        tenant_conf: TenantConfig::default(),
    };

    println!("🔁 Attaching to pageserver {}", dest_ps.conf.id);
    dest_ps.location_config(tenant_id, dest_conf)?;

    if let Some(baseline) = baseline_lsns {
        println!("🕑 Waiting for LSN to catch up...");
        await_lsn(tenant_id, &dest_ps, baseline)?;
    }

    let cplane = ComputeControlPlane::load(env.clone())?;
    for (endpoint_name, endpoint) in &cplane.endpoints {
        if endpoint.tenant_id == tenant_id {
            println!(
                "🔁 Reconfiguring endpoint {} to use pageserver {}",
                endpoint_name, dest_ps.conf.id
            );
            endpoint.reconfigure(Some(dest_ps.conf.id))?;
        }
    }

    for other_ps_conf in &env.pageservers {
        if other_ps_conf.id == dest_ps.conf.id {
            continue;
        }

        let other_ps = PageServerNode::from_env(env, other_ps_conf);
        let other_ps_tenants = other_ps.tenant_list()?;

        // Check if this tenant is attached
        let found = other_ps_tenants
            .into_iter()
            .map(|t| t.id)
            .any(|i| i == tenant_id);
        if !found {
            continue;
        }

        // Downgrade to a secondary location
        let secondary_conf = LocationConfig {
            shard_count: 0,
            shard_number: 0,
            shard_stripe_size: 0,
            mode: LocationConfigMode::Secondary,
            generation: None,
            secondary_conf: Some(LocationConfigSecondary { warm: true }),
            tenant_conf: TenantConfig::default(),
        };

        println!(
            "💤 Switching to secondary mode on pageserver {}",
            other_ps.conf.id
        );
        other_ps.location_config(tenant_id, secondary_conf)?;
    }

    println!(
        "🔁 Switching to AttachedSingle mode on pageserver {}",
        dest_ps.conf.id
    );
    let dest_conf = LocationConfig {
        shard_count: 0,
        shard_number: 0,
        shard_stripe_size: 0,
        mode: LocationConfigMode::AttachedSingle,
        generation: gen.map(Generation::new),
        secondary_conf: None,
        tenant_conf: TenantConfig::default(),
    };
    dest_ps.location_config(tenant_id, dest_conf)?;

    println!("✅ Migration complete");

    Ok(())
}
