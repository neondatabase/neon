use crate::tenant_mgr;
use crate::layered_repository::layer_map;
use crate::PageServerConf;

use anyhow::Result;
use lazy_static::lazy_static;
use tracing::*;

use std::collections::VecDeque;
use std::sync::Mutex;
use std::thread::JoinHandle;

use zenith_utils::zid::{ZTenantId, ZTimelineId};

lazy_static! {
    static ref JOB_QUEUE: Mutex<GlobalJobQueue> = Mutex::new(GlobalJobQueue::default());
}


#[derive(Default)]
struct GlobalJobQueue {
    jobs: VecDeque<GlobalJob>,
}

pub enum GlobalJob {
    // To release memory
    EvictSomeLayer,

    // To advance 'disk_consistent_lsn'
    CheckpointTimeline(ZTenantId, ZTimelineId),

    // To free up disk space
    GarbageCollect(ZTenantId),
}

pub fn schedule_job(job: GlobalJob) {
    let mut queue = JOB_QUEUE.lock().unwrap();
    queue.jobs.push_back(job);
}

///
/// Launch the global job handler thread
///
/// TODO: This ought to be a pool of threads
///
pub fn launch_global_job_thread(conf: &'static PageServerConf) -> JoinHandle<()> {
    std::thread::Builder::new()
        .name("Global Job thread".into())
        .spawn(move || {
            // FIXME: relaunch it? Panic is not good.
            
            global_job_loop(conf).expect("Global job thread died");
        })
        .unwrap()
}

pub fn global_job_loop(conf: &'static PageServerConf) -> Result<()> {
    while !tenant_mgr::shutdown_requested() {
        std::thread::sleep(conf.checkpoint_period);
        info!("global job thread waking up");

        let mut queue = JOB_QUEUE.lock().unwrap();
        while let Some(job) = queue.jobs.pop_front() {
            drop(queue);

            let result = match job {
                GlobalJob::EvictSomeLayer => {
                    evict_layer()
                },
                GlobalJob::CheckpointTimeline(tenantid, timelineid) => {
                    checkpoint_timeline(tenantid, timelineid)
                }
                GlobalJob::GarbageCollect(tenantid) => {
                    gc_tenant(tenantid)
                }
            };

            if let Err(err) = result {
                error!("job ended in error: {:#}", err);
            }

            queue = JOB_QUEUE.lock().unwrap();
        }
    }
    trace!("Checkpointer thread shut down");
    Ok(())
}

// Freeze and write out an in-memory layer
fn evict_layer() -> Result<()>
{
    // Pick a victim
    while let Some(layer_id) = layer_map::find_victim() {
        let victim_layer = match layer_map::get_layer(layer_id) {
            Some(l) => l,
            None => continue,
        };

        let tenantid = victim_layer.get_tenant_id();
        let timelineid = victim_layer.get_timeline_id();

        let _entered = info_span!("global evict", timeline = %timelineid, tenant = %tenantid)
            .entered();
        
        info!("evicting {}", victim_layer.filename().display());
        
        drop(victim_layer);

        let timeline = tenant_mgr::get_timeline_for_tenant(tenantid, timelineid)?;

        timeline.upgrade_to_layered_timeline().evict_layer(layer_id)?
    }
    info!("no more eviction needed");
    Ok(())
}

fn checkpoint_timeline(tenantid: ZTenantId, timelineid: ZTimelineId) -> Result<()> {
    let timeline = tenant_mgr::get_timeline_for_tenant(tenantid, timelineid)?;

    timeline.checkpoint_scheduled()
}

fn gc_tenant(tenantid: ZTenantId) -> Result<()> {
    let tenant = tenant_mgr::get_repository_for_tenant(tenantid)?;

    tenant.gc_scheduled()?;

    Ok(())
}
