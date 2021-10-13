use std::time::Duration;
use std::{collections::BinaryHeap, sync::Mutex, thread};

use crate::tenant_mgr;
use crate::{relish_storage::RelishStorage, PageServerConf};

lazy_static::lazy_static! {
    static ref UPLOAD_QUEUE: Mutex<BinaryHeap<SyncTask>> = Mutex::new(BinaryHeap::new());
}

pub fn schedule_timeline_upload(_local_timeline: ()) {
    // UPLOAD_QUEUE
    //     .lock()
    //     .unwrap()
    //     .push(SyncTask::Upload(local_timeline))
}

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord)]
enum SyncTask {}

pub fn run_storage_sync_thread<
    P: std::fmt::Debug,
    S: 'static + RelishStorage<RelishStoragePath = P>,
>(
    config: &'static PageServerConf,
    relish_storage: S,
    max_concurrent_sync: usize,
) -> anyhow::Result<Option<thread::JoinHandle<anyhow::Result<()>>>> {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;

    let handle = thread::Builder::new()
        .name("Queue based relish storage sync".to_string())
        .spawn(move || {
            while !tenant_mgr::pageserver_shutdown_requested() {
                let mut queue_accessor = UPLOAD_QUEUE.lock().unwrap();
                log::debug!("Upload queue length: {}", queue_accessor.len());
                let next_task = queue_accessor.pop();
                drop(queue_accessor);
                match next_task {
                    Some(task) => runtime.block_on(async {
                        // suppress warnings
                        let _ = (config, task, &relish_storage, max_concurrent_sync);
                        todo!("omitted for brevity")
                    }),
                    None => {
                        thread::sleep(Duration::from_secs(1));
                        continue;
                    }
                }
            }
            log::debug!("Queue based relish storage sync thread shut down");
            Ok(())
        })?;
    Ok(Some(handle))
}
