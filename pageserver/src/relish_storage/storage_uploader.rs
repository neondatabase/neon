use std::{
    collections::VecDeque,
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
    thread,
};

use zenith_utils::zid::ZTimelineId;

use crate::{relish_storage::RelishStorage, RelishStorageConfig};

use super::{local_fs::LocalFs, rust_s3::RustS3};

pub struct QueueBasedRelishUploader {
    upload_queue: Arc<Mutex<VecDeque<(ZTimelineId, PathBuf)>>>,
}

impl QueueBasedRelishUploader {
    pub fn new(
        config: &RelishStorageConfig,
        page_server_workdir: &'static Path,
    ) -> anyhow::Result<Self> {
        let upload_queue = Arc::new(Mutex::new(VecDeque::new()));
        let _handle = match config {
            RelishStorageConfig::LocalFs(root) => {
                let relish_storage = LocalFs::new(root.clone())?;
                create_upload_thread(
                    Arc::clone(&upload_queue),
                    relish_storage,
                    page_server_workdir,
                )?
            }
            RelishStorageConfig::AwsS3(s3_config) => {
                let relish_storage = RustS3::new(s3_config)?;
                create_upload_thread(
                    Arc::clone(&upload_queue),
                    relish_storage,
                    page_server_workdir,
                )?
            }
        };

        Ok(Self { upload_queue })
    }

    pub fn schedule_upload(&self, timeline_id: ZTimelineId, relish_path: PathBuf) {
        self.upload_queue
            .lock()
            .unwrap()
            .push_back((timeline_id, relish_path))
    }
}

fn create_upload_thread<P, S: 'static + RelishStorage<RelishStoragePath = P>>(
    upload_queue: Arc<Mutex<VecDeque<(ZTimelineId, PathBuf)>>>,
    relish_storage: S,
    page_server_workdir: &'static Path,
) -> std::io::Result<thread::JoinHandle<()>> {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    thread::Builder::new()
        .name("Queue based relish uploader".to_string())
        .spawn(move || loop {
            runtime.block_on(async {
                upload_loop_step(&upload_queue, &relish_storage, page_server_workdir).await;
            })
        })
}

async fn upload_loop_step<P, S: 'static + RelishStorage<RelishStoragePath = P>>(
    upload_queue: &Mutex<VecDeque<(ZTimelineId, PathBuf)>>,
    relish_storage: &S,
    page_server_workdir: &Path,
) {
    let mut queue_accessor = upload_queue.lock().unwrap();
    log::debug!("current upload queue length: {}", queue_accessor.len());
    let next_upload = queue_accessor.pop_front();
    drop(queue_accessor);

    let (relish_timeline_id, relish_local_path) = match next_upload {
        Some(data) => data,
        None => {
            // Don't spin and allow others to use the queue.
            // In future, could be improved to be more clever about delays depending on relish upload stats
            thread::sleep(std::time::Duration::from_secs(1));
            return;
        }
    };

    if let Err(e) = upload_relish(relish_storage, page_server_workdir, &relish_local_path).await {
        log::error!(
            "Failed to upload relish '{}' for timeline {}, reason: {}",
            relish_local_path.display(),
            relish_timeline_id,
            e
        );
        upload_queue
            .lock()
            .unwrap()
            .push_back((relish_timeline_id, relish_local_path))
    } else {
        log::debug!("Relish successfully uploaded");
    }
}

async fn upload_relish<P, S: RelishStorage<RelishStoragePath = P>>(
    relish_storage: &S,
    page_server_workdir: &Path,
    relish_local_path: &Path,
) -> anyhow::Result<()> {
    let destination = S::derive_destination(page_server_workdir, relish_local_path)?;
    relish_storage
        .upload_relish(relish_local_path, &destination)
        .await
}
