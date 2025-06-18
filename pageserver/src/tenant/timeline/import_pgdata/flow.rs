//! Import a PGDATA directory into an empty root timeline.
//!
//! This module is adapted hackathon code by Heikki and Stas.
//! Other code in the parent module was written by Christian as part of a customer PoC.
//!
//! The hackathon code was producing image layer files as a free-standing program.
//!
//! It has been modified to
//! - run inside a running Pageserver, within the proper lifecycles of Timeline -> Tenant(Shard)
//! - => sharding-awareness: produce image layers with only the data relevant for this shard
//! - => S3 as the source for the PGDATA instead of local filesystem
//!
//! TODOs before productionization:
//! - ChunkProcessingJob should cut up an ImportJob to hit exactly target image layer size.
//!
//! An incomplete set of TODOs from the Hackathon:
//! - version-specific CheckPointData (=> pgv abstraction, already exists for regular walingest)

use std::collections::HashSet;
use std::hash::{Hash, Hasher};
use std::num::NonZeroUsize;
use std::ops::Range;
use std::sync::Arc;

use anyhow::ensure;
use bytes::Bytes;
use futures::stream::FuturesOrdered;
use itertools::Itertools;
use pageserver_api::config::TimelineImportConfig;
use pageserver_api::key::{
    CHECKPOINT_KEY, CONTROLFILE_KEY, DBDIR_KEY, Key, TWOPHASEDIR_KEY, rel_block_to_key,
    rel_dir_to_key, rel_size_to_key, relmap_file_key, slru_block_to_key, slru_dir_to_key,
    slru_segment_size_to_key,
};
use pageserver_api::keyspace::{ShardedRange, singleton_range};
use pageserver_api::models::{ShardImportProgress, ShardImportProgressV1, ShardImportStatus};
use pageserver_api::reltag::{RelTag, SlruKind};
use pageserver_api::shard::ShardIdentity;
use postgres_ffi::BLCKSZ;
use postgres_ffi::relfile_utils::parse_relfilename;
use remote_storage::RemotePath;
use tokio::sync::Semaphore;
use tokio_stream::StreamExt;
use tracing::{debug, instrument};
use utils::bin_ser::BeSer;
use utils::lsn::Lsn;
use utils::pausable_failpoint;

use super::Timeline;
use super::importbucket_client::{ControlFile, RemoteStorageWrapper};
use crate::assert_u64_eq_usize::UsizeIsU64;
use crate::context::{DownloadBehavior, RequestContext};
use crate::controller_upcall_client::{StorageControllerUpcallApi, StorageControllerUpcallClient};
use crate::pgdatadir_mapping::{
    DbDirectory, RelDirectory, SlruSegmentDirectory, TwoPhaseDirectory,
};
use crate::task_mgr::TaskKind;
use crate::tenant::storage_layer::{AsLayerDesc, ImageLayerWriter, Layer};
use crate::tenant::timeline::layer_manager::LayerManagerLockHolder;

pub async fn run(
    timeline: Arc<Timeline>,
    control_file: ControlFile,
    storage: RemoteStorageWrapper,
    import_progress: Option<ShardImportProgress>,
    ctx: &RequestContext,
) -> anyhow::Result<()> {
    // Match how we run the import based on the progress version.
    // If there's no import progress, it means that this is a new import
    // and we can use whichever version we want.
    match import_progress {
        Some(ShardImportProgress::V1(progress)) => {
            run_v1(timeline, control_file, storage, Some(progress), ctx).await
        }
        None => run_v1(timeline, control_file, storage, None, ctx).await,
    }
}

async fn run_v1(
    timeline: Arc<Timeline>,
    control_file: ControlFile,
    storage: RemoteStorageWrapper,
    import_progress: Option<ShardImportProgressV1>,
    ctx: &RequestContext,
) -> anyhow::Result<()> {
    let planner = Planner {
        control_file,
        storage: storage.clone(),
        shard: timeline.shard_identity,
        tasks: Vec::default(),
    };

    // Use the job size limit encoded in the progress if we are resuming an import.
    // This ensures that imports have stable plans even if the pageserver config changes.
    let import_config = {
        match &import_progress {
            Some(progress) => {
                let base = &timeline.conf.timeline_import_config;
                TimelineImportConfig {
                    import_job_soft_size_limit: NonZeroUsize::new(progress.job_soft_size_limit)
                        .unwrap(),
                    import_job_concurrency: base.import_job_concurrency,
                    import_job_checkpoint_threshold: base.import_job_checkpoint_threshold,
                    import_job_max_byte_range_size: base.import_job_max_byte_range_size,
                }
            }
            None => timeline.conf.timeline_import_config.clone(),
        }
    };

    let plan = planner.plan(&import_config).await?;

    // Hash the plan and compare with the hash of the plan we got back from the storage controller.
    // If the two match, it means that the planning stage had the same output.
    //
    // This is not intended to be a cryptographically secure hash.
    const SEED: u64 = 42;
    let mut hasher = twox_hash::XxHash64::with_seed(SEED);
    plan.hash(&mut hasher);
    let plan_hash = hasher.finish();

    if let Some(progress) = &import_progress {
        // Handle collisions on jobs of unequal length
        if progress.jobs != plan.jobs.len() {
            anyhow::bail!("Import plan job length does not match storcon metadata")
        }

        if plan_hash != progress.import_plan_hash {
            anyhow::bail!("Import plan does not match storcon metadata");
        }
    }

    pausable_failpoint!("import-timeline-pre-execute-pausable");

    let jobs_count = import_progress.as_ref().map(|p| p.jobs);
    let start_from_job_idx = import_progress.map(|progress| progress.completed);

    tracing::info!(
        start_from_job_idx=?start_from_job_idx,
        jobs=?jobs_count,
        "Executing import plan"
    );

    plan.execute(timeline, start_from_job_idx, plan_hash, &import_config, ctx)
        .await
}

struct Planner {
    control_file: ControlFile,
    storage: RemoteStorageWrapper,
    shard: ShardIdentity,
    tasks: Vec<AnyImportTask>,
}

#[derive(Hash)]
struct Plan {
    jobs: Vec<ChunkProcessingJob>,
    // Included here such that it ends up in the hash for the plan
    shard: ShardIdentity,
}

impl Planner {
    /// Creates an import plan
    ///
    /// This function is and must remain pure: given the same input, it will generate the same import plan.
    async fn plan(mut self, import_config: &TimelineImportConfig) -> anyhow::Result<Plan> {
        let pgdata_lsn = Lsn(self.control_file.control_file_data().checkPoint).align();
        anyhow::ensure!(pgdata_lsn.is_valid());

        let datadir = PgDataDir::new(&self.storage).await?;

        // Import dbdir (00:00:00 keyspace)
        // This is just constructed here, but will be written to the image layer in the first call to import_db()
        let dbdir_buf = Bytes::from(DbDirectory::ser(&DbDirectory {
            dbdirs: datadir
                .dbs
                .iter()
                .map(|db| ((db.spcnode, db.dboid), true))
                .collect(),
        })?);
        self.tasks
            .push(ImportSingleKeyTask::new(DBDIR_KEY, dbdir_buf).into());

        // Import databases (00:spcnode:dbnode keyspace for each db)
        for db in datadir.dbs {
            self.import_db(&db).await?;
        }

        // Import SLRUs
        if self.shard.is_shard_zero() {
            // pg_xact (01:00 keyspace)
            self.import_slru(SlruKind::Clog, &self.storage.pgdata().join("pg_xact"))
                .await?;
            // pg_multixact/members (01:01 keyspace)
            self.import_slru(
                SlruKind::MultiXactMembers,
                &self.storage.pgdata().join("pg_multixact/members"),
            )
            .await?;
            // pg_multixact/offsets (01:02 keyspace)
            self.import_slru(
                SlruKind::MultiXactOffsets,
                &self.storage.pgdata().join("pg_multixact/offsets"),
            )
            .await?;
        }

        // Import pg_twophase.
        // TODO: as empty
        let twophasedir_buf = TwoPhaseDirectory::ser(&TwoPhaseDirectory {
            xids: HashSet::new(),
        })?;
        self.tasks
            .push(AnyImportTask::SingleKey(ImportSingleKeyTask::new(
                TWOPHASEDIR_KEY,
                Bytes::from(twophasedir_buf),
            )));

        // Controlfile, checkpoint
        self.tasks
            .push(AnyImportTask::SingleKey(ImportSingleKeyTask::new(
                CONTROLFILE_KEY,
                self.control_file.control_file_buf().clone(),
            )));

        let checkpoint_buf = self
            .control_file
            .control_file_data()
            .checkPointCopy
            .encode()?;
        self.tasks
            .push(AnyImportTask::SingleKey(ImportSingleKeyTask::new(
                CHECKPOINT_KEY,
                checkpoint_buf,
            )));

        // Sort the tasks by the key ranges they handle.
        // The plan being generated here needs to be stable across invocations
        // of this method.
        self.tasks.sort_by_key(|task| match task {
            AnyImportTask::SingleKey(key) => (key.key, key.key.next()),
            AnyImportTask::RelBlocks(rel_blocks) => {
                (rel_blocks.key_range.start, rel_blocks.key_range.end)
            }
            AnyImportTask::SlruBlocks(slru_blocks) => {
                (slru_blocks.key_range.start, slru_blocks.key_range.end)
            }
        });

        // Assigns parts of key space to later parallel jobs
        // Note: The image layers produced here may have gaps, meaning,
        //       there is not an image for each key in the layer's key range.
        //       The read path stops traversal at the first image layer, regardless
        //       of whether a base image has been found for a key or not.
        //       (Concept of sparse image layers doesn't exist.)
        //       This behavior is exactly right for the base image layers we're producing here.
        //       But, since no other place in the code currently produces image layers with gaps,
        //       it seems noteworthy.
        let mut last_end_key = Key::MIN;
        let mut current_chunk = Vec::new();
        let mut current_chunk_size: usize = 0;
        let mut jobs = Vec::new();
        for task in std::mem::take(&mut self.tasks).into_iter() {
            let task_size = task.total_size(&self.shard);
            let projected_chunk_size = current_chunk_size.saturating_add(task_size);
            if projected_chunk_size > import_config.import_job_soft_size_limit.into() {
                let key_range = last_end_key..task.key_range().start;
                jobs.push(ChunkProcessingJob::new(
                    key_range.clone(),
                    std::mem::take(&mut current_chunk),
                    pgdata_lsn,
                ));
                last_end_key = key_range.end;
                current_chunk_size = 0;
            }
            current_chunk_size = current_chunk_size.saturating_add(task_size);
            current_chunk.push(task);
        }
        jobs.push(ChunkProcessingJob::new(
            last_end_key..Key::MAX,
            current_chunk,
            pgdata_lsn,
        ));

        Ok(Plan {
            jobs,
            shard: self.shard,
        })
    }

    #[instrument(level = tracing::Level::DEBUG, skip_all, fields(dboid=%db.dboid, tablespace=%db.spcnode, path=%db.path))]
    async fn import_db(&mut self, db: &PgDataDirDb) -> anyhow::Result<()> {
        debug!("start");
        scopeguard::defer! {
            debug!("return");
        }

        // Import relmap (00:spcnode:dbnode:00:*:00)
        let relmap_key = relmap_file_key(db.spcnode, db.dboid);
        debug!("Constructing relmap entry, key {relmap_key}");
        let relmap_path = db.path.join("pg_filenode.map");
        let relmap_buf = self.storage.get(&relmap_path).await?;
        self.tasks
            .push(AnyImportTask::SingleKey(ImportSingleKeyTask::new(
                relmap_key, relmap_buf,
            )));

        // Import reldir (00:spcnode:dbnode:00:*:01)
        let reldir_key = rel_dir_to_key(db.spcnode, db.dboid);
        debug!("Constructing reldirs entry, key {reldir_key}");
        let reldir_buf = RelDirectory::ser(&RelDirectory {
            rels: db
                .files
                .iter()
                .map(|f| (f.rel_tag.relnode, f.rel_tag.forknum))
                .collect(),
        })?;
        self.tasks
            .push(AnyImportTask::SingleKey(ImportSingleKeyTask::new(
                reldir_key,
                Bytes::from(reldir_buf),
            )));

        // Import data (00:spcnode:dbnode:reloid:fork:blk) and set sizes for each last
        // segment in a given relation (00:spcnode:dbnode:reloid:fork:ff)
        for file in &db.files {
            debug!(%file.path, %file.filesize, "importing file");
            let len = file.filesize;
            ensure!(len % 8192 == 0);
            let start_blk: u32 = file.segno * (1024 * 1024 * 1024 / 8192);
            let start_key = rel_block_to_key(file.rel_tag, start_blk);
            let end_key = rel_block_to_key(file.rel_tag, start_blk + (len / 8192) as u32);
            self.tasks
                .push(AnyImportTask::RelBlocks(ImportRelBlocksTask::new(
                    self.shard,
                    start_key..end_key,
                    &file.path,
                    self.storage.clone(),
                )));

            // Set relsize for the last segment (00:spcnode:dbnode:reloid:fork:ff)
            if let Some(nblocks) = file.nblocks {
                let size_key = rel_size_to_key(file.rel_tag);
                //debug!("Setting relation size (path={path}, rel_tag={rel_tag}, segno={segno}) to {nblocks}, key {size_key}");
                let buf = nblocks.to_le_bytes();
                self.tasks
                    .push(AnyImportTask::SingleKey(ImportSingleKeyTask::new(
                        size_key,
                        Bytes::from(buf.to_vec()),
                    )));
            }
        }

        Ok(())
    }

    async fn import_slru(&mut self, kind: SlruKind, path: &RemotePath) -> anyhow::Result<()> {
        assert!(self.shard.is_shard_zero());

        let segments = self.storage.listfilesindir(path).await?;
        let segments: Vec<(String, u32, usize)> = segments
            .into_iter()
            .filter_map(|(path, size)| {
                let filename = path.object_name()?;
                let segno = u32::from_str_radix(filename, 16).ok()?;
                Some((filename.to_string(), segno, size))
            })
            .collect();

        // Write SlruDir
        let slrudir_key = slru_dir_to_key(kind);
        let segnos: HashSet<u32> = segments
            .iter()
            .map(|(_path, segno, _size)| *segno)
            .collect();
        let slrudir = SlruSegmentDirectory { segments: segnos };
        let slrudir_buf = SlruSegmentDirectory::ser(&slrudir)?;
        self.tasks
            .push(AnyImportTask::SingleKey(ImportSingleKeyTask::new(
                slrudir_key,
                Bytes::from(slrudir_buf),
            )));

        for (segpath, segno, size) in segments {
            // SlruSegBlocks for each segment
            let p = path.join(&segpath);
            let file_size = size;
            ensure!(file_size % 8192 == 0);
            let nblocks = u32::try_from(file_size / 8192)?;
            let start_key = slru_block_to_key(kind, segno, 0);
            let end_key = slru_block_to_key(kind, segno, nblocks);
            debug!(%p, segno=%segno, %size, %start_key, %end_key, "scheduling SLRU segment");
            self.tasks
                .push(AnyImportTask::SlruBlocks(ImportSlruBlocksTask::new(
                    start_key..end_key,
                    &p,
                    self.storage.clone(),
                )));

            // Followed by SlruSegSize
            let segsize_key = slru_segment_size_to_key(kind, segno);
            let segsize_buf = nblocks.to_le_bytes();
            self.tasks
                .push(AnyImportTask::SingleKey(ImportSingleKeyTask::new(
                    segsize_key,
                    Bytes::copy_from_slice(&segsize_buf),
                )));
        }
        Ok(())
    }
}

impl Plan {
    async fn execute(
        self,
        timeline: Arc<Timeline>,
        start_after_job_idx: Option<usize>,
        import_plan_hash: u64,
        import_config: &TimelineImportConfig,
        ctx: &RequestContext,
    ) -> anyhow::Result<()> {
        let storcon_client = StorageControllerUpcallClient::new(timeline.conf, &timeline.cancel);

        let mut work = FuturesOrdered::new();
        let semaphore = Arc::new(Semaphore::new(import_config.import_job_concurrency.into()));

        let jobs_in_plan = self.jobs.len();

        let mut jobs = self
            .jobs
            .into_iter()
            .enumerate()
            .map(|(idx, job)| (idx + 1, job))
            .filter(|(idx, _job)| {
                // Filter out any jobs that have been done already
                if let Some(start_after) = start_after_job_idx {
                    *idx > start_after
                } else {
                    true
                }
            })
            .peekable();

        let mut last_completed_job_idx = start_after_job_idx.unwrap_or(0);
        let checkpoint_every: usize = import_config.import_job_checkpoint_threshold.into();
        let max_byte_range_size: usize = import_config.import_job_max_byte_range_size.into();

        // Run import jobs concurrently up to the limit specified by the pageserver configuration.
        // Note that we process completed futures in the oreder of insertion. This will be the
        // building block for resuming imports across pageserver restarts or tenant migrations.
        while last_completed_job_idx < jobs_in_plan {
            tokio::select! {
                permit = semaphore.clone().acquire_owned(), if jobs.peek().is_some() => {
                    let permit = permit.expect("never closed");
                    let (job_idx, job) = jobs.next().expect("we peeked");

                    let job_timeline = timeline.clone();
                    let ctx = ctx.detached_child(TaskKind::ImportPgdata, DownloadBehavior::Error);

                    work.push_back(tokio::task::spawn(async move {
                        let _permit = permit;
                        let res = job.run(job_timeline, max_byte_range_size, &ctx).await;
                        (job_idx, res)
                    }));
                },
                maybe_complete_job_idx = work.next() => {
                    pausable_failpoint!("import-task-complete-pausable");

                    match maybe_complete_job_idx {
                        Some(Ok((job_idx, res))) => {
                            assert!(last_completed_job_idx.checked_add(1).unwrap() == job_idx);

                            res?;
                            last_completed_job_idx = job_idx;

                            if last_completed_job_idx % checkpoint_every == 0 {
                                tracing::info!(last_completed_job_idx, jobs=%jobs_in_plan, "Checkpointing import status");

                                let progress = ShardImportProgressV1 {
                                    jobs: jobs_in_plan,
                                    completed: last_completed_job_idx,
                                    import_plan_hash,
                                    job_soft_size_limit: import_config.import_job_soft_size_limit.into(),
                                };

                                timeline.remote_client.schedule_index_upload_for_file_changes()?;
                                timeline.remote_client.wait_completion().await?;

                                storcon_client.put_timeline_import_status(
                                    timeline.tenant_shard_id,
                                    timeline.timeline_id,
                                    timeline.generation,
                                    ShardImportStatus::InProgress(Some(ShardImportProgress::V1(progress)))
                                )
                                .await
                                .map_err(|_err| {
                                    anyhow::anyhow!("Shut down while putting timeline import status")
                                })?;
                            }
                        },
                        Some(Err(_)) => {
                            anyhow::bail!(
                                "import job panicked or cancelled"
                            );
                        }
                        None => {}
                    }
                }
            }
        }

        Ok(())
    }
}

//
// dbdir iteration tools
//

struct PgDataDir {
    pub dbs: Vec<PgDataDirDb>, // spcnode, dboid, path
}

struct PgDataDirDb {
    pub spcnode: u32,
    pub dboid: u32,
    pub path: RemotePath,
    pub files: Vec<PgDataDirDbFile>,
}

struct PgDataDirDbFile {
    pub path: RemotePath,
    pub rel_tag: RelTag,
    pub segno: u32,
    pub filesize: usize,
    // Cummulative size of the given fork, set only for the last segment of that fork
    pub nblocks: Option<usize>,
}

impl PgDataDir {
    async fn new(storage: &RemoteStorageWrapper) -> anyhow::Result<Self> {
        let datadir_path = storage.pgdata();
        // Import ordinary databases, DEFAULTTABLESPACE_OID is smaller than GLOBALTABLESPACE_OID, so import them first
        // Traverse database in increasing oid order

        let basedir = &datadir_path.join("base");
        let db_oids: Vec<_> = storage
            .listdir(basedir)
            .await?
            .into_iter()
            .filter_map(|path| path.object_name().and_then(|name| name.parse::<u32>().ok()))
            .sorted()
            .collect();
        debug!(?db_oids, "found databases");
        let mut databases = Vec::new();
        for dboid in db_oids {
            databases.push(
                PgDataDirDb::new(
                    storage,
                    &basedir.join(dboid.to_string()),
                    postgres_ffi_types::constants::DEFAULTTABLESPACE_OID,
                    dboid,
                    &datadir_path,
                )
                .await?,
            );
        }

        // special case for global catalogs
        databases.push(
            PgDataDirDb::new(
                storage,
                &datadir_path.join("global"),
                postgres_ffi_types::constants::GLOBALTABLESPACE_OID,
                0,
                &datadir_path,
            )
            .await?,
        );

        databases.sort_by_key(|db| (db.spcnode, db.dboid));

        Ok(Self { dbs: databases })
    }
}

impl PgDataDirDb {
    #[instrument(level = tracing::Level::DEBUG, skip_all, fields(%dboid, %db_path))]
    async fn new(
        storage: &RemoteStorageWrapper,
        db_path: &RemotePath,
        spcnode: u32,
        dboid: u32,
        datadir_path: &RemotePath,
    ) -> anyhow::Result<Self> {
        let mut files: Vec<PgDataDirDbFile> = storage
            .listfilesindir(db_path)
            .await?
            .into_iter()
            .filter_map(|(path, size)| {
                debug!(%path, %size, "found file in dbdir");
                path.object_name().and_then(|name| {
                    // returns (relnode, forknum, segno)
                    parse_relfilename(name).ok().map(|x| (size, x))
                })
            })
            .sorted_by_key(|(_, relfilename)| *relfilename)
            .map(|(filesize, (relnode, forknum, segno))| {
                let rel_tag = RelTag {
                    spcnode,
                    dbnode: dboid,
                    relnode,
                    forknum,
                };

                let path = datadir_path.join(rel_tag.to_segfile_name(segno));
                anyhow::ensure!(filesize % BLCKSZ as usize == 0);
                let nblocks = filesize / BLCKSZ as usize;

                Ok(PgDataDirDbFile {
                    path,
                    filesize,
                    rel_tag,
                    segno,
                    nblocks: Some(nblocks), // first non-cummulative sizes
                })
            })
            .collect::<anyhow::Result<_, _>>()?;

        // Set cummulative sizes. Do all of that math here, so that later we could easier
        // parallelize over segments and know with which segments we need to write relsize
        // entry.
        let mut cumulative_nblocks: usize = 0;
        let mut prev_rel_tag: Option<RelTag> = None;
        for i in 0..files.len() {
            if prev_rel_tag == Some(files[i].rel_tag) {
                cumulative_nblocks += files[i].nblocks.unwrap();
            } else {
                cumulative_nblocks = files[i].nblocks.unwrap();
            }

            files[i].nblocks = if i == files.len() - 1 || files[i + 1].rel_tag != files[i].rel_tag {
                Some(cumulative_nblocks)
            } else {
                None
            };

            prev_rel_tag = Some(files[i].rel_tag);
        }

        Ok(PgDataDirDb {
            files,
            path: db_path.clone(),
            spcnode,
            dboid,
        })
    }
}

trait ImportTask {
    fn key_range(&self) -> Range<Key>;

    fn total_size(&self, shard_identity: &ShardIdentity) -> usize {
        let range = ShardedRange::new(self.key_range(), shard_identity);
        let page_count = range.page_count();
        if page_count == u32::MAX {
            tracing::warn!(
                "Import task has non contiguous key range: {}..{}",
                self.key_range().start,
                self.key_range().end
            );

            // Tasks should operate on contiguous ranges. It is unexpected for
            // ranges to violate this assumption. Calling code handles this by mapping
            // any task on a non contiguous range to its own image layer.
            usize::MAX
        } else {
            page_count as usize * 8192
        }
    }

    async fn doit(
        self,
        layer_writer: &mut ImageLayerWriter,
        max_byte_range_size: usize,
        ctx: &RequestContext,
    ) -> anyhow::Result<usize>;
}

struct ImportSingleKeyTask {
    key: Key,
    buf: Bytes,
}

impl Hash for ImportSingleKeyTask {
    fn hash<H: Hasher>(&self, state: &mut H) {
        let ImportSingleKeyTask { key, buf } = self;

        key.hash(state);
        // The key value might not have a stable binary representation.
        // For instance, the db directory uses an unstable hash-map.
        // To work around this we are a bit lax here and only hash the
        // size of the buffer which must be consistent.
        buf.len().hash(state);
    }
}

impl ImportSingleKeyTask {
    fn new(key: Key, buf: Bytes) -> Self {
        ImportSingleKeyTask { key, buf }
    }
}

impl ImportTask for ImportSingleKeyTask {
    fn key_range(&self) -> Range<Key> {
        singleton_range(self.key)
    }

    async fn doit(
        self,
        layer_writer: &mut ImageLayerWriter,
        _max_byte_range_size: usize,
        ctx: &RequestContext,
    ) -> anyhow::Result<usize> {
        layer_writer.put_image(self.key, self.buf, ctx).await?;
        Ok(1)
    }
}

struct ImportRelBlocksTask {
    shard_identity: ShardIdentity,
    key_range: Range<Key>,
    path: RemotePath,
    storage: RemoteStorageWrapper,
}

impl Hash for ImportRelBlocksTask {
    fn hash<H: Hasher>(&self, state: &mut H) {
        let ImportRelBlocksTask {
            shard_identity: _,
            key_range,
            path,
            storage: _,
        } = self;

        key_range.hash(state);
        path.hash(state);
    }
}

impl ImportRelBlocksTask {
    fn new(
        shard_identity: ShardIdentity,
        key_range: Range<Key>,
        path: &RemotePath,
        storage: RemoteStorageWrapper,
    ) -> Self {
        ImportRelBlocksTask {
            shard_identity,
            key_range,
            path: path.clone(),
            storage,
        }
    }
}

impl ImportTask for ImportRelBlocksTask {
    fn key_range(&self) -> Range<Key> {
        self.key_range.clone()
    }

    #[instrument(level = tracing::Level::DEBUG, skip_all, fields(%self.path))]
    async fn doit(
        self,
        layer_writer: &mut ImageLayerWriter,
        max_byte_range_size: usize,
        ctx: &RequestContext,
    ) -> anyhow::Result<usize> {
        debug!("Importing relation file");

        let (rel_tag, start_blk) = self.key_range.start.to_rel_block()?;
        let (rel_tag_end, end_blk) = self.key_range.end.to_rel_block()?;
        assert_eq!(rel_tag, rel_tag_end);

        let ranges = (start_blk..end_blk)
            .enumerate()
            .filter_map(|(i, blknum)| {
                let key = rel_block_to_key(rel_tag, blknum);
                if self.shard_identity.is_key_disposable(&key) {
                    return None;
                }
                let file_offset = i.checked_mul(8192).unwrap();
                Some((
                    vec![key],
                    file_offset,
                    file_offset.checked_add(8192).unwrap(),
                ))
            })
            .coalesce(|(mut acc, acc_start, acc_end), (mut key, start, end)| {
                assert_eq!(key.len(), 1);
                assert!(!acc.is_empty());
                assert!(acc_end > acc_start);
                if acc_end == start && end - acc_start <= max_byte_range_size {
                    acc.push(key.pop().unwrap());
                    Ok((acc, acc_start, end))
                } else {
                    Err(((acc, acc_start, acc_end), (key, start, end)))
                }
            });

        let mut nimages = 0;
        for (keys, range_start, range_end) in ranges {
            let range_buf = self
                .storage
                .get_range(&self.path, range_start.into_u64(), range_end.into_u64())
                .await?;
            let mut buf = Bytes::from(range_buf);
            for key in keys {
                // The writer buffers writes internally
                let image = buf.split_to(8192);
                layer_writer.put_image(key, image, ctx).await?;
                nimages += 1;
            }
        }

        Ok(nimages)
    }
}

struct ImportSlruBlocksTask {
    key_range: Range<Key>,
    path: RemotePath,
    storage: RemoteStorageWrapper,
}

impl Hash for ImportSlruBlocksTask {
    fn hash<H: Hasher>(&self, state: &mut H) {
        let ImportSlruBlocksTask {
            key_range,
            path,
            storage: _,
        } = self;

        key_range.hash(state);
        path.hash(state);
    }
}

impl ImportSlruBlocksTask {
    fn new(key_range: Range<Key>, path: &RemotePath, storage: RemoteStorageWrapper) -> Self {
        ImportSlruBlocksTask {
            key_range,
            path: path.clone(),
            storage,
        }
    }
}

impl ImportTask for ImportSlruBlocksTask {
    fn key_range(&self) -> Range<Key> {
        self.key_range.clone()
    }

    async fn doit(
        self,
        layer_writer: &mut ImageLayerWriter,
        _max_byte_range_size: usize,
        ctx: &RequestContext,
    ) -> anyhow::Result<usize> {
        debug!("Importing SLRU segment file {}", self.path);
        let buf = self.storage.get(&self.path).await?;

        // TODO(vlad): Does timestamp to LSN work for imported timelines?
        // Probably not since we don't append the `xact_time` to it as in
        // [`WalIngest::ingest_xact_record`].
        let (kind, segno, start_blk) = self.key_range.start.to_slru_block()?;
        let (_kind, _segno, end_blk) = self.key_range.end.to_slru_block()?;
        let mut blknum = start_blk;
        let mut nimages = 0;
        let mut file_offset = 0;
        while blknum < end_blk {
            let key = slru_block_to_key(kind, segno, blknum);
            let buf = &buf[file_offset..(file_offset + 8192)];
            file_offset += 8192;
            layer_writer
                .put_image(key, Bytes::copy_from_slice(buf), ctx)
                .await?;
            nimages += 1;
            blknum += 1;
        }
        Ok(nimages)
    }
}

#[derive(Hash)]
enum AnyImportTask {
    SingleKey(ImportSingleKeyTask),
    RelBlocks(ImportRelBlocksTask),
    SlruBlocks(ImportSlruBlocksTask),
}

impl ImportTask for AnyImportTask {
    fn key_range(&self) -> Range<Key> {
        match self {
            Self::SingleKey(t) => t.key_range(),
            Self::RelBlocks(t) => t.key_range(),
            Self::SlruBlocks(t) => t.key_range(),
        }
    }
    /// returns the number of images put into the `layer_writer`
    async fn doit(
        self,
        layer_writer: &mut ImageLayerWriter,
        max_byte_range_size: usize,
        ctx: &RequestContext,
    ) -> anyhow::Result<usize> {
        match self {
            Self::SingleKey(t) => t.doit(layer_writer, max_byte_range_size, ctx).await,
            Self::RelBlocks(t) => t.doit(layer_writer, max_byte_range_size, ctx).await,
            Self::SlruBlocks(t) => t.doit(layer_writer, max_byte_range_size, ctx).await,
        }
    }
}

impl From<ImportSingleKeyTask> for AnyImportTask {
    fn from(t: ImportSingleKeyTask) -> Self {
        Self::SingleKey(t)
    }
}

impl From<ImportRelBlocksTask> for AnyImportTask {
    fn from(t: ImportRelBlocksTask) -> Self {
        Self::RelBlocks(t)
    }
}

impl From<ImportSlruBlocksTask> for AnyImportTask {
    fn from(t: ImportSlruBlocksTask) -> Self {
        Self::SlruBlocks(t)
    }
}

#[derive(Hash)]
struct ChunkProcessingJob {
    range: Range<Key>,
    tasks: Vec<AnyImportTask>,

    pgdata_lsn: Lsn,
}

impl ChunkProcessingJob {
    fn new(range: Range<Key>, tasks: Vec<AnyImportTask>, pgdata_lsn: Lsn) -> Self {
        assert!(pgdata_lsn.is_valid());
        Self {
            range,
            tasks,
            pgdata_lsn,
        }
    }

    async fn run(
        self,
        timeline: Arc<Timeline>,
        max_byte_range_size: usize,
        ctx: &RequestContext,
    ) -> anyhow::Result<()> {
        let mut writer = ImageLayerWriter::new(
            timeline.conf,
            timeline.timeline_id,
            timeline.tenant_shard_id,
            &self.range,
            self.pgdata_lsn,
            &timeline.gate,
            timeline.cancel.clone(),
            ctx,
        )
        .await?;

        let mut nimages = 0;
        for task in self.tasks {
            nimages += task.doit(&mut writer, max_byte_range_size, ctx).await?;
        }

        let resident_layer = if nimages > 0 {
            let (desc, path) = writer.finish(ctx).await?;

            {
                let guard = timeline
                    .layers
                    .read(LayerManagerLockHolder::ImportPgData)
                    .await;
                let existing_layer = guard.try_get_from_key(&desc.key());
                if let Some(layer) = existing_layer {
                    if layer.metadata().generation == timeline.generation {
                        return Err(anyhow::anyhow!(
                            "Import attempted to rewrite layer file in the same generation: {}",
                            layer.local_path()
                        ));
                    }
                }
            }

            Layer::finish_creating(timeline.conf, &timeline, desc, &path)?
        } else {
            // dropping the writer cleans up
            return Ok(());
        };

        // The same import job might run multiple times since not each job is checkpointed.
        // Hence, we must support the cases where the layer already exists. We cannot be
        // certain that the existing layer is identical to the new one, so in that case
        // we replace the old layer with the one we just generated.

        let mut guard = timeline
            .layers
            .write(LayerManagerLockHolder::ImportPgData)
            .await;

        let existing_layer = guard
            .try_get_from_key(&resident_layer.layer_desc().key())
            .cloned();
        match existing_layer {
            Some(existing) => {
                // Unlink the remote layer from the index without scheduling its deletion.
                // When `existing_layer` drops [`LayerInner::drop`] will schedule its deletion from
                // remote storage, but that assumes that the layer was unlinked from the index first.
                timeline
                    .remote_client
                    .schedule_unlinking_of_layers_from_index_part(std::iter::once(
                        existing.layer_desc().layer_name(),
                    ))?;

                guard.open_mut()?.rewrite_layers(
                    &[(existing.clone(), resident_layer.clone())],
                    &[],
                    &timeline.metrics,
                );
            }
            None => {
                guard
                    .open_mut()?
                    .track_new_image_layers(&[resident_layer.clone()], &timeline.metrics);
            }
        }

        crate::tenant::timeline::drop_layer_manager_wlock(guard);

        timeline
            .remote_client
            .schedule_layer_file_upload(resident_layer)?;

        Ok(())
    }
}
