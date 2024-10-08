//! Import a PGDATA directory into a root timeline.
//!
//!

use std::sync::Arc;

use anyhow::{bail, ensure, Context};
use bytes::Bytes;

use itertools::Itertools;
use pageserver_api::{
    key::{rel_block_to_key, rel_dir_to_key, rel_size_to_key, relmap_file_key, DBDIR_KEY},
    reltag::RelTag,
    shard::ShardIdentity,
};
use postgres_ffi::{pg_constants, relfile_utils::parse_relfilename, ControlFileData, BLCKSZ};
use tokio::{io::AsyncRead, task::JoinSet};
use tokio_epoll_uring::BoundedBuf;
use tokio_util::sync::CancellationToken;
use tracing::debug;

use crate::pgdatadir_mapping::{SlruSegmentDirectory, TwoPhaseDirectory};
use crate::{
    assert_u64_eq_usize::U64IsUsize,
    context::{DownloadBehavior, RequestContext},
    pgdatadir_mapping::{DbDirectory, RelDirectory},
    task_mgr::TaskKind,
    tenant::storage_layer::ImageLayerWriter,
};
use tokio::io::AsyncReadExt;

use pageserver_api::key::Key;
use pageserver_api::key::{
    slru_block_to_key, slru_dir_to_key, slru_segment_size_to_key, CHECKPOINT_KEY, CONTROLFILE_KEY,
    TWOPHASEDIR_KEY,
};
use pageserver_api::keyspace::singleton_range;
use pageserver_api::keyspace::{contiguous_range_len, is_contiguous_range};
use pageserver_api::reltag::SlruKind;
use utils::bin_ser::BeSer;
use utils::lsn::Lsn;

use std::collections::HashSet;
use std::ops::Range;

use super::{uninit::UninitializedTimeline, Timeline};

use remote_storage::{
    Download, GenericRemoteStorage, Listing, ListingObject, RemotePath, RemoteStorage,
};

pub(crate) struct Prepared {
    pgdata_dir: RemotePath,
    control_file: ControlFileData,
    storage: RemoteStorageWrapper,
}

/// Prepare for importing a PGDATA dump from remote storage.
///
/// # Arguments
///
/// * `storage` - The remote storage containing the PGDATA dump.
/// * `pgdata_dir` - The RemotePath prefix inside the storage leading to the root of the PGDATA dump.
/// * `ctx` - The request context.
/// * `cancel` - A cancellation token for the operation.
///
/// # Returns
///
/// Returns a `Prepared` struct containing information about the PGDATA dump.
pub(crate) async fn prepare(
    storage: GenericRemoteStorage,
    pgdata_dir: RemotePath,
    ctx: &RequestContext,
    cancel: &CancellationToken,
) -> anyhow::Result<Prepared> {
    let storage_wrapper = RemoteStorageWrapper::new(storage, cancel.clone());

    let controlfile_path = pgdata_dir.join("global/pg_control");
    let controlfile_buf = storage_wrapper.get(&controlfile_path).await?;
    let control_file = ControlFileData::decode(&controlfile_buf)?;

    let prepared = Prepared {
        pgdata_dir,
        control_file,
        storage: storage_wrapper,
    };
    prepared.try_pg_version()?;
    Ok(prepared)
}

impl Prepared {
    pub(crate) fn base_lsn(&self) -> Lsn {
        Lsn(self.control_file.checkPoint).align()
    }
    pub(crate) fn pg_version(&self) -> u32 {
        self.try_pg_version()
            .expect("prepare() checks that try_pg_version doesn't error")
    }
    fn try_pg_version(&self) -> anyhow::Result<u32> {
        Ok(match self.control_file.catalog_version_no {
            // thesea are from catversion.h
            202107181 => 14,
            202209061 => 15,
            202307071 => 16,
            /* XXX pg17 */
            catversion => {
                bail!("unrecognized catalog version {catversion}")
            }
        })
    }
}

pub async fn doit(
    uninit_timeline: &UninitializedTimeline<'_>,
    prepared: Prepared,
    ctx: &RequestContext,
    cancel: &CancellationToken,
) -> anyhow::Result<()> {
    let raw_timeline = uninit_timeline.raw_timeline()?;
    // ensure prepare() + doit() were used correctly
    assert_eq!(raw_timeline.pg_version, prepared.pg_version());
    assert_eq!(raw_timeline.ancestor_lsn, Lsn(0));
    assert!(raw_timeline.ancestor_timeline.is_none());

    let pgdata_lsn = prepared.base_lsn();
    let Prepared {
        pgdata_dir,
        control_file,
        storage,
    } = prepared;
    PgImportEnv {
        timeline: raw_timeline.clone(),
        pgdata_dir,
        control_file,
        pgdata_lsn,
        tasks: Vec::new(),
        storage,
    }
    .doit(ctx)
    .await
}

// TODO: rename to `State`
struct PgImportEnv {
    timeline: Arc<Timeline>,
    pgdata_dir: RemotePath,
    pgdata_lsn: Lsn,
    control_file: ControlFileData,
    tasks: Vec<AnyImportTask>,
    storage: RemoteStorageWrapper,
}

impl PgImportEnv {
    async fn doit(mut self, ctx: &RequestContext) -> anyhow::Result<()> {
        // Read control file
        let controlfile_path = self.pgdata_dir.join("global").join("pg_control");
        let controlfile_buf = self.storage.get(&controlfile_path).await?;

        let pgdata_lsn = Lsn(self.control_file.checkPoint).align();

        self.pgdata_lsn = pgdata_lsn;

        let datadir = PgDataDir::new(&self.storage, &self.pgdata_dir).await?;

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

        // pg_xact (01:00 keyspace)
        self.import_slru(SlruKind::Clog, &self.pgdata_dir.join("pg_xact"))
            .await?;
        // pg_multixact/members (01:01 keyspace)
        self.import_slru(
            SlruKind::MultiXactMembers,
            &self.pgdata_dir.join("pg_multixact/members"),
        )
        .await?;
        // pg_multixact/offsets (01:02 keyspace)
        self.import_slru(
            SlruKind::MultiXactOffsets,
            &self.pgdata_dir.join("pg_multixact/offsets"),
        )
        .await?;

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
                Bytes::from(controlfile_buf),
            )));

        let checkpoint_buf = self.control_file.checkPointCopy.encode()?;
        self.tasks
            .push(AnyImportTask::SingleKey(ImportSingleKeyTask::new(
                CHECKPOINT_KEY,
                checkpoint_buf,
            )));

        // Assigns parts of key space to later parallel jobs
        let mut last_end_key = Key::MIN;
        let mut current_chunk = Vec::new();
        let mut current_chunk_size: usize = 0;
        let mut parallel_jobs = Vec::new();
        for task in std::mem::take(&mut self.tasks).into_iter() {
            if current_chunk_size + task.total_size() > 1024 * 1024 * 1024 {
                let key_range = last_end_key..task.key_range().start;
                parallel_jobs.push(ChunkProcessingJob::new(
                    key_range.clone(),
                    std::mem::take(&mut current_chunk),
                    &self,
                ));
                last_end_key = key_range.end;
                current_chunk_size = 0;
            }
            current_chunk_size += task.total_size();
            current_chunk.push(task);
        }
        parallel_jobs.push(ChunkProcessingJob::new(
            last_end_key..Key::NON_L0_MAX,
            current_chunk,
            &self,
        ));

        // Start all jobs simultaneosly
        let mut work = JoinSet::new();
        // TODO: semaphore?
        for job in parallel_jobs {
            let ctx: RequestContext =
                ctx.detached_child(TaskKind::ImportPgdata, DownloadBehavior::Error);
            work.spawn(async move { job.run(&ctx).await });
        }
        let mut results = Vec::new();
        while let Some(result) = work.join_next().await {
            results.push(result);
        }

        if results.iter().all(|r| r.is_ok()) {
            Ok(())
        } else {
            let mut msg = String::new();
            for result in results {
                if let Err(err) = result {
                    msg.push_str(&format!("{err:?}\n\n"));
                }
            }
            bail!("Some parallel jobs failed:\n\n{msg}");
        }
    }

    async fn import_db(&mut self, db: &PgDataDirDb) -> anyhow::Result<()> {
        debug!(
            "Importing database (path={}, tablespace={}, dboid={})",
            db.path, db.spcnode, db.dboid
        );

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
            let len = file.filesize as usize;
            ensure!(len % 8192 == 0);
            let start_blk: u32 = file.segno * (1024 * 1024 * 1024 / 8192);
            let start_key = rel_block_to_key(file.rel_tag, start_blk);
            let end_key = rel_block_to_key(file.rel_tag, start_blk + (len / 8192) as u32);
            self.tasks
                .push(AnyImportTask::RelBlocks(ImportRelBlocksTask::new(
                    *self.timeline.get_shard_identity(),
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
        let segments = self.storage.listdir(path).await?;
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
            self.tasks
                .push(AnyImportTask::SlruBlocks(ImportSlruBlocksTask::new(
                    *self.timeline.get_shard_identity(),
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
    async fn new(
        storage: &RemoteStorageWrapper,
        datadir_path: &RemotePath,
    ) -> anyhow::Result<Self> {
        // Import ordinary databases, DEFAULTTABLESPACE_OID is smaller than GLOBALTABLESPACE_OID, so import them first
        // Traverse database in increasing oid order

        let basedir = &datadir_path.join("base");
        let db_oids = storage
            .listdir(basedir)
            .await?
            .into_iter()
            .filter_map(|(path, len)| path.object_name().and_then(|name| name.parse::<u32>().ok()))
            .sorted();
        let mut databases = Vec::new();
        for dboid in db_oids {
            databases.push(
                PgDataDirDb::new(
                    storage,
                    &basedir.join(dboid.to_string()),
                    pg_constants::DEFAULTTABLESPACE_OID,
                    dboid,
                    datadir_path,
                )
                .await?,
            );
        }

        // special case for global catalogs
        databases.push(
            PgDataDirDb::new(
                storage,
                &datadir_path.join("global"),
                postgres_ffi::pg_constants::GLOBALTABLESPACE_OID,
                0,
                datadir_path,
            )
            .await?,
        );

        databases.sort_by_key(|db| (db.spcnode, db.dboid));

        Ok(Self { dbs: databases })
    }
}

impl PgDataDirDb {
    async fn new(
        storage: &RemoteStorageWrapper,
        db_path: &RemotePath,
        spcnode: u32,
        dboid: u32,
        datadir_path: &RemotePath,
    ) -> anyhow::Result<Self> {
        let mut files: Vec<PgDataDirDbFile> = storage
            .listdir(db_path)
            .await?
            .into_iter()
            .filter_map(|(path, size)| {
                path.object_name().and_then(|name| {
                    // returns (relnode, forknum, segno)
                    parse_relfilename(&name).ok().map(|x| (size, x))
                })
            })
            .sorted()
            .map(|(filesize, (relnode, forknum, segno))| {
                let rel_tag = RelTag {
                    spcnode,
                    dbnode: dboid,
                    relnode,
                    forknum,
                };

                let path = datadir_path.join(rel_tag.to_segfile_name(segno));
                assert!(filesize % BLCKSZ as usize == 0); // TODO: this should result in an error
                let nblocks = filesize / BLCKSZ as usize;

                PgDataDirDbFile {
                    path,
                    filesize,
                    rel_tag,
                    segno,
                    nblocks: Some(nblocks), // first non-cummulative sizes
                }
            })
            .collect();

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

async fn read_all_bytes(reader: &mut (impl AsyncRead + Unpin)) -> anyhow::Result<Bytes> {
    let mut buf: Vec<u8> = vec![];
    reader.read_to_end(&mut buf).await?;
    Ok(Bytes::from(buf))
}

trait ImportTask {
    fn key_range(&self) -> Range<Key>;

    fn total_size(&self) -> usize {
        // TODO: revisit this
        if is_contiguous_range(&self.key_range()) {
            contiguous_range_len(&self.key_range()) as usize * 8192
        } else {
            u32::MAX as usize
        }
    }

    async fn doit(
        self,
        layer_writer: &mut ImageLayerWriter,
        ctx: &RequestContext,
    ) -> anyhow::Result<usize>;
}

struct ImportSingleKeyTask {
    key: Key,
    buf: Bytes,
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

    async fn doit(
        self,
        layer_writer: &mut ImageLayerWriter,
        ctx: &RequestContext,
    ) -> anyhow::Result<usize> {
        debug!("Importing relation file {}", self.path);
        let buf = self.storage.get(&self.path).await?;

        let (rel_tag, start_blk) = self.key_range.start.to_rel_block()?;
        let (_rel_tag, end_blk) = self.key_range.end.to_rel_block()?;
        let mut blknum = start_blk; /* XXX below code assumes start_blk is 0, that's probably wrong/ */
        let mut nimages = 0;
        let mut file_offset: u64 = 0;
        while blknum < end_blk {
            let key = rel_block_to_key(rel_tag, blknum);
            if self.shard_identity.is_key_disposable(&key) {
                // Skip blocks that are not in the shard
            } else {
                let buf = &buf[file_offset as usize..(file_offset + 8192) as usize];
                layer_writer
                    .put_image(key, Bytes::copy_from_slice(buf), ctx)
                    .await?;
                nimages += 1;
            }
            blknum += 1;
            file_offset += 8192;
        }
        Ok(nimages)
    }
}

struct ImportSlruBlocksTask {
    shard_identity: ShardIdentity,
    key_range: Range<Key>,
    path: RemotePath,
    storage: RemoteStorageWrapper,
}

impl ImportSlruBlocksTask {
    fn new(
        shard_identity: ShardIdentity,
        key_range: Range<Key>,
        path: &RemotePath,
        storage: RemoteStorageWrapper,
    ) -> Self {
        ImportSlruBlocksTask {
            shard_identity,
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
        ctx: &RequestContext,
    ) -> anyhow::Result<usize> {
        debug!("Importing SLRU segment file {}", self.path);
        let buf = self.storage.get(&self.path).await?;

        let (kind, segno, start_blk) = self.key_range.start.to_slru_block()?;
        let (_kind, _segno, end_blk) = self.key_range.end.to_slru_block()?;
        let mut blknum = start_blk;
        let mut nimages = 0;
        while blknum < end_blk {
            let key = slru_block_to_key(kind, segno, blknum);
            assert!(
                !self.shard_identity.is_key_disposable(&key),
                "SLRU keys need to go into every shard"
            );
            let buf = &buf[(blknum * 8192) as usize..((blknum + 1) * 8192) as usize];
            layer_writer
                .put_image(key, Bytes::copy_from_slice(buf), ctx)
                .await?;
            blknum += 1;
            nimages += 1;
        }
        Ok(nimages)
    }
}

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
        ctx: &RequestContext,
    ) -> anyhow::Result<usize> {
        match self {
            Self::SingleKey(t) => t.doit(layer_writer, ctx).await,
            Self::RelBlocks(t) => t.doit(layer_writer, ctx).await,
            Self::SlruBlocks(t) => t.doit(layer_writer, ctx).await,
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

struct ChunkProcessingJob {
    timeline: Arc<Timeline>,
    range: Range<Key>,
    tasks: Vec<AnyImportTask>,

    pgdata_lsn: Lsn,
}

impl ChunkProcessingJob {
    fn new(range: Range<Key>, tasks: Vec<AnyImportTask>, env: &PgImportEnv) -> Self {
        assert!(env.pgdata_lsn.is_valid());
        Self {
            timeline: env.timeline.clone(),
            range,
            tasks,
            pgdata_lsn: env.pgdata_lsn,
        }
    }

    async fn run(self, ctx: &RequestContext) -> anyhow::Result<()> {
        let mut writer = ImageLayerWriter::new(
            self.timeline.conf,
            self.timeline.timeline_id,
            self.timeline.tenant_shard_id,
            &self.range,
            self.pgdata_lsn,
            ctx,
        )
        .await?;

        let mut nimages = 0;
        for task in self.tasks {
            nimages += task.doit(&mut writer, ctx).await?;
        }

        let resident_layer = if nimages > 0 {
            writer.finish(&self.timeline, ctx).await?
        } else {
            // dropping the writer cleans up
            return Ok(());
        };

        // this is sharing the same code as create_image_layers
        let mut guard = self.timeline.layers.write().await;
        guard
            .open_mut()?
            .track_new_image_layers(&[resident_layer.clone()], &self.timeline.metrics);
        super::drop_wlock(guard);

        // Schedule the layer for upload but don't add barriers such as
        // wait for completion or index upload, so we don't inhibit upload parallelism.
        // TODO: limit upload parallelism somehow (e.g. by limiting concurrency of jobs?)
        // TODO: or regulate parallelism by upload queue depth? Prob should happen at a higher level.
        self.timeline
            .remote_client
            .schedule_layer_file_upload(resident_layer)?;

        Ok(())
    }
}

#[derive(Clone)]
struct RemoteStorageWrapper {
    storage: GenericRemoteStorage,
    cancel: CancellationToken,
}

impl RemoteStorageWrapper {
    fn new(storage: GenericRemoteStorage, cancel: CancellationToken) -> Self {
        Self { storage, cancel }
    }

    async fn listdir(&self, prefix: &RemotePath) -> anyhow::Result<Vec<(RemotePath, usize)>> {
        let Listing { keys, prefixes: _ } = self
            .storage
            .list(
                Some(prefix),
                remote_storage::ListingMode::WithDelimiter,
                None,
                &self.cancel,
            )
            .await?;
        Ok(keys
            .into_iter()
            .map(|ListingObject { key, size, .. }| (key, size.into_usize()))
            .collect())
    }

    async fn get(&self, path: &RemotePath) -> anyhow::Result<Bytes> {
        let Download {
            mut download_stream,
            ..
        } = self.storage.download(path, &self.cancel).await?;

        let mut reader = tokio_util::io::StreamReader::new(download_stream);

        // XXX optimize this, can we get the capacity hint from somewhere?
        let mut buf = Vec::new();
        tokio::io::copy_buf(&mut reader, &mut buf).await?;
        Ok(Bytes::from(buf))
    }
}
