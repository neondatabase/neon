use std::fs::metadata;

use anyhow::{bail, ensure, Context};
use bytes::Bytes;
use camino::{Utf8Path, Utf8PathBuf};

use itertools::Itertools;
use pageserver_api::{key::{rel_block_to_key, rel_dir_to_key, rel_size_to_key, relmap_file_key, DBDIR_KEY}, reltag::RelTag};
use postgres_ffi::{pg_constants, relfile_utils::parse_relfilename, ControlFileData, BLCKSZ};
use tokio::{io::AsyncRead, task::{self, JoinHandle}};
use tracing::debug;
use utils::{id::{NodeId, TenantId, TimelineId}, shard::{ShardCount, ShardNumber, TenantShardId}};
use walkdir::WalkDir;

use crate::{context::{DownloadBehavior, RequestContext}, pgdatadir_mapping::{DbDirectory, RelDirectory}, task_mgr::TaskKind, tenant::storage_layer::ImageLayerWriter};
use crate::pgdatadir_mapping::{SlruSegmentDirectory, TwoPhaseDirectory};
use crate::config::PageServerConf;
use tokio::io::AsyncReadExt;

use crate::tenant::storage_layer::PersistentLayerDesc;
use utils::generation::Generation;
use utils::lsn::Lsn;
use crate::tenant::IndexPart;
use crate::tenant::metadata::TimelineMetadata;
use crate::tenant::remote_timeline_client;
use crate::tenant::remote_timeline_client::LayerFileMetadata;
use pageserver_api::shard::ShardIndex;
use pageserver_api::key::Key;
use pageserver_api::keyspace::{is_contiguous_range, contiguous_range_len};
use pageserver_api::keyspace::singleton_range;
use pageserver_api::reltag::SlruKind;
use pageserver_api::key::{slru_block_to_key, slru_dir_to_key, slru_segment_size_to_key, TWOPHASEDIR_KEY, CONTROLFILE_KEY, CHECKPOINT_KEY};
use utils::bin_ser::BeSer;

use std::collections::HashSet;
use std::ops::Range;

pub struct PgImportEnv {
    conf: &'static PageServerConf,
    tli: TimelineId,
    tsi: TenantShardId,

    pgdata_lsn: Lsn,

    tasks: Vec<AnyImportTask>,

    layers: Vec<PersistentLayerDesc>,
}

impl PgImportEnv {

    pub async fn init(dstdir: &Utf8Path, tenant_id: TenantId, timeline_id: TimelineId) -> anyhow::Result<PgImportEnv> {
        let config = toml_edit::Document::new();
        let conf = PageServerConf::parse_and_validate(
            NodeId(42), 
            &config,
            dstdir
        )?;
        let conf = Box::leak(Box::new(conf));

        let tsi = TenantShardId {
            tenant_id,
            shard_number: ShardNumber(0),
            shard_count: ShardCount(0),
        };

        Ok(PgImportEnv {
            conf, 
            tli: timeline_id,
            tsi,
            pgdata_lsn: Lsn(0), // Will be filled in later, when the control file is imported

            tasks: Vec::new(),
            layers: Vec::new(),
        })
    }

    pub async fn import_datadir(&mut self, pgdata_path: &Utf8PathBuf) -> anyhow::Result<()> {
        // Read control file
        let controlfile_path = pgdata_path.join("global").join("pg_control");
        let controlfile_buf = std::fs::read(&controlfile_path)
            .with_context(|| format!("reading controlfile: {controlfile_path}"))?;
        let control_file = ControlFileData::decode(&controlfile_buf)?;

        let pgdata_lsn = Lsn(control_file.checkPoint).align();
        let timeline_path = self.conf.timeline_path(&self.tsi, &self.tli);

        println!("Importing {pgdata_path} to {timeline_path} as lsn {pgdata_lsn}...");
        self.pgdata_lsn = pgdata_lsn;

        let datadir = PgDataDir::new(pgdata_path);

        // Import dbdir (00:00:00 keyspace)
        // This is just constructed here, but will be written to the image layer in the first call to import_db()
        let dbdir_buf = Bytes::from(DbDirectory::ser(&DbDirectory {
            dbdirs: datadir.dbs.iter().map(|db| ((db.spcnode, db.dboid), true)).collect(),
        })?);
        self.tasks.push(ImportSingleKeyTask::new(DBDIR_KEY, dbdir_buf).into());

        // Import databases (00:spcnode:dbnode keyspace for each db)
        for db in datadir.dbs {
            self.import_db(&db).await?;
        }

        // Import SLRUs

        // pg_xact (01:00 keyspace)
        self.import_slru(SlruKind::Clog, &pgdata_path.join("pg_xact")).await?;
        // pg_multixact/members (01:01 keyspace)
        self.import_slru(SlruKind::MultiXactMembers, &pgdata_path.join("pg_multixact/members")).await?;
        // pg_multixact/offsets (01:02 keyspace)
        self.import_slru(SlruKind::MultiXactOffsets, &pgdata_path.join("pg_multixact/offsets")).await?;

        // Import pg_twophase.
        // TODO: as empty
        let twophasedir_buf = TwoPhaseDirectory::ser(
            &TwoPhaseDirectory { xids: HashSet::new() }
        )?;
        self.tasks.push(AnyImportTask::SingleKey(ImportSingleKeyTask::new(TWOPHASEDIR_KEY, Bytes::from(twophasedir_buf))));

        // Controlfile, checkpoint
        self.tasks.push(AnyImportTask::SingleKey(ImportSingleKeyTask::new(CONTROLFILE_KEY, Bytes::from(controlfile_buf))));

        let checkpoint_buf = control_file.checkPointCopy.encode()?;
        self.tasks.push(AnyImportTask::SingleKey(ImportSingleKeyTask::new(CHECKPOINT_KEY, checkpoint_buf)));

        // Assigns parts of key space to later parallel jobs
        let mut last_end_key = Key::MIN;
        let mut current_chunk = Vec::new();
        let mut current_chunk_size: usize = 0;
        let mut parallel_jobs = Vec::new();
        for task in std::mem::take(&mut self.tasks).into_iter() {
            if current_chunk_size + task.total_size() > 1024*1024*1024 {
                let key_range = last_end_key..task.key_range().start;
                parallel_jobs.push(ChunkProcessingJob::new(
                    key_range.clone(),
                    std::mem::take(&mut current_chunk),
                    self
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
            self
        ));

        // Start all jobs simultaneosly
        // TODO: semaphore?
        let mut handles = vec![];
        for job in parallel_jobs {
            let handle: JoinHandle<anyhow::Result<PersistentLayerDesc>> = task::spawn(async move {
                let layerdesc = job.run().await?;
                Ok(layerdesc)
            });
            handles.push(handle);
        }

        // Wait for all jobs to complete
        for handle in handles {
            let layerdesc = handle.await??;
            self.layers.push(layerdesc);
        }

        // Create index_part.json file
        self.create_index_part(&control_file).await?;

        Ok(())
    }

    async fn import_db(
        &mut self,
        db: &PgDataDirDb,
    ) -> anyhow::Result<()> {
        debug!(
            "Importing database (path={}, tablespace={}, dboid={})",
            db.path, db.spcnode, db.dboid
        );

        // Import relmap (00:spcnode:dbnode:00:*:00)
        let relmap_key = relmap_file_key(db.spcnode, db.dboid);
        debug!("Constructing relmap entry, key {relmap_key}");
        let mut relmap_file = tokio::fs::File::open(&db.path.join("pg_filenode.map")).await?;
        let relmap_buf = read_all_bytes(&mut relmap_file).await?;
        self.tasks.push(AnyImportTask::SingleKey(ImportSingleKeyTask::new(relmap_key, relmap_buf)));

        // Import reldir (00:spcnode:dbnode:00:*:01)
        let reldir_key = rel_dir_to_key(db.spcnode, db.dboid);
        debug!("Constructing reldirs entry, key {reldir_key}");
        let reldir_buf = RelDirectory::ser(&RelDirectory {
            rels: db.files.iter().map(|f| (f.rel_tag.relnode, f.rel_tag.forknum)).collect(),
        })?;
        self.tasks.push(AnyImportTask::SingleKey(ImportSingleKeyTask::new(reldir_key, Bytes::from(reldir_buf))));

        // Import data (00:spcnode:dbnode:reloid:fork:blk) and set sizes for each last
        // segment in a given relation (00:spcnode:dbnode:reloid:fork:ff)
        for file in &db.files {
            let len = metadata(&file.path)?.len() as usize;
            ensure!(len % 8192 == 0);
            let start_blk: u32 = file.segno * (1024 * 1024 * 1024 / 8192);
            let start_key = rel_block_to_key(file.rel_tag, start_blk);
            let end_key = rel_block_to_key(file.rel_tag, start_blk + (len / 8192) as u32);
            self.tasks.push(AnyImportTask::RelBlocks(ImportRelBlocksTask::new(start_key..end_key, &file.path)));

            // Set relsize for the last segment (00:spcnode:dbnode:reloid:fork:ff)
            if let Some(nblocks) = file.nblocks {
                let size_key = rel_size_to_key(file.rel_tag);
                //debug!("Setting relation size (path={path}, rel_tag={rel_tag}, segno={segno}) to {nblocks}, key {size_key}");
                let buf = nblocks.to_le_bytes();
                self.tasks.push(AnyImportTask::SingleKey(ImportSingleKeyTask::new(size_key, Bytes::from(buf.to_vec()))));
            }
        }

        Ok(())
    }

    async fn import_slru(
        &mut self,
        kind: SlruKind,
        path: &Utf8PathBuf,
    ) -> anyhow::Result<()> {
        let segments: Vec<(String, u32)> = WalkDir::new(path)
            .max_depth(1)
            .into_iter()
            .filter_map(|entry| {
                let entry = entry.ok()?;
                let filename = entry.file_name();
                let filename = filename.to_string_lossy();
                let segno = u32::from_str_radix(&filename, 16).ok()?;
                Some((filename.to_string(), segno))
            }).collect();

        // Write SlruDir
        let slrudir_key = slru_dir_to_key(kind);
        let segnos: HashSet<u32> = segments.iter().map(|(_path, segno)| { *segno }).collect();
        let slrudir = SlruSegmentDirectory {
            segments: segnos,
        };
        let slrudir_buf = SlruSegmentDirectory::ser(&slrudir)?;
        self.tasks.push(AnyImportTask::SingleKey(ImportSingleKeyTask::new(slrudir_key, Bytes::from(slrudir_buf))));

        for (segpath, segno) in segments {
            // SlruSegBlocks for each segment
            let p = path.join(Utf8PathBuf::from(segpath));
            let file_size = std::fs::metadata(&p)?.len();
            ensure!(file_size % 8192 == 0);
            let nblocks = u32::try_from(file_size / 8192)?;
            let start_key = slru_block_to_key(kind, segno, 0);
            let end_key = slru_block_to_key(kind, segno, nblocks);
            self.tasks.push(AnyImportTask::SlruBlocks(ImportSlruBlocksTask::new(start_key..end_key, &p)));

            // Followed by SlruSegSize
            let segsize_key = slru_segment_size_to_key(kind, segno);
            let segsize_buf = nblocks.to_le_bytes();
            self.tasks.push(AnyImportTask::SingleKey(ImportSingleKeyTask::new(segsize_key, Bytes::copy_from_slice(&segsize_buf))));
        }
        Ok(())
    }

    async fn create_index_part(&mut self, control_file: &ControlFileData) -> anyhow::Result<()> {
        let dstdir = &self.conf.workdir;

        let pg_version = match control_file.catalog_version_no {
            // thesea are from catversion.h
            202107181 => 14,
            202209061 => 15,
            202307071 => 16,
            catversion => { bail!("unrecognized catalog version {catversion}")},
        };

        let metadata = TimelineMetadata::new(
            // FIXME: The 'disk_consistent_lsn' should be the LSN at the *end* of the
            // checkpoint record, and prev_record_lsn should point to its beginning.
            // We should read the real end of the record from the WAL, but here we
            // just fake it.
            Lsn(self.pgdata_lsn.0 + 8),
            Some(self.pgdata_lsn),
            None, // no ancestor
            Lsn(0),
            self.pgdata_lsn,  // latest_gc_cutoff_lsn
            self.pgdata_lsn,  // initdb_lsn
            pg_version,
        );
        let generation = Generation::none();
        let mut index_part = IndexPart::empty(metadata);

        for l in self.layers.iter() {
            let name = l.layer_name();
            let metadata = LayerFileMetadata::new(l.file_size, generation, ShardIndex::unsharded());
            if let Some(_) = index_part.layer_metadata.insert(name.clone(), metadata) {
                bail!("duplicate layer filename {name}");
            }
        }

        let data = index_part.to_s3_bytes()?;
        let path = remote_timeline_client::remote_index_path(&self.tsi, &self.tli, generation);
        let path = dstdir.join(path.get_path());
        std::fs::write(&path, data)
            .context("could not write {path}")?;

        Ok(())
    }
}

//
// dbdir iteration tools
//

struct PgDataDir {
    pub dbs: Vec<PgDataDirDb> // spcnode, dboid, path
}

struct PgDataDirDb {
    pub spcnode: u32,
    pub dboid: u32,
    pub path: Utf8PathBuf,
    pub files: Vec<PgDataDirDbFile>
}

struct PgDataDirDbFile {
    pub path: Utf8PathBuf,
    pub rel_tag: RelTag,
    pub segno: u32,

    // Cummulative size of the given fork, set only for the last segment of that fork
    pub nblocks: Option<usize>,
}

impl PgDataDir {
    fn new(datadir_path: &Utf8PathBuf) -> Self {
        // Import ordinary databases, DEFAULTTABLESPACE_OID is smaller than GLOBALTABLESPACE_OID, so import them first
        // Traverse database in increasing oid order
        let mut databases = WalkDir::new(datadir_path.join("base"))
            .max_depth(1)
            .into_iter()
            .filter_map(|entry| {
                entry.ok().and_then(|path| {
                    path.file_name().to_string_lossy().parse::<u32>().ok()
                })
            })
            .sorted()
            .map(|dboid| {
                PgDataDirDb::new(
                    datadir_path.join("base").join(dboid.to_string()),
                    pg_constants::DEFAULTTABLESPACE_OID,
                    dboid,
                    datadir_path
                )
            })
            .collect::<Vec<_>>();

        // special case for global catalogs
        databases.push(PgDataDirDb::new(
            datadir_path.join("global"),
            postgres_ffi::pg_constants::GLOBALTABLESPACE_OID,
            0,
            datadir_path,
        ));

        databases.sort_by_key(|db| (db.spcnode, db.dboid));

        Self {
            dbs: databases
        }
    }
}

impl PgDataDirDb {
    fn new(db_path: Utf8PathBuf, spcnode: u32, dboid: u32, datadir_path: &Utf8PathBuf) -> Self {
        let mut files: Vec<PgDataDirDbFile> = WalkDir::new(&db_path)
            .min_depth(1)
            .max_depth(2)
            .into_iter()
            .filter_map(|entry| {
                entry.ok().and_then(|path| {
                    let relfile = path.file_name().to_string_lossy();
                    // returns (relnode, forknum, segno)
                    parse_relfilename(&relfile).ok()
                })
            })
            .sorted()
            .map(|(relnode, forknum, segno)| {
                let rel_tag = RelTag {
                    spcnode,
                    dbnode: dboid,
                    relnode,
                    forknum,
                };

                let path = datadir_path.join(rel_tag.to_segfile_name(segno));
                let len = metadata(&path).unwrap().len() as usize;
                assert!(len % BLCKSZ as usize == 0);
                let nblocks = len / BLCKSZ as usize;

                PgDataDirDbFile {
                    path,
                    rel_tag,
                    segno,
                    nblocks: Some(nblocks), // first non-cummulative sizes
                }
            })
            .collect();

        // Set cummulative sizes. Do all of that math here, so that later we could easier
        // parallelize over segments and know with which segments we need to write relsize
        // entry.
        let mut cumulative_nblocks: usize= 0;
        let mut prev_rel_tag: Option<RelTag> = None;
        for i in 0..files.len() {
            if prev_rel_tag == Some(files[i].rel_tag) {
                cumulative_nblocks += files[i].nblocks.unwrap();
            } else {
                cumulative_nblocks = files[i].nblocks.unwrap();
            }

            files[i].nblocks = if i == files.len() - 1 || files[i+1].rel_tag != files[i].rel_tag {
                Some(cumulative_nblocks)
            } else {
                None
            };

            prev_rel_tag = Some(files[i].rel_tag);
        }


        PgDataDirDb {
            files,
            path: db_path,
            spcnode,
            dboid,
        }
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
        if is_contiguous_range(&self.key_range()) {
            contiguous_range_len(&self.key_range()) as usize * 8192
        } else {
            u32::MAX as usize
        }
    }

    async fn doit(self, layer_writer: &mut ImageLayerWriter, ctx: &RequestContext) -> anyhow::Result<()>;
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

    async fn doit(self, layer_writer: &mut ImageLayerWriter, ctx: &RequestContext) -> anyhow::Result<()> {
        layer_writer.put_image(self.key, self.buf, ctx).await?;
        Ok(())
    }
}

struct ImportRelBlocksTask {
    key_range: Range<Key>,
    path: Utf8PathBuf,
}

impl ImportRelBlocksTask {
    fn new(key_range: Range<Key>, path: &Utf8Path) -> Self {
        ImportRelBlocksTask {
            key_range,
            path: path.into()
        }
    }
}

impl ImportTask for ImportRelBlocksTask {
    fn key_range(&self) -> Range<Key> {
        self.key_range.clone()
    }

    async fn doit(self, layer_writer: &mut ImageLayerWriter, ctx: &RequestContext) -> anyhow::Result<()> {
        debug!("Importing relation file {}", self.path);
        let mut reader = tokio::fs::File::open(&self.path).await?;
        let mut buf: [u8; 8192] = [0u8; 8192];

        let (rel_tag, start_blk) = self.key_range.start.to_rel_block()?;
        let (_rel_tag, end_blk) = self.key_range.end.to_rel_block()?;
        let mut blknum = start_blk;
        while blknum < end_blk {
            reader.read_exact(&mut buf).await?;
            let key = rel_block_to_key(rel_tag.clone(), blknum);
            layer_writer.put_image(key, Bytes::copy_from_slice(&buf), ctx).await?;
            blknum += 1;
        }
        Ok(())
    }
}

struct ImportSlruBlocksTask {
    key_range: Range<Key>,
    path: Utf8PathBuf,
}

impl ImportSlruBlocksTask {
    fn new(key_range: Range<Key>, path: &Utf8Path) -> Self {
        ImportSlruBlocksTask {
            key_range,
            path: path.into()
        }
    }
}

impl ImportTask for ImportSlruBlocksTask {
    fn key_range(&self) -> Range<Key> {
        self.key_range.clone()
    }

    async fn doit(self, layer_writer: &mut ImageLayerWriter, ctx: &RequestContext) -> anyhow::Result<()> {
        debug!("Importing SLRU segment file {}", self.path);
        let mut reader = tokio::fs::File::open(&self.path).await
            .context(format!("opening {}", &self.path))?;
        let mut buf: [u8; 8192] = [0u8; 8192];

        let (kind, segno, start_blk) = self.key_range.start.to_slru_block()?;
        let (_kind, _segno, end_blk) = self.key_range.end.to_slru_block()?;
        let mut blknum = start_blk;
        while blknum < end_blk {
            reader.read_exact(&mut buf).await?;
            let key = slru_block_to_key(kind, segno, blknum);
            layer_writer.put_image(key, Bytes::copy_from_slice(&buf), ctx).await?;
            blknum += 1;
        }
        Ok(())
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
            Self::SlruBlocks(t) => t.key_range()
        }
    }
    async fn doit(self, layer_writer: &mut ImageLayerWriter, ctx: &RequestContext) -> anyhow::Result<()> {
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
    range: Range<Key>,
    tasks: Vec<AnyImportTask>,

    dstdir: Utf8PathBuf,
    tenant_id: TenantId,
    timeline_id: TimelineId,
    pgdata_lsn: Lsn,
}

impl ChunkProcessingJob {
    fn new(range: Range<Key>, tasks: Vec<AnyImportTask>, env: &PgImportEnv) -> Self {
        assert!(env.pgdata_lsn.is_valid());
        Self {
            range,
            tasks,
            dstdir: env.conf.workdir.clone(),
            tenant_id: env.tsi.tenant_id,
            timeline_id: env.tli,
            pgdata_lsn: env.pgdata_lsn,
        }
    }

    async fn run(self) -> anyhow::Result<PersistentLayerDesc> {
        let ctx: RequestContext = RequestContext::new(TaskKind::DebugTool, DownloadBehavior::Error);
        let config = toml_edit::Document::new();
        let conf: &'static PageServerConf = Box::leak(Box::new(PageServerConf::parse_and_validate(
            NodeId(42),
            &config,
            &self.dstdir
        )?));
        let tsi = TenantShardId {
            tenant_id: self.tenant_id,
            shard_number: ShardNumber(0),
            shard_count: ShardCount(0),
        };

        let mut layer = ImageLayerWriter::new(
            &conf,
            self.timeline_id,
            tsi,
            &self.range,
            self.pgdata_lsn,
            &ctx,
        ).await?;

        for task in self.tasks {
            task.doit(&mut layer, &ctx).await?;
        }

        let layerdesc = layer.finish_raw(&ctx).await?;
        Ok(layerdesc)
    }
}
