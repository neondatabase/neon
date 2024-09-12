use std::{fs::metadata, time::Instant};

use anyhow::{bail, ensure, Context};
use bytes::Bytes;
use camino::{Utf8Path, Utf8PathBuf};

use itertools::Itertools;
use pageserver_api::{key::{rel_block_to_key, rel_dir_to_key, rel_size_to_key, relmap_file_key, DBDIR_KEY}, reltag::RelTag};
use postgres_ffi::{pg_constants, relfile_utils::parse_relfilename, ControlFileData, BLCKSZ};
use tokio::io::AsyncRead;
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
use pageserver_api::reltag::SlruKind;
use pageserver_api::key::{slru_block_to_key, slru_dir_to_key, slru_segment_size_to_key, TWOPHASEDIR_KEY, CONTROLFILE_KEY, CHECKPOINT_KEY};
use pageserver_api::key::rel_key_range;
use utils::bin_ser::BeSer;

use std::collections::HashSet;

pub struct PgImportEnv {
    ctx: RequestContext,
    conf: &'static PageServerConf,
    tli: TimelineId,
    tsi: TenantShardId,

    pgdata_lsn: Lsn,

    layers: Vec<PersistentLayerDesc>,
}

impl PgImportEnv {

    pub async fn init(dstdir: &Utf8Path, tenant_id: TenantId, timeline_id: TimelineId) -> anyhow::Result<PgImportEnv> {
        let ctx: RequestContext = RequestContext::new(TaskKind::DebugTool, DownloadBehavior::Error);
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
            ctx,
            conf, 
            tli: timeline_id,
            tsi,
            pgdata_lsn: Lsn(0), // Will be filled in later, when the control file is imported

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
        let mut dbdir_buf = Some(Bytes::from(DbDirectory::ser(&DbDirectory {
            dbdirs: datadir.dbs.iter().map(|db| ((db.spcnode, db.dboid), true)).collect(),
        })?));
        // Import databases (00:spcnode:dbnode keyspace for each db)
        let mut start_key = Key::MIN;
        for db in datadir.dbs {
            start_key = self.import_db(start_key, &db, dbdir_buf.take()).await?;
        }

        let mut tail_layer = ImageLayerWriter::new(
            &self.conf,
            self.tli,
            self.tsi,
            &(start_key..Key::NON_L0_MAX),
            pgdata_lsn,
            &self.ctx,
        ).await?;

        // Import SLRUs

        // pg_xact (01:00 keyspace)
        self.import_slru(&mut tail_layer, SlruKind::Clog, &pgdata_path.join("pg_xact")).await?;
        // pg_multixact/members (01:01 keyspace)
        self.import_slru(&mut tail_layer, SlruKind::MultiXactMembers, &pgdata_path.join("pg_multixact/members")).await?;
        // pg_multixact/offsets (01:02 keyspace)
        self.import_slru(&mut tail_layer, SlruKind::MultiXactOffsets, &pgdata_path.join("pg_multixact/offsets")).await?;

        // Import pg_twophase.
        // TODO: as empty
        let twophasedir_buf = TwoPhaseDirectory::ser(
            &TwoPhaseDirectory { xids: HashSet::new() }
        )?;
        tail_layer.put_image(TWOPHASEDIR_KEY, Bytes::from(twophasedir_buf), &self.ctx).await?;

        // Controlfile, checkpoint
        tail_layer.put_image(CONTROLFILE_KEY, Bytes::from(controlfile_buf), &self.ctx).await?;

        let checkpoint_buf = control_file.checkPointCopy.encode()?;
        tail_layer.put_image(CHECKPOINT_KEY, checkpoint_buf, &self.ctx).await?;

        let layerdesc = tail_layer.finish_raw(&self.ctx).await?;
        self.layers.push(layerdesc);

        // should we anything about the wal?

        // Create index_part.json file
        self.create_index_part(&control_file).await?;

        Ok(())
    }

    async fn import_db(
        &mut self,
        start_key: Key,
        db: &PgDataDirDb,
        mut dbdir_buf: Option<Bytes>,
    ) -> anyhow::Result<Key> {
        debug!(
            "Importing database (path={}, tablespace={}, dboid={})",
            db.path, db.spcnode, db.dboid
        );

        // Import data (00:spcnode:dbnode:reloid:fork:blk) and set sizes for each last
        // segment in a given relation (00:spcnode:dbnode:reloid:fork:ff)

        let mut chunks: Vec<Vec<&PgDataDirDbFile>> = Vec::new();
        let mut accumulated_bytes: usize = 0;
        let mut current_chunk: Vec<&PgDataDirDbFile> = Vec::new();

        for file in &db.files {
            let real_size = if let Some(nblocks) = file.nblocks {
                nblocks * 8192
            } else {
                1024*1024*1024
            };
            if accumulated_bytes + real_size >= 512*1024*1024 {
                chunks.push(current_chunk);
                current_chunk = Vec::new();
                accumulated_bytes = 0;
            }
            current_chunk.push(file);
            accumulated_bytes += real_size;
        };
        if !current_chunk.is_empty() {
            chunks.push(current_chunk);
        }

        let mut last_key_end: Key = start_key;
        let mut first = true;
        for chunk in chunks {
            let key_start = last_key_end;
            let key_end = rel_key_range(chunk.last().unwrap().rel_tag).end;
            let mut layer_writer = ImageLayerWriter::new(
                &self.conf,
                self.tli,
                self.tsi,
                &(key_start..key_end),
                self.pgdata_lsn,
                &self.ctx,
            ).await?;

            if first {
                if let Some(dbdir_buf) = dbdir_buf.take() {
                    layer_writer.put_image(DBDIR_KEY, dbdir_buf, &self.ctx).await?;
                }

                // Import relmap (00:spcnode:dbnode:00:*:00)
                let relmap_key = relmap_file_key(db.spcnode, db.dboid);
                debug!("Constructing relmap entry, key {relmap_key}");
                let mut relmap_file = tokio::fs::File::open(&db.path.join("pg_filenode.map")).await?;
                let relmap_buf = read_all_bytes(&mut relmap_file).await?;
                layer_writer.put_image(relmap_key, relmap_buf, &self.ctx).await?;

                // Import reldir (00:spcnode:dbnode:00:*:01)
                let reldir_key = rel_dir_to_key(db.spcnode, db.dboid);
                debug!("Constructing reldirs entry, key {reldir_key}");
                let reldir_buf = RelDirectory::ser(&RelDirectory {
                    rels: db.files.iter().map(|f| (f.rel_tag.relnode, f.rel_tag.forknum)).collect(),
                })?;
                layer_writer.put_image(reldir_key, reldir_buf.into(), &self.ctx).await?;

                first = false;
            }

            for file in chunk {
                self.import_rel_file(&mut layer_writer, &file).await?;
            }
            last_key_end = key_end;

            let layerdesc = layer_writer.finish_raw(&self.ctx).await?;
            self.layers.push(layerdesc);
        }

        Ok(last_key_end)
    }

    async fn import_rel_file(
        &mut self,
        layer_writer: &mut ImageLayerWriter,
        segment: &PgDataDirDbFile,
    ) -> anyhow::Result<()> {
        let (path, rel_tag, segno) = (&segment.path, segment.rel_tag, segment.segno);

        debug!("Importing relation file (path={path}, rel_tag={rel_tag}, segno={segno})");
        let start = Instant::now();

        let mut reader = tokio::fs::File::open(&path).await?;
        let len = metadata(&path)?.len() as usize;

        let mut buf: [u8; 8192] = [0u8; 8192];

        ensure!(len % BLCKSZ as usize == 0);
        let nblocks = len / BLCKSZ as usize;

        let mut blknum: u32 = segno * (1024 * 1024 * 1024 / BLCKSZ as u32);

        loop {
            let r = reader.read_exact(&mut buf).await;
            match r {
                Ok(_) => {
                    let key = rel_block_to_key(rel_tag.clone(), blknum);
                    layer_writer.put_image(key, Bytes::copy_from_slice(&buf), &self.ctx).await?;
                }

                Err(err) => match err.kind() {
                    std::io::ErrorKind::UnexpectedEof => {
                        // reached EOF. That's expected.
                        let relative_blknum = blknum - segno * (1024 * 1024 * 1024 / BLCKSZ as u32);
                        ensure!(relative_blknum == nblocks as u32, "unexpected EOF");
                        break;
                    }
                    _ => {
                        bail!("error reading file {}: {:#}", path, err);
                    }
                },
            };
            blknum += 1;
        }

        debug!("Importing relation file (path={path}, rel_tag={rel_tag}, segno={segno}): done in {:.6} s", start.elapsed().as_secs_f64());

        // Set relsize for the last segment (00:spcnode:dbnode:reloid:fork:ff)
        if let Some(nblocks) = segment.nblocks {
            let size_key = rel_size_to_key(rel_tag);
            debug!("Setting relation size (path={path}, rel_tag={rel_tag}, segno={segno}) to {nblocks}, key {size_key}");
            let buf = nblocks.to_le_bytes();
            layer_writer.put_image(size_key, Bytes::from(buf.to_vec()), &self.ctx).await?;
        }

        Ok(())
    }

    async fn import_slru(
        &mut self,
        layer_writer: &mut ImageLayerWriter,
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
        layer_writer.put_image(slrudir_key, slrudir_buf.into(), &self.ctx).await?;

        for (segpath, segno) in segments {
            // SlruSegBlocks for each segment
            let p = path.join(Utf8PathBuf::from(segpath));
            let mut reader = tokio::fs::File::open(&p).await
                .context(format!("opening {}", &p))?;

            let mut rpageno = 0;
            loop {
                let mut buf: Vec<u8> = Vec::new();
                buf.resize(8192, 0);
                let r = reader.read_exact(&mut buf).await;
                match r {
                    Ok(_) => {},
                    Err(err) if err.kind() == std::io::ErrorKind::UnexpectedEof => {
                        // reached EOF. That's expected
                        break;
                    }
                    Err(err) => {
                        bail!("error reading file {}: {:#}", &p, err);
                    }
                };
                let slruseg_key = slru_block_to_key(kind, segno, rpageno);
                layer_writer.put_image(slruseg_key, Bytes::from(buf), &self.ctx).await?;
                rpageno += 1;
            }
            let npages: u32 = rpageno;

            // Followed by SlruSegSize
            let segsize_key = slru_segment_size_to_key(kind, segno);
            let segsize_buf = npages.to_le_bytes();
            layer_writer.put_image(segsize_key, Bytes::copy_from_slice(&segsize_buf), &self.ctx).await?;
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
