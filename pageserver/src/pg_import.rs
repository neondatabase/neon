use std::{collections::HashMap, fs::metadata, path::Path, str::FromStr};

use anyhow::{bail, ensure, Context};
use bytes::Bytes;
use camino::{Utf8Path, Utf8PathBuf};

use itertools::Itertools;
use pageserver_api::{key::{rel_block_to_key, DBDIR_KEY}, reltag::RelTag};
use postgres_ffi::{pg_constants, relfile_utils::parse_relfilename, ControlFileData, DBState_DB_SHUTDOWNED, Oid, BLCKSZ};
use tokio::io::AsyncRead;
use tracing::{debug, trace, warn};
use utils::{id::{NodeId, TenantId, TimelineId}, shard::{ShardCount, ShardNumber, TenantShardId}};
use walkdir::WalkDir;

use crate::{context::{DownloadBehavior, RequestContext}, import_datadir, pgdatadir_mapping::DbDirectory, task_mgr::TaskKind, tenant::storage_layer::ImageLayerWriter};
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
use utils::bin_ser::BeSer;

pub struct PgImportEnv {
    ctx: RequestContext,
    conf: &'static PageServerConf,
    tli: TimelineId,
    tsi: TenantShardId,

    pgdata_lsn: Lsn,
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
        })
    }

    pub async fn import_datadir(&mut self, pgdata_path: &Utf8PathBuf) -> anyhow::Result<()> {
        // Read control file
        let control_file = self.import_controlfile(pgdata_path).await?;
        let pgdata_lsn = Lsn(control_file.checkPoint).align();
        let timeline_path = self.conf.timeline_path(&self.tsi, &self.tli);

        println!("Importing {pgdata_path} to {timeline_path} as lsn {pgdata_lsn}...");
        self.pgdata_lsn = pgdata_lsn;

        let datadir = PgDataDir::new(pgdata_path);

        let range = Key::MIN..Key::NON_L0_MAX;
        let mut one_big_layer = ImageLayerWriter::new(
            &self.conf,
            self.tli,
            self.tsi,
            &range,
            pgdata_lsn,
            &self.ctx,
        ).await?;

        // // 1. DbDir; relmap files; reldir
        // self.import_dirs(&mut one_big_layer, &pgdata_path).await?;

        // let buf = DbDirectory::ser(&DbDirectory {
        //     dbdirs: HashMap::new(),
        // })?;
        // one_big_layer.put_image(DBDIR_KEY, buf.into(), &self.ctx).await?;

        // 4. Import data
        for db in datadir.dbs {
            self.import_db(&mut one_big_layer, &db).await?;
        }

        let layerdesc = one_big_layer.finish_layer(&self.ctx).await?;

        // should we anything about the wal?

        // Create index_part.json file
        self.create_index_part(&[layerdesc], &control_file).await?;

        Ok(())
    }

    // // Write necessary metadata about databases/relations. We store them as serialized hashmaps.
    // //
    // // 1. DbDir: (spcnode, dbnode) -> bool (do relmapper and PG_VERSION files exist)
    // // 2. Relmap file: (spcnode, dbnode) -> contents of `pg_filenode.map` file
    // // 3. Collection of RelDirs: HashSet of (relfilenode, forknum) for each (spcnode, dbnode)
    // async fn import_dirs(
    //     &mut self,
    //     layer_writer: &mut ImageLayerWriter,
    //     path: &Utf8PathBuf,
    // ) -> anyhow::Result<()> {

    //     Ok(())
    // }

    async fn import_controlfile(&mut self, pgdata_path: &Utf8Path) -> anyhow::Result<ControlFileData> {
        let controlfile_path = pgdata_path.join("global").join("pg_control");
        let controlfile_buf = std::fs::read(&controlfile_path)
            .with_context(|| format!("reading controlfile: {controlfile_path}"))?;
        ControlFileData::decode(&controlfile_buf)
    }

    async fn import_db(
        &mut self,
        layer_writer: &mut ImageLayerWriter,
        db: &PgDataDirDb,
    ) -> anyhow::Result<()> {
        debug!(
            "Importing database (path={}, tablespace={}, dboid={})",
            db.path, db.spcnode, db.dboid
        );

        for file in &db.files {
            self.import_rel_file(layer_writer, &file.path, &file.rel_tag, file.segno).await?;
        };

        Ok(())
    }

    async fn import_rel_file(
        &mut self,
        layer_writer: &mut ImageLayerWriter,
        path: &Utf8PathBuf,
        rel_tag: &RelTag,
        segno: u32,
    ) -> anyhow::Result<()> {
        debug!("Importing relation file (path={path}, rel_tag={rel_tag}, segno={segno})");

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

        Ok(())
    }

    async fn create_index_part(&mut self, layers: &[PersistentLayerDesc], control_file: &ControlFileData) -> anyhow::Result<()> {
        let dstdir = &self.conf.workdir;

        let pg_version = match control_file.catalog_version_no {
            // thesea are from catversion.h
            202107181 => 14,
            202209061 => 15,
            202307071 => 16,
            catversion => { bail!("unrecognized catalog version {catversion}")},
        };

        let metadata = TimelineMetadata::new(
            self.pgdata_lsn,
            None, // prev_record_lsn
            None, // no ancestor
            Lsn(0),
            self.pgdata_lsn,  // latest_gc_cutoff_lsn
            self.pgdata_lsn,  // initdb_lsn
            pg_version,
        );
        let generation = Generation::none();
        let mut index_part = IndexPart::empty(metadata);

        for l in layers {
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
    pub path: Utf8PathBuf,
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
            path: datadir_path.clone(),
            dbs: databases
        }
    }
}

impl PgDataDirDb {
    fn new(db_path: Utf8PathBuf, spcnode: u32, dboid: u32, datadir_path: &Utf8PathBuf) -> Self {
        PgDataDirDb {
            files: WalkDir::new(&db_path)
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

                    PgDataDirDbFile {
                        path: datadir_path.join(rel_tag.to_segfile_name(segno)),
                        rel_tag,
                        segno,
                    }
                })
                .collect(),
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

///////////////////////////////
// Set up timeline
// most of that is needed for image_layer_writer.finish()
// refactoring finish might be a better idea
///////////////////////////////


// let shard_id = ShardIdentity::unsharded();
// let tli = TimelineId::generate();
// let aaa_atc = Arc::new(ArcSwap::from(Arc::new(atc)));
// let tl_metadata = TimelineMetadata::new(
//     Lsn(0),
//     None,
//     None,
//     Lsn(0),
//     Lsn(4242),
//     Lsn(4242),
//     16,
// );
// let tc = models::TenantConfig {
//     ..models::TenantConfig::default()
// };
// let atc = AttachedTenantConf::try_from(LocationConf::attached_single(
//     TenantConfOpt{
//         ..Default::default()
//     },
//     Generation::new(42),
//     &ShardParameters::default(),
// ))?;



// // let walredo_mgr = Arc::new(WalRedoManager::from(TestRedoManager));

// let config = RemoteStorageConfig {
//     storage: RemoteStorageKind::LocalFs {
//         local_path: Utf8PathBuf::from("remote")
//     },
//     timeout: RemoteStorageConfig::DEFAULT_TIMEOUT,
// };
// let remote_storage = GenericRemoteStorage::from_config(&config).await.unwrap();
// let deletion_queue = MockDeletionQueue::new(Some(remote_storage.clone()));

// let remote_client = RemoteTimelineClient::new(
//     remote_storage,
//     deletion_queue.new_client(),
//     &conf,
//     tsi,
//     tli,
//     Generation::Valid(42),
// );


// let resources = TimelineResources {
//     remote_client,
//     timeline_get_throttle: tenant.timeline_get_throttle.clone(),
//     l0_flush_global_state: tenant.l0_flush_global_state.clone(),
// };


// let timeline = Timeline::new(
//     &conf,
//     aaa_atc,
//     &tl_metadata,
//     None,
//     tli,
//     TenantShardId {
//         tenant_id: tni,
//         shard_number: ShardNumber(0),
//         shard_count: ShardCount(0)
//     },
//     Generation::Valid(42),
//     shard_id,
//     None,
//     resources,
//     16,
//     state,
//     last_aux_file_policy,
//     self.cancel.child_token(),
// );
