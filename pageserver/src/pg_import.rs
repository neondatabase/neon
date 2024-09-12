use std::{fs::metadata, path::Path};

use anyhow::{bail, ensure, Context};
use bytes::Bytes;
use camino::{Utf8Path, Utf8PathBuf};

use itertools::Itertools;
use pageserver_api::{key::rel_block_to_key, reltag::RelTag};
use postgres_ffi::{pg_constants, relfile_utils::parse_relfilename, ControlFileData, DBState_DB_SHUTDOWNED, Oid, BLCKSZ};
use tokio::io::AsyncRead;
use tracing::{debug, trace, warn};
use utils::{id::{NodeId, TenantId, TimelineId}, shard::{ShardCount, ShardNumber, TenantShardId}};
use walkdir::WalkDir;

use crate::{context::{DownloadBehavior, RequestContext}, import_datadir, task_mgr::TaskKind, tenant::storage_layer::ImageLayerWriter};
use crate::config::PageServerConf;
use tokio::io::AsyncReadExt;

use pageserver_api::key::Key;

pub struct PgImportEnv {
    ctx: RequestContext,
    conf: &'static PageServerConf,
    tli: TimelineId,
    tsi: TenantShardId,
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
        })
    }

    pub async fn import_datadir(&mut self, pgdata_path: &Utf8Path) -> anyhow::Result<()> {

        let pgdata_lsn = import_datadir::get_lsn_from_controlfile(&pgdata_path)?.align();

        let timeline_path = self.conf.timeline_path(&self.tsi, &self.tli);

        println!("Importing {pgdata_path} to {timeline_path} as lsn {pgdata_lsn}...");

        let range = Key::MIN..Key::NON_L0_MAX;
        let mut one_big_layer = ImageLayerWriter::new(
            &self.conf,
            self.tli,
            self.tsi,
            &range,
            pgdata_lsn,
            &self.ctx,
        ).await?;

        // Import ordinary databases, DEFAULTTABLESPACE_OID is smaller than GLOBALTABLESPACE_OID, so import them first
        // Traverse database in increasing oid order
        let dbdirs = WalkDir::new(pgdata_path.join("base"))
            .max_depth(1)
            .into_iter()
            .filter_map(|entry| {
                entry.ok().and_then(|path| {
                    path.file_name().to_string_lossy().parse::<u32>().ok()
                })
            })
            .sorted();

        for dboid in dbdirs {
            let path = pgdata_path.join("base").join(dboid.to_string());
            self.import_db(&mut one_big_layer, &path, dboid, pg_constants::DEFAULTTABLESPACE_OID).await?;
        };

        // global catalogs now
        self.import_db(&mut one_big_layer, &pgdata_path.join("global"), 0, postgres_ffi::pg_constants::GLOBALTABLESPACE_OID).await?;
        
        one_big_layer.finish_layer(&self.ctx).await?;

        // should we anything about the wal?

        Ok(())
    }

    async fn import_db(
        &mut self,
        layer_writer: &mut ImageLayerWriter,
        path: &Utf8PathBuf,
        dboid: u32,
        spcnode: u32,
    ) -> anyhow::Result<()> {

        debug!("Importing database (path={path}, tablespace={spcnode}, dboid={dboid})");

        // traverse database directory in the same order as our RelKey ordering
        let reldirs = WalkDir::new(path)
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
            .sorted();

        for (relnode, forknum, segno) in reldirs {
            let rel_tag = RelTag {
                spcnode,
                dbnode: dboid,
                relnode,
                forknum,
            };

            self.import_rel_file(layer_writer, path, rel_tag, segno).await?;
        };

        Ok(())
    }

    async fn import_rel_file(
        &mut self,
        layer_writer: &mut ImageLayerWriter,
        db_path: &Utf8PathBuf,
        rel_tag: RelTag,
        segno: u32,
    ) -> anyhow::Result<()> {
        let path = db_path.join(rel_tag.relnode.to_string());

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
                    let key = rel_block_to_key(rel_tag, blknum);
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
