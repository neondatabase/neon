use std::{num::NonZeroUsize, sync::Arc};

use bytes::{Buf, Bytes};
use clap::Parser;
use pageserver::{config::PageServerConf, walrecord::NeonWalRecord, walredo::PostgresRedoManager};
use pageserver_api::{key::Key, shard::TenantShardId};
use tokio::{sync::Barrier, task::JoinSet};
use utils::{id::TenantId, lsn::Lsn};

#[derive(clap::Parser)]
struct Args {
    #[clap(long, default_value = "1")]
    managers: NonZeroUsize,

    #[clap(long, default_value = "1")]
    clients_per_manager: NonZeroUsize,
}

fn main() {
    utils::logging::init(
        utils::logging::LogFormat::Plain,
        utils::logging::TracingErrorLayerEnablement::Disabled,
        utils::logging::Output::Stderr,
    )
    .unwrap();
    utils::logging::replace_panic_hook_with_tracing_panic_hook().forget();

    let args = Args::parse();

    let repo_dir = camino_tempfile::tempdir_in(env!("CARGO_TARGET_TMPDIR")).unwrap();

    let conf = PageServerConf::dummy_conf(repo_dir.path().to_path_buf());
    let conf = Box::leak(Box::new(conf));
    let tenant_shard_id = TenantShardId::unsharded(TenantId::generate());

    let Args {
        managers,
        clients_per_manager,
    } = args;

    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap();

    let start = Arc::new(Barrier::new(managers.get() * clients_per_manager.get()));

    let mut tasks = JoinSet::new();

    for _ in 0..managers.get() {
        let manager = PostgresRedoManager::new(conf, tenant_shard_id);
        let manager = Arc::new(manager);

        for _ in 0..clients_per_manager.get() {
            rt.block_on(async { tasks.spawn(client(Arc::clone(&manager), Arc::clone(&start))) });
        }
    }

    while let Some(res) = rt.block_on(tasks.join_next()) {
        let _: () = res.unwrap();
    }
}

async fn client(mgr: Arc<PostgresRedoManager>, start: Arc<Barrier>) {
    let input = Request::short_input();
    start.wait().await;
    loop {
        let page = input.execute(&mgr).await.unwrap();
        assert_eq!(page.remaining(), 8192);
    }
}

fn pg_record(will_init: bool, bytes: &'static [u8]) -> NeonWalRecord {
    let rec = Bytes::from_static(bytes);
    NeonWalRecord::Postgres { will_init, rec }
}

macro_rules! lsn {
    ($input:expr) => {{
        let input = $input;
        match <Lsn as std::str::FromStr>::from_str(input) {
            Ok(lsn) => lsn,
            Err(e) => panic!("failed to parse {}: {}", input, e),
        }
    }};
}

/// Simple wrapper around `WalRedoManager::request_redo`.
///
/// In benchmarks this is cloned around.
#[derive(Clone)]
struct Request {
    key: Key,
    lsn: Lsn,
    base_img: Option<(Lsn, Bytes)>,
    records: Vec<(Lsn, NeonWalRecord)>,
    pg_version: u32,
}

impl Request {
    async fn execute(&self, manager: &PostgresRedoManager) -> anyhow::Result<Bytes> {
        let Request {
            key,
            lsn,
            base_img,
            records,
            pg_version,
        } = self;

        // TODO: avoid these clones
        manager
            .request_redo(*key, *lsn, base_img.clone(), records.clone(), *pg_version)
            .await
    }

    #[allow(clippy::octal_escapes)]
    fn short_input() -> Request {
        Request {
        key: Key {
            field1: 0,
            field2: 1663,
            field3: 13010,
            field4: 1259,
            field5: 0,
            field6: 0,
        },
        lsn: lsn!("0/16E2408"),
        base_img: None,
        records: vec![
            (
                lsn!("0/16A9388"),
                pg_record(true, b"j\x03\0\0\0\x04\0\0\xe8\x7fj\x01\0\0\0\0\0\n\0\0\xd0\x16\x13Y\0\x10\0\04\x03\xd4\0\x05\x7f\x06\0\0\xd22\0\0\xeb\x04\0\0\0\0\0\0\xff\x03\0\0\0\0\x80\xeca\x01\0\0\x01\0\xd4\0\xa0\x1d\0 \x04 \0\0\0\0/\0\x01\0\xa0\x9dX\x01\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0.\0\x01\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\00\x9f\x9a\x01P\x9e\xb2\x01\0\x04\0\0\0\0\0\0\0\0\0\0\0\0\0\0\x02\0!\0\x01\x08 \xff\xff\xff?\0\0\0\0\0\0@\0\0another_table\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\x98\x08\0\0\x02@\0\0\0\0\0\0\n\0\0\0\x02\0\0\0\0@\0\0\0\0\0\0\0\0\0\0\0\0\x80\xbf\0\0\0\0\0\0\0\0\0\0pr\x01\0\0\0\0\0\0\0\0\x01d\0\0\0\0\0\0\x04\0\0\x01\0\0\0\0\0\0\0\x0c\x02\0\0\0\0\0\0\0\0\0\0\0\0\0\0/\0!\x80\x03+ \xff\xff\xff\x7f\0\0\0\0\0\xdf\x04\0\0pg_type\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\x0b\0\0\0G\0\0\0\0\0\0\0\n\0\0\0\x02\0\0\0\0\0\0\0\0\0\0\0\x0e\0\0\0\0@\x16D\x0e\0\0\0K\x10\0\0\x01\0pr \0\0\0\0\0\0\0\0\x01n\0\0\0\0\0\xd6\x02\0\0\x01\0\0\0[\x01\0\0\0\0\0\0\0\t\x04\0\0\x02\0\0\0\x01\0\0\0\n\0\0\0\n\0\0\0\x7f\0\0\0\0\0\0\0\n\0\0\0\x02\0\0\0\0\0\0C\x01\0\0\x15\x01\0\0\0\0\0\0\0\0\0\0\0\0\0\0.\0!\x80\x03+ \xff\xff\xff\x7f\0\0\0\0\0;\n\0\0pg_statistic\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\x0b\0\0\0\xfd.\0\0\0\0\0\0\n\0\0\0\x02\0\0\0;\n\0\0\0\0\0\0\x13\0\0\0\0\0\xcbC\x13\0\0\0\x18\x0b\0\0\x01\0pr\x1f\0\0\0\0\0\0\0\0\x01n\0\0\0\0\0\xd6\x02\0\0\x01\0\0\0C\x01\0\0\0\0\0\0\0\t\x04\0\0\x01\0\0\0\x01\0\0\0\n\0\0\0\n\0\0\0\x7f\0\0\0\0\0\0\x02\0\x01"),
            ),
            (
                lsn!("0/16D4080"),
                pg_record(false, b"\xbc\0\0\0\0\0\0\0h?m\x01\0\0\0\0p\n\0\09\x08\xa3\xea\0 \x8c\0\x7f\x06\0\0\xd22\0\0\xeb\x04\0\0\0\0\0\0\xff\x02\0@\0\0another_table\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\x98\x08\0\0\x02@\0\0\0\0\0\0\n\0\0\0\x02\0\0\0\0@\0\0\0\0\0\0\x05\0\0\0\0@zD\x05\0\0\0\0\0\0\0\0\0pr\x01\0\0\0\0\0\0\0\0\x01d\0\0\0\0\0\0\x04\0\0\x01\0\0\0\x02\0"),
            ),
        ],
        pg_version: 14,
    }
    }
}
