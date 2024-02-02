use std::sync::Arc;

use utils::pre_spawned_pool;

use crate::config::PageServerConf;

use super::process::WalRedoProcess;

pub struct Pool {
    v14: pre_spawned_pool::Pool<Arc<WalRedoProcess>, Launcher>,
    v15: pre_spawned_pool::Pool<Arc<WalRedoProcess>, Launcher>,
    v16: pre_spawned_pool::Pool<Arc<WalRedoProcess>, Launcher>,
}

struct Launcher {
    pg_version: u32,
    conf: &'static PageServerConf,
}

impl utils::pre_spawned_pool::Launcher<Arc<WalRedoProcess>> for Launcher{
    fn create(&self) -> anyhow::Result<Arc<WalRedoProcess>> {
        WalRedoProcess::launch(self.conf, self.pg_version)
    }
}

impl Pool {
    pub fn get(&self, pg_version: usize) -> anyhow::Result<Arc<WalRedoProcess>> {
        let pool = match pg_version {
            14 => &self.v14,
            15 => &self.v15,
            16 => &self.v16,
            x => anyhow::bail!("unknown pg version: {x}"),
        };
        pool.get()
    }
}
