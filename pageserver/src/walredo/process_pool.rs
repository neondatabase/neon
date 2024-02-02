use std::sync::Arc;

use utils::pre_spawned_pool;

use crate::config::PageServerConf;

use super::process::WalRedoProcess;

pub struct Pool {
    v14: pre_spawned_pool::Client<Arc<WalRedoProcess>>,
    v15: pre_spawned_pool::Client<Arc<WalRedoProcess>>,
    v16: pre_spawned_pool::Client<Arc<WalRedoProcess>>,
}

struct Launcher {
    pg_version: u32,
    conf: &'static PageServerConf,
}

impl utils::pre_spawned_pool::Launcher<Arc<WalRedoProcess>> for Launcher {
    fn create(&self) -> anyhow::Result<Arc<WalRedoProcess>> {
        Ok(Arc::new(WalRedoProcess::launch(
            self.conf,
            self.pg_version,
        )?))
    }
}

impl Pool {
    pub async fn launch(conf: &'static PageServerConf) -> Self {
        Self {
            v14: pre_spawned_pool::Pool::launch(Launcher {
                pg_version: 14,
                conf,
            })
            .await,
            v15: pre_spawned_pool::Pool::launch(Launcher {
                pg_version: 15,
                conf,
            })
            .await,
            v16: pre_spawned_pool::Pool::launch(Launcher {
                pg_version: 16,
                conf,
            })
            .await,
        }
    }
    pub fn get(
        &self,
        pg_version: usize,
    ) -> Result<Arc<WalRedoProcess>, pre_spawned_pool::GetError> {
        let pool = match pg_version {
            14 => &self.v14,
            15 => &self.v15,
            16 => &self.v16,
            x => anyhow::bail!("unknown pg version: {x}"),
        };
        pool.get()
    }
}
