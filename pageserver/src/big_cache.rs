use std::collections::HashMap;

use bytes::Bytes;
use anyhow::Result;
use once_cell::{sync::OnceCell, sync::Lazy};
use utils::lsn::Lsn;
use std::sync::Mutex;

use crate::repository::Key;

pub static BIG_CACHE: Lazy<BigCache> = Lazy::new(|| {
    BigCache {
        index: Mutex::new(HashMap::new()),
    }
});

pub struct BigCache {
    // TODO point to offset in ephemeral file instead
    index: Mutex<HashMap<Key, (Lsn, Bytes)>>,
}

impl BigCache {
    pub fn lookup(&self, key: &Key) -> Result<Option<(Lsn, Bytes)>> {
        Ok(self.index.lock().unwrap().get(key).map(|img| img.clone()))
    }

    pub fn memorize(&self, key: Key, img: (Lsn, Bytes)) -> Result<()> {
        self.index.lock().unwrap().insert(key, img);
        Ok(())
    }
}
