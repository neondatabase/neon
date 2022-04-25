use std::{collections::HashMap, io::Write, path::Path};

use bytes::Bytes;
use anyhow::Result;
use once_cell::{sync::OnceCell, sync::Lazy};
use utils::lsn::Lsn;
use std::sync::Mutex;
use std::fs::File;

use crate::{layered_repository::ephemeral_file::EphemeralFile, repository::Key};

pub static BIG_CACHE: Lazy<BigCache> = Lazy::new(|| {
    BigCache {
        index: Mutex::new(HashMap::new()),
        file: Mutex::new(File::create(Path::new("big_cache.tmp")).unwrap()),
    }
});

pub struct BigCache {
    // TODO point to file offset instead
    index: Mutex<HashMap<Key, (Lsn, Bytes)>>,
    file: Mutex<File>,  // TODO use ephemeral file instead
}

impl BigCache {
    pub fn lookup(&self, key: &Key) -> Result<Option<(Lsn, Bytes)>> {
        // TODO read this from the file instead
        Ok(self.index.lock().unwrap().get(key).map(|img| img.clone()))
    }

    pub fn memorize(&self, key: Key, img: (Lsn, Bytes)) -> Result<()> {
        self.index.lock().unwrap().insert(key, img.clone());
        self.file.lock().unwrap().write(&img.1)?;
        Ok(())
    }
}
