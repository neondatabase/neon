use std::{collections::HashMap, io::{Seek, SeekFrom, Write}, ops::Range, os::unix::prelude::FileExt, path::Path};

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
        file: Mutex::new(File::options()
            .read(true)
            .write(true)
            .create(true)
            .open(Path::new("big_cache.tmp")).unwrap()),
    }
});

// TODO rename this
pub struct BigCache {
    // TODO more granular locks for parallel access
    index: Mutex<HashMap<Key, (Lsn, Range<u64>)>>,
    file: Mutex<File>,  // TODO use EphemeralFile
}

impl BigCache {
    pub fn lookup(&self, key: &Key) -> Result<Option<(Lsn, Bytes)>> {
        match self.index.lock().unwrap().get(key).map(|img| img.clone()) {
            Some((lsn, range)) => {
                let mut buf = vec![0u8; (range.end - range.start) as usize];
                self.file.lock().unwrap().read_exact_at(&mut buf, range.start)?;
                Ok(Some((lsn, Bytes::copy_from_slice(&buf))))
            }
            None => Ok(None),
        }
    }

    // TODO add 10MB buffer
    pub fn memorize(&self, key: Key, img: (Lsn, Bytes)) -> Result<()> {
        let mut file = self.file.lock().unwrap();
        let begin = file.seek(SeekFrom::Current(0))?;
        file.write(&img.1)?;
        let end = file.seek(SeekFrom::Current(0))?;

        self.index.lock().unwrap().insert(key, (img.0, begin..end));
        Ok(())
    }
}
