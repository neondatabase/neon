//!
//! Utils for logging wal metadata. Useful for tests only, and too expensive to run in prod.
//!
//! Ideally we'd get this wal metadata using pg_waldump from the compute pg_wal directory,
//! but pg_waldump doesn't provide all the metadata we need. We could write a rust program
//! to analyze pg wal, but we'd need to port some c code for decoding wal files. This module
//! is a temporary hack that allows us to print the metadata that the pageserver decodes
//! using postgres_ffi::waldecoder.
//!
//! Logging wal metadata could add significant write overhead to the pageserver. Tests that
//! rely on this should either spin up a dedicated pageserver for wal metadata logging, or
//! only measure read performance.
//!
use anyhow::Result;
use bytes::{BufMut, Bytes, BytesMut};
use once_cell::sync::OnceCell;
use serde::{Deserialize, Serialize};
use std::fs::File;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use zenith_utils::lsn::Lsn;

use crate::{config::PageServerConf, repository::Key, walrecord::DecodedBkpBlock};

pub static WAL_METADATA_FILE: OnceCell<File> = OnceCell::new();

#[derive(Copy, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Debug, Serialize, Deserialize)]
pub struct Page {
    spcnode: u32,
    dbnode: u32,
    relnode: u32,
    forknum: u8,
    blkno: u32,
}

impl Page {
    pub async fn read<Reader>(buf: &mut Reader) -> Result<Page>
    where
        Reader: tokio::io::AsyncRead + Unpin,
    {
        let spcnode = buf.read_u32().await?;
        let dbnode = buf.read_u32().await?;
        let relnode = buf.read_u32().await?;
        let forknum = buf.read_u8().await?;
        let blkno = buf.read_u32().await?;
        Ok(Page {
            spcnode,
            dbnode,
            relnode,
            forknum,
            blkno,
        })
    }

    pub async fn write(&self, buf: &mut BytesMut) -> Result<()> {
        buf.put_u32(self.spcnode);
        buf.put_u32(self.dbnode);
        buf.put_u32(self.relnode);
        buf.put_u8(self.forknum);
        buf.put_u32(self.blkno);
        Ok(())
    }
}

impl From<&DecodedBkpBlock> for Page {
    fn from(blk: &DecodedBkpBlock) -> Self {
        Page {
            spcnode: blk.rnode_spcnode,
            dbnode: blk.rnode_dbnode,
            relnode: blk.rnode_relnode,
            forknum: blk.forknum,
            blkno: blk.blkno,
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct WalEntryMetadata {
    pub lsn: Lsn,
    pub size: usize,
    pub affected_pages: Vec<Page>,
}

pub fn init(conf: &'static PageServerConf) -> Result<()> {
    let wal_metadata_file_dir = conf.workdir.join("wal_metadata.log");
    WAL_METADATA_FILE
        .set(File::create(wal_metadata_file_dir)?)
        .expect("wal_metadata file is already created");
    Ok(())
}

pub fn write(wal_meta: WalEntryMetadata) -> Result<()> {
    if let Some(mut file) = WAL_METADATA_FILE.get() {
        let mut line = serde_json::to_string(&wal_meta)?;
        line.push('\n');
        std::io::prelude::Write::write_all(&mut file, line.as_bytes())?;
    }
    Ok(())
}
