//! This module contains the encoding and decoding of the local manifest file.
//!
//! MANIFEST is a write-ahead log which is stored locally to each timeline. It
//! records the state of the storage engine. It contains a snapshot of the
//! state and all operations proceeding that snapshot. The file begins with a
//! header recording MANIFEST version number. After that, it contains a snapshot.
//! The snapshot is followed by a list of operations. Each operation is a list
//! of records. Each record is either an addition or a removal of a layer.
//!
//! With MANIFEST, we can:
//!
//! 1. recover state quickly by reading the file, potentially boosting the
//!    startup speed.
//! 2. ensure all operations are atomic and avoid corruption, solving issues
//!    like redundant image layer and preparing us for future compaction
//!    strategies.
//!
//! There is also a format for storing all layer files on S3, called
//! `index_part.json`. Compared with index_part, MANIFEST is an WAL which
//! records all operations as logs, and therefore we can easily replay the
//! operations when recovering from crash, while ensuring those operations
//! are atomic upon restart.
//!
//! Currently, this is not used in the system. Future refactors will ensure
//! the storage state will be recorded in this file, and the system can be
//! recovered from this file. This is tracked in
//! <https://github.com/neondatabase/neon/issues/4418>

use std::io::{self, Read, Write};

use crate::virtual_file::VirtualFile;
use anyhow::Result;
use bytes::{Buf, BufMut, Bytes, BytesMut};
use crc32c::crc32c;
use serde::{Deserialize, Serialize};
use tracing::log::warn;
use utils::lsn::Lsn;

use super::storage_layer::PersistentLayerDesc;

pub struct Manifest {
    file: VirtualFile,
}

#[derive(Clone, Serialize, Deserialize, PartialEq, Eq, Debug)]
pub struct Snapshot {
    pub layers: Vec<PersistentLayerDesc>,
}

/// serde by default encode this in tagged enum, and therefore it will be something
/// like `{ "AddLayer": { ... } }`.
#[derive(Clone, Serialize, Deserialize, PartialEq, Eq, Debug)]
pub enum Record {
    AddLayer(PersistentLayerDesc),
    RemoveLayer(PersistentLayerDesc),
}

/// `echo neon.manifest | sha1sum` and take the leading 8 bytes.
const MANIFEST_MAGIC_NUMBER: u64 = 0xf5c44592b806109c;
const MANIFEST_VERSION: u64 = 1;

#[derive(Clone, Serialize, Deserialize, PartialEq, Eq, Debug)]
pub struct ManifestHeader {
    magic_number: u64,
    version: u64,
}

const MANIFEST_HEADER_LEN: usize = 16;

impl ManifestHeader {
    fn encode(&self) -> BytesMut {
        let mut buf = BytesMut::with_capacity(MANIFEST_HEADER_LEN);
        buf.put_u64(self.magic_number);
        buf.put_u64(self.version);
        buf
    }

    fn decode(mut buf: &[u8]) -> Self {
        assert!(buf.len() == MANIFEST_HEADER_LEN, "invalid header");
        Self {
            magic_number: buf.get_u64(),
            version: buf.get_u64(),
        }
    }
}

#[derive(Clone, Serialize, Deserialize, PartialEq, Eq, Debug)]
pub enum Operation {
    /// A snapshot of the current state.
    ///
    /// Lsn field represents the LSN that is persisted to disk for this snapshot.
    Snapshot(Snapshot, Lsn),
    /// An atomic operation that changes the state.
    ///
    /// Lsn field represents the LSN that is persisted to disk after the operation is done.
    /// This will only change when new L0 is flushed to the disk.
    Operation(Vec<Record>, Lsn),
}

struct RecordHeader {
    size: u32,
    checksum: u32,
}

const RECORD_HEADER_LEN: usize = 8;

impl RecordHeader {
    fn encode(&self) -> BytesMut {
        let mut buf = BytesMut::with_capacity(RECORD_HEADER_LEN);
        buf.put_u32(self.size);
        buf.put_u32(self.checksum);
        buf
    }

    fn decode(mut buf: &[u8]) -> Self {
        assert!(buf.len() == RECORD_HEADER_LEN, "invalid header");
        Self {
            size: buf.get_u32(),
            checksum: buf.get_u32(),
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum ManifestLoadError {
    #[error("manifest header is corrupted")]
    CorruptedManifestHeader,
    #[error("unsupported manifest version: got {0}, expected {1}")]
    UnsupportedVersion(u64, u64),
    #[error("error when decoding record: {0}")]
    DecodeRecord(serde_json::Error),
    #[error("I/O error: {0}")]
    Io(io::Error),
}

#[must_use = "Should check if the manifest is partially corrupted"]
pub struct ManifestPartiallyCorrupted(bool);

impl Manifest {
    /// Create a new manifest by writing the manifest header and a snapshot record to the given file.
    pub fn init(file: VirtualFile, snapshot: Snapshot, lsn: Lsn) -> Result<Self> {
        let mut manifest = Self { file };
        manifest.append_manifest_header(ManifestHeader {
            magic_number: MANIFEST_MAGIC_NUMBER,
            version: MANIFEST_VERSION,
        })?;
        manifest.append_operation(Operation::Snapshot(snapshot, lsn))?;
        Ok(manifest)
    }

    /// Load a manifest. Returns the manifest and a list of operations. If the manifest is corrupted,
    /// the bool flag will be set to true and the user is responsible to reconstruct a new manifest and
    /// backup the current one.
    pub fn load(
        mut file: VirtualFile,
    ) -> Result<(Self, Vec<Operation>, ManifestPartiallyCorrupted), ManifestLoadError> {
        let mut buf = vec![];
        file.read_to_end(&mut buf).map_err(ManifestLoadError::Io)?;

        // Read manifest header
        let mut buf = Bytes::from(buf);
        if buf.remaining() < MANIFEST_HEADER_LEN {
            return Err(ManifestLoadError::CorruptedManifestHeader);
        }
        let header = ManifestHeader::decode(&buf[..MANIFEST_HEADER_LEN]);
        buf.advance(MANIFEST_HEADER_LEN);
        if header.version != MANIFEST_VERSION {
            return Err(ManifestLoadError::UnsupportedVersion(
                header.version,
                MANIFEST_VERSION,
            ));
        }

        // Read operations
        let mut operations = Vec::new();
        let corrupted = loop {
            if buf.remaining() == 0 {
                break false;
            }
            if buf.remaining() < RECORD_HEADER_LEN {
                warn!("incomplete header when decoding manifest, could be corrupted");
                break true;
            }
            let RecordHeader { size, checksum } = RecordHeader::decode(&buf[..RECORD_HEADER_LEN]);
            let size = size as usize;
            buf.advance(RECORD_HEADER_LEN);
            if buf.remaining() < size {
                warn!("incomplete data when decoding manifest, could be corrupted");
                break true;
            }
            let data = &buf[..size];
            if crc32c(data) != checksum {
                warn!("checksum mismatch when decoding manifest, could be corrupted");
                break true;
            }
            // if the following decode fails, we cannot use the manifest or safely ignore any record.
            operations.push(serde_json::from_slice(data).map_err(ManifestLoadError::DecodeRecord)?);
            buf.advance(size);
        };
        Ok((
            Self { file },
            operations,
            ManifestPartiallyCorrupted(corrupted),
        ))
    }

    fn append_data(&mut self, data: &[u8]) -> Result<()> {
        if data.len() >= u32::MAX as usize {
            panic!("data too large");
        }
        let header = RecordHeader {
            size: data.len() as u32,
            checksum: crc32c(data),
        };
        let header = header.encode();
        self.file.write_all(&header)?;
        self.file.write_all(data)?;
        self.file.sync_all()?;
        Ok(())
    }

    fn append_manifest_header(&mut self, header: ManifestHeader) -> Result<()> {
        let encoded = header.encode();
        self.file.write_all(&encoded)?;
        Ok(())
    }

    /// Add an operation to the manifest. The operation will be appended to the end of the file,
    /// and the file will fsync.
    pub fn append_operation(&mut self, operation: Operation) -> Result<()> {
        let encoded = Vec::from(serde_json::to_string(&operation)?);
        self.append_data(&encoded)
    }
}

#[cfg(test)]
mod tests {
    use std::fs::OpenOptions;

    use crate::repository::Key;

    use super::*;

    #[test]
    fn test_read_manifest() {
        let testdir = crate::config::PageServerConf::test_repo_dir("test_read_manifest");
        std::fs::create_dir_all(&testdir).unwrap();
        let file = VirtualFile::create(&testdir.join("MANIFEST")).unwrap();
        let layer1 = PersistentLayerDesc::new_test(Key::from_i128(0)..Key::from_i128(233));
        let layer2 = PersistentLayerDesc::new_test(Key::from_i128(233)..Key::from_i128(2333));
        let layer3 = PersistentLayerDesc::new_test(Key::from_i128(2333)..Key::from_i128(23333));
        let layer4 = PersistentLayerDesc::new_test(Key::from_i128(23333)..Key::from_i128(233333));

        // Write a manifest with a snapshot and some operations
        let snapshot = Snapshot {
            layers: vec![layer1, layer2],
        };
        let mut manifest = Manifest::init(file, snapshot.clone(), Lsn::from(0)).unwrap();
        manifest
            .append_operation(Operation::Operation(
                vec![Record::AddLayer(layer3.clone())],
                Lsn::from(1),
            ))
            .unwrap();
        drop(manifest);

        // Open the second time and write
        let file = VirtualFile::open_with_options(
            &testdir.join("MANIFEST"),
            OpenOptions::new()
                .read(true)
                .write(true)
                .create_new(false)
                .truncate(false),
        )
        .unwrap();
        let (mut manifest, operations, corrupted) = Manifest::load(file).unwrap();
        assert!(!corrupted.0);
        assert_eq!(operations.len(), 2);
        assert_eq!(
            &operations[0],
            &Operation::Snapshot(snapshot.clone(), Lsn::from(0))
        );
        assert_eq!(
            &operations[1],
            &Operation::Operation(vec![Record::AddLayer(layer3.clone())], Lsn::from(1))
        );
        manifest
            .append_operation(Operation::Operation(
                vec![
                    Record::RemoveLayer(layer3.clone()),
                    Record::AddLayer(layer4.clone()),
                ],
                Lsn::from(2),
            ))
            .unwrap();
        drop(manifest);

        // Open the third time and verify
        let file = VirtualFile::open_with_options(
            &testdir.join("MANIFEST"),
            OpenOptions::new()
                .read(true)
                .write(true)
                .create_new(false)
                .truncate(false),
        )
        .unwrap();
        let (_manifest, operations, corrupted) = Manifest::load(file).unwrap();
        assert!(!corrupted.0);
        assert_eq!(operations.len(), 3);
        assert_eq!(&operations[0], &Operation::Snapshot(snapshot, Lsn::from(0)));
        assert_eq!(
            &operations[1],
            &Operation::Operation(vec![Record::AddLayer(layer3.clone())], Lsn::from(1))
        );
        assert_eq!(
            &operations[2],
            &Operation::Operation(
                vec![Record::RemoveLayer(layer3), Record::AddLayer(layer4)],
                Lsn::from(2)
            )
        );
    }
}
