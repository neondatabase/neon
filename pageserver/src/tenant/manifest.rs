use std::io::{Read, Write};

use crate::virtual_file::VirtualFile;
use anyhow::Result;
use bytes::{Buf, BufMut, Bytes, BytesMut};
use crc32c::crc32c;
use serde::{Deserialize, Serialize};
use tracing::log::warn;

use super::storage_layer::PersistentLayerDesc;

pub struct Manifest {
    file: VirtualFile,
}

#[derive(Clone, Serialize, Deserialize, PartialEq, Eq, Debug)]
pub struct Snapshot {
    pub layers: Vec<PersistentLayerDesc>,
}

#[derive(Clone, Serialize, Deserialize, PartialEq, Eq, Debug)]
pub enum Record {
    AddLayer(PersistentLayerDesc),
    RemoveLayer(PersistentLayerDesc),
}

#[derive(Clone, Serialize, Deserialize, PartialEq, Eq, Debug)]
pub enum Operation {
    /// A snapshot of the current state
    Snapshot(Snapshot),
    /// An atomic operation that changes the state
    Operation(Vec<Record>),
}

struct Header {
    size: u32,
    checksum: u32,
}

const HEADER_LEN: usize = 8;

impl Header {
    fn encode(&self) -> BytesMut {
        let mut buf = BytesMut::with_capacity(HEADER_LEN);
        buf.put_u32(self.size);
        buf.put_u32(self.checksum);
        buf
    }

    fn decode(mut buf: &[u8]) -> Self {
        assert!(buf.len() == HEADER_LEN, "invalid header");
        Self {
            size: buf.get_u32(),
            checksum: buf.get_u32(),
        }
    }
}

impl Manifest {
    pub fn init(file: VirtualFile, snapshot: Snapshot) -> Result<Self> {
        let mut manifest = Self { file };
        manifest.append_operation(Operation::Snapshot(snapshot))?;
        Ok(manifest)
    }

    pub fn load(mut file: VirtualFile) -> Result<(Self, Vec<Operation>)> {
        let mut buf = vec![];
        file.read_to_end(&mut buf)?;
        let mut buf = Bytes::from(buf);
        let mut operations = Vec::new();
        loop {
            if buf.remaining() == 0 {
                break;
            }
            if buf.remaining() < HEADER_LEN {
                warn!("incomplete header when decoding manifest, could be corrupted");
                break;
            }
            let Header { size, checksum } = Header::decode(&buf[..HEADER_LEN]);
            let size = size as usize;
            buf.advance(HEADER_LEN);
            if buf.remaining() < size {
                warn!("incomplete data when decoding manifest, could be corrupted");
                break;
            }
            let data = &buf[..size];
            if crc32c(data) != checksum {
                warn!("checksum mismatch when decoding manifest, could be corrupted");
                break;
            }
            operations.push(serde_json::from_slice(data)?);
            buf.advance(size);
        }
        Ok((Self { file }, operations))
    }

    fn append_data(&mut self, data: &[u8]) -> Result<()> {
        if data.len() >= u32::MAX as usize {
            panic!("data too large");
        }
        let header = Header {
            size: data.len() as u32,
            checksum: crc32c(data),
        };
        let header = header.encode();
        self.file.write_all(&header)?;
        self.file.write_all(data)?;
        self.file.sync_all()?;
        Ok(())
    }

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
        let snapshot = Snapshot {
            layers: vec![layer1, layer2],
        };
        let mut manifest = Manifest::init(file, snapshot.clone()).unwrap();
        manifest
            .append_operation(Operation::Operation(vec![Record::AddLayer(layer3.clone())]))
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
        let (mut manifest, operations) = Manifest::load(file).unwrap();
        assert_eq!(operations.len(), 2);
        assert_eq!(&operations[0], &Operation::Snapshot(snapshot.clone()));
        assert_eq!(
            &operations[1],
            &Operation::Operation(vec![Record::AddLayer(layer3.clone())])
        );
        manifest
            .append_operation(Operation::Operation(vec![
                Record::RemoveLayer(layer3.clone()),
                Record::AddLayer(layer4.clone()),
            ]))
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
        let (_manifest, operations) = Manifest::load(file).unwrap();
        assert_eq!(operations.len(), 3);
        assert_eq!(&operations[0], &Operation::Snapshot(snapshot.clone()));
        assert_eq!(
            &operations[1],
            &Operation::Operation(vec![Record::AddLayer(layer3.clone())])
        );
        assert_eq!(
            &operations[2],
            &Operation::Operation(vec![
                Record::RemoveLayer(layer3.clone()),
                Record::AddLayer(layer4.clone())
            ])
        );
    }
}
