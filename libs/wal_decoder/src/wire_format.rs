use bytes::{BufMut, Bytes, BytesMut};
use pageserver_api::key::CompactKey;
use prost::{DecodeError, EncodeError, Message};
use tokio::io::AsyncWriteExt;
use utils::bin_ser::{BeSer, DeserializeError, SerializeError};
use utils::lsn::Lsn;
use utils::postgres_client::{Compression, InterpretedFormat};

use crate::models::{
    FlushUncommittedRecords, InterpretedWalRecord, InterpretedWalRecords, MetadataRecord,
};

use crate::serialized_batch::{
    ObservedValueMeta, SerializedValueBatch, SerializedValueMeta, ValueMeta,
};

use crate::models::proto;

#[derive(Debug, thiserror::Error)]
pub enum ToWireFormatError {
    #[error("{0}")]
    Bincode(#[from] SerializeError),
    #[error("{0}")]
    Protobuf(#[from] ProtobufSerializeError),
    #[error("{0}")]
    Compression(#[from] std::io::Error),
}

#[derive(Debug, thiserror::Error)]
pub enum ProtobufSerializeError {
    #[error("{0}")]
    MetadataRecord(#[from] SerializeError),
    #[error("{0}")]
    Encode(#[from] EncodeError),
}

#[derive(Debug, thiserror::Error)]
pub enum FromWireFormatError {
    #[error("{0}")]
    Bincode(#[from] DeserializeError),
    #[error("{0}")]
    Protobuf(#[from] ProtobufDeserializeError),
    #[error("{0}")]
    Decompress(#[from] std::io::Error),
}

#[derive(Debug, thiserror::Error)]
pub enum ProtobufDeserializeError {
    #[error("{0}")]
    Transcode(#[from] TranscodeError),
    #[error("{0}")]
    Decode(#[from] DecodeError),
}

#[derive(Debug, thiserror::Error)]
pub enum TranscodeError {
    #[error("{0}")]
    BadInput(String),
    #[error("{0}")]
    MetadataRecord(#[from] DeserializeError),
}

pub trait ToWireFormat {
    fn to_wire(
        self,
        format: InterpretedFormat,
        compression: Option<Compression>,
    ) -> impl std::future::Future<Output = Result<Bytes, ToWireFormatError>> + Send;
}

pub trait FromWireFormat {
    type T;
    fn from_wire(
        buf: &Bytes,
        format: InterpretedFormat,
        compression: Option<Compression>,
    ) -> impl std::future::Future<Output = Result<Self::T, FromWireFormatError>> + Send;
}

impl ToWireFormat for InterpretedWalRecords {
    async fn to_wire(
        self,
        format: InterpretedFormat,
        compression: Option<Compression>,
    ) -> Result<Bytes, ToWireFormatError> {
        use async_compression::tokio::write::ZstdEncoder;
        use async_compression::Level;

        let encode_res: Result<Bytes, ToWireFormatError> = match format {
            InterpretedFormat::Bincode => {
                let buf = BytesMut::new();
                let mut buf = buf.writer();
                self.ser_into(&mut buf)?;
                Ok(buf.into_inner().freeze())
            }
            InterpretedFormat::Protobuf => {
                let proto: proto::InterpretedWalRecords = self.try_into()?;
                let mut buf = BytesMut::new();
                proto
                    .encode(&mut buf)
                    .map_err(|e| ToWireFormatError::Protobuf(e.into()))?;

                Ok(buf.freeze())
            }
        };

        let buf = encode_res?;
        let compressed_buf = match compression {
            Some(Compression::Zstd { level }) => {
                let mut encoder = ZstdEncoder::with_quality(
                    Vec::with_capacity(buf.len() / 4),
                    Level::Precise(level as i32),
                );
                encoder.write_all(&buf).await?;
                encoder.shutdown().await?;
                Bytes::from(encoder.into_inner())
            }
            None => buf,
        };

        Ok(compressed_buf)
    }
}

impl FromWireFormat for InterpretedWalRecords {
    type T = Self;

    async fn from_wire(
        buf: &Bytes,
        format: InterpretedFormat,
        compression: Option<Compression>,
    ) -> Result<Self, FromWireFormatError> {
        let decompressed_buf = match compression {
            Some(Compression::Zstd { .. }) => {
                use async_compression::tokio::write::ZstdDecoder;
                let mut decoded_buf = Vec::with_capacity(buf.len());
                let mut decoder = ZstdDecoder::new(&mut decoded_buf);
                decoder.write_all(buf).await?;
                decoder.flush().await?;
                Bytes::from(decoded_buf)
            }
            None => buf.clone(),
        };

        match format {
            InterpretedFormat::Bincode => {
                InterpretedWalRecords::des(&decompressed_buf).map_err(FromWireFormatError::Bincode)
            }
            InterpretedFormat::Protobuf => {
                let proto = proto::InterpretedWalRecords::decode(decompressed_buf)
                    .map_err(|e| FromWireFormatError::Protobuf(e.into()))?;
                InterpretedWalRecords::try_from(proto)
                    .map_err(|e| FromWireFormatError::Protobuf(e.into()))
            }
        }
    }
}

impl TryFrom<InterpretedWalRecords> for proto::InterpretedWalRecords {
    type Error = SerializeError;

    fn try_from(value: InterpretedWalRecords) -> Result<Self, Self::Error> {
        let records = value
            .records
            .into_iter()
            .map(proto::InterpretedWalRecord::try_from)
            .collect::<Result<Vec<_>, _>>()?;
        Ok(proto::InterpretedWalRecords {
            records,
            next_record_lsn: value.next_record_lsn.map(|l| l.0),
        })
    }
}

impl TryFrom<InterpretedWalRecord> for proto::InterpretedWalRecord {
    type Error = SerializeError;

    fn try_from(value: InterpretedWalRecord) -> Result<Self, Self::Error> {
        let metadata_record = value
            .metadata_record
            .map(|meta_rec| -> Result<Vec<u8>, Self::Error> {
                let mut buf = Vec::new();
                meta_rec.ser_into(&mut buf)?;
                Ok(buf)
            })
            .transpose()?;

        Ok(proto::InterpretedWalRecord {
            metadata_record,
            batch: Some(proto::SerializedValueBatch::from(value.batch)),
            next_record_lsn: value.next_record_lsn.0,
            flush_uncommitted: matches!(value.flush_uncommitted, FlushUncommittedRecords::Yes),
            xid: value.xid,
        })
    }
}

impl From<SerializedValueBatch> for proto::SerializedValueBatch {
    fn from(value: SerializedValueBatch) -> Self {
        proto::SerializedValueBatch {
            raw: value.raw,
            metadata: value
                .metadata
                .into_iter()
                .map(proto::ValueMeta::from)
                .collect(),
            max_lsn: value.max_lsn.0,
            len: value.len as u64,
        }
    }
}

impl From<ValueMeta> for proto::ValueMeta {
    fn from(value: ValueMeta) -> Self {
        match value {
            ValueMeta::Observed(obs) => proto::ValueMeta {
                r#type: proto::ValueMetaType::Observed.into(),
                key: Some(proto::CompactKey::from(obs.key)),
                lsn: obs.lsn.0,
                batch_offset: None,
                len: None,
                will_init: None,
            },
            ValueMeta::Serialized(ser) => proto::ValueMeta {
                r#type: proto::ValueMetaType::Serialized.into(),
                key: Some(proto::CompactKey::from(ser.key)),
                lsn: ser.lsn.0,
                batch_offset: Some(ser.batch_offset),
                len: Some(ser.len as u64),
                will_init: Some(ser.will_init),
            },
        }
    }
}

impl From<CompactKey> for proto::CompactKey {
    fn from(value: CompactKey) -> Self {
        proto::CompactKey {
            high: (value.raw() >> 64) as u64,
            low: value.raw() as u64,
        }
    }
}

impl TryFrom<proto::InterpretedWalRecords> for InterpretedWalRecords {
    type Error = TranscodeError;

    fn try_from(value: proto::InterpretedWalRecords) -> Result<Self, Self::Error> {
        let records = value
            .records
            .into_iter()
            .map(InterpretedWalRecord::try_from)
            .collect::<Result<_, _>>()?;

        Ok(InterpretedWalRecords {
            records,
            next_record_lsn: value.next_record_lsn.map(Lsn::from),
        })
    }
}

impl TryFrom<proto::InterpretedWalRecord> for InterpretedWalRecord {
    type Error = TranscodeError;

    fn try_from(value: proto::InterpretedWalRecord) -> Result<Self, Self::Error> {
        let metadata_record = value
            .metadata_record
            .map(|mrec| -> Result<_, DeserializeError> { MetadataRecord::des(&mrec) })
            .transpose()?;

        let batch = {
            let batch = value.batch.ok_or_else(|| {
                TranscodeError::BadInput("InterpretedWalRecord::batch missing".to_string())
            })?;

            SerializedValueBatch::try_from(batch)?
        };

        Ok(InterpretedWalRecord {
            metadata_record,
            batch,
            next_record_lsn: Lsn(value.next_record_lsn),
            flush_uncommitted: if value.flush_uncommitted {
                FlushUncommittedRecords::Yes
            } else {
                FlushUncommittedRecords::No
            },
            xid: value.xid,
        })
    }
}

impl TryFrom<proto::SerializedValueBatch> for SerializedValueBatch {
    type Error = TranscodeError;

    fn try_from(value: proto::SerializedValueBatch) -> Result<Self, Self::Error> {
        let metadata = value
            .metadata
            .into_iter()
            .map(ValueMeta::try_from)
            .collect::<Result<Vec<_>, _>>()?;

        Ok(SerializedValueBatch {
            raw: value.raw,
            metadata,
            max_lsn: Lsn(value.max_lsn),
            len: value.len as usize,
        })
    }
}

impl TryFrom<proto::ValueMeta> for ValueMeta {
    type Error = TranscodeError;

    fn try_from(value: proto::ValueMeta) -> Result<Self, Self::Error> {
        match proto::ValueMetaType::try_from(value.r#type) {
            Ok(proto::ValueMetaType::Serialized) => {
                Ok(ValueMeta::Serialized(SerializedValueMeta {
                    key: value
                        .key
                        .ok_or_else(|| {
                            TranscodeError::BadInput("ValueMeta::key missing".to_string())
                        })?
                        .into(),
                    lsn: Lsn(value.lsn),
                    batch_offset: value.batch_offset.ok_or_else(|| {
                        TranscodeError::BadInput("ValueMeta::batch_offset missing".to_string())
                    })?,
                    len: value.len.ok_or_else(|| {
                        TranscodeError::BadInput("ValueMeta::len missing".to_string())
                    })? as usize,
                    will_init: value.will_init.ok_or_else(|| {
                        TranscodeError::BadInput("ValueMeta::will_init missing".to_string())
                    })?,
                }))
            }
            Ok(proto::ValueMetaType::Observed) => Ok(ValueMeta::Observed(ObservedValueMeta {
                key: value
                    .key
                    .ok_or_else(|| TranscodeError::BadInput("ValueMeta::key missing".to_string()))?
                    .into(),
                lsn: Lsn(value.lsn),
            })),
            Err(_) => Err(TranscodeError::BadInput(format!(
                "Unexpected ValueMeta::type {}",
                value.r#type
            ))),
        }
    }
}

impl From<proto::CompactKey> for CompactKey {
    fn from(value: proto::CompactKey) -> Self {
        (((value.high as i128) << 64) | (value.low as i128)).into()
    }
}

#[test]
fn test_compact_key_with_large_relnode() {
    use pageserver_api::key::Key;

    let inputs = vec![
        Key {
            field1: 0,
            field2: 0x100,
            field3: 0x200,
            field4: 0,
            field5: 0x10,
            field6: 0x5,
        },
        Key {
            field1: 0,
            field2: 0x100,
            field3: 0x200,
            field4: 0x007FFFFF,
            field5: 0x10,
            field6: 0x5,
        },
        Key {
            field1: 0,
            field2: 0x100,
            field3: 0x200,
            field4: 0x00800000,
            field5: 0x10,
            field6: 0x5,
        },
        Key {
            field1: 0,
            field2: 0x100,
            field3: 0x200,
            field4: 0x00800001,
            field5: 0x10,
            field6: 0x5,
        },
        Key {
            field1: 0,
            field2: 0xFFFFFFFF,
            field3: 0xFFFFFFFF,
            field4: 0xFFFFFFFF,
            field5: 0x0,
            field6: 0x0,
        },
    ];

    for input in inputs {
        assert!(input.is_valid_key_on_write_path());
        let compact = input.to_compact();
        let proto: proto::CompactKey = compact.into();
        let from_proto: CompactKey = proto.into();

        assert_eq!(
            compact, from_proto,
            "Round trip failed for key with relnode={:#x}",
            input.field4
        );
    }
}
