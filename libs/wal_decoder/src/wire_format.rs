use bytes::{BufMut, Bytes, BytesMut};
use pageserver_api::key::CompactKey;
use prost::{DecodeError, EncodeError, Message};
use utils::bin_ser::{BeSer, DeserializeError, SerializeError};
use utils::lsn::Lsn;
use utils::postgres_client::InterpretedFormat;

use crate::models::{
    FlushUncommittedRecords, InterpretedWalRecord, InterpretedWalRecords, MetadataRecord,
};

use crate::serialized_batch::{
    ObservedValueMeta, SerializedValueBatch, SerializedValueMeta, ValueMeta,
};

use crate::models::proto::CompactKey as ProtoCompactKey;
use crate::models::proto::InterpretedWalRecord as ProtoInterpretedWalRecord;
use crate::models::proto::InterpretedWalRecords as ProtoInterpretedWalRecords;
use crate::models::proto::SerializedValueBatch as ProtoSerializedValueBatch;
use crate::models::proto::ValueMeta as ProtoValueMeta;
use crate::models::proto::ValueMetaType as ProtoValueMetaType;

#[derive(Debug, thiserror::Error)]
pub enum ToWireFormatError {
    #[error("{0}")]
    Bincode(#[from] SerializeError),
    #[error("{0}")]
    Protobuf(#[from] ProtobufSerializeError),
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
    fn to_wire(self, format: InterpretedFormat) -> Result<Bytes, ToWireFormatError>;
}

pub trait FromWireFormat {
    type T;
    fn from_wire(buf: &Bytes, format: InterpretedFormat) -> Result<Self::T, FromWireFormatError>;
}

impl ToWireFormat for InterpretedWalRecords {
    fn to_wire(self, format: InterpretedFormat) -> Result<Bytes, ToWireFormatError> {
        match format {
            InterpretedFormat::Bincode => {
                let buf = BytesMut::new();
                let mut buf = buf.writer();
                self.ser_into(&mut buf)?;
                Ok(buf.into_inner().freeze())
            }
            InterpretedFormat::Protobuf => {
                let proto: ProtoInterpretedWalRecords = self.try_into()?;
                let mut buf = BytesMut::new();
                proto
                    .encode(&mut buf)
                    .map_err(|e| ToWireFormatError::Protobuf(e.into()))?;

                Ok(buf.freeze())
            }
        }
    }
}

impl FromWireFormat for InterpretedWalRecords {
    type T = Self;

    fn from_wire(buf: &Bytes, format: InterpretedFormat) -> Result<Self, FromWireFormatError> {
        match format {
            InterpretedFormat::Bincode => {
                InterpretedWalRecords::des(buf).map_err(FromWireFormatError::Bincode)
            }
            InterpretedFormat::Protobuf => {
                let proto = ProtoInterpretedWalRecords::decode(buf.clone())
                    .map_err(|e| FromWireFormatError::Protobuf(e.into()))?;
                InterpretedWalRecords::try_from(proto)
                    .map_err(|e| FromWireFormatError::Protobuf(e.into()))
            }
        }
    }
}

impl TryFrom<InterpretedWalRecords> for ProtoInterpretedWalRecords {
    type Error = SerializeError;

    fn try_from(value: InterpretedWalRecords) -> Result<Self, Self::Error> {
        let records = value
            .records
            .into_iter()
            .map(ProtoInterpretedWalRecord::try_from)
            .collect::<Result<Vec<_>, _>>()?;
        Ok(ProtoInterpretedWalRecords {
            records,
            next_record_lsn: value.next_record_lsn.map(|l| l.0),
        })
    }
}

impl TryFrom<InterpretedWalRecord> for ProtoInterpretedWalRecord {
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

        Ok(ProtoInterpretedWalRecord {
            metadata_record,
            batch: Some(ProtoSerializedValueBatch::from(value.batch)),
            next_record_lsn: value.next_record_lsn.0,
            flush_uncommitted: matches!(value.flush_uncommitted, FlushUncommittedRecords::Yes),
            xid: value.xid,
        })
    }
}

impl From<SerializedValueBatch> for ProtoSerializedValueBatch {
    fn from(value: SerializedValueBatch) -> Self {
        ProtoSerializedValueBatch {
            raw: value.raw,
            metadata: value
                .metadata
                .into_iter()
                .map(ProtoValueMeta::from)
                .collect(),
            max_lsn: value.max_lsn.0,
            len: value.len as u64,
        }
    }
}

impl From<ValueMeta> for ProtoValueMeta {
    fn from(value: ValueMeta) -> Self {
        match value {
            ValueMeta::Observed(obs) => ProtoValueMeta {
                r#type: ProtoValueMetaType::Observed.into(),
                key: Some(ProtoCompactKey::from(obs.key)),
                lsn: obs.lsn.0,
                batch_offset: None,
                len: None,
                will_init: None,
            },
            ValueMeta::Serialized(ser) => ProtoValueMeta {
                r#type: ProtoValueMetaType::Serialized.into(),
                key: Some(ProtoCompactKey::from(ser.key)),
                lsn: ser.lsn.0,
                batch_offset: Some(ser.batch_offset),
                len: Some(ser.len as u64),
                will_init: Some(ser.will_init),
            },
        }
    }
}

impl From<CompactKey> for ProtoCompactKey {
    fn from(value: CompactKey) -> Self {
        ProtoCompactKey {
            high: (value.raw() >> 64) as i64,
            low: value.raw() as i64,
        }
    }
}

impl TryFrom<ProtoInterpretedWalRecords> for InterpretedWalRecords {
    type Error = TranscodeError;

    fn try_from(value: ProtoInterpretedWalRecords) -> Result<Self, Self::Error> {
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

impl TryFrom<ProtoInterpretedWalRecord> for InterpretedWalRecord {
    type Error = TranscodeError;

    fn try_from(value: ProtoInterpretedWalRecord) -> Result<Self, Self::Error> {
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

impl TryFrom<ProtoSerializedValueBatch> for SerializedValueBatch {
    type Error = TranscodeError;

    fn try_from(value: ProtoSerializedValueBatch) -> Result<Self, Self::Error> {
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

impl TryFrom<ProtoValueMeta> for ValueMeta {
    type Error = TranscodeError;

    fn try_from(value: ProtoValueMeta) -> Result<Self, Self::Error> {
        match ProtoValueMetaType::try_from(value.r#type) {
            Ok(ProtoValueMetaType::Serialized) => Ok(ValueMeta::Serialized(SerializedValueMeta {
                key: value
                    .key
                    .ok_or_else(|| TranscodeError::BadInput("ValueMeta::key missing".to_string()))?
                    .into(),
                lsn: Lsn(value.lsn),
                batch_offset: value.batch_offset.ok_or_else(|| {
                    TranscodeError::BadInput("ValueMeta::batch_offset missing".to_string())
                })?,
                len: value
                    .len
                    .ok_or_else(|| TranscodeError::BadInput("ValueMeta::len missing".to_string()))?
                    as usize,
                will_init: value.will_init.ok_or_else(|| {
                    TranscodeError::BadInput("ValueMeta::will_init missing".to_string())
                })?,
            })),
            Ok(ProtoValueMetaType::Observed) => Ok(ValueMeta::Observed(ObservedValueMeta {
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

impl From<ProtoCompactKey> for CompactKey {
    fn from(value: ProtoCompactKey) -> Self {
        (((value.high as i128) << 64) | (value.low as i128)).into()
    }
}
