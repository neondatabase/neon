use bytes::{BufMut, Bytes, BytesMut};
use prost::Message;
use tokio::io::AsyncWriteExt;
use utils::postgres_client::{Compression, InterpretedFormat};

use crate::models::proto;
use crate::models::InterpretedWalRecords;

use crate::protobuf_conversions::TranscodeError;

use utils::bin_ser::{BeSer, DeserializeError, SerializeError};

#[derive(Debug, thiserror::Error)]
pub enum EncodeError {
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
    Encode(#[from] prost::EncodeError),
}

#[derive(Debug, thiserror::Error)]
pub enum DecodeError {
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
    Decode(#[from] prost::DecodeError),
}

pub fn encoder_from_proto(
    format: InterpretedFormat,
    compression: Option<Compression>,
) -> Box<dyn Encoder> {
    match format {
        InterpretedFormat::Bincode => Box::new(BincodeEncoder { compression }),
        InterpretedFormat::Protobuf => Box::new(ProtobufEncoder { compression }),
    }
}

pub fn make_decoder(
    format: InterpretedFormat,
    compression: Option<Compression>,
) -> Box<dyn Decoder> {
    match format {
        InterpretedFormat::Bincode => Box::new(BincodeDecoder { compression }),
        InterpretedFormat::Protobuf => Box::new(ProtobufDecoder { compression }),
    }
}

#[async_trait::async_trait]
pub trait Encoder: Send + Sync {
    async fn encode(&self, records: InterpretedWalRecords) -> Result<Bytes, EncodeError>;
}

#[async_trait::async_trait]
pub trait Decoder: Send + Sync {
    async fn decode(&self, buf: &Bytes) -> Result<InterpretedWalRecords, DecodeError>;
}

struct BincodeDecoder {
    compression: Option<Compression>,
}

#[async_trait::async_trait]
impl Decoder for BincodeDecoder {
    async fn decode(&self, buf: &Bytes) -> Result<InterpretedWalRecords, DecodeError> {
        let decompressed_buf = match self.compression {
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

        InterpretedWalRecords::des(&decompressed_buf).map_err(DecodeError::Bincode)
    }
}

struct BincodeEncoder {
    compression: Option<Compression>,
}

#[async_trait::async_trait]
impl Encoder for BincodeEncoder {
    async fn encode(&self, records: InterpretedWalRecords) -> Result<Bytes, EncodeError> {
        use async_compression::tokio::write::ZstdEncoder;
        use async_compression::Level;

        let buf = BytesMut::new();
        let mut buf = buf.writer();
        records.ser_into(&mut buf)?;
        let buf = buf.into_inner().freeze();

        let compressed_buf = match self.compression {
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

struct ProtobufDecoder {
    compression: Option<Compression>,
}

#[async_trait::async_trait]
impl Decoder for ProtobufDecoder {
    async fn decode(&self, buf: &Bytes) -> Result<InterpretedWalRecords, DecodeError> {
        let decompressed_buf = match self.compression {
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

        let proto = proto::InterpretedWalRecords::decode(decompressed_buf)
            .map_err(|e| DecodeError::Protobuf(e.into()))?;
        InterpretedWalRecords::try_from(proto).map_err(|e| DecodeError::Protobuf(e.into()))
    }
}

struct ProtobufEncoder {
    compression: Option<Compression>,
}

#[async_trait::async_trait]
impl Encoder for ProtobufEncoder {
    async fn encode(&self, records: InterpretedWalRecords) -> Result<Bytes, EncodeError> {
        use async_compression::tokio::write::ZstdEncoder;
        use async_compression::Level;

        let proto: proto::InterpretedWalRecords = records.try_into()?;
        let mut buf = BytesMut::new();
        proto
            .encode(&mut buf)
            .map_err(|e| EncodeError::Protobuf(e.into()))?;

        let buf = buf.freeze();

        let compressed_buf = match self.compression {
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
