use bytes::{BufMut, Bytes, BytesMut};
use utils::bin_ser::{BeSer, DeserializeError, SerializeError};
use utils::postgres_client::InterpretedFormat;

use crate::models::InterpretedWalRecord;

#[derive(Debug, thiserror::Error)]
pub enum ToWireFormatError {
    #[error("{0}")]
    Bincode(SerializeError),
}

#[derive(Debug, thiserror::Error)]
pub enum FromWireFormatError {
    #[error("{0}")]
    Bincode(DeserializeError),
}

pub trait ToWireFormat {
    fn to_wire(self, format: InterpretedFormat) -> Result<Bytes, ToWireFormatError>;
}

pub trait FromWireFormat {
    type T;
    fn from_wire(buf: &Bytes, format: InterpretedFormat) -> Result<Self::T, FromWireFormatError>;
}

impl ToWireFormat for Vec<InterpretedWalRecord> {
    fn to_wire(self, format: InterpretedFormat) -> Result<Bytes, ToWireFormatError> {
        match format {
            InterpretedFormat::Bincode => {
                let buf = BytesMut::new();
                let mut buf = buf.writer();
                self.ser_into(&mut buf)
                    .map_err(ToWireFormatError::Bincode)?;
                Ok(buf.into_inner().freeze())
            }
        }
    }
}

impl FromWireFormat for Vec<InterpretedWalRecord> {
    type T = Self;

    fn from_wire(buf: &Bytes, format: InterpretedFormat) -> Result<Self, FromWireFormatError> {
        match format {
            InterpretedFormat::Bincode => {
                Vec::<InterpretedWalRecord>::des(buf).map_err(FromWireFormatError::Bincode)
            }
        }
    }
}
