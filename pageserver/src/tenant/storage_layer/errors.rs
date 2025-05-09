use crate::tenant::blob_io::WriteBlobError;

#[derive(Debug, thiserror::Error)]
pub enum PutError {
    #[error(transparent)]
    WriteBlob(WriteBlobError),
    #[error(transparent)]
    Other(anyhow::Error),
}

impl PutError {
    pub fn is_cancel(&self) -> bool {
        match self {
            PutError::WriteBlob(e) => e.is_cancel(),
            PutError::Other(_) => false,
        }
    }
    pub fn into_anyhow(self) -> anyhow::Error {
        match self {
            PutError::WriteBlob(e) => e.into_anyhow(),
            PutError::Other(e) => e,
        }
    }
}
