//! Tools for client/server signature management.

use super::key::{ScramKey, SCRAM_KEY_LEN};

/// A collection of message parts needed to derive the client's signature.
#[derive(Debug)]
pub(crate) struct SignatureBuilder<'a> {
    pub(crate) client_first_message_bare: &'a str,
    pub(crate) server_first_message: &'a str,
    pub(crate) client_final_message_without_proof: &'a str,
}

impl SignatureBuilder<'_> {
    pub(crate) fn build(&self, key: &ScramKey) -> Signature {
        let parts = [
            self.client_first_message_bare.as_bytes(),
            b",",
            self.server_first_message.as_bytes(),
            b",",
            self.client_final_message_without_proof.as_bytes(),
        ];

        super::hmac_sha256(key.as_ref(), parts).into()
    }
}

/// A computed value which, when xored with `ClientProof`,
/// produces `ClientKey` that we need for authentication.
#[derive(Debug)]
#[repr(transparent)]
pub(crate) struct Signature {
    bytes: [u8; SCRAM_KEY_LEN],
}

impl Signature {
    /// Derive `ClientKey` from client's signature and proof.
    pub(crate) fn derive_client_key(&self, proof: &[u8; SCRAM_KEY_LEN]) -> ScramKey {
        // This is how the proof is calculated:
        //
        // 1. sha256(ClientKey) -> StoredKey
        // 2. hmac_sha256(StoredKey, [messages...]) -> ClientSignature
        // 3. ClientKey ^ ClientSignature -> ClientProof
        //
        // Step 3 implies that we can restore ClientKey from the proof
        // by xoring the latter with the ClientSignature. Afterwards we
        // can check that the presumed ClientKey meets our expectations.
        let mut signature = self.bytes;
        for (i, x) in proof.iter().enumerate() {
            signature[i] ^= x;
        }

        signature.into()
    }
}

impl From<[u8; SCRAM_KEY_LEN]> for Signature {
    fn from(bytes: [u8; SCRAM_KEY_LEN]) -> Self {
        Self { bytes }
    }
}

impl AsRef<[u8]> for Signature {
    fn as_ref(&self) -> &[u8] {
        &self.bytes
    }
}
