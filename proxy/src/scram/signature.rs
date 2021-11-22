//! Tools for client/server signature management.

use super::key::{ScramKey, SCRAM_KEY_LEN};
use hmac::{Hmac, Mac, NewMac};
use sha2::Sha256;

#[derive(Debug)]
pub struct SignatureBuilder<'a> {
    pub client_first_message_bare: &'a str,
    pub server_first_message: &'a str,
    pub client_final_message_without_proof: &'a str,
}

impl SignatureBuilder<'_> {
    pub fn build(&self, key: &ScramKey) -> Signature {
        let mut mac = Hmac::<Sha256>::new_varkey(key.as_ref()).expect("bad key size");

        mac.update(self.client_first_message_bare.as_bytes());
        mac.update(b",");
        mac.update(self.server_first_message.as_bytes());
        mac.update(b",");
        mac.update(self.client_final_message_without_proof.as_bytes());

        // TODO: maybe newer `hmac` et al already migrated to regular arrays?
        let mut signature = [0u8; SCRAM_KEY_LEN];
        signature.copy_from_slice(mac.finalize().into_bytes().as_slice());
        signature.into()
    }
}

#[derive(Debug)]
#[repr(transparent)]
pub struct Signature {
    bytes: [u8; SCRAM_KEY_LEN],
}

impl Signature {
    /// Derive ClientKey from client's signature and proof
    pub fn derive_client_key(&self, proof: &[u8; SCRAM_KEY_LEN]) -> ScramKey {
        let signature = self.as_ref().iter();

        // This is how the proof is calculated:
        //
        // 1. sha256(ClientKey) -> StoredKey
        // 2. hmac_sha256(StoredKey, [messages...]) -> ClientSignature
        // 3. ClientKey ^ ClientSignature -> ClientProof
        //
        // Step 3 implies that we can restore ClientKey from the proof
        // by xoring the latter with the ClientSignature again. Afterwards
        // we can check that the presumed ClientKey meets our expectations.
        let mut bytes = [0u8; SCRAM_KEY_LEN];
        for (i, value) in signature.zip(proof).map(|(x, y)| x ^ y).enumerate() {
            bytes[i] = value;
        }

        bytes.into()
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
