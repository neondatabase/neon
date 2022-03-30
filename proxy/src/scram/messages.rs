//! Definitions for SCRAM messages.

use super::base64_decode_array;
use super::channel_binding::ChannelBinding;
use super::key::{ScramKey, SCRAM_KEY_LEN};
use super::signature::SignatureBuilder;
use std::fmt;
use std::ops::Range;

/// Faithfully taken from PostgreSQL.
pub const SCRAM_RAW_NONCE_LEN: usize = 18;

/// Although we ignore all extensions, we still have to validate the message.
fn validate_sasl_extensions<'a>(parts: impl Iterator<Item = &'a str>) -> Option<()> {
    for mut chars in parts.map(|s| s.chars()) {
        let attr = chars.next()?;
        if !('a'..'z').contains(&attr) && !('A'..'Z').contains(&attr) {
            return None;
        }
        let eq = chars.next()?;
        if eq != '=' {
            return None;
        }
    }

    Some(())
}

#[derive(Debug)]
pub struct ClientFirstMessage<'a> {
    /// `client-first-message-bare`.
    pub bare: &'a str,
    /// Channel binding mode.
    pub cbind_flag: ChannelBinding<&'a str>,
    /// (Client username)[<https://github.com/postgres/postgres/blob/94226d4506e66d6e7cbf/src/backend/libpq/auth-scram.c#L13>].
    pub username: &'a str,
    /// Client nonce.
    pub nonce: &'a str,
}

impl<'a> ClientFirstMessage<'a> {
    // NB: FromStr doesn't work with lifetimes
    pub fn parse(input: &'a str) -> Option<Self> {
        let mut parts = input.split(',');

        let cbind_flag = ChannelBinding::parse(parts.next()?)?;

        // PG doesn't support authorization identity,
        // so we don't bother defining GS2 header type
        let authzid = parts.next()?;
        if !authzid.is_empty() {
            return None;
        }

        // Unfortunately, `parts.as_str()` is unstable
        let pos = authzid.as_ptr() as usize - input.as_ptr() as usize + 1;
        let (_, bare) = input.split_at(pos);

        // In theory, these might be preceded by "reserved-mext" (i.e. "m=")
        let username = parts.next()?.strip_prefix("n=")?;
        let nonce = parts.next()?.strip_prefix("r=")?;

        // Validate but ignore auth extensions
        validate_sasl_extensions(parts)?;

        Some(Self {
            bare,
            cbind_flag,
            username,
            nonce,
        })
    }

    pub fn build_server_first_message(
        &self,
        nonce: &[u8; SCRAM_RAW_NONCE_LEN],
        salt_base64: &str,
        iterations: u32,
    ) -> OwnedServerFirstMessage {
        use std::fmt::Write;

        let mut message = String::new();
        write!(&mut message, "r={}", self.nonce).unwrap();
        base64::encode_config_buf(nonce, base64::STANDARD, &mut message);
        let combined_nonce = 2..message.len();
        write!(&mut message, ",s={},i={}", salt_base64, iterations).unwrap();

        // This design guarantees that it's impossible to create a
        // server-first-message without receiving a client-first-message
        OwnedServerFirstMessage {
            message,
            nonce: combined_nonce,
        }
    }
}

#[derive(Debug)]
pub struct ClientFinalMessage<'a> {
    /// `client-final-message-without-proof`.
    pub without_proof: &'a str,
    /// Channel binding data (base64).
    pub channel_binding: &'a str,
    /// Combined client & server nonce.
    pub nonce: &'a str,
    /// Client auth proof.
    pub proof: [u8; SCRAM_KEY_LEN],
}

impl<'a> ClientFinalMessage<'a> {
    // NB: FromStr doesn't work with lifetimes
    pub fn parse(input: &'a str) -> Option<Self> {
        let (without_proof, proof) = input.rsplit_once(',')?;

        let mut parts = without_proof.split(',');
        let channel_binding = parts.next()?.strip_prefix("c=")?;
        let nonce = parts.next()?.strip_prefix("r=")?;

        // Validate but ignore auth extensions
        validate_sasl_extensions(parts)?;

        let proof = base64_decode_array(proof.strip_prefix("p=")?)?;

        Some(Self {
            without_proof,
            channel_binding,
            nonce,
            proof,
        })
    }

    pub fn build_server_final_message(
        &self,
        signature_builder: SignatureBuilder,
        server_key: &ScramKey,
    ) -> String {
        let mut buf = String::from("v=");
        base64::encode_config_buf(
            signature_builder.build(server_key),
            base64::STANDARD,
            &mut buf,
        );

        buf
    }
}

pub struct OwnedServerFirstMessage {
    /// Owned `server-first-message`.
    message: String,
    /// Slice into `message`.
    nonce: Range<usize>,
}

impl OwnedServerFirstMessage {
    /// Extract combined nonce from the message.
    #[inline(always)]
    pub fn nonce(&self) -> &str {
        &self.message[self.nonce.clone()]
    }

    /// Get reference to a text representation of the message.
    #[inline(always)]
    pub fn as_str(&self) -> &str {
        &self.message
    }
}

impl fmt::Debug for OwnedServerFirstMessage {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ServerFirstMessage")
            .field("message", &self.as_str())
            .field("nonce", &self.nonce())
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_client_first_message() {
        use ChannelBinding::*;

        // (Almost) real strings captured during debug sessions
        let cases = [
            (NotSupportedClient, "n,,n=pepe,r=t8JwklwKecDLwSsA72rHmVju"),
            (NotSupportedServer, "y,,n=pepe,r=t8JwklwKecDLwSsA72rHmVju"),
            (
                Required("tls-server-end-point"),
                "p=tls-server-end-point,,n=pepe,r=t8JwklwKecDLwSsA72rHmVju",
            ),
        ];

        for (cb, input) in cases {
            let msg = ClientFirstMessage::parse(input).unwrap();

            assert_eq!(msg.bare, "n=pepe,r=t8JwklwKecDLwSsA72rHmVju");
            assert_eq!(msg.username, "pepe");
            assert_eq!(msg.nonce, "t8JwklwKecDLwSsA72rHmVju");
            assert_eq!(msg.cbind_flag, cb);
        }
    }

    #[test]
    fn parse_client_final_message() {
        let input = [
            "c=eSws",
            "r=iiYEfS3rOgn8S3rtpSdrOsHtPLWvIkdgmHxA0hf3JNOAG4dU",
            "p=SRpfsIVS4Gk11w1LqQ4QvCUBZYQmqXNSDEcHqbQ3CHI=",
        ]
        .join(",");

        let msg = ClientFinalMessage::parse(&input).unwrap();
        assert_eq!(
            msg.without_proof,
            "c=eSws,r=iiYEfS3rOgn8S3rtpSdrOsHtPLWvIkdgmHxA0hf3JNOAG4dU"
        );
        assert_eq!(
            msg.nonce,
            "iiYEfS3rOgn8S3rtpSdrOsHtPLWvIkdgmHxA0hf3JNOAG4dU"
        );
        assert_eq!(
            base64::encode(msg.proof),
            "SRpfsIVS4Gk11w1LqQ4QvCUBZYQmqXNSDEcHqbQ3CHI="
        );
    }
}
