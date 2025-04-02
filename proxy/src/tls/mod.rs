pub mod client_config;
pub mod postgres_rustls;
pub mod server_config;

use anyhow::Context;
use rustls::pki_types::CertificateDer;
use sha2::{Digest, Sha256};
use tracing::{error, info};
use x509_cert::der::{Reader, SliceReader, oid};

/// <https://github.com/postgres/postgres/blob/ca481d3c9ab7bf69ff0c8d71ad3951d407f6a33c/src/include/libpq/pqcomm.h#L159>
pub const PG_ALPN_PROTOCOL: &[u8] = b"postgresql";

/// Channel binding parameter
///
/// <https://www.rfc-editor.org/rfc/rfc5929#section-4>
/// Description: The hash of the TLS server's certificate as it
/// appears, octet for octet, in the server's Certificate message.  Note
/// that the Certificate message contains a certificate_list, in which
/// the first element is the server's certificate.
///
/// The hash function is to be selected as follows:
///
/// * if the certificate's signatureAlgorithm uses a single hash
///   function, and that hash function is either MD5 or SHA-1, then use SHA-256;
///
/// * if the certificate's signatureAlgorithm uses a single hash
///   function and that hash function neither MD5 nor SHA-1, then use
///   the hash function associated with the certificate's
///   signatureAlgorithm;
///
/// * if the certificate's signatureAlgorithm uses no hash functions or
///   uses multiple hash functions, then this channel binding type's
///   channel bindings are undefined at this time (updates to is channel
///   binding type may occur to address this issue if it ever arises).
#[derive(Debug, Clone, Copy)]
pub enum TlsServerEndPoint {
    Sha256([u8; 32]),
    Undefined,
}

impl TlsServerEndPoint {
    pub fn new(cert: &CertificateDer<'_>) -> anyhow::Result<Self> {
        const SHA256_OIDS: &[oid::ObjectIdentifier] = &[
            // I'm explicitly not adding MD5 or SHA1 here... They're bad.
            oid::db::rfc5912::ECDSA_WITH_SHA_256,
            oid::db::rfc5912::SHA_256_WITH_RSA_ENCRYPTION,
        ];

        let certificate = SliceReader::new(cert)
            .context("Failed to parse cerficiate")?
            .decode::<x509_cert::Certificate>()
            .context("Failed to parse cerficiate")?;

        let subject = certificate.tbs_certificate.subject;
        info!(%subject, "parsing TLS certificate");

        let oid = certificate.signature_algorithm.oid;
        if SHA256_OIDS.contains(&oid) {
            let tls_server_end_point: [u8; 32] = Sha256::new().chain_update(cert).finalize().into();
            info!(%subject, tls_server_end_point = %base64::encode(tls_server_end_point), "determined channel binding");
            Ok(Self::Sha256(tls_server_end_point))
        } else {
            error!(%subject, "unknown channel binding");
            Ok(Self::Undefined)
        }
    }

    pub fn supported(&self) -> bool {
        !matches!(self, TlsServerEndPoint::Undefined)
    }
}
