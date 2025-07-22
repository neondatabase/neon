// For details about authentication see docs/authentication.md

use std::fmt::Display;
use std::fs;
use std::sync::Arc;
use std::{borrow::Cow, io, path::Path};

use anyhow::Result;
use arc_swap::ArcSwap;
use camino::Utf8Path;
use jsonwebtoken::{
    Algorithm, DecodingKey, EncodingKey, Header, TokenData, Validation, decode, encode,
};
use oid_registry::OID_PKCS1_RSAENCRYPTION;
use pem::Pem;
use rustls_pki_types::CertificateDer;
use serde::{Deserialize, Deserializer, Serialize, de::DeserializeOwned};
use uuid::Uuid;

use crate::id::TenantId;

/// Signature algorithms to use. We allow EdDSA and RSA/SHA-256.
const STORAGE_TOKEN_ALGORITHM: Algorithm = Algorithm::EdDSA;
const HADRON_STORAGE_TOKEN_ALGORITHM: Algorithm = Algorithm::RS256;

#[derive(Debug, Serialize, Deserialize, Clone, Copy, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum Scope {
    /// Provides access to all data for a specific tenant (specified in `struct Claims` below)
    // TODO: join these two?
    Tenant,
    /// Provides access to all data for a specific tenant, but based on endpoint ID. This token scope
    /// is only used by compute to fetch the spec for a specific endpoint. The spec contains a Tenant-scoped
    /// token authorizing access to all data of a tenant, so the spec-fetch API requires a TenantEndpoint
    /// scope token to ensure that untrusted compute nodes can't fetch spec for arbitrary endpoints.
    TenantEndpoint,
    /// Provides blanket access to all tenants on the pageserver plus pageserver-wide APIs.
    /// Should only be used e.g. for status check/tenant creation/list.
    PageServerApi,
    /// Provides blanket access to all data on the safekeeper plus safekeeper-wide APIs.
    /// Should only be used e.g. for status check.
    /// Currently also used for connection from any pageserver to any safekeeper.
    SafekeeperData,
    /// The scope used by pageservers in upcalls to storage controller and cloud control plane
    #[serde(rename = "generations_api")]
    GenerationsApi,
    /// Allows access to control plane managment API and all storage controller endpoints.
    Admin,

    /// Allows access to control plane & storage controller endpoints used in infrastructure automation (e.g. node registration)
    Infra,

    /// Allows access to storage controller APIs used by the scrubber, to interrogate the state
    /// of a tenant & post scrub results.
    Scrubber,

    /// This scope is used for communication with other storage controller instances.
    /// At the time of writing, this is only used for the step down request.
    #[serde(rename = "controller_peer")]
    ControllerPeer,
}

fn deserialize_empty_string_as_none_uuid<'de, D>(deserializer: D) -> Result<Option<Uuid>, D::Error>
where
    D: Deserializer<'de>,
{
    let opt = Option::<String>::deserialize(deserializer)?;
    match opt.as_deref() {
        Some("") => Ok(None),
        Some(s) => Uuid::parse_str(s)
            .map(Some)
            .map_err(serde::de::Error::custom),
        None => Ok(None),
    }
}

/// JWT payload. See docs/authentication.md for the format
#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
pub struct Claims {
    #[serde(default)]
    pub tenant_id: Option<TenantId>,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        // Neon control plane includes this field as empty in the claims.
        // Consider it None in those cases.
        deserialize_with = "deserialize_empty_string_as_none_uuid"
    )]
    pub endpoint_id: Option<Uuid>,
    pub scope: Scope,
}

impl Claims {
    pub fn new(tenant_id: Option<TenantId>, scope: Scope) -> Self {
        Self {
            tenant_id,
            scope,
            endpoint_id: None,
        }
    }

    pub fn new_for_endpoint(endpoint_id: Uuid) -> Self {
        Self {
            tenant_id: None,
            endpoint_id: Some(endpoint_id),
            scope: Scope::TenantEndpoint,
        }
    }
}

pub struct SwappableJwtAuth(ArcSwap<JwtAuth>);

impl SwappableJwtAuth {
    pub fn new(jwt_auth: JwtAuth) -> Self {
        SwappableJwtAuth(ArcSwap::new(Arc::new(jwt_auth)))
    }
    pub fn swap(&self, jwt_auth: JwtAuth) {
        self.0.swap(Arc::new(jwt_auth));
    }
    pub fn decode<D: DeserializeOwned>(
        &self,
        token: &str,
    ) -> std::result::Result<TokenData<D>, AuthError> {
        self.0.load().decode(token)
    }
}

impl std::fmt::Debug for SwappableJwtAuth {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Swappable({:?})", self.0.load())
    }
}

#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub struct AuthError(pub Cow<'static, str>);

impl Display for AuthError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

pub struct JwtAuth {
    decoding_keys: Vec<DecodingKey>,
    validation: Validation,
}

impl JwtAuth {
    pub fn new(decoding_keys: Vec<DecodingKey>) -> Self {
        let mut validation = Validation::default();
        validation.algorithms = vec![STORAGE_TOKEN_ALGORITHM];
        // The default 'required_spec_claims' is 'exp'. But we don't want to require
        // expiration.
        validation.required_spec_claims = [].into();
        Self {
            decoding_keys,
            validation,
        }
    }

    pub fn from_key_path(key_path: &Utf8Path) -> Result<Self> {
        let metadata = key_path.metadata()?;
        let decoding_keys = if metadata.is_dir() {
            let mut keys = Vec::new();
            for entry in fs::read_dir(key_path)? {
                let path = entry?.path();
                if !path.is_file() {
                    // Ignore directories (don't recurse)
                    continue;
                }
                let public_key = fs::read(path)?;
                keys.push(DecodingKey::from_ed_pem(&public_key)?);
            }
            keys
        } else if metadata.is_file() {
            let public_key = fs::read(key_path)?;
            vec![DecodingKey::from_ed_pem(&public_key)?]
        } else {
            anyhow::bail!("path is neither a directory or a file")
        };
        if decoding_keys.is_empty() {
            anyhow::bail!(
                "Configured for JWT auth with zero decoding keys. All JWT gated requests would be rejected."
            );
        }
        Ok(Self::new(decoding_keys))
    }

    // Helper function to parse a X509 certificate file and extract the RSA public keys from it as `DecodingKey`s.
    // - `ceritificate_file_path`: the path to the certificate file. It must be a file, not a directory or anything else.
    // Returns the successfully extracted decoding keys. Non-RSA keys and non-X509-parsable certificates are skipped.
    // Multuple keys may be returned because a single file can contain multiple certificates.
    fn extract_rsa_decoding_keys_from_certificate<P: AsRef<Path>>(
        certificate_file_path: P,
    ) -> Result<Vec<DecodingKey>> {
        let certs: io::Result<Vec<CertificateDer<'static>>> = rustls_pemfile::certs(
            &mut io::BufReader::new(fs::File::open(certificate_file_path)?),
        )
        .collect();

        Ok(certs?
            .iter()
            .filter_map(
                |cert| match x509_parser::parse_x509_certificate(cert) {
                    Ok((_, cert)) => {
                        let public_key = cert.public_key();
                        // Note that we are just extracting the public key from the certificate, not the signature.
                        // So the algorithm is just the asymmetric crypto such as RSA, no hashes of or anything like
                        // that.
                        if *public_key.algorithm.oid() == OID_PKCS1_RSAENCRYPTION {
                            Some(DecodingKey::from_rsa_der(&public_key.subject_public_key.data))
                        } else {
                            tracing::warn!(
                                "Unsupported public key algorithm: {:?} found in certificate. Skipping.",
                                public_key.algorithm
                            );
                            None
                        }
                    }
                    Err(e) => {
                        tracing::warn!("Error parsing certificate: {}. Skipping.", e);
                        None
                    }
                },
            )
            .collect())
    }

    /// Create a `JwtAuth` that can decode tokens using RSA public keys in X509 certificates from the given path.
    /// - `cert_path`: the path to a directory or a file containing X509 certificates. If it is a directory, all files
    ///                under the first level of the directory will be inspected for certificates.
    /// Returns the `JwtAuth` with the decoding keys extracted from the certificates, or error.
    /// Used by Hadron.
    pub fn from_cert_path(cert_path: &Utf8Path) -> Result<Self> {
        tracing::info!(
            "Loading public keys in certificates from path: {}",
            cert_path
        );

        let mut decoding_keys = Vec::new();

        let metadata = cert_path.metadata()?;
        if metadata.is_dir() {
            for entry in fs::read_dir(cert_path)? {
                let path = entry?.path();
                if !path.is_file() {
                    // Ignore directories (don't recurse)
                    continue;
                }
                decoding_keys.extend(
                    Self::extract_rsa_decoding_keys_from_certificate(path).unwrap_or_default(),
                );
            }
        } else if metadata.is_file() {
            decoding_keys.extend(
                Self::extract_rsa_decoding_keys_from_certificate(cert_path).unwrap_or_default(),
            );
        } else {
            anyhow::bail!("{cert_path} is neither a directory or a file")
        }
        if decoding_keys.is_empty() {
            anyhow::bail!(
                "Configured for JWT auth with zero decoding keys. All JWT gated requests would be rejected."
            );
        }

        // Note that we need to create a `JwtAuth` with a different `validation` from the default one created by `new()` in this case
        // because the `jsonwebtoken` crate requires that all algorithms in `validation.algorithms` belong to the same algorithm family
        // (all RSA or all EdDSA).
        let mut validation = Validation::default();
        validation.algorithms = vec![HADRON_STORAGE_TOKEN_ALGORITHM];
        validation.required_spec_claims = [].into();
        Ok(Self {
            validation,
            decoding_keys,
        })
    }

    pub fn from_key(key: String) -> Result<Self> {
        Ok(Self::new(vec![DecodingKey::from_ed_pem(key.as_bytes())?]))
    }

    /// Attempt to decode the token with the internal decoding keys.
    ///
    /// The function tries the stored decoding keys in succession,
    /// and returns the first yielding a successful result.
    /// If there is no working decoding key, it returns the last error.
    pub fn decode<D: DeserializeOwned>(
        &self,
        token: &str,
    ) -> std::result::Result<TokenData<D>, AuthError> {
        let mut res = None;
        for decoding_key in &self.decoding_keys {
            res = Some(decode(token, decoding_key, &self.validation));
            if let Some(Ok(res)) = res {
                return Ok(res);
            }
        }
        if let Some(res) = res {
            res.map_err(|e| AuthError(Cow::Owned(e.to_string())))
        } else {
            Err(AuthError(Cow::Borrowed("no JWT decoding keys configured")))
        }
    }
}

impl std::fmt::Debug for JwtAuth {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("JwtAuth")
            .field("validation", &self.validation)
            .finish()
    }
}

// this function is used only for testing purposes in CLI e g generate tokens during init
pub fn encode_from_key_file<S: Serialize>(claims: &S, pem: &Pem) -> Result<String> {
    let key = EncodingKey::from_ed_der(pem.contents());
    Ok(encode(&Header::new(STORAGE_TOKEN_ALGORITHM), claims, &key)?)
}

/// Encode (i.e., sign) a Hadron auth token with the given claims and RSA private key. This is used
/// by HCC to sign tokens when deploying compute or returning the compute spec. The resulting token
/// is used by the compute node to authenticate with HCC and PS/SK.
pub fn encode_hadron_token<S: Serialize>(claims: &S, key_data: &[u8]) -> Result<String> {
    let key = EncodingKey::from_rsa_pem(key_data)?;
    encode_hadron_token_with_encoding_key(claims, &key)
}

pub fn encode_hadron_token_with_encoding_key<S: Serialize>(
    claims: &S,
    encoding_key: &EncodingKey,
) -> Result<String> {
    Ok(encode(
        &Header::new(HADRON_STORAGE_TOKEN_ALGORITHM),
        claims,
        encoding_key,
    )?)
}

#[cfg(test)]
mod tests {
    use io::Write;
    use std::str::FromStr;

    use super::*;

    // Generated with:
    //
    // openssl genpkey -algorithm ed25519 -out ed25519-priv.pem
    // openssl pkey -in ed25519-priv.pem -pubout -out ed25519-pub.pem
    const TEST_PUB_KEY_ED25519: &str = r#"
-----BEGIN PUBLIC KEY-----
MCowBQYDK2VwAyEARYwaNBayR+eGI0iXB4s3QxE3Nl2g1iWbr6KtLWeVD/w=
-----END PUBLIC KEY-----
"#;

    const TEST_PRIV_KEY_ED25519: &str = r#"
-----BEGIN PRIVATE KEY-----
MC4CAQAwBQYDK2VwBCIEID/Drmc1AA6U/znNRWpF3zEGegOATQxfkdWxitcOMsIH
-----END PRIVATE KEY-----
"#;

    #[test]
    fn test_decode() {
        let expected_claims = Claims {
            tenant_id: Some(TenantId::from_str("3d1f7595b468230304e0b73cecbcb081").unwrap()),
            endpoint_id: None,
            scope: Scope::Tenant,
        };

        // A test token containing the following payload, signed using TEST_PRIV_KEY_ED25519:
        //
        // ```
        // {
        //   "scope": "tenant",
        //   "tenant_id": "3d1f7595b468230304e0b73cecbcb081",
        //   "iss": "neon.controlplane",
        //   "iat": 1678442479
        // }
        // ```
        //
        let encoded_eddsa = "eyJhbGciOiJFZERTQSIsInR5cCI6IkpXVCJ9.eyJzY29wZSI6InRlbmFudCIsInRlbmFudF9pZCI6IjNkMWY3NTk1YjQ2ODIzMDMwNGUwYjczY2VjYmNiMDgxIiwiaXNzIjoibmVvbi5jb250cm9scGxhbmUiLCJpYXQiOjE2Nzg0NDI0Nzl9.rNheBnluMJNgXzSTTJoTNIGy4P_qe0JUHl_nVEGuDCTgHOThPVr552EnmKccrCKquPeW3c2YUk0Y9Oh4KyASAw";

        // Check it can be validated with the public key
        let auth = JwtAuth::new(vec![
            DecodingKey::from_ed_pem(TEST_PUB_KEY_ED25519.as_bytes()).unwrap(),
        ]);
        let claims_from_token: Claims = auth.decode(encoded_eddsa).unwrap().claims;
        assert_eq!(claims_from_token, expected_claims);
    }

    #[test]
    fn test_encode() {
        let claims = Claims {
            tenant_id: Some(TenantId::from_str("3d1f7595b468230304e0b73cecbcb081").unwrap()),
            endpoint_id: None,
            scope: Scope::Tenant,
        };

        let pem = pem::parse(TEST_PRIV_KEY_ED25519).unwrap();
        let encoded = encode_from_key_file(&claims, &pem).unwrap();

        // decode it back
        let auth = JwtAuth::new(vec![
            DecodingKey::from_ed_pem(TEST_PUB_KEY_ED25519.as_bytes()).unwrap(),
        ]);
        let decoded: TokenData<Claims> = auth.decode(&encoded).unwrap();

        assert_eq!(decoded.claims, claims);
    }

    #[test]
    fn test_decode_with_key_from_certificate() {
        // Tests that we can sign (encode) a token with a RSA private key and verify (decode) it with the
        // corresponding public key extracted from a certificate.

        // Generate two RSA key pairs and create self-signed certificates with it.
        let key_pair_1 = rcgen::KeyPair::generate_for(&rcgen::PKCS_RSA_SHA256).unwrap();
        let key_pair_2 = rcgen::KeyPair::generate_for(&rcgen::PKCS_RSA_SHA256).unwrap();
        let mut params = rcgen::CertificateParams::default();
        params
            .distinguished_name
            .push(rcgen::DnType::CommonName, "eng-brickstore@databricks.com");
        let cert_1 = params.clone().self_signed(&key_pair_1).unwrap();
        let cert_2 = params.self_signed(&key_pair_2).unwrap();

        // Write the certificates and keys to a temporary dir.
        let dir = camino_tempfile::tempdir().unwrap();
        {
            fs::File::create(dir.path().join("cert_1.pem"))
                .unwrap()
                .write_all(cert_1.pem().as_bytes())
                .unwrap();
            fs::File::create(dir.path().join("key_1.pem"))
                .unwrap()
                .write_all(key_pair_1.serialize_pem().as_bytes())
                .unwrap();
            fs::File::create(dir.path().join("cert_2.pem"))
                .unwrap()
                .write_all(cert_2.pem().as_bytes())
                .unwrap();
            fs::File::create(dir.path().join("key_2.pem"))
                .unwrap()
                .write_all(key_pair_2.serialize_pem().as_bytes())
                .unwrap();
        }
        // Instantiate a `JwtAuth` with the certificate path. The resulting `JwtAuth` should extract the RSA public
        // keys out of the X509 certificates and use them as the decoding keys. Since we specified a directory, both
        // X509 certificates will be loaded, but the private key files are skipped.
        let auth = JwtAuth::from_cert_path(dir.path()).unwrap();
        assert_eq!(auth.decoding_keys.len(), 2);

        // Also create a `JwtAuth`, specifying a single certificate file for it to get the decoding key from.
        let auth_cert_1 = JwtAuth::from_cert_path(&dir.path().join("cert_1.pem")).unwrap();
        assert_eq!(auth_cert_1.decoding_keys.len(), 1);

        // Encode tokens with some claims.
        let claims = Claims {
            tenant_id: Some(TenantId::generate()),
            endpoint_id: None,
            scope: Scope::Tenant,
        };
        let encoded_1 =
            encode_hadron_token(&claims, key_pair_1.serialize_pem().as_bytes()).unwrap();
        let encoded_2 =
            encode_hadron_token(&claims, key_pair_2.serialize_pem().as_bytes()).unwrap();

        // Verify that we can decode the token with matching decoding keys (decoding also verifies the signature).
        assert_eq!(auth.decode::<Claims>(&encoded_1).unwrap().claims, claims);
        assert_eq!(auth.decode::<Claims>(&encoded_2).unwrap().claims, claims);
        assert_eq!(
            auth_cert_1.decode::<Claims>(&encoded_1).unwrap().claims,
            claims
        );

        // Verify that the token cannot be decoded with a mismatched decode key.
        assert!(auth_cert_1.decode::<Claims>(&encoded_2).is_err());
    }
}
