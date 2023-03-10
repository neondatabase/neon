// For details about authentication see docs/authentication.md
//
// TODO: use ed25519 keys
// Relevant issue: https://github.com/Keats/jsonwebtoken/issues/162

use serde;
use std::fs;
use std::path::Path;

use anyhow::Result;
use jsonwebtoken::{
    decode, encode, Algorithm, Algorithm::*, DecodingKey, EncodingKey, Header, TokenData,
    Validation,
};
use serde::{Deserialize, Serialize};
use serde_with::{serde_as, DisplayFromStr};

use crate::id::TenantId;

/// Algorithms accepted during validation.
///
/// Accept all RSA-based algorithms. We pass this list to jsonwebtoken::decode,
/// which checks that the algorithm in the token is one of these.
///
/// XXX: It also fails the validation if there are any algorithms in this list that belong
/// to different family than the token's algorithm. In other words, we can *not* list any
/// non-RSA algorithms here, or the validation always fails with InvalidAlgorithm error.
const ACCEPTED_ALGORITHMS: &[Algorithm] = &[RS256, RS384, RS512];

/// Algorithm to use when generating a new token in [`encode_from_key_file`]
const ENCODE_ALGORITHM: Algorithm = Algorithm::RS256;

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum Scope {
    // Provides access to all data for a specific tenant (specified in `struct Claims` below)
    // TODO: join these two?
    Tenant,
    // Provides blanket access to all tenants on the pageserver plus pageserver-wide APIs.
    // Should only be used e.g. for status check/tenant creation/list.
    PageServerApi,
    // Provides blanket access to all data on the safekeeper plus safekeeper-wide APIs.
    // Should only be used e.g. for status check.
    // Currently also used for connection from any pageserver to any safekeeper.
    SafekeeperData,
}

/// JWT payload. See docs/authentication.md for the format
#[serde_as]
#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
pub struct Claims {
    #[serde(default)]
    #[serde_as(as = "Option<DisplayFromStr>")]
    pub tenant_id: Option<TenantId>,
    pub scope: Scope,
}

impl Claims {
    pub fn new(tenant_id: Option<TenantId>, scope: Scope) -> Self {
        Self { tenant_id, scope }
    }
}

pub struct JwtAuth {
    decoding_key: DecodingKey,
    validation: Validation,
}

impl JwtAuth {
    pub fn new(decoding_key: DecodingKey) -> Self {
        let mut validation = Validation::default();
        validation.algorithms = ACCEPTED_ALGORITHMS.into();
        // The default 'required_spec_claims' is 'exp'. But we don't want to require
        // expiration.
        validation.required_spec_claims = [].into();
        Self {
            decoding_key,
            validation,
        }
    }

    pub fn from_key_path(key_path: &Path) -> Result<Self> {
        let public_key = fs::read(key_path)?;
        Ok(Self::new(DecodingKey::from_rsa_pem(&public_key)?))
    }

    pub fn decode(&self, token: &str) -> Result<TokenData<Claims>> {
        Ok(decode(token, &self.decoding_key, &self.validation)?)
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
pub fn encode_from_key_file(claims: &Claims, key_data: &[u8]) -> Result<String> {
    let key = EncodingKey::from_rsa_pem(key_data)?;
    Ok(encode(&Header::new(ENCODE_ALGORITHM), claims, &key)?)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::str::FromStr;

    // generated with:
    //
    // openssl genpkey -algorithm rsa -out storage-auth-priv.pem
    // openssl pkey -in storage-auth-priv.pem -pubout -out storage-auth-pub.pem
    const TEST_PUB_KEY_RSA: &[u8] = br#"
-----BEGIN PUBLIC KEY-----
MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEAy6OZ+/kQXcueVJA/KTzO
v4ljxylc/Kcb0sXWuXg1GB8k3nDA1gK66LFYToH0aTnqrnqG32Vu6wrhwuvqsZA7
jQvP0ZePAbWhpEqho7EpNunDPcxZ/XDy5TQlB1P58F9I3lkJXDC+DsHYLuuzwhAv
vo2MtWRdYlVHblCVLyZtANHhUMp2HUhgjHnJh5UrLIKOl4doCBxkM3rK0wjKsNCt
M92PCR6S9rvYzldfeAYFNppBkEQrXt2CgUqZ4KaS4LXtjTRUJxljijA4HWffhxsr
euRu3ufq8kVqie7fum0rdZZSkONmce0V0LesQ4aE2jB+2Sn48h6jb4dLXGWdq8TV
wQIDAQAB
-----END PUBLIC KEY-----
"#;
    const TEST_PRIV_KEY_RSA: &[u8] = br#"
-----BEGIN PRIVATE KEY-----
MIIEvQIBADANBgkqhkiG9w0BAQEFAASCBKcwggSjAgEAAoIBAQDLo5n7+RBdy55U
kD8pPM6/iWPHKVz8pxvSxda5eDUYHyTecMDWArrosVhOgfRpOequeobfZW7rCuHC
6+qxkDuNC8/Rl48BtaGkSqGjsSk26cM9zFn9cPLlNCUHU/nwX0jeWQlcML4Owdgu
67PCEC++jYy1ZF1iVUduUJUvJm0A0eFQynYdSGCMecmHlSssgo6Xh2gIHGQzesrT
CMqw0K0z3Y8JHpL2u9jOV194BgU2mkGQRCte3YKBSpngppLgte2NNFQnGWOKMDgd
Z9+HGyt65G7e5+ryRWqJ7t+6bSt1llKQ42Zx7RXQt6xDhoTaMH7ZKfjyHqNvh0tc
ZZ2rxNXBAgMBAAECggEAVz3u4Wlx3o02dsoZlSQs+xf0PEX3RXKeU+1YMbtTG9Nz
6yxpIQaoZrpbt76rJE2gwkFR+PEu1NmjoOuLb6j4KlQuI4AHz1auOoGSwFtM6e66
K4aZ4x95oEJ3vqz2fkmEIWYJwYpMUmwvnuJx76kZm0xvROMLsu4QHS2+zCVtO5Tr
hvS05IMVuZ2TdQBZw0+JaFdwXbgDjQnQGY5n9MoTWSx1a4s/FF4Eby65BbDutcpn
Vt3jQAOmO1X2kbPeWSGuPJRzyUs7Kg8qfeglBIR3ppGP3vPYAdWX+ho00bmsVkSp
Q8vjul6C3WiM+kjwDxotHSDgbl/xldAl7OqPh0bfAQKBgQDnycXuq14Vg8nZvyn9
rTnvucO8RBz5P6G+FZ+44cAS2x79+85onARmMnm+9MKYLSMo8fOvsK034NDI68XM
04QQ/vlfouvFklMTGJIurgEImTZbGCmlMYCvFyIxaEWixon8OpeI4rFe4Hmbiijh
PxhxWg221AwvBS2sco8J/ylEkQKBgQDg6Rh2QYb/j0Wou1rJPbuy3NhHofd5Rq35
4YV3f2lfVYcPrgRhwe3T9SVII7Dx8LfwzsX5TAlf48ESlI3Dzv40uOCDM+xdtBRI
r96SfSm+jup6gsXU3AsdNkrRK3HoOG9Z/TkrUp213QAIlVnvIx65l4ckFMlpnPJ0
lo1LDXZWMQKBgFArzjZ7N5OhfdO+9zszC3MLgdRAivT7OWqR+CjujIz5FYMr8Xzl
WfAvTUTrS9Nu6VZkObFvHrrRG+YjBsuN7YQjbQXTSFGSBwH34bgbn2fl9pMTjHQC
50uoaL9GHa/rlBaV/YvvPQJgCi/uXa1rMX0jdNLkDULGO8IF7cu7Yf7BAoGBAIUU
J29BkpmAst0GDs/ogTlyR18LTR0rXyHt+UUd1MGeH859TwZw80JpWWf4BmkB4DTS
hH3gKePdJY7S65ci0XNsuRupC4DeXuorde0DtkGU2tUmr9wlX0Ynq9lcdYfMbMa4
eK1TsxG69JwfkxlWlIWITWRiEFM3lJa7xlrUWmLhAoGAFpKWF/hn4zYg3seU9gai
EYHKSbhxA4mRb+F0/9IlCBPMCqFrL5yftUsYIh2XFKn8+QhO97Nmk8wJSK6TzQ5t
ZaSRmgySrUUhx4nZ/MgqWCFv8VUbLM5MBzwxPKhXkSTfR4z2vLYLJwVY7Tb4kZtp
8ismApXVGHpOCstzikV9W7k=
-----END PRIVATE KEY-----
"#;

    #[test]
    fn test_decode() -> Result<(), anyhow::Error> {
        let expected_claims = Claims {
            tenant_id: Some(TenantId::from_str("3d1f7595b468230304e0b73cecbcb081")?),
            scope: Scope::Tenant,
        };

        // Here are tokens containing the following payload, signed using TEST_PRIV_KEY_RSA
        // using RS512, RS384 and RS256 algorithms:
        //
        // ```
        // {
        //   "scope": "tenant",
        //   "tenant_id": "3d1f7595b468230304e0b73cecbcb081",
        //   "iss": "neon.controlplane",
        //   "exp": 1709200879,
        //   "iat": 1678442479
        // }
        // ```
        //
        // These were encoded with the online debugger at https://jwt.io
        //
        let encoded_rs512 = "eyJhbGciOiJSUzUxMiIsInR5cCI6IkpXVCJ9.eyJzY29wZSI6InRlbmFudCIsInRlbmFudF9pZCI6IjNkMWY3NTk1YjQ2ODIzMDMwNGUwYjczY2VjYmNiMDgxIiwiaXNzIjoibmVvbi5jb250cm9scGxhbmUiLCJleHAiOjE3MDkyMDA4NzksImlhdCI6MTY3ODQ0MjQ3OX0.QmqfteDQmDGoxQ5EFkasbt35Lx0W0Nh63muQnYZvFq93DSh4ZbOG9Mc4yaiXZoiS5HgeKtFKv3mbWkDqjz3En06aY17hWwguBtAsGASX48lYeCPADYGlGAuaWnOnVRwe3iiOC7tvPFvwX_45S84X73sNUXyUiXv6nLdcDqVXudtNrGST_DnZDnjuUJX11w7sebtKqQQ8l9-iGHiXOl5yevpMCoB1OcTWcT6DfDtffoNuMHDC3fyhmEGG5oKAt1qBybqAIiyC9-UBAowRZXhdfxrzUl-I9jzKWvk85c5ulhVRwbPeP6TTTlPKwFzBNHg1i2U-1GONew5osQ3aoptwsA";

        let encoded_rs384 = "eyJhbGciOiJSUzM4NCIsInR5cCI6IkpXVCJ9.eyJzY29wZSI6InRlbmFudCIsInRlbmFudF9pZCI6IjNkMWY3NTk1YjQ2ODIzMDMwNGUwYjczY2VjYmNiMDgxIiwiaXNzIjoibmVvbi5jb250cm9scGxhbmUiLCJleHAiOjE3MDkyMDA4NzksImlhdCI6MTY3ODQ0MjQ3OX0.qqk4nkxKzOJP38c_g57_w_SfdQVmCsDT_bsLmdFj_N6LIB22gr6U6_P_5mvk3pIAsp0VCTDwPrCU908TxqjibEkwvQoJwbogHamSGHpD7eJBxGblSnA-Nr3MlEMxpFtec8QokSm6C5mH7DoBYjB2xzeOlxAmpR2GAzInKiMkU4kZ_OcqqrmVcMXY_6VnbxZWMekuw56zE1-PP_qNF1HvYOH-P08ONP8qdo5UPtBG7QBEFlCqZXJZCFihQaI4Vzil9rDuZGCm3I7xQJ8-yh1PX3BTbGo8EzqLdRyBeTpr08UTuRbp_MJDWevHpP3afvJetAItqZXIoZQrbJjcByHqKw";

        let encoded_rs256 = "eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9.eyJzY29wZSI6InRlbmFudCIsInRlbmFudF9pZCI6IjNkMWY3NTk1YjQ2ODIzMDMwNGUwYjczY2VjYmNiMDgxIiwiaXNzIjoibmVvbi5jb250cm9scGxhbmUiLCJleHAiOjE3MDkyMDA4NzksImlhdCI6MTY3ODQ0MjQ3OX0.dF2N9KXG8ftFKHYbd5jQtXMQqv0Ej8FISGp1b_dmqOCotXj5S1y2AWjwyB_EXHM77JXfbEoJPAPrFFBNfd8cWtkCSTvpxWoHaecGzegDFGv5ZSc5AECFV1Daahc3PI3jii9wEiGkFOiwiBNfZ5INomOAsV--XXxlqIwKbTcgSYI7lrOTfecXAbAHiMKQlQYiIBSGnytRCgafhRkyGzPAL8ismthFJ9RHfeejyskht-9GbVHURw02bUyijuHEulpf9eEY3ZiB28de6jnCdU7ftIYaUMaYWt0nZQGkzxKPSfSLZNy14DTOYLDS04DVstWQPqnCUW_ojg0wJETOOfo9Zw";

        // Check that RS512, RS384 and RS256 tokens can all be validated
        let auth = JwtAuth::new(DecodingKey::from_rsa_pem(TEST_PUB_KEY_RSA)?);

        for encoded in [encoded_rs512, encoded_rs384, encoded_rs256] {
            let claims_from_token = auth.decode(encoded)?.claims;
            assert_eq!(claims_from_token, expected_claims);
        }
        Ok(())
    }

    #[test]
    fn test_encode() -> Result<(), anyhow::Error> {
        let claims = Claims {
            tenant_id: Some(TenantId::from_str("3d1f7595b468230304e0b73cecbcb081")?),
            scope: Scope::Tenant,
        };

        let encoded = encode_from_key_file(&claims, TEST_PRIV_KEY_RSA)?;

        // decode it back
        let auth = JwtAuth::new(DecodingKey::from_rsa_pem(TEST_PUB_KEY_RSA)?);
        let decoded = auth.decode(&encoded)?;

        assert_eq!(decoded.claims, claims);

        Ok(())
    }
}
