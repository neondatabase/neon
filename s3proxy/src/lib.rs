use anyhow::Result;
use axum::extract::{FromRequestParts, Path};
use axum::response::{IntoResponse, Response};
use axum::{RequestPartsExt, http::StatusCode, http::request::Parts};
use axum_extra::TypedHeader;
use axum_extra::headers::{Authorization, authorization::Bearer};
use camino::{Utf8Path, Utf8PathBuf};
use jsonwebtoken::{DecodingKey, Validation};
use remote_storage::{GenericRemoteStorage, RemotePath};
use serde::{Deserialize, Serialize};
use std::fmt::Display;
use std::result::Result as StdResult;
use std::sync::Arc;
use tokio_util::sync::CancellationToken;

// simplified version of utils::auth::JwtAuth
pub struct JwtAuth {
    decoding_key: DecodingKey,
    validation: Validation,
}

pub const VALIDATION_ALGO: jsonwebtoken::Algorithm = jsonwebtoken::Algorithm::EdDSA;
impl JwtAuth {
    pub fn new(key: &[u8]) -> Result<Self> {
        let decoding_key = DecodingKey::from_ed_pem(key)?;
        let mut validation = Validation::new(VALIDATION_ALGO);
        validation.required_spec_claims = [].into();
        Ok(Self {
            decoding_key,
            validation,
        })
    }

    pub fn decode<T: serde::de::DeserializeOwned>(&self, token: &str) -> Result<T> {
        Ok(jsonwebtoken::decode(token, &self.decoding_key, &self.validation).map(|t| t.claims)?)
    }
}

fn normalize_key(key: &str) -> StdResult<Utf8PathBuf, String> {
    let key = clean_utf8(&Utf8PathBuf::from(key));
    if key.starts_with("..") || key == Utf8PathBuf::from(".") || key == Utf8PathBuf::from("/") {
        return Err(format!("invalid key {key}"));
    }
    let Ok(path) = key.strip_prefix("/").map(Utf8PathBuf::from) else {
        return Ok(key);
    };
    Ok(path)
}

// Copied from path_clean crate with PathBuf->Utf8PathBuf
fn clean_utf8(path: &Utf8Path) -> Utf8PathBuf {
    use camino::Utf8Component;
    let mut out = Vec::new();
    for comp in path.components() {
        match comp {
            Utf8Component::CurDir => (),
            Utf8Component::ParentDir => match out.last() {
                Some(Utf8Component::RootDir) => (),
                Some(Utf8Component::Normal(_)) => {
                    out.pop();
                }
                None
                | Some(Utf8Component::CurDir)
                | Some(Utf8Component::ParentDir)
                | Some(Utf8Component::Prefix(_)) => out.push(comp),
            },
            comp => out.push(comp),
        }
    }
    if !out.is_empty() {
        out.iter().collect()
    } else {
        Utf8PathBuf::from(".")
    }
}

pub struct Proxy {
    pub auth: JwtAuth,
    pub storage: GenericRemoteStorage,
    pub cancel: CancellationToken,
}

// libs/utils/src/id.rs has TenantId and TimelineId but we don't need
// them as both types are used as strings.
// Validity is checked via JWT verification so we're fine to ignore parsing rules
// If needed further, we can use smallstr::SmallString<[u8; 16]>;
type TimelineId = String;
type TenantId = String;
type EndpointId = String;

#[derive(Deserialize, Serialize, PartialEq)]
pub struct Claims {
    pub tenant_id: TenantId,
    pub timeline_id: TimelineId,
    pub endpoint_id: EndpointId,
}

impl Display for Claims {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Claims(tenant_id {} timeline_id {} endpoint_id {})",
            self.tenant_id, self.timeline_id, self.endpoint_id,
        )
    }
}

#[derive(Deserialize, Serialize)]
struct KeyPath {
    #[serde(flatten)]
    auth: Claims,
    path: String,
}

#[derive(Debug, PartialEq)]
pub struct S3Path {
    pub path: RemotePath,
}

impl TryFrom<&KeyPath> for S3Path {
    type Error = String;
    fn try_from(KeyPath { auth, path }: &KeyPath) -> StdResult<Self, Self::Error> {
        let Claims {
            tenant_id,
            timeline_id,
            endpoint_id,
        } = &auth;
        let prefix = format!("{tenant_id}/{timeline_id}/{endpoint_id}",);
        let path = Utf8PathBuf::from(prefix).join(normalize_key(&path)?);
        let path = RemotePath::new(&path).unwrap(); // unwrap() because the path is already relative
        Ok(S3Path { path })
    }
}

fn unauthorized() -> Response {
    StatusCode::UNAUTHORIZED.into_response()
}
fn bad_request(e: impl ToString) -> Response {
    (StatusCode::BAD_REQUEST, e.to_string()).into_response()
}

impl FromRequestParts<Arc<Proxy>> for S3Path {
    type Rejection = Response;
    async fn from_request_parts(
        parts: &mut Parts,
        state: &Arc<Proxy>,
    ) -> Result<Self, Self::Rejection> {
        let TypedHeader(Authorization(bearer)) = parts
            .extract::<TypedHeader<Authorization<Bearer>>>()
            .await
            .map_err(|_| bad_request("invalid token"))?;
        let claims: Claims = state
            .auth
            .decode(bearer.token())
            .map_err(|_| bad_request("invalid token"))?;
        let Path(path): Path<KeyPath> = parts
            .extract()
            .await
            .map_err(|_| bad_request("invalid route"))?;
        if path.auth != claims {
            tracing::debug!(%path.auth, %claims, "route doesn't match claims");
            return Err(unauthorized());
        }
        (&path).try_into().map_err(|_| bad_request("invalid route"))
    }
}

#[derive(Deserialize, Serialize, PartialEq)]
pub struct PrefixKeyPath {
    pub tenant_id: TenantId,
    pub timeline_id: Option<TimelineId>,
    pub endpoint_id: Option<EndpointId>,
}

impl Display for PrefixKeyPath {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "PrefixKeyPath(tenant_id {} timeline_id {} endpoint_id {})",
            self.tenant_id,
            self.timeline_id.as_ref().unwrap_or(&"".to_string()),
            self.endpoint_id.as_ref().unwrap_or(&"".to_string())
        )
    }
}

#[derive(Debug, PartialEq)]
pub struct PrefixS3Path {
    pub path: RemotePath,
}

impl From<&PrefixKeyPath> for PrefixS3Path {
    fn from(path: &PrefixKeyPath) -> Self {
        let path = Utf8PathBuf::from(path.tenant_id.to_string())
            .join(path.timeline_id.as_ref().unwrap_or(&"".to_string()))
            .join(path.endpoint_id.as_ref().unwrap_or(&"".to_string()));
        let path = RemotePath::new(&path).unwrap(); // unwrap() because the path is already relative
        PrefixS3Path { path }
    }
}

impl FromRequestParts<Arc<Proxy>> for PrefixS3Path {
    type Rejection = Response;
    async fn from_request_parts(
        parts: &mut Parts,
        state: &Arc<Proxy>,
    ) -> Result<Self, Self::Rejection> {
        let TypedHeader(Authorization(bearer)) = parts
            .extract::<TypedHeader<Authorization<Bearer>>>()
            .await
            .map_err(|_| bad_request("invalid token"))?;
        let claims: PrefixKeyPath = state
            .auth
            .decode(bearer.token())
            .map_err(|_| bad_request("invalid token"))?;
        let Path(path) = parts
            .extract::<Path<PrefixKeyPath>>()
            .await
            .map_err(|_| bad_request("invalid route"))?;
        if path != claims {
            tracing::debug!(%path, %claims, "route doesn't match claims");
            return Err(unauthorized());
        }
        Ok((&path).into())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use camino::Utf8PathBuf;

    #[test]
    fn normalize_key() {
        let f = super::normalize_key;
        assert_eq!(f("hello/world/..").unwrap(), Utf8PathBuf::from("hello"));
        assert_eq!(
            f("ololo/1/../../not_ololo").unwrap(),
            Utf8PathBuf::from("not_ololo")
        );
        assert!(f("ololo/1/../../../").is_err());
        assert!(f(".").is_err());
        assert!(f("../").is_err());
        assert!(f("").is_err());
        assert_eq!(f("/1/2/3").unwrap(), Utf8PathBuf::from("1/2/3"));
        assert!(f("/1/2/3/../../../").is_err());
        assert!(f("/1/2/3/../../../../").is_err());
    }

    const TENANT_ID: &str = "1adcba3c01c578d1c1be7b8048a4484d";
    const TIMELINE_ID: &str = "16fde223c5e55c4b791e63284681d951";
    const ENDPOINT_ID: &str = "ep-winter-frost-a662z3vg";

    #[test]
    fn s3_path() {
        let auth = Claims {
            tenant_id: TENANT_ID.into(),
            timeline_id: TIMELINE_ID.into(),
            endpoint_id: ENDPOINT_ID.into(),
        };
        let s3_path = |key| {
            let path = &format!("{TENANT_ID}/{TIMELINE_ID}/{ENDPOINT_ID}/{key}");
            let path = RemotePath::from_string(path).unwrap();
            S3Path { path }
        };

        let path = "cache_key".to_string();
        let mut key_path = KeyPath { auth, path };
        assert_eq!(S3Path::try_from(&key_path).unwrap(), s3_path(key_path.path));

        key_path.path = "we/can/have/nested/paths".to_string();
        assert_eq!(S3Path::try_from(&key_path).unwrap(), s3_path(key_path.path));

        key_path.path = "../error/hello/../".to_string();
        assert!(S3Path::try_from(&key_path).is_err());
    }

    #[test]
    fn prefix_s3_path() {
        let mut path = PrefixKeyPath {
            tenant_id: TENANT_ID.into(),
            timeline_id: None,
            endpoint_id: None,
        };
        let prefix_path = |s: String| RemotePath::from_string(&s).unwrap();
        assert_eq!(
            PrefixS3Path::from(&path).path,
            prefix_path(format!("{TENANT_ID}"))
        );

        path.timeline_id = Some(TIMELINE_ID.into());
        assert_eq!(
            PrefixS3Path::from(&path).path,
            prefix_path(format!("{TENANT_ID}/{TIMELINE_ID}"))
        );

        path.endpoint_id = Some(ENDPOINT_ID.into());
        assert_eq!(
            PrefixS3Path::from(&path).path,
            prefix_path(format!("{TENANT_ID}/{TIMELINE_ID}/{ENDPOINT_ID}"))
        );
    }
}
