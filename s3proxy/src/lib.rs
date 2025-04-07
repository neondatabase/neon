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
use tracing::debug;
use utils::id::{TenantId, TimelineId};

// simplified version of utils::auth::JwtAuth
pub struct JwtAuth {
    decoding_key: DecodingKey,
    validation: Validation,
}

pub const VALIDATION_ALGO: jsonwebtoken::Algorithm = jsonwebtoken::Algorithm::EdDSA;
impl JwtAuth {
    pub fn new(key: &[u8]) -> Result<Self> {
        Ok(Self {
            decoding_key: DecodingKey::from_ed_pem(key)?,
            validation: Validation::new(VALIDATION_ALGO),
        })
    }

    pub fn decode<T: serde::de::DeserializeOwned>(&self, token: &str) -> Result<T> {
        Ok(jsonwebtoken::decode(token, &self.decoding_key, &self.validation).map(|t| t.claims)?)
    }
}

fn normalize_key(key: &str) -> StdResult<Utf8PathBuf, String> {
    let key = clean_utf8(&Utf8PathBuf::from(key));
    if key.starts_with("..") || key == "." || key == "/" {
        return Err(format!("invalid key {key}"));
    }
    match key.strip_prefix("/").map(Utf8PathBuf::from) {
        Ok(p) => Ok(p),
        _ => Ok(key),
    }
}

// Copied from path_clean crate with PathBuf->Utf8PathBuf
fn clean_utf8(path: &Utf8Path) -> Utf8PathBuf {
    use camino::Utf8Component as Comp;
    let mut out = Vec::new();
    for comp in path.components() {
        match comp {
            Comp::CurDir => (),
            Comp::ParentDir => match out.last() {
                Some(Comp::RootDir) => (),
                Some(Comp::Normal(_)) => {
                    out.pop();
                }
                None | Some(Comp::CurDir) | Some(Comp::ParentDir) | Some(Comp::Prefix(_)) => {
                    out.push(comp)
                }
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

pub type EndpointId = String; // If needed, reuse small string from proxy/src/types.rc

#[derive(Deserialize, Serialize, PartialEq)]
pub struct Claims {
    pub tenant_id: TenantId,
    pub timeline_id: TimelineId,
    pub endpoint_id: EndpointId,
    pub exp: u64,
}

impl Display for Claims {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Claims(tenant_id {} timeline_id {} endpoint_id {} exp {})",
            self.tenant_id, self.timeline_id, self.endpoint_id, self.exp
        )
    }
}

#[derive(Deserialize, Serialize)]
struct KeyRequest {
    tenant_id: TenantId,
    timeline_id: TimelineId,
    endpoint_id: EndpointId,
    path: String,
}

#[derive(Debug, PartialEq)]
pub struct S3Path {
    pub path: RemotePath,
}

impl TryFrom<&KeyRequest> for S3Path {
    type Error = String;
    fn try_from(req: &KeyRequest) -> StdResult<Self, Self::Error> {
        let KeyRequest {
            tenant_id,
            timeline_id,
            endpoint_id,
            path,
        } = &req;
        let prefix = format!("{tenant_id}/{timeline_id}/{endpoint_id}",);
        let path = Utf8PathBuf::from(prefix).join(normalize_key(path)?);
        let path = RemotePath::new(&path).unwrap(); // unwrap() because the path is already relative
        Ok(S3Path { path })
    }
}

fn unauthorized() -> Response {
    StatusCode::UNAUTHORIZED.into_response()
}

pub fn bad_request(e: impl ToString) -> Response {
    (StatusCode::BAD_REQUEST, e.to_string()).into_response()
}

pub fn ok() -> Response {
    StatusCode::OK.into_response()
}

pub fn internal_error() -> Response {
    StatusCode::INTERNAL_SERVER_ERROR.into_response()
}

pub fn not_found(key: impl ToString) -> Response {
    (StatusCode::NOT_FOUND, key.to_string()).into_response()
}

impl FromRequestParts<Arc<Proxy>> for S3Path {
    type Rejection = Response;
    async fn from_request_parts(
        parts: &mut Parts,
        state: &Arc<Proxy>,
    ) -> Result<Self, Self::Rejection> {
        let Path(path): Path<KeyRequest> = parts
            .extract()
            .await
            .map_err(|_| bad_request("invalid route"))?;
        let TypedHeader(Authorization(bearer)) = parts
            .extract::<TypedHeader<Authorization<Bearer>>>()
            .await
            .map_err(|_| bad_request("invalid token"))?;
        let claims: Claims = state.auth.decode(bearer.token()).map_err(|err| {
            debug!(%err, "decoding token");
            bad_request("invalid token")
        })?;
        let route = Claims {
            tenant_id: path.tenant_id,
            timeline_id: path.timeline_id,
            endpoint_id: path.endpoint_id.clone(),
            exp: claims.exp,
        };
        if route != claims {
            debug!(%route, %claims, "route doesn't match claims");
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
            self.timeline_id
                .as_ref()
                .map(ToString::to_string)
                .unwrap_or("".to_string()),
            self.endpoint_id
                .as_ref()
                .map(ToString::to_string)
                .unwrap_or("".to_string())
        )
    }
}

#[derive(Debug, PartialEq)]
pub struct PrefixS3Path {
    pub path: RemotePath,
}

impl From<&PrefixKeyPath> for PrefixS3Path {
    fn from(path: &PrefixKeyPath) -> Self {
        let timeline_id = path
            .timeline_id
            .as_ref()
            .map(ToString::to_string)
            .unwrap_or("".to_string());
        let endpoint_id = path
            .endpoint_id
            .as_ref()
            .map(ToString::to_string)
            .unwrap_or("".to_string());
        let path = Utf8PathBuf::from(path.tenant_id.to_string())
            .join(timeline_id)
            .join(endpoint_id);
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
        let Path(path) = parts
            .extract::<Path<PrefixKeyPath>>()
            .await
            .map_err(|_| bad_request("invalid route"))?;
        let TypedHeader(Authorization(bearer)) = parts
            .extract::<TypedHeader<Authorization<Bearer>>>()
            .await
            .map_err(|_| bad_request("invalid token"))?;
        let claims: PrefixKeyPath = state.auth.decode(bearer.token()).map_err(|err| {
            debug!(%path, %err, "decoding token");
            bad_request("invalid token")
        })?;
        if path != claims {
            debug!(%path, %claims, "route doesn't match claims");
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

    const TENANT_ID: TenantId =
        TenantId::from_array([1, 1, 2, 3, 4, 5, 6, 7, 8, 9, 1, 2, 3, 4, 5, 6]);
    const TIMELINE_ID: TimelineId =
        TimelineId::from_array([1, 1, 2, 3, 4, 5, 6, 7, 8, 9, 1, 2, 3, 4, 5, 7]);
    const ENDPOINT_ID: &str = "ep-winter-frost-a662z3vg";

    #[test]
    fn s3_path() {
        let auth = Claims {
            tenant_id: TENANT_ID,
            timeline_id: TIMELINE_ID,
            endpoint_id: ENDPOINT_ID.into(),
            exp: u64::max_value(),
        };
        let s3_path = |key| {
            let path = &format!("{TENANT_ID}/{TIMELINE_ID}/{ENDPOINT_ID}/{key}");
            let path = RemotePath::from_string(path).unwrap();
            S3Path { path }
        };

        let path = "cache_key".to_string();
        let mut key_path = KeyRequest {
            path,
            tenant_id: auth.tenant_id.clone(),
            timeline_id: auth.timeline_id.clone(),
            endpoint_id: auth.endpoint_id.clone(),
        };
        assert_eq!(S3Path::try_from(&key_path).unwrap(), s3_path(key_path.path));

        key_path.path = "we/can/have/nested/paths".to_string();
        assert_eq!(S3Path::try_from(&key_path).unwrap(), s3_path(key_path.path));

        key_path.path = "../error/hello/../".to_string();
        assert!(S3Path::try_from(&key_path).is_err());
    }

    #[test]
    fn prefix_s3_path() {
        let mut path = PrefixKeyPath {
            tenant_id: TENANT_ID,
            timeline_id: None,
            endpoint_id: None,
        };
        let prefix_path = |s: String| RemotePath::from_string(&s).unwrap();
        assert_eq!(
            PrefixS3Path::from(&path).path,
            prefix_path(format!("{TENANT_ID}"))
        );

        path.timeline_id = Some(TIMELINE_ID);
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
