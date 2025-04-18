use std::collections::HashSet;

use anyhow::{Result, anyhow};
use axum::{RequestExt, body::Body};
use axum_extra::{
    TypedHeader,
    headers::{Authorization, authorization::Bearer},
};
use compute_api::requests::ComputeClaims;
use futures::future::BoxFuture;
use http::{Request, Response, StatusCode};
use jsonwebtoken::{Algorithm, DecodingKey, TokenData, Validation, jwk::JwkSet};
use tower_http::auth::AsyncAuthorizeRequest;
use tracing::{debug, warn};

use crate::http::JsonResponse;

#[derive(Clone, Debug)]
pub(in crate::http) struct Authorize {
    compute_id: String,
    jwks: JwkSet,
    validation: Validation,
}

impl Authorize {
    pub fn new(compute_id: String, jwks: JwkSet) -> Self {
        let mut validation = Validation::new(Algorithm::EdDSA);
        // Nothing is currently required
        validation.required_spec_claims = HashSet::new();
        validation.validate_exp = true;
        // Unused by the control plane
        validation.validate_aud = false;
        // Unused by the control plane
        validation.validate_nbf = false;

        Self {
            compute_id,
            jwks,
            validation,
        }
    }
}

impl AsyncAuthorizeRequest<Body> for Authorize {
    type RequestBody = Body;
    type ResponseBody = Body;
    type Future = BoxFuture<'static, Result<Request<Body>, Response<Self::ResponseBody>>>;

    fn authorize(&mut self, mut request: Request<Body>) -> Self::Future {
        let compute_id = self.compute_id.clone();
        let jwks = self.jwks.clone();
        let validation = self.validation.clone();

        Box::pin(async move {
            let TypedHeader(Authorization(bearer)) = request
                .extract_parts::<TypedHeader<Authorization<Bearer>>>()
                .await
                .map_err(|_| {
                    JsonResponse::error(StatusCode::BAD_REQUEST, "invalid authorization token")
                })?;

            let data = match Self::verify(&jwks, bearer.token(), &validation) {
                Ok(claims) => claims,
                Err(e) => return Err(JsonResponse::error(StatusCode::UNAUTHORIZED, e)),
            };

            if data.claims.compute_id != compute_id {
                return Err(JsonResponse::error(
                    StatusCode::UNAUTHORIZED,
                    "invalid compute ID in authorization token claims",
                ));
            }

            // Make claims available to any subsequent middleware or request
            // handlers
            request.extensions_mut().insert(data.claims);

            Ok(request)
        })
    }
}

impl Authorize {
    /// Verify the token using the JSON Web Key set and return the token data.
    fn verify(
        jwks: &JwkSet,
        token: &str,
        validation: &Validation,
    ) -> Result<TokenData<ComputeClaims>> {
        debug_assert!(!jwks.keys.is_empty());

        debug!("verifying token {}", token);

        for jwk in jwks.keys.iter() {
            let decoding_key = match DecodingKey::from_jwk(jwk) {
                Ok(key) => key,
                Err(e) => {
                    warn!(
                        "failed to construct decoding key from {}: {}",
                        jwk.common.key_id.as_ref().unwrap(),
                        e
                    );

                    continue;
                }
            };

            match jsonwebtoken::decode::<ComputeClaims>(token, &decoding_key, validation) {
                Ok(data) => return Ok(data),
                Err(e) => {
                    warn!(
                        "failed to decode authorization token using {}: {}",
                        jwk.common.key_id.as_ref().unwrap(),
                        e
                    );

                    continue;
                }
            }
        }

        Err(anyhow!("failed to verify authorization token"))
    }
}
