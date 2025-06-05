//! Library to gate infrastructure behind Google Oauth for domain.
//!
//! Why not oauth-rs? Oauth .exchange_code() doesn't work with "request failed". Also, we can't get
//! id token from it, and I don't want to pull in whole openid library just for that.
//! Id token saves us a request to openid endpoint and one Oauth scope we don't use
use anyhow::{Context, Result, bail};
use axum::extract::{FromRef, FromRequestParts, Query, State as AxumState};
use axum::response::Redirect;
use axum_extra::extract::PrivateCookieJar;
use axum_extra::extract::cookie::{Cookie, Key};
use chrono::{Duration, Local, TimeZone, Utc};
use cookie::CookieBuilder;
use core::num::NonZeroI32;
use reqwest::StatusCode;
use serde::Deserialize;
use std::sync::Arc;
use tokio_postgres::Socket;

const OAUTH_BASE_URL: &str = "https://accounts.google.com/o/oauth2/v2/auth";
const OAUTH_TOKEN_URL: &str = "https://oauth2.googleapis.com/token";
const COOKIE_SID: &str = "sid";
const COOKIE_CSRF: &str = "csrf";

pub struct Config {
    pub oauth_client_id: String,
    pub oauth_client_secret: String,
    pub oauth_redirect_url: String,
    pub oauth_allowed_domain: String,
    pub cookie_settings: fn(CookieBuilder) -> CookieBuilder,
}

pub struct InnerState {
    config: Config,
    cookie_jar_key: Key,
    pub db_client: tokio_postgres::Client,
}

#[derive(Clone)]
pub struct State(Arc<InnerState>);
type DbConn = tokio_postgres::Connection<Socket, tokio_postgres_rustls::RustlsStream<Socket>>;

impl State {
    pub async fn new(config: Config, db_connstr: &str) -> Result<(Self, DbConn)> {
        let mut roots = rustls::RootCertStore::empty();
        for cert in rustls_native_certs::load_native_certs().expect("could not load platform certs")
        {
            roots.add(cert).unwrap();
        }
        let tls_config = rustls::ClientConfig::builder()
            .with_root_certificates(roots)
            .with_no_client_auth();
        let tls = tokio_postgres_rustls::MakeRustlsConnect::new(tls_config);

        let (db_client, db_conn) = tokio_postgres::connect(&db_connstr, tls).await?;
        let inner = InnerState {
            config,
            cookie_jar_key: Key::generate(),
            db_client,
        };
        Ok((Self { 0: Arc::new(inner) }, db_conn))
    }
}

impl std::ops::Deref for State {
    type Target = InnerState;
    fn deref(&self) -> &Self::Target {
        &*self.0
    }
}

impl FromRef<State> for Key {
    fn from_ref(state: &State) -> Self {
        state.cookie_jar_key.clone()
    }
}

#[derive(Deserialize)]
pub struct UserId {
    pub id: NonZeroI32,
}

#[derive(Deserialize)]
pub struct AuthRequest {
    code: String,
}

#[derive(Deserialize)]
struct AuthResponse {
    access_token: String,
    id_token: String,
    expires_in: u64,
}

#[derive(Deserialize)]
struct UserInfo {
    hd: String,
    sub: String,
}

impl axum::extract::OptionalFromRequestParts<State> for UserId {
    type Rejection = StatusCode;
    async fn from_request_parts(
        parts: &mut axum::http::request::Parts,
        state: &State,
    ) -> Result<Option<Self>, Self::Rejection> {
        let jar: PrivateCookieJar = PrivateCookieJar::from_request_parts(parts, state)
            .await
            .unwrap(); // infallible
        let Some(session_id) = jar.get(COOKIE_SID).map(|cookie| cookie.value().to_owned()) else {
            return Ok(None);
        };

        let client = &state.db_client;
        let query = client
            .query_opt(
                "SELECT user_id FROM sessions WHERE session_id = $1",
                &[&session_id],
            )
            .await;
        let id = match query {
            Ok(Some(row)) => row.get::<usize, i32>(0),
            Ok(None) => return Ok(None),
            Err(_) => return Err(StatusCode::INTERNAL_SERVER_ERROR),
        };
        let id = NonZeroI32::new(id).unwrap(); // postgres id guaranteed not to be zero
        Ok(Some(Self { id }))
    }
}

fn decode_id_token(token: String) -> Option<UserInfo> {
    let payload = token.split(".").skip(1).take(1).collect::<Vec<&str>>();
    let decoded = base64::decode_config(payload.get(0)?, base64::STANDARD_NO_PAD).ok()?;
    serde_json::from_slice::<UserInfo>(&decoded).ok()
}

fn generate_csrf_token(num_bytes: u32) -> String {
    use rand::{Rng, thread_rng};
    let random_bytes: Vec<u8> = (0..num_bytes).map(|_| thread_rng().r#gen::<u8>()).collect();
    base64::encode_config(&random_bytes, base64::URL_SAFE_NO_PAD)
}

pub async fn authorize(
    state: AxumState<State>,
    jar: PrivateCookieJar,
) -> (PrivateCookieJar, Redirect) {
    let csrf_token = generate_csrf_token(16);
    let client_id = &state.config.oauth_client_id;
    let redirect_uri = &state.config.oauth_redirect_url;
    let auth_url = format!(
        "{OAUTH_BASE_URL}?response_type=code\
        &client_id={client_id}\
        &state={csrf_token}\
        &redirect_uri={redirect_uri}\
        &scope=https%3A%2F%2Fwww.googleapis.com%2Fauth%2Fuserinfo.email"
    );

    let csrf_cookie =
        (state.config.cookie_settings)(Cookie::build((COOKIE_CSRF, csrf_token))).build();
    let url = Into::<String>::into(auth_url);
    (jar.add(csrf_cookie), Redirect::to(&url))
}

pub async fn authorized(
    state: AxumState<State>,
    jar: PrivateCookieJar,
    Query(auth_request): Query<AuthRequest>,
) -> Result<PrivateCookieJar> {
    let params = [
        ("grant_type", "authorization_code"),
        ("redirect_uri", &state.config.oauth_redirect_url),
        ("code", &auth_request.code),
        ("client_id", &state.config.oauth_client_id),
        ("client_secret", &state.config.oauth_client_secret),
    ];
    let auth_response = reqwest::Client::new()
        .post(OAUTH_TOKEN_URL)
        .form(&params)
        .send()
        .await
        .context("exchanging oauth code for token")?
        .json::<AuthResponse>()
        .await
        .context("deserializing access_token response")?;
    let Some(UserInfo { hd, sub }) = decode_id_token(auth_response.id_token) else {
        bail!("failed to decode id token")
    };

    let allowed_domain = &state.config.oauth_allowed_domain;
    if hd != *allowed_domain {
        bail!("{hd} doesn't match {allowed_domain}")
    }

    let token_duration = Duration::try_seconds(auth_response.expires_in as i64).unwrap();
    let expires_at = Utc.from_utc_datetime(&(Local::now().naive_local() + token_duration));
    let cookie_max_age = time::Duration::new(token_duration.num_seconds(), 0);

    let session_cookie = (state.config.cookie_settings)(Cookie::build((
        COOKIE_SID,
        auth_response.access_token.clone(),
    )))
    .max_age(cookie_max_age)
    .build();

    state
        .db_client
        .query(
            "WITH user_insert AS (\
                INSERT INTO users (sub) VALUES ($1) \
                ON CONFLICT (sub) DO UPDATE SET sub = excluded.sub RETURNING id)\
        INSERT INTO sessions (user_id, session_id, expires_at) \
        SELECT id, $2, $3 FROM user_insert \
        ON CONFLICT (user_id) DO UPDATE SET \
            session_id = excluded.session_id, \
             expires_at = excluded.expires_at",
            &[&sub, &auth_response.access_token, &expires_at],
        )
        .await
        .with_context(|| format!("updating session for {sub}"))?;

    let csrf_cookie = jar.get(COOKIE_CSRF).unwrap(); // set in authorize()
    Ok(jar.remove(csrf_cookie).add(session_cookie))
}
