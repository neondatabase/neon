//! Paster is a service to share logs or code snippets outside of
//! Slack, not relying on public services
use anyhow::Result;
use shortener::google_oauth_gate::{AuthRequest, State, UserId};
use axum::Form;
use axum::extract::{FromRef, FromRequestParts, Path, Query, State as AxumStateT};
use axum::http::StatusCode;
use axum::response::{Html, IntoResponse};
use axum::response::{Redirect, Response};
use axum::routing::get;
use axum_extra::extract::PrivateCookieJar;
use axum_extra::extract::cookie::{Cookie, Key};
use chrono::{Duration, Local, TimeZone, Utc};
use core::num::NonZeroI32;
use serde::Deserialize;
use std::env;
use std::sync::Arc;
use tracing::{error, info};
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;

const SOCKET: &str = "127.0.0.1:12344";
const HOST: &str = "http://127.0.0.1:12344";
const ALLOWED_OAUTH_DOMAIN: &str = "neon.tech";

fn oauth_redirect_url() -> String {
    format!("{HOST}{AUTHORIZED_ROUTE}")
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::registry()
        .with(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| format!("{}=info", env!("CARGO_CRATE_NAME")).into()),
        )
        .with(tracing_subscriber::fmt::layer())
        .init();

    let oauth_client_id = env::var("OAUTH_CLIENT_ID").expect("Missing OAUTH_CLIENT_ID");
    let oauth_client_secret = env::var("OAUTH_CLIENT_SECRET").expect("Missing OAUTH_CLIENT_SECRET");

    let db_connstr = env::var("DB_CONNSTR").expect("Missing DB_CONNSTR");
    let mut roots = rustls::RootCertStore::empty();
    for cert in rustls_native_certs::load_native_certs().expect("could not load platform certs") {
        roots.add(cert).unwrap();
    }
    let config = rustls::ClientConfig::builder()
        .with_root_certificates(roots)
        .with_no_client_auth();
    let tls = tokio_postgres_rustls::MakeRustlsConnect::new(config);
    info!("initialized TLS");

    let (db_client, db_conn) = tokio_postgres::connect(&db_connstr, tls).await?;
    tokio::spawn(async move {
        if let Err(err) = db_conn.await {
            error!(%err, "connecting to database");
            std::process::exit(1);
        }
    });
    info!("connected to database");

    let state = InnerState {
        db_client,
        cookie_jar_key: Key::generate(),
        oauth_client_id,
        oauth_client_secret,
    };
    let router = axum::Router::new()
        .route("/", get(index).post(paste))
        .route("/authorize", get(authorize))
        .route(AUTHORIZED_ROUTE, get(authorized))
        .route("/{id}", get(view_paste))
        .with_state(State { 0: Arc::new(state) });
    let listener = tokio::net::TcpListener::bind(SOCKET)
        .await
        .expect("failed to bind TcpListener");
    info!("listening on {SOCKET}");
    axum::serve(listener, router).await.unwrap();
    Ok(())
}

#[derive(Deserialize)]
pub struct UserId {
    id: NonZeroI32,
}

impl axum::extract::OptionalFromRequestParts<State> for UserId {
    type Rejection = Response;
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
            Err(err) => {
                error!(%err, "querying user session");
                return Ok(None);
            }
        };
        let id = NonZeroI32::new(id).unwrap(); // postgres id guaranteed not to be zero
        Ok(Some(Self { id }))
    }
}

#[derive(Deserialize)]
struct Paste {
    paste: String,
}

fn paste_form() -> Html<String> {
    Html(
        r#"
            <form method="post">
                <textarea name="paste" style="width:100%;height:80%"></textarea>
                <input type="submit" value="Paste" style="margin-top:10px">
            </form>"#
            .to_string(),
    )
}

fn authorize_link(paste_id: i32) -> String {
    format!("<a href=\"/authorize?paste_id={paste_id}\">Authorize</a>")
}

async fn index(user: Option<UserId>) -> Html<String> {
    if user.is_some() {
        return paste_form();
    }
    Html(authorize_link(0))
}

async fn paste(
    state: AxumState,
    user: Option<UserId>,
    Form(Paste { paste }): Form<Paste>,
) -> Response {
    let user_id = match user {
        None => return StatusCode::FORBIDDEN.into_response(),
        Some(user) => user.id,
    };
    if paste.is_empty() {
        return paste_form().into_response();
    }

    let query = state
        .db_client
        .query_one(
            "INSERT INTO pastes (user_id, paste) VALUES ($1, $2) RETURNING id",
            &[&user_id.get(), &paste],
        )
        .await;
    let id = match query {
        Ok(row) => row.get::<usize, i32>(0),
        Err(err) => {
            error!(%err, "inserting paste");
            return StatusCode::INTERNAL_SERVER_ERROR.into_response();
        }
    };
    Redirect::to(&format!("/{id}")).into_response()
}

async fn view_paste(state: AxumState, user: Option<UserId>, Path(paste_id): Path<i32>) -> Response {
    let user_id = match user {
        None => return Html(authorize_link(paste_id)).into_response(),
        Some(user) => user.id,
    };

    let query = state
        .db_client
        .query_opt("SELECT paste FROM pastes WHERE id = $1", &[&paste_id])
        .await;
    let row = match query {
        Ok(None) => return StatusCode::NOT_FOUND.into_response(),
        Ok(Some(row)) => row,
        Err(err) => {
            error!(%err, %paste_id, %user_id, "querying paste");
            return StatusCode::INTERNAL_SERVER_ERROR.into_response();
        }
    };
    row.get::<usize, String>(0).into_response()
}

#[derive(Deserialize)]
struct AuthRequest {
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

fn decode_id_token(token: String) -> Option<UserInfo> {
    let payload = token.split(".").skip(1).take(1).collect::<Vec<&str>>();
    let decoded = base64::decode_config(payload.get(0)?, base64::STANDARD_NO_PAD).ok()?;
    serde_json::from_slice::<UserInfo>(&decoded).ok()
}

#[derive(Deserialize)]
struct AuthorizeQuery {
    paste_id: i32,
}

fn generate_csrf_token(num_bytes: u32) -> String {
    use rand::{Rng, thread_rng};
    let random_bytes: Vec<u8> = (0..num_bytes).map(|_| thread_rng().r#gen::<u8>()).collect();
    base64::encode_config(&random_bytes, base64::URL_SAFE_NO_PAD)
}

async fn authorize(
    state: AxumState,
    jar: PrivateCookieJar,
    Query(AuthorizeQuery { paste_id }): Query<AuthorizeQuery>,
) -> (PrivateCookieJar, Redirect) {
    let csrf_token = generate_csrf_token(16);
    let client_id = &state.oauth_client_id;
    let redirect_uri = oauth_redirect_url();
    let auth_url = format!(
        "{OAUTH_BASE_URL}?response_type=code\
        &client_id={client_id}\
        &state={csrf_token}\
        &redirect_uri={redirect_uri}\
        &scope=https%3A%2F%2Fwww.googleapis.com%2Fauth%2Fuserinfo.email"
    );

    let redirect_cookie = Cookie::build((COOKIE_REDIRECT, paste_id.to_string()))
        .path("/")
        //.TODO secure(true) not true for localhost
        //.domain(COOKIE_DOMAIN)
        .secure(false)
        .same_site(axum_extra::extract::cookie::SameSite::Lax)
        .http_only(true)
        .build();
    let csrf_cookie = Cookie::build((COOKIE_CSRF, csrf_token))
        .path("/")
        //.TODO secure(true) not true for localhost
        //.domain(COOKIE_DOMAIN)
        .secure(false)
        .same_site(axum_extra::extract::cookie::SameSite::Lax)
        .http_only(true)
        .build();
    let jar = jar.add(redirect_cookie).add(csrf_cookie);
    let url = Into::<String>::into(auth_url);
    (jar, Redirect::to(&url))
}

async fn authorized(
    state: AxumState,
    jar: PrivateCookieJar,
    Query(auth_request): Query<AuthRequest>,
) -> Result<(PrivateCookieJar, Redirect), Response> {
    let params = [
        ("grant_type", "authorization_code"),
        ("redirect_uri", &oauth_redirect_url()),
        ("code", &auth_request.code),
        ("client_id", &state.oauth_client_id),
        ("client_secret", &state.oauth_client_secret),
    ];
    let auth_response = reqwest::Client::new()
        .post(OAUTH_TOKEN_URL)
        .form(&params)
        .send()
        .await
        .map_err(|err| {
            error!(%err, "exchanging oauth code for token");
            StatusCode::INTERNAL_SERVER_ERROR.into_response()
        })?
        .json::<AuthResponse>()
        .await
        .map_err(|err| {
            error!(%err, "deserializing access token response");
            StatusCode::INTERNAL_SERVER_ERROR.into_response()
        })?;
    let Some(UserInfo { hd, sub }) = decode_id_token(auth_response.id_token) else {
        error!("Failed to decode response id token");
        return Err(StatusCode::UNAUTHORIZED.into_response());
    };
    if hd != ALLOWED_OAUTH_DOMAIN {
        error!(hd, "Domain doesn't match {ALLOWED_OAUTH_DOMAIN}");
        return Err(StatusCode::UNAUTHORIZED.into_response());
    }

    let token_duration = Duration::try_seconds(auth_response.expires_in as i64).unwrap();
    let expires_at = Utc.from_utc_datetime(&(Local::now().naive_local() + token_duration));
    let cookie_max_age = time::Duration::new(token_duration.num_seconds(), 0);

    let session_cookie = Cookie::build((COOKIE_SID, auth_response.access_token.clone()))
        .path("/")
        //.TODO secure(true) not true for localhost
        //.domain(COOKIE_DOMAIN)
        .secure(false)
        .same_site(axum_extra::extract::cookie::SameSite::Lax)
        .http_only(true)
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
        .map_err(|err| {
            error!(%err, %sub, "updating session");
            return StatusCode::INTERNAL_SERVER_ERROR.into_response();
        })?;

    let csrf_cookie = jar.get(COOKIE_CSRF).unwrap(); // set in authorize()
    let jar = jar.remove(csrf_cookie).add(session_cookie);
    match jar.get(COOKIE_REDIRECT) {
        Some(redirect_cookie) => {
            let mut value = redirect_cookie.value_trimmed();
            if value == "0" {
                value = "";
            }
            let redirect_url = format!("/{value}");
            Ok((jar.remove(redirect_cookie), Redirect::to(&redirect_url)))
        }
        None => Ok((jar, Redirect::to("/"))),
    }
}
