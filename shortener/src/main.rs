//! Shortener is a service to gate access to internal infrastructure
//! URLs behind team authorisation to expose less private information.
pub mod google_oauth_gate;
use crate::google_oauth_gate::{AuthRequest, State, UserId};
use anyhow::Result;
use axum::Form;
use axum::extract::State as AxumState;
use axum::extract::{Path, Query};
use axum::http::StatusCode;
use axum::response::{Html, IntoResponse};
use axum::response::{Redirect, Response};
use axum::routing::get;
use axum_extra::extract::PrivateCookieJar;
use axum_extra::extract::cookie::Cookie;
use cookie::CookieBuilder;
use google_oauth_gate::Config;
use serde::Deserialize;
use std::env;
use tracing::{error, info};
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;

const SOCKET: &str = "127.0.0.1:12344";
const HOST: &str = "http://127.0.0.1:12344";
const COOKIE_REDIRECT: &str = "redirect";
const ALLOWED_OAUTH_DOMAIN: &str = "neon.tech";
const AUTHORIZED_ROUTE: &str = "/authorized";
const SHORT_URL_LEN: usize = 6;

fn cookie_settings(b: CookieBuilder) -> CookieBuilder {
    if HOST.contains("127.0.0.1") {
        b.path("/")
            .secure(false)
            .same_site(axum_extra::extract::cookie::SameSite::Lax)
            .http_only(true)
    } else {
        b.path("/")
            .domain(ALLOWED_OAUTH_DOMAIN)
            .secure(true)
            .http_only(false)
    }
}

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

    let config = Config {
        oauth_client_id,
        oauth_client_secret,
        oauth_redirect_url: oauth_redirect_url(),
        oauth_allowed_domain: ALLOWED_OAUTH_DOMAIN.to_string(),
        cookie_settings,
    };
    let (state, db_conn) = State::new(config, &db_connstr).await?;
    tokio::spawn(async move {
        if let Err(err) = db_conn.await {
            error!(%err, "connecting to database");
            std::process::exit(1);
        }
    });

    let router = axum::Router::new()
        .route("/", get(index).post(shorten))
        .route("/authorize", get(authorize))
        .route(AUTHORIZED_ROUTE, get(authorized))
        .route("/{short_url}", get(redirect))
        .with_state(state);
    let listener = tokio::net::TcpListener::bind(SOCKET)
        .await
        .expect("failed to bind TcpListener");
    info!("listening on {SOCKET}");
    axum::serve(listener, router).await.unwrap();
    Ok(())
}

#[derive(Deserialize)]
struct LongUrl {
    url: String,
}

fn shorten_form(short_url: &str) -> Html<String> {
    let mut form = r#"
        <div style="margin:auto;width:50%;padding:10px">
            <form method="post">
                <input type="text" name="url" style="width:100%">
                <input type="submit" value="Shorten" style="margin-top:10px">
            </form>"#
        .to_string();
    if !short_url.is_empty() {
        form += &format!(
            r#"
            <p>
                <a id="short" href="{0}">{0}</a>
                <button onclick="copy()">Copy</button>
            </p>
            <script>
                function copy() {{
                    navigator.clipboard.writeText(document.querySelector("\#short").textContent);
                }}
            </script>"#,
            short_url
        );
    }
    form += "</div>";
    Html(form)
}

fn authorize_link(short_url: &str) -> Html<String> {
    Html(format!(
        "<a href=\"/authorize?short_url={short_url}\">Authorize</a>"
    ))
}

async fn index(user: Option<UserId>) -> Html<String> {
    if user.is_some() {
        return shorten_form("");
    }
    authorize_link("")
}

async fn shorten(
    state: AxumState<State>,
    user: Option<UserId>,
    Form(LongUrl { url }): Form<LongUrl>,
) -> Response {
    let user_id = match user {
        None => return StatusCode::FORBIDDEN.into_response(),
        Some(user) => user.id.get(),
    };
    if url.is_empty() {
        return shorten_form("").into_response();
    }

    let mut short_url = "".to_string();
    for i in 0..20 {
        short_url = nanoid::nanoid!(SHORT_URL_LEN);
        let query = state
            .db_client
            .query_opt(
                "INSERT INTO urls (user_id, short_url, long_url) VALUES ($1, $2, $3) \
                 ON CONFLICT (short_url) DO NOTHING \
                 RETURNING short_url",
                &[&user_id, &short_url, &url],
            )
            .await;
        match query {
            Ok(Some(_)) => break,
            Ok(None) => {
                info!(short_url, "url clash, retry {i}");
                continue;
            }
            Err(err) => {
                error!(%err, "inserting shortened url");
                return StatusCode::INTERNAL_SERVER_ERROR.into_response();
            }
        };
    }
    shorten_form(&format!("{HOST}/{short_url}")).into_response()
}

async fn redirect(
    state: AxumState<State>,
    user: Option<UserId>,
    Path(short_url): Path<String>,
) -> Response {
    let user_id = match user {
        None => return authorize_link(&short_url).into_response(),
        Some(user) => user.id,
    };

    let query = state
        .db_client
        .query_opt(
            "SELECT long_url FROM urls WHERE short_url = $1",
            &[&short_url],
        )
        .await;
    match query {
        Ok(Some(row)) => Redirect::permanent(row.get(0)).into_response(),
        Ok(None) => StatusCode::NOT_FOUND.into_response(),
        Err(err) => {
            error!(%err, %short_url, %user_id, "querying long url");
            StatusCode::INTERNAL_SERVER_ERROR.into_response()
        }
    }
}

#[derive(Deserialize)]
struct AuthorizeQuery {
    short_url: String,
}

async fn authorize(
    state: AxumState<State>,
    jar: PrivateCookieJar,
    Query(AuthorizeQuery { short_url }): Query<AuthorizeQuery>,
) -> (PrivateCookieJar, Redirect) {
    let (jar, auth_redirect) = google_oauth_gate::authorize(state, jar).await;
    let redirect_cookie = Cookie::build((COOKIE_REDIRECT, short_url))
        .path("/")
        //.TODO secure(true) not true for localhost
        //.domain(COOKIE_DOMAIN)
        .secure(false)
        .same_site(axum_extra::extract::cookie::SameSite::Lax)
        .http_only(true)
        .build();
    (jar.add(redirect_cookie), auth_redirect)
}

async fn authorized(
    state: AxumState<State>,
    jar: PrivateCookieJar,
    query: Query<AuthRequest>,
) -> Result<(PrivateCookieJar, Redirect), Response> {
    use google_oauth_gate::authorized;
    let jar = authorized(state, jar, query).await.map_err(|err| {
        error!(%err, "authorizing");
        return StatusCode::UNAUTHORIZED.into_response();
    })?;
    let Some(redirect_cookie) = jar.get(COOKIE_REDIRECT) else {
        return Ok((jar, Redirect::to("/")));
    };
    let redirect_url = Redirect::to(&format!("/{}", redirect_cookie.value_trimmed()));
    Ok((jar.remove(redirect_cookie), redirect_url))
}
