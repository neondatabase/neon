use anyhow::anyhow;
use hyper::{Body, Request, Response, StatusCode};
use routerify::RouterBuilder;
use routerify::ext::RequestExt;
use tokio::net::TcpStream;
use std::net::TcpListener;
use zenith_utils::http::endpoint;
use zenith_utils::http::error::ApiError;
use zenith_utils::http::json::json_response;

use crate::{cplane_api::{DatabaseInfo, ProxyAuthResponse}, mgmt::{PsqlSessionResponse, PsqlSessionResult}};

async fn auth_handler(_: Request<Body>) -> Result<Response<Body>, ApiError> {
    Ok(json_response(StatusCode::OK, ProxyAuthResponse::Ready {
        conn_info: DatabaseInfo {
            host: "127.0.0.1".into(),
            port: 5432,
            dbname: "postgres".into(),
            user: "postgres".into(),
            password: Some("postgres".into()),
        }
    })?)
}

async fn link_handler(req: Request<Body>) -> Result<Response<Body>, ApiError> {
    let session_id = req.param("sessionId").unwrap().to_string();

    let payload = PsqlSessionResponse {
        session_id,
        result: PsqlSessionResult::Success(DatabaseInfo {
                host: "127.0.0.1".into(),
                port: 5432,
                dbname: "postgres".into(),
                user: "postgres".into(),
                password: Some("postgres".into()),
            })

    };
    let query_text = &serde_json::to_string(&payload).unwrap();

    let _output = std::process::Command::new("psql")
        .args(["-h", "127.0.0.1",
               "-p", "7000",
               "-c", query_text])
        .output()
        .unwrap();

    Ok(json_response(StatusCode::OK, "")?)
}

fn make_router() -> RouterBuilder<hyper::Body, ApiError> {
    let router = endpoint::make_router();
    router
        .get("/authenticate_proxy_request", auth_handler)
        .get("/psql_session/:sessionId", link_handler)
}

pub async fn thread_main(http_listener: TcpListener) -> anyhow::Result<()> {
    let service = || routerify::RouterService::new(make_router().build()?);

    hyper::Server::from_tcp(http_listener)?
        .serve(service().map_err(|e| anyhow!(e))?)
        .await?;

    Ok(())
}
