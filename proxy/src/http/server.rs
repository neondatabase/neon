use regex::Regex;
use rustls::{OwnedTrustAnchor, RootCertStore};
use std::{net::TcpListener, sync::Arc};

use anyhow::anyhow;
use futures::{sink::SinkExt, stream::StreamExt};
use hyper::{Body, Request, Response, StatusCode};
use hyper_tungstenite::tungstenite::Message;
use hyper_tungstenite::HyperWebsocket;
use tokio::io::AsyncReadExt;
use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;
use utils::http::{
    endpoint, error::ApiError, json::json_response, request::parse_request_param, RouterBuilder,
    RouterService,
};

async fn status_handler(_: Request<Body>) -> Result<Response<Body>, ApiError> {
    json_response(StatusCode::OK, "")
}

async fn ws_handler(mut request: Request<Body>) -> Result<Response<Body>, ApiError> {
    // Check if the request is a websocket upgrade request.
    if hyper_tungstenite::is_upgrade_request(&request) {
        let (response, websocket) = hyper_tungstenite::upgrade(&mut request, None)
            .map_err(|e| ApiError::BadRequest(e.into()))?;

        let connstr: String = parse_request_param(&request, "connstr")?;
        let r = Regex::new(r"@(.*:(\d+)").unwrap();
        let caps = r.captures(&connstr).unwrap();
        let endpoint: String = caps.get(1).unwrap().as_str().to_owned();

        // connect to itself, but through remote endpoint, lol
        let neon_sock = TcpStream::connect(endpoint)
            .await
            .map_err(|e| ApiError::InternalServerError(e.into()))?;

        // Spawn a task to handle the websocket connection.
        tokio::spawn(async move {
            if let Err(e) = serve_websocket(websocket, neon_sock).await {
                eprintln!("Error in websocket connection: {}", e);
            }
        });

        // Return the response so the spawned future can continue.
        Ok(response)
    } else {
        json_response(StatusCode::OK, "hi")
    }
}

async fn serve_websocket(
    websocket: HyperWebsocket,
    mut neon_sock: TcpStream,
) -> anyhow::Result<()> {
    let mut websocket = websocket.await?;
    let mut buf = [0u8; 8192];

    tokio::select! {
        Some(message) = websocket.next() => {
            match message? {
                Message::Text(msg) => {
                    println!("Received text message: {}", msg);
                }
                Message::Binary(msg) => {
                    println!("Received binary message: {:02X?}", msg);
                    neon_sock.write_all(&msg).await?;
                    websocket
                        .send(Message::binary(b"Thank you, come again.".to_vec()))
                        .await?;
                }
                Message::Ping(msg) => {
                    // No need to send a reply: tungstenite takes care of this for you.
                    println!("Received ping message: {:02X?}", msg);
                }
                Message::Pong(msg) => {
                    println!("Received pong message: {:02X?}", msg);
                }
                Message::Close(msg) => {
                    // No need to send a reply: tungstenite takes care of this for you.
                    if let Some(msg) = &msg {
                        println!(
                            "Received close message with code {} and message: {}",
                            msg.code, msg.reason
                        );
                    } else {
                        println!("Received close message");
                    }
                }
                Message::Frame(_msg) => {
                    unreachable!();
                }
            }
        },
        res = neon_sock.read(&mut buf) => {
            let res = res?;
            websocket
            .send(Message::binary(&buf[0..res]))
            .await?;
        }
    }

    Ok(())
}

fn make_router() -> RouterBuilder<hyper::Body, ApiError> {
    let router = endpoint::make_router();
    router
        .get("/v1/status", status_handler)
        .get("/v1/ws", ws_handler)
}

pub async fn thread_main(http_listener: TcpListener) -> anyhow::Result<()> {
    scopeguard::defer! {
        println!("http has shut down");
    }

    let service = || RouterService::new(make_router().build()?);

    hyper::Server::from_tcp(http_listener)?
        .serve(service().map_err(|e| anyhow!(e))?)
        .await?;

    Ok(())
}
