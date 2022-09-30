use std::convert::Infallible;
use std::net::TcpListener;

use anyhow::anyhow;
use futures::{sink::SinkExt, stream::StreamExt};
use hyper::{Body, Request, Response, StatusCode};
use hyper_tungstenite::tungstenite::Message;
use hyper_tungstenite::HyperWebsocket;
use tokio::io::AsyncReadExt;
use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;
use utils::http::{endpoint, error::ApiError, json::json_response, RouterBuilder, RouterService};

async fn status_handler(_: Request<Body>) -> Result<Response<Body>, ApiError> {
    json_response(StatusCode::OK, "")
}

async fn ws_handler(mut request: Request<Body>) -> Result<Response<Body>, ApiError> {
    // fetch query param
    let endpoint = request.uri().query().and_then(|v| {
        url::form_urlencoded::parse(v.as_bytes())
            .into_owned()
            .map(|(_name, value)| value)
            .next()
    });

    // Check if the request is a websocket upgrade request.
    if hyper_tungstenite::is_upgrade_request(&request) {
        let (response, websocket) = hyper_tungstenite::upgrade(&mut request, None)
            .map_err(|e| ApiError::BadRequest(e.into()))?;

        // forward somewhere, if asked
        if let Some(endpoint) = endpoint {
            println!("forwarding to {:?}", endpoint);
            let neon_sock = TcpStream::connect(endpoint)
                .await
                .map_err(|e| ApiError::InternalServerError(e.into()))?;

            // Spawn a task to handle the websocket connection.
            tokio::spawn(async move {
                if let Err(e) = serve_websocket(websocket, neon_sock).await {
                    println!("Error in websocket connection: {:?}", e);
                }
            });

            // Return the response so the spawned future can continue.
            return Ok(response);
        }

        panic!("ffewfwe");
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

    println!("serving ws");

    loop {
        tokio::select! {
            Some(message) = websocket.next() => {
                match message? {
                    Message::Text(msg) => {
                        println!("Received text message: {}", msg);
                    }
                    Message::Binary(msg) => {
                        println!("Received binary message: {:02X?}", msg);
                        neon_sock.write_all(&msg).await?;
                        neon_sock.flush().await?;
                        println!("sent that binary msg");
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
                if res == 0 {
                    anyhow::bail!("read 0 from pg");
                }
                println!("Received binary message from pg: {:02X?}", &buf[0..res]);
                websocket
                .send(Message::binary(&buf[0..res]))
                .await?;
            }
        }
    }
}

fn make_router() -> RouterBuilder<hyper::Body, ApiError> {
    let router = endpoint::make_router();
    router
        .get("/", ws_handler)
        .get("/v1/status", status_handler)
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

pub async fn ws_thread_main(ws_listener: TcpListener) -> anyhow::Result<()> {
    scopeguard::defer! {
        println!("ws has shut down");
    }

    hyper::Server::from_tcp(ws_listener)?
        .serve(hyper::service::make_service_fn(|_connection| async {
            Ok::<_, Infallible>(hyper::service::service_fn(ws_handler))
        }))
        .await?;
    Ok(())
}
