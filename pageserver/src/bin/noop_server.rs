use anyhow::Context;
use bytes::Buf;
use clap::Parser;
use pageserver_api::models::{PagestreamBeMessage, PagestreamErrorResponse, PagestreamFeMessage};
use postgres_backend::{AuthType, PostgresBackend, QueryError};
use pq_proto::{BeMessage, FeMessage};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio_util::sync::CancellationToken;

#[derive(clap::Parser)]
struct Args {
    bind: String,
}

#[tokio::main]
async fn main() {
    let args = Args::parse();

    let listener = tokio::net::TcpListener::bind(&args.bind).await.unwrap();
    loop {
        let (socket, _) = listener.accept().await.unwrap();
        tokio::spawn(async move {
            handle_connection(socket).await.unwrap();
        });
    }
}

async fn handle_connection(socket: tokio::net::TcpStream) -> anyhow::Result<()> {
    socket
        .set_nodelay(true)
        .context("could not set TCP_NODELAY")?;

    let peer_addr = socket.peer_addr().context("get peer address")?;
    let socket = tokio_io_timeout::TimeoutReader::new(socket);
    tokio::pin!(socket);
    let pgbackend = PostgresBackend::new_from_io(socket, peer_addr, AuthType::Trust, None)?;
    let mut conn_handler = NoOpHandler;
    let cancel = CancellationToken::new();
    pgbackend
        .run(&mut conn_handler, || {
            let cancel = cancel.clone();
            async move { cancel.cancelled().await }
        })
        .await?;
    anyhow::Ok(())
}

struct NoOpHandler;

#[async_trait::async_trait]
impl<IO> postgres_backend::Handler<IO> for NoOpHandler
where
    IO: AsyncRead + AsyncWrite + Send + Sync + Unpin,
{
    fn startup(
        &mut self,
        _pgb: &mut PostgresBackend<IO>,
        _sm: &pq_proto::FeStartupPacket,
    ) -> Result<(), QueryError> {
        Ok(())
    }

    async fn process_query(
        &mut self,
        pgb: &mut PostgresBackend<IO>,
        query_string: &str,
    ) -> Result<(), QueryError> {
        if !query_string.starts_with("pagestream ") {
            return Err(QueryError::Other(anyhow::anyhow!("not a pagestream query")));
        }

        // switch client to COPYBOTH
        pgb.write_message_noflush(&BeMessage::CopyBothResponse)?;
        pgb.flush().await?;

        loop {
            let msg = pgb.read_message().await?;

            let copy_data_bytes = match msg {
                Some(FeMessage::CopyData(bytes)) => bytes,
                Some(FeMessage::Terminate) => return Ok(()),
                Some(m) => {
                    return Err(QueryError::Other(anyhow::anyhow!(
                        "unexpected message: {m:?} during COPY"
                    )));
                }
                None => return Ok(()), // client disconnected
            };

            let neon_fe_msg = PagestreamFeMessage::parse(&mut copy_data_bytes.reader())?;

            let response = match neon_fe_msg {
                PagestreamFeMessage::NoOp => Ok(PagestreamBeMessage::NoOp),
                x => Err(QueryError::Other(anyhow::anyhow!(
                    "this server only supports no-op: {x:?}"
                ))),
            };

            let response = response.unwrap_or_else(|e| {
                PagestreamBeMessage::Error(PagestreamErrorResponse {
                    message: e.to_string(),
                })
            });

            pgb.write_message_noflush(&BeMessage::CopyData(&response.serialize()))?;
            pgb.flush().await?;
        }
    }
}
