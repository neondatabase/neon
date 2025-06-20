use bytes::BytesMut;
use postgres_protocol2::message::frontend;
use tokio::io::{AsyncRead, AsyncWrite, AsyncWriteExt};

use crate::Error;

pub async fn cancel_query_raw<S>(
    mut stream: S,
    process_id: i32,
    secret_key: i32,
) -> Result<(), Error>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let mut buf = BytesMut::new();
    frontend::cancel_request(process_id, secret_key, &mut buf);

    stream.write_all(&buf).await.map_err(Error::io)?;
    stream.flush().await.map_err(Error::io)?;
    stream.shutdown().await.map_err(Error::io)?;

    Ok(())
}
