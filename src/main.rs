
use postgres_protocol::message::backend::ReplicationMessage;
use tokio_stream::StreamExt;
use tokio_postgres::{connect_replication, Error, NoTls, ReplicationMode};

#[tokio::main] // By default, tokio_postgres uses the tokio crate as its runtime.
async fn main() -> Result<(), Error> {
    let conninfo = "host=localhost port=65432 user=stas dbname=postgres";
    // form replication connection
    let (mut rclient, rconnection) =
        connect_replication(conninfo, NoTls, ReplicationMode::Physical).await?;
    tokio::spawn(async move {
        if let Err(e) = rconnection.await {
            eprintln!("connection error: {}", e);
        }
    });
    let identify_system = rclient.identify_system().await?;
    let mut physical_stream = rclient
        .start_physical_replication(None, identify_system.xlogpos(), None)
        .await?;
    while let Some(replication_message) = physical_stream.next().await {
        match replication_message? {
            ReplicationMessage::XLogData(xlog_data) => {
                // eprintln!("received XLogData: {:#?}", xlog_data);
                eprintln!("received XLogData:");
            }
            ReplicationMessage::PrimaryKeepAlive(keepalive) => {
                // eprintln!("received PrimaryKeepAlive: {:#?}", keepalive);
                eprintln!("received PrimaryKeepAlive:");
            }
            _ => (),
        }
    }
    Ok(())
}
