use std::thread;

mod page_cache;
mod walreader;
mod walreceiver;

fn main() -> Result<(), Error> {


    // Launch the WAL receiver thread. It will try to connect to the WAL safekeeper,
    // and stream the WAL. If the connection is lost, it will reconnect on its own.
    // We just fire and forget it here.
    let handler = thread::spawn(|| {
        // thread code
        walreceiver::thread_main();
    });

    handler.join();     // never returns.

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
