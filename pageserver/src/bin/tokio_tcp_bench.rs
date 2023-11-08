use std::env::args;

use clap::Parser;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

#[derive(clap::Parser)]
struct Args {
    #[clap(subcommand)]
    mode: Mode,
}

#[derive(clap::Parser)]
enum Mode {
    Client(Client),
    Server(Server),
}

#[derive(clap::Parser)]
struct Client {
    num_tasks: usize,
}
#[derive(clap::Parser)]
struct Server {}

#[tokio::main]
async fn main() {
    let args: &'static _ = Box::leak(Box::new(Args::parse()));

    match &args.mode {
        Mode::Client(x) => client::client(x).await,
        Mode::Server(x) => server::server(x).await,
    }
}

mod client {
    use std::sync::{atomic::{Ordering, AtomicU64}, Arc};

    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    use super::Client;

    #[derive(Debug, Default)]
    struct Stats {
        completed_requests: AtomicU64,
    }

    impl Stats {
        fn inc(&self) {
            self.completed_requests.fetch_add(1, Ordering::Relaxed);
        }
    }
    pub(crate) async fn client(args: &'static Client) {
        let mut stats = Arc::new(Stats::default());

        tokio::spawn({
            let stats = Arc::clone(&stats);
            async move {
                loop {
                    let start = std::time::Instant::now();
                    tokio::time::sleep(std::time::Duration::from_secs(1)).await;
                    let completed_requests = stats.completed_requests.swap(0, Ordering::Relaxed);
                    let elapsed = start.elapsed();
                    println!(
                        "RPS: {:.0} RPS/client: {:.2}",
                        completed_requests as f64 / elapsed.as_secs_f64(),
                        completed_requests as f64 / elapsed.as_secs_f64() / args.num_tasks as f64,
                    );
                }
            }
        });

        let mut tasks = Vec::new();
        for _  in 0..args.num_tasks {
            let stats = Arc::clone(&stats);
            let t = tokio::spawn(client_task(args, stats));
            tasks.push(t);
        }

        for t in tasks {
            t.await.unwrap();
        }
    }

    async fn client_task(args: &'static Client, stats: Arc<Stats>) -> anyhow::Result<()> {
        let mut conn = tokio::net::TcpStream::connect("localhost:65000").await?;
        conn.set_nodelay(true)?;

        loop {
            let mut buf = [0u8; 1];
            conn.write_all(&buf).await?;
            conn.read_exact(&mut buf).await?;
            stats.inc();
        }
    }
}

mod server {

    use anyhow::Context;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    use super::Server;

    pub(crate) async fn server(args: &'static Server) {
        let listener = tokio::net::TcpListener::bind("localhost:65000").await.unwrap();
        loop {
            let (socket, _) = listener.accept().await.unwrap();
            tokio::spawn(async move {
                server_handle_connection(args, socket).await.unwrap();
            });
        }
    }

    async fn server_handle_connection(
        args: &'static Server,
        socket: tokio::net::TcpStream,
    ) -> anyhow::Result<()> {
        socket
            .set_nodelay(true)
            .context("could not set TCP_NODELAY")?;
        // let socket = tokio_io_timeout::TimeoutReader::new(socket);
        tokio::pin!(socket);

        loop {
            let mut buf = [0u8; 4096];
            socket.read_exact(&mut buf).await?;
            socket.write_all(&buf).await?;
        }
    }
}
