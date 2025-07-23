//! Client for making request to a running Postgres server's communicator control socket.
//!
//! The storage communicator process that runs inside Postgres exposes an HTTP endpoint in
//! a Unix Domain Socket in the Postgres data directory. This provides access to it.

use std::path::Path;

use anyhow::Context;
use hyper::client::conn::http1::SendRequest;
use hyper_util::rt::TokioIo;

/// Name of the socket within the Postgres data directory. This better match that in
/// `pgxn/neon/communicator/src/lib.rs`.
const NEON_COMMUNICATOR_SOCKET_NAME: &str = "neon-communicator.socket";

/// Open a connection to the communicator's control socket, prepare to send requests to it
/// with hyper.
pub async fn connect_communicator_socket<B>(pgdata: &Path) -> anyhow::Result<SendRequest<B>>
where
    B: hyper::body::Body + 'static + Send,
    B::Data: Send,
    B::Error: Into<Box<dyn std::error::Error + Send + Sync>>,
{
    let socket_path = pgdata.join(NEON_COMMUNICATOR_SOCKET_NAME);
    let socket_path_len = socket_path.display().to_string().len();

    // There is a limit of around 100 bytes (108 on Linux?) on the length of the path to a
    // Unix Domain socket. The limit is on the connect(2) function used to open the
    // socket, not on the absolute path itself. Postgres changes the current directory to
    // the data directory and uses a relative path to bind to the socket, and the relative
    // path "./neon-communicator.socket" is always short, but when compute_ctl needs to
    // open the socket, we need to use a full path, which can be arbitrarily long.
    //
    // There are a few ways we could work around this:
    //
    // 1. Change the current directory to the Postgres data directory and use a relative
    //    path in the connect(2) call. That's problematic because the current directory
    //    applies to the whole process. We could change the current directory early in
    //    compute_ctl startup, and that might be a good idea anyway for other reasons too:
    //    it would be more robust if the data directory is moved around or unlinked for
    //    some reason, and you would be less likely to accidentally litter other parts of
    //    the filesystem with e.g. temporary files. However, that's a pretty invasive
    //    change.
    //
    // 2. On Linux, you could open() the data directory, and refer to the the socket
    //    inside it as "/proc/self/fd/<fd>/neon-communicator.socket". But that's
    //    Linux-only.
    //
    // 3. Create a symbolic link to the socket with a shorter path, and use that.
    //
    // We use the symbolic link approach here. Hopefully the paths we use in production
    // are shorter, so that we can open the socket directly, so that this hack is needed
    // only in development.
    let connect_result = if socket_path_len < 100 {
        // We can open the path directly with no hacks.
        tokio::net::UnixStream::connect(socket_path).await
    } else {
        // The path to the socket is too long. Create a symlink to it with a shorter path.
        let short_path = std::env::temp_dir().join(format!(
            "compute_ctl.short-socket.{}.{}",
            std::process::id(),
            tokio::task::id()
        ));
        std::os::unix::fs::symlink(&socket_path, &short_path)?;

        // Delete the symlink as soon as we have connected to it. There's a small chance
        // of leaking if the process dies before we remove it, so try to keep that window
        // as small as possible.
        scopeguard::defer! {
            if let Err(err) = std::fs::remove_file(&short_path) {
                tracing::warn!("could not remove symlink \"{}\" created for socket: {}",
                               short_path.display(), err);
            }
        }

        tracing::info!(
            "created symlink \"{}\" for socket \"{}\", opening it now",
            short_path.display(),
            socket_path.display()
        );

        tokio::net::UnixStream::connect(&short_path).await
    };

    let stream = connect_result.context("connecting to communicator control socket")?;

    let io = TokioIo::new(stream);
    let (request_sender, connection) = hyper::client::conn::http1::handshake(io).await?;

    // spawn a task to poll the connection and drive the HTTP state
    tokio::spawn(async move {
        if let Err(err) = connection.await {
            eprintln!("Error in connection: {err}");
        }
    });

    Ok(request_sender)
}
