use tokio::{
    net::{TcpListener, ToSocketAddrs},
    runtime,
};

lazy_static::lazy_static! {
    static ref RUNTIME: runtime::Runtime = runtime::Builder::new_current_thread().enable_all().build().unwrap();
}

/// Bind a [`TcpListener`] to addr with `SO_REUSEADDR` set to true (done implicitly in tokio).
pub fn bind<A: ToSocketAddrs>(addr: A) -> std::io::Result<TcpListener> {
    RUNTIME.block_on(TcpListener::bind(addr))
}

pub fn to_blocking_listener(tokio_listener: TcpListener) -> std::io::Result<std::net::TcpListener> {
    let std_listener = tokio_listener.into_std()?;
    std_listener.set_nonblocking(false)?;
    Ok(std_listener)
}
