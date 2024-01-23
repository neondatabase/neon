use std::{
    io,
    net::{TcpListener, ToSocketAddrs},
};

use nix::sys::socket::{setsockopt, sockopt::ReuseAddr};

/// Bind a [`TcpListener`] to addr with `SO_REUSEADDR` set to true.
pub fn bind<A: ToSocketAddrs>(addr: A) -> io::Result<TcpListener> {
    let listener = TcpListener::bind(addr)?;

    setsockopt(&listener, ReuseAddr, &true)?;

    Ok(listener)
}
