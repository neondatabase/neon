use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::path::Path;
use std::str;
use std::{fs, thread};

use anyhow::Context;
use tracing::info;

pub fn download_file(mut stream: TcpStream) -> anyhow::Result<()> {
    let mut buf = [0; 512];

    stream.read(&mut buf).expect("Error reading from stream");

    let filename = str::from_utf8(&buf)
        .context("filename is not UTF-8")?
        .trim_end();

    println!("requested file {}", filename);

    let from_prefix = "/tmp/from_prefix";
    let to_prefix = "/tmp/to_prefix";

    let filepath = Path::new(from_prefix).join(filename);
    let copy_to_filepath = Path::new(to_prefix).join(filename);
    fs::copy(filepath, copy_to_filepath)?;

    // Write back the response to the TCP stream
    match stream.write("OK".as_bytes()) {
        Err(e) => anyhow::bail!("Read-Server: Error writing to stream {}", e),
        Ok(_) => (),
    }

    Ok(())
}
