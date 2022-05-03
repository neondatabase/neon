use std::fs::File;
use anyhow::Result;
use postgres_ffi::waldecoder::WalStreamDecoder;
use utils::lsn::Lsn;


#[tokio::main]
async fn main() -> Result<()> {
    let partial_path = "/home/bojan/tmp/pg_wal/000000010000000000000017";
    let startpos = Lsn(0);
    let bytes = std::fs::read(partial_path)?;

    let mut decoder = WalStreamDecoder::new(startpos);
    decoder.feed_bytes(&bytes);
    while let Some((lsn, rec)) = decoder.poll_decode()? {
        println!("lsn: {}", lsn);
    }
    Ok(())
}
