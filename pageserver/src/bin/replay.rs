use anyhow::Result;
use postgres_ffi::{pg_constants::WAL_SEGMENT_SIZE, waldecoder::WalStreamDecoder};
use utils::lsn::Lsn;


#[tokio::main]
async fn main() -> Result<()> {
    let partial_path = "/home/bojan/tmp/sk_wal";
    let startpos = Lsn(23761464);  // I got this by grepping sk log for "restart decoder"
    let xlogoff: usize = startpos.segment_offset(WAL_SEGMENT_SIZE);

    let mut decoder = WalStreamDecoder::new(startpos);
    let bytes = std::fs::read(partial_path)?;
    decoder.feed_bytes(&bytes[xlogoff..]);

    while let Some((lsn, rec)) = decoder.poll_decode()? {
        println!("lsn: {}", lsn);
    }
    Ok(())
}
