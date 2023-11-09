use std::{cmp::min, sync::Arc};

use anyhow::Context;
use postgres_backend::{CopyStreamHandlerEnd, PostgresBackend};
use postgres_ffi::{get_current_timestamp, waldecoder::WalStreamDecoder, MAX_SEND_SIZE};
use pq_proto::{BeMessage, WalSndKeepAlive, XLogDataBody};
use tokio::io::{AsyncRead, AsyncWrite};
use tracing::{trace};
use utils::lsn::Lsn;

use crate::{
    safekeeper::Term,
    send_wal::{wait_for_lsn, EndWatch, WalSenderGuard},
    timeline::Timeline,
    wal_storage::WalReader,
};

/// A half driving sending WAL.
pub struct WalSender<'a, IO> {
    pub pgb: &'a mut PostgresBackend<IO>,
    pub tli: Arc<Timeline>,
    pub appname: Option<String>,
    // Position since which we are sending next chunk.
    pub start_pos: Lsn,
    // WAL up to this position is known to be locally available.
    // Usually this is the same as the latest commit_lsn, but in case of
    // walproposer recovery, this is flush_lsn.
    //
    // We send this LSN to the receiver as wal_end, so that it knows how much
    // WAL this safekeeper has. This LSN should be as fresh as possible.
    pub end_pos: Lsn,
    /// When streaming uncommitted part, the term the client acts as the leader
    /// in. Streaming is stopped if local term changes to a different (higher)
    /// value.
    pub term: Option<Term>,
    /// Watch channel receiver to learn end of available WAL (and wait for its advancement).
    pub end_watch: EndWatch,
    pub ws_guard: Arc<WalSenderGuard>,
    pub wal_reader: WalReader,
    // buffer for readling WAL into to send it
    pub send_buf: [u8; MAX_SEND_SIZE],
    pub waldecoder: WalStreamDecoder,
}

impl<IO: AsyncRead + AsyncWrite + Unpin> WalSender<'_, IO> {
    /// Send WAL until
    /// - an error occurs
    /// - receiver is caughtup and there is no computes (if streaming up to commit_lsn)
    ///
    /// Err(CopyStreamHandlerEnd) is always returned; Result is used only for ?
    /// convenience.
    pub async fn run(&mut self) -> Result<(), CopyStreamHandlerEnd> {
        loop {
            // Wait for the next portion if it is not there yet, or just
            // update our end of WAL available for sending value, we
            // communicate it to the receiver.
            self.wait_wal().await?;
            assert!(
                self.end_pos > self.start_pos,
                "nothing to send after waiting for WAL"
            );

            // try to send as much as available, capped by MAX_SEND_SIZE
            let mut send_size = self
                .end_pos
                .checked_sub(self.start_pos)
                .context("reading wal without waiting for it first")?
                .0 as usize;
            send_size = min(send_size, self.send_buf.len());
            let send_buf = &mut self.send_buf[..send_size];
            let send_size: usize;
            {
                // If uncommitted part is being pulled, check that the term is
                // still the expected one.
                let _term_guard = if let Some(t) = self.term {
                    Some(self.tli.acquire_term(t).await?)
                } else {
                    None
                };
                // read wal into buffer
                send_size = self.wal_reader.read(send_buf).await?
            };
            let send_buf = &send_buf[..send_size];

            // feed waldecoder with the data
            self.waldecoder.feed_bytes(send_buf);
            self.start_pos += send_size as u64;

            while let Some((lsn, recdata)) =
                self.waldecoder.poll_decode().context("wal decoding")?
            {
                // It is important to deal with the aligned records as lsn in getPage@LSN is
                // aligned and can be several bytes bigger. Without this alignment we are
                // at risk of hitting a deadlock.
                if !lsn.is_aligned() {
                    return Err(CopyStreamHandlerEnd::ServerInitiated(format!(
                        "unaligned record at {}",
                        lsn
                    )));
                }

                trace!(
                    "read record of {} bytes of WAL ending at {}",
                    recdata.len(),
                    lsn
                );

                // and send it
                self.pgb
                    .write_message(&BeMessage::XLogData(XLogDataBody {
                        wal_start: lsn.0,
                        wal_end: self.end_pos.0,
                        timestamp: get_current_timestamp(),
                        data: &recdata,
                    }))
                    .await?;
            }
        }
    }

    /// wait until we have WAL to stream, sending keepalives and checking for
    /// exit in the meanwhile
    async fn wait_wal(&mut self) -> Result<(), CopyStreamHandlerEnd> {
        loop {
            self.end_pos = self.end_watch.get();
            if self.end_pos > self.start_pos {
                // We have something to send.
                trace!("got end_pos {:?}, streaming", self.end_pos);
                return Ok(());
            }

            // Wait for WAL to appear, now self.end_pos == self.start_pos.
            if let Some(lsn) = wait_for_lsn(&mut self.end_watch, self.term, self.start_pos).await? {
                self.end_pos = lsn;
                trace!("got end_pos {:?}, streaming", self.end_pos);
                return Ok(());
            }

            // Timed out waiting for WAL, check for termination and send KA.
            // Check for termination only if we are streaming up to commit_lsn
            // (to pageserver).
            if let EndWatch::Commit(_) = self.end_watch {
                if self.ws_guard.should_stop(&self.tli).await {
                    return Err(CopyStreamHandlerEnd::ServerInitiated(format!(
                        "ending streaming to {:?} at {}, receiver is caughtup and there is no computes",
                        self.appname, self.start_pos,
                    )));
                }
            }

            self.pgb
                .write_message(&BeMessage::KeepAlive(WalSndKeepAlive {
                    wal_end: self.end_pos.0,
                    timestamp: get_current_timestamp(),
                    request_reply: true,
                }))
                .await?;
        }
    }
}
