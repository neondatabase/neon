use std::time::Duration;

use anyhow::Context;
use futures::StreamExt;
use pageserver_api::shard::ShardIdentity;
use postgres_backend::{CopyStreamHandlerEnd, PostgresBackend};
use postgres_ffi::MAX_SEND_SIZE;
use postgres_ffi::{get_current_timestamp, waldecoder::WalStreamDecoder};
use pq_proto::{BeMessage, InterpretedWalRecordsBody, WalSndKeepAlive};
use tokio::io::{AsyncRead, AsyncWrite};
use utils::lsn::Lsn;
use wal_decoder::codec::Encoder;
use wal_decoder::models::{InterpretedWalRecord, InterpretedWalRecords};

use crate::send_wal::EndWatchView;
use crate::wal_reader_stream::{WalBytes, WalReaderStreamBuilder};

/// Shard-aware interpreted record sender.
/// This is used for sending WAL to the pageserver. Said WAL
/// is pre-interpreted and filtered for the shard.
pub(crate) struct InterpretedWalSender<'a, IO> {
    pub(crate) encoder: Box<dyn Encoder>,
    pub(crate) pgb: &'a mut PostgresBackend<IO>,
    pub(crate) wal_stream_builder: WalReaderStreamBuilder,
    pub(crate) end_watch_view: EndWatchView,
    pub(crate) shard: ShardIdentity,
    pub(crate) pg_version: u32,
    pub(crate) appname: Option<String>,
}

struct Batch {
    wal_end_lsn: Lsn,
    available_wal_end_lsn: Lsn,
    records: InterpretedWalRecords,
}

impl<IO: AsyncRead + AsyncWrite + Unpin> InterpretedWalSender<'_, IO> {
    /// Send interpreted WAL to a receiver.
    /// Stops when an error occurs or the receiver is caught up and there's no active compute.
    ///
    /// Err(CopyStreamHandlerEnd) is always returned; Result is used only for ?
    /// convenience.
    pub(crate) async fn run(self) -> Result<(), CopyStreamHandlerEnd> {
        const KEEPALIVE_AFTER: Duration = Duration::from_secs(1);

        let mut wal_position = self.wal_stream_builder.start_pos();
        let mut wal_decoder =
            WalStreamDecoder::new(self.wal_stream_builder.start_pos(), self.pg_version);

        let stream = self.wal_stream_builder.build(MAX_SEND_SIZE).await?;
        let mut stream = std::pin::pin!(stream);

        let (tx, mut rx) = tokio::sync::mpsc::channel::<Batch>(2);

        let wal_read_future = async move {
            loop {
                let _guard = tx
                    .reserve()
                    .await
                    .with_context(|| "Failed to reserve channel slot")?;
                let wal = stream.next().await;

                let WalBytes {
                    wal,
                    wal_start_lsn: _,
                    wal_end_lsn,
                    available_wal_end_lsn,
                } = match wal {
                    Some(some) => some?,
                    None => {
                        break;
                    }
                };

                wal_position = wal_end_lsn;
                wal_decoder.feed_bytes(&wal);

                let mut records = Vec::new();
                let mut max_next_record_lsn = None;
                while let Some((next_record_lsn, recdata)) = wal_decoder
                    .poll_decode()
                    .with_context(|| "Failed to decode WAL")?
                {
                    assert!(next_record_lsn.is_aligned());
                    max_next_record_lsn = Some(next_record_lsn);

                    // Deserialize and interpret WAL record
                    let interpreted = InterpretedWalRecord::from_bytes_filtered(
                        recdata,
                        &self.shard,
                        next_record_lsn,
                        self.pg_version,
                    )
                    .with_context(|| "Failed to interpret WAL")?;

                    if !interpreted.is_empty() {
                        records.push(interpreted);
                    }
                }

                let batch = InterpretedWalRecords {
                    records,
                    next_record_lsn: max_next_record_lsn,
                };

                tx.send(Batch {
                    wal_end_lsn,
                    available_wal_end_lsn,
                    records: batch,
                })
                .await
                .unwrap();
            }

            Ok::<_, CopyStreamHandlerEnd>(wal_position)
        };

        let encode_and_send_future = async move {
            loop {
                let timeout_or_batch = tokio::time::timeout(KEEPALIVE_AFTER, rx.recv()).await;
                let batch = match timeout_or_batch {
                    Ok(batch) => match batch {
                        Some(b) => b,
                        None => {
                            break;
                        }
                    },
                    Err(_) => {
                        self.pgb
                            .write_message(&BeMessage::KeepAlive(WalSndKeepAlive {
                                wal_end: self.end_watch_view.get().0,
                                timestamp: get_current_timestamp(),
                                request_reply: true,
                            }))
                            .await?;

                        continue;
                    }
                };

                let buf = self
                    .encoder
                    .encode(batch.records)
                    .await
                    .with_context(|| "Failed to serialize interpreted WAL")
                    .map_err(CopyStreamHandlerEnd::from)?;

                self.pgb
                    .write_message(&BeMessage::InterpretedWalRecords(
                        InterpretedWalRecordsBody {
                            streaming_lsn: batch.wal_end_lsn.0,
                            commit_lsn: batch.available_wal_end_lsn.0,
                            data: &buf,
                        },
                    ))
                    .await?;
            }

            Ok::<_, CopyStreamHandlerEnd>(())
        };

        let pipeline_ok = tokio::try_join!(wal_read_future, encode_and_send_future)?;
        let (final_wal_position, _) = pipeline_ok;

        // The loop above ends when the receiver is caught up and there's no more WAL to send.
        Err(CopyStreamHandlerEnd::ServerInitiated(format!(
            "ending streaming to {:?} at {}, receiver is caughtup and there is no computes",
            self.appname, final_wal_position,
        )))
    }
}
