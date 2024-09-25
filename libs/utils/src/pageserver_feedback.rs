use std::time::{Duration, SystemTime};

use bytes::{Buf, BufMut, Bytes, BytesMut};
use pq_proto::{read_cstr, PG_EPOCH};
use serde::{Deserialize, Serialize};
use tracing::{trace, warn};

use crate::lsn::Lsn;

/// Feedback pageserver sends to safekeeper and safekeeper resends to compute.
///
/// Serialized in custom flexible key/value format. In replication protocol, it
/// is marked with NEON_STATUS_UPDATE_TAG_BYTE to differentiate from postgres
/// Standby status update / Hot standby feedback messages.
///
/// serde Serialize is used only for human readable dump to json (e.g. in
/// safekeepers debug_dump).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct PageserverFeedback {
    /// Last known size of the timeline. Used to enforce timeline size limit.
    pub current_timeline_size: u64,
    /// LSN last received and ingested by the pageserver. Controls backpressure.
    pub last_received_lsn: Lsn,
    /// LSN up to which data is persisted by the pageserver to its local disc.
    /// Controls backpressure.
    pub disk_consistent_lsn: Lsn,
    /// LSN up to which data is persisted by the pageserver on s3; safekeepers
    /// consider WAL before it can be removed.
    pub remote_consistent_lsn: Lsn,
    // Serialize with RFC3339 format.
    #[serde(with = "serde_systemtime")]
    pub replytime: SystemTime,
    /// Used to track feedbacks from different shards. Always zero for unsharded tenants.
    pub shard_number: u32,
}

impl PageserverFeedback {
    pub fn empty() -> PageserverFeedback {
        PageserverFeedback {
            current_timeline_size: 0,
            last_received_lsn: Lsn::INVALID,
            remote_consistent_lsn: Lsn::INVALID,
            disk_consistent_lsn: Lsn::INVALID,
            replytime: *PG_EPOCH,
            shard_number: 0,
        }
    }

    // Serialize PageserverFeedback using custom format
    // to support protocol extensibility.
    //
    // Following layout is used:
    // char - number of key-value pairs that follow.
    //
    // key-value pairs:
    // null-terminated string - key,
    // uint32 - value length in bytes
    // value itself
    //
    // TODO: change serialized fields names once all computes migrate to rename.
    pub fn serialize(&self, buf: &mut BytesMut) {
        let buf_ptr = buf.len();
        buf.put_u8(0); // # of keys, will be filled later
        let mut nkeys = 0;

        nkeys += 1;
        buf.put_slice(b"current_timeline_size\0");
        buf.put_i32(8);
        buf.put_u64(self.current_timeline_size);

        nkeys += 1;
        buf.put_slice(b"ps_writelsn\0");
        buf.put_i32(8);
        buf.put_u64(self.last_received_lsn.0);

        nkeys += 1;
        buf.put_slice(b"ps_flushlsn\0");
        buf.put_i32(8);
        buf.put_u64(self.disk_consistent_lsn.0);

        nkeys += 1;
        buf.put_slice(b"ps_applylsn\0");
        buf.put_i32(8);
        buf.put_u64(self.remote_consistent_lsn.0);

        let timestamp = self
            .replytime
            .duration_since(*PG_EPOCH)
            .expect("failed to serialize pg_replytime earlier than PG_EPOCH")
            .as_micros() as i64;

        nkeys += 1;
        buf.put_slice(b"ps_replytime\0");
        buf.put_i32(8);
        buf.put_i64(timestamp);

        if self.shard_number > 0 {
            nkeys += 1;
            buf.put_slice(b"shard_number\0");
            buf.put_i32(4);
            buf.put_u32(self.shard_number);
        }

        buf[buf_ptr] = nkeys;
    }

    // Deserialize PageserverFeedback message
    // TODO: change serialized fields names once all computes migrate to rename.
    pub fn parse(mut buf: Bytes) -> PageserverFeedback {
        let mut rf = PageserverFeedback::empty();
        let nfields = buf.get_u8();
        for _ in 0..nfields {
            let key = read_cstr(&mut buf).unwrap();
            match key.as_ref() {
                b"current_timeline_size" => {
                    let len = buf.get_i32();
                    assert_eq!(len, 8);
                    rf.current_timeline_size = buf.get_u64();
                }
                b"ps_writelsn" => {
                    let len = buf.get_i32();
                    assert_eq!(len, 8);
                    rf.last_received_lsn = Lsn(buf.get_u64());
                }
                b"ps_flushlsn" => {
                    let len = buf.get_i32();
                    assert_eq!(len, 8);
                    rf.disk_consistent_lsn = Lsn(buf.get_u64());
                }
                b"ps_applylsn" => {
                    let len = buf.get_i32();
                    assert_eq!(len, 8);
                    rf.remote_consistent_lsn = Lsn(buf.get_u64());
                }
                b"ps_replytime" => {
                    let len = buf.get_i32();
                    assert_eq!(len, 8);
                    let raw_time = buf.get_i64();
                    if raw_time > 0 {
                        rf.replytime = *PG_EPOCH + Duration::from_micros(raw_time as u64);
                    } else {
                        rf.replytime = *PG_EPOCH - Duration::from_micros(-raw_time as u64);
                    }
                }
                b"shard_number" => {
                    let len = buf.get_i32();
                    assert_eq!(len, 4);
                    rf.shard_number = buf.get_u32();
                }
                _ => {
                    let len = buf.get_i32();
                    warn!(
                        "PageserverFeedback parse. unknown key {} of len {len}. Skip it.",
                        String::from_utf8_lossy(key.as_ref())
                    );
                    buf.advance(len as usize);
                }
            }
        }
        trace!("PageserverFeedback parsed is {:?}", rf);
        rf
    }
}

mod serde_systemtime {
    use std::time::SystemTime;

    use chrono::{DateTime, Utc};
    use serde::{Deserialize, Deserializer, Serializer};

    pub fn serialize<S>(ts: &SystemTime, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let chrono_dt: DateTime<Utc> = (*ts).into();
        serializer.serialize_str(&chrono_dt.to_rfc3339())
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<SystemTime, D::Error>
    where
        D: Deserializer<'de>,
    {
        let time: String = Deserialize::deserialize(deserializer)?;
        Ok(DateTime::parse_from_rfc3339(&time)
            .map_err(serde::de::Error::custom)?
            .into())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_replication_feedback_serialization() {
        let mut rf = PageserverFeedback::empty();
        // Fill rf with some values
        rf.current_timeline_size = 12345678;
        // Set rounded time to be able to compare it with deserialized value,
        // because it is rounded up to microseconds during serialization.
        rf.replytime = *PG_EPOCH + Duration::from_secs(100_000_000);
        let mut data = BytesMut::new();
        rf.serialize(&mut data);

        let rf_parsed = PageserverFeedback::parse(data.freeze());
        assert_eq!(rf, rf_parsed);
    }

    #[test]
    fn test_replication_feedback_unknown_key() {
        let mut rf = PageserverFeedback::empty();
        // Fill rf with some values
        rf.current_timeline_size = 12345678;
        // Set rounded time to be able to compare it with deserialized value,
        // because it is rounded up to microseconds during serialization.
        rf.replytime = *PG_EPOCH + Duration::from_secs(100_000_000);
        let mut data = BytesMut::new();
        rf.serialize(&mut data);

        // Add an extra field to the buffer and adjust number of keys
        data[0] += 1;
        data.put_slice(b"new_field_one\0");
        data.put_i32(8);
        data.put_u64(42);

        // Parse serialized data and check that new field is not parsed
        let rf_parsed = PageserverFeedback::parse(data.freeze());
        assert_eq!(rf, rf_parsed);
    }
}
