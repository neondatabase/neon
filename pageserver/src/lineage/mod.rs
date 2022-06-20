use std::mem::size_of;

use anyhow::Result;
use bytes::Bytes;
use serde::{Deserialize, Serialize};

use utils::lsn::Lsn;

use crate::lineage::reader::LineageReader;
use crate::lineage::record_iops::{LineageAnyValue, PrefixedByPageImage, RecordType};
use crate::lineage::spec::{
    LineageRecordHandler, SerializedCLogAbortsHandler, SerializedCLogBothHandler,
    SerializedCLogCommitsHandler, SerializedMultixactMembersHandler,
    SerializedMultixactOffsetHandler, SerializedRecordHandler,
};
use crate::repository::Value;
use crate::walrecord::{ClearVisibilityMapFlags, Postgres, ZenithWalRecord};

mod clog_specialize;
mod reader;
mod record_iops;
mod spec;
mod writer;

/// Don't use unstable function, but use the feature.
/// See https://github.com/rust-lang/rust/issues/73014 as to why
/// std::default::default is unstable; and why that is OK.
fn default<T: Default>() -> T {
    <T as Default>::default()
}

/// Kinds of lineages we distinguish.
///
/// These mostly are one for each type of ZenithWalRecord, but also includes
/// an ANY type (for all record types) and one that can contain any of the CLOG
/// record variants (COMMIT and ABORT).
#[derive(Copy, Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
enum LineageType {
    /// Non-specialized variants.
    AnyValueType = 1,

    /// Array of ZenithWalRecord::Postgres
    PGWal = 2,
    /// Same as above, but prefixed by singular Value::Image.
    PGWalWithPageImage = 3,

    // Set of specialized records based on `ZenithWalRecord::Variant`-s
    // Saves (up to) 4 bytes /record uncompressed.

    // VizMap
    /// Only Visibilitymap flags being set.
    /// See ZenithWalRecord::ClearVisibilityMapFlags
    VisMapFlags = 4,
    VisMapFlagsWithPageImage = 5,

    // CLOG
    /// Only Commits
    CLogCommits = 6,
    CLogCommitsWithPageImage = 7,
    /// Only Aborts
    CLogAborts = 8,
    CLogAbortsWithPageImage = 9,
    /// Mix of Aborts and Commits
    CLogAny = 10,
    CLogAnyWithPageImage = 11,

    /// Multi-xact
    MultixactOffsetCreates = 12,
    MultixactOffsetCreatesWithPageImage = 13,

    MultixactMemberCreates = 14,
    MultixactMemberCreatesWithPageImage = 15,
}

/// Have a default
impl Default for LineageType {
    fn default() -> Self {
        LineageType::AnyValueType
    }
}

/// Macro that defines a function on &self that forwards the calls to the
/// corresponding handler, based on the type.
macro_rules! forward_to_impl {
    ($name:ident ($($params:ident : $typ:ty),* ) -> $ret:ty) => {
        pub fn $name(&self, $($params : $typ),* ) -> $ret {
            match self {
                LineageType::AnyValueType => SerializedRecordHandler::<LineageAnyValue>::default()
                    .$name($($params),*),
                LineageType::PGWal => SerializedRecordHandler::<RecordType<Postgres>>::default()
                    .$name($($params),*),
                LineageType::PGWalWithPageImage => SerializedRecordHandler::<PrefixedByPageImage<RecordType<Postgres>>>::default()
                    .$name($($params),*),
                LineageType::VisMapFlags => SerializedRecordHandler::<RecordType<ClearVisibilityMapFlags>>::default()
                    .$name($($params),*),
                LineageType::VisMapFlagsWithPageImage => SerializedRecordHandler::<PrefixedByPageImage<RecordType<ClearVisibilityMapFlags>>>::default()
                    .$name($($params),*),
                LineageType::CLogCommits => SerializedCLogCommitsHandler()
                    .$name($($params),*),
                LineageType::CLogCommitsWithPageImage => PrefixedByPageImage(SerializedCLogCommitsHandler())
                    .$name($($params),*),
                LineageType::CLogAborts => SerializedCLogAbortsHandler()
                    .$name($($params),*),
                LineageType::CLogAbortsWithPageImage => PrefixedByPageImage(SerializedCLogAbortsHandler())
                    .$name($($params),*),
                LineageType::CLogAny => SerializedCLogBothHandler()
                    .$name($($params),*),
                LineageType::CLogAnyWithPageImage => PrefixedByPageImage(SerializedCLogBothHandler())
                    .$name($($params),*),
                LineageType::MultixactOffsetCreates => SerializedMultixactOffsetHandler()
                    .$name($($params),*),
                LineageType::MultixactOffsetCreatesWithPageImage => PrefixedByPageImage(SerializedMultixactOffsetHandler())
                    .$name($($params),*),
                LineageType::MultixactMemberCreates => SerializedMultixactMembersHandler()
                    .$name($($params),*),
                LineageType::MultixactMemberCreatesWithPageImage => PrefixedByPageImage(SerializedMultixactMembersHandler())
                    .$name($($params),*),
            }
        }
    }
}

impl LineageType {
    forward_to_impl!(get_reader (bytes: Bytes, limit: Lsn) -> Vec<(Lsn, Value)>);
    forward_to_impl!(write (vec: &[(Lsn, Value)]) -> Result<Bytes>);

    /// Determines what type can best handle this Value type.
    /// Note that for Image this is only stored in AnyValueType, and that the
    /// ...WithPrefixImage cannot be returned here: images are only prefixed at
    /// the final stage when the rest of the types are already determined.
    pub fn type_for(value: &Value) -> LineageType {
        match value {
            Value::Image(_) => LineageType::AnyValueType,
            Value::WalRecord(it) => match it {
                ZenithWalRecord::Postgres(_) => LineageType::PGWal,
                ZenithWalRecord::ClearVisibilityMapFlags(_) => LineageType::VisMapFlags,
                ZenithWalRecord::ClogSetCommitted(_) => LineageType::CLogCommits,
                ZenithWalRecord::ClogSetAborted(_) => LineageType::CLogAborts,
                ZenithWalRecord::MultixactOffsetCreate(_) => LineageType::MultixactOffsetCreates,
                ZenithWalRecord::MultixactMembersCreate(_) => LineageType::MultixactMemberCreates,
            },
            Value::NonInitiatingLineage(_) => default(),
            Value::InitiatingLineage(_) => default(),
        }
    }

    /// Combine the lineage type with another. Unordered combine operations:
    /// A + B == B + A
    ///
    /// Note that we cannot combine ...WithPrefixImage, as that has ordering and could result in pageImage in the middle -- which is incorrect.
    fn combine_with(&self, other_type: &LineageType) -> LineageType {
        match &self {
            LineageType::AnyValueType => *self,
            LineageType::PGWal => match other_type {
                LineageType::PGWal => LineageType::PGWal,
                _ => default(),
            },
            LineageType::PGWalWithPageImage => default(),
            LineageType::VisMapFlags => match other_type {
                LineageType::VisMapFlags => LineageType::VisMapFlags,
                _ => default(),
            },
            LineageType::VisMapFlagsWithPageImage => default(),
            LineageType::CLogCommits => match other_type {
                LineageType::CLogCommits => LineageType::CLogCommits,
                LineageType::CLogAborts => LineageType::CLogAny,
                LineageType::CLogAny => LineageType::CLogAny,
                _ => default(),
            },
            LineageType::CLogCommitsWithPageImage => default(),
            LineageType::CLogAborts => match other_type {
                LineageType::CLogCommits => LineageType::CLogAny,
                LineageType::CLogAborts => LineageType::CLogAborts,
                LineageType::CLogAny => LineageType::CLogAny,
                _ => default(),
            },
            LineageType::CLogAbortsWithPageImage => default(),
            LineageType::CLogAny => match other_type {
                LineageType::CLogCommits => LineageType::CLogAny,
                LineageType::CLogAborts => LineageType::CLogAny,
                LineageType::CLogAny => LineageType::CLogAny,
                _ => default(),
            },
            LineageType::CLogAnyWithPageImage => default(),
            LineageType::MultixactOffsetCreates => match other_type {
                LineageType::MultixactOffsetCreates => LineageType::MultixactOffsetCreates,
                _ => default(),
            },
            LineageType::MultixactOffsetCreatesWithPageImage => default(),
            LineageType::MultixactMemberCreates => match other_type {
                LineageType::MultixactMemberCreates => LineageType::MultixactMemberCreates,
                _ => default(),
            },
            LineageType::MultixactMemberCreatesWithPageImage => default(),
        }
    }

    fn add_preceding_image(&self) -> LineageType {
        match self {
            LineageType::PGWal => LineageType::PGWalWithPageImage,
            LineageType::VisMapFlags => LineageType::VisMapFlagsWithPageImage,
            LineageType::CLogCommits => LineageType::CLogCommitsWithPageImage,
            LineageType::CLogAborts => LineageType::CLogAbortsWithPageImage,
            LineageType::CLogAny => LineageType::CLogAnyWithPageImage,
            LineageType::MultixactOffsetCreates => LineageType::MultixactOffsetCreatesWithPageImage,
            LineageType::MultixactMemberCreates => LineageType::MultixactMemberCreatesWithPageImage,
            // Anything that already has an image would have 2 prefixing images, which should not
            // happen and thus requires AnyValueType -- we can't efficiently handle it otherwise.
            LineageType::AnyValueType => LineageType::AnyValueType,
            LineageType::PGWalWithPageImage => LineageType::AnyValueType,
            LineageType::VisMapFlagsWithPageImage => LineageType::AnyValueType,
            LineageType::CLogCommitsWithPageImage => LineageType::AnyValueType,
            LineageType::CLogAbortsWithPageImage => LineageType::AnyValueType,
            LineageType::CLogAnyWithPageImage => LineageType::AnyValueType,
            LineageType::MultixactOffsetCreatesWithPageImage => LineageType::AnyValueType,
            LineageType::MultixactMemberCreatesWithPageImage => LineageType::AnyValueType,
        }
    }
}

/// Storage structure, used in Value::[NonInitiatingLineage, InitiatingLineage]
/// Contains the serialized information of a Vec<(Lsn, Value)>.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct Lineage {
    kind: LineageType,
    data: Bytes,
}

impl Lineage {
    pub fn guestimate_size(&self) -> usize {
        self.data.len() + size_of::<u64>()
    }

    /// Get the contained values; up to and including the given LSN.
    /// Note that the results are in ascending order of Lsn: Smallest Lsn First.
    pub fn recs_up_to(&self, limit: Lsn) -> Vec<(Lsn, Value)> {
        self.kind.get_reader(self.data.clone(), limit)
    }
}

/// helper for debug information.
pub fn describe_lineage(lineage: &Lineage) -> String {
    format!(
        "kind: {:?} n_records: {}",
        lineage.kind,
        LineageReader::<()>::num_records(&lineage.data)
    )
}

/// Helper struct for writing Lineages.
///
/// Contains the current cached state of (Lsn, Value)-pairs, and an optionally
/// determined type of the Lineage up to the last received record, plus a
/// guesstimated size cache.
pub struct LineageWriteHandler {
    state: Vec<(Lsn, Value)>,
    determined_type: Option<LineageType>,
    guestimated_size: u64,
}

impl Default for LineageWriteHandler {
    fn default() -> Self {
        Self::new()
    }
}

impl LineageWriteHandler {
    pub fn new() -> Self {
        Self {
            state: Vec::with_capacity(128),
            determined_type: Default::default(),
            guestimated_size: 0,
        }
    }

    /// Append a Value record to the lineage
    /// First records always return None.
    /// will_init records added to a non-empty handler will flush the list
    /// Before starting with a new key, run `LineageWriteHandler::finish(&self)`
    /// to get the yet-to-be-processed lineage.
    pub fn append_record(&mut self, lsn: Lsn, val: Value) -> Result<Option<(Lsn, Value)>> {
        if self.state.is_empty() {
            // prefix image is only added to the type at the end.
            if val.is_image() {
                self.determined_type = None
            } else {
                self.determined_type = Some(LineageType::type_for(&val));
            }
            // add sizing estimate.
            self.guestimated_size += (size_of::<Lsn>() + val.guestimate_size()) as u64;
            self.state.push((lsn, val));

            return Ok(None);
        }

        // flush when initiating record is pushed onto other records
        if val.will_init() {
            let result = self.finish()?;

            debug_assert!(result.is_some());
            debug_assert!(self.state.is_empty());
            debug_assert!(self.determined_type.is_none());
            debug_assert_eq!(self.guestimated_size, 0);

            if val.is_image() {
                self.determined_type = None;
            } else {
                self.determined_type = Some(LineageType::type_for(&val));
            }

            self.guestimated_size += (size_of::<Lsn>() + val.guestimate_size()) as u64;
            self.state.push((lsn, val));

            return Ok(result);
        }

        let add_typ = LineageType::type_for(&val);
        self.guestimated_size += (size_of::<Lsn>() + val.guestimate_size()) as u64;
        self.state.push((lsn, val));

        debug_assert!(self.state[self.state.len() - 2].0 < self.state[self.state.len() - 1].0);

        if let Some(typ) = &self.determined_type {
            self.determined_type = Some(typ.combine_with(&add_typ));
        } else {
            self.determined_type = Some(add_typ);
        }

        Ok(None)
    }

    pub fn guesstimate_size(&self) -> u64 {
        self.guestimated_size
    }

    pub fn finish(&mut self) -> Result<Option<(Lsn, Value)>> {
        if self.state.is_empty() {
            debug_assert!(self.determined_type == None);
            return Ok(None);
        }

        if self.state.len() == 1 {
            // empty self.state
            let res = self.state.pop().unwrap();

            debug_assert!(self.state.is_empty());

            // reset determined type
            self.determined_type = None;
            self.guestimated_size = 0;
            return Ok(Some(res));
        }

        let will_init = self.state[0].1.will_init();
        let lineage_type = if self.state[0].1.is_image() {
            self.determined_type
                .unwrap_or(LineageType::AnyValueType)
                .add_preceding_image()
        } else {
            self.determined_type.unwrap_or(LineageType::AnyValueType)
        };

        let lineage_lsn = self.state[0].0;
        let lineage_bytes = lineage_type.write(&self.state);

        let lineage = Lineage {
            kind: lineage_type,
            data: lineage_bytes?,
        };

        if cfg!(debug_assertions) {
            let deser = lineage.recs_up_to(Lsn(u64::MAX));
            debug_assert_eq!(&deser, &self.state);
            debug_assert_eq!(deser.first().unwrap().0, lineage_lsn);
        }

        let value = if will_init {
            Value::InitiatingLineage(lineage)
        } else {
            Value::NonInitiatingLineage(lineage)
        };

        self.state.clear();
        self.guestimated_size = 0;
        self.determined_type = None;

        Ok(Some((lineage_lsn, value)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::walrecord::{
        ClogSetAborted, ClogSetCommitted, MultiXactMember, MultixactMembersCreate,
        MultixactOffsetCreate,
    };
    use postgres_ffi::pg_constants::VISIBILITYMAP_VALID_BITS;
    use postgres_ffi::xlog_utils::TimestampTz;
    use postgres_ffi::{MultiXactId, MultiXactOffset, MultiXactStatus, TransactionId};

    #[derive(Default)]
    struct WalGenerator {
        current_lsn: u64,
    }

    impl WalGenerator {
        fn img(&mut self) -> (Lsn, Value) {
            self.current_lsn += 8;

            (Lsn(self.current_lsn), Value::Image(BS_RECORD_DATA.clone()))
        }

        fn pg(&mut self, init: bool) -> (Lsn, Value) {
            self.current_lsn += 8;
            (
                Lsn(self.current_lsn),
                Value::WalRecord(ZenithWalRecord::Postgres(Postgres {
                    will_init: init,
                    rec: BS_RECORD_DATA.clone(),
                })),
            )
        }

        fn vismap(&mut self, old_blockno: Option<u32>, new_blockno: Option<u32>) -> (Lsn, Value) {
            self.current_lsn += 8;
            (
                Lsn(self.current_lsn),
                Value::WalRecord(ZenithWalRecord::ClearVisibilityMapFlags(
                    ClearVisibilityMapFlags {
                        new_heap_blkno: old_blockno,
                        old_heap_blkno: new_blockno,
                        flags: VISIBILITYMAP_VALID_BITS,
                    },
                )),
            )
        }

        fn commit(&mut self, xids: Vec<u32>, timestamp: u64) -> (Lsn, Value) {
            self.current_lsn += 8;
            (
                Lsn(self.current_lsn),
                Value::WalRecord(ZenithWalRecord::ClogSetCommitted(ClogSetCommitted {
                    xids,
                    timestamp: timestamp as TimestampTz,
                })),
            )
        }

        fn abort(&mut self, xids: Vec<u32>) -> (Lsn, Value) {
            self.current_lsn += 8;
            (
                Lsn(self.current_lsn),
                Value::WalRecord(ZenithWalRecord::ClogSetAborted(ClogSetAborted { xids })),
            )
        }

        fn mxactoff(&mut self, mid: MultiXactId, moff: MultiXactOffset) -> (Lsn, Value) {
            self.current_lsn += 8;
            (
                Lsn(self.current_lsn),
                Value::WalRecord(ZenithWalRecord::MultixactOffsetCreate(
                    MultixactOffsetCreate { mid, moff },
                )),
            )
        }

        fn mxactmembers(
            &mut self,
            moff: MultiXactOffset,
            members: Vec<MultiXactMember>,
        ) -> (Lsn, Value) {
            self.current_lsn += 8;
            (
                Lsn(self.current_lsn),
                Value::WalRecord(ZenithWalRecord::MultixactMembersCreate(
                    MultixactMembersCreate { moff, members },
                )),
            )
        }

        fn mxactmem(&self, xid: TransactionId, status: u32) -> MultiXactMember {
            MultiXactMember {
                xid,
                status: status as MultiXactStatus,
            }
        }
    }

    static BS_RECORD_DATA: Bytes = Bytes::from_static(b"1234567890abcdef1234567890abcdef");

    fn changes_stream_matches(vec: Vec<(Lsn, Value)>) -> Result<()> {
        let mut handler = LineageWriteHandler::new();

        let mut serialized_records = vec![];
        let mut deserialized_records = Vec::with_capacity(vec.len());

        for it in vec.iter() {
            let written = handler.append_record(it.0, it.1.clone())?;
            if let Some((lsn, rec)) = written {
                serialized_records.push((lsn, rec));
            }
        }

        if let Some((lsn, rec)) = handler.finish()? {
            serialized_records.push((lsn, rec));
        }

        for (lsn, value) in serialized_records.into_iter() {
            match value {
                Value::Image(_) => deserialized_records.push((lsn, value)),
                Value::WalRecord(_) => deserialized_records.push((lsn, value)),
                Value::NonInitiatingLineage(lin) => {
                    deserialized_records.extend_from_slice(&lin.recs_up_to(Lsn(u64::MAX))[..])
                }
                Value::InitiatingLineage(lin) => {
                    deserialized_records.extend_from_slice(&lin.recs_up_to(Lsn(u64::MAX))[..])
                }
            }
        }

        assert_eq!(vec, deserialized_records);

        Ok(())
    }

    #[test]
    fn test_stream_of_image() {
        let mut generator = WalGenerator::default();

        let records = vec![
            generator.img(),
            generator.img(),
            generator.img(),
            generator.img(),
        ];

        changes_stream_matches(records)
            .expect("Failure in (de)serialization of records is an error");
    }

    #[test]
    fn test_stream_of_pg_wal() {
        let mut generator = WalGenerator::default();

        let records: Vec<(Lsn, Value)> = vec![
            generator.pg(false),
            generator.pg(false),
            generator.pg(false),
            generator.pg(false),
            generator.pg(false),
        ];

        changes_stream_matches(records)
            .expect("Failure in (de)serialization of records is an error");
    }

    #[test]
    fn test_stream_of_pg_wal_with_prefix_image() -> Result<()> {
        let mut generator = WalGenerator::default();

        let records: Vec<(Lsn, Value)> = vec![
            generator.img(),
            generator.pg(false),
            generator.pg(false),
            generator.pg(false),
            generator.pg(true),
            generator.pg(false),
            generator.pg(false),
            generator.pg(false),
        ];

        changes_stream_matches(records).map(|_| ())
    }

    #[test]
    fn test_stream_of_commit_recs() {
        let mut generator = WalGenerator::default();

        let records = vec![
            generator.commit(vec![1], 1),
            generator.commit(vec![2], 2),
            generator.commit(vec![3], 3),
            generator.commit(vec![4], 4),
            generator.commit(vec![5], 5),
        ];

        changes_stream_matches(records)
            .expect("Failure in (de)serialization of records is an error");
    }

    #[test]
    fn test_stream_of_commit_recs_with_quirks() {
        let mut generator = WalGenerator::default();

        let records = vec![
            generator.commit(vec![1, 9, 23], 100),
            generator.commit(vec![3], 0),
            generator.commit(vec![999], 2),
            generator.commit(vec![123, 323, 2356], 30),
            generator.commit(vec![u32::MAX - 1, u32::MAX], 44),
        ];

        changes_stream_matches(records)
            .expect("Failure in (de)serialization of records is an error");
    }

    #[test]
    fn test_stream_of_aborts() {
        let mut generator = WalGenerator::default();

        let records = vec![
            generator.abort(vec![1]),
            generator.abort(vec![2]),
            generator.abort(vec![3]),
            generator.abort(vec![4]),
            generator.abort(vec![5]),
        ];

        changes_stream_matches(records)
            .expect("Failure in (de)serialization of records is an error");
    }
    #[test]
    fn test_mixed_stream() {
        let mut generator = WalGenerator::default();

        let records = vec![
            generator.img(),
            generator.commit(vec![1], 1),
            generator.abort(vec![2]),
            generator.abort(vec![3]),
            generator.abort(vec![4]),
            generator.abort(vec![5]),
            generator.img(),
            generator.commit(vec![6], 2),
            generator.commit(vec![8], 3),
            generator.commit(vec![7], 4),
            generator.commit(vec![9], 5),
            generator.commit(vec![10], 6),
        ];

        changes_stream_matches(records)
            .expect("Failure in (de)serialization of records is an error");
    }

    #[test]
    fn test_mixed_clog_stream() {
        let mut generator = WalGenerator::default();

        let records = vec![
            generator.abort(vec![1]),
            generator.abort(vec![2]),
            generator.commit(vec![999], 2),
            generator.commit(vec![123, 323, 2356], 30),
            generator.abort(vec![5]),
        ];

        changes_stream_matches(records)
            .expect("Failure in (de)serialization of records is an error");
    }

    #[test]
    fn test_visflags_stream() {
        let mut generator = WalGenerator::default();

        let records = vec![
            generator.vismap(Some(1), None),
            generator.vismap(Some(1), Some(5)),
            generator.vismap(None, Some(2)),
            generator.vismap(Some(2), None),
            generator.vismap(Some(99), Some(3)),
        ];

        changes_stream_matches(records)
            .expect("Failure in (de)serialization of records is an error");
    }

    #[test]
    fn test_mxactoffsets_stream() {
        let mut generator = WalGenerator::default();

        let records = vec![
            generator.mxactoff(12, 0x050),
            generator.mxactoff(13, 0x100),
            generator.mxactoff(14, 0x200),
            generator.mxactoff(15, 0x400),
            generator.mxactoff(16, 0x800),
        ];

        changes_stream_matches(records)
            .expect("Failure in (de)serialization of records is an error");
    }

    #[test]
    fn test_mxactmembers_stream() {
        let mut generator = WalGenerator::default();

        let records = vec![
            generator.mxactmembers(0x050, vec![generator.mxactmem(1, 5)]),
            generator.mxactmembers(
                0x100,
                vec![generator.mxactmem(1, 5), generator.mxactmem(2, 5)],
            ),
            generator.mxactmembers(
                0x200,
                vec![
                    generator.mxactmem(1, 5),
                    generator.mxactmem(2, 5),
                    generator.mxactmem(3, 5),
                ],
            ),
            generator.mxactmembers(0x400, vec![generator.mxactmem(4, 5)]),
            generator.mxactmembers(
                0x800,
                vec![
                    generator.mxactmem(1, 5),
                    generator.mxactmem(2, 5),
                    generator.mxactmem(3, 5),
                    generator.mxactmem(4, 5),
                ],
            ),
            generator.mxactmembers(
                0x050,
                vec![generator.mxactmem(1, 5), generator.mxactmem(2, 2)],
            ),
        ];

        changes_stream_matches(records)
            .expect("Failure in (de)serialization of records is an error");
    }
}
