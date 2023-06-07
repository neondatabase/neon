//! Tests for postgres_ffi xlog_utils module. Put it here to break cyclic dependency.

use super::*;
use crate::{error, info};
use regex::Regex;
use std::cmp::min;
use std::fs::{self, File};
use std::io::Write;
use std::{env, str::FromStr};
use utils::const_assert;
use utils::lsn::Lsn;

fn init_logging() {
    let _ = env_logger::Builder::from_env(env_logger::Env::default().default_filter_or(
        format!("crate=info,postgres_ffi::{PG_MAJORVERSION}::xlog_utils=trace"),
    ))
    .is_test(true)
    .try_init();
}

fn test_end_of_wal<C: crate::Crafter>(test_name: &str) {
    use crate::*;

    let pg_version = PG_MAJORVERSION[1..3].parse::<u32>().unwrap();

    // Craft some WAL
    let top_path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("..")
        .join("..")
        .join("..");
    let cfg = Conf {
        pg_version,
        pg_distrib_dir: top_path.join("pg_install"),
        datadir: top_path.join(format!("test_output/{}-{PG_MAJORVERSION}", test_name)),
    };
    if cfg.datadir.exists() {
        fs::remove_dir_all(&cfg.datadir).unwrap();
    }
    cfg.initdb().unwrap();
    let srv = cfg.start_server().unwrap();
    let (intermediate_lsns, expected_end_of_wal_partial) =
        C::craft(&mut srv.connect_with_timeout().unwrap()).unwrap();
    let intermediate_lsns: Vec<Lsn> = intermediate_lsns
        .iter()
        .map(|&lsn| u64::from(lsn).into())
        .collect();
    let expected_end_of_wal: Lsn = u64::from(expected_end_of_wal_partial).into();
    srv.kill();

    // Check find_end_of_wal on the initial WAL
    let last_segment = cfg
        .wal_dir()
        .read_dir()
        .unwrap()
        .map(|f| f.unwrap().file_name().into_string().unwrap())
        .filter(|fname| IsXLogFileName(fname))
        .max()
        .unwrap();
    check_pg_waldump_end_of_wal(&cfg, &last_segment, expected_end_of_wal);
    for start_lsn in intermediate_lsns
        .iter()
        .chain(std::iter::once(&expected_end_of_wal))
    {
        // Erase all WAL before `start_lsn` to ensure it's not used by `find_end_of_wal`.
        // We assume that `start_lsn` is non-decreasing.
        info!(
            "Checking with start_lsn={}, erasing WAL before it",
            start_lsn
        );
        for file in fs::read_dir(cfg.wal_dir()).unwrap().flatten() {
            let fname = file.file_name().into_string().unwrap();
            if !IsXLogFileName(&fname) {
                continue;
            }
            let (segno, _) = XLogFromFileName(&fname, WAL_SEGMENT_SIZE);
            let seg_start_lsn = XLogSegNoOffsetToRecPtr(segno, 0, WAL_SEGMENT_SIZE);
            if seg_start_lsn > u64::from(*start_lsn) {
                continue;
            }
            let mut f = File::options().write(true).open(file.path()).unwrap();
            const ZEROS: [u8; WAL_SEGMENT_SIZE] = [0u8; WAL_SEGMENT_SIZE];
            f.write_all(
                &ZEROS[0..min(
                    WAL_SEGMENT_SIZE,
                    (u64::from(*start_lsn) - seg_start_lsn) as usize,
                )],
            )
            .unwrap();
        }
        check_end_of_wal(&cfg, &last_segment, *start_lsn, expected_end_of_wal);
    }
}

fn check_pg_waldump_end_of_wal(
    cfg: &crate::Conf,
    last_segment: &str,
    expected_end_of_wal: Lsn,
) {
    // Get the actual end of WAL by pg_waldump
    let waldump_output = cfg
        .pg_waldump("000000010000000000000001", last_segment)
        .unwrap()
        .stderr;
    let waldump_output = std::str::from_utf8(&waldump_output).unwrap();
    let caps = match Regex::new(r"invalid record length at (.+):")
        .unwrap()
        .captures(waldump_output)
    {
        Some(caps) => caps,
        None => {
            error!("Unable to parse pg_waldump's stderr:\n{}", waldump_output);
            panic!();
        }
    };
    let waldump_wal_end = Lsn::from_str(caps.get(1).unwrap().as_str()).unwrap();
    info!(
        "waldump erred on {}, expected wal end at {}",
        waldump_wal_end, expected_end_of_wal
    );
    assert_eq!(waldump_wal_end, expected_end_of_wal);
}

fn check_end_of_wal(
    cfg: &crate::Conf,
    last_segment: &str,
    start_lsn: Lsn,
    expected_end_of_wal: Lsn,
) {
    // Check end_of_wal on non-partial WAL segment (we treat it as fully populated)
    // let wal_end = find_end_of_wal(&cfg.wal_dir(), WAL_SEGMENT_SIZE, start_lsn).unwrap();
    // info!(
    //     "find_end_of_wal returned wal_end={} with non-partial WAL segment",
    //     wal_end
    // );
    // assert_eq!(wal_end, expected_end_of_wal_non_partial);

    // Rename file to partial to actually find last valid lsn, then rename it back.
    fs::rename(
        cfg.wal_dir().join(last_segment),
        cfg.wal_dir().join(format!("{}.partial", last_segment)),
    )
    .unwrap();
    let wal_end = find_end_of_wal(&cfg.wal_dir(), WAL_SEGMENT_SIZE, start_lsn).unwrap();
    info!(
        "find_end_of_wal returned wal_end={} with partial WAL segment",
        wal_end
    );
    assert_eq!(wal_end, expected_end_of_wal);
    fs::rename(
        cfg.wal_dir().join(format!("{}.partial", last_segment)),
        cfg.wal_dir().join(last_segment),
    )
    .unwrap();
}

const_assert!(WAL_SEGMENT_SIZE == 16 * 1024 * 1024);

#[test]
pub fn test_find_end_of_wal_simple() {
    init_logging();
    test_end_of_wal::<crate::Simple>("test_find_end_of_wal_simple");
}

#[test]
pub fn test_find_end_of_wal_crossing_segment_followed_by_small_one() {
    init_logging();
    test_end_of_wal::<crate::WalRecordCrossingSegmentFollowedBySmallOne>(
        "test_find_end_of_wal_crossing_segment_followed_by_small_one",
    );
}

#[test]
pub fn test_find_end_of_wal_last_crossing_segment() {
    init_logging();
    test_end_of_wal::<crate::LastWalRecordCrossingSegment>(
        "test_find_end_of_wal_last_crossing_segment",
    );
}

/// Check the math in update_next_xid
///
/// NOTE: These checks are sensitive to the value of XID_CHECKPOINT_INTERVAL,
/// currently 1024.
#[test]
pub fn test_update_next_xid() {
    let checkpoint_buf = [0u8; std::mem::size_of::<CheckPoint>()];
    let mut checkpoint = CheckPoint::decode(&checkpoint_buf).unwrap();

    checkpoint.nextXid = FullTransactionId { value: 10 };
    assert_eq!(checkpoint.nextXid.value, 10);

    // The input XID gets rounded up to the next XID_CHECKPOINT_INTERVAL
    // boundary
    checkpoint.update_next_xid(100);
    assert_eq!(checkpoint.nextXid.value, 1024);

    // No change
    checkpoint.update_next_xid(500);
    assert_eq!(checkpoint.nextXid.value, 1024);
    checkpoint.update_next_xid(1023);
    assert_eq!(checkpoint.nextXid.value, 1024);

    // The function returns the *next* XID, given the highest XID seen so
    // far. So when we pass 1024, the nextXid gets bumped up to the next
    // XID_CHECKPOINT_INTERVAL boundary.
    checkpoint.update_next_xid(1024);
    assert_eq!(checkpoint.nextXid.value, 2048);
}

#[test]
pub fn test_encode_logical_message() {
    let expected = [
        64, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 21, 0, 0, 170, 34, 166, 227, 255,
        38, 0, 0, 0, 0, 0, 0, 0, 0, 7, 0, 0, 0, 0, 0, 0, 0, 7, 0, 0, 0, 0, 0, 0, 0, 112, 114,
        101, 102, 105, 120, 0, 109, 101, 115, 115, 97, 103, 101,
    ];
    let actual = encode_logical_message("prefix", "message");
    assert_eq!(expected, actual[..]);
}
