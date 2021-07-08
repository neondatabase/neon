//!
//! Import data and WAL from a PostgreSQL data directory in s3
//!
use log::*;

use anyhow::Result;
use bytes::{Buf, Bytes, BytesMut};

use crate::repository::{Timeline, BufferTag, RelTag};
use crate::object_key::{DatabaseTag, ObjectTag, PrepareTag, SlruBufferTag};

use postgres_ffi::pg_constants;
use postgres_ffi::relfile_utils::*;
use postgres_ffi::*;
use zenith_utils::lsn::Lsn;
use zenith_utils::s3_utils::S3Storage;

///
/// Import all relation data pages from s3
///
pub fn import_timeline_from_postgres_s3(
    bucket_name: &str,
    timeline: &dyn Timeline,
    lsn: Lsn,
) -> Result<()> {
    let s3_storage = S3Storage::new_from_env(bucket_name)?;

    // List out contents of directory
    let results = s3_storage.list_bucket("global/".to_string())?;

    for result in results {
        for object in result.contents {
            // Download every relation file, slurping them into memory

            let key = object.key;
            let relpath = key.strip_prefix("global/");

            match relpath {
                None => continue,

                // These special files appear in the snapshot, but are not needed by the page server
                Some("pg_internal.init") => {}
                Some("PG_VERSION") => {}
                Some("pg_control") => import_nonrel_file_from_s3(
                    &s3_storage,
                    timeline,
                    lsn,
                    ObjectTag::ControlFile,
                    &key,
                )?,
                Some("pg_filenode.map") => import_nonrel_file_from_s3(
                    &s3_storage,
                    timeline,
                    lsn,
                    ObjectTag::FileNodeMap(DatabaseTag {
                        spcnode: pg_constants::GLOBALTABLESPACE_OID,
                        dbnode: 0,
                    }),
                    &key,
                )?,

                // Load any relation files into the page server
                Some(relpath) => import_relfile_from_s3(
                    &s3_storage,
                    &key,
                    &relpath,
                    timeline,
                    lsn,
                    pg_constants::GLOBALTABLESPACE_OID,
                    0,
                )?,
            }
        }
    }

    // // Scan 'base'. It contains database dirs, the database OID is the filename.
    // // E.g. 'base/12345', where 12345 is the database OID.
    let results = s3_storage.list_bucket("base/".to_string())?;
    for result in results {
        for object in result.contents {
            // Download every relation file, slurping them into memory

            let key = object.key;
            let dbpath = key.strip_prefix("base/").ok_or(FilePathError::InvalidFileName)?;

            let mut s = dbpath.split('/');
            let dboid_str = s.next().ok_or(FilePathError::InvalidFileName)?;
            let dboid = dboid_str.parse::<u32>()?;
            let relpath = s.next();

            match relpath {
                None => continue,

                // These special files appear in the snapshot, but are not needed by the page server
                Some("pg_internal.init") => {}
                Some("PG_VERSION") => continue,
                Some("pg_filenode.map") => import_nonrel_file_from_s3(
                    &s3_storage,
                    timeline,
                    lsn,
                    ObjectTag::FileNodeMap(DatabaseTag {
                        spcnode: pg_constants::DEFAULTTABLESPACE_OID,
                        dbnode: dboid,
                    }),
                    &key,
                )?,

                // Load any relation files into the page server
                Some(relpath) => import_relfile_from_s3(
                    &s3_storage,
                    &key,
                    &relpath,
                    timeline,
                    lsn,
                    pg_constants::DEFAULTTABLESPACE_OID,
                    dboid,
                )?,
            }
        }
    }

    let results = s3_storage.list_bucket("pg_xact/".to_string())?;
    for result in results {
        for object in result.contents {
            let key = object.key;
            let relpath = key.strip_prefix("pg_xact/").unwrap();

            import_slru_file_from_s3(
                &s3_storage,
                timeline,
                lsn,
                |blknum| ObjectTag::Clog(SlruBufferTag { blknum }),
                &key,
                &relpath,
            )?;
        }
    }

    let results = s3_storage
        .list_bucket("pg_multixact/members/".to_string())?;
    for result in results {
        for object in result.contents {
            let key = object.key;
            let relpath = key.strip_prefix("pg_multixact/members/").unwrap();

            import_slru_file_from_s3(
                &s3_storage,
                timeline,
                lsn,
                |blknum| ObjectTag::MultiXactMembers(SlruBufferTag { blknum }),
                &key,
                &relpath,
            )?;
        }
    }

    let results = s3_storage
        .list_bucket("pg_multixact/offsets/".to_string())?;
    for result in results {
        for object in result.contents {
            let key = object.key;
            let relpath = key.strip_prefix("pg_multixact/offsets/").unwrap();

            import_slru_file_from_s3(
                &s3_storage,
                timeline,
                lsn,
                |blknum| ObjectTag::MultiXactOffsets(SlruBufferTag { blknum }),
                &key,
                &relpath,
            )?;
        }
    }

    let results = s3_storage.list_bucket("pg_twophase/".to_string())?;
    for result in results {
        for object in result.contents {
            let key = object.key;
            let relpath = key.strip_prefix("pg_twophase/").unwrap();

            let xid = u32::from_str_radix(&relpath, 16)?;
            import_nonrel_file_from_s3(
                &s3_storage,
                timeline,
                lsn,
                ObjectTag::TwoPhase(PrepareTag { xid }),
                &key,
            )?;
        }
    }
    // TODO: Scan pg_tblspc

    timeline.checkpoint()?;

    Ok(())
}

// subroutine of import_timeline_from_postgres_datadir(), to load one relation file.
fn import_relfile_from_s3(
    s3_storage: &S3Storage,
    path: &str,
    filename: &str,
    timeline: &dyn Timeline,
    lsn: Lsn,
    spcoid: Oid,
    dboid: Oid,
) -> Result<()> {
    let p = parse_relfilename(filename);
    if let Err(e) = p {
        warn!("unrecognized file in snapshot: {:?} ({})", path, e);
        return Err(e.into());
    }
    let (relnode, forknum, segno) = p?;

    //Retrieve file from s3
    let data = s3_storage.get_object(path)?;
    let mut bytes = BytesMut::from(data.as_slice()).freeze();

    let mut blknum: u32 = segno * (1024 * 1024 * 1024 / pg_constants::BLCKSZ as u32);

    while bytes.remaining() >= 8192 {
        let tag = ObjectTag::RelationBuffer(BufferTag {
            rel: RelTag {
                spcnode: spcoid,
                dbnode: dboid,
                relnode,
                forknum,
            },
            blknum,
        });
        timeline.put_page_image(tag, lsn, bytes.copy_to_bytes(8192))?;
        blknum += 1;
    }

    Ok(())
}

fn import_nonrel_file_from_s3(
    s3_storage: &S3Storage,
    timeline: &dyn Timeline,
    lsn: Lsn,
    tag: ObjectTag,
    path: &str,
) -> Result<()> {
    // read the whole file
    let data = s3_storage.get_object(path)?;

    timeline.put_page_image(tag, lsn, Bytes::copy_from_slice(&data[..]))?;
    Ok(())
}

fn import_slru_file_from_s3(
    s3_storage: &S3Storage,
    timeline: &dyn Timeline,
    lsn: Lsn,
    gen_tag: fn(blknum: u32) -> ObjectTag,
    path: &str,
    filename: &str,
) -> Result<()> {
    let data = s3_storage.get_object(path)?;
    let mut bytes = BytesMut::from(data.as_slice()).freeze();

    let segno = u32::from_str_radix(filename, 16)?;
    let mut blknum: u32 = segno * pg_constants::SLRU_PAGES_PER_SEGMENT;

    while bytes.remaining() >= 8192 {
        timeline.put_page_image(gen_tag(blknum), lsn, bytes.copy_to_bytes(8192))?;
        blknum += 1;
    }

    Ok(())
}

// Returns checkpoint LSN from controlfile
pub fn get_lsn_from_controlfile_s3(bucket_name: &str) -> Result<Lsn> {
    let s3_storage = S3Storage::new_from_env(bucket_name)?;
    let path = "global/pg_control";

    let controlfile = ControlFileData::decode(&s3_storage.get_object(path)?)?;
    let lsn = controlfile.checkPoint;

    Ok(Lsn(lsn))
}
