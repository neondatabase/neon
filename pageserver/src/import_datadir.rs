//!
//! Import data and WAL from a PostgreSQL data directory and WAL segments into
//! a zenith Timeline.
//!
use std::fs;
use std::fs::File;
use std::io::{Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};

use anyhow::{bail, ensure, Context, Result};
use bytes::Bytes;
use tracing::*;

use crate::pgdatadir_mapping::*;
use crate::relish::*;
use crate::repository::Repository;
use crate::walingest::WalIngest;
use postgres_ffi::relfile_utils::*;
use postgres_ffi::waldecoder::*;
use postgres_ffi::xlog_utils::*;
use postgres_ffi::{pg_constants, ControlFileData, DBState_DB_SHUTDOWNED};
use postgres_ffi::{Oid, TransactionId};
use zenith_utils::lsn::Lsn;

///
/// Import all relation data pages from local disk into the repository.
///
/// This is currently only used to import a cluster freshly created by initdb.
/// The code that deals with the checkpoint would not work right if the
/// cluster was not shut down cleanly.
pub fn import_timeline_from_postgres_datadir<R: Repository>(
    path: &Path,
    tline: &mut DatadirTimeline<R>,
    lsn: Lsn,
) -> Result<()> {
    let mut pg_control: Option<ControlFileData> = None;

    let mut writer = tline.begin_record(lsn);
    writer.init_empty()?;

    // Scan 'global'
    let mut relfiles: Vec<PathBuf> = Vec::new();
    writer.put_dbdir_creation(pg_constants::GLOBALTABLESPACE_OID, 0)?;
    for direntry in fs::read_dir(path.join("global"))? {
        let direntry = direntry?;
        match direntry.file_name().to_str() {
            None => continue,

            Some("pg_control") => {
                pg_control = Some(import_control_file(&mut writer, &direntry.path())?);
            }
            Some("pg_filenode.map") => {
                import_relmap_file(
                    &mut writer,
                    pg_constants::GLOBALTABLESPACE_OID,
                    0,
                    &direntry.path(),
                )?;
            }

            // Load any relation files into the page server (but only after the other files)
            _ => relfiles.push(direntry.path()),
        }
    }
    for relfile in relfiles {
        import_relfile(&mut writer, &relfile, pg_constants::GLOBALTABLESPACE_OID, 0)?;
    }

    // Scan 'base'. It contains database dirs, the database OID is the filename.
    // E.g. 'base/12345', where 12345 is the database OID.
    for direntry in fs::read_dir(path.join("base"))? {
        let direntry = direntry?;

        //skip all temporary files
        if direntry.file_name().to_str().unwrap() == "pgsql_tmp" {
            continue;
        }

        let dboid = direntry.file_name().to_str().unwrap().parse::<u32>()?;

        let mut relfiles: Vec<PathBuf> = Vec::new();
        for direntry in fs::read_dir(direntry.path())? {
            let direntry = direntry?;
            match direntry.file_name().to_str() {
                None => continue,

                Some("PG_VERSION") => {
                    writer.put_dbdir_creation(pg_constants::DEFAULTTABLESPACE_OID, dboid)?;
                }
                Some("pg_filenode.map") => import_relmap_file(
                    &mut writer,
                    pg_constants::DEFAULTTABLESPACE_OID,
                    dboid,
                    &direntry.path(),
                )?,

                // Load any relation files into the page server
                _ => relfiles.push(direntry.path()),
            }
        }
        for relfile in relfiles {
            import_relfile(
                &mut writer,
                &relfile,
                pg_constants::DEFAULTTABLESPACE_OID,
                dboid,
            )?;
        }
    }
    for entry in fs::read_dir(path.join("pg_xact"))? {
        let entry = entry?;
        import_slru_file(&mut writer, SlruKind::Clog, &entry.path())?;
    }
    for entry in fs::read_dir(path.join("pg_multixact").join("members"))? {
        let entry = entry?;
        import_slru_file(&mut writer, SlruKind::MultiXactMembers, &entry.path())?;
    }
    for entry in fs::read_dir(path.join("pg_multixact").join("offsets"))? {
        let entry = entry?;
        import_slru_file(&mut writer, SlruKind::MultiXactOffsets, &entry.path())?;
    }
    for entry in fs::read_dir(path.join("pg_twophase"))? {
        let entry = entry?;
        let xid = u32::from_str_radix(entry.path().to_str().unwrap(), 16)?;
        import_twophase_file(&mut writer, xid, &entry.path())?;
    }
    // TODO: Scan pg_tblspc

    // We're done importing all the data files.
    writer.finish()?;

    // We expect the Postgres server to be shut down cleanly.
    let pg_control = pg_control.context("pg_control file not found")?;
    ensure!(
        pg_control.state == DBState_DB_SHUTDOWNED,
        "Postgres cluster was not shut down cleanly"
    );
    ensure!(
        pg_control.checkPointCopy.redo == lsn.0,
        "unexpected checkpoint REDO pointer"
    );

    // Import WAL. This is needed even when starting from a shutdown checkpoint, because
    // this reads the checkpoint record itself, advancing the tip of the timeline to
    // *after* the checkpoint record. And crucially, it initializes the 'prev_lsn'.
    import_wal(
        &path.join("pg_wal"),
        tline,
        Lsn(pg_control.checkPointCopy.redo),
        lsn,
    )?;

    Ok(())
}

// subroutine of import_timeline_from_postgres_datadir(), to load one relation file.
fn import_relfile<R: Repository>(
    timeline: &mut DatadirTimelineWriter<R>,
    path: &Path,
    spcoid: Oid,
    dboid: Oid,
) -> Result<()> {
    // Does it look like a relation file?
    trace!("importing rel file {}", path.display());

    let p = parse_relfilename(path.file_name().unwrap().to_str().unwrap());
    if let Err(e) = p {
        warn!("unrecognized file in postgres datadir: {:?} ({})", path, e);
        return Err(e.into());
    }
    let (relnode, forknum, segno) = p.unwrap();

    let mut file = File::open(path)?;
    let mut buf: [u8; 8192] = [0u8; 8192];

    let len = file.metadata().unwrap().len();
    ensure!(len % pg_constants::BLCKSZ as u64 == 0);
    let nblocks = len / pg_constants::BLCKSZ as u64;

    if segno != 0 {
        todo!();
    }

    let rel = RelTag {
        spcnode: spcoid,
        dbnode: dboid,
        relnode,
        forknum,
    };
    timeline.put_rel_creation(rel, nblocks as u32)?;

    let mut blknum: u32 = segno * (1024 * 1024 * 1024 / pg_constants::BLCKSZ as u32);
    loop {
        let r = file.read_exact(&mut buf);
        match r {
            Ok(_) => {
                timeline.put_rel_page_image(rel, blknum, Bytes::copy_from_slice(&buf))?;
            }

            // TODO: UnexpectedEof is expected
            Err(err) => match err.kind() {
                std::io::ErrorKind::UnexpectedEof => {
                    // reached EOF. That's expected.
                    // FIXME: maybe check that we read the full length of the file?
                    break;
                }
                _ => {
                    bail!("error reading file {}: {:#}", path.display(), err);
                }
            },
        };
        blknum += 1;
    }
    ensure!(blknum == nblocks as u32);

    Ok(())
}

/// FIXME
/// Import a "non-blocky" file into the repository
///
/// This is used for small files like the control file, twophase files etc. that
/// are just slurped into the repository as one blob.
///
fn import_relmap_file<R: Repository>(
    timeline: &mut DatadirTimelineWriter<R>,
    spcnode: Oid,
    dbnode: Oid,
    path: &Path,
) -> Result<()> {
    let mut file = File::open(path)?;
    let mut buffer = Vec::new();
    // read the whole file
    file.read_to_end(&mut buffer)?;

    trace!("importing relmap file {}", path.display());

    timeline.put_relmap_file(spcnode, dbnode, Bytes::copy_from_slice(&buffer[..]))?;
    Ok(())
}

fn import_twophase_file<R: Repository>(
    timeline: &mut DatadirTimelineWriter<R>,
    xid: TransactionId,
    path: &Path,
) -> Result<()> {
    let mut file = File::open(path)?;
    let mut buffer = Vec::new();
    // read the whole file
    file.read_to_end(&mut buffer)?;

    trace!("importing non-rel file {}", path.display());

    timeline.put_twophase_file(xid, Bytes::copy_from_slice(&buffer[..]))?;
    Ok(())
}

///
/// Import pg_control file into the repository.
///
/// The control file is imported as is, but we also extract the checkpoint record
/// from it and store it separated.
fn import_control_file<R: Repository>(
    timeline: &mut DatadirTimelineWriter<R>,
    path: &Path,
) -> Result<ControlFileData> {
    let mut file = File::open(path)?;
    let mut buffer = Vec::new();
    // read the whole file
    file.read_to_end(&mut buffer)?;

    trace!("importing control file {}", path.display());

    // Import it as ControlFile
    timeline.put_control_file(Bytes::copy_from_slice(&buffer[..]))?;

    // Extract the checkpoint record and import it separately.
    let pg_control = ControlFileData::decode(&buffer)?;
    let checkpoint_bytes = pg_control.checkPointCopy.encode();
    timeline.put_checkpoint(checkpoint_bytes)?;

    Ok(pg_control)
}

///
/// Import an SLRU segment file
///
fn import_slru_file<R: Repository>(
    timeline: &mut DatadirTimelineWriter<R>,
    slru: SlruKind,
    path: &Path,
) -> Result<()> {
    trace!("importing slru file {}", path.display());

    let mut file = File::open(path)?;
    let mut buf: [u8; 8192] = [0u8; 8192];
    let segno = u32::from_str_radix(path.file_name().unwrap().to_str().unwrap(), 16)?;

    let len = file.metadata().unwrap().len();
    ensure!(len % pg_constants::BLCKSZ as u64 == 0); // we assume SLRU block size is the same as BLCKSZ
    let nblocks = len / pg_constants::BLCKSZ as u64;

    ensure!(nblocks <= pg_constants::SLRU_PAGES_PER_SEGMENT as u64);

    timeline.put_slru_segment_creation(slru, segno, nblocks as u32)?;

    let mut rpageno = 0;
    loop {
        let r = file.read_exact(&mut buf);
        match r {
            Ok(_) => {
                timeline.put_slru_page_image(slru, segno, rpageno, Bytes::copy_from_slice(&buf))?;
            }

            // TODO: UnexpectedEof is expected
            Err(err) => match err.kind() {
                std::io::ErrorKind::UnexpectedEof => {
                    // reached EOF. That's expected.
                    // FIXME: maybe check that we read the full length of the file?
                    break;
                }
                _ => {
                    bail!("error reading file {}: {:#}", path.display(), err);
                }
            },
        };
        rpageno += 1;
    }
    ensure!(rpageno == nblocks as u32);

    Ok(())
}

/// Scan PostgreSQL WAL files in given directory and load all records between
/// 'startpoint' and 'endpoint' into the repository.
fn import_wal<R: Repository>(
    walpath: &Path,
    tline: &mut DatadirTimeline<R>,
    startpoint: Lsn,
    endpoint: Lsn,
) -> Result<()> {
    let mut waldecoder = WalStreamDecoder::new(startpoint);

    let mut segno = startpoint.segment_number(pg_constants::WAL_SEGMENT_SIZE);
    let mut offset = startpoint.segment_offset(pg_constants::WAL_SEGMENT_SIZE);
    let mut last_lsn = startpoint;

    let mut walingest = WalIngest::new(tline, startpoint)?;

    while last_lsn <= endpoint {
        // FIXME: assume postgresql tli 1 for now
        let filename = XLogFileName(1, segno, pg_constants::WAL_SEGMENT_SIZE);
        let mut buf = Vec::new();

        // Read local file
        let mut path = walpath.join(&filename);

        // It could be as .partial
        if !PathBuf::from(&path).exists() {
            path = walpath.join(filename + ".partial");
        }

        // Slurp the WAL file
        let mut file = File::open(&path)?;

        if offset > 0 {
            file.seek(SeekFrom::Start(offset as u64))?;
        }

        let nread = file.read_to_end(&mut buf)?;
        if nread != pg_constants::WAL_SEGMENT_SIZE - offset as usize {
            // Maybe allow this for .partial files?
            error!("read only {} bytes from WAL file", nread);
        }

        waldecoder.feed_bytes(&buf);

        let mut nrecords = 0;
        while last_lsn <= endpoint {
            if let Some((lsn, recdata)) = waldecoder.poll_decode()? {
                walingest.ingest_record(tline, recdata, lsn)?;
                last_lsn = lsn;

                nrecords += 1;

                trace!("imported record at {} (end {})", lsn, endpoint);
            }
        }

        debug!("imported {} records up to {}", nrecords, last_lsn);

        segno += 1;
        offset = 0;
    }

    if last_lsn != startpoint {
        debug!("reached end of WAL at {}", last_lsn);
    } else {
        info!("no WAL to import at {}", last_lsn);
    }

    Ok(())
}
