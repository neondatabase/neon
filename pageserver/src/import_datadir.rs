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
use crate::reltag::{RelTag, SlruKind};
use crate::repository::Repository;
use crate::walingest::WalIngest;
use postgres_ffi::relfile_utils::*;
use postgres_ffi::waldecoder::*;
use postgres_ffi::xlog_utils::*;
use postgres_ffi::{pg_constants, ControlFileData, DBState_DB_SHUTDOWNED};
use postgres_ffi::{Oid, TransactionId};
use utils::lsn::Lsn;

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

    let mut modification = tline.begin_modification(lsn);
    modification.init_empty()?;

    // Scan 'global'
    let mut relfiles: Vec<PathBuf> = Vec::new();
    for direntry in fs::read_dir(path.join("global"))? {
        let direntry = direntry?;
        match direntry.file_name().to_str() {
            None => continue,

            Some("pg_control") => {
                pg_control = Some(import_control_file(&mut modification, &direntry.path())?);
            }
            Some("pg_filenode.map") => {
                import_relmap_file(
                    &mut modification,
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
        import_relfile(
            &mut modification,
            &relfile,
            pg_constants::GLOBALTABLESPACE_OID,
            0,
        )?;
    }

    // Scan 'base'. It contains database dirs, the database OID is the filename.
    // E.g. 'base/12345', where 12345 is the database OID.
    for direntry in fs::read_dir(path.join("base"))? {
        let direntry = direntry?;

        //skip all temporary files
        if direntry.file_name().to_string_lossy() == "pgsql_tmp" {
            continue;
        }

        let dboid = direntry.file_name().to_string_lossy().parse::<u32>()?;

        let mut relfiles: Vec<PathBuf> = Vec::new();
        for direntry in fs::read_dir(direntry.path())? {
            let direntry = direntry?;
            match direntry.file_name().to_str() {
                None => continue,

                Some("PG_VERSION") => {
                    //modification.put_dbdir_creation(pg_constants::DEFAULTTABLESPACE_OID, dboid)?;
                }
                Some("pg_filenode.map") => import_relmap_file(
                    &mut modification,
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
                &mut modification,
                &relfile,
                pg_constants::DEFAULTTABLESPACE_OID,
                dboid,
            )?;
        }
    }
    for entry in fs::read_dir(path.join("pg_xact"))? {
        let entry = entry?;
        import_slru_file(&mut modification, SlruKind::Clog, &entry.path())?;
    }
    for entry in fs::read_dir(path.join("pg_multixact").join("members"))? {
        let entry = entry?;
        import_slru_file(&mut modification, SlruKind::MultiXactMembers, &entry.path())?;
    }
    for entry in fs::read_dir(path.join("pg_multixact").join("offsets"))? {
        let entry = entry?;
        import_slru_file(&mut modification, SlruKind::MultiXactOffsets, &entry.path())?;
    }
    for entry in fs::read_dir(path.join("pg_twophase"))? {
        let entry = entry?;
        let xid = u32::from_str_radix(&entry.path().to_string_lossy(), 16)?;
        import_twophase_file(&mut modification, xid, &entry.path())?;
    }
    // TODO: Scan pg_tblspc

    // We're done importing all the data files.
    modification.commit()?;

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

fn import_relfile<R: Repository>(
    modification: &mut DatadirModification<R>,
    path: &Path,
    spcoid: Oid,
    dboid: Oid,
) -> anyhow::Result<()> {
    let file = File::open(path)?;
    let len = file.metadata().unwrap().len() as usize;

    import_rel(modification, path, spcoid, dboid, file, len)
}

// subroutine of import_timeline_from_postgres_datadir(), to load one relation file.
fn import_rel<R: Repository, Reader: Read>(
    modification: &mut DatadirModification<R>,
    path: &Path,
    spcoid: Oid,
    dboid: Oid,
    mut reader: Reader,
    len: usize,
) -> anyhow::Result<()> {
    // Does it look like a relation file?
    trace!("importing rel file {}", path.display());

    let (relnode, forknum, segno) = parse_relfilename(&path.file_name().unwrap().to_string_lossy())
        .map_err(|e| {
            warn!("unrecognized file in postgres datadir: {:?} ({})", path, e);
            e
        })?;

    let mut buf: [u8; 8192] = [0u8; 8192];

    ensure!(len % pg_constants::BLCKSZ as usize == 0);
    let nblocks = len / pg_constants::BLCKSZ as usize;

    let rel = RelTag {
        spcnode: spcoid,
        dbnode: dboid,
        relnode,
        forknum,
    };

    let mut blknum: u32 = segno * (1024 * 1024 * 1024 / pg_constants::BLCKSZ as u32);

    // Call put_rel_creation for every segment of the relation,
    // because there is no guarantee about the order in which we are processing segments.
    // ignore "relation already exists" error
    if let Err(e) = modification.put_rel_creation(rel, nblocks as u32) {
        if e.to_string().contains("already exists") {
            info!("relation {} already exists. we must be extending it", rel);
        } else {
            return Err(e);
        }
    }

    loop {
        let r = reader.read_exact(&mut buf);
        match r {
            Ok(_) => {
                modification.put_rel_page_image(rel, blknum, Bytes::copy_from_slice(&buf))?;
            }

            // TODO: UnexpectedEof is expected
            Err(err) => match err.kind() {
                std::io::ErrorKind::UnexpectedEof => {
                    // reached EOF. That's expected.
                    let relative_blknum =
                        blknum - segno * (1024 * 1024 * 1024 / pg_constants::BLCKSZ as u32);
                    ensure!(relative_blknum == nblocks as u32, "unexpected EOF");
                    break;
                }
                _ => {
                    bail!("error reading file {}: {:#}", path.display(), err);
                }
            },
        };
        blknum += 1;
    }

    // Update relation size
    //
    // If we process rel segments out of order,
    // put_rel_extend will skip the update.
    modification.put_rel_extend(rel, blknum)?;

    Ok(())
}

/// Import a relmapper (pg_filenode.map) file into the repository
fn import_relmap_file<R: Repository>(
    modification: &mut DatadirModification<R>,
    spcnode: Oid,
    dbnode: Oid,
    path: &Path,
) -> Result<()> {
    let mut file = File::open(path)?;
    let mut buffer = Vec::new();
    // read the whole file
    file.read_to_end(&mut buffer)?;

    trace!("importing relmap file {}", path.display());

    modification.put_relmap_file(spcnode, dbnode, Bytes::copy_from_slice(&buffer[..]))?;
    Ok(())
}

/// Import a twophase state file (pg_twophase/<xid>) into the repository
fn import_twophase_file<R: Repository>(
    modification: &mut DatadirModification<R>,
    xid: TransactionId,
    path: &Path,
) -> Result<()> {
    let mut file = File::open(path)?;
    let mut buffer = Vec::new();
    // read the whole file
    file.read_to_end(&mut buffer)?;

    trace!("importing non-rel file {}", path.display());

    modification.put_twophase_file(xid, Bytes::copy_from_slice(&buffer[..]))?;
    Ok(())
}

///
/// Import pg_control file into the repository.
///
/// The control file is imported as is, but we also extract the checkpoint record
/// from it and store it separated.
fn import_control_file<R: Repository>(
    modification: &mut DatadirModification<R>,
    path: &Path,
) -> Result<ControlFileData> {
    let mut file = File::open(path)?;
    let mut buffer = Vec::new();
    // read the whole file
    file.read_to_end(&mut buffer)?;

    trace!("importing control file {}", path.display());

    // Import it as ControlFile
    modification.put_control_file(Bytes::copy_from_slice(&buffer[..]))?;

    // Extract the checkpoint record and import it separately.
    let pg_control = ControlFileData::decode(&buffer)?;
    let checkpoint_bytes = pg_control.checkPointCopy.encode()?;
    modification.put_checkpoint(checkpoint_bytes)?;

    Ok(pg_control)
}

fn import_slru_file<R: Repository>(
    modification: &mut DatadirModification<R>,
    slru: SlruKind,
    path: &Path,
) -> Result<()> {
    let file = File::open(path)?;
    let len = file.metadata().unwrap().len() as usize;
    import_slru(modification, slru, path, file, len)
}

///
/// Import an SLRU segment file
///
fn import_slru<R: Repository, Reader: Read>(
    modification: &mut DatadirModification<R>,
    slru: SlruKind,
    path: &Path,
    mut reader: Reader,
    len: usize,
) -> Result<()> {
    trace!("importing slru file {}", path.display());

    let mut buf: [u8; 8192] = [0u8; 8192];
    let segno = u32::from_str_radix(&path.file_name().unwrap().to_string_lossy(), 16)?;

    ensure!(len % pg_constants::BLCKSZ as usize == 0); // we assume SLRU block size is the same as BLCKSZ
    let nblocks = len / pg_constants::BLCKSZ as usize;

    ensure!(nblocks <= pg_constants::SLRU_PAGES_PER_SEGMENT as usize);

    modification.put_slru_segment_creation(slru, segno, nblocks as u32)?;

    let mut rpageno = 0;
    loop {
        let r = reader.read_exact(&mut buf);
        match r {
            Ok(_) => {
                modification.put_slru_page_image(
                    slru,
                    segno,
                    rpageno,
                    Bytes::copy_from_slice(&buf),
                )?;
            }

            // TODO: UnexpectedEof is expected
            Err(err) => match err.kind() {
                std::io::ErrorKind::UnexpectedEof => {
                    // reached EOF. That's expected.
                    ensure!(rpageno == nblocks as u32, "unexpected EOF");
                    break;
                }
                _ => {
                    bail!("error reading file {}: {:#}", path.display(), err);
                }
            },
        };
        rpageno += 1;
    }

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

pub fn import_basebackup_from_tar<R: Repository, Reader: Read>(
    tline: &mut DatadirTimeline<R>,
    reader: Reader,
    base_lsn: Lsn,
) -> Result<()> {
    info!("importing base at {}", base_lsn);
    let mut modification = tline.begin_modification(base_lsn);
    modification.init_empty()?;

    // Import base
    for base_tar_entry in tar::Archive::new(reader).entries()? {
        let entry = base_tar_entry.unwrap();
        let header = entry.header();
        let len = header.entry_size()? as usize;
        let file_path = header.path().unwrap().into_owned();

        match header.entry_type() {
            tar::EntryType::Regular => {
                // let mut buffer = Vec::new();
                // entry.read_to_end(&mut buffer).unwrap();

                import_file(&mut modification, file_path.as_ref(), entry, len)?;
            }
            tar::EntryType::Directory => {
                info!("directory {:?}", file_path);
                if file_path.starts_with("pg_wal") {
                    info!("found pg_wal in base lol");
                }
            }
            _ => {
                panic!("tar::EntryType::?? {}", file_path.display());
            }
        }
    }

    modification.commit()?;
    Ok(())
}

pub fn import_wal_from_tar<R: Repository, Reader: Read>(
    tline: &mut DatadirTimeline<R>,
    reader: Reader,
    start_lsn: Lsn,
    end_lsn: Lsn,
) -> Result<()> {
    // Set up walingest mutable state
    let mut waldecoder = WalStreamDecoder::new(start_lsn);
    let mut segno = start_lsn.segment_number(pg_constants::WAL_SEGMENT_SIZE);
    let mut offset = start_lsn.segment_offset(pg_constants::WAL_SEGMENT_SIZE);
    let mut last_lsn = start_lsn;
    let mut walingest = WalIngest::new(tline, start_lsn)?;

    // Ingest wal until end_lsn
    info!("importing wal until {}", end_lsn);
    let mut pg_wal_tar = tar::Archive::new(reader);
    let mut pg_wal_entries_iter = pg_wal_tar.entries()?;
    while last_lsn <= end_lsn {
        let bytes = {
            let entry = pg_wal_entries_iter.next().expect("expected more wal")?;
            let header = entry.header();
            let file_path = header.path().unwrap().into_owned();

            match header.entry_type() {
                tar::EntryType::Regular => {
                    // FIXME: assume postgresql tli 1 for now
                    let expected_filename = XLogFileName(1, segno, pg_constants::WAL_SEGMENT_SIZE);
                    let file_name = file_path.file_name().unwrap().to_string_lossy();
                    ensure!(expected_filename == file_name);

                    info!("processing wal file {:?}", file_path);
                    read_all_bytes(entry)?
                }
                tar::EntryType::Directory => {
                    info!("directory {:?}", file_path);
                    continue;
                }
                _ => {
                    panic!("tar::EntryType::?? {}", file_path.display());
                }
            }
        };

        waldecoder.feed_bytes(&bytes[offset..]);

        while last_lsn <= end_lsn {
            if let Some((lsn, recdata)) = waldecoder.poll_decode()? {
                walingest.ingest_record(tline, recdata, lsn)?;
                last_lsn = lsn;

                info!("imported record at {} (end {})", lsn, end_lsn);
            }
        }

        info!("imported records up to {}", last_lsn);
        segno += 1;
        offset = 0;
    }

    if last_lsn != start_lsn {
        info!("reached end of WAL at {}", last_lsn);
    } else {
        info!("there was no WAL to import at {}", last_lsn);
    }

    // Log any extra unused files
    for e in &mut pg_wal_entries_iter {
        let entry = e.unwrap();
        let header = entry.header();
        let file_path = header.path().unwrap().into_owned();
        info!("skipping {:?}", file_path);
    }

    Ok(())
}

pub fn import_file<R: Repository, Reader: Read>(
    modification: &mut DatadirModification<R>,
    file_path: &Path,
    reader: Reader,
    len: usize,
) -> Result<Option<ControlFileData>> {
    info!("looking at {:?}", file_path);

    if file_path.starts_with("global") {
        let spcnode = pg_constants::GLOBALTABLESPACE_OID;
        let dbnode = 0;

        match file_path.file_name().unwrap().to_string_lossy().as_ref() {
            "pg_control" => {
                let bytes = read_all_bytes(reader)?;

                // Extract the checkpoint record and import it separately.
                let pg_control = ControlFileData::decode(&bytes[..])?;
                let checkpoint_bytes = pg_control.checkPointCopy.encode()?;
                modification.put_checkpoint(checkpoint_bytes)?;
                info!("imported control file");

                // Import it as ControlFile
                modification.put_control_file(bytes)?;
                return Ok(Some(pg_control));
            }
            "pg_filenode.map" => {
                let bytes = read_all_bytes(reader)?;
                modification.put_relmap_file(spcnode, dbnode, bytes)?;
                info!("imported relmap file")
            }
            "PG_VERSION" => {
                info!("ignored");
            }
            _ => {
                import_rel(modification, file_path, spcnode, dbnode, reader, len)?;
                info!("imported rel creation");
            }
        }
    } else if file_path.starts_with("base") {
        let spcnode = pg_constants::DEFAULTTABLESPACE_OID;
        let dbnode: u32 = file_path
            .iter()
            .nth(1)
            .unwrap()
            .to_string_lossy()
            .parse()
            .unwrap();

        match file_path.file_name().unwrap().to_string_lossy().as_ref() {
            "pg_filenode.map" => {
                let bytes = read_all_bytes(reader)?;
                modification.put_relmap_file(spcnode, dbnode, bytes)?;
                info!("imported relmap file")
            }
            "PG_VERSION" => {
                info!("ignored");
            }
            _ => {
                import_rel(modification, file_path, spcnode, dbnode, reader, len)?;
                info!("imported rel creation");
            }
        }
    } else if file_path.starts_with("pg_xact") {
        let slru = SlruKind::Clog;

        import_slru(modification, slru, file_path, reader, len)?;
        info!("imported clog slru");
    } else if file_path.starts_with("pg_multixact/offsets") {
        let slru = SlruKind::MultiXactOffsets;

        import_slru(modification, slru, file_path, reader, len)?;
        info!("imported multixact offsets slru");
    } else if file_path.starts_with("pg_multixact/members") {
        let slru = SlruKind::MultiXactMembers;

        import_slru(modification, slru, file_path, reader, len)?;
        info!("imported multixact members slru");
    } else if file_path.starts_with("pg_twophase") {
        let xid = u32::from_str_radix(&file_path.file_name().unwrap().to_string_lossy(), 16)?;

        let bytes = read_all_bytes(reader)?;
        modification.put_twophase_file(xid, Bytes::copy_from_slice(&bytes[..]))?;
        info!("imported twophase file");
    } else if file_path.starts_with("pg_wal") {
        panic!("found wal file in base section");
    } else {
        info!("ignored");
    }

    // TODO: pg_tblspc ??

    Ok(None)
}

fn read_all_bytes<Reader: Read>(mut reader: Reader) -> Result<Bytes> {
    let mut buf: Vec<u8> = vec![];
    reader.read_to_end(&mut buf)?;
    Ok(Bytes::copy_from_slice(&buf[..]))
}
