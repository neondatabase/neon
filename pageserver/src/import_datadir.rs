//!
//! Import data and WAL from a PostgreSQL data directory and WAL segments into
//! a zenith Timeline.
//!
use std::fs::File;
use std::io::{Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};

use anyhow::{bail, ensure, Context, Result};
use bytes::Bytes;
use tracing::*;
use walkdir::WalkDir;

use crate::pgdatadir_mapping::*;
use crate::reltag::{RelTag, SlruKind};
use crate::repository::Repository;
use crate::repository::Timeline;
use crate::walingest::WalIngest;
use crate::walrecord::DecodedWALRecord;
use postgres_ffi::relfile_utils::*;
use postgres_ffi::waldecoder::*;
use postgres_ffi::xlog_utils::*;
use postgres_ffi::Oid;
use postgres_ffi::{pg_constants, ControlFileData, DBState_DB_SHUTDOWNED};
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

    // TODO this shoud be start_lsn, which is not necessarily equal to end_lsn (aka lsn)
    // Then fishing out pg_control would be unnecessary
    let mut modification = tline.begin_modification();
    modification.init_empty()?;

    // Import all but pg_wal
    let all_but_wal = WalkDir::new(path)
        .into_iter()
        .filter_entry(|entry| !entry.path().ends_with("pg_wal"));
    for entry in all_but_wal {
        let entry = entry?;
        let metadata = entry.metadata().expect("error getting dir entry metadata");
        if metadata.is_file() {
            let absolute_path = entry.path();
            let relative_path = absolute_path.strip_prefix(path)?;

            let file = File::open(absolute_path)?;
            let len = metadata.len() as usize;
            if let Some(control_file) = import_file(&mut modification, relative_path, file, len)? {
                pg_control = Some(control_file);
            }
        }
    }

    // We're done importing all the data files.
    modification.commit(lsn)?;
    drop(modification);

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

    let filename = &path
        .file_name()
        .expect("missing rel filename")
        .to_string_lossy();
    let (relnode, forknum, segno) = parse_relfilename(filename).map_err(|e| {
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
            debug!("relation {} already exists. we must be extending it", rel);
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
    let filename = &path
        .file_name()
        .expect("missing slru filename")
        .to_string_lossy();
    let segno = u32::from_str_radix(filename, 16)?;

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
        let mut modification = tline.begin_modification();
        let mut decoded = DecodedWALRecord::default();
        while last_lsn <= endpoint {
            if let Some((lsn, recdata)) = waldecoder.poll_decode()? {
                walingest.ingest_record(recdata, lsn, &mut modification, &mut decoded)?;
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
        info!("reached end of WAL at {}", last_lsn);
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
    let mut modification = tline.begin_modification();
    modification.init_empty()?;

    let mut pg_control: Option<ControlFileData> = None;

    // Import base
    for base_tar_entry in tar::Archive::new(reader).entries()? {
        let entry = base_tar_entry?;
        let header = entry.header();
        let len = header.entry_size()? as usize;
        let file_path = header.path()?.into_owned();

        match header.entry_type() {
            tar::EntryType::Regular => {
                if let Some(res) = import_file(&mut modification, file_path.as_ref(), entry, len)? {
                    // We found the pg_control file.
                    pg_control = Some(res);
                }
            }
            tar::EntryType::Directory => {
                debug!("directory {:?}", file_path);
            }
            _ => {
                panic!("tar::EntryType::?? {}", file_path.display());
            }
        }
    }

    // sanity check: ensure that pg_control is loaded
    let _pg_control = pg_control.context("pg_control file not found")?;

    modification.commit(base_lsn)?;
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
            let file_path = header.path()?.into_owned();

            match header.entry_type() {
                tar::EntryType::Regular => {
                    // FIXME: assume postgresql tli 1 for now
                    let expected_filename = XLogFileName(1, segno, pg_constants::WAL_SEGMENT_SIZE);
                    let file_name = file_path
                        .file_name()
                        .expect("missing wal filename")
                        .to_string_lossy();
                    ensure!(expected_filename == file_name);

                    debug!("processing wal file {:?}", file_path);
                    read_all_bytes(entry)?
                }
                tar::EntryType::Directory => {
                    debug!("directory {:?}", file_path);
                    continue;
                }
                _ => {
                    panic!("tar::EntryType::?? {}", file_path.display());
                }
            }
        };

        waldecoder.feed_bytes(&bytes[offset..]);

        let mut modification = tline.begin_modification();
        let mut decoded = DecodedWALRecord::default();
        while last_lsn <= end_lsn {
            if let Some((lsn, recdata)) = waldecoder.poll_decode()? {
                walingest.ingest_record(recdata, lsn, &mut modification, &mut decoded)?;
                last_lsn = lsn;

                debug!("imported record at {} (end {})", lsn, end_lsn);
            }
        }

        debug!("imported records up to {}", last_lsn);
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
        let entry = e?;
        let header = entry.header();
        let file_path = header.path()?.into_owned();
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
    debug!("looking at {:?}", file_path);

    if file_path.starts_with("global") {
        let spcnode = pg_constants::GLOBALTABLESPACE_OID;
        let dbnode = 0;

        match file_path
            .file_name()
            .expect("missing filename")
            .to_string_lossy()
            .as_ref()
        {
            "pg_control" => {
                let bytes = read_all_bytes(reader)?;

                // Extract the checkpoint record and import it separately.
                let pg_control = ControlFileData::decode(&bytes[..])?;
                let checkpoint_bytes = pg_control.checkPointCopy.encode()?;
                modification.put_checkpoint(checkpoint_bytes)?;
                debug!("imported control file");

                // Import it as ControlFile
                modification.put_control_file(bytes)?;
                return Ok(Some(pg_control));
            }
            "pg_filenode.map" => {
                let bytes = read_all_bytes(reader)?;
                modification.put_relmap_file(spcnode, dbnode, bytes)?;
                debug!("imported relmap file")
            }
            "PG_VERSION" => {
                debug!("ignored");
            }
            _ => {
                import_rel(modification, file_path, spcnode, dbnode, reader, len)?;
                debug!("imported rel creation");
            }
        }
    } else if file_path.starts_with("base") {
        let spcnode = pg_constants::DEFAULTTABLESPACE_OID;
        let dbnode: u32 = file_path
            .iter()
            .nth(1)
            .expect("invalid file path, expected dbnode")
            .to_string_lossy()
            .parse()?;

        match file_path
            .file_name()
            .expect("missing base filename")
            .to_string_lossy()
            .as_ref()
        {
            "pg_filenode.map" => {
                let bytes = read_all_bytes(reader)?;
                modification.put_relmap_file(spcnode, dbnode, bytes)?;
                debug!("imported relmap file")
            }
            "PG_VERSION" => {
                debug!("ignored");
            }
            _ => {
                import_rel(modification, file_path, spcnode, dbnode, reader, len)?;
                debug!("imported rel creation");
            }
        }
    } else if file_path.starts_with("pg_xact") {
        let slru = SlruKind::Clog;

        import_slru(modification, slru, file_path, reader, len)?;
        debug!("imported clog slru");
    } else if file_path.starts_with("pg_multixact/offsets") {
        let slru = SlruKind::MultiXactOffsets;

        import_slru(modification, slru, file_path, reader, len)?;
        debug!("imported multixact offsets slru");
    } else if file_path.starts_with("pg_multixact/members") {
        let slru = SlruKind::MultiXactMembers;

        import_slru(modification, slru, file_path, reader, len)?;
        debug!("imported multixact members slru");
    } else if file_path.starts_with("pg_twophase") {
        let file_name = &file_path
            .file_name()
            .expect("missing twophase filename")
            .to_string_lossy();
        let xid = u32::from_str_radix(file_name, 16)?;

        let bytes = read_all_bytes(reader)?;
        modification.put_twophase_file(xid, Bytes::copy_from_slice(&bytes[..]))?;
        debug!("imported twophase file");
    } else if file_path.starts_with("pg_wal") {
        debug!("found wal file in base section. ignore it");
    } else if file_path.starts_with("zenith.signal") {
        // Parse zenith signal file to set correct previous LSN
        let bytes = read_all_bytes(reader)?;
        // zenith.signal format is "PREV LSN: prev_lsn"
        let zenith_signal = std::str::from_utf8(&bytes)?;
        let zenith_signal = zenith_signal.split(':').collect::<Vec<_>>();
        let prev_lsn = zenith_signal[1].trim().parse::<Lsn>()?;

        let writer = modification.tline.tline.writer();
        writer.finish_write(prev_lsn);

        debug!("imported zenith signal {}", prev_lsn);
    } else if file_path.starts_with("pg_tblspc") {
        // TODO Backups exported from neon won't have pg_tblspc, but we will need
        // this to import arbitrary postgres databases.
        bail!("Importing pg_tblspc is not implemented");
    } else {
        debug!("ignored");
    }

    Ok(None)
}

fn read_all_bytes<Reader: Read>(mut reader: Reader) -> Result<Bytes> {
    let mut buf: Vec<u8> = vec![];
    reader.read_to_end(&mut buf)?;
    Ok(Bytes::copy_from_slice(&buf[..]))
}
