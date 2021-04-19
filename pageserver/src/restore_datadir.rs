//
// Restore chunks from S3
//
// This runs once at Page Server startup. It loads all the "base images" from
// S3 into the in-memory page cache. It also initializes the "last valid LSN"
// in the page cache to the LSN of the base image, so that when the WAL receiver
// is started, it starts streaming from that LSN.
//

use bytes::{Buf, BytesMut};
use log::*;
use regex::Regex;
use std::env;
use std::fmt;

use tokio::runtime;

use futures::future;

use crate::{page_cache, pg_constants, PageServerConf};
use std::fs;
use walkdir::WalkDir;

pub fn restore_main(conf: &PageServerConf) {
    // Create a new thread pool
    let runtime = runtime::Runtime::new().unwrap();

    runtime.block_on(async {
        let result = restore_chunk(conf).await;

        match result {
            Ok(_) => {
                return;
            }
            Err(err) => {
                error!("error: {}", err);
                return;
            }
        }
    });
}

async fn restore_chunk(conf: &PageServerConf) -> Result<(), FilePathError> {
    let pgdata_base_path = env::var("PGDATA_BASE_PATH").unwrap();
    info!("Restoring from local dir...");

    let sys_id: u64 = 42;
    let control_lsn = 0; //TODO get it from sysid
    let mut slurp_futures: Vec<_> = Vec::new();

    for e in WalkDir::new(pgdata_base_path.clone()) {
        let entry = e.unwrap();

        if !entry.path().is_dir() {
            let path = entry.path().to_str().unwrap();

            let relpath = path
                .strip_prefix(&format!("{}/", pgdata_base_path))
                .unwrap();
            info!(
                "Restoring file {} relpath {}",
                entry.path().display(),
                relpath
            );

            let parsed = parse_rel_file_path(&relpath);

            match parsed {
                Ok(mut p) => {
                    p.lsn = control_lsn;

                    let f = slurp_base_file(conf, sys_id, path.to_string(), p);

                    slurp_futures.push(f);
                }
                Err(e) => {
                    warn!("unrecognized file: {} ({})", relpath, e);
                }
            };
        }
    }

    let pcache = page_cache::get_pagecache(conf, sys_id);
    pcache.init_valid_lsn(control_lsn);

    info!("{} files to restore...", slurp_futures.len());

    future::join_all(slurp_futures).await;
    info!("restored!");
    Ok(())
}

#[derive(Debug)]
struct FilePathError {
    msg: String,
}

impl FilePathError {
    fn new(msg: &str) -> FilePathError {
        FilePathError {
            msg: msg.to_string(),
        }
    }
}

impl From<core::num::ParseIntError> for FilePathError {
    fn from(e: core::num::ParseIntError) -> Self {
        return FilePathError {
            msg: format!("invalid filename: {}", e),
        };
    }
}

impl fmt::Display for FilePathError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "invalid filename")
    }
}

fn forkname_to_forknum(forkname: Option<&str>) -> Result<u32, FilePathError> {
    match forkname {
        // "main" is not in filenames, it's implicit if the fork name is not present
        None => Ok(0),
        Some("fsm") => Ok(1),
        Some("vm") => Ok(2),
        Some("init") => Ok(3),
        Some(_) => Err(FilePathError::new("invalid forkname")),
    }
}

#[derive(Debug)]
struct ParsedBaseImageFileName {
    pub spcnode: u32,
    pub dbnode: u32,
    pub relnode: u32,
    pub forknum: u32,
    pub segno: u32,

    pub lsn: u64,
}

// formats:
// <oid>
// <oid>_<fork name>
// <oid>.<segment number>
// <oid>_<fork name>.<segment number>
fn parse_filename(fname: &str) -> Result<(u32, u32, u32, u64), FilePathError> {
    let re = Regex::new(r"^(?P<relnode>\d+)(_(?P<forkname>[a-z]+))?(\.(?P<segno>\d+))?$").unwrap();

    let caps = re
        .captures(fname)
        .ok_or_else(|| FilePathError::new("invalid relation data file name"))?;

    let relnode_str = caps.name("relnode").unwrap().as_str();
    let relnode = u32::from_str_radix(relnode_str, 10)?;

    let forkname_match = caps.name("forkname");
    let forkname = if forkname_match.is_none() {
        None
    } else {
        Some(forkname_match.unwrap().as_str())
    };
    let forknum = forkname_to_forknum(forkname)?;

    let segno_match = caps.name("segno");
    let segno = if segno_match.is_none() {
        0
    } else {
        u32::from_str_radix(segno_match.unwrap().as_str(), 10)?
    };
    return Ok((relnode, forknum, segno, 0));
}

fn parse_rel_file_path(path: &str) -> Result<ParsedBaseImageFileName, FilePathError> {
    /*
     * Relation data files can be in one of the following directories:
     *
     * global/
     *		shared relations
     *
     * base/<db oid>/
     *		regular relations, default tablespace
     *
     * pg_tblspc/<tblspc oid>/<tblspc version>/
     *		within a non-default tablespace (the name of the directory
     *		depends on version)
     *
     * And the relation data files themselves have a filename like:
     *
     * <oid>.<segment number>
     */
    if let Some(fname) = path.strip_prefix("global/") {
        if fname.contains("pg_control") {
            return Ok(ParsedBaseImageFileName {
                spcnode: pg_constants::GLOBALTABLESPACE_OID,
                dbnode: 0,
                relnode: 0,
                forknum: pg_constants::PG_CONTROLFILE_FORKNUM,
                segno: 0,
                lsn: 0,
            });
        }

        if fname.contains("pg_filenode") {
            return Ok(ParsedBaseImageFileName {
                spcnode: pg_constants::GLOBALTABLESPACE_OID,
                dbnode: 0,
                relnode: 0,
                forknum: pg_constants::PG_FILENODEMAP_FORKNUM,
                segno: 0,
                lsn: 0,
            });
        }

        let (relnode, forknum, segno, lsn) = parse_filename(fname)?;

        return Ok(ParsedBaseImageFileName {
            spcnode: pg_constants::GLOBALTABLESPACE_OID,
            dbnode: 0,
            relnode,
            forknum,
            segno,
            lsn,
        });
    } else if let Some(dbpath) = path.strip_prefix("base/") {
        let mut s = dbpath.split("/");
        let dbnode_str = s
            .next()
            .ok_or_else(|| FilePathError::new("invalid relation data file name"))?;
        let dbnode = u32::from_str_radix(dbnode_str, 10)?;
        let fname = s
            .next()
            .ok_or_else(|| FilePathError::new("invalid relation data file name"))?;
        if s.next().is_some() {
            return Err(FilePathError::new("invalid relation data file name"));
        };

        if fname.contains("pg_filenode") {
            return Ok(ParsedBaseImageFileName {
                spcnode: pg_constants::DEFAULTTABLESPACE_OID,
                dbnode: dbnode,
                relnode: 0,
                forknum: pg_constants::PG_FILENODEMAP_FORKNUM,
                segno: 0,
                lsn: 0,
            });
        }

        let (relnode, forknum, segno, lsn) = parse_filename(fname)?;

        return Ok(ParsedBaseImageFileName {
            spcnode: pg_constants::DEFAULTTABLESPACE_OID,
            dbnode,
            relnode,
            forknum,
            segno,
            lsn,
        });
    } else if let Some(fname) = path.strip_prefix("pg_xact/") {
        return Ok(ParsedBaseImageFileName {
            spcnode: 0,
            dbnode: 0,
            relnode: 0,
            forknum: pg_constants::PG_XACT_FORKNUM,
            segno: u32::from_str_radix(fname, 10).unwrap(),
            lsn: 0,
        });
    } else if let Some(fname) = path.strip_prefix("pg_multixact/members/") {
        return Ok(ParsedBaseImageFileName {
            spcnode: 0,
            dbnode: 0,
            relnode: 0,
            forknum: pg_constants::PG_MXACT_MEMBERS_FORKNUM,
            segno: u32::from_str_radix(fname, 10).unwrap(),
            lsn: 0,
        });
    } else if let Some(fname) = path.strip_prefix("pg_multixact/offsets/") {
        return Ok(ParsedBaseImageFileName {
            spcnode: 0,
            dbnode: 0,
            relnode: 0,
            forknum: pg_constants::PG_MXACT_OFFSETS_FORKNUM,
            segno: u32::from_str_radix(fname, 10).unwrap(),
            lsn: 0,
        });
    } else if let Some(_) = path.strip_prefix("pg_tblspc/") {
        // TODO
        return Err(FilePathError::new("tablespaces not supported"));
    } else {
        return Err(FilePathError::new("invalid relation data file name"));
    }
}

async fn slurp_base_file(
    conf: &PageServerConf,
    sys_id: u64,
    file_path: String,
    parsed: ParsedBaseImageFileName,
) {
    info!("slurp_base_file local path {}", file_path);

    let mut data = fs::read(file_path).unwrap();

    // pg_filenode.map has non-standard size - 512 bytes
    // enlarge it to treat as a regular page
    if parsed.forknum == pg_constants::PG_FILENODEMAP_FORKNUM {
        data.resize(8192, 0);
    }

    let data_bytes: &[u8] = &data;
    let mut bytes = BytesMut::from(data_bytes).freeze();

    // FIXME: use constants (BLCKSZ)
    let mut blknum: u32 = parsed.segno * (1024 * 1024 * 1024 / 8192);

    let pcache = page_cache::get_pagecache(conf, sys_id);

    let reltag = page_cache::RelTag {
        spcnode: parsed.spcnode,
        dbnode: parsed.dbnode,
        relnode: parsed.relnode,
        forknum: parsed.forknum as u8,
    };

    while bytes.remaining() >= 8192 {
        let tag = page_cache::BufferTag {
            spcnode: parsed.spcnode,
            dbnode: parsed.dbnode,
            relnode: parsed.relnode,
            forknum: parsed.forknum as u8,
            blknum: blknum,
        };

        pcache.put_page_image(tag, parsed.lsn, bytes.copy_to_bytes(8192));

        pcache.relsize_inc(&reltag, Some(blknum));
        blknum += 1;
    }
}
