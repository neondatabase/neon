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

use s3::bucket::Bucket;
use s3::creds::Credentials;
use s3::region::Region;
use s3::S3Error;

use tokio::runtime;

use futures::future;

use crate::{controlfile, page_cache, pg_constants, PageServerConf};

struct Storage {
    region: Region,
    credentials: Credentials,
    bucket: String,
}

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
                error!("S3 error: {}", err);
                return;
            }
        }
    });
}

//
// Restores one chunk from S3.
//
// 1. Fetch the last base image >= given LSN
// 2. Fetch all WAL
//
// Load it all into the page cache.
//
async fn restore_chunk(conf: &PageServerConf) -> Result<(), S3Error> {
    let backend = Storage {
        region: Region::Custom {
            region: env::var("S3_REGION").unwrap().into(),
            endpoint: env::var("S3_ENDPOINT").unwrap().into(),
        },
        credentials: Credentials::new(
            Some(&env::var("S3_ACCESSKEY").unwrap()),
            Some(&env::var("S3_SECRET").unwrap()),
            None,
            None,
            None,
        )
        .unwrap(),
        bucket: "zenith-testbucket".to_string(),
    };

    info!("Restoring from S3...");

    // Create Bucket in REGION for BUCKET
    let bucket = Bucket::new_with_path_style(&backend.bucket, backend.region, backend.credentials)?;

    // List out contents of directory
    let results: Vec<s3::serde_types::ListBucketResult> = bucket
        .list("relationdata/".to_string(), Some("".to_string()))
        .await?;

    //Before uploading other files, slurp pg_control to set systemid

    let control_results: Vec<s3::serde_types::ListBucketResult> = bucket
        .list(
            "relationdata/global/pg_control".to_string(),
            Some("".to_string()),
        )
        .await?;
    let object = &(&control_results[0]).contents[0];
    let (data, _) = bucket.get_object(&object.key).await.unwrap();
    let bytes = BytesMut::from(data.as_slice()).freeze();
    let c = postgres_ffi::decode_pg_control(bytes);

    let pcache = page_cache::get_pagecache(conf.clone(), c.system_identifier);
    pcache.set_controldata(c.clone());
    trace!("uploaded controlfile {:?}", pcache.get_controldata());

    let sys_id: u64 = c.system_identifier;
    let mut oldest_lsn = 0;
    let mut slurp_futures: Vec<_> = Vec::new();

    for result in results {
        for object in result.contents {
            // Download every relation file, slurping them into memory

            let key = object.key;
            let relpath = key.strip_prefix("relationdata/").unwrap();

            let parsed = parse_rel_file_path(&relpath);

            match parsed {
                Ok(p) => {
                    if oldest_lsn == 0 || p.lsn < oldest_lsn {
                        oldest_lsn = p.lsn;
                    }
                    let b = bucket.clone();
                    let f = slurp_base_file(conf, sys_id, b, key.to_string(), p);

                    slurp_futures.push(f);
                }
                Err(e) => {
                    warn!("unrecognized file: {} ({})", relpath, e);
                }
            };
        }
    }

    if oldest_lsn == 0 {
        panic!("no base backup found");
    }

    //Now add nonrelation files
    let nonrelresults: Vec<s3::serde_types::ListBucketResult> = bucket
        .list("nonreldata/".to_string(), Some("".to_string()))
        .await?;
    for result in nonrelresults {
        for object in result.contents {
            // Download needed non relation files, slurping them into memory

            let key = object.key;
            let relpath = key.strip_prefix("nonreldata/").unwrap();
            trace!("list nonrelfiles {}", relpath);

            let parsed = parse_nonrel_file_path(&relpath);

            match parsed {
                Ok(p) => {
                    let b = bucket.clone();
                    let f = slurp_base_file(conf, sys_id, b, key.to_string(), p);

                    slurp_futures.push(f);
                }
                Err(e) => {
                    warn!("unrecognized file: {} ({})", relpath, e);
                }
            };
        }
    }

    pcache.init_valid_lsn(oldest_lsn);

    info!("{} files to restore...", slurp_futures.len());

    future::join_all(slurp_futures).await;
    info!(
        "restored! {:?} to {:?}",
        pcache.first_valid_lsn, pcache.last_valid_lsn
    );

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

fn parse_lsn_from_filename(fname: &str) -> Result<u64, FilePathError> {
    let (_, lsn_str) = fname.split_at(fname.len() - 16);

    let (lsnhi, lsnlo) = lsn_str.split_at(8);
    let lsn_hi = u64::from_str_radix(lsnhi, 16)?;
    let lsn_lo = u64::from_str_radix(lsnlo, 16)?;
    let lsn = lsn_hi << 32 | lsn_lo;

    return Ok(lsn);
}

// formats:
// <oid>
// <oid>_<fork name>
// <oid>.<segment number>
// <oid>_<fork name>.<segment number>

fn parse_filename(fname: &str) -> Result<(u32, u32, u32, u64), FilePathError> {
    let re = Regex::new(r"^(?P<relnode>\d+)(_(?P<forkname>[a-z]+))?(\.(?P<segno>\d+))?_(?P<lsnhi>[[:xdigit:]]{8})(?P<lsnlo>[[:xdigit:]]{8})$").unwrap();

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

    let lsn_hi = u64::from_str_radix(caps.name("lsnhi").unwrap().as_str(), 16)?;
    let lsn_lo = u64::from_str_radix(caps.name("lsnlo").unwrap().as_str(), 16)?;
    let lsn = lsn_hi << 32 | lsn_lo;

    return Ok((relnode, forknum, segno, lsn));
}

fn parse_nonrel_file_path(path: &str) -> Result<ParsedBaseImageFileName, FilePathError> {
    //TODO parse segno from xact filenames too
    if let Some(fname) = path.strip_prefix("pg_xact/") {
        let lsn = parse_lsn_from_filename(fname.clone())?;

        return Ok(ParsedBaseImageFileName {
            spcnode: 0,
            dbnode: 0,
            relnode: 0,
            forknum: pg_constants::PG_XACT_FORKNUM,
            segno: 0,
            lsn,
        });
    } else if let Some(fname) = path.strip_prefix("pg_multixact/offsets") {
        let lsn = parse_lsn_from_filename(fname.clone())?;

        return Ok(ParsedBaseImageFileName {
            spcnode: 0,
            dbnode: 0,
            relnode: 0,
            forknum: pg_constants::PG_MXACT_OFFSETS_FORKNUM,
            segno: 0,
            lsn,
        });
    } else if let Some(fname) = path.strip_prefix("pg_multixact/members") {
        let lsn = parse_lsn_from_filename(fname.clone())?;

        return Ok(ParsedBaseImageFileName {
            spcnode: 0,
            dbnode: 0,
            relnode: 0,
            forknum: pg_constants::PG_MXACT_MEMBERS_FORKNUM,
            segno: 0,
            lsn,
        });
    } else {
        return Err(FilePathError::new("invalid non relation data file name"));
    }
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
            let lsn = parse_lsn_from_filename(fname.clone())?;

            return Ok(ParsedBaseImageFileName {
                spcnode: pg_constants::GLOBALTABLESPACE_OID,
                dbnode: 0,
                relnode: 0,
                forknum: pg_constants::PG_CONTROLFILE_FORKNUM,
                segno: 0,
                lsn,
            });
        }

        if fname.contains("pg_filenode") {
            let lsn = parse_lsn_from_filename(fname.clone())?;

            return Ok(ParsedBaseImageFileName {
                spcnode: pg_constants::GLOBALTABLESPACE_OID,
                dbnode: 0,
                relnode: 0,
                forknum: pg_constants::PG_FILENODEMAP_FORKNUM,
                segno: 0,
                lsn,
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
            let lsn = parse_lsn_from_filename(fname.clone())?;

            return Ok(ParsedBaseImageFileName {
                spcnode: pg_constants::DEFAULTTABLESPACE_OID,
                dbnode,
                relnode: 0,
                forknum: pg_constants::PG_FILENODEMAP_FORKNUM,
                segno: 0,
                lsn,
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
    } else if let Some(_) = path.strip_prefix("pg_tblspc/") {
        // TODO
        return Err(FilePathError::new("tablespaces not supported"));
    } else {
        return Err(FilePathError::new("invalid relation data file name"));
    }
}

//
// Load a base file from S3, and insert it into the page cache
//
async fn slurp_base_file(
    conf: &PageServerConf,
    sys_id: u64,
    bucket: Bucket,
    s3path: String,
    parsed: ParsedBaseImageFileName,
) {
    // FIXME: rust-s3 opens a new connection for each request. Should reuse
    // the reqwest::Client object. But that requires changes to rust-s3 itself.
    let (data, code) = bucket.get_object(s3path.clone()).await.unwrap();

    trace!("got response: {} on {}", code, &s3path);
    assert_eq!(200, code);

    let mut bytes = BytesMut::from(data.as_slice()).freeze();

    let pcache = page_cache::get_pagecache(conf.clone(), sys_id);

    // pg_filenode.map has non-standard size - 512 bytes
    if parsed.forknum == pg_constants::PG_FILENODEMAP_FORKNUM {
        let b = bytes.clone();
        controlfile::decode_filemapping(b);
        while bytes.remaining() >= 512 {
            let tag = page_cache::BufferTag {
                spcnode: parsed.spcnode,
                dbnode: parsed.dbnode,
                relnode: parsed.relnode,
                forknum: parsed.forknum as u8,
                blknum: 0,
            };

            pcache.put_page_image(tag, parsed.lsn, bytes.copy_to_bytes(512));
        }

        let tag = page_cache::RelTag {
            spcnode: parsed.spcnode,
            dbnode: parsed.dbnode,
            relnode: parsed.relnode,
            forknum: parsed.forknum as u8,
        };

        pcache.relsize_inc(&tag, Some(0));
    } else {
        // FIXME: use constants (BLCKSZ)
        let mut blknum: u32 = parsed.segno * (1024 * 1024 * 1024 / 8192);
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
}
