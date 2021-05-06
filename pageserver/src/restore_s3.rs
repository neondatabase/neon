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

use crate::{page_cache, PageServerConf};
use postgres_ffi::pg_constants;
use postgres_ffi::relfile_utils::*;

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
            Ok(_) => {}
            Err(err) => {
                error!("S3 error: {}", err);
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
            region: env::var("S3_REGION").unwrap(),
            endpoint: env::var("S3_ENDPOINT").unwrap(),
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

    // TODO: get that from backup
    let sys_id: u64 = 42;
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

    let pcache = page_cache::get_pagecache(conf, sys_id);
    pcache.init_valid_lsn(oldest_lsn);

    info!("{} files to restore...", slurp_futures.len());

    future::join_all(slurp_futures).await;
    info!("restored!");

    Ok(())
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
    let re = Regex::new(r"^(?P<relnode>\d+)(_(?P<forkname>[a-z]+))?(\.(?P<segno>\d+))?_(?P<lsnhi>[[:xdigit:]]{8})(?P<lsnlo>[[:xdigit:]]{8})$").unwrap();

    let caps = re
        .captures(fname)
        .ok_or_else(|| FilePathError::new("invalid relation data file name"))?;

    let relnode_str = caps.name("relnode").unwrap().as_str();
    let relnode: u32 = relnode_str.parse()?;

    let forkname = caps.name("forkname").map(|f| f.as_str());
    let forknum = forkname_to_forknum(forkname)?;

    let segno_match = caps.name("segno");
    let segno = if segno_match.is_none() {
        0
    } else {
        segno_match.unwrap().as_str().parse::<u32>()?
    };

    let lsn_hi: u64 = caps.name("lsnhi").unwrap().as_str().parse()?;
    let lsn_lo: u64 = caps.name("lsnlo").unwrap().as_str().parse()?;
    let lsn = lsn_hi << 32 | lsn_lo;

    Ok((relnode, forknum, segno, lsn))
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
        let (relnode, forknum, segno, lsn) = parse_filename(fname)?;

        Ok(ParsedBaseImageFileName {
            spcnode: pg_constants::GLOBALTABLESPACE_OID,
            dbnode: 0,
            relnode,
            forknum,
            segno,
            lsn,
        })
    } else if let Some(dbpath) = path.strip_prefix("base/") {
        let mut s = dbpath.split("/");
        let dbnode_str = s
            .next()
            .ok_or_else(|| FilePathError::new("invalid relation data file name"))?;
        let dbnode: u32 = dbnode_str.parse()?;
        let fname = s
            .next()
            .ok_or_else(|| FilePathError::new("invalid relation data file name"))?;
        if s.next().is_some() {
            return Err(FilePathError::new("invalid relation data file name"));
        };

        let (relnode, forknum, segno, lsn) = parse_filename(fname)?;

        Ok(ParsedBaseImageFileName {
            spcnode: pg_constants::DEFAULTTABLESPACE_OID,
            dbnode,
            relnode,
            forknum,
            segno,
            lsn,
        })
    } else if let Some(_) = path.strip_prefix("pg_tblspc/") {
        // TODO
        Err(FilePathError::new("tablespaces not supported"))
    } else {
        Err(FilePathError::new("invalid relation data file name"))
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

    // FIXME: use constants (BLCKSZ)
    let mut blknum: u32 = parsed.segno * (1024 * 1024 * 1024 / 8192);

    let pcache = page_cache::get_pagecache(conf, sys_id);

    while bytes.remaining() >= 8192 {
        let tag = page_cache::BufferTag {
            rel: page_cache::RelTag {
                spcnode: parsed.spcnode,
                dbnode: parsed.dbnode,
                relnode: parsed.relnode,
                forknum: parsed.forknum as u8,
            },
            blknum,
        };

        pcache.put_page_image(tag, parsed.lsn, bytes.copy_to_bytes(8192));

        blknum += 1;
    }
}
