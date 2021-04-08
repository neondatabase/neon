#![allow(non_camel_case_types)]
#![allow(non_snake_case)]

use std::io::prelude::*;
use std::fs::File;
use std::io::SeekFrom;

use bytes::{Buf, Bytes};

use log::*;

type XLogRecPtr = u64;


#[repr(C)]
#[derive(Debug)]
#[derive(Clone)]
/*
 * Body of CheckPoint XLOG records.  This is declared here because we keep
 * a copy of the latest one in pg_control for possible disaster recovery.
 * Changing this struct requires a PG_CONTROL_VERSION bump.
 */
pub struct CheckPoint {
	pub	redo: XLogRecPtr,			/* next RecPtr available when we began to
								 * create CheckPoint (i.e. REDO start point) */
	pub ThisTimeLineID: u32,	/* current TLI */
    pub PrevTimeLineID: u32,	/* previous TLI, if this record begins a new
								 * timeline (equals ThisTimeLineID otherwise) */
	pub fullPageWrites: bool,		/* current full_page_writes */
	pub nextXid: u64,	/* next free transaction ID */
	pub nextOid: u32,		/* next free OID */
	pub nextMulti: u32,		/* next free MultiXactId */
	pub nextMultiOffset: u32,	/* next free MultiXact offset */
	pub oldestXid: u32,	/* cluster-wide minimum datfrozenxid */
	pub	oldestXidDB: u32,	/* database with minimum datfrozenxid */
	pub oldestMulti: u32,	/* cluster-wide minimum datminmxid */
	pub	oldestMultiDB: u32,	/* database with minimum datminmxid */
	pub time: u64,			/* time stamp of checkpoint */
	pub oldestCommitTsXid: u32,	/* oldest Xid with valid commit
										 * timestamp */
	pub newestCommitTsXid: u32,	/* newest Xid with valid commit
										 * timestamp */

	/*
	 * Oldest XID still running. This is only needed to initialize hot standby
	 * mode from an online checkpoint, so we only bother calculating this for
	 * online checkpoints and only when wal_level is replica. Otherwise it's
	 * set to InvalidTransactionId.
	 */
	pub oldestActiveXid: u32,
}


#[repr(C)]
#[derive(Debug)]
#[derive(Clone)]
pub struct ControlFileDataZenith {
    pub system_identifier: u64,
	pg_control_version: u32, /* PG_CONTROL_VERSION */
	catalog_version_no: u32, /* see catversion.h */

	state: i32,			/* see enum above */
	time: i64,			/* time stamp of last pg_control update */
	pub checkPoint: XLogRecPtr,
    checkPointCopy: CheckPoint, /* copy of last check point record */
	unloggedLSN: XLogRecPtr,	/* current fake LSN value, for unlogged rels */
	minRecoveryPoint: XLogRecPtr,
	minRecoveryPointTLI: u32,
	backupStartPoint: XLogRecPtr,
	backupEndPoint: XLogRecPtr,
	backupEndRequired: bool
}

impl ControlFileDataZenith {
    pub fn new() -> ControlFileDataZenith
    {
        ControlFileDataZenith {
			system_identifier: 0,
            pg_control_version: 0,
			catalog_version_no: 0,
            state: 0,
            time: 0,
            checkPoint: 0,
            checkPointCopy:
            {
                CheckPoint
                {
                    redo: 0,
                    ThisTimeLineID: 0,
                    PrevTimeLineID: 0,
                    fullPageWrites: false,
                    nextXid: 0,
                    nextOid:0,
                    nextMulti: 0,
                    nextMultiOffset: 0,
                    oldestXid: 0,
                    oldestXidDB: 0,
                    oldestMulti: 0,
                    oldestMultiDB: 0,
                    time: 0,
                    oldestCommitTsXid: 0,
                    newestCommitTsXid: 0,
                    oldestActiveXid:0
                }
            },
            unloggedLSN:  0,
			minRecoveryPoint: 0,
		 	minRecoveryPointTLI: 0,
			backupStartPoint: 0,
			backupEndPoint: 0,
		   	backupEndRequired: false,
        }
    } 
}

pub fn decode_pg_control(mut buf: Bytes) -> ControlFileDataZenith {

    info!("decode pg_control");

    let controlfile : ControlFileDataZenith = ControlFileDataZenith {
			system_identifier: buf.get_u64_le(),
            pg_control_version: buf.get_u32_le(), 
			catalog_version_no: buf.get_u32_le(),
            state: buf.get_i32_le(),
            time: { buf.advance(4); buf.get_i64_le() },
            checkPoint: buf.get_u64_le(),
            checkPointCopy:
            {
                CheckPoint
                {
                    redo: buf.get_u64_le(),
                    ThisTimeLineID: buf.get_u32_le(),
                    PrevTimeLineID: buf.get_u32_le(),
                    fullPageWrites: buf.get_u8() != 0,
                    nextXid: { buf.advance(7); buf.get_u64_le()},
                    nextOid: buf.get_u32_le(),
                    nextMulti: buf.get_u32_le(),
                    nextMultiOffset: buf.get_u32_le(),
                    oldestXid:buf.get_u32_le(),
                    oldestXidDB: buf.get_u32_le(),
                    oldestMulti: buf.get_u32_le(),
                    oldestMultiDB: buf.get_u32_le(),
                    time: { buf.advance(4); buf.get_u64_le()},
                    oldestCommitTsXid: buf.get_u32_le(),
                    newestCommitTsXid: buf.get_u32_le(),
                    oldestActiveXid:buf.get_u32_le()
                }
            },
            unloggedLSN:  buf.get_u64_le(),
			minRecoveryPoint: buf.get_u64_le(),
		 	minRecoveryPointTLI: buf.get_u32_le(),
			backupStartPoint:{ buf.advance(4); buf.get_u64_le()},
			backupEndPoint: buf.get_u64_le(),
		   	backupEndRequired: buf.get_u8() != 0,
        };

        return controlfile;
}

pub fn parse_controlfile(b: Bytes)
{
    let controlfile = decode_pg_control(b);

    info!("controlfile {:X}/{:X}",
    controlfile.checkPoint >> 32, controlfile.checkPoint);
    info!("controlfile {:?}", controlfile);
}




const MAX_MAPPINGS: usize = 62;

#[derive(Debug)]
struct RelMapping
{
	mapoid: u32,			/* OID of a catalog */
	mapfilenode: u32	/* its filenode number */
}

#[derive(Debug)]
pub struct RelMapFile
{
	magic: i32,			/* always RELMAPPER_FILEMAGIC */
	num_mappings: i32,	/* number of valid RelMapping entries */
	mappings: [u8; MAX_MAPPINGS*8],
	crc: u32,			/* CRC of all above */
	pad: i32			/* to make the struct size be 512 exactly */
}

pub fn decode_filemapping(mut buf: Bytes) -> RelMapFile {

    info!("decode filemap");

    let file : RelMapFile = RelMapFile {
        magic: buf.get_i32_le(),			/* always RELMAPPER_FILEMAGIC */
        num_mappings: buf.get_i32_le(),	/* number of valid RelMapping entries */
        mappings: { 
            let mut arr = [0 as u8; MAX_MAPPINGS*8];
            buf.copy_to_slice(&mut arr);
            arr
         }
            ,
        crc: buf.get_u32_le(),			/* CRC of all above */
        pad: buf.get_i32_le()		
    };

    info!("decode filemap {:?}", file);
    file
}

pub fn write_buf_to_file(filepath: String, buf: Bytes, blkno: u32) {

    info!("write_buf_to_file {}", filepath.clone());

    let mut buffer = File::create(filepath.clone()).unwrap();
    buffer.seek(SeekFrom::Start(8192*blkno as u64)).unwrap();

    buffer.write_all(&buf).unwrap();

    info!("DONE write_buf_to_file {}", filepath);
}