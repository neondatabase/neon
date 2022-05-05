use crate::lineage::clog_specialize::{
    CLogAbortedReader, CLogAbortedWriter, CLogBothReader, CLogBothWriter, CLogCommittedReader,
    CLogCommittedWriter, MultixactMembersReader, MultixactMembersWriter, MultixactOffsetsReader,
    MultixactOffsetsWriter,
};
use crate::lineage::reader::{LineageReader, ReadRecords};
use crate::lineage::writer::{LineageWriter, WriteRecords};
use crate::repository::Value;
use anyhow::{Error, Result};
use bytes::Bytes;
use std::marker::PhantomData;
use utils::lsn::Lsn;

/// Layout of read/write:
///
/// Base IO type: Lineage Reader/Writer. Responsible for reading/writing LSNs.
/// Inner IO type: responsible for (de)serializing the Values
///
///
/// LineageIO < handles LSNs
///   RecordIO < generic argument of LineageIO, handles records.
///

pub trait LineageRecordHandler: Sized {
    type Reader: Iterator<Item = Value> + From<Bytes> + Sized;
    type Writer: TryFrom<Vec<Value>, Error = Error> + Into<Bytes> + Sized;

    fn get_reader(&self, bytes: Bytes, limit: Lsn) -> Vec<(Lsn, Value)> {
        get_reader::<Self>(bytes, limit).collect::<Vec<(Lsn, Value)>>()
    }

    fn write(&self, vec: &[(Lsn, Value)]) -> Result<Bytes> {
        write::<Self>(vec)
    }
}

// pub struct LineageIO();
//
// impl LineageIO {
//     fn get_reader<Handler: LineageRecordHandler>(bytes: Bytes, limit: Lsn) -> LineageRea
// }

fn get_reader<RecordHandler: LineageRecordHandler>(
    bytes: Bytes,
    limit: Lsn,
) -> LineageReader<RecordHandler::Reader> {
    LineageReader::<RecordHandler::Reader>::new(bytes, limit)
}

fn write<RecordHandler: LineageRecordHandler>(vec: &[(Lsn, Value)]) -> Result<Bytes> {
    LineageWriter::<RecordHandler::Writer>::write(vec)
}
//
// pub trait LineageIO<RecordHandler, R, W>
//     where
//         R: Iterator<Item=Value> + From<Bytes> + Sized,
//         W: TryFrom<Vec<Value>, Error=Error> + Into<Bytes> + Sized,
//         RecordHandler: LineageRecordHandler<Reader=R, Writer=W>
// {
// }

/// Record-by-record IO, only needs to know de/serialization of Value types.
pub trait RecordIOps {
    type ReaderIter: Iterator<Item = Value>;
    type WriterIter: Iterator<Item = Result<Bytes>>;

    fn read_records_from(iter: Box<dyn Iterator<Item = Bytes>>) -> Self::ReaderIter;
    fn serialize_records(iter: Box<dyn Iterator<Item = Value>>) -> Self::WriterIter;
}

#[derive(Copy, Clone, Default)]
pub struct SerializedRecordHandler<Inner: RecordIOps>(PhantomData<Inner>);

impl<IOps> LineageRecordHandler for SerializedRecordHandler<IOps>
where
    IOps: RecordIOps,
{
    type Reader = ReadRecords<IOps>;
    type Writer = WriteRecords<IOps>;
}

pub struct SerializedCLogCommitsHandler();

impl LineageRecordHandler for SerializedCLogCommitsHandler {
    type Reader = CLogCommittedReader;
    type Writer = CLogCommittedWriter;
}

pub struct SerializedCLogAbortsHandler();
impl LineageRecordHandler for SerializedCLogAbortsHandler {
    type Reader = CLogAbortedReader;
    type Writer = CLogAbortedWriter;
}

pub struct SerializedCLogBothHandler();
impl LineageRecordHandler for SerializedCLogBothHandler {
    type Reader = CLogBothReader;
    type Writer = CLogBothWriter;
}
pub struct SerializedMultixactOffsetHandler();
impl LineageRecordHandler for SerializedMultixactOffsetHandler {
    type Reader = MultixactOffsetsReader;
    type Writer = MultixactOffsetsWriter;
}
pub struct SerializedMultixactMembersHandler();
impl LineageRecordHandler for SerializedMultixactMembersHandler {
    type Reader = MultixactMembersReader;
    type Writer = MultixactMembersWriter;
}
