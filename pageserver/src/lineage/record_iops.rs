use crate::lineage::spec::{LineageRecordHandler, RecordIOps};
use crate::repository::Value;
use anyhow::{anyhow, Context, Error, Result};
use byteorder::{ReadBytesExt, WriteBytesExt, LE};
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use std::io::Cursor;
use std::iter::{once, Chain, FilterMap, Map};
use std::marker::PhantomData;
use utils::bin_ser::LeSer;

#[derive(Copy, Clone, Default, Debug)]
pub struct PrefixedByPageImage<T>(pub T);

impl<T> RecordIOps for PrefixedByPageImage<T>
where
    T: RecordIOps,
{
    type ReaderIter = Chain<core::option::IntoIter<Value>, T::ReaderIter>;
    type WriterIter = Chain<core::iter::Once<Result<Bytes>>, T::WriterIter>;

    fn read_records_from(mut iter: Box<dyn Iterator<Item = Bytes>>) -> Self::ReaderIter {
        let res = iter.next();

        res.map(Value::Image)
            .into_iter()
            .chain(T::read_records_from(iter))
    }

    fn serialize_records(mut iter: Box<dyn Iterator<Item = Value>>) -> Self::WriterIter {
        let image = iter.next().context("Missing record");

        let bytes = match image {
            Ok(Value::Image(it)) => Ok(it),
            _ => Err(anyhow!("Unexpected value in serialization: {:?}", image)),
        };

        once(bytes).chain(T::serialize_records(iter))
    }
}

pub struct PrefixedByPageImageReadHandler<T>
where
    T: LineageRecordHandler,
{
    prefix_page: Option<Bytes>,
    rest: T::Reader,
}

impl<T> From<Bytes> for PrefixedByPageImageReadHandler<T>
where
    T: LineageRecordHandler,
{
    fn from(input: Bytes) -> Self {
        let mut cursor = Cursor::new(&input[..]);
        let rest_len = (cursor.read_u32::<LE>()).unwrap() as usize;
        let pos = cursor.position() as usize;
        drop(cursor);
        let page = input.slice(pos..(pos + rest_len));

        Self {
            prefix_page: Some(page),
            rest: T::Reader::from(input.slice((pos + rest_len)..)),
        }
    }
}
impl<T> Iterator for PrefixedByPageImageReadHandler<T>
where
    T: LineageRecordHandler,
{
    type Item = Value;

    fn next(&mut self) -> Option<Self::Item> {
        match self.prefix_page.clone() {
            None => self.rest.next(),
            Some(image) => {
                self.prefix_page = None;
                Some(Value::Image(image))
            }
        }
    }
}

pub struct PrefixedByPageImageWriteHandler<T>
where
    T: LineageRecordHandler,
{
    page: Bytes,
    rest: T::Writer,
}

impl<T> TryFrom<Vec<Value>> for PrefixedByPageImageWriteHandler<T>
where
    T: LineageRecordHandler,
{
    type Error = Error;

    fn try_from(vec: Vec<Value>) -> Result<Self> {
        let mut iter = vec.into_iter();
        let page_data = iter.next();
        let bytes = match page_data {
            Some(Value::Image(bytes)) => bytes,
            _ => {
                return Err(anyhow!(
                    "Failed to extract page from alledgedly page-prefixed iterator"
                ))
            }
        };
        Ok(Self {
            page: bytes,
            rest: T::Writer::try_from(iter.collect())?,
        })
    }
}

impl<T> From<PrefixedByPageImageWriteHandler<T>> for Bytes
where
    T: LineageRecordHandler,
{
    fn from(value: PrefixedByPageImageWriteHandler<T>) -> Bytes {
        let inner_bytes = value.rest.into();
        let bytes = Vec::with_capacity(4 /* u32 */ + value.page.len() + inner_bytes.len());
        let mut cursor = Cursor::new(bytes);

        cursor
            .write_u32::<LE>(value.page.len() as u32)
            .expect("u32 Serialization to memory");

        let mut bytes = cursor.into_inner();

        bytes.extend_from_slice(&value.page);
        bytes.extend_from_slice(&inner_bytes);

        Bytes::from(bytes)
    }
}

impl<T> LineageRecordHandler for PrefixedByPageImage<T>
where
    T: LineageRecordHandler,
{
    type Reader = PrefixedByPageImageReadHandler<T>;
    type Writer = PrefixedByPageImageWriteHandler<T>;
}

#[derive(Copy, Clone, Default, Debug, PartialEq, Eq)]
pub struct LineageAnyValue();

impl RecordIOps for LineageAnyValue {
    type ReaderIter = FilterMap<Box<dyn Iterator<Item = Bytes>>, fn(Bytes) -> Option<Value>>;
    type WriterIter = Map<Box<dyn Iterator<Item = Value>>, fn(Value) -> Result<Bytes>>;

    fn read_records_from(iter: Box<dyn Iterator<Item = Bytes>>) -> Self::ReaderIter {
        iter.filter_map(|val_bytes| {
            if let Ok(value) = Value::des(&val_bytes) {
                Some(value)
            } else {
                None
            }
        })
    }

    fn serialize_records(iter: Box<dyn Iterator<Item = Value>>) -> Self::WriterIter {
        iter.map(|value| match value.ser() {
            Ok(v) => Ok(Bytes::from(v)),
            Err(err) => Err(anyhow!(err)),
        })
    }
}

#[derive(Copy, Clone, Default, Debug, PartialEq, Eq)]
pub struct RecordType<T>
where
    T: Serialize + Deserialize<'static> + TryFrom<Value> + Into<Value> + LeSer,
{
    phantom: PhantomData<T>,
}

impl<T> RecordIOps for RecordType<T>
where
    T: Serialize + for<'de> Deserialize<'de> + TryFrom<Value> + Into<Value> + LeSer,
{
    type ReaderIter = FilterMap<Box<dyn Iterator<Item = Bytes>>, fn(Bytes) -> Option<Value>>;
    type WriterIter = Map<Box<dyn Iterator<Item = Value>>, fn(Value) -> Result<Bytes>>;

    fn read_records_from(iter: Box<dyn Iterator<Item = Bytes>>) -> Self::ReaderIter {
        iter.filter_map(|val_bytes| {
            T::des(val_bytes.as_ref())
                .ok()
                .map(<T as Into<Value>>::into)
        })
    }

    fn serialize_records(iter: Box<dyn Iterator<Item = Value>>) -> Self::WriterIter {
        iter.map(|val| {
            let it = <T as TryFrom<Value>>::try_from(val);
            match it {
                Ok(value) => match value.ser() {
                    Ok(val) => Ok(Bytes::from(val)),
                    Err(it) => Err(anyhow!("Failed to serialize data: {:?}", it)),
                },
                Err(_) => Err(anyhow!("Failure to deserialize")),
            }
        })
    }
}
