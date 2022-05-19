use crate::lineage::spec::RecordIOps;
use crate::repository::Value;
use anyhow::{Error, Result};
use bytes::Bytes;
use std::marker::PhantomData;
use utils::bitpacker::lsn_packing::LsnPacker;
use utils::bitpacker::u64packing::U64Packer;
use utils::bitpacker::Packer;
use utils::lsn::Lsn;

/// Writer of lineages.
/// Doesn't actually store data, but is used for holding function
/// implementations and type parameters.
///
/// This doesn't have internal state, because the writer can't apply partial
/// return values, i.e. it must consume the whole of its arguments, as opposed
/// to the LineageReader which might not need to return all of its contained
/// values.
pub struct LineageWriter<T: TryFrom<Vec<Value>, Error = Error> + Into<Bytes>>(PhantomData<T>);

impl<T> LineageWriter<T>
where
    T: TryFrom<Vec<Value>, Error = Error> + Into<Bytes>,
{
    /// Write out the
    pub fn write(vec: &[(Lsn, Value)]) -> Result<Bytes> {
        assert_ne!(vec.len(), 0usize);
        let mut lsns = None;
        let values = vec
            .iter()
            .inspect(|(lsn, _)| {
                let it = match &mut lsns {
                    None => {
                        lsns = Some(LsnPacker::new(*lsn));
                        lsns.as_mut().unwrap()
                    }
                    Some(it) => it,
                };

                it.add(*lsn);
            })
            .map(|(_, value)| value.clone())
            .collect::<Vec<Value>>();

        let data_bytes = T::try_from(values)?.into();

        let coded_lsns = lsns.unwrap().finish().as_bytes();

        let mut vec = Vec::with_capacity(coded_lsns.len() + data_bytes.len());

        vec.extend_from_slice(&coded_lsns);
        vec.extend_from_slice(&data_bytes);

        Ok(Bytes::from(vec))
    }
}

pub struct WriteRecords<T: RecordIOps> {
    data: Bytes,
    phantom: PhantomData<T>,
}

impl<T> TryFrom<Vec<Value>> for WriteRecords<T>
where
    T: RecordIOps,
{
    type Error = Error;

    fn try_from(iter: Vec<Value>) -> Result<Self> {
        let mut writer = LineageBytesWriter::new();

        for it in T::serialize_records(Box::new(iter.into_iter())) {
            match it {
                Ok(bytes) => writer.add(bytes)?,
                Err(e) => return Err(e),
            }
        }

        let bytes = writer.finish()?;

        Ok(Self {
            data: bytes,
            phantom: Default::default(),
        })
    }
}

impl<T> From<WriteRecords<T>> for Bytes
where
    T: RecordIOps,
{
    fn from(value: WriteRecords<T>) -> Bytes {
        value.data
    }
}

/// A serializer helper that serializes multiple Bytes objects into one blob of
/// bytes.
///
/// When it receives a Bytes, it stores the length in the length_coder and
/// stores the received value in the record buffer. When the user is finished,
/// the length_coder and contained records are then appended into a single
/// Bytes object.

pub struct LineageBytesWriter {
    length_coder: U64Packer,
    total_records_length: u64,
    buffered_records: Vec<Bytes>,
}

impl LineageBytesWriter {
    fn new() -> Self {
        Self {
            length_coder: U64Packer::new(),
            total_records_length: 0,
            buffered_records: vec![],
        }
    }
}

impl LineageBytesWriter {
    fn add(&mut self, input: Bytes) -> Result<()> {
        self.length_coder.add(input.len() as u64);
        self.total_records_length += input.len() as u64;
        self.buffered_records.push(input);

        Ok(())
    }

    fn finish(self) -> Result<Bytes> {
        let coder_result = self.length_coder.finish().as_bytes();
        let mut result =
            Vec::with_capacity(self.total_records_length as usize + coder_result.len());

        result.extend_from_slice(&coder_result);

        for rec in &self.buffered_records {
            result.extend_from_slice(rec);
        }

        Ok(Bytes::from(result))
    }
}
