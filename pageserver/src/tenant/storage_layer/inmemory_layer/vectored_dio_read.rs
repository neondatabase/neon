use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
};

use itertools::Itertools;
use tokio_epoll_uring::{BoundedBuf, IoBufMut, Slice};

use crate::context::RequestContext;

pub trait File {
    async fn read_at_to_end<'a, 'b, B: IoBufMut + Send>(
        &'b self,
        start: u32,
        dst: Slice<B>,
        ctx: &'a RequestContext,
    ) -> std::io::Result<(Slice<B>, usize)>;
}

trait Sealed {}

pub trait Buffer: Sealed + std::ops::Deref<Target = [u8]> {
    fn cap(&self) -> usize;
    fn len(&self) -> usize;
    fn remaining(&self) -> usize {
        self.cap().checked_sub(self.len()).unwrap()
    }
    /// Panics if the total length would exceed the initialized capacity.
    fn extend_from_slice(&mut self, src: &[u8]);
}

pub struct ValueRead<B: Buffer> {
    pos: u32,
    state: MutexRefCell<Result<B, Arc<std::io::Error>>>,
}

struct MutexRefCell<T>(Mutex<T>);
impl<T> MutexRefCell<T> {
    fn new(value: T) -> Self {
        Self(Mutex::new(value))
    }
    fn borrow(&self) -> impl std::ops::Deref<Target = T> + '_ {
        self.0.lock().unwrap()
    }
    fn borrow_mut(&self) -> impl std::ops::DerefMut<Target = T> + '_ {
        self.0.lock().unwrap()
    }
    fn into_inner(self) -> T {
        self.0.into_inner().unwrap()
    }
}

impl<B: Buffer> ValueRead<B> {
    pub fn new(pos: u32, buf: B) -> Self {
        Self {
            pos,
            state: MutexRefCell::new(Ok(buf)),
        }
    }
    pub fn into_result(self) -> Result<B, Arc<std::io::Error>> {
        self.state.into_inner()
    }
}

impl Sealed for Vec<u8> {}
impl Buffer for Vec<u8> {
    fn cap(&self) -> usize {
        self.capacity()
    }

    fn len(&self) -> usize {
        self.len()
    }

    fn extend_from_slice(&mut self, src: &[u8]) {
        if self.len() + src.len() > self.cap() {
            panic!("Buffer capacity exceeded");
        }
        Vec::extend_from_slice(self, src);
    }
}

pub async fn execute<'a, 'b, 'c, I, F, B>(file: &'c F, reads: I, ctx: &'b RequestContext)
where
    I: IntoIterator<Item = &'a ValueRead<B>> + Send,
    F: File + Send,
    B: Buffer + IoBufMut + Send,
{
    const DIO_CHUNK_SIZE: usize = 512;

    // Plan which parts of which chunks need to be appended to which buffer
    struct ChunkReadDestination<'a, B: Buffer> {
        value_read: &'a ValueRead<B>,
        offset_in_chunk: u32,
        len: u32,
    }
    // use of BTreeMap's sorted iterator is critical to ensure buffer is filled in order
    let mut chunk_reads: BTreeMap<u32, Vec<ChunkReadDestination<B>>> = BTreeMap::new();
    for value_read in reads {
        let ValueRead { pos, state } = value_read;
        let len = state
            .borrow()
            .as_ref()
            .expect("we haven't started reading, no chance it's in Err() state")
            .len();
        let mut remaining = usize::try_from(len).unwrap();
        let mut chunk_no = *pos / (DIO_CHUNK_SIZE as u32);
        let mut offset_in_chunk = usize::try_from(*pos % (DIO_CHUNK_SIZE as u32)).unwrap();
        while remaining > 0 {
            let remaining_in_chunk = std::cmp::min(remaining, DIO_CHUNK_SIZE - offset_in_chunk);
            chunk_reads
                .entry(chunk_no)
                .or_default()
                .push(ChunkReadDestination {
                    value_read,
                    offset_in_chunk: offset_in_chunk as u32,
                    len: remaining_in_chunk as u32,
                });
            offset_in_chunk = 0;
            chunk_no += 1;
            remaining -= remaining_in_chunk;
        }
    }

    // Merge adjacent chunk reads (merging pass on the BTreeMap iterator)
    const MAX_CHUNK_BATCH_SIZE: usize = {
        let desired = 128 * 1024; // 128k
        if desired % DIO_CHUNK_SIZE != 0 {
            panic!("MAX_CHUNK_BATCH_SIZE must be a multiple of DIO_CHUNK_SIZE")
            // compile-time error
        }
        desired / DIO_CHUNK_SIZE
    };
    struct MergedRead<'a, B: Buffer> {
        start_chunk_no: u32,
        nchunks: u32,
        dsts: Vec<MergedChunkReadDestination<'a, B>>,
    }
    struct MergedChunkReadDestination<'a, B: Buffer> {
        value_read: &'a ValueRead<B>,
        offset_in_merged_read: u32,
        len: u32,
    }
    let mut merged_reads: Vec<MergedRead<B>> = Vec::new();
    let mut chunk_reads = chunk_reads.into_iter().peekable();
    loop {
        let mut last_chunk_no = None;
        let to_merge: Vec<(u32, Vec<ChunkReadDestination<B>>)> = chunk_reads
            .peeking_take_while(|(chunk_no, _)| {
                if let Some(last_chunk_no) = last_chunk_no {
                    if *chunk_no != last_chunk_no + 1 {
                        return false;
                    }
                }
                last_chunk_no = Some(*chunk_no);
                true
            })
            .take(MAX_CHUNK_BATCH_SIZE)
            .collect(); // TODO: avoid this .collect()
        let Some(start_chunk_no) = to_merge.first().map(|(chunk_no, _)| *chunk_no) else {
            break;
        };
        let nchunks = to_merge.len() as u32;
        let dsts = to_merge
            .into_iter()
            .enumerate()
            .flat_map(|(i, (_, dsts))| {
                dsts.into_iter().map(
                    move |ChunkReadDestination {
                              value_read,
                              offset_in_chunk,
                              len,
                          }| {
                        MergedChunkReadDestination {
                            value_read,
                            offset_in_merged_read: i as u32 * DIO_CHUNK_SIZE as u32
                                + offset_in_chunk,
                            len,
                        }
                    },
                )
            })
            .collect();
        merged_reads.push(MergedRead {
            start_chunk_no,
            nchunks,
            dsts,
        });
    }
    drop(chunk_reads);

    // Execute reads and fill the destination
    // TODO: prefetch
    let get_chunk_buf = |nchunks| Vec::with_capacity(nchunks as usize * (DIO_CHUNK_SIZE));
    for MergedRead {
        start_chunk_no,
        nchunks,
        dsts,
    } in merged_reads
    {
        let all_done = dsts
            .iter()
            .all(|MergedChunkReadDestination { value_read, .. }| {
                value_read.state.borrow().is_err()
            });
        if all_done {
            continue;
        }
        let (merged_read_buf_slice, nread) = match file
            .read_at_to_end(
                start_chunk_no * DIO_CHUNK_SIZE as u32,
                get_chunk_buf(nchunks).slice_full(),
                ctx,
            )
            .await
        {
            Ok(t) => t,
            Err(e) => {
                let e = Arc::new(e);
                for MergedChunkReadDestination { value_read, .. } in dsts {
                    *value_read.state.borrow_mut() = Err(Arc::clone(&e));
                    // this will make later reads for the given ValueRead short-circuit, see top of loop body
                }
                continue;
            }
        };
        let merged_read_buf = merged_read_buf_slice.into_inner();
        assert_eq!(nread, merged_read_buf.len());
        let merged_read_buf = &merged_read_buf[..nread];
        for MergedChunkReadDestination {
            value_read,
            offset_in_merged_read,
            len,
        } in dsts
        {
            if let Ok(buf) = &mut *value_read.state.borrow_mut() {
                let data = &merged_read_buf
                    [offset_in_merged_read as usize..(offset_in_merged_read + len) as usize];
                assert!(buf.remaining() >= data.len());
                buf.extend_from_slice(data);
            }
        }
    }
}
