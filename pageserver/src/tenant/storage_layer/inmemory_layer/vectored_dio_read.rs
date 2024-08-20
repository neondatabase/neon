use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
};

use itertools::Itertools;
use tokio_epoll_uring::{BoundedBuf, IoBufMut, Slice};

use crate::context::RequestContext;

mod sealed {
    pub trait Sealed {}
}

/// The file interface we require. At runtime, this is a [`crate::virtual_file::VirtualFile`].
pub trait File: Send {
    async fn read_at_to_end<'a, 'b, B: IoBufMut + Send>(
        &'b self,
        start: u32,
        dst: Slice<B>,
        ctx: &'a RequestContext,
    ) -> std::io::Result<(Slice<B>, usize)>;
}

/// A logical read that our user wants to do.
#[derive(Debug)]
pub struct ValueRead<B: Buffer> {
    pos: u32,
    // TODO: use tri-state enum to distinguish between not started, started, and finished-with-ok-or-err
    state: MutexRefCell<Result<B, Arc<std::io::Error>>>,
}

impl<B: Buffer> ValueRead<B> {
    /// Create a new [`ValueRead`] from [`File`] of the data in the file in range `[ pos, pos + buf.cap() )`.
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

/// The buffer into which a [`ValueRead`] result is placed.
pub trait Buffer: sealed::Sealed + std::ops::Deref<Target = [u8]> {
    fn cap(&self) -> usize;
    fn len(&self) -> usize;
    fn remaining(&self) -> usize {
        self.cap().checked_sub(self.len()).unwrap()
    }
    /// Panics if the total length would exceed the initialized capacity.
    fn extend_from_slice(&mut self, src: &[u8]);
}

/// The minimum alignment and size requirement for disk offsets and memory buffer size for direct IO.
const DIO_CHUNK_SIZE: usize = 512;

/// If multiple chunks need to be read, merge adjacent chunk reads into batches of max size `MAX_CHUNK_BATCH_SIZE`.
/// (The unit is the number of chunks.)
const MAX_CHUNK_BATCH_SIZE: usize = {
    let desired = 128 * 1024; // 128k
    if desired % DIO_CHUNK_SIZE != 0 {
        panic!("MAX_CHUNK_BATCH_SIZE must be a multiple of DIO_CHUNK_SIZE")
        // compile-time error
    }
    desired / DIO_CHUNK_SIZE
};

/// Execute the given `reads` against `file`.
/// The results are placed in the buffers of the [`ValueRead`]s.
/// Retrieve the results by calling [`ValueRead::into_result`] on each [`ValueRead`].
///
/// The [`ValueRead`]s must be freshly created using [`ValueRead::new`] when calling this function.
/// Otherwise, it might panic or the value resad result will be undefined.
/// TODO: prevent this through type system.
pub async fn execute<'a, I, F, B>(file: &F, reads: I, ctx: &RequestContext)
where
    I: IntoIterator<Item = &'a ValueRead<B>>,
    F: File,
    B: Buffer + IoBufMut + Send,
{
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
        let state = state.borrow();
        match state.as_ref() {
            Err(_) => panic!("The `ValueRead`s that are passed in must be freshly created using `ValueRead::new`"),
            Ok(buf) => {
                if buf.len() != 0 {
                    panic!("The `ValueRead`s that are passed in must be freshly created using `ValueRead::new`");
                }
            }
        }
        let remaining = state
            .as_ref()
            .expect("we haven't started reading, no chance it's in Err() state")
            .remaining();
        let mut remaining = usize::try_from(remaining).unwrap();
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

#[derive(Debug)]
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

impl sealed::Sealed for Vec<u8> {}
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

#[cfg(test)]
mod tests {
    use rand::Rng;

    use crate::{
        context::DownloadBehavior, task_mgr::TaskKind,
        virtual_file::owned_buffers_io::slice::SliceMutExt,
    };

    use super::*;
    use std::{cell::RefCell, collections::VecDeque};

    struct InMemoryFile {
        content: Vec<u8>,
    }

    impl InMemoryFile {
        fn new_random(len: usize) -> Self {
            Self {
                content: rand::thread_rng()
                    .sample_iter(rand::distributions::Standard)
                    .take(len)
                    .collect(),
            }
        }
        fn test_value_read(&self, pos: u32, len: usize) -> TestValueRead {
            let expected_result = self.content[pos as usize..pos as usize + len].to_vec();
            TestValueRead {
                pos,
                expected_result,
            }
        }
    }

    impl File for InMemoryFile {
        async fn read_at_to_end<'a, 'b, B: IoBufMut + Send>(
            &'b self,
            start: u32,
            mut dst: Slice<B>,
            _ctx: &'a RequestContext,
        ) -> std::io::Result<(Slice<B>, usize)> {
            let len = std::cmp::min(
                dst.bytes_total(),
                self.content.len().saturating_sub(start as usize),
            );
            let dst_slice: &mut [u8] = dst.as_mut_rust_slice_full_zeroed();
            dst_slice[..len].copy_from_slice(&self.content[start as usize..start as usize + len]);
            rand::Rng::fill(&mut rand::thread_rng(), &mut dst_slice[len..]); // to discover bugs
            Ok((dst, len))
        }
    }

    #[derive(Debug, Clone)]
    struct TestValueRead {
        pos: u32,
        expected_result: Vec<u8>,
    }

    impl TestValueRead {
        fn make_value_read(&self) -> ValueRead<Vec<u8>> {
            ValueRead::new(self.pos, Vec::with_capacity(self.expected_result.len()))
        }
    }

    #[tokio::test]
    async fn test_blackbox() {
        let ctx = RequestContext::new(TaskKind::UnitTest, DownloadBehavior::Error);
        let cs = DIO_CHUNK_SIZE;
        let cs_u32 = u32::try_from(cs).unwrap();

        let file = InMemoryFile::new_random(10 * cs);

        let test_value_reads = vec![
            file.test_value_read(0, 1),
            // adjacent to value_read0
            file.test_value_read(1, 2),
            // gap
            // spans adjacent chunks
            file.test_value_read(cs_u32 - 1, 2),
            // gap
            //  tail of chunk 3, all of chunk 4, and 2 bytes of chunk 5
            file.test_value_read(3 * cs_u32 - 1, cs + 2),
            // gap
            file.test_value_read(5 * cs_u32, 1),
        ];
        let test_value_reads_perms = test_value_reads.iter().permutations(test_value_reads.len());

        // test all orderings of ValueReads, the order shouldn't matter for the results
        for test_value_reads in test_value_reads_perms {
            let value_reads: Vec<ValueRead<_>> = test_value_reads
                .iter()
                .map(|tr| tr.make_value_read())
                .collect();
            execute(&file, value_reads.iter(), &ctx).await;
            for (value_read, test_value_read) in
                value_reads.into_iter().zip(test_value_reads.iter())
            {
                let res = value_read
                    .into_result()
                    .expect("InMemoryFile is infallible");
                assert_eq!(res, test_value_read.expected_result);
            }
        }
    }

    #[tokio::test]
    #[should_panic]
    async fn test_reusing_value_reads_panics() {
        let ctx = RequestContext::new(TaskKind::UnitTest, DownloadBehavior::Error);
        let file = InMemoryFile::new_random(DIO_CHUNK_SIZE);
        let a = file.test_value_read(23, 10);
        let value_reads = vec![a.make_value_read()];
        execute(&file, &value_reads, &ctx).await;
        // reuse pancis
        execute(&file, &value_reads, &ctx).await;
    }

    struct RecorderFile<'a> {
        recorded: RefCell<Vec<RecordedRead>>,
        file: &'a InMemoryFile,
    }

    struct RecordedRead {
        pos: u32,
        req_len: usize,
        res: Vec<u8>,
    }

    impl<'a> RecorderFile<'a> {
        fn new(file: &'a InMemoryFile) -> RecorderFile<'a> {
            Self {
                recorded: Default::default(),
                file,
            }
        }
    }

    impl<'x> File for RecorderFile<'x> {
        async fn read_at_to_end<'a, 'b, B: IoBufMut + Send>(
            &'b self,
            start: u32,
            dst: Slice<B>,
            ctx: &'a RequestContext,
        ) -> std::io::Result<(Slice<B>, usize)> {
            let (dst, nread) = self.file.read_at_to_end(start, dst, ctx).await?;
            self.recorded.borrow_mut().push(RecordedRead {
                pos: start,
                req_len: dst.bytes_total(),
                res: Vec::from(&dst[..nread]),
            });
            Ok((dst, nread))
        }
    }

    async fn execute_and_validate_test_value_reads<I, F>(
        file: &F,
        test_value_reads: I,
        ctx: &RequestContext,
    ) where
        I: IntoIterator<Item = TestValueRead>,
        F: File,
    {
        let (tmp, test_value_reads) = test_value_reads.into_iter().tee();
        let value_reads = tmp.map(|tr| tr.make_value_read()).collect::<Vec<_>>();
        execute(file, value_reads.iter(), &ctx).await;
        for (value_read, test_value_read) in value_reads.into_iter().zip(test_value_reads) {
            let res = value_read
                .into_result()
                .expect("InMemoryFile is infallible");
            assert_eq!(res, test_value_read.expected_result);
        }
    }

    #[tokio::test]
    async fn test_value_reads_to_same_chunk_are_merged_into_one_chunk_read() {
        let ctx = RequestContext::new(TaskKind::UnitTest, DownloadBehavior::Error);

        let file = InMemoryFile::new_random(2 * DIO_CHUNK_SIZE);

        let a = file.test_value_read(DIO_CHUNK_SIZE as u32, 10);
        let b = file.test_value_read(DIO_CHUNK_SIZE as u32 + 30, 20);

        let recorder = RecorderFile::new(&file);

        execute_and_validate_test_value_reads(&recorder, vec![a, b], &ctx).await;

        let recorded = recorder.recorded.borrow();
        assert_eq!(recorded.len(), 1);
        let RecordedRead { pos, req_len, .. } = &recorded[0];
        assert_eq!(*pos, DIO_CHUNK_SIZE as u32);
        assert_eq!(*req_len, DIO_CHUNK_SIZE);
    }

    #[tokio::test]
    async fn test_max_chunk_batch_size_is_respected() {
        let ctx = RequestContext::new(TaskKind::UnitTest, DownloadBehavior::Error);

        let file = InMemoryFile::new_random(4 * MAX_CHUNK_BATCH_SIZE * DIO_CHUNK_SIZE);

        // read the 10th byte of each chunk 3 .. 3+2*MAX_CHUNK_BATCH_SIZE
        assert!(3 < MAX_CHUNK_BATCH_SIZE, "test assumption");
        assert!(10 < DIO_CHUNK_SIZE, "test assumption");
        let mut test_value_reads = Vec::new();
        for i in 3..3 + MAX_CHUNK_BATCH_SIZE + MAX_CHUNK_BATCH_SIZE / 2 {
            test_value_reads.push(file.test_value_read(i as u32 * DIO_CHUNK_SIZE as u32 + 10, 1));
        }

        let recorder = RecorderFile::new(&file);

        execute_and_validate_test_value_reads(&recorder, test_value_reads, &ctx).await;

        let recorded = recorder.recorded.borrow();
        assert_eq!(recorded.len(), 2);
        {
            let RecordedRead { pos, req_len, .. } = &recorded[0];
            assert_eq!(*pos as usize, 3 * DIO_CHUNK_SIZE);
            assert_eq!(*req_len, MAX_CHUNK_BATCH_SIZE * DIO_CHUNK_SIZE);
        }
        {
            let RecordedRead { pos, req_len, .. } = &recorded[1];
            assert_eq!(*pos as usize, (3 + MAX_CHUNK_BATCH_SIZE) * DIO_CHUNK_SIZE);
            assert_eq!(*req_len, MAX_CHUNK_BATCH_SIZE / 2 * DIO_CHUNK_SIZE);
        }
    }

    struct ExpectedRead {
        expect_pos: u32,
        expect_len: usize,
        respond: Result<Vec<u8>, String>,
    }

    struct MockFile {
        expected: RefCell<VecDeque<ExpectedRead>>,
    }

    impl Drop for MockFile {
        fn drop(&mut self) {
            assert!(
                self.expected.borrow().is_empty(),
                "expected reads not satisfied"
            );
        }
    }

    macro_rules! mock_file {
        ($($pos:expr , $len:expr => $respond:expr),* $(,)?) => {{
            MockFile {
                expected: RefCell::new(VecDeque::from(vec![$(ExpectedRead {
                    expect_pos: $pos,
                    expect_len: $len,
                    respond: $respond,
                }),*])),
            }
        }};
    }

    impl File for MockFile {
        async fn read_at_to_end<'a, 'b, B: IoBufMut + Send>(
            &'b self,
            start: u32,
            mut dst: Slice<B>,
            _ctx: &'a RequestContext,
        ) -> std::io::Result<(Slice<B>, usize)> {
            let ExpectedRead {
                expect_pos,
                expect_len,
                respond,
            } = self
                .expected
                .borrow_mut()
                .pop_front()
                .expect("unexpected read");
            assert_eq!(start, expect_pos);
            assert_eq!(dst.bytes_total(), expect_len);
            match respond {
                Ok(mocked_bytes) => {
                    let len = std::cmp::min(dst.bytes_total(), mocked_bytes.len());
                    let dst_slice: &mut [u8] = dst.as_mut_rust_slice_full_zeroed();
                    dst_slice[..len].copy_from_slice(&mocked_bytes[..len]);
                    rand::Rng::fill(&mut rand::thread_rng(), &mut dst_slice[len..]); // to discover bugs
                    Ok((dst, len))
                }
                Err(e) => Err(std::io::Error::new(std::io::ErrorKind::Other, e)),
            }
        }
    }

    #[tokio::test]
    async fn test_mock_file() {
        // Self-test to ensure the relevant features of mock file work as expected.

        let ctx = RequestContext::new(TaskKind::UnitTest, DownloadBehavior::Error);

        let mock_file = mock_file! {
            0    , 512 => Ok(vec![0; 512]),
            512  , 512 => Ok(vec![1; 512]),
            1024 , 512 => Ok(vec![2; 10]),
            2048,  1024 => Err("foo".to_owned()),
        };

        let buf = Vec::with_capacity(512);
        let (buf, nread) = mock_file
            .read_at_to_end(0, buf.slice_full(), &ctx)
            .await
            .unwrap();
        assert_eq!(nread, 512);
        assert_eq!(&buf.into_inner()[..nread], &[0; 512]);

        let buf = Vec::with_capacity(512);
        let (buf, nread) = mock_file
            .read_at_to_end(512, buf.slice_full(), &ctx)
            .await
            .unwrap();
        assert_eq!(nread, 512);
        assert_eq!(&buf.into_inner()[..nread], &[1; 512]);

        let buf = Vec::with_capacity(512);
        let (buf, nread) = mock_file
            .read_at_to_end(1024, buf.slice_full(), &ctx)
            .await
            .unwrap();
        assert_eq!(nread, 10);
        assert_eq!(&buf.into_inner()[..nread], &[2; 10]);

        let buf = Vec::with_capacity(1024);
        let err = mock_file
            .read_at_to_end(2048, buf.slice_full(), &ctx)
            .await
            .err()
            .unwrap();
        assert_eq!(err.to_string(), "foo");
    }

    #[tokio::test]
    async fn test_error_on_one_read_fails_all_value_reads() {
        let ctx = RequestContext::new(TaskKind::UnitTest, DownloadBehavior::Error);

        let file = mock_file!(
            0 * DIO_CHUNK_SIZE as u32, MAX_CHUNK_BATCH_SIZE*DIO_CHUNK_SIZE => Ok(vec![0; MAX_CHUNK_BATCH_SIZE*DIO_CHUNK_SIZE]),
            (MAX_CHUNK_BATCH_SIZE*DIO_CHUNK_SIZE) as u32, DIO_CHUNK_SIZE => Err("foo".to_owned()),
            (MAX_CHUNK_BATCH_SIZE*DIO_CHUNK_SIZE + 2*DIO_CHUNK_SIZE) as u32, DIO_CHUNK_SIZE => Ok(vec![1; DIO_CHUNK_SIZE]),
        );

        // 1 full batch and first chunk of the second batch
        let read_spanning_two_batches = ValueRead::new(
            DIO_CHUNK_SIZE as u32 / 2,
            Vec::with_capacity(MAX_CHUNK_BATCH_SIZE * DIO_CHUNK_SIZE),
        );
        let second_read_in_failing_chunk = ValueRead::new(
            (MAX_CHUNK_BATCH_SIZE * DIO_CHUNK_SIZE) as u32 + DIO_CHUNK_SIZE as u32 - 10,
            Vec::with_capacity(5),
        );
        let read_unaffected = ValueRead::new(
            (MAX_CHUNK_BATCH_SIZE * DIO_CHUNK_SIZE) as u32 + 2 * DIO_CHUNK_SIZE as u32 + 10,
            Vec::with_capacity(5),
        );

        // TODO test all permutations

        execute(
            &file,
            [
                &read_spanning_two_batches,
                &second_read_in_failing_chunk,
                &read_unaffected,
            ],
            &ctx,
        )
        .await;

        assert_eq!(
            read_spanning_two_batches
                .into_result()
                .err()
                .unwrap()
                .to_string(),
            "foo".to_owned(),
        );
        assert_eq!(
            second_read_in_failing_chunk
                .into_result()
                .err()
                .unwrap()
                .to_string(),
            "foo".to_owned(),
        );

        assert_eq!(read_unaffected.into_result().unwrap(), vec![1; 5],);
    }

    // TODO: short reads at end
}
