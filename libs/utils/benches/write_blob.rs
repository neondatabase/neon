use std::{io, sync::RwLock};

use criterion::{criterion_group, criterion_main, Criterion};

pub fn write_blob(c: &mut Criterion) {
    let mut group = c.benchmark_group("write_blob");
    group.bench_function("old", |b| {
        let mut old = old::EphemeralFile::default();
        let srcbuf = [0u8; 1];
        let page_cache = FakePageCache::new();
        b.iter(|| old.write_blob(&srcbuf, page_cache))
    });
    group.bench_function("pr-5004-writer", |b| {
        let srcbuf = [0u8; 1];
        let mut new = pr5004::EphemeralFile {
            size: 0,
            page_cache: FakePageCache::new(),
        };
        b.iter(|| new.write_blob(&srcbuf))
    });
    group.bench_function("pr-4994-nopagecache", |b| {
        let mut new = pr4994::EphemeralFile::default();
        let srcbuf = [0u8; 1];
        b.iter(|| new.write_blob(&srcbuf))
    });
}

criterion_group!(benches, write_blob);
criterion_main!(benches);

const PAGE_SZ: usize = 8192;

pub struct FakePageCache {
    inner: RwLock<FakePageCacheInner>,
}

impl FakePageCache {
    fn new() -> &'static Self {
        Box::leak(Box::new(Self {
            inner: RwLock::new(FakePageCacheInner {
                buf: Box::leak(Box::new([0u8; PAGE_SZ])),
                fake_virtual_file: FakeVirtualFile,
            }),
        }))
    }
}

struct FakePageCacheInner {
    buf: &'static mut [u8; PAGE_SZ],
    fake_virtual_file: FakeVirtualFile,
}

struct FakePageWriteGuard<'a> {
    blknum: u32,
    inner: std::sync::RwLockWriteGuard<'a, FakePageCacheInner>,
}

impl FakePageCache {
    fn get_buf_for_write(&self, blknum: u32) -> Result<FakePageWriteGuard<'_>, io::Error> {
        let inner = self
            .inner
            .try_write()
            .expect("fake expects only one outstanding buf at a time");
        Ok(FakePageWriteGuard { inner, blknum })
    }
}

impl<'a> Drop for FakePageWriteGuard<'a> {
    fn drop(&mut self) {
        let inner = &mut *self.inner;
        inner
            .fake_virtual_file
            .write_exact_at(inner.buf, self.blknum as u64 * PAGE_SZ as u64)
            .expect("write_exact_at");
    }
}

impl std::ops::Deref for FakePageWriteGuard<'_> {
    type Target = [u8];

    fn deref(&self) -> &[u8] {
        self.inner.buf
    }
}

impl std::ops::DerefMut for FakePageWriteGuard<'_> {
    fn deref_mut(&mut self) -> &mut [u8] {
        self.inner.buf
    }
}

#[derive(Default)]
struct FakeVirtualFile;

impl FakeVirtualFile {
    fn write_exact_at(&mut self, buf: &[u8], offset: u64) -> Result<(), io::Error> {
        std::hint::black_box((buf, offset));
        Ok(())
    }
}

mod old {
    use std::{cmp::min, io};

    use crate::{FakePageCache, PAGE_SZ};

    #[derive(Default)]
    pub struct EphemeralFile {
        size: u64,
    }

    impl EphemeralFile {
        pub fn write_blob(
            &mut self,
            srcbuf: &[u8],
            page_cache: &'static FakePageCache,
        ) -> Result<u64, io::Error> {
            let pos = self.size;

            let mut blknum = (self.size / PAGE_SZ as u64) as u32;
            let mut off = (pos % PAGE_SZ as u64) as usize;

            let mut buf = page_cache.get_buf_for_write(blknum)?;

            // Write the length field
            if srcbuf.len() < 0x80 {
                buf[off] = srcbuf.len() as u8;
                off += 1;
            } else {
                let mut len_buf = u32::to_be_bytes(srcbuf.len() as u32);
                len_buf[0] |= 0x80;
                let thislen = PAGE_SZ - off;
                if thislen < 4 {
                    // it needs to be split across pages
                    buf[off..(off + thislen)].copy_from_slice(&len_buf[..thislen]);
                    blknum += 1;
                    buf = page_cache.get_buf_for_write(blknum)?;
                    buf[0..4 - thislen].copy_from_slice(&len_buf[thislen..]);
                    off = 4 - thislen;
                } else {
                    buf[off..off + 4].copy_from_slice(&len_buf);
                    off += 4;
                }
            }

            // Write the payload
            let mut buf_remain = srcbuf;
            while !buf_remain.is_empty() {
                let mut page_remain = PAGE_SZ - off;
                if page_remain == 0 {
                    blknum += 1;
                    buf = page_cache.get_buf_for_write(blknum)?;
                    off = 0;
                    page_remain = PAGE_SZ;
                }
                let this_blk_len = min(page_remain, buf_remain.len());
                buf[off..(off + this_blk_len)].copy_from_slice(&buf_remain[..this_blk_len]);
                off += this_blk_len;
                buf_remain = &buf_remain[this_blk_len..];
            }

            if srcbuf.len() < 0x80 {
                self.size += 1;
            } else {
                self.size += 4;
            }
            self.size += srcbuf.len() as u64;

            Ok(pos)
        }
    }
}

mod pr5004 {
    use std::{cmp::min, io};

    use crate::{FakePageCache, FakePageWriteGuard, PAGE_SZ};

    pub struct EphemeralFile {
        pub size: u64,
        pub page_cache: &'static FakePageCache,
    }

    impl EphemeralFile {
        pub fn write_blob(&mut self, srcbuf: &[u8]) -> Result<u64, io::Error> {
            struct Writer<'f> {
                ephemeral_file: &'f mut EphemeralFile,
                blknum: u32,
                off: usize,
                buf: Option<MemoizedPageWriteGuard>,
            }
            struct MemoizedPageWriteGuard {
                guard: FakePageWriteGuard<'static>,
                blknum: u32,
            }
            impl<'f> Writer<'f> {
                fn new(ephemeral_file: &'f mut EphemeralFile) -> Writer<'f> {
                    Writer {
                        blknum: (ephemeral_file.size / PAGE_SZ as u64) as u32,
                        off: (ephemeral_file.size % PAGE_SZ as u64) as usize,
                        ephemeral_file,
                        buf: None,
                    }
                }
                fn push_bytes(&mut self, src: &[u8]) -> Result<(), io::Error> {
                    let mut src_remaining = src;
                    while !src_remaining.is_empty() {
                        {
                            let head_page = match &mut self.buf {
                                Some(MemoizedPageWriteGuard { blknum, guard })
                                    if *blknum == self.blknum =>
                                {
                                    guard
                                }
                                _ => {
                                    let buf = self
                                        .ephemeral_file
                                        .page_cache
                                        .get_buf_for_write(self.blknum)?;
                                    self.buf = Some(MemoizedPageWriteGuard {
                                        guard: buf,
                                        blknum: self.blknum,
                                    });
                                    &mut self.buf.as_mut().unwrap().guard
                                }
                            };
                            let dst_remaining = &mut head_page[self.off..];
                            let n = min(dst_remaining.len(), src_remaining.len());
                            dst_remaining[..n].copy_from_slice(&src_remaining[..n]);
                            self.off += n;
                            src_remaining = &src_remaining[n..];
                        }
                        if self.off == PAGE_SZ {
                            self.blknum += 1;
                            self.off = 0;
                        }
                    }
                    Ok(())
                }
            }

            let pos = self.size;
            let mut writer = Writer::new(self);

            // Write the length field
            if srcbuf.len() < 0x80 {
                // short one-byte length header
                let len_buf = [srcbuf.len() as u8];
                writer.push_bytes(&len_buf)?;
            } else {
                let mut len_buf = u32::to_be_bytes(srcbuf.len() as u32);
                len_buf[0] |= 0x80;
                writer.push_bytes(&len_buf)?;
            }

            // Write the payload
            writer.push_bytes(srcbuf)?;

            if srcbuf.len() < 0x80 {
                self.size += 1;
            } else {
                self.size += 4;
            }
            self.size += srcbuf.len() as u64;

            Ok(pos)
        }
    }
}

mod pr4994 {
    use std::{cmp::min, io};

    use crate::{FakeVirtualFile, PAGE_SZ};

    pub struct EphemeralFile {
        mutable_head: [u8; PAGE_SZ],
        size: u64,
        virtual_file: FakeVirtualFile,
    }

    impl Default for EphemeralFile {
        fn default() -> Self {
            Self {
                mutable_head: [0u8; PAGE_SZ],
                size: 0,
                virtual_file: FakeVirtualFile::default(),
            }
        }
    }

    impl EphemeralFile {
        pub fn write_blob(&mut self, srcbuf: &[u8]) -> Result<u64, io::Error> {
            struct Writer<'a> {
                ephemeral_file: &'a mut EphemeralFile,
                blknum: u32,
                off: usize,
            }

            impl<'a> Writer<'a> {
                fn new(ephemeral_file: &'a mut EphemeralFile) -> Writer<'a> {
                    Writer {
                        blknum: (ephemeral_file.size / PAGE_SZ as u64) as u32,
                        off: (ephemeral_file.size % PAGE_SZ as u64) as usize,
                        ephemeral_file,
                    }
                }
                fn push_bytes(&mut self, src: &[u8]) -> Result<(), io::Error> {
                    let mut src_remaining = src;
                    while !src_remaining.is_empty() {
                        {
                            let dst_remaining = &mut self.ephemeral_file.mutable_head[self.off..];
                            let n = min(dst_remaining.len(), src_remaining.len());
                            dst_remaining[..n].copy_from_slice(&src_remaining[..n]);
                            self.off += n;
                            src_remaining = &src_remaining[n..];
                        }
                        if self.off == PAGE_SZ {
                            self.ephemeral_file.virtual_file.write_exact_at(
                                &self.ephemeral_file.mutable_head,
                                self.blknum as u64 * PAGE_SZ as u64,
                            )?;
                            self.blknum += 1;
                            self.off = 0;
                        }
                    }
                    Ok(())
                }
            }

            let pos = self.size;
            let mut writer = Writer::new(self);

            // Write the length field
            if srcbuf.len() < 0x80 {
                writer.push_bytes(&[srcbuf.len() as u8])?;
            } else {
                let mut len_buf = u32::to_be_bytes(srcbuf.len() as u32);
                len_buf[0] |= 0x80;
                writer.push_bytes(&len_buf)?;
            }

            // Write the payload
            writer.push_bytes(srcbuf)?;

            if srcbuf.len() < 0x80 {
                self.size += 1;
            } else {
                self.size += 4;
            }
            self.size += srcbuf.len() as u64;

            Ok(pos)
        }
    }
}
