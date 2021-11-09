use std::io::Write;
use std::os::unix::prelude::FileExt;

use anyhow::Result;
use bookfile::{BookWriter, BoundedReader, ChapterId, ChapterWriter};
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
pub struct BlobRange {
    offset: u64,
    size: usize,
}

pub fn read_blob<F: FileExt>(reader: &BoundedReader<&'_ F>, range: &BlobRange) -> Result<Vec<u8>> {
    let mut buf = vec![0u8; range.size];
    reader.read_exact_at(&mut buf, range.offset)?;
    Ok(buf)
}

pub struct BlobWriter<W> {
    writer: ChapterWriter<W>,
    offset: u64,
}

impl<W: Write> BlobWriter<W> {
    // This function takes a BookWriter and creates a new chapter to ensure offset is 0.
    pub fn new(book_writer: BookWriter<W>, chapter_id: impl Into<ChapterId>) -> Self {
        let writer = book_writer.new_chapter(chapter_id);
        Self { writer, offset: 0 }
    }

    pub fn write_blob(&mut self, blob: &[u8]) -> Result<BlobRange> {
        self.writer.write_all(blob)?;

        let range = BlobRange {
            offset: self.offset,
            size: blob.len(),
        };
        self.offset += blob.len() as u64;
        Ok(range)
    }

    pub fn close(self) -> bookfile::Result<BookWriter<W>> {
        self.writer.close()
    }
}
