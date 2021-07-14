use crate::{Page, PageIter, SnapFile, SnapFileMeta, SnapWriter};
use anyhow::{bail, Result};
use std::cmp::Ordering;
use std::path::Path;

// A helper struct that holds an iterator, along with the last
// value taken from the iterator.
struct PageStepper<'a> {
    it: PageIter<'a>,
    pub cache: Option<(u64, Page)>,
}

impl<'a> PageStepper<'a> {
    fn new(snapfile: &'a SnapFile) -> Result<Self> {
        let mut it = snapfile.all_pages();
        let cache = it.next().transpose()?;
        Ok(PageStepper { it, cache })
    }

    /// Read a new page from the iterator, returning the previous page.
    fn step(&mut self) -> Result<Option<(u64, Page)>> {
        let mut next = self.it.next().transpose()?;
        std::mem::swap(&mut self.cache, &mut next);
        Ok(next)
    }
}

/// Squash two snapshot files into one.
///
/// The resulting snapshot will contain all of the pages from both files.
/// If the same page number is stored in both, it will keep the page from
/// the newer snapshot.
///
/// The name of the resulting file will be automatically generated from
/// the snapshot metadata.
pub fn squash(older: &Path, newer: &Path, out_dir: &Path) -> Result<()> {
    let mut snap1 = SnapFile::new(older)?;
    let mut snap2 = SnapFile::new(newer)?;

    let meta1 = snap1.read_meta()?;
    let meta2 = snap2.read_meta()?;

    // Check that snap1 is the predecessor of snap2.
    match meta2.predecessor {
        Some(pred) if pred.timeline == meta1.timeline => {}
        _ => {
            bail!(
                "snap file {:?} is not the predecessor of {:?}",
                &older,
                &newer,
            );
        }
    }

    // The new combined snapshot will have most fields from meta2 (the later
    // snapshot), but will have the predecessor from meta1.
    let new_meta = SnapFileMeta {
        // There is some danger in squashing snapshots across two timelines,
        // in that it's possible to get confused about what the history
        // looks like. Ultimately, it should be possible to squash our way
        // to a "complete" snapshot (that contains all pages), so this must
        // be possible.
        timeline: meta2.timeline,
        predecessor: meta1.predecessor,
        lsn: meta2.lsn,
    };
    let mut snap_writer = SnapWriter::new(&out_dir, new_meta)?;

    let mut iter1 = PageStepper::new(&snap1)?;
    let mut iter2 = PageStepper::new(&snap2)?;

    loop {
        let next_page = match (&iter1.cache, &iter2.cache) {
            (None, None) => break,
            (Some(_), None) => iter1.step()?,
            (None, Some(_)) => iter2.step()?,
            (Some(x), Some(y)) => {
                // If these are two different page numbers, then advance the iterator
                // with the numerically lower number.
                // If they are the same page number, then store the one from the newer
                // snapshot, and discard the other (advancing both iterators).
                match x.0.cmp(&y.0) {
                    Ordering::Less => iter1.step()?,
                    Ordering::Greater => iter2.step()?,
                    Ordering::Equal => {
                        let _ = iter1.step()?;
                        iter2.step()?
                    }
                }
            }
        };
        // This can't be None, because we would already checked inside the match
        // statement.
        let (page_num, page_data) = next_page.unwrap();
        snap_writer.write_page(page_num, page_data)?;
    }

    snap_writer.finish()?;
    Ok(())
}
