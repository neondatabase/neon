use super::storage_layer::PageVersion;
use std::collections::VecDeque;
use zenith_utils::lsn::Lsn;

/// A data structure that holds one or more versions of a particular page number.
//
#[derive(Default, Clone)]
pub struct PageHistory {
    /// Pages stored in order, from oldest to newest.
    pages: VecDeque<(Lsn, PageVersion)>,
}

impl PageHistory {
    /// Create a new PageHistory containing a single image.
    pub fn from_image(lsn: Lsn, image: PageVersion) -> Self {
        let mut pages = VecDeque::new();
        pages.push_back((lsn, image));
        PageHistory { pages }
    }

    /// Push a newer page image.
    pub fn push(&mut self, lsn: Lsn, page: PageVersion) {
        if let Some((back_lsn, _)) = self.pages.back() {
            debug_assert_ne!(
                back_lsn, &lsn,
                "push page at lsn {:?} but one already exists",
                lsn
            );
            debug_assert!(back_lsn < &lsn, "pushed page is older than latest lsn");
        }
        self.pages.push_back((lsn, page));
    }

    pub fn latest(&self) -> Option<(Lsn, &PageVersion)> {
        self.pages.back().map(|(lsn, page)| (*lsn, page))
    }

    /// Split a page history at a particular LSN.
    ///
    /// This consumes this PageHistory and returns two new ones.
    /// Any changes exactly matching the split LSN will be in the
    /// "old" history.
    //
    // FIXME: Is this necessary? There is some debate whether "splitting"
    // layers is the best design.
    //
    pub fn split_at(self, split_lsn: Lsn) -> (PageHistory, PageHistory) {
        let mut old = PageHistory::default();
        let mut new = PageHistory::default();
        for (lsn, page) in self.pages {
            if lsn > split_lsn {
                new.push(lsn, page)
            } else {
                old.push(lsn, page);
            }
        }
        (old, new)
    }

    pub fn iter(&self) -> impl Iterator<Item = &(Lsn, PageVersion)> {
        self.pages.iter()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn page_history() {
        fn make_page(b: u8) -> PageVersion {
            let image = vec![b; 8192].into();
            PageVersion {
                page_image: Some(image),
                record: None,
            }
        }

        let mut ph = PageHistory::default();
        ph.push(10.into(), make_page(1));
        ph.push(20.into(), make_page(2));
        ph.push(30.into(), make_page(3));

        let (latest_lsn, latest_image) = ph.latest().unwrap();
        assert_eq!(latest_lsn, 30.into());
        assert!(matches!(latest_image, PageVersion { page_image: Some(im), .. } if im[0] == 3));

        let mut it = ph.iter();
        assert_eq!(it.next().unwrap().0, 10.into());
        assert_eq!(it.next().unwrap().0, 20.into());
        assert_eq!(it.next().unwrap().0, 30.into());
        assert!(it.next().is_none());
    }
}
