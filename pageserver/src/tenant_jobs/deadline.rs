use std::ops::Deref;
use std::time::Instant;

#[derive(Debug)]
pub struct Deadline<T> {
    pub start_by: Instant,
    pub inner: T,
}

impl<T> Deref for Deadline<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

// Order by priority: a deadline is greater if it starts sooner
impl<T> PartialOrd for Deadline<T> {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        other.start_by.partial_cmp(&self.start_by)
    }
}

impl<T> Ord for Deadline<T> {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        other.start_by.cmp(&self.start_by)
    }
}

impl<T> PartialEq for Deadline<T> {
    fn eq(&self, other: &Self) -> bool {
        self.start_by == other.start_by
    }
}

impl<T> Eq for Deadline<T> {}
