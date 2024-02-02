//! A heapless buffer for events of sorts.

use std::ops;

use heapless::HistoryBuffer;

#[derive(Debug, Clone)]
pub struct HistoryBufferWithDropCounter<T, const L: usize> {
    buffer: HistoryBuffer<T, L>,
    drop_count: u64,
}

impl<T, const L: usize> HistoryBufferWithDropCounter<T, L> {
    pub fn write(&mut self, data: T) {
        let len_before = self.buffer.len();
        self.buffer.write(data);
        let len_after = self.buffer.len();
        self.drop_count += u64::from(len_before == len_after);
    }
    pub fn drop_count(&self) -> u64 {
        self.drop_count
    }
    pub fn map<U, F: Fn(&T) -> U>(&self, f: F) -> HistoryBufferWithDropCounter<U, L> {
        let mut buffer = HistoryBuffer::new();
        buffer.extend(self.buffer.oldest_ordered().map(f));
        HistoryBufferWithDropCounter::<U, L> {
            buffer,
            drop_count: self.drop_count,
        }
    }
}

impl<T, const L: usize> Default for HistoryBufferWithDropCounter<T, L> {
    fn default() -> Self {
        Self {
            buffer: HistoryBuffer::default(),
            drop_count: 0,
        }
    }
}

impl<T, const L: usize> ops::Deref for HistoryBufferWithDropCounter<T, L> {
    type Target = HistoryBuffer<T, L>;

    fn deref(&self) -> &Self::Target {
        &self.buffer
    }
}

#[derive(serde::Serialize)]
struct SerdeRepr<T> {
    buffer: Vec<T>,
    drop_count: u64,
}

impl<'a, T, const L: usize> From<&'a HistoryBufferWithDropCounter<T, L>> for SerdeRepr<T>
where
    T: Clone + serde::Serialize,
{
    fn from(value: &'a HistoryBufferWithDropCounter<T, L>) -> Self {
        let HistoryBufferWithDropCounter { buffer, drop_count } = value;
        SerdeRepr {
            buffer: buffer.iter().cloned().collect(),
            drop_count: *drop_count,
        }
    }
}

impl<T, const L: usize> serde::Serialize for HistoryBufferWithDropCounter<T, L>
where
    T: Clone + serde::Serialize,
{
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        SerdeRepr::from(self).serialize(serializer)
    }
}

#[cfg(test)]
mod test {
    use super::HistoryBufferWithDropCounter;

    #[test]
    fn test_basics() {
        let mut b = HistoryBufferWithDropCounter::<_, 2>::default();
        b.write(1);
        b.write(2);
        b.write(3);
        assert!(b.iter().any(|e| *e == 2));
        assert!(b.iter().any(|e| *e == 3));
        assert!(!b.iter().any(|e| *e == 1));
    }

    #[test]
    fn test_drop_count_works() {
        let mut b = HistoryBufferWithDropCounter::<_, 2>::default();
        b.write(1);
        assert_eq!(b.drop_count(), 0);
        b.write(2);
        assert_eq!(b.drop_count(), 0);
        b.write(3);
        assert_eq!(b.drop_count(), 1);
        b.write(4);
        assert_eq!(b.drop_count(), 2);
    }

    #[test]
    fn test_clone_works() {
        let mut b = HistoryBufferWithDropCounter::<_, 2>::default();
        b.write(1);
        b.write(2);
        b.write(3);
        assert_eq!(b.drop_count(), 1);
        let mut c = b.clone();
        assert_eq!(c.drop_count(), 1);
        assert!(c.iter().any(|e| *e == 2));
        assert!(c.iter().any(|e| *e == 3));
        assert!(!c.iter().any(|e| *e == 1));

        c.write(4);
        assert!(c.iter().any(|e| *e == 4));
        assert!(!b.iter().any(|e| *e == 4));
    }

    #[test]
    fn test_map() {
        let mut b = HistoryBufferWithDropCounter::<_, 2>::default();

        b.write(1);
        assert_eq!(b.drop_count(), 0);
        {
            let c = b.map(|i| i + 10);
            assert_eq!(c.oldest_ordered().cloned().collect::<Vec<_>>(), vec![11]);
            assert_eq!(c.drop_count(), 0);
        }

        b.write(2);
        assert_eq!(b.drop_count(), 0);
        {
            let c = b.map(|i| i + 10);
            assert_eq!(
                c.oldest_ordered().cloned().collect::<Vec<_>>(),
                vec![11, 12]
            );
            assert_eq!(c.drop_count(), 0);
        }

        b.write(3);
        assert_eq!(b.drop_count(), 1);
        {
            let c = b.map(|i| i + 10);
            assert_eq!(
                c.oldest_ordered().cloned().collect::<Vec<_>>(),
                vec![12, 13]
            );
            assert_eq!(c.drop_count(), 1);
        }
    }
}
