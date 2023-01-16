//! Non-shared synchronization primitives.

/// Heap-based "wakeup correct thread" helper.
///
/// This is used to synchronize access to a ticketed resource, which should avoid waking up threads
/// when it's not their turn, as would happen with mutex and condvar. On Linux this could be
/// replaced with direct futex usage but there is a desire to be portable.
#[derive(Default, Debug)]
pub struct UnparkInOrder(std::collections::BinaryHeap<HeapEntry>);

#[derive(Debug)]
struct HeapEntry(std::cmp::Reverse<u32>, std::thread::Thread);

impl PartialEq for HeapEntry {
    fn eq(&self, other: &Self) -> bool {
        self.0 == other.0 && self.1.id() == other.1.id()
    }
}

impl Eq for HeapEntry {}

impl PartialOrd for HeapEntry {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for HeapEntry {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.0.cmp(&other.0)
    }
}

impl From<(u32, std::thread::Thread)> for HeapEntry {
    fn from(value: (u32, std::thread::Thread)) -> Self {
        HeapEntry(std::cmp::Reverse(value.0), value.1)
    }
}

impl UnparkInOrder {
    /// Store current thread with the ticket number.
    pub fn store_current(&mut self, id: u32) {
        self.0.push(HeapEntry::from((id, std::thread::current())));
    }

    /// Returns true if the current thread with the ticket is the first one or frontmost.
    pub fn current_is_front(&self, expected_id: u32) -> bool {
        let ret = match self.0.peek() {
            Some(HeapEntry(id, first)) => {
                let cur = std::thread::current();
                id.0 == expected_id && cur.id() == first.id()
            }
            None => false,
        };
        ret
    }

    /// Pops the frontmost value.
    ///
    /// ## Panics
    ///
    /// If the frontmost is not the current thread, or when `!current_is_front()`.
    pub fn pop_front(&mut self, expected_id: u32) -> u32 {
        use std::collections::binary_heap::PeekMut;

        let cur = std::thread::current();
        let next = self.0.peek_mut();
        let next = next.expect("should not be empty because we were just unparked");
        let t = &next.1;
        let id = next.0 .0;
        assert_eq!(cur.id(), t.id());
        assert_eq!(id, expected_id);

        PeekMut::<'_, HeapEntry>::pop(next);

        id
    }

    /// Unpark the frontmost value.
    ///
    /// Does not remove the frontmost element, only unparks or wakes it up.
    pub fn unpark_front(&self, turn: u32) {
        match self.0.peek() {
            Some(HeapEntry(id, t)) if id.0 == turn => {
                t.unpark();
            }
            Some(_) | None => {
                // Not an error, the thread we are hoping to wakeup just hasn't yet arrived to the
                // parking lot.
            }
        }
    }

    /// Park the current thread while it's not it's turn.
    ///
    /// This assumes that the current thread's [`std::thread::Thread::unpark`] will be called, most
    /// likely by the [`unpark_front`] method.
    pub fn park_while<'a, T, F>(
        mut guard: std::sync::MutexGuard<'a, T>,
        consumer: &'a std::sync::Mutex<T>,
        mut cond: F,
    ) -> std::sync::MutexGuard<'a, T>
    where
        F: FnMut(&mut T) -> bool,
    {
        while cond(&mut *guard) {
            drop(guard);
            std::thread::park();
            guard = consumer.lock().unwrap();
        }
        guard
    }
}

#[test]
fn unparks_in_order() {
    let mut uio = UnparkInOrder::default();
    uio.store_current(0);
    assert!(uio.current_is_front(0));
    uio.pop_front(0);
    uio.unpark_front(1); // there is no front() right now

    uio.store_current(2);
    uio.store_current(1);
    assert!(uio.current_is_front(1));
    uio.pop_front(1);
    uio.unpark_front(2); // unparking 2 => ThreadId(11)
    uio.store_current(3);
}
