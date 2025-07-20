//! A vendoring of `thread_local` 1.1.9 with some changes needed for TLS destructors

// Copyright 2017 Amanieu d'Antras
//
// Licensed under the Apache License, Version 2.0, <LICENSE-APACHE or
// http://apache.org/licenses/LICENSE-2.0> or the MIT license <LICENSE-MIT or
// http://opensource.org/licenses/MIT>, at your option. This file may not be
// copied, modified, or distributed except according to those terms.

use std::cell::UnsafeCell;
use std::fmt;
use std::iter::FusedIterator;
use std::mem::MaybeUninit;
use std::panic::UnwindSafe;
use std::ptr;
use std::sync::atomic::{AtomicBool, AtomicPtr, AtomicUsize, Ordering};

// Use usize::BITS once it has stabilized and the MSRV has been bumped.
#[cfg(target_pointer_width = "16")]
const POINTER_WIDTH: u8 = 16;
#[cfg(target_pointer_width = "32")]
const POINTER_WIDTH: u8 = 32;
#[cfg(target_pointer_width = "64")]
const POINTER_WIDTH: u8 = 64;

/// The total number of buckets stored in each thread local.
/// All buckets combined can hold up to `usize::MAX - 1` entries.
const BUCKETS: usize = (POINTER_WIDTH - 1) as usize;

/// Thread-local variable wrapper
///
/// See the [module-level documentation](index.html) for more.
pub(crate) struct ThreadLocal<T: Send> {
    /// The buckets in the thread local. The nth bucket contains `2^n`
    /// elements. Each bucket is lazily allocated.
    buckets: [AtomicPtr<Entry<T>>; BUCKETS],

    /// The number of values in the thread local. This can be less than the real number of values,
    /// but is never more.
    values: AtomicUsize,
}

struct Entry<T> {
    present: AtomicBool,
    value: UnsafeCell<MaybeUninit<T>>,
}

impl<T> Drop for Entry<T> {
    fn drop(&mut self) {
        if *self.present.get_mut() {
            // safety: If present is true, then this value is allocated
            // and we cannot touch it ever again after this function
            unsafe { ptr::drop_in_place((*self.value.get()).as_mut_ptr()) }
        }
    }
}

// Safety: ThreadLocal is always Sync, even if T isn't
unsafe impl<T: Send> Sync for ThreadLocal<T> {}

impl<T: Send> Default for ThreadLocal<T> {
    fn default() -> ThreadLocal<T> {
        ThreadLocal::new()
    }
}

impl<T: Send> Drop for ThreadLocal<T> {
    fn drop(&mut self) {
        // Free each non-null bucket
        for (i, bucket) in self.buckets.iter_mut().enumerate() {
            let bucket_ptr = *bucket.get_mut();

            let this_bucket_size = 1 << i;

            if bucket_ptr.is_null() {
                continue;
            }

            // Safety: the bucket_ptr is allocated and the bucket size is correct.
            unsafe { deallocate_bucket(bucket_ptr, this_bucket_size) };
        }
    }
}

impl<T: Send> ThreadLocal<T> {
    /// Creates a new empty `ThreadLocal`.
    pub const fn new() -> ThreadLocal<T> {
        Self {
            buckets: [const { AtomicPtr::new(ptr::null_mut()) }; BUCKETS],
            values: AtomicUsize::new(0),
        }
    }

    /// Returns the element for the current thread, if it exists.
    pub fn get(&self) -> Option<&T> {
        thread_id::get().and_then(|t| self.get_inner(t))
    }

    /// Returns the element for the current thread, or creates it if it doesn't
    /// exist.
    pub fn get_or<F>(&self, create: F) -> &T
    where
        F: FnOnce() -> T,
    {
        let thread = thread_id::get_or_init();
        if let Some(val) = self.get_inner(thread) {
            return val;
        }

        self.insert(thread, create())
    }

    fn get_inner(&self, thread: thread_id::Thread) -> Option<&T> {
        // Safety: There would need to be isize::MAX+1 threads for the bucket to overflow.
        let bucket_ptr = unsafe { self.buckets.get_unchecked(thread.bucket) };
        let bucket_ptr = bucket_ptr.load(Ordering::Acquire);
        if bucket_ptr.is_null() {
            return None;
        }
        // Safety: the bucket always has enough capacity for this index.
        // Safety: If present is true, then this entry is allocated and it is safe to read.
        unsafe {
            let entry = &*bucket_ptr.add(thread.index);
            if entry.present.load(Ordering::Relaxed) {
                Some(&*(&*entry.value.get()).as_ptr())
            } else {
                None
            }
        }
    }

    #[cold]
    fn insert(&self, thread: thread_id::Thread, data: T) -> &T {
        // Safety: There would need to be isize::MAX+1 threads for the bucket to overflow.
        let bucket_atomic_ptr = unsafe { self.buckets.get_unchecked(thread.bucket) };
        let bucket_ptr: *const _ = bucket_atomic_ptr.load(Ordering::Acquire);

        // If the bucket doesn't already exist, we need to allocate it
        let bucket_ptr = if bucket_ptr.is_null() {
            let new_bucket = allocate_bucket(thread.bucket_size());

            match bucket_atomic_ptr.compare_exchange(
                ptr::null_mut(),
                new_bucket,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => new_bucket,
                // If the bucket value changed (from null), that means
                // another thread stored a new bucket before we could,
                // and we can free our bucket and use that one instead
                Err(bucket_ptr) => {
                    // Safety: the bucket_ptr is allocated and the bucket size is correct.
                    unsafe { deallocate_bucket(new_bucket, thread.bucket_size()) }
                    bucket_ptr
                }
            }
        } else {
            bucket_ptr
        };

        // Insert the new element into the bucket
        // Safety: the bucket always has enough capacity for this index.
        let entry = unsafe { &*bucket_ptr.add(thread.index) };
        let value_ptr = entry.value.get();
        // Safety: present is false, so no other threads will be reading this,
        // and it is owned by our thread, so no other threads will be writing to this.
        unsafe { value_ptr.write(MaybeUninit::new(data)) };
        entry.present.store(true, Ordering::Release);

        self.values.fetch_add(1, Ordering::Release);

        // Safety: present is true, so it is now safe to read.
        unsafe { &*(&*value_ptr).as_ptr() }
    }

    /// Returns an iterator over the local values of all threads in unspecified
    /// order.
    ///
    /// This call can be done safely, as `T` is required to implement [`Sync`].
    pub fn iter(&self) -> Iter<'_, T>
    where
        T: Sync,
    {
        Iter {
            thread_local: self,
            yielded: 0,
            bucket: 0,
            bucket_size: 1,
            index: 0,
        }
    }
}

impl<'a, T: Send + Sync> IntoIterator for &'a ThreadLocal<T> {
    type Item = &'a T;
    type IntoIter = Iter<'a, T>;

    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

impl<T: Send + fmt::Debug> fmt::Debug for ThreadLocal<T> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "ThreadLocal {{ local_data: {:?} }}", self.get())
    }
}

impl<T: Send + UnwindSafe> UnwindSafe for ThreadLocal<T> {}

/// Iterator over the contents of a `ThreadLocal`.
#[derive(Debug)]
pub struct Iter<'a, T: Send + Sync> {
    thread_local: &'a ThreadLocal<T>,
    yielded: usize,
    bucket: usize,
    bucket_size: usize,
    index: usize,
}

impl<'a, T: Send + Sync> Iterator for Iter<'a, T> {
    type Item = &'a T;
    fn next(&mut self) -> Option<Self::Item> {
        while self.bucket < BUCKETS {
            let bucket = self.thread_local.buckets[self.bucket].load(Ordering::Acquire);

            if !bucket.is_null() {
                while self.index < self.bucket_size {
                    // Safety: the bucket always has enough capacity for this index.
                    let entry = unsafe { &*bucket.add(self.index) };
                    self.index += 1;
                    if entry.present.load(Ordering::Acquire) {
                        self.yielded += 1;
                        // Safety: If present is true, then this entry is allocated and it is safe to read.
                        return Some(unsafe { &*(&*entry.value.get()).as_ptr() });
                    }
                }
            }

            self.bucket_size <<= 1;
            self.bucket += 1;
            self.index = 0;
        }

        None
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let total = self.thread_local.values.load(Ordering::Acquire);
        (total - self.yielded, None)
    }
}
impl<T: Send + Sync> FusedIterator for Iter<'_, T> {}

fn allocate_bucket<T>(size: usize) -> *mut Entry<T> {
    Box::into_raw(
        (0..size)
            .map(|_| Entry::<T> {
                present: AtomicBool::new(false),
                value: UnsafeCell::new(MaybeUninit::uninit()),
            })
            .collect(),
    ) as *mut _
}

unsafe fn deallocate_bucket<T>(bucket: *mut Entry<T>, size: usize) {
    // Safety: caller must guarantee that bucket is allocated and of the correct size.
    let _ = unsafe { Box::from_raw(std::slice::from_raw_parts_mut(bucket, size)) };
}

mod thread_id {
    // Copyright 2017 Amanieu d'Antras
    //
    // Licensed under the Apache License, Version 2.0, <LICENSE-APACHE or
    // http://apache.org/licenses/LICENSE-2.0> or the MIT license <LICENSE-MIT or
    // http://opensource.org/licenses/MIT>, at your option. This file may not be
    // copied, modified, or distributed except according to those terms.

    use super::POINTER_WIDTH;
    use std::cell::Cell;
    use std::cmp::Reverse;
    use std::collections::BinaryHeap;
    use std::sync::Mutex;

    /// Thread ID manager which allocates thread IDs. It attempts to aggressively
    /// reuse thread IDs where possible to avoid cases where a ThreadLocal grows
    /// indefinitely when it is used by many short-lived threads.
    struct ThreadIdManager {
        free_from: usize,
        free_list: Option<BinaryHeap<Reverse<usize>>>,
    }

    impl ThreadIdManager {
        const fn new() -> Self {
            Self {
                free_from: 0,
                free_list: None,
            }
        }

        fn alloc(&mut self) -> usize {
            if let Some(id) = self.free_list.as_mut().and_then(|heap| heap.pop()) {
                id.0
            } else {
                // `free_from` can't overflow as each thread takes up at least 2 bytes of memory and
                // thus we can't even have `usize::MAX / 2 + 1` threads.

                let id = self.free_from;
                self.free_from += 1;
                id
            }
        }

        fn free(&mut self, id: usize) {
            self.free_list
                .get_or_insert_with(BinaryHeap::new)
                .push(Reverse(id));
        }
    }

    static THREAD_ID_MANAGER: Mutex<ThreadIdManager> = Mutex::new(ThreadIdManager::new());

    /// Data which is unique to the current thread while it is running.
    /// A thread ID may be reused after a thread exits.
    #[derive(Clone, Copy)]
    pub(super) struct Thread {
        /// The bucket this thread's local storage will be in.
        pub(super) bucket: usize,
        /// The index into the bucket this thread's local storage is in.
        pub(super) index: usize,
    }

    impl Thread {
        pub(super) fn new(id: usize) -> Self {
            let bucket = usize::from(POINTER_WIDTH) - ((id + 1).leading_zeros() as usize) - 1;
            let bucket_size = 1 << bucket;
            let index = id - (bucket_size - 1);

            Self { bucket, index }
        }

        /// The size of the bucket this thread's local storage will be in.
        pub(super) fn bucket_size(&self) -> usize {
            1 << self.bucket
        }
    }

    // This is split into 2 thread-local variables so that we can check whether the
    // thread is initialized without having to register a thread-local destructor.
    //
    // This makes the fast path smaller, and it is necessary for GlobalAlloc as we are not allowed
    // to use thread locals with destructors during alloc.
    thread_local! { static THREAD: Cell<Option<Thread>> = const { Cell::new(None) }; }
    thread_local! { static THREAD_GUARD: ThreadGuard = const { ThreadGuard { id: Cell::new(0) } }; }

    // Guard to ensure the thread ID is released on thread exit.
    struct ThreadGuard {
        // We keep a copy of the thread ID in the ThreadGuard: we can't
        // reliably access THREAD in our Drop impl due to the unpredictable
        // order of TLS destructors.
        id: Cell<usize>,
    }

    impl Drop for ThreadGuard {
        fn drop(&mut self) {
            // Release the thread ID. Any further accesses to the thread ID
            // will go through get_slow which will either panic or
            // initialize a new ThreadGuard.
            let _ = THREAD.try_with(|thread| thread.set(None));
            THREAD_ID_MANAGER.lock().unwrap().free(self.id.get());
        }
    }

    /// Returns a thread ID for the current thread.
    #[inline]
    pub(crate) fn get() -> Option<Thread> {
        THREAD.with(|thread| thread.get())
    }

    /// Returns a thread ID for the current thread, allocating one if needed.
    #[inline]
    pub(crate) fn get_or_init() -> Thread {
        THREAD.with(|thread| {
            if let Some(thread) = thread.get() {
                thread
            } else {
                get_slow(thread)
            }
        })
    }

    /// Out-of-line slow path for allocating a thread ID.
    #[cold]
    fn get_slow(thread: &Cell<Option<Thread>>) -> Thread {
        let id = THREAD_ID_MANAGER.lock().unwrap().alloc();
        let new = Thread::new(id);
        thread.set(Some(new));
        THREAD_GUARD.with(|guard| guard.id.set(id));
        new
    }

    #[test]
    fn test_thread() {
        let thread = Thread::new(0);
        assert_eq!(thread.bucket, 0);
        assert_eq!(thread.bucket_size(), 1);
        assert_eq!(thread.index, 0);

        let thread = Thread::new(1);
        assert_eq!(thread.bucket, 1);
        assert_eq!(thread.bucket_size(), 2);
        assert_eq!(thread.index, 0);

        let thread = Thread::new(2);
        assert_eq!(thread.bucket, 1);
        assert_eq!(thread.bucket_size(), 2);
        assert_eq!(thread.index, 1);

        let thread = Thread::new(3);
        assert_eq!(thread.bucket, 2);
        assert_eq!(thread.bucket_size(), 4);
        assert_eq!(thread.index, 0);

        let thread = Thread::new(19);
        assert_eq!(thread.bucket, 4);
        assert_eq!(thread.bucket_size(), 16);
        assert_eq!(thread.index, 4);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::cell::RefCell;
    use std::sync::Arc;
    use std::sync::atomic::AtomicUsize;
    use std::sync::atomic::Ordering::Relaxed;
    use std::thread;

    fn make_create() -> Arc<dyn Fn() -> usize + Send + Sync> {
        let count = AtomicUsize::new(0);
        Arc::new(move || count.fetch_add(1, Relaxed))
    }

    #[test]
    fn same_thread() {
        let create = make_create();
        let tls = ThreadLocal::new();
        assert_eq!(None, tls.get());
        assert_eq!("ThreadLocal { local_data: None }", format!("{:?}", &tls));
        assert_eq!(0, *tls.get_or(|| create()));
        assert_eq!(Some(&0), tls.get());
        assert_eq!(0, *tls.get_or(|| create()));
        assert_eq!(Some(&0), tls.get());
        assert_eq!(0, *tls.get_or(|| create()));
        assert_eq!(Some(&0), tls.get());
        assert_eq!("ThreadLocal { local_data: Some(0) }", format!("{:?}", &tls));
        // tls.clear();
        // assert_eq!(None, tls.get());
    }

    #[test]
    fn different_thread() {
        let create = make_create();
        let tls = Arc::new(ThreadLocal::new());
        assert_eq!(None, tls.get());
        assert_eq!(0, *tls.get_or(|| create()));
        assert_eq!(Some(&0), tls.get());

        let tls2 = tls.clone();
        let create2 = create.clone();
        thread::spawn(move || {
            assert_eq!(None, tls2.get());
            assert_eq!(1, *tls2.get_or(|| create2()));
            assert_eq!(Some(&1), tls2.get());
        })
        .join()
        .unwrap();

        assert_eq!(Some(&0), tls.get());
        assert_eq!(0, *tls.get_or(|| create()));
    }

    #[test]
    fn iter() {
        let tls = Arc::new(ThreadLocal::new());
        tls.get_or(|| Box::new(1));

        let tls2 = tls.clone();
        thread::spawn(move || {
            tls2.get_or(|| Box::new(2));
            let tls3 = tls2.clone();
            thread::spawn(move || {
                tls3.get_or(|| Box::new(3));
            })
            .join()
            .unwrap();
            drop(tls2);
        })
        .join()
        .unwrap();

        let tls = Arc::try_unwrap(tls).unwrap();

        let mut v = tls.iter().map(|x| **x).collect::<Vec<i32>>();
        v.sort_unstable();
        assert_eq!(vec![1, 2, 3], v);
    }

    #[test]
    fn miri_iter_soundness_check() {
        let tls = Arc::new(ThreadLocal::new());
        let _local = tls.get_or(|| Box::new(1));

        let tls2 = tls.clone();
        let join_1 = thread::spawn(move || {
            let _tls = tls2.get_or(|| Box::new(2));
            let iter = tls2.iter();
            for item in iter {
                println!("{item:?}");
            }
        });

        let iter = tls.iter();
        for item in iter {
            println!("{item:?}");
        }

        join_1.join().ok();
    }

    #[test]
    fn test_drop() {
        let local = ThreadLocal::new();
        struct Dropped(Arc<AtomicUsize>);
        impl Drop for Dropped {
            fn drop(&mut self) {
                self.0.fetch_add(1, Relaxed);
            }
        }

        let dropped = Arc::new(AtomicUsize::new(0));
        local.get_or(|| Dropped(dropped.clone()));
        assert_eq!(dropped.load(Relaxed), 0);
        drop(local);
        assert_eq!(dropped.load(Relaxed), 1);
    }

    #[test]
    fn test_earlyreturn_buckets() {
        struct Dropped(Arc<AtomicUsize>);
        impl Drop for Dropped {
            fn drop(&mut self) {
                self.0.fetch_add(1, Relaxed);
            }
        }
        let dropped = Arc::new(AtomicUsize::new(0));

        // We use a high `id` here to guarantee that a lazily allocated bucket somewhere in the middle is used.
        // Neither iteration nor `Drop` must early-return on `null` buckets that are used for lower `buckets`.
        let thread = thread_id::Thread::new(1234);
        assert!(thread.bucket > 1);

        let local = ThreadLocal::new();
        local.insert(thread, Dropped(dropped.clone()));

        let item = local.iter().next().unwrap();
        assert_eq!(item.0.load(Relaxed), 0);
        drop(local);
        assert_eq!(dropped.load(Relaxed), 1);
    }

    #[test]
    fn is_sync() {
        fn foo<T: Sync>() {}
        foo::<ThreadLocal<String>>();
        foo::<ThreadLocal<RefCell<String>>>();
    }
}
