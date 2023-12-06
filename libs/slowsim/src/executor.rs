use std::{thread::JoinHandle, cell::{OnceCell, RefCell}, sync::{mpsc, Arc, atomic::{AtomicU8, Ordering}}};

use tracing::debug;

use crate::time::Timing;

/// Stores status of the running threads. Threads are registered in the runtime upon creation
/// and deregistered upon termination.
pub struct Runtime {
    // stores handles to all threads that are currently running
    threads: Vec<ThreadHandle>,
    // stores current time and pending wakeups
    clock: Arc<Timing>,
}

impl Runtime {
    /// Init new runtime. Virtual time is 0ms, no running threads.
    pub fn new() -> Self {
        Self {
            threads: Vec::new(),
            clock: Arc::new(Timing::new()),
        }
    }

    /// Spawn a new thread and register it in the runtime.
    pub fn spawn<F>(&mut self, f: F)
    where
        F: FnOnce() + Send + 'static,
    {
        let (tx, rx) = mpsc::channel();

        let clock = self.clock.clone();
        let join = std::thread::spawn(move || {
            with_thread_context(|ctx| {
                ctx.clock.set(clock);
                tx.send(ctx.clone()).expect("failed to send thread context");
                // suspend thread to put it to `threads` in sleeping state
                ctx.yield_me(0);
            });
            f()
        });

        let ctx = rx.recv().expect("failed to receive thread context");
        let handle = ThreadHandle::new(ctx, join);

        self.threads.push(handle);
    }

    pub fn step(&mut self) -> bool {
        let mut ran = false;
        self.threads.retain(|thread: &ThreadHandle| {
            let res = thread.ctx.wakeup.compare_exchange(PENDING_WAKEUP, NO_WAKEUP, Ordering::SeqCst, Ordering::SeqCst);
            if !res.is_ok() {
                return true
            }
            ran = true;

            let status = thread.step();
            if status == Status::Sleep {
                true
            } else {
                debug!("thread {} has finished", thread);
                false
            }
        });

        if !ran {
            if let Some(ctx_to_wake) = self.clock.step() {
                ctx_to_wake.inc_wake();
            } else {
                return false;
            }
        }

        true
    }
}

/// [`Runtime`] stores [`ThreadHandle`] for each thread which is currently running.
struct ThreadHandle {
    ctx: Arc<ThreadContext>,
    join: JoinHandle<()>,
}

impl ThreadHandle {
    /// Create a new [`ThreadHandle`] and wait until thread will enter [`Status::Sleep`] state.
    fn new(ctx: Arc<ThreadContext>, join: JoinHandle<()>) -> Self {
        let mut status = ctx.mutex.lock();
        // wait until thread will go into the first yield
        while *status != Status::Sleep {
            ctx.condvar.wait(&mut status);
        }
        drop(status);

        Self { ctx, join }
    }

    /// Allows thread to execute one step of its execution.
    /// Returns [`Status`] of the thread after the step.
    fn step(&self) -> Status {
        let mut status = self.ctx.mutex.lock();
        assert!(matches!(*status, Status::Sleep));

        *status = Status::Running;
        self.ctx.condvar.notify_all();

        while *status == Status::Running {
            self.ctx.condvar.wait(&mut status);
        }
        
        *status
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Status {
    /// Thread is running.
    Running,
    /// Waiting for event to complete, will be woken up by the event.
    Sleep,
    /// Thread finished execution.
    Finished,
}

const NO_WAKEUP: u8 = 0;
const PENDING_WAKEUP: u8 = 1;

pub struct ThreadContext {
    // used to block thread until it is woken up
    mutex: parking_lot::Mutex<Status>,
    condvar: parking_lot::Condvar,

    // used as a flag to indicate runtime that thread is ready to be woken up
    wakeup: AtomicU8,

    clock: OnceCell<Arc<Timing>>,
}

impl ThreadContext {
    fn new() -> Self {
        Self {
            mutex: parking_lot::Mutex::new(Status::Running),
            condvar: parking_lot::Condvar::new(),
            wakeup: AtomicU8::new(NO_WAKEUP),
            clock: OnceCell::new(),
        }
    }
}

// Functions for executor to control thread execution.
impl ThreadContext {
    /// Set atomic flag to indicate that thread is ready to be woken up.
    fn inc_wake(&self) {
        self.wakeup.store(1, Ordering::SeqCst);
    }
}

// Internal functions.
impl ThreadContext {
    /// Blocks thread until it's woken up by the executor. If `after_ms` is 0, is will be
    /// worken on the next step. If `after_ms` > 0, wakeup is scheduled after that time.
    /// Otherwise wakeup is not scheduled inside `yield_me`, and should be arranged before
    /// calling this function.
    fn yield_me(self: &Arc<Self>, after_ms: i64) {
        let mut status = self.mutex.lock();
        assert!(matches!(*status, Status::Running));

        if after_ms == 0 {
            // tell executor that we are ready to be woken up
            self.inc_wake();
        } else if after_ms > 0 {
            // schedule wakeup
            self.clock.get().unwrap().schedule_wakeup(after_ms as u64, self.clone());
        }
        *status = Status::Sleep;
        self.condvar.notify_all();

        // wait until executor wakes us up
        while *status != Status::Running {
            self.condvar.wait(&mut status);
        }
    }

    fn finish_me(&self) {
        let mut status = self.mutex.lock();
        assert!(matches!(*status, Status::Running));

        *status = Status::Finished;
        self.condvar.notify_all();
    }
}

/// Invokes the given closure with a reference to the current thread [`ThreadContext`].
#[inline(always)]
fn with_thread_context<T>(f: impl FnOnce(&Arc<ThreadContext>) -> T) -> T {
    thread_local!(static THREAD_DATA: Arc<ThreadContext> = Arc::new(ThreadContext::new()));
    THREAD_DATA.
    THREAD_DATA.with(f)
}

/// Waker is used to wake up threads that are blocked on condition.
/// It keeps track of contexts [`Arc<ThreadContext>`] and can increment the counter
/// of several contexts to send a notification.
pub struct Waker {
    // contexts that are waiting for a notification
    contexts: parking_lot::Mutex<smallvec::SmallVec<[Arc<ThreadContext>; 8]>>,
}

impl Waker {
    pub fn new() -> Self {
        Self {
            contexts: parking_lot::Mutex::new(smallvec::SmallVec::new()),
        }
    }

    /// Subscribe current thread to receive a wake notification later.
    pub fn wake_me_later(&self) {
        with_thread_context(|ctx| {
            self.contexts.lock().push(ctx.clone());
        });
    }

    /// Wake up all threads that are waiting for a notification and clear the list.
    pub fn wake_all(&self) {
        let mut v = self.contexts.lock();
        for ctx in v.iter() {
            ctx.inc_wake();
        }
        v.clear();
    }
}

/// See [`ThreadContext::yield_me`].
pub fn yield_me(after_ms: i64) {
    with_thread_context(|ctx| {
        ctx.yield_me(after_ms)
    })
}

/// Get current time.
pub fn now() -> u64 {
    with_thread_context(|ctx| {
        ctx.clock.get().unwrap().now()
    })
}

/// Trait for polling channels until they have something.
pub trait PollSome {
    /// Schedule wakeup for message arrival.
    fn wake_me(&self);

    /// Check if channel has a ready message.
    fn has_some(&self) -> bool;
}

/// Blocks current thread until one of the channels has a ready message.
pub fn epoll_chans(chans: &[dyn PollSome], timeout: i64) -> Option<usize> {
    let deadline = now() + timeout as u64;

    loop {
        for chan in chans {
            chan.wake_me()
        }

        for (i, chan) in chans.iter().enumerate() {
            if chan.has_some() {
                return Some(i);
            }
        }

        if timeout < 0 {
            // block until wakeup
            yield_me(-1);
        } else {
            let current_time = now();
            if current_time >= deadline {
                return None;
            }

            yield_me(deadline - current_time);
        }
    }
}
