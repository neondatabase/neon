use std::{
    panic::AssertUnwindSafe,
    sync::{
        atomic::{AtomicBool, AtomicU32, AtomicU8, Ordering},
        mpsc, Arc, OnceLock,
    },
    thread::JoinHandle,
};

use tracing::{debug, error, trace};

use crate::time::Timing;

/// Stores status of the running threads. Threads are registered in the runtime upon creation
/// and deregistered upon termination.
pub struct Runtime {
    // stores handles to all threads that are currently running
    threads: Vec<ThreadHandle>,
    // stores current time and pending wakeups
    clock: Arc<Timing>,
    // thread counter
    thread_counter: AtomicU32,
    // Thread step counter -- how many times all threads has been actually
    // stepped (note that all world/time/executor/thread have slightly different
    // meaning of steps). For observability.
    pub step_counter: u64,
}

impl Runtime {
    /// Init new runtime, no running threads.
    pub fn new(clock: Arc<Timing>) -> Self {
        Self {
            threads: Vec::new(),
            clock,
            thread_counter: AtomicU32::new(0),
            step_counter: 0,
        }
    }

    /// Spawn a new thread and register it in the runtime.
    pub fn spawn<F>(&mut self, f: F) -> ExternalHandle
    where
        F: FnOnce() + Send + 'static,
    {
        let (tx, rx) = mpsc::channel();

        let clock = self.clock.clone();
        let tid = self.thread_counter.fetch_add(1, Ordering::SeqCst);
        debug!("spawning thread-{}", tid);

        let join = std::thread::spawn(move || {
            let _guard = tracing::info_span!("", tid).entered();

            let res = std::panic::catch_unwind(AssertUnwindSafe(|| {
                with_thread_context(|ctx| {
                    assert!(ctx.clock.set(clock).is_ok());
                    ctx.id.store(tid, Ordering::SeqCst);
                    tx.send(ctx.clone()).expect("failed to send thread context");
                    // suspend thread to put it to `threads` in sleeping state
                    ctx.yield_me(0);
                });

                // start user-provided function
                f();
            }));
            debug!("thread finished");

            if let Err(e) = res {
                with_thread_context(|ctx| {
                    if !ctx.allow_panic.load(std::sync::atomic::Ordering::SeqCst) {
                        error!("thread panicked, terminating the process: {:?}", e);
                        std::process::exit(1);
                    }

                    debug!("thread panicked: {:?}", e);
                    let mut result = ctx.result.lock();
                    if result.0 == -1 {
                        *result = (256, format!("thread panicked: {:?}", e));
                    }
                });
            }

            with_thread_context(|ctx| {
                ctx.finish_me();
            });
        });

        let ctx = rx.recv().expect("failed to receive thread context");
        let handle = ThreadHandle::new(ctx.clone(), join);

        self.threads.push(handle);

        ExternalHandle { ctx }
    }

    /// Returns true if there are any unfinished activity, such as running thread or pending events.
    /// Otherwise returns false, which means all threads are blocked forever.
    pub fn step(&mut self) -> bool {
        trace!("runtime step");

        // have we run any thread?
        let mut ran = false;

        self.threads.retain(|thread: &ThreadHandle| {
            let res = thread.ctx.wakeup.compare_exchange(
                PENDING_WAKEUP,
                NO_WAKEUP,
                Ordering::SeqCst,
                Ordering::SeqCst,
            );
            if res.is_err() {
                // thread has no pending wakeups, leaving as is
                return true;
            }
            ran = true;

            trace!("entering thread-{}", thread.ctx.tid());
            let status = thread.step();
            self.step_counter += 1;
            trace!(
                "out of thread-{} with status {:?}",
                thread.ctx.tid(),
                status
            );

            if status == Status::Sleep {
                true
            } else {
                trace!("thread has finished");
                // removing the thread from the list
                false
            }
        });

        if !ran {
            trace!("no threads were run, stepping clock");
            if let Some(ctx_to_wake) = self.clock.step() {
                trace!("waking up thread-{}", ctx_to_wake.tid());
                ctx_to_wake.inc_wake();
            } else {
                return false;
            }
        }

        true
    }

    /// Kill all threads. This is done by setting a flag in each thread context and waking it up.
    pub fn crash_all_threads(&mut self) {
        for thread in self.threads.iter() {
            thread.ctx.crash_stop();
        }

        // all threads should be finished after a few steps
        while !self.threads.is_empty() {
            self.step();
        }
    }
}

impl Drop for Runtime {
    fn drop(&mut self) {
        debug!("dropping the runtime");
        self.crash_all_threads();
    }
}

#[derive(Clone)]
pub struct ExternalHandle {
    ctx: Arc<ThreadContext>,
}

impl ExternalHandle {
    /// Returns true if thread has finished execution.
    pub fn is_finished(&self) -> bool {
        let status = self.ctx.mutex.lock();
        *status == Status::Finished
    }

    /// Returns exitcode and message, which is available after thread has finished execution.
    pub fn result(&self) -> (i32, String) {
        let result = self.ctx.result.lock();
        result.clone()
    }

    /// Returns thread id.
    pub fn id(&self) -> u32 {
        self.ctx.id.load(Ordering::SeqCst)
    }

    /// Sets a flag to crash thread on the next wakeup.
    pub fn crash_stop(&self) {
        self.ctx.crash_stop();
    }
}

struct ThreadHandle {
    ctx: Arc<ThreadContext>,
    _join: JoinHandle<()>,
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

        Self { ctx, _join: join }
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
    /// Waiting for event to complete, will be resumed by the executor step, once wakeup flag is set.
    Sleep,
    /// Thread finished execution.
    Finished,
}

const NO_WAKEUP: u8 = 0;
const PENDING_WAKEUP: u8 = 1;

pub struct ThreadContext {
    id: AtomicU32,
    // used to block thread until it is woken up
    mutex: parking_lot::Mutex<Status>,
    condvar: parking_lot::Condvar,
    // used as a flag to indicate runtime that thread is ready to be woken up
    wakeup: AtomicU8,
    clock: OnceLock<Arc<Timing>>,
    // execution result, set by exit() call
    result: parking_lot::Mutex<(i32, String)>,
    // determines if process should be killed on receiving panic
    allow_panic: AtomicBool,
    // acts as a signal that thread should crash itself on the next wakeup
    crash_request: AtomicBool,
}

impl ThreadContext {
    pub(crate) fn new() -> Self {
        Self {
            id: AtomicU32::new(0),
            mutex: parking_lot::Mutex::new(Status::Running),
            condvar: parking_lot::Condvar::new(),
            wakeup: AtomicU8::new(NO_WAKEUP),
            clock: OnceLock::new(),
            result: parking_lot::Mutex::new((-1, String::new())),
            allow_panic: AtomicBool::new(false),
            crash_request: AtomicBool::new(false),
        }
    }
}

// Functions for executor to control thread execution.
impl ThreadContext {
    /// Set atomic flag to indicate that thread is ready to be woken up.
    fn inc_wake(&self) {
        self.wakeup.store(PENDING_WAKEUP, Ordering::SeqCst);
    }

    /// Internal function used for event queues.
    pub(crate) fn schedule_wakeup(self: &Arc<Self>, after_ms: u64) {
        self.clock
            .get()
            .unwrap()
            .schedule_wakeup(after_ms, self.clone());
    }

    fn tid(&self) -> u32 {
        self.id.load(Ordering::SeqCst)
    }

    fn crash_stop(&self) {
        let status = self.mutex.lock();
        if *status == Status::Finished {
            debug!(
                "trying to crash thread-{}, which is already finished",
                self.tid()
            );
            return;
        }
        assert!(matches!(*status, Status::Sleep));
        drop(status);

        self.allow_panic.store(true, Ordering::SeqCst);
        self.crash_request.store(true, Ordering::SeqCst);
        // set a wakeup
        self.inc_wake();
        // it will panic on the next wakeup
    }
}

// Internal functions.
impl ThreadContext {
    /// Blocks thread until it's woken up by the executor. If `after_ms` is 0, is will be
    /// woken on the next step. If `after_ms` > 0, wakeup is scheduled after that time.
    /// Otherwise wakeup is not scheduled inside `yield_me`, and should be arranged before
    /// calling this function.
    fn yield_me(self: &Arc<Self>, after_ms: i64) {
        let mut status = self.mutex.lock();
        assert!(matches!(*status, Status::Running));

        match after_ms.cmp(&0) {
            std::cmp::Ordering::Less => {
                // block until something wakes us up
            }
            std::cmp::Ordering::Equal => {
                // tell executor that we are ready to be woken up
                self.inc_wake();
            }
            std::cmp::Ordering::Greater => {
                // schedule wakeup
                self.clock
                    .get()
                    .unwrap()
                    .schedule_wakeup(after_ms as u64, self.clone());
            }
        }

        *status = Status::Sleep;
        self.condvar.notify_all();

        // wait until executor wakes us up
        while *status != Status::Running {
            self.condvar.wait(&mut status);
        }

        if self.crash_request.load(Ordering::SeqCst) {
            panic!("crashed by request");
        }
    }

    /// Called only once, exactly before thread finishes execution.
    fn finish_me(&self) {
        let mut status = self.mutex.lock();
        assert!(matches!(*status, Status::Running));

        *status = Status::Finished;
        {
            let mut result = self.result.lock();
            if result.0 == -1 {
                *result = (0, "finished normally".to_owned());
            }
        }
        self.condvar.notify_all();
    }
}

/// Invokes the given closure with a reference to the current thread [`ThreadContext`].
#[inline(always)]
fn with_thread_context<T>(f: impl FnOnce(&Arc<ThreadContext>) -> T) -> T {
    thread_local!(static THREAD_DATA: Arc<ThreadContext> = Arc::new(ThreadContext::new()));
    THREAD_DATA.with(f)
}

/// Waker is used to wake up threads that are blocked on condition.
/// It keeps track of contexts [`Arc<ThreadContext>`] and can increment the counter
/// of several contexts to send a notification.
pub struct Waker {
    // contexts that are waiting for a notification
    contexts: parking_lot::Mutex<smallvec::SmallVec<[Arc<ThreadContext>; 8]>>,
}

impl Default for Waker {
    fn default() -> Self {
        Self::new()
    }
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
    with_thread_context(|ctx| ctx.yield_me(after_ms))
}

/// Get current time.
pub fn now() -> u64 {
    with_thread_context(|ctx| ctx.clock.get().unwrap().now())
}

pub fn exit(code: i32, msg: String) {
    with_thread_context(|ctx| {
        ctx.allow_panic.store(true, Ordering::SeqCst);
        let mut result = ctx.result.lock();
        *result = (code, msg);
        panic!("exit");
    });
}

pub(crate) fn get_thread_ctx() -> Arc<ThreadContext> {
    with_thread_context(|ctx| ctx.clone())
}

/// Trait for polling channels until they have something.
pub trait PollSome {
    /// Schedule wakeup for message arrival.
    fn wake_me(&self);

    /// Check if channel has a ready message.
    fn has_some(&self) -> bool;
}

/// Blocks current thread until one of the channels has a ready message. Returns
/// index of the channel that has a message. If timeout is reached, returns None.
///
/// Negative timeout means block forever. Zero timeout means check channels and return
/// immediately. Positive timeout means block until timeout is reached.
pub fn epoll_chans(chans: &[Box<dyn PollSome>], timeout: i64) -> Option<usize> {
    let deadline = if timeout < 0 {
        0
    } else {
        now() + timeout as u64
    };

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

            yield_me((deadline - current_time) as i64);
        }
    }
}
