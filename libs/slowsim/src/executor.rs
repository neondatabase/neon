use std::{thread::JoinHandle, cell::{OnceCell, RefCell}, sync::{mpsc, Arc, atomic::{AtomicU8, Ordering}}};

/// Stores status of the running threads. Threads are registered in the runtime upon creation
/// and deregistered upon termination.
pub struct Runtime {
    // stores handles to all threads that are currently running
    threads: Vec<ThreadHandle>,
}

impl Runtime {
    /// Spawn a new thread and register it in the runtime.
    pub fn spawn<F>(&mut self, f: F)
    where
        F: FnOnce() + Send + 'static,
    {
        let (tx, rx) = mpsc::channel();

        let join = std::thread::spawn(move || {
            with_thread_context(|ctx| {
                tx.send(ctx.clone()).expect("failed to send thread context");
                ctx.yield_me();
            });
            f()
        });

        let ctx = rx.recv().expect("failed to receive thread context");
        let handle = ThreadHandle::new(ctx, join);

        // TODO: remove this
        handle.hacky_continue();

        self.threads.push(handle);
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

    fn hacky_continue(&self) {
        let mut status = self.ctx.mutex.lock();
        assert!(matches!(*status, Status::Sleep));

        *status = Status::Running;
        self.ctx.condvar.notify_all();
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

pub struct ThreadContext {
    // used to block thread until it is woken up
    mutex: parking_lot::Mutex<Status>,
    condvar: parking_lot::Condvar,

    // used as a flag to indicate runtime that thread is ready to be woken up
    wakeup: AtomicU8,
}

impl ThreadContext {
    fn new() -> Self {
        Self {
            mutex: parking_lot::Mutex::new(Status::Running),
            condvar: parking_lot::Condvar::new(),
            wakeup: AtomicU8::new(0),
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
    fn yield_me(&self) {
        let mut status = self.mutex.lock();
        assert!(matches!(*status, Status::Running));

        // tell executor that we are ready to be woken up
        self.inc_wake();
        *status = Status::Sleep;

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
    THREAD_DATA.with(f)
}

/// Waker is used to wake up threads that are blocked on condition.
/// It keeps track of contexts [`Arc<ThreadContext>`] and can increment the counter
/// of several contexts to send a notification.
pub struct Waker {
    // contexts that are waiting for a notification
    contexts: smallvec::SmallVec<[Arc<ThreadContext>; 8]>,
}

impl Waker {
    pub fn new() -> Self {
        Self {
            contexts: smallvec::SmallVec::new(),
        }
    }

    /// Subscribe current thread to receive a wake notification later.
    pub fn wake_me_later(&mut self) {
        with_thread_context(|ctx| {
            self.contexts.push(ctx.clone());
        });
    }

    /// Wake up all threads that are waiting for a notification and clear the list.
    pub fn wake_all(&mut self) {
        for ctx in self.contexts.iter() {
            ctx.inc_wake();
        }
        self.contexts.clear();
    }
}
