use std::sync::{Arc, Condvar, Mutex};

/// This is a custom waitgroup for internal use, shouldn't be used by the custom code.
#[derive(Clone)]
pub struct WaitGroup {
    inner: Arc<Inner>,
}

/// Inner state of a `WaitGroup`.
struct Inner {
    // using std convar
    cvar: Condvar,
    count: Mutex<i32>,
}

impl Default for WaitGroup {
    fn default() -> Self {
        Self {
            inner: Arc::new(Inner {
                cvar: Condvar::new(),
                count: Mutex::new(0),
            }),
        }
    }
}

impl WaitGroup {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn wait(&self) {
        if *self.inner.count.lock().unwrap() <= 0 {
            return;
        }

        let mut count = self.inner.count.lock().unwrap();
        while *count > 0 {
            count = self.inner.cvar.wait(count).unwrap();
        }
    }

    pub fn add(&self, delta: i32) {
        let mut count = self.inner.count.lock().unwrap();
        *count += delta;
        if *count <= 0 {
            self.inner.cvar.notify_all();
        }
    }

    pub fn done(&self) {
        self.add(-1);
    }
}
