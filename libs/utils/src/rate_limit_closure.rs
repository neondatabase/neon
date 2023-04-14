//! A helper to rate limit the invocation of a closure.

use std::time::{Duration, Instant};

pub struct RateLimitClosure {
    last: Option<Instant>,
    interval: Duration,
}

impl RateLimitClosure {
    pub fn new(interval: Duration) -> Self {
        Self {
            last: None,
            interval,
        }
    }

    pub fn call<F: FnOnce()>(&mut self, f: F) {
        let now = Instant::now();
        match self.last {
            Some(last) if now - last > self.interval => {
                f();
                self.last = Some(now);
            }
            None => {
                f();
                self.last = Some(now);
            }
            Some(_) => {}
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::AtomicUsize;

    #[test]
    fn basics() {
        use super::RateLimitClosure;
        use std::sync::atomic::Ordering::Relaxed;
        use std::time::Duration;

        let called = AtomicUsize::new(0);
        let mut f = RateLimitClosure::new(Duration::from_millis(100));

        let cl = || {
            called.fetch_add(1, Relaxed);
        };

        f.call(cl);
        assert_eq!(called.load(Relaxed), 1);
        f.call(cl);
        assert_eq!(called.load(Relaxed), 1);
        f.call(cl);
        assert_eq!(called.load(Relaxed), 1);
        std::thread::sleep(Duration::from_millis(100));
        f.call(cl);
        assert_eq!(called.load(Relaxed), 2);
        f.call(cl);
        assert_eq!(called.load(Relaxed), 2);
        std::thread::sleep(Duration::from_millis(100));
        f.call(cl);
        assert_eq!(called.load(Relaxed), 3);
    }
}
