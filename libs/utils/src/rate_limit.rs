//! A helper to rate limit operations.

use std::time::{Duration, Instant};

pub struct RateLimit {
    last: Option<Instant>,
    interval: Duration,
}

impl RateLimit {
    pub fn new(interval: Duration) -> Self {
        Self {
            last: None,
            interval,
        }
    }

    /// Call `f` if the rate limit allows.
    /// Don't call it otherwise.
    pub fn call<F: FnOnce()>(&mut self, f: F) {
        let now = Instant::now();
        match self.last {
            Some(last) if now - last <= self.interval => {
                // ratelimit
            }
            _ => {
                self.last = Some(now);
                f();
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::AtomicUsize;

    #[test]
    fn basics() {
        use super::RateLimit;
        use std::sync::atomic::Ordering::Relaxed;
        use std::time::Duration;

        let called = AtomicUsize::new(0);
        let mut f = RateLimit::new(Duration::from_millis(100));

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
