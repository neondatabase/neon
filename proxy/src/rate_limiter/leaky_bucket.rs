use tokio::{sync::Mutex, time::Instant};

pub struct LeakyBucket {
    rps: f64,
    max: f64,
    current: Mutex<(f64, Instant)>,
}

impl LeakyBucket {
    pub fn new(rps: f64, max: f64) -> Self {
        assert!(rps > 0.0, "rps must be positive");
        assert!(max > 0.0, "max must be positive");
        Self {
            rps,
            max,
            current: Mutex::new((0.0, Instant::now())),
        }
    }

    pub async fn check(&self, n: f64) -> bool {
        if n > self.max {
            return false;
        }

        let now = Instant::now();
        {
            let mut current = self.current.lock().await;
            let drain = now.duration_since(current.1);
            let drain = drain.as_secs_f64() * self.rps;

            current.0 = (current.0 - drain).clamp(0.0, self.max);
            current.1 = now;

            if current.0 + n > self.max {
                return false;
            }
            current.0 += n;
        }

        true
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::LeakyBucket;

    #[tokio::test(start_paused = true)]
    async fn check() {
        let mut bucket = LeakyBucket::new(500.0, 2000.0);

        // should work for 2000 requests this second
        for i in 0..2000 {
            assert!(bucket.check(1.0).await, "should handle the load: {i}");
        }
        assert!(!bucket.check(1.0).await, "should not handle the load");
        assert_eq!(bucket.current.get_mut().0, 2000.0);

        // in 1ms we should drain 0.5 tokens.
        // make sure we don't lose any tokens
        tokio::time::advance(Duration::from_millis(1)).await;
        assert!(!bucket.check(1.0).await, "should not handle the load");
        tokio::time::advance(Duration::from_millis(1)).await;
        assert!(bucket.check(1.0).await, "should handle the load");

        // in 10ms we should drain 5 tokens
        tokio::time::advance(Duration::from_millis(10)).await;
        for _ in 0..5 {
            assert!(bucket.check(1.0).await, "should handle the load");
        }
        assert!(!bucket.check(1.0).await, "should not handle the load");

        // in 10s we should drain 5000 tokens
        // but cap is only 2000
        tokio::time::advance(Duration::from_secs(10)).await;
        for _ in 0..2000 {
            assert!(bucket.check(1.0).await, "should handle the load");
        }
        assert!(!bucket.check(1.0).await, "should not handle the load");

        // should sustain 500rps
        for _ in 0..2000 {
            tokio::time::advance(Duration::from_millis(10)).await;
            for _ in 0..5 {
                assert!(bucket.check(1.0).await, "should handle the load");
            }
        }
    }
}
