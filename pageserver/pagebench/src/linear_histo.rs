use std::time::Duration;


const LATENCY_BUCKET_BASE: u64 = 0;
const LATENCY_BUCKET_STEP: u64 = 100;
const LATENCY_BUCKETS_LEN: usize = 10;

#[derive(Debug)]
pub(crate) struct LinearHisto {
    below: u64,
    counters: [u64; LATENCY_BUCKETS_LEN],
    above: u64,
}

impl LinearHisto {
    pub(crate) fn new() -> Self {
        LinearHisto {
            below: 0,
            counters: std::array::from_fn(|_| 0),
            above: 0,
        }
    }

    pub(crate) fn observe(&mut self, latency: Duration) {
        let latency: u64 = latency.as_micros().try_into().unwrap();
        let bucket = if latency < LATENCY_BUCKET_BASE {
            &mut self.below
        } else if latency
            >= (LATENCY_BUCKET_BASE + (LATENCY_BUCKETS_LEN as u64) * LATENCY_BUCKET_STEP)
        {
            &mut self.above
        } else {
            &mut self.counters[((latency - LATENCY_BUCKET_BASE) / LATENCY_BUCKET_STEP) as usize]
        };
        *bucket += 1;
    }

    pub(crate) fn add(&mut self, other: &Self) {
        let Self {
            ref mut below,
            ref mut counters,
            ref mut above,
        } = self;
        *below += other.below;
        for i in 0..counters.len() {
            counters[i] += other.counters[i];
        }
        *above += other.above;
    }

    fn bucket_lower(&self, idx: usize) -> u64 {
        let idx = idx as u64;
        LATENCY_BUCKET_BASE + idx * LATENCY_BUCKET_STEP
    }
    fn bucket_upper(&self, idx: usize) -> u64 {
        let idx = idx as u64;
        LATENCY_BUCKET_BASE + (idx + 1) * LATENCY_BUCKET_STEP
    }

    pub(crate) fn mean(&self) -> Option<Duration> {
        if self.below > 0 || self.above > 0 {
            return None;
        }
        let mut sum = 0;
        let mut count = 0;
        for (bucket_idx, counter) in self.counters.iter().enumerate() {
            let bucket_mean = self.bucket_lower(bucket_idx) + self.bucket_upper(bucket_idx) / 2;
            sum += counter * bucket_mean;
            count += counter;
        }
        Some(Duration::from_micros(sum / count))
    }

    pub(crate) fn percentile(&self, p: f64) -> Duration {
        Duration::from_micros(0)
    }
}
