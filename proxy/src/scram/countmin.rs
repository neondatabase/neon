use std::hash::Hash;

/// estimator of hash jobs per second.
/// <https://en.wikipedia.org/wiki/Count%E2%80%93min_sketch>
pub(crate) struct CountMinSketch {
    // one for each depth
    hashers: Vec<ahash::RandomState>,
    width: usize,
    depth: usize,
    // buckets, width*depth
    buckets: Vec<u32>,
}

impl CountMinSketch {
    /// Given parameters (ε, δ),
    ///   set width = ceil(e/ε)
    ///   set depth = ceil(ln(1/δ))
    ///
    /// guarantees:
    /// actual <= estimate
    /// estimate <= actual + ε * N with probability 1 - δ
    /// where N is the cardinality of the stream
    pub(crate) fn with_params(epsilon: f64, delta: f64) -> Self {
        CountMinSketch::new(
            (std::f64::consts::E / epsilon).ceil() as usize,
            (1.0_f64 / delta).ln().ceil() as usize,
        )
    }

    fn new(width: usize, depth: usize) -> Self {
        Self {
            #[cfg(test)]
            hashers: (0..depth)
                .map(|i| {
                    // digits of pi for good randomness
                    ahash::RandomState::with_seeds(
                        314159265358979323,
                        84626433832795028,
                        84197169399375105,
                        82097494459230781 + i as u64,
                    )
                })
                .collect(),
            #[cfg(not(test))]
            hashers: (0..depth).map(|_| ahash::RandomState::new()).collect(),
            width,
            depth,
            buckets: vec![0; width * depth],
        }
    }

    pub(crate) fn inc_and_return<T: Hash>(&mut self, t: &T, x: u32) -> u32 {
        let mut min = u32::MAX;
        for row in 0..self.depth {
            let col = (self.hashers[row].hash_one(t) as usize) % self.width;

            let row = &mut self.buckets[row * self.width..][..self.width];
            row[col] = row[col].saturating_add(x);
            min = std::cmp::min(min, row[col]);
        }
        min
    }

    pub(crate) fn reset(&mut self) {
        self.buckets.clear();
        self.buckets.resize(self.width * self.depth, 0);
    }
}

#[cfg(test)]
mod tests {
    use rand::rngs::StdRng;
    use rand::seq::SliceRandom;
    use rand::{Rng, SeedableRng};

    use super::CountMinSketch;

    fn eval_precision(n: usize, p: f64, q: f64) -> usize {
        // fixed value of phi for consistent test
        let mut rng = StdRng::seed_from_u64(16180339887498948482);

        #[allow(non_snake_case)]
        let mut N = 0;

        let mut ids = vec![];

        for _ in 0..n {
            // number to insert at once
            let n = rng.gen_range(1..4096);
            // number of insert operations
            let m = rng.gen_range(1..100);

            let id = uuid::Builder::from_random_bytes(rng.gen()).into_uuid();
            ids.push((id, n, m));

            // N = sum(actual)
            N += n * m;
        }

        // q% of counts will be within p of the actual value
        let mut sketch = CountMinSketch::with_params(p / N as f64, 1.0 - q);

        // insert a bunch of entries in a random order
        let mut ids2 = ids.clone();
        while !ids2.is_empty() {
            ids2.shuffle(&mut rng);
            ids2.retain_mut(|id| {
                sketch.inc_and_return(&id.0, id.1);
                id.2 -= 1;
                id.2 > 0
            });
        }

        let mut within_p = 0;
        for (id, n, m) in ids {
            let actual = n * m;
            let estimate = sketch.inc_and_return(&id, 0);

            // This estimate has the guarantee that actual <= estimate
            assert!(actual <= estimate);

            // This estimate has the guarantee that estimate <= actual + εN with probability 1 - δ.
            // ε = p / N, δ = 1 - q;
            // therefore, estimate <= actual + p with probability q.
            if estimate as f64 <= actual as f64 + p {
                within_p += 1;
            }
        }
        within_p
    }

    #[test]
    fn precision() {
        assert_eq!(eval_precision(100, 100.0, 0.99), 100);
        assert_eq!(eval_precision(1000, 100.0, 0.99), 1000);
        assert_eq!(eval_precision(100, 4096.0, 0.99), 100);
        assert_eq!(eval_precision(1000, 4096.0, 0.99), 1000);

        // seems to be more precise than the literature indicates?
        // probably numbers are too small to truly represent the probabilities.
        assert_eq!(eval_precision(100, 4096.0, 0.90), 100);
        assert_eq!(eval_precision(1000, 4096.0, 0.90), 1000);
        assert_eq!(eval_precision(100, 4096.0, 0.1), 96);
        assert_eq!(eval_precision(1000, 4096.0, 0.1), 988);
    }

    // returns memory usage in bytes, and the time complexity per insert.
    fn eval_cost(p: f64, q: f64) -> (usize, usize) {
        #[allow(non_snake_case)]
        // N = sum(actual)
        // Let's assume 1021 samples, all of 4096
        let N = 1021 * 4096;
        let sketch = CountMinSketch::with_params(p / N as f64, 1.0 - q);

        let memory = size_of::<u32>() * sketch.buckets.len();
        let time = sketch.depth;
        (memory, time)
    }

    #[test]
    fn memory_usage() {
        assert_eq!(eval_cost(100.0, 0.99), (2273580, 5));
        assert_eq!(eval_cost(4096.0, 0.99), (55520, 5));
        assert_eq!(eval_cost(4096.0, 0.90), (33312, 3));
        assert_eq!(eval_cost(4096.0, 0.1), (11104, 1));
    }
}
