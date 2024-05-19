//! Custom threadpool implementation for password hashing.
//!
//! Requirements:
//! 1. Fairness per endpoint.
//! 2. Yield support for high iteration counts.

use std::{
    hash::Hash,
    sync::{Arc, OnceLock},
};

use crossbeam_deque::{Injector, Stealer, Worker};
use hmac::{
    digest::{consts::U32, generic_array::GenericArray},
    Hmac, Mac,
};
use parking_lot::{Condvar, Mutex};
use rand::Rng;
use rand::{rngs::SmallRng, seq::SliceRandom, SeedableRng};
use sha2::Sha256;
use tokio::sync::oneshot;

use crate::intern::EndpointIdInt;

pub struct ThreadPool {
    queue: Injector<JobSpec>,
    /// for work stealing
    threads: OnceLock<Box<[Stealer<JobSpec>]>>,
    /// for signals about work
    condvar: Condvar,
    /// for the condvar notification.
    lock: Mutex<State>,
}

enum State {
    Idle,
    WorkAvailable,
}

impl ThreadPool {
    pub fn new() -> Self {
        Self {
            queue: Injector::new(),
            threads: OnceLock::new(),
            condvar: Condvar::new(),
            lock: Mutex::new(State::Idle),
        }
    }

    pub fn spawn_job(
        &self,
        endpoint: EndpointIdInt,
        pbkdf2: Pbkdf2,
    ) -> oneshot::Receiver<[u8; 32]> {
        let (tx, rx) = oneshot::channel();

        self.queue.push(JobSpec {
            response: tx,
            pbkdf2,
            endpoint,
        });

        let mut lock = self.lock.lock();
        *lock = State::WorkAvailable;
        self.condvar.notify_one();

        rx
    }

    // pub fn shutdown(&self) {
    //     let mut lock = self.lock.lock();
    //     *lock = State::Shutdown;
    //     self.condvar.notify_all();
    // }

    pub fn spawn_workers(self: &Arc<Self>, workers: usize) {
        let _guard = self.lock.lock();

        let mut threads = Vec::with_capacity(workers);
        for _ in 0..workers {
            let worker = Worker::new_fifo();
            threads.push(worker.stealer());

            let pool = Arc::clone(self);
            std::thread::spawn(move || {
                let mut rng = SmallRng::from_entropy();

                // used to determine whether we should temporarily skip tasks
                // for fairness
                let mut sketch = CountMinSketch::new(32, 8);

                /// interval when we should steal from the global queue
                /// so that tail latencies are managed appropriately
                const STEAL_INTERVAL: usize = 61;

                /// How often to reset the sketch values
                const SKETCH_RESET_INTERVAL: usize = 1021;

                'wait: loop {
                    // wait for notification of work
                    {
                        let mut lock = pool.lock.lock();
                        #[allow(clippy::while_let_loop)]
                        loop {
                            match *lock {
                                State::Idle => pool.condvar.wait(&mut lock),
                                State::WorkAvailable => break,
                                // State::Shutdown => return,
                            }
                        }
                    }

                    for i in 0.. {
                        let mut job = match worker.pop() {
                            Some(job) => job,
                            None => 'job: {
                                // try steal from the global queue
                                loop {
                                    match pool.queue.steal_batch_and_pop(&worker) {
                                        crossbeam_deque::Steal::Success(job) => break 'job job,
                                        crossbeam_deque::Steal::Retry => continue,
                                        crossbeam_deque::Steal::Empty => break,
                                    }
                                }
                                // try steal from a random worker queue
                                loop {
                                    let thread =
                                        pool.threads.get().unwrap().choose(&mut rng).unwrap();
                                    match thread.steal_batch_and_pop(&worker) {
                                        crossbeam_deque::Steal::Success(job) => break 'job job,
                                        crossbeam_deque::Steal::Retry => continue,
                                        crossbeam_deque::Steal::Empty => continue 'wait,
                                    }
                                }
                            }
                        };

                        // receiver is closed, cancel the task
                        if !job.response.is_closed() {
                            let rate = sketch.inc(&job.endpoint);

                            const P: f64 = 32.0;
                            // probability decreases as rate increases.
                            // lower probability, higher chance of being skipped
                            // rate = 0    => probability = 100%
                            // rate = 10   => probability = 92.7%
                            // rate = 50   => probability = 78.6%
                            // rate = 500  => probability = 55.2%
                            // rate = 1021 => probability = 49.8%
                            let probability = P.ln() / (P + rate as f64).ln();
                            if rng.gen_bool(probability) {
                                match job.pbkdf2.turn() {
                                    std::task::Poll::Ready(result) => {
                                        let _ = job.response.send(result);
                                    }
                                    std::task::Poll::Pending => worker.push(job),
                                }
                            } else {
                                // skip for now
                                worker.push(job)
                            }
                        }

                        // if we get stuck with a few long lived jobs in the queue
                        // it's better to try and steal from the queue too for fairness
                        if i % STEAL_INTERVAL == 0 {
                            let _ = pool.queue.steal_batch(&worker);
                        }

                        if i % SKETCH_RESET_INTERVAL == 0 {
                            sketch.reset();
                        }
                    }
                }
            });
        }

        self.threads
            .set(threads.into_boxed_slice())
            .expect("spawn_workers should not be called multiple times");
    }
}

/// estimator of hash jobs per second.
/// <https://en.wikipedia.org/wiki/Count%E2%80%93min_sketch>
struct CountMinSketch {
    // one for each width
    hashers: Vec<ahash::RandomState>,
    width: usize,
    depth: usize,
    // buckets, width*depth
    buckets: Vec<u64>,
}

impl CountMinSketch {
    fn new(width: usize, depth: usize) -> Self {
        Self {
            hashers: (0..width).map(|_| ahash::RandomState::new()).collect(),
            width,
            depth,
            buckets: vec![0; width * depth],
        }
    }

    fn inc<T: Hash>(&mut self, t: &T) -> u64 {
        let mut min = 0;
        for w in 0..self.width {
            let hash = self.hashers[w].hash_one(t) as usize;
            let bucket = &mut self.buckets[w * self.depth + hash % self.depth];
            *bucket = bucket.saturating_add(1);
            min = std::cmp::min(min, *bucket);
        }
        min
    }

    // fn get<T: Hash>(&mut self, t: &T) -> u64 {
    //     let mut min = 0;
    //     for w in 0..self.width {
    //         let hash = self.hashers[w].hash_one(t) as usize;
    //         min = std::cmp::min(min, self.buckets[w * self.depth + hash % self.depth]);
    //     }
    //     min
    // }

    fn reset(&mut self) {
        self.buckets.clear();
        self.buckets.resize(self.width * self.depth, 0);
    }
}

impl Default for ThreadPool {
    fn default() -> Self {
        Self::new()
    }
}

struct JobSpec {
    response: oneshot::Sender<[u8; 32]>,
    pbkdf2: Pbkdf2,
    endpoint: EndpointIdInt,
}

pub struct Pbkdf2 {
    hmac: Hmac<Sha256>,
    prev: GenericArray<u8, U32>,
    hi: GenericArray<u8, U32>,
    iterations: u32,
}

// inspired from <https://github.com/neondatabase/rust-postgres/blob/20031d7a9ee1addeae6e0968e3899ae6bf01cee2/postgres-protocol/src/authentication/sasl.rs#L36-L61>
impl Pbkdf2 {
    pub fn start(str: &[u8], salt: &[u8], iterations: u32) -> Self {
        let hmac =
            Hmac::<Sha256>::new_from_slice(str).expect("HMAC is able to accept all key sizes");

        let prev = hmac
            .clone()
            .chain_update(salt)
            .chain_update(1u32.to_be_bytes())
            .finalize()
            .into_bytes();

        Self {
            hmac,
            // one consumed for the hash above
            iterations: iterations - 1,
            hi: prev,
            prev,
        }
    }

    fn turn(&mut self) -> std::task::Poll<[u8; 32]> {
        let Self {
            hmac,
            prev,
            hi,
            iterations,
        } = self;

        let n = (*iterations).clamp(0, 4096);
        for _ in 0..n {
            *prev = hmac.clone().chain_update(*prev).finalize().into_bytes();

            for (hi, prev) in hi.iter_mut().zip(*prev) {
                *hi ^= prev;
            }
        }

        *iterations -= n;
        if *iterations == 0 {
            std::task::Poll::Ready((*hi).into())
        } else {
            std::task::Poll::Pending
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use crate::EndpointId;

    use super::*;

    #[tokio::test]
    async fn hash_is_correct() {
        let pool = Arc::new(ThreadPool::new());
        pool.spawn_workers(1);

        let ep = EndpointId::from("foo");
        let ep = EndpointIdInt::from(ep);

        let salt = [0x55; 32];
        let actual = pool
            .spawn_job(ep, Pbkdf2::start(b"password", &salt, 4096))
            .await
            .unwrap();

        let expected = [
            10, 114, 73, 188, 140, 222, 196, 156, 214, 184, 79, 157, 119, 242, 16, 31, 53, 242,
            178, 43, 95, 8, 225, 182, 122, 40, 219, 21, 89, 147, 64, 140,
        ];
        assert_eq!(actual, expected)
    }
}
