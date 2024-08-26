//! Custom threadpool implementation for password hashing.
//!
//! Requirements:
//! 1. Fairness per endpoint.
//! 2. Yield support for high iteration counts.

use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc,
};

use crossbeam_deque::{Injector, Stealer, Worker};
use itertools::Itertools;
use parking_lot::{Condvar, Mutex};
use rand::Rng;
use rand::{rngs::SmallRng, SeedableRng};
use tokio::sync::oneshot;

use crate::{
    intern::EndpointIdInt,
    metrics::{ThreadPoolMetrics, ThreadPoolWorkerId},
    scram::countmin::CountMinSketch,
};

use super::pbkdf2::Pbkdf2;

pub struct ThreadPool {
    queue: Injector<JobSpec>,
    stealers: Vec<Stealer<JobSpec>>,
    parkers: Vec<(Condvar, Mutex<ThreadState>)>,
    /// bitpacked representation.
    /// lower 8 bits = number of sleeping threads
    /// next 8 bits = number of idle threads (searching for work)
    counters: AtomicU64,

    pub metrics: Arc<ThreadPoolMetrics>,
}

#[derive(PartialEq)]
enum ThreadState {
    Parked,
    Active,
}

impl ThreadPool {
    pub fn new(n_workers: u8) -> Arc<Self> {
        let workers = (0..n_workers).map(|_| Worker::new_fifo()).collect_vec();
        let stealers = workers.iter().map(|w| w.stealer()).collect_vec();

        let parkers = (0..n_workers)
            .map(|_| (Condvar::new(), Mutex::new(ThreadState::Active)))
            .collect_vec();

        let pool = Arc::new(Self {
            queue: Injector::new(),
            stealers,
            parkers,
            // threads start searching for work
            counters: AtomicU64::new((n_workers as u64) << 8),
            metrics: Arc::new(ThreadPoolMetrics::new(n_workers as usize)),
        });

        for (i, worker) in workers.into_iter().enumerate() {
            let pool = Arc::clone(&pool);
            std::thread::spawn(move || thread_rt(pool, worker, i));
        }

        pool
    }

    pub fn spawn_job(
        &self,
        endpoint: EndpointIdInt,
        pbkdf2: Pbkdf2,
    ) -> oneshot::Receiver<[u8; 32]> {
        let (tx, rx) = oneshot::channel();

        let queue_was_empty = self.queue.is_empty();

        self.metrics.injector_queue_depth.inc();
        self.queue.push(JobSpec {
            response: tx,
            pbkdf2,
            endpoint,
        });

        // inspired from <https://github.com/rayon-rs/rayon/blob/3e3962cb8f7b50773bcc360b48a7a674a53a2c77/rayon-core/src/sleep/mod.rs#L242>
        let counts = self.counters.load(Ordering::SeqCst);
        let num_awake_but_idle = (counts >> 8) & 0xff;
        let num_sleepers = counts & 0xff;

        // If the queue is non-empty, then we always wake up a worker
        // -- clearly the existing idle jobs aren't enough. Otherwise,
        // check to see if we have enough idle workers.
        if !queue_was_empty || num_awake_but_idle == 0 {
            let num_to_wake = Ord::min(1, num_sleepers);
            self.wake_any_threads(num_to_wake);
        }

        rx
    }

    #[cold]
    fn wake_any_threads(&self, mut num_to_wake: u64) {
        if num_to_wake > 0 {
            for i in 0..self.parkers.len() {
                if self.wake_specific_thread(i) {
                    num_to_wake -= 1;
                    if num_to_wake == 0 {
                        return;
                    }
                }
            }
        }
    }

    fn wake_specific_thread(&self, index: usize) -> bool {
        let (condvar, lock) = &self.parkers[index];

        let mut state = lock.lock();
        if *state == ThreadState::Parked {
            condvar.notify_one();

            // When the thread went to sleep, it will have incremented
            // this value. When we wake it, its our job to decrement
            // it. We could have the thread do it, but that would
            // introduce a delay between when the thread was
            // *notified* and when this counter was decremented. That
            // might mislead people with new work into thinking that
            // there are sleeping threads that they should try to
            // wake, when in fact there is nothing left for them to
            // do.
            self.counters.fetch_sub(1, Ordering::SeqCst);
            *state = ThreadState::Active;

            true
        } else {
            false
        }
    }

    fn steal(&self, rng: &mut impl Rng, skip: usize, worker: &Worker<JobSpec>) -> Option<JobSpec> {
        // announce thread as idle
        self.counters.fetch_add(256, Ordering::SeqCst);

        // try steal from the global queue
        loop {
            match self.queue.steal_batch_and_pop(worker) {
                crossbeam_deque::Steal::Success(job) => {
                    self.metrics
                        .injector_queue_depth
                        .set(self.queue.len() as i64);
                    // no longer idle
                    self.counters.fetch_sub(256, Ordering::SeqCst);
                    return Some(job);
                }
                crossbeam_deque::Steal::Retry => continue,
                crossbeam_deque::Steal::Empty => break,
            }
        }

        // try steal from our neighbours
        loop {
            let mut retry = false;
            let start = rng.gen_range(0..self.stealers.len());
            let job = (start..self.stealers.len())
                .chain(0..start)
                .filter(|i| *i != skip)
                .find_map(
                    |victim| match self.stealers[victim].steal_batch_and_pop(worker) {
                        crossbeam_deque::Steal::Success(job) => Some(job),
                        crossbeam_deque::Steal::Empty => None,
                        crossbeam_deque::Steal::Retry => {
                            retry = true;
                            None
                        }
                    },
                );
            if job.is_some() {
                // no longer idle
                self.counters.fetch_sub(256, Ordering::SeqCst);
                return job;
            }
            if !retry {
                return None;
            }
        }
    }
}

fn thread_rt(pool: Arc<ThreadPool>, worker: Worker<JobSpec>, index: usize) {
    /// interval when we should steal from the global queue
    /// so that tail latencies are managed appropriately
    const STEAL_INTERVAL: usize = 61;

    /// How often to reset the sketch values
    const SKETCH_RESET_INTERVAL: usize = 1021;

    let mut rng = SmallRng::from_entropy();

    // used to determine whether we should temporarily skip tasks for fairness.
    // 99% of estimates will overcount by no more than 4096 samples
    let mut sketch = CountMinSketch::with_params(1.0 / (SKETCH_RESET_INTERVAL as f64), 0.01);

    let (condvar, lock) = &pool.parkers[index];

    'wait: loop {
        // wait for notification of work
        {
            let mut lock = lock.lock();

            // queue is empty
            pool.metrics
                .worker_queue_depth
                .set(ThreadPoolWorkerId(index), 0);

            // subtract 1 from idle count, add 1 to sleeping count.
            pool.counters.fetch_sub(255, Ordering::SeqCst);

            *lock = ThreadState::Parked;
            condvar.wait(&mut lock);
        }

        for i in 0.. {
            let Some(mut job) = worker
                .pop()
                .or_else(|| pool.steal(&mut rng, index, &worker))
            else {
                continue 'wait;
            };

            pool.metrics
                .worker_queue_depth
                .set(ThreadPoolWorkerId(index), worker.len() as i64);

            // receiver is closed, cancel the task
            if !job.response.is_closed() {
                let rate = sketch.inc_and_return(&job.endpoint, job.pbkdf2.cost());

                const P: f64 = 2000.0;
                // probability decreases as rate increases.
                // lower probability, higher chance of being skipped
                //
                // estimates (rate in terms of 4096 rounds):
                // rate = 0    => probability = 100%
                // rate = 10   => probability = 71.3%
                // rate = 50   => probability = 62.1%
                // rate = 500  => probability = 52.3%
                // rate = 1021 => probability = 49.8%
                //
                // My expectation is that the pool queue will only begin backing up at ~1000rps
                // in which case the SKETCH_RESET_INTERVAL represents 1 second. Thus, the rates above
                // are in requests per second.
                let probability = P.ln() / (P + rate as f64).ln();
                if pool.queue.len() > 32 || rng.gen_bool(probability) {
                    pool.metrics
                        .worker_task_turns_total
                        .inc(ThreadPoolWorkerId(index));

                    match job.pbkdf2.turn() {
                        std::task::Poll::Ready(result) => {
                            let _ = job.response.send(result);
                        }
                        std::task::Poll::Pending => worker.push(job),
                    }
                } else {
                    pool.metrics
                        .worker_task_skips_total
                        .inc(ThreadPoolWorkerId(index));

                    // skip for now
                    worker.push(job);
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
}

struct JobSpec {
    response: oneshot::Sender<[u8; 32]>,
    pbkdf2: Pbkdf2,
    endpoint: EndpointIdInt,
}

#[cfg(test)]
mod tests {
    use crate::EndpointId;

    use super::*;

    #[tokio::test]
    async fn hash_is_correct() {
        let pool = ThreadPool::new(1);

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
        assert_eq!(actual, expected);
    }
}
