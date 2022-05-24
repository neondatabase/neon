use std::{collections::VecDeque, ops::Sub, panic::{self, AssertUnwindSafe}, sync::{Condvar, Mutex}, time::{Duration, Instant}};

// TODO maybe make jobs tenant-specific? Makes monitorin easier.

pub trait Job: std::fmt::Debug + Send + Clone + 'static {
    fn run(&self);
}

#[derive(Debug, Clone)]
enum JobStatus {
    Ready,
    Running(Instant),
    Stuck,  // TODO remember error
}

#[derive(Debug)]
struct JobStatusTable<J: Job> {
    jobs: Vec<(J, JobStatus)>,
    next: usize,
    begin: Instant,
}

#[derive(Debug)]
struct Pool<J: Job> {
    job_table: Mutex<JobStatusTable<J>>,
    condvar: Condvar,  // Notified when queue becomes nonempty
    period: Duration,
}

impl<J: Job> Pool<J> {
    fn new() -> Self {
        Pool {
            job_table: Mutex::new(JobStatusTable::<J> {
                jobs: vec![],
                next: 0,
                begin: Instant::now(),
            }),
            condvar: Condvar::new(),
            period: Duration::from_millis(10),
        }
    }

    fn worker_main(&self) -> anyhow::Result<()> {
        let mut job_table = self.job_table.lock().unwrap();
        loop {
            if job_table.next < job_table.jobs.len() {
                let curr = job_table.next;
                let (job, status) = job_table.jobs[curr].clone();
                job_table.next += 1;

                if matches!(status, JobStatus::Ready) {
                    // Run job without holding lock
                    job_table.jobs[curr].1 = JobStatus::Running(Instant::now());
                    drop(job_table);
                    // TODO don't run job if previous iteration is still running
                    let result = panic::catch_unwind(AssertUnwindSafe(|| {
                        job.run();
                    }));
                    job_table = self.job_table.lock().unwrap();

                    match result {
                        Ok(()) => {
                            job_table.jobs[curr].1 = JobStatus::Ready;
                        },
                        Err(e) => {
                            job_table.jobs[curr].1 = JobStatus::Stuck;
                            println!("Job panicked, thread is ok.");
                        },
                    }
                }
            } else {
                let since_last_cycle = Instant::now().duration_since(job_table.begin);
                let until_next_cycle = self.period.saturating_sub(since_last_cycle);
                if until_next_cycle.is_zero() {
                    job_table.next = 0;
                    job_table.begin = Instant::now();
                } else {
                    job_table = self.condvar.wait_timeout(job_table, until_next_cycle).unwrap().0;
                }
            }
        }
    }

    fn queue_job(&self, job: J) {
        // Add the job to the back of the queue
        let mut job_table = self.job_table.lock().unwrap();
        job_table.jobs.push((job, JobStatus::Ready));

        // If the queue was empty, wake up the next worker thread to pick up
        // the job.
        if job_table.next == job_table.jobs.len() - 1 {
            self.condvar.notify_one();
        }
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use once_cell::sync::OnceCell;

    use crate::thread_mgr::{self, ThreadKind};
    use super::*;

    #[derive(Debug, Clone, Eq, PartialEq)]
    struct PrintJob {
        to_print: String
    }

    impl Job for PrintJob {
        fn run(&self) {
            if self.to_print == "pls panic" {
                panic!("AAA");
            }
            println!("{}", self.to_print);
        }
    }

    static TEST_POOL: OnceCell<Pool<PrintJob>> = OnceCell::new();

    #[tokio::test]
    async fn pool_1() {
        TEST_POOL.set(Pool::<PrintJob>::new()).unwrap();

        thread_mgr::spawn(
            ThreadKind::GarbageCollector,  // change this
            None,
            None,
            "test_worker_1",
            true,
            move || {
                TEST_POOL.get().unwrap().worker_main()
            },
        ).unwrap();

        let j = PrintJob {
            to_print: "hello from job".to_string(),
        };
        TEST_POOL.get().unwrap().queue_job(j.clone());
        TEST_POOL.get().unwrap().queue_job(j.clone());
        TEST_POOL.get().unwrap().queue_job(j.clone());

        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    #[tokio::test]
    async fn pool_panic() {
        TEST_POOL.set(Pool::<PrintJob>::new()).unwrap();

        thread_mgr::spawn(
            ThreadKind::GarbageCollector,  // change this
            None,
            None,
            "test_worker_1",
            true,
            move || {
                TEST_POOL.get().unwrap().worker_main()
            },
        ).unwrap();

        let j = PrintJob {
            to_print: "hello from job".to_string(),
        };
        let panic = PrintJob {
            to_print: "pls panic".to_string(),
        };

        TEST_POOL.get().unwrap().queue_job(panic.clone());
        TEST_POOL.get().unwrap().queue_job(j.clone());
        TEST_POOL.get().unwrap().queue_job(j.clone());

        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}
