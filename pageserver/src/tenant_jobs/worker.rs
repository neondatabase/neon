use std::{any::Any, fmt::Debug, panic::{self, AssertUnwindSafe}, sync::{Condvar, Mutex}, time::{Duration, Instant}};

pub trait Job: std::fmt::Debug + Send + Clone + 'static {
    type ErrorType;
    fn run(&self) -> Result<(), Self::ErrorType>;
}

#[derive(Debug)]
enum JobError<J: Job> {
    Panic(Box<dyn Any + Send>),
    Error(J::ErrorType),
}

#[derive(Debug)]
enum JobStatus<J: Job> where J::ErrorType: Debug {
    Ready,
    Running(Instant),  // TODO add worker id
    Stuck(JobError<J>),
}

#[derive(Debug)]
struct JobStatusTable<J: Job> where J::ErrorType: Debug {
    // TODO this vec is no good. Too much index arithmetic.
    jobs: Vec<(J, JobStatus<J>)>,
    next: usize,
    begin: Instant,
    period: Duration,
}

impl<J: Job> JobStatusTable<J> where J::ErrorType: Debug {
    fn next(&mut self) -> Option<(usize, J)> {
        while self.next < self.jobs.len() {
            let curr = self.next;
            self.next += 1;

            match self.jobs[curr].1 {
                JobStatus::Ready => {
                    self.jobs[curr].1 = JobStatus::Running(Instant::now());
                    return Some((curr, self.jobs[curr].0.clone()))
                }
                JobStatus::Running(_) => println!("Job already running, skipping this round"),
                JobStatus::Stuck(_) => println!("Job stuck, skipping"),
            }
        }
        None
    }

    fn check_end_of_cycle(&mut self) -> Option<Duration> {
        let until_next = self.period.saturating_sub(Instant::now().duration_since(self.begin));
        if until_next.is_zero() {
            self.next = 0;
            self.begin = Instant::now();
            None
        } else {
            Some(until_next)
        }
    }
}

#[derive(Debug)]
pub struct Pool<J: Job> where J::ErrorType: Debug {
    job_table: Mutex<JobStatusTable<J>>,
    condvar: Condvar,  // Notified when idle worker should wake up
}

impl<J: Job> Pool<J> where J::ErrorType: Debug {
    fn new() -> Self {
        Pool {
            job_table: Mutex::new(JobStatusTable::<J> {
                jobs: vec![],
                next: 0,
                begin: Instant::now(),
                period: Duration::from_millis(10),
            }),
            condvar: Condvar::new(),
        }
    }

    fn worker_main(&self) -> anyhow::Result<()> {
        let mut job_table = self.job_table.lock().unwrap();
        loop {
            if let Some((id, job)) = job_table.next() {
                // Run job without holding lock
                drop(job_table);
                let result = panic::catch_unwind(AssertUnwindSafe(|| {
                    job.run()
                }));
                job_table = self.job_table.lock().unwrap();

                // Update job status
                match result {
                    Ok(Ok(())) => {
                        job_table.jobs[id].1 = JobStatus::Ready;
                    },
                    Ok(Err(e)) => {
                        job_table.jobs[id].1 = JobStatus::Stuck(JobError::Error(e));
                        println!("Job errored, thread is ok.");
                    },
                    Err(e) => {
                        job_table.jobs[id].1 = JobStatus::Stuck(JobError::Panic(e));
                        println!("Job panicked, thread is ok.");
                    },
                }
            } else {
                if let Some(wait_time) = job_table.check_end_of_cycle() {
                    job_table = self.condvar.wait_timeout(job_table, wait_time).unwrap().0;
                }
            }
        }
    }

    fn queue_job(&self, job: J) {
        // Add the job to the back of the queue
        let mut job_table = self.job_table.lock().unwrap();
        job_table.jobs.push((job, JobStatus::Ready));

        // Notify workers if they're waiting for work.
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
        type ErrorType = String;

        fn run(&self) -> Result<(), String> {
            if self.to_print == "pls panic" {
                panic!("AAA");
            }
            println!("{}", self.to_print);
            Ok(())
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

        TEST_POOL.get().unwrap().queue_job(PrintJob {
            to_print: "hello from job 1".to_string(),
        });
        TEST_POOL.get().unwrap().queue_job(PrintJob {
            to_print: "hello from job 2".to_string(),
        });

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
