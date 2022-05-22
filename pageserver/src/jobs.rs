use std::{collections::VecDeque, panic::{self, AssertUnwindSafe}, sync::{Condvar, Mutex}};
use tracing::{debug, error, info, warn};

// TODO maybe make jobs tenant-specific? Makes monitorin easier.

pub trait Job: std::fmt::Debug + Send + 'static {
    fn run(&self);
}

#[derive(Debug)]
enum WorkerCommand<J: Job> {
    Shutdown,
    DoJob(J),
}

#[derive(Debug)]
struct Pool<J: Job> {
    job_queue: Mutex<VecDeque<WorkerCommand<J>>>,
    condvar: Condvar,  // Notified when queue becomes nonempty
}

impl<J: Job> Pool<J> {
    fn new() -> Self {
        Pool {
            job_queue: Mutex::new(VecDeque::<WorkerCommand<J>>::new()),
            condvar: Condvar::new(),
        }
    }

    fn worker_main(&self) -> anyhow::Result<()> {
        let mut q = self.job_queue.lock().unwrap();
        loop {
            if let Some(command) = q.pop_front() {
                drop(q);
                match command {
                    WorkerCommand::Shutdown => return Ok(()),
                    WorkerCommand::DoJob(job) => {
                        let result = panic::catch_unwind(AssertUnwindSafe(|| {
                            job.run();
                        }));
                        if let Err(e) = result {
                            // TODO mark job as broken
                            println!("Job panicked, thread is ok.");
                        }
                    },
                }
                q = self.job_queue.lock().unwrap();
            } else {
                q = self.condvar.wait(q).unwrap();
            }
        }
    }

    fn queue_job(&self, job: J) {
        // Add the job to the back of the queue
        let mut q = self.job_queue.lock().unwrap();
        q.push_back(WorkerCommand::DoJob(job));

        // If the queue was empty, wake up the next worker thread to pick up
        // the job.
        if q.len() == 1 {
            self.condvar.notify_one();
        }
    }

    fn shutdown_one(&self) {
        // Add shutdown command to the front of the queue
        let mut q = self.job_queue.lock().unwrap();
        q.push_front(WorkerCommand::Shutdown);

        // If the queue was empty, wake up the next worker thread.
        if q.len() == 1 {
            self.condvar.notify_one();
        }

        // TODO wait?
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
        TEST_POOL.get().unwrap().shutdown_one();
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
        TEST_POOL.get().unwrap().shutdown_one();
    }
}
