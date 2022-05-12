use std::{collections::VecDeque, sync::{Condvar, Mutex}, time::Duration};

use crate::thread_mgr::{is_shutdown_requested, shutdown_watcher};

pub trait Job: std::fmt::Debug + Send + 'static {
    fn run(&self);
}

#[derive(Debug)]
struct Pool<J: Job> {
    job_queue: Mutex<VecDeque<J>>,
    condvar: Condvar,  // Notified when queue becomes nonempty
}

impl<J: Job> Pool<J> {
    fn new() -> Self {
        Pool {
            job_queue: Mutex::new(VecDeque::<J>::new()),
            condvar: Condvar::new(),
        }
    }

    fn worker_main_2(&self) -> anyhow::Result<()> {
        let runtime = tokio::runtime::Builder::new_current_thread()
             .enable_all()
             .build()?;

        runtime.block_on(async {
            loop {
                tokio::select! {
                    _ = shutdown_watcher() => break,
                    // TODO i need tokio::sync::Mutex
                    q = self.job_queue.lock() {
                        if let Some(job) = q.pop_front() {
                            drop(q);
                            job.run();
                            q = self.job_queue.lock().unwrap();
                        } else {
                            // TODO can't wait here, i might want to shut down
                            // q = self.condvar.wait(q).unwrap();
                            std::thread::sleep(Duration::from_millis(5))
                        }
                    },
                };
            }
        });

        Ok(())
    }

    fn worker_main(&self) -> anyhow::Result<()> {
        let mut q = self.job_queue.lock().unwrap();
        while !is_shutdown_requested() {
            if let Some(job) = q.pop_front() {
                drop(q);
                job.run();
                q = self.job_queue.lock().unwrap();
            } else {
                // TODO can't wait here, i might want to shut down
                // q = self.condvar.wait(q).unwrap();
                std::thread::sleep(Duration::from_millis(5))
            }
        }

        Ok(())
    }

    fn queue_job(&self, job: J) {
        // Add the job to the back of the queue
        let mut q = self.job_queue.lock().unwrap();
        q.push_back(job);

        // If the queue was empty, wake up the next worker thread to pick up
        // the job.
        if q.len() == 1 {
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

        thread_mgr::shutdown_threads(None, None, None);
        TEST_POOL.get().unwrap().queue_job(j.clone());
    }
}
