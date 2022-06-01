use lazy_static::lazy_static;
use metrics::{register_gauge_vec, GaugeVec};
use std::{
    fmt::Debug,
    sync::{Arc, Condvar, Mutex},
    time::Instant,
};

use crate::thread_mgr::{get_shutdown_aware_condvar, is_shutdown_requested};

use super::job::{Job, JobStatusTable};

lazy_static! {
    static ref POOL_UTILIZATION_GAUGE: GaugeVec = register_gauge_vec!(
        "pageserver_pool_utilization",
        "Number of bysy workers",
        &["pool_name"]
    )
    .expect("Failed to register safekeeper_commit_lsn gauge vec");
}

#[derive(Debug)]
pub struct Pool<J: Job>
where
    J::ErrorType: Debug,
{
    job_table: Mutex<JobStatusTable<J>>,
    condvar: Arc<Condvar>, // Notified when idle worker should wake up
}

impl<J: Job> Default for Pool<J>
where
    J::ErrorType: Debug,
{
    fn default() -> Self {
        Self::new()
    }
}

impl<J: Job> Pool<J>
where
    J::ErrorType: Debug,
{
    pub fn new() -> Self {
        Pool {
            job_table: Mutex::new(JobStatusTable::<J>::new()),
            condvar: get_shutdown_aware_condvar(),
        }
    }

    pub fn worker_main(&self, worker_name: String) -> anyhow::Result<()> {
        let mut job_table = self.job_table.lock().unwrap();
        while !is_shutdown_requested() {
            match job_table.take_job(worker_name.clone()) {
                super::job::TakeResult::Assigned(job) => {
                    // Run job without holding lock
                    drop(job_table);
                    POOL_UTILIZATION_GAUGE
                        .with_label_values(&["todo_put_pool_name_here"])
                        .inc();
                    let result = job.run_safe();
                    POOL_UTILIZATION_GAUGE
                        .with_label_values(&["todo_put_pool_name_here"])
                        .dec();
                    job_table = self.job_table.lock().unwrap();

                    // Reschedule or report error
                    job_table.report(job, result);
                }
                super::job::TakeResult::WaitUntil(time) => {
                    let wait_time = time.duration_since(Instant::now());
                    job_table = self.condvar.wait_timeout(job_table, wait_time).unwrap().0;
                }
                super::job::TakeResult::WaitForJobs => {
                    job_table = self.condvar.wait(job_table).unwrap();
                }
            }
        }

        Ok(())
    }

    pub fn queue_job(&self, job: J) {
        let mut job_table = self.job_table.lock().unwrap();
        job_table.schedule(job);
        self.condvar.notify_all();
    }
}

#[cfg(test)]
mod tests {
    use std::{ops::Add, time::Duration};

    use once_cell::sync::OnceCell;

    use super::*;
    use crate::thread_mgr::{self, ThreadKind};

    #[derive(Debug, Clone, Eq, PartialEq, PartialOrd, Ord, Hash)]
    struct PrintJob {
        to_print: String,
    }

    impl Job for PrintJob {
        type ErrorType = String;

        fn run(&self) -> Result<Option<Instant>, String> {
            if self.to_print == "pls panic" {
                panic!("AAA");
            }
            println!("{}", self.to_print);
            Ok(Some(Instant::now().add(Duration::from_millis(10))))
        }
    }

    static TEST_POOL: OnceCell<Pool<PrintJob>> = OnceCell::new();

    #[tokio::test]
    async fn pool_1() {
        TEST_POOL.set(Pool::<PrintJob>::new()).unwrap();

        thread_mgr::spawn(
            ThreadKind::GarbageCollectionWorker,
            None,
            None,
            "test_worker_1",
            true,
            move || TEST_POOL.get().unwrap().worker_main("test_worker_1".into()),
        )
        .unwrap();

        thread_mgr::spawn(
            ThreadKind::GarbageCollectionWorker,
            None,
            None,
            "test_worker_2",
            true,
            move || TEST_POOL.get().unwrap().worker_main("test_worker_2".into()),
        )
        .unwrap();

        TEST_POOL.get().unwrap().queue_job(PrintJob {
            to_print: "hello from job".to_string(),
        });

        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}
