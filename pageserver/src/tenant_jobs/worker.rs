use lazy_static::lazy_static;
use metrics::{register_gauge_vec, GaugeVec};
use std::{
    any::Any,
    collections::{BinaryHeap, HashMap},
    fmt::Debug,
    hash::Hash,
    panic::{self, AssertUnwindSafe},
    sync::{Arc, Condvar, Mutex},
    time::Instant,
};

use crate::thread_mgr::{get_shutdown_aware_condvar, is_shutdown_requested};

lazy_static! {
    static ref POOL_UTILIZATION_GAUGE: GaugeVec = register_gauge_vec!(
        "pageserver_pool_utilization",
        "Number of bysy workers",
        &["pool_name"]
    )
    .expect("Failed to register safekeeper_commit_lsn gauge vec");
}

pub trait Job: std::fmt::Debug + Send + Clone + PartialOrd + Ord + Hash + 'static {
    type ErrorType;
    fn run(&self) -> Result<Option<Instant>, Self::ErrorType>;
}

#[derive(Debug)]
enum JobError<J: Job> {
    Panic(Box<dyn Any + Send>),
    Error(J::ErrorType),
}

#[derive(Debug)]
enum JobStatus<J: Job>
where
    J::ErrorType: Debug,
{
    Ready {
        #[allow(dead_code)]
        scheduled_for: Instant,
    },
    Running {
        #[allow(dead_code)]
        worker_name: String,

        #[allow(dead_code)]
        started_at: Instant,
    },
    Stuck(JobError<J>),
}

// TODO make this generic event, put in different module
#[derive(Debug)]
struct Deadline<J: Job>
where
    J::ErrorType: Debug,
{
    start_by: Instant,
    job: J,
}

impl<J: Job> PartialOrd for Deadline<J>
where
    J::ErrorType: Debug,
{
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        other.start_by.partial_cmp(&self.start_by)
    }
}

impl<J: Job> Ord for Deadline<J>
where
    J::ErrorType: Debug,
{
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        other.start_by.cmp(&self.start_by)
    }
}

impl<J: Job> PartialEq for Deadline<J>
where
    J::ErrorType: Debug,
{
    fn eq(&self, other: &Self) -> bool {
        self.start_by == other.start_by
    }
}

impl<J: Job> Eq for Deadline<J> where J::ErrorType: Debug {}

#[derive(Debug, Default)]
struct JobStatusTable<J: Job>
where
    J::ErrorType: Debug,
{
    /// Complete summary of current state
    status: HashMap<J, JobStatus<J>>,

    /// Index over status for finding the next scheduled job
    queue: BinaryHeap<Deadline<J>>,
}

impl<J: Job> JobStatusTable<J>
where
    J::ErrorType: Debug,
{
    fn pop_due(&mut self) -> Option<Deadline<J>> {
        if let Some(deadline) = self.queue.peek() {
            if Instant::now() > deadline.start_by {
                return self.queue.pop();
            }
        }
        None
    }

    fn set_status(&mut self, job: &J, status: JobStatus<J>) {
        let s = self.status.get_mut(job).expect("status not found");
        *s = status;
    }
}

#[derive(Debug)]
pub struct Pool<J: Job>
where
    J::ErrorType: Debug,
{
    job_table: Mutex<JobStatusTable<J>>,
    condvar: Arc<Condvar>, // Notified when idle worker should wake up
}

impl<J: Job> Pool<J>
where
    J::ErrorType: Debug,
{
    pub fn new() -> Self {
        Pool {
            job_table: Mutex::new(JobStatusTable::<J> {
                status: HashMap::<J, JobStatus<J>>::new(),
                queue: BinaryHeap::<Deadline<J>>::new(),
            }),
            condvar: get_shutdown_aware_condvar(),
        }
    }

    pub fn worker_main(&self, worker_name: String) -> anyhow::Result<()> {
        let mut job_table = self.job_table.lock().unwrap();
        while !is_shutdown_requested() {
            if let Some(Deadline { job, .. }) = job_table.pop_due() {
                job_table.set_status(
                    &job,
                    JobStatus::Running {
                        worker_name: worker_name.clone(),
                        started_at: Instant::now(),
                    },
                );

                // Run job without holding lock
                drop(job_table);
                POOL_UTILIZATION_GAUGE
                    .with_label_values(&["todo_put_pool_name_here"])
                    .inc();
                let result = panic::catch_unwind(AssertUnwindSafe(|| job.run()));
                POOL_UTILIZATION_GAUGE
                    .with_label_values(&["todo_put_pool_name_here"])
                    .dec();
                job_table = self.job_table.lock().unwrap();

                // Update job status
                match result {
                    Ok(Ok(Some(reschedule_for))) => {
                        job_table.set_status(
                            &job,
                            JobStatus::Ready {
                                scheduled_for: reschedule_for,
                            },
                        );
                        job_table.queue.push(Deadline {
                            job: job.clone(),
                            start_by: reschedule_for,
                        });
                    }
                    Ok(Ok(None)) => {
                        job_table.status.remove(&job);
                    }
                    Ok(Err(e)) => {
                        job_table.set_status(&job, JobStatus::Stuck(JobError::Error(e)));
                        println!("Job errored, thread is ok.");
                    }
                    Err(e) => {
                        job_table.set_status(&job, JobStatus::Stuck(JobError::Panic(e)));
                        println!("Job panicked, thread is ok.");
                    }
                }
            } else {
                match job_table.queue.peek() {
                    Some(deadline) => {
                        let wait_time = deadline.start_by.duration_since(Instant::now());
                        job_table = self.condvar.wait_timeout(job_table, wait_time).unwrap().0;
                    }
                    None => {
                        job_table = self.condvar.wait(job_table).unwrap();
                    }
                }
            }
        }

        Ok(())
    }

    pub fn queue_job(&self, job: J) {
        let mut job_table = self.job_table.lock().unwrap();
        let scheduled_for = Instant::now();
        job_table
            .status
            .insert(job.clone(), JobStatus::Ready { scheduled_for });
        job_table.queue.push(Deadline {
            job,
            start_by: scheduled_for,
        });

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
