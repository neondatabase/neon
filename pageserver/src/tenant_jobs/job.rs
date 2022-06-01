
use std::{any::Any, collections::{BinaryHeap, HashMap}, fmt::Debug, hash::Hash, panic::{self, AssertUnwindSafe}, time::Instant};

use super::deadline::Deadline;

pub trait Job: std::fmt::Debug + Send + Clone + PartialOrd + Ord + Hash + 'static {
    type ErrorType;

    fn run(&self) -> Result<Option<Instant>, Self::ErrorType>;

    fn run_safe(&self) -> Result<Option<Instant>, JobError<Self>> {
        match panic::catch_unwind(AssertUnwindSafe(|| self.run())) {
            Ok(Ok(res)) => Ok(res),
            Ok(Err(e)) => Err(JobError::Error(e)),
            Err(e) => Err(JobError::Panic(e)),
        }
    }
}

#[derive(Debug)]
pub enum JobError<J: Job> {
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

#[derive(Debug, Default)]
pub struct JobStatusTable<J: Job>
where
    J::ErrorType: Debug,
{
    /// Complete summary of current state
    status: HashMap<J, JobStatus<J>>,

    /// Index over status for finding the next scheduled job
    queue: BinaryHeap<Deadline<J>>,
}

pub enum TakeResult<J: Job>
where
    J::ErrorType: Debug,
{
    Assigned(Deadline<J>),
    WaitUntil(Instant),
    WaitForJobs,
}

impl<J: Job> JobStatusTable<J>
where
    J::ErrorType: Debug,
{
    pub fn new() -> Self {
        JobStatusTable::<J> {
            status: HashMap::<J, JobStatus<J>>::new(),
            queue: BinaryHeap::<Deadline<J>>::new(),
        }
    }

    pub fn schedule(&mut self, job: J) {
        let scheduled_for = Instant::now();
        self.status.insert(job.clone(), JobStatus::Ready { scheduled_for });
        self.queue.push(Deadline {
            start_by: scheduled_for,
            inner: job,
        });
    }

    pub fn take_job(&mut self, worker_name: String) -> TakeResult<J> {
        if let Some(deadline) = self.queue.peek() {
            if Instant::now() > deadline.start_by {
                let job = self.queue.pop().expect("failed to pop job");
                self.set_status(&job, JobStatus::Running {
                    worker_name,
                    started_at: Instant::now(),
                });
                return TakeResult::Assigned(job);
            } else {
                TakeResult::<J>::WaitUntil(deadline.start_by);
            }
        }
        TakeResult::WaitForJobs
    }

    pub fn report(&mut self, job: Deadline<J>, result: Result<Option<Instant>, JobError<J>>) {
        match result {
            Ok(Some(reschedule_for)) => {
                self.reschedule(&job, reschedule_for);
            }
            Ok(None) => {
                self.status.remove(&job);
            }
            Err(e) => {
                self.set_status(&job, JobStatus::Stuck(e));
                println!("Job errored, thread is ok.");
            }
        }
    }

    fn reschedule(&mut self, job: &J, start_by: Instant) {
        self.set_status(
            &job,
            JobStatus::Ready {
                scheduled_for: start_by,
            },
        );
        self.queue.push(Deadline {
            start_by,
            inner: job.clone(),
        });
    }

    fn set_status(&mut self, job: &J, status: JobStatus<J>) {
        let s = self.status.get_mut(job).expect("status not found");
        *s = status;
    }
}
