use std::collections::{HashMap, VecDeque};
use std::ops::Add;
use tokio::time::{Duration, Instant};

use crate::thread_mgr::shutdown_watcher;
use tokio::sync::Mutex;
use tokio::sync::mpsc::{Receiver, Sender, channel};
use tokio::time::{sleep, sleep_until};

use super::worker::{Job, Worker, Report};

pub struct Spawner<J: Job> {
    send_worker: Sender<Worker<J>>,
    send_report: Sender<Report<J>>,
}

impl<J: Job> Spawner<J> {
    pub fn spawn_worker(&self) {
        use crate::{jobs::worker::run_worker, thread_mgr::{self, ThreadKind}};

        let enlist = self.send_worker.clone();
        let report = self.send_report.clone();
        thread_mgr::spawn(
            ThreadKind::GcWorker,
            None,
            None,
            "gc_worker_1",
            true,
            move || {
                run_worker(enlist, report)
            },
        ).unwrap();
    }
}

#[derive(Debug)]
pub enum Status {
    Scheduled,
    Running,
    Stuck,
}

pub struct Scheduler<J: Job> {
    send_work: Sender<J>,
    recv_report: Receiver<Report<J>>,

    pub period: Duration,
    pub chores: Mutex<HashMap<J, Status>>,
}

impl<J: Job> Scheduler<J> {
    pub async fn handle_report(&mut self, report: Report<J>) {
        let job = report.for_job;
        match report.result {
            Ok(()) => {
                // Reschedule job to run again
                if let Some(status) = self.chores.lock().await.get_mut(&job) {
                    *status = Status::Scheduled;
                }
            },
            Err(e) => {
                // Remember error that got job stuck
                println!("task panicked");
                if let Some(status) = self.chores.lock().await.get_mut(&job) {
                    *status = Status::Stuck;
                }
            }
        }
    }

    pub fn run(mut self) -> anyhow::Result<()> {
        let runtime = tokio::runtime::Builder::new_current_thread()
             .enable_all()
             .build()?;

        runtime.block_on(async {
            let mut next_iteration = Instant::now();
            loop {
                tokio::select! {
                    _ = shutdown_watcher() => break,
                    _ = sleep_until(next_iteration) => {
                        next_iteration = Instant::now().add(self.period);
                        for (job, status) in self.chores.lock().await.iter_mut() {
                            if matches!(status, Status::Scheduled) {
                                self.send_work.send(job.clone()).await.unwrap();
                                *status = Status::Running;
                            }
                        }
                    }
                    report = self.recv_report.recv() => {
                        let report = report.expect("report channel closed");
                        self.handle_report(report).await;
                    }
                }
            }
        });

        Ok(())
    }
}

#[derive(Debug)]
pub struct Board<J: Job> {
    workers: Vec<Worker<J>>,
    jobs: VecDeque<J>,
    recv_work: Receiver<J>,
    recv_worker: Receiver<Worker<J>>,
}

impl<J: Job> Board<J> {
    pub fn new() -> (Board<J>, Spawner<J>, Scheduler<J>) {
        let worker = channel::<Worker<J>>(100);
        let work = channel::<J>(100);
        let report = channel::<Report<J>>(100);

        let board = Board {
            workers: vec![],
            jobs: VecDeque::new(),
            recv_worker: worker.1,
            recv_work: work.1,
        };
        let spawner = Spawner {
            send_worker: worker.0,
            send_report: report.0,
        };
        let scheduler = Scheduler {
            send_work: work.0,
            recv_report: report.1,
            period: Duration::from_millis(10),
            chores: Mutex::new(HashMap::new()),
        };

        (board, spawner, scheduler)
    }


    pub async fn handle_job(&mut self, job: J) {
        // Assign to a worker if any are availabe
        while let Some(w) = self.workers.pop() {
            if let Ok(()) = w.0.send(job.clone()).await {
                return;
            }
        }
        self.jobs.push_back(job);
    }

    pub async fn handle_worker(&mut self, worker: Worker<J>) {
        // Assign jobs if any are queued
        if let Some(j) = self.jobs.pop_front() {
            worker.0.send(j).await.ok();
        } else {
            self.workers.push(worker);
        }
    }

    pub fn run(mut self) -> anyhow::Result<()> {
        let runtime = tokio::runtime::Builder::new_current_thread()
             .enable_all()
             .build()?;

        runtime.block_on(async {
            loop {
                tokio::select! {
                    _ = shutdown_watcher() => break,
                    worker = self.recv_worker.recv() => {
                        let worker = worker.expect("worker channel closed");
                        self.handle_worker(worker).await;
                    },
                    job = self.recv_work.recv() => {
                        let job = job.expect("job channel closed");
                        self.handle_job(job).await;
                    },
                }
            }
        });

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use crate::thread_mgr::{self, ThreadKind};
    use super::*;

    #[derive(Debug, Clone, Eq, PartialEq, Hash)]
    struct PrintJob {
        to_print: String
    }

    impl Job for PrintJob {
        fn run(&self) {
            println!("{}", self.to_print);
        }
    }

    #[tokio::test]
    async fn sched_1() {
        let (board, spawner, scheduler) = Board::<PrintJob>::new();

        // Schedule recurring job
        let j = PrintJob {
            to_print: "hello from job".to_string(),
        };
        scheduler.chores.lock().await.insert(j, Status::Scheduled);

        // Spawn board
        thread_mgr::spawn(
            ThreadKind::GcScheduler,
            None,
            None,
            "gc_scheduler",
            true,
            move || {
                board.run()
            },
        ).unwrap();

        // Spawn scheduler
        thread_mgr::spawn(
            ThreadKind::GcScheduler,
            None,
            None,
            "gc_scheduler",
            true,
            move || {
                scheduler.run()
            },
        ).unwrap();

        // Spawn worker
        spawner.spawn_worker();

        // Wait for job to run a few times
        sleep(Duration::from_millis(100)).await;

        // Cleanup
        thread_mgr::shutdown_threads(None, None, None);
    }
}
