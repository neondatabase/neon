use std::collections::VecDeque;
use std::time::Duration;

use crate::thread_mgr::shutdown_watcher;
use tokio::sync::mpsc::{Receiver, Sender, channel};
use tokio::time::sleep;

use super::worker::{Job, Worker, Report};

#[derive(Debug)]
pub struct Sched<J: Job> {
    /// Idle workers
    workers: Vec<Worker<J>>,

    /// Queued due jobs
    jobs: VecDeque<J>,

    /// Channel for registering due jobs
    pub send_work: Sender<J>, // TODO should job specify report destination?
    recv_work: Receiver<J>,

    /// Channel for enlisting idle workers
    recv_worker: Receiver<Worker<J>>,

    /// Channel where workers report results
    recv_report: Receiver<Report<J>>,
}

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

impl<J: Job> Sched<J> {
    pub fn new() -> (Sched<J>, Spawner<J>) {
        let worker = channel::<Worker<J>>(100);
        let work = channel::<J>(100);
        let report = channel::<Report<J>>(100);

        let sched = Sched {
            workers: vec![],
            jobs: VecDeque::new(),
            recv_worker: worker.1,
            send_work: work.0,
            recv_work: work.1,
            recv_report: report.1,
        };
        let spawner = Spawner {
            send_worker: worker.0,
            send_report: report.0,
        };

        (sched, spawner)
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

    pub async fn handle_report(&mut self, report: Report<J>) {
        // Reschedule job to run again
        let send_work = self.send_work.clone();
        let job = report.for_job;
        match report.result {
            Ok(()) => {
                tokio::spawn(async move {
                    sleep(Duration::from_millis(10)).await;
                    send_work.send(job).await.unwrap();
                });
            },
            Err(e) => {
                // TODO mark chore as blocked
                println!("task panicked");
            }
        }
    }

    pub fn run(mut self) -> anyhow::Result<()> {
        let runtime = tokio::runtime::Builder::new_current_thread()
             .enable_all()
             .build()?;

        runtime.block_on(async {
            loop {
                let shutdown_watcher = shutdown_watcher();
                tokio::select! {
                    _ = shutdown_watcher => break,
                    worker = self.recv_worker.recv() => {
                        let worker = worker.expect("worker channel closed");
                        self.handle_worker(worker).await;
                    },
                    job = self.recv_work.recv() => {
                        let job = job.expect("job channel closed");
                        self.handle_job(job).await;
                    },
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

#[cfg(test)]
mod tests {
    use crate::{jobs::worker::run_worker, thread_mgr::{self, ThreadKind}};
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

    #[tokio::test]
    async fn sched_1() {
        let (sched, spawner) = Sched::<PrintJob>::new();

        let send_work = sched.send_work.clone();

        // Spawn scheduler
        thread_mgr::spawn(
            ThreadKind::GcScheduler,
            None,
            None,
            "gc_scheduler",
            true,
            move || {
                sched.run()
            },
        ).unwrap();

        spawner.spawn_worker();

        // Send a job
        let j = PrintJob {
            to_print: "hello from job".to_string(),
        };
        send_work.send(j.clone()).await.unwrap();

        sleep(Duration::from_millis(100)).await;

        thread_mgr::shutdown_threads(None, None, None);
    }
}
