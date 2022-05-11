use std::collections::VecDeque;
use std::time::Duration;

use crate::thread_mgr::shutdown_watcher;
use tokio::sync::mpsc::{Receiver, Sender, channel};
use tokio::time::sleep;

use super::worker::{Job, Worker, Report};


#[derive(Debug)]
pub struct Sched<J: Job> {
    pub worker: (Sender<Worker<J>>, Receiver<Worker<J>>),
    pub work: (Sender<J>, Receiver<J>),
    pub report: (Sender<Report<J>>, Receiver<Report<J>>),
}

impl<J: Job> Sched<J> {
    pub fn new() -> Self {
        Sched {
            worker: channel::<Worker<J>>(100),
            work: channel::<J>(100),
            report: channel::<Report<J>>(100),
        }
    }

    pub fn run(mut self) -> anyhow::Result<()> {
        let runtime = tokio::runtime::Builder::new_current_thread()
             .enable_all()
             .build()?;

        runtime.block_on(async {
            let mut workers = Vec::<Worker<J>>::new();
            let mut jobs = VecDeque::<J>::new();
            loop {
                let shutdown_watcher = shutdown_watcher();
                tokio::select! {
                    _ = shutdown_watcher => break,
                    worker = self.worker.1.recv() => {
                        // Assign to next job in queue, if nonempty
                        if let Some(j) = jobs.pop_front() {
                            worker.unwrap().0.send(j).await.unwrap();
                        } else {
                            workers.push(worker.unwrap());
                        }
                    },
                    job = self.work.1.recv() => {
                        // Assign to first worker in pool, if nonempty
                        if let Some(w) = workers.pop() {
                            w.0.send(job.unwrap()).await.unwrap();
                        } else {
                            jobs.push_back(job.unwrap());
                        }
                    },
                    report = self.report.1.recv() => {
                        // Reschedule job to run again
                        let send_work = self.work.0.clone();
                        let job = report.unwrap().for_job;
                        tokio::spawn(async move {
                            sleep(Duration::from_millis(10)).await;
                            send_work.send(job).await.unwrap();
                        });
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
        let s = Sched::<PrintJob>::new();

        let send_work = s.work.0.clone();

        // Used for workers
        let enlist = s.worker.0.clone();
        let report = s.report.0.clone();

        // Spawn scheduler
        thread_mgr::spawn(
            ThreadKind::GcScheduler,
            None,
            None,
            "gc_scheduler",
            true,
            move || {
                s.run()
            },
        ).unwrap();

        // Spawn worker 1
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

        // Send a job
        let j = PrintJob {
            to_print: "hello from job".to_string(),
        };
        send_work.send(j.clone()).await.unwrap();

        sleep(Duration::from_millis(100)).await;

        thread_mgr::shutdown_threads(None, None, None);
    }
}
