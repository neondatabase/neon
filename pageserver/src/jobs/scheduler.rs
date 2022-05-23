use std::collections::VecDeque;
use std::time::Duration;

use crate::thread_mgr::shutdown_watcher;
use tokio::sync::mpsc::{Receiver, Sender, channel};
use tokio::time::sleep;

use super::worker::{Job, Worker, Report, Work};

// TODO spawn a tokio task for each tenant
// x) Why not use the simpler worker implementation then?

#[derive(Debug)]
pub struct Sched<J: Job> {
    /// Idle workers
    workers: Vec<Worker<J>>,

    /// Queued due jobs
    work_queue: VecDeque<Work<J>>,

    /// Channel for registering due jobs
    pub send_work: Sender<Work<J>>,
    recv_work: Receiver<Work<J>>,

    /// Channel for enlisting idle workers
    recv_worker: Receiver<Worker<J>>,
}

pub struct Spawner<J: Job> {
    send_worker: Sender<Worker<J>>,
}

impl<J: Job> Spawner<J> {
    pub fn spawn_worker(&self) {
        use crate::{jobs::worker::run_worker, thread_mgr::{self, ThreadKind}};

        let enlist = self.send_worker.clone();
        thread_mgr::spawn(
            ThreadKind::GcWorker,
            None,
            None,
            "gc_worker_1",
            true,
            move || {
                run_worker(enlist)
            },
        ).unwrap();
    }
}

impl<J: Job> Sched<J> {
    pub fn new() -> (Sched<J>, Spawner<J>) {
        let worker = channel::<Worker<J>>(100);
        let work = channel::<Work<J>>(100);

        let sched = Sched {
            workers: vec![],
            work_queue: VecDeque::new(),
            recv_worker: worker.1,
            send_work: work.0,
            recv_work: work.1,
        };
        let spawner = Spawner {
            send_worker: worker.0,
        };

        (sched, spawner)
    }

    pub async fn handle_work(&mut self, work: Work<J>) {
        // Assign to a worker if any are availabe
        while let Some(worker) = self.workers.pop() {
            if let Ok(()) = worker.0.send(work.clone()).await {
                return;
            }
        }
        self.work_queue.push_back(work);
    }

    pub async fn handle_worker(&mut self, worker: Worker<J>) {
        // Assign jobs if any are queued
        if let Some(j) = self.work_queue.pop_front() {
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
                let shutdown_watcher = shutdown_watcher();
                tokio::select! {
                    _ = shutdown_watcher => break,
                    worker = self.recv_worker.recv() => {
                        let worker = worker.expect("worker channel closed");
                        self.handle_worker(worker).await;
                    },
                    work = self.recv_work.recv() => {
                        let work = work.expect("job channel closed");
                        self.handle_work(work).await;
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
        let when_done = channel::<Report<PrintJob>>(100);
        let work = Work {
            job: PrintJob {
                to_print: "hello from job".to_string(),
            },
            when_done: when_done.0,
        };
        send_work.send(work.clone()).await.unwrap();

        sleep(Duration::from_millis(100)).await;

        thread_mgr::shutdown_threads(None, None, None);
    }
}
