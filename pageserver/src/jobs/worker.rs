//!
//! Worker thread that can be used in a thread pool to process jobs.
//!
use crate::thread_mgr::shutdown_watcher;
use tokio::sync::mpsc::{Sender, channel};
use std::any::Any;
use std::panic::AssertUnwindSafe;
use std::panic::catch_unwind;

pub trait Job: std::fmt::Debug + Send + 'static + Clone {
    fn run(&self);
}

// TODO make scheduler an async fn, leave rescheduling to chore_mgr
pub struct Work<J: Job> {
    pub job: J,
    pub when_done: Sender<Report<J>>,
}

#[derive(Debug)]
pub struct Worker<J: Job>(pub Sender<J>);

#[derive(Debug)]
pub struct Report<J: Job> {
    pub for_job: J,
    pub result: Result<(), Box<dyn Any + Send>>
}

pub fn run_worker<J: Job>(enlist: Sender<Worker<J>>, report: Sender<Report<J>>) -> anyhow::Result<()> {
    let runtime = tokio::runtime::Builder::new_current_thread()
         .enable_all()
         .build()?;

    runtime.block_on(async {
        loop {
            let (send_work, mut get_work) = channel::<J>(100);
            enlist.send(Worker(send_work)).await.unwrap();

            let shutdown_watcher = shutdown_watcher();
            tokio::select! {
                _ = shutdown_watcher => break,
                j = get_work.recv() => {
                    if let Some(job) = j {
                        let result = catch_unwind(AssertUnwindSafe(|| {
                            job.run();
                        }));
                        report.send(Report {
                            for_job: job,
                            result: result,
                        }).await.unwrap();
                    } else {
                        // channel closed
                        return;
                    }
                }
            };
        }
    });

    Ok(())
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
    async fn worker_1() {
        let mut worker = channel::<Worker<PrintJob>>(100);
        let mut result = channel::<Report<PrintJob>>(100);

        thread_mgr::spawn(
            ThreadKind::GcWorker,
            None,
            None,
            "gc_worker_1",
            true,
            move || {
                run_worker(worker.0, result.0)
            },
        ).unwrap();

        let j = PrintJob {
            to_print: "hello from job".to_string(),
        };
        let w = worker.1.recv().await.unwrap();
        w.0.send(j.clone()).await.unwrap();

        println!("waiting for result");
        let report = result.1.recv().await.unwrap();
        assert_eq!(j, report.for_job);
        println!("got result");

        thread_mgr::shutdown_threads(None, None, None);
    }

    #[test]
    fn worker_cancellation() {
    }
}
