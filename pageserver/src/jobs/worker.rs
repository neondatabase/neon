//!
//! Worker thread that can be used in a thread pool to process jobs.
//!
use crate::thread_mgr::shutdown_watcher;
use tokio::sync::mpsc::{Sender, channel};

pub trait Job: std::fmt::Debug {
    fn run(&self);
}

#[derive(Debug)]
pub struct Worker<J: Job>(pub Sender<J>);

#[derive(Debug)]
pub struct Report<J: Job> {
    for_job: J,
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
                    let job = j.unwrap();
                    job.run();
                    report.send(Report {
                        for_job: job,
                    }).await.unwrap();
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
