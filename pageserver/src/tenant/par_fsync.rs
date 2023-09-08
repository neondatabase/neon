use std::{
    io,
    path::{Path, PathBuf},
    sync::atomic::{AtomicUsize, Ordering},
};

fn fsync_path(path: &Path) -> io::Result<()> {
    // TODO use VirtualFile::fsync_all once we fully go async.
    let file = std::fs::File::open(path)?;
    file.sync_all()
}

fn parallel_worker(paths: &[PathBuf], next_path_idx: &AtomicUsize) -> io::Result<()> {
    while let Some(path) = paths.get(next_path_idx.fetch_add(1, Ordering::Relaxed)) {
        fsync_path(path)?;
    }

    Ok(())
}

fn fsync_in_thread_pool(paths: &[PathBuf]) -> io::Result<()> {
    // TODO: remove this function in favor of `par_fsync_async` once we asyncify everything.

    /// Use at most this number of threads.
    /// Increasing this limit will
    /// - use more memory
    /// - increase the cost of spawn/join latency
    const MAX_NUM_THREADS: usize = 64;
    let num_threads = paths.len().min(MAX_NUM_THREADS);
    let next_path_idx = AtomicUsize::new(0);

    std::thread::scope(|s| -> io::Result<()> {
        let mut handles = vec![];
        // Spawn `num_threads - 1`, as the current thread is also a worker.
        for _ in 1..num_threads {
            handles.push(s.spawn(|| parallel_worker(paths, &next_path_idx)));
        }

        parallel_worker(paths, &next_path_idx)?;

        for handle in handles {
            handle.join().unwrap()?;
        }

        Ok(())
    })
}

/// Parallel fsync all files. Can be used in non-async context as it is using rayon thread pool.
pub fn par_fsync(paths: &[PathBuf]) -> io::Result<()> {
    if paths.len() == 1 {
        fsync_path(&paths[0])?;
        return Ok(());
    }

    fsync_in_thread_pool(paths)
}

/// Parallel fsync asynchronously. If number of files are less than PARALLEL_PATH_THRESHOLD, fsync is done in the current
/// execution thread. Otherwise, we will spawn_blocking and run it in tokio.
pub async fn par_fsync_async(paths: &[PathBuf]) -> io::Result<()> {
    const MAX_CONCURRENT_FSYNC: usize = 64;
    let mut next = paths.iter().peekable();
    let mut js = tokio::task::JoinSet::new();
    loop {
        while js.len() < MAX_CONCURRENT_FSYNC && next.peek().is_some() {
            let next = next.next().expect("just peeked");
            let next = next.to_owned();
            js.spawn_blocking(move || fsync_path(&next));
        }

        // now the joinset has been filled up, wait for next to complete
        if let Some(res) = js.join_next().await {
            res??;
        } else {
            // last item had already completed
            assert!(
                next.peek().is_none(),
                "joinset emptied, we shouldn't have more work"
            );
            return Ok(());
        }
    }
}
