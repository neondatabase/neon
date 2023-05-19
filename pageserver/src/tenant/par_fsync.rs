use std::{
    io,
    path::{Path, PathBuf},
    sync::atomic::{AtomicUsize, Ordering},
};

use futures::{stream, StreamExt};

use crate::virtual_file::VirtualFile;

fn fsync_path(path: &Path) -> io::Result<()> {
    let file = VirtualFile::open(path)?;
    file.sync_all()
}

fn parallel_worker(paths: &[PathBuf], next_path_idx: &AtomicUsize) -> io::Result<()> {
    while let Some(path) = paths.get(next_path_idx.fetch_add(1, Ordering::Relaxed)) {
        fsync_path(path)?;
    }

    Ok(())
}

fn fsync_in_thread_pool(paths: &[PathBuf]) -> io::Result<()> {
    // TODO: remove this function in favor of `fsync_in_tokio_worker` once we asyncify everything.

    /// Use at most this number of threads.
    /// Increasing this limit will
    /// - use more memory
    /// - increase the cost of spawn/join latency
    const MAX_NUM_THREADS: usize = 64;
    let num_threads = paths.len().min(MAX_NUM_THREADS);
    let next_path_idx = AtomicUsize::new(0);

    crossbeam_utils::thread::scope(|s| -> io::Result<()> {
        let mut handles = vec![];
        // Spawn `num_threads - 1`, as the current thread is also a worker.
        for _ in 1..num_threads {
            handles.push(s.spawn(|_| parallel_worker(paths, &next_path_idx)));
        }

        parallel_worker(paths, &next_path_idx)?;

        for handle in handles {
            handle.join().unwrap()?;
        }

        Ok(())
    })
    .unwrap()
}

async fn fsync_in_tokio_worker(paths: &[PathBuf]) -> io::Result<()> {
    const MAX_CONCURRENT_FSYNC: usize = 64;
    let mut s = stream::iter(paths.to_vec())
        .map(|path| async move { tokio::task::spawn_blocking(move || fsync_path(&path)).await })
        .buffer_unordered(MAX_CONCURRENT_FSYNC);
    while let Some(res) = s.next().await {
        res??;
    }
    Ok(())
}

/// Parallel fsync all files. Can be used in non-async context as it is using rayon thread pool.
pub fn par_fsync(paths: &[PathBuf]) -> io::Result<()> {
    const PARALLEL_PATH_THRESHOLD: usize = 1;
    if paths.len() <= PARALLEL_PATH_THRESHOLD {
        for path in paths {
            fsync_path(path)?;
        }
        return Ok(());
    }

    fsync_in_thread_pool(paths)
}

/// Parallel fsync asynchronously. If number of files are less than PARALLEL_PATH_THRESHOLD, fsync is done in the current
/// execution thread. Otherwise, we will spawn_blocking and run it in tokio.
pub async fn par_fsync_async(paths: &[PathBuf]) -> io::Result<()> {
    const PARALLEL_PATH_THRESHOLD: usize = 1;
    if paths.len() <= PARALLEL_PATH_THRESHOLD {
        for path in paths {
            tokio::task::block_in_place(|| fsync_path(path))?;
        }
        return Ok(());
    }

    fsync_in_tokio_worker(paths).await
}
