use std::{
    io,
    path::{Path, PathBuf},
    sync::atomic::{AtomicUsize, Ordering},
};

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

pub fn par_fsync(paths: &[PathBuf]) -> io::Result<()> {
    const PARALLEL_PATH_THRESHOLD: usize = 1;
    if paths.len() <= PARALLEL_PATH_THRESHOLD {
        for path in paths {
            fsync_path(path)?;
        }
        return Ok(());
    }

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
