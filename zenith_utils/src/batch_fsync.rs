use crossbeam_utils::thread;

use std::{
    fs::File,
    io,
    path::PathBuf,
    sync::atomic::{AtomicUsize, Ordering},
};

pub fn batch_fsync(paths: &[PathBuf]) -> std::io::Result<()> {
    let next = AtomicUsize::new(0);

    let num_threads = std::cmp::min(paths.len() / 2, 256);

    if num_threads <= 1 {
        for path in paths {
            let file = File::open(&path)?;
            file.sync_all()?;
        }

        return Ok(());
    }

    thread::scope(|s| -> io::Result<()> {
        let mut handles = Vec::new();
        for _ in 0..num_threads {
            handles.push(s.spawn(|_| loop {
                let idx = next.fetch_add(1, Ordering::Relaxed);
                if idx >= paths.len() {
                    return io::Result::Ok(());
                }

                let file = File::open(&paths[idx])?;
                file.sync_all()?;
            }));
        }

        for handle in handles {
            handle.join().unwrap()?;
        }

        Ok(())
    })
    .unwrap()
}
