use std::{fs::File, io};

const MAX_PENDING_FILES: usize = 100;

#[derive(Default)]
pub struct BatchFsync {
    pending: Vec<File>,
    done: bool,
}

impl BatchFsync {
    pub fn add(&mut self, file: File) -> io::Result<()> {
        if self.pending.len() == MAX_PENDING_FILES {
            self.sync_batch()?;
        }

        self.pending.push(file);

        Ok(())
    }

    /// Must be called before drop.
    pub fn done(mut self) -> io::Result<()> {
        self.done = true;
        self.sync_batch()
    }

    fn sync_batch(&mut self) -> io::Result<()> {
        // TODO parallelize
        for pending_file in self.pending.drain(..) {
            pending_file.sync_all()?;
        }
        self.pending.clear();

        Ok(())
    }
}

impl Drop for BatchFsync {
    fn drop(&mut self) {
        assert!(self.done);
    }
}
