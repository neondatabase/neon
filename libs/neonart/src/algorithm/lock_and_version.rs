use std::sync::atomic::{AtomicU64, Ordering};

pub(crate) struct AtomicLockAndVersion {
    inner: AtomicU64,
}

impl AtomicLockAndVersion {
    pub(crate) fn new() -> AtomicLockAndVersion {
        AtomicLockAndVersion {
            inner: AtomicU64::new(0),
        }
    }
}

pub(crate) type ResultOrRestart<T> = Result<T, ()>;

const fn restart<T>() -> ResultOrRestart<T> {
    Err(())
}

impl AtomicLockAndVersion {
    pub(crate) fn read_lock_or_restart(&self) -> ResultOrRestart<u64> {
        let version = self.await_node_unlocked();
        if is_obsolete(version) {
            return restart();
        }
        Ok(version)
    }

    pub(crate) fn check_or_restart(&self, version: u64) -> ResultOrRestart<()> {
        self.read_unlock_or_restart(version)
    }

    pub(crate) fn read_unlock_or_restart(&self, version: u64) -> ResultOrRestart<()> {
        if self.inner.load(Ordering::Acquire) != version {
            return restart();
        }
        Ok(())
    }

    pub(crate) fn upgrade_to_write_lock_or_restart(&self, version: u64) -> ResultOrRestart<()> {
        if self
            .inner
            .compare_exchange(
                version,
                set_locked_bit(version),
                Ordering::Acquire,
                Ordering::Relaxed,
            )
            .is_err()
        {
            return restart();
        }
        Ok(())
    }

    pub(crate) fn write_unlock(&self) {
        // reset locked bit and overflow into version
        self.inner.fetch_add(2, Ordering::Release);
    }

    pub(crate) fn write_unlock_obsolete(&self) {
        // set obsolete, reset locked, overflow into version
        self.inner.fetch_add(3, Ordering::Release);
    }

    // Helper functions
    fn await_node_unlocked(&self) -> u64 {
        let mut version = self.inner.load(Ordering::Acquire);
        while (version & 2) == 2 {
            // spinlock
            std::thread::yield_now();
            version = self.inner.load(Ordering::Acquire)
        }
        version
    }
}

fn set_locked_bit(version: u64) -> u64 {
    return version + 2;
}

fn is_obsolete(version: u64) -> bool {
    return (version & 1) == 1;
}
