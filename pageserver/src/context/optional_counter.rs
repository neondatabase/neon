use std::{
    sync::atomic::{AtomicU32, Ordering},
    time::Duration,
};

#[derive(Debug)]
pub struct CounterU32 {
    inner: AtomicU32,
}
impl Default for CounterU32 {
    fn default() -> Self {
        Self {
            inner: AtomicU32::new(u32::MAX),
        }
    }
}
impl CounterU32 {
    pub fn open(&self) -> Result<(), &'static str> {
        match self
            .inner
            .compare_exchange(u32::MAX, 0, Ordering::Relaxed, Ordering::Relaxed)
        {
            Ok(_) => Ok(()),
            Err(_) => Err("open() called on clsoed state"),
        }
    }
    pub fn close(&self) -> Result<u32, &'static str> {
        match self.inner.swap(u32::MAX, Ordering::Relaxed) {
            u32::MAX => Err("close() called on closed state"),
            x => Ok(x),
        }
    }

    pub fn add(&self, count: u32) -> Result<(), &'static str> {
        if count == 0 {
            return Ok(());
        }
        let mut had_err = None;
        self.inner
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |cur| match cur {
                u32::MAX => {
                    had_err = Some("add() called on closed state");
                    None
                }
                x => {
                    let (new, overflowed) = x.overflowing_add(count);
                    if new == u32::MAX || overflowed {
                        had_err = Some("add() overflowed the counter");
                        None
                    } else {
                        Some(new)
                    }
                }
            })
            .map_err(|_| had_err.expect("we set it whenever the function returns None"))
            .map(|_| ())
    }
}

#[derive(Default, Debug)]
pub struct MicroSecondsCounterU32 {
    inner: CounterU32,
}

impl MicroSecondsCounterU32 {
    pub fn open(&self) -> Result<(), &'static str> {
        self.inner.open()
    }
    pub fn add(&self, duration: Duration) -> Result<(), &'static str> {
        match duration.as_micros().try_into() {
            Ok(x) => self.inner.add(x),
            Err(_) => Err("add(): duration conversion error"),
        }
    }
    pub fn close_and_checked_sub_from(&self, from: Duration) -> Result<Duration, &'static str> {
        let val = self.inner.close()?;
        let val = Duration::from_micros(val as u64);
        let subbed = match from.checked_sub(val) {
            Some(v) => v,
            None => return Err("Duration::checked_sub"),
        };
        Ok(subbed)
    }
}

#[cfg(test)]
mod tests {

    use super::*;

    #[test]
    fn test_basic() {
        let counter = MicroSecondsCounterU32::default();
        counter.open().unwrap();
        counter.add(Duration::from_micros(23)).unwrap();
        let res = counter
            .close_and_checked_sub_from(Duration::from_micros(42))
            .unwrap();
        assert_eq!(res, Duration::from_micros(42 - 23));
    }
}
