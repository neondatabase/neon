use utils::seqwait;

/// `seqwait::MonotonicCounter` impl
#[derive(Copy, Clone)]
pub struct UsizeCounter(usize);

impl UsizeCounter {
    pub fn new(inital: usize) -> Self {
        UsizeCounter(inital)
    }
}

impl seqwait::MonotonicCounter<usize> for UsizeCounter {
    fn cnt_advance(&mut self, new_val: usize) {
        assert!(self.0 < new_val);
        self.0 = new_val;
    }

    fn cnt_value(&self) -> usize {
        self.0
    }
}
