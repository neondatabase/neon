use std::collections::VecDeque;

/// Maintain a sliding window for calculating Max over a period of time.
///
/// The window maintains a queue of samples. Each sample consists of a
/// "value" and the timestamp that it was measured at.
///
/// The queue is ordered by time, newest samples are at the front.
/// When a new sample is added, we delete any older samples in the
/// queue with a lower value, because they cannot affect the max
/// anymore. This means that the queue is always also ordered by value,
/// with the greatest value at the back:
///
/// front     back
///
///          #
///      #   #
///  #   #   #
///  #   #   #
///
///
/// V: Value
/// T: Time unit
#[derive(Debug)]
pub struct SlidingMax<V, T> {
    samples: VecDeque<(V, T)>,
}

impl<V: std::cmp::PartialOrd, T: std::cmp::PartialOrd> SlidingMax<V, T> {
    pub fn new(initial_val: V, initial_time: T) -> SlidingMax<V, T> {
        SlidingMax {
            samples: VecDeque::from([(initial_val, initial_time)]),
        }
    }

    /// Add a new sample to the window.
    ///
    /// We assume that the time is >= the time of any existing sample
    /// in the queue, although we don't check it, and the code still
    /// works without e.g. panicking if you violate that. It just
    /// might not produce the correct result, until the disordered
    /// samples have fallen off the window.
    pub fn add_sample(&mut self, sample: V, time: T) {
        while let Some((v, _t)) = self.samples.front() {
            if sample < *v {
                break;
            } else {
                self.samples.pop_front();
                continue;
            }
        }
        self.samples.push_front((sample, time))
    }

    /// Remove samples older than 'threshold' from the window
    pub fn trim(&mut self, threshold: T) {
        while self.samples.len() >= 2 {
            let (_v, t) = self.samples.back().unwrap();

            if *t < threshold {
                self.samples.pop_back();
            } else {
                break;
            }
        }
    }

    /// Get the current max over the window
    pub fn get_max(&self) -> &V {
        &self.samples.back().unwrap().0
    }
}
