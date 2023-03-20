/// Three-state `max` accumulator.
///
/// If it accumulates over 0 or many `Some(T)` values, it is `Accurate` maximum of those values.
/// If a single `None` value is merged, it becomes `Approximate` variant.
///
/// Remove when `Layer::file_size` is no longer an `Option`.
#[derive(Default, Debug)]
pub enum ApproxAccurate<T> {
    Approximate(T),
    Accurate(T),
    #[default]
    Empty,
}

impl<T: Ord + Copy + Default> ApproxAccurate<T> {
    /// `max(a, b)` where the approximate is inflicted receiving a `None`, or infected onwards.
    #[must_use]
    pub fn max(self, next: Option<T>) -> ApproxAccurate<T> {
        use ApproxAccurate::*;
        match (self, next) {
            (Accurate(a) | Approximate(a), None) => Approximate(a),
            (Empty, None) => Approximate(T::default()),
            (Accurate(a), Some(b)) => Accurate(a.max(b)),
            (Approximate(a), Some(b)) => Approximate(a.max(b)),
            (Empty, Some(b)) => Accurate(b),
        }
    }

    pub fn is_approximate(&self) -> bool {
        matches!(self, ApproxAccurate::Approximate(_))
    }

    pub fn accurate(self) -> Option<T> {
        use ApproxAccurate::*;
        match self {
            Accurate(a) => Some(a),
            Empty => Some(T::default()),
            Approximate(_) => None,
        }
    }

    pub fn unwrap_accurate_or(self, default: T) -> T {
        use ApproxAccurate::*;
        match self {
            Accurate(a) => a,
            Approximate(_) => default,
            // Empty is still accurate, just special case for above `max`
            Empty => T::default(),
        }
    }
}
