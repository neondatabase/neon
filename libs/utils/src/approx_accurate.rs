/// Three-state `max` accumulator.
///
/// If it accumulates over 0 or many `Some(T)` values, it is `Accurate` maximum of those values.
/// If a single `None` value is merged, it becomes `Approximate` variant.
///
/// Remove when `Layer::file_size` is no longer an `Option`.
#[derive(Default, Debug, Clone, Copy)]
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

#[cfg(test)]
mod tests {
    use super::ApproxAccurate;

    #[test]
    fn accumulate_only_some() {
        let acc = (0..=5)
            .into_iter()
            .map(Some)
            .fold(ApproxAccurate::default(), |acc, next| acc.max(next));

        assert_eq!(acc.accurate(), Some(5));
        assert!(!acc.is_approximate());
        assert_eq!(acc.unwrap_accurate_or(42), 5);
    }

    #[test]
    fn accumulate_some_and_none() {
        let acc = [Some(0), None, Some(2)]
            .into_iter()
            .fold(ApproxAccurate::default(), |acc, next| acc.max(next));

        assert_eq!(acc.accurate(), None);
        assert!(acc.is_approximate());
        assert_eq!(acc.unwrap_accurate_or(42), 42);
    }

    #[test]
    fn accumulate_none_and_some() {
        let acc = [None, Some(1), None]
            .into_iter()
            .fold(ApproxAccurate::default(), |acc, next| acc.max(next));

        assert_eq!(acc.accurate(), None);
        assert!(acc.is_approximate());
        assert_eq!(acc.unwrap_accurate_or(42), 42);
    }

    #[test]
    fn accumulate_none() {
        let acc = ApproxAccurate::<i32>::default();

        // it is accurate empty
        assert_eq!(acc.accurate(), Some(0));
        assert!(!acc.is_approximate());
        assert_eq!(acc.unwrap_accurate_or(42), 0);
    }
}
