/// Create a reporter for an error that outputs similar to [`anyhow::Error`] with Display with alternative setting.
///
/// It can be used with `anyhow::Error` as well.
///
/// Why would one use this instead of converting to `anyhow::Error` on the spot? Because
/// anyhow::Error would also capture a stacktrace on the spot, which you would later discard after
/// formatting.
///
/// ## Usage
///
/// ```rust
/// #[derive(Debug, thiserror::Error)]
/// enum MyCoolError {
///   #[error("should never happen")]
///   Bad(#[source] std::io::Error),
/// }
///
/// # fn failing_call() -> Result<(), MyCoolError> { Err(MyCoolError::Bad(std::io::ErrorKind::PermissionDenied.into())) }
///
/// # fn main() {
/// use utils::error::report_compact_sources;
///
/// if let Err(e) = failing_call() {
///     let e = report_compact_sources(&e);
///     assert_eq!(format!("{e}"), "should never happen: permission denied");
/// }
/// # }
/// ```
///
/// ## TODO
///
/// When we are able to describe return position impl trait in traits, this should of course be an
/// extension trait. Until then avoid boxing with this more ackward interface.
pub fn report_compact_sources<E: std::error::Error>(e: &E) -> impl std::fmt::Display + '_ {
    struct AnyhowDisplayAlternateAlike<'a, E>(&'a E);

    impl<E: std::error::Error> std::fmt::Display for AnyhowDisplayAlternateAlike<'_, E> {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "{}", self.0)?;

            // why is E a generic parameter here? hope that rustc will see through a default
            // Error::source implementation and leave the following out if there cannot be any
            // sources:
            Sources(self.0.source()).try_for_each(|src| write!(f, ": {}", src))
        }
    }

    struct Sources<'a>(Option<&'a (dyn std::error::Error + 'static)>);

    impl<'a> Iterator for Sources<'a> {
        type Item = &'a (dyn std::error::Error + 'static);

        fn next(&mut self) -> Option<Self::Item> {
            let rem = self.0;

            let next = self.0.and_then(|x| x.source());
            self.0 = next;
            rem
        }
    }

    AnyhowDisplayAlternateAlike(e)
}

#[cfg(test)]
mod tests {
    use super::report_compact_sources;

    #[test]
    fn report_compact_sources_examples() {
        use std::fmt::Write;

        #[derive(Debug, thiserror::Error)]
        enum EvictionError {
            #[error("cannot evict a remote layer")]
            CannotEvictRemoteLayer,
            #[error("stat failed")]
            StatFailed(#[source] std::io::Error),
            #[error("layer was no longer part of LayerMap")]
            LayerNotFound(#[source] anyhow::Error),
        }

        let examples = [
            (
                line!(),
                EvictionError::CannotEvictRemoteLayer,
                "cannot evict a remote layer",
            ),
            (
                line!(),
                EvictionError::StatFailed(std::io::ErrorKind::PermissionDenied.into()),
                "stat failed: permission denied",
            ),
            (
                line!(),
                EvictionError::LayerNotFound(anyhow::anyhow!("foobar")),
                "layer was no longer part of LayerMap: foobar",
            ),
        ];

        let mut s = String::new();

        for (line, example, expected) in examples {
            s.clear();

            write!(s, "{}", report_compact_sources(&example)).expect("string grows");

            assert_eq!(s, expected, "example on line {line}");
        }
    }
}
