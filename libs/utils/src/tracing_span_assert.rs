//! Assert that the current [`tracing::Span`] has a given set of fields.
//!
//! Only meaningful when tracing has been configured as in example. Absence of
//! `tracing_error::ErrorLayer` is not detected yet.
//!
//! # Usage
//!
//! ```rust
//! # fn main() {
//! use tracing_subscriber::prelude::*;
//! let registry = tracing_subscriber::registry()
//!    .with(tracing_error::ErrorLayer::default());
//!
//! // Register the registry as the global subscriber.
//! // In this example, we'll only use it as a thread-local subscriber.
//! let _guard = tracing::subscriber::set_default(registry);
//!
//! // Then, in the main code:
//!
//! let span = tracing::info_span!("TestSpan", test_id = 1);
//! let _guard = span.enter();
//!
//! // ... down the call stack
//!
//! use utils::tracing_span_assert::{check_fields_present, MultiNameExtractor};
//! let extractor = MultiNameExtractor::new("TestExtractor", ["test", "test_id"]);
//! if let Err(missing) = check_fields_present!([&extractor]) {
//!    // if you copypaste this to a custom assert method, remember to add #[track_caller]
//!    // to get the "user" code location for the panic.
//!    panic!("Missing fields: {missing:?}");
//! }
//! # }
//! ```
//!
//! Recommended reading: https://docs.rs/tracing-subscriber/0.3.16/tracing_subscriber/layer/index.html#per-layer-filtering
//!

#[derive(Debug)]
pub enum ExtractionResult {
    Present,
    Absent,
}

pub trait Extractor: Send + Sync + std::fmt::Debug {
    fn name(&self) -> &str;
    fn extract(&self, fields: &tracing::field::FieldSet) -> ExtractionResult;
}

#[derive(Debug)]
pub struct MultiNameExtractor<const L: usize> {
    name: &'static str,
    field_names: [&'static str; L],
}

impl<const L: usize> MultiNameExtractor<L> {
    pub fn new(name: &'static str, field_names: [&'static str; L]) -> MultiNameExtractor<L> {
        MultiNameExtractor { name, field_names }
    }
}
impl<const L: usize> Extractor for MultiNameExtractor<L> {
    fn name(&self) -> &str {
        self.name
    }
    fn extract(&self, fields: &tracing::field::FieldSet) -> ExtractionResult {
        if fields.iter().any(|f| self.field_names.contains(&f.name())) {
            ExtractionResult::Present
        } else {
            ExtractionResult::Absent
        }
    }
}

/// Checks that the given extractors are satisfied with the current span hierarchy.
///
/// This should not be called directly, but used through [`check_fields_present`] which allows
/// `Summary::Unconfigured` only when the calling crate is being `#[cfg(test)]` as a conservative default.
#[doc(hidden)]
pub fn check_fields_present0<const L: usize>(
    must_be_present: [&dyn Extractor; L],
) -> Result<Summary, Vec<&dyn Extractor>> {
    let mut missing = must_be_present.into_iter().collect::<Vec<_>>();
    let trace = tracing_error::SpanTrace::capture();
    trace.with_spans(|md, _formatted_fields| {
        missing.retain(|extractor| match extractor.extract(md.fields()) {
            ExtractionResult::Present => false,
            ExtractionResult::Absent => true,
        });
        !missing.is_empty() // continue walking up until we've found all missing
    });
    if missing.is_empty() {
        Ok(Summary::FoundEverything)
    } else if !tracing_subscriber_configured() {
        Ok(Summary::Unconfigured)
    } else {
        // we can still hit here if a tracing subscriber has been configured but the ErrorLayer is
        // missing, which can be annoying. for this case, we could probably use
        // SpanTrace::status().
        Err(missing)
    }
}

/// Checks that the given extractors are satisfied with the current span hierarchy.
///
/// The macro is the preferred way of checking if fields exist while passing checks if a test does
/// not have tracing configured.
///
/// Why mangled name? Because #[macro_export] will expose it at utils::__check_fields_present.
/// However we can game a module namespaced macro for `use` purposes by re-exporting the
/// #[macro_export] exported name with an alias (below).
#[doc(hidden)]
#[macro_export]
macro_rules! __check_fields_present {
    ($extractors:expr) => {{
        {
            use $crate::tracing_span_assert::{check_fields_present0, Summary::*, Extractor};

            match dbg!(check_fields_present0($extractors)) {
                Ok(FoundEverything) => Ok(()),
                Ok(Unconfigured) if cfg!(test) => {
                    // allow unconfigured in tests
                    Ok(())
                },
                Ok(Unconfigured) => {
                    panic!("utils::tracing_span_assert: outside of #[cfg(test)] expected tracing to be configured with tracing_error::ErrorLayer")
                },
                Err(missing) => Err(missing)
            }
        }
    }}
}

pub use crate::__check_fields_present as check_fields_present;

/// Explanation for why the check was deemed ok.
///
/// Mainly useful for testing, or configuring per-crate behaviour as in with
/// [`check_fields_present`].
#[derive(Debug)]
pub enum Summary {
    /// All extractors were found.
    ///
    /// Should only happen when tracing is properly configured.
    FoundEverything,

    /// Tracing has not been configured at all. This is ok for tests running without tracing set
    /// up.
    Unconfigured,
}

fn tracing_subscriber_configured() -> bool {
    // SpanTrace::status() is not a strong indicator because it short circuits to EMPTY on empty
    // spans, so let's check if there's an ErrorLayer directly. alas, we cannot due to
    // pub(crate) but we can check if the subscriber is the noop impl.

    let span = tracing::Span::current();
    let mut configured_at_all = false;
    span.with_subscriber(|(_, s)| {
        // assume that if there is a non-NoSubscriber, it's a proper one
        configured_at_all = s
            .downcast_ref::<tracing::subscriber::NoSubscriber>()
            .is_none();
    });

    configured_at_all
}

#[cfg(test)]
mod tests {

    use tracing_subscriber::prelude::*;

    use super::*;

    use std::{
        collections::HashSet,
        fmt::{self},
        hash::{Hash, Hasher},
    };

    struct MemoryIdentity<'a>(&'a dyn Extractor);

    impl<'a> MemoryIdentity<'a> {
        fn as_ptr(&self) -> *const () {
            self.0 as *const _ as *const ()
        }
    }
    impl<'a> PartialEq for MemoryIdentity<'a> {
        fn eq(&self, other: &Self) -> bool {
            self.as_ptr() == other.as_ptr()
        }
    }
    impl<'a> Eq for MemoryIdentity<'a> {}
    impl<'a> Hash for MemoryIdentity<'a> {
        fn hash<H: Hasher>(&self, state: &mut H) {
            self.as_ptr().hash(state);
        }
    }
    impl<'a> fmt::Debug for MemoryIdentity<'a> {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "{:p}: {}", self.as_ptr(), self.0.name())
        }
    }

    struct Setup {
        _current_thread_subscriber_guard: tracing::subscriber::DefaultGuard,
        tenant_extractor: MultiNameExtractor<2>,
        timeline_extractor: MultiNameExtractor<2>,
    }

    fn setup_current_thread() -> Setup {
        let tenant_extractor = MultiNameExtractor::new("TenantId", ["tenant_id", "tenant"]);
        let timeline_extractor = MultiNameExtractor::new("TimelineId", ["timeline_id", "timeline"]);

        let registry = tracing_subscriber::registry()
            .with(tracing_subscriber::fmt::layer())
            .with(tracing_error::ErrorLayer::default());

        let guard = tracing::subscriber::set_default(registry);

        Setup {
            _current_thread_subscriber_guard: guard,
            tenant_extractor,
            timeline_extractor,
        }
    }

    fn assert_missing(missing: Vec<&dyn Extractor>, expected: Vec<&dyn Extractor>) {
        let missing: HashSet<MemoryIdentity> =
            HashSet::from_iter(missing.into_iter().map(MemoryIdentity));
        let expected: HashSet<MemoryIdentity> =
            HashSet::from_iter(expected.into_iter().map(MemoryIdentity));
        assert_eq!(missing, expected);
    }

    #[test]
    fn positive_one_level() {
        let setup = setup_current_thread();
        let span = tracing::info_span!("root", tenant_id = "tenant-1", timeline_id = "timeline-1");
        let _guard = span.enter();
        check_fields_present0([&setup.tenant_extractor, &setup.timeline_extractor]).unwrap();
    }

    #[test]
    fn negative_one_level() {
        let setup = setup_current_thread();
        let span = tracing::info_span!("root", timeline_id = "timeline-1");
        let _guard = span.enter();
        let missing = check_fields_present0([&setup.tenant_extractor, &setup.timeline_extractor])
            .unwrap_err();
        assert_missing(missing, vec![&setup.tenant_extractor]);
    }

    #[test]
    fn positive_multiple_levels() {
        let setup = setup_current_thread();

        let span = tracing::info_span!("root");
        let _guard = span.enter();

        let span = tracing::info_span!("child", tenant_id = "tenant-1");
        let _guard = span.enter();

        let span = tracing::info_span!("grandchild", timeline_id = "timeline-1");
        let _guard = span.enter();

        check_fields_present0([&setup.tenant_extractor, &setup.timeline_extractor]).unwrap();
    }

    #[test]
    fn negative_multiple_levels() {
        let setup = setup_current_thread();

        let span = tracing::info_span!("root");
        let _guard = span.enter();

        let span = tracing::info_span!("child", timeline_id = "timeline-1");
        let _guard = span.enter();

        let missing = check_fields_present0([&setup.tenant_extractor]).unwrap_err();
        assert_missing(missing, vec![&setup.tenant_extractor]);
    }

    #[test]
    fn positive_subset_one_level() {
        let setup = setup_current_thread();
        let span = tracing::info_span!("root", tenant_id = "tenant-1", timeline_id = "timeline-1");
        let _guard = span.enter();
        check_fields_present0([&setup.tenant_extractor]).unwrap();
    }

    #[test]
    fn positive_subset_multiple_levels() {
        let setup = setup_current_thread();

        let span = tracing::info_span!("root");
        let _guard = span.enter();

        let span = tracing::info_span!("child", tenant_id = "tenant-1");
        let _guard = span.enter();

        let span = tracing::info_span!("grandchild", timeline_id = "timeline-1");
        let _guard = span.enter();

        check_fields_present0([&setup.tenant_extractor]).unwrap();
    }

    #[test]
    fn negative_subset_one_level() {
        let setup = setup_current_thread();
        let span = tracing::info_span!("root", timeline_id = "timeline-1");
        let _guard = span.enter();
        let missing = check_fields_present0([&setup.tenant_extractor]).unwrap_err();
        assert_missing(missing, vec![&setup.tenant_extractor]);
    }

    #[test]
    fn negative_subset_multiple_levels() {
        let setup = setup_current_thread();

        let span = tracing::info_span!("root");
        let _guard = span.enter();

        let span = tracing::info_span!("child", timeline_id = "timeline-1");
        let _guard = span.enter();

        let missing = check_fields_present0([&setup.tenant_extractor]).unwrap_err();
        assert_missing(missing, vec![&setup.tenant_extractor]);
    }

    #[test]
    fn tracing_error_subscriber_not_set_up_straight_line() {
        // no setup
        let span = tracing::info_span!("foo", e = "some value");
        let _guard = span.enter();

        let extractor = MultiNameExtractor::new("E", ["e"]);
        let res = check_fields_present0([&extractor]);
        assert!(matches!(res, Ok(Summary::Unconfigured)), "{res:?}");

        // similarly for a not found key
        let extractor = MultiNameExtractor::new("F", ["foobar"]);
        let res = check_fields_present0([&extractor]);
        assert!(matches!(res, Ok(Summary::Unconfigured)), "{res:?}");
    }

    #[test]
    fn tracing_error_subscriber_not_set_up_with_instrument() {
        // no setup

        // demo a case where span entering is used to establish a parent child connection, but
        // when we re-enter the subspan SpanTrace::with_spans iterates over nothing.
        let span = tracing::info_span!("foo", e = "some value");
        let _guard = span.enter();

        let subspan = tracing::info_span!("bar", f = "foobar");
        drop(_guard);

        // normally this would work, but without any tracing-subscriber configured, both
        // check_field_present find nothing
        let _guard = subspan.enter();
        let extractors: [&dyn Extractor; 2] = [
            &MultiNameExtractor::new("E", ["e"]),
            &MultiNameExtractor::new("F", ["f"]),
        ];

        let res = check_fields_present0(extractors);
        assert!(matches!(res, Ok(Summary::Unconfigured)), "{res:?}");

        // similarly for a not found key
        let extractor = MultiNameExtractor::new("G", ["g"]);
        let res = check_fields_present0([&extractor]);
        assert!(matches!(res, Ok(Summary::Unconfigured)), "{res:?}");
    }

    #[test]
    #[should_panic]
    fn panics_if_tracing_error_subscriber_has_wrong_filter() {
        let r = tracing_subscriber::registry().with({
            tracing_error::ErrorLayer::default().with_filter(
                tracing_subscriber::filter::dynamic_filter_fn(|md, _| {
                    if md.is_span() && *md.level() == tracing::Level::INFO {
                        return false;
                    }
                    true
                }),
            )
        });

        let _guard = tracing::subscriber::set_default(r);

        let span = tracing::info_span!("foo", e = "some value");
        let _guard = span.enter();

        let extractor = MultiNameExtractor::new("E", ["e"]);
        let missing = check_fields_present([&extractor]).unwrap_err();
        assert_missing(missing, vec![&extractor]);
    }
}
