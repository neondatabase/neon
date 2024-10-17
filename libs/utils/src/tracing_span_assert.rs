//! Assert that the current [`tracing::Span`] has a given set of fields.
//!
//! Can only produce meaningful positive results when tracing has been configured as in example.
//! Absence of `tracing_error::ErrorLayer` is not detected yet.
//!
//! `#[cfg(test)]` code will get a pass when using the `check_fields_present` macro in case tracing
//! is completly unconfigured.
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
//! let span = tracing::info_span!("TestSpan", tenant_id = 1);
//! let _guard = span.enter();
//!
//! // ... down the call stack
//!
//! use utils::tracing_span_assert::{check_fields_present, ConstExtractor};
//! let extractor = ConstExtractor::new("tenant_id");
//! if let Err(missing) = check_fields_present!([&extractor]) {
//!    // if you copypaste this to a custom assert method, remember to add #[track_caller]
//!    // to get the "user" code location for the panic.
//!    panic!("Missing fields: {missing:?}");
//! }
//! # }
//! ```
//!
//! Recommended reading: <https://docs.rs/tracing-subscriber/0.3.16/tracing_subscriber/layer/index.html#per-layer-filtering>
//!

#[derive(Debug)]
pub enum ExtractionResult {
    Present,
    Absent,
}

pub trait Extractor: Send + Sync + std::fmt::Debug {
    fn id(&self) -> &str;
    fn extract(&self, fields: &tracing::field::FieldSet) -> ExtractionResult;
}

#[derive(Debug)]
pub struct ConstExtractor {
    field_name: &'static str,
}

impl ConstExtractor {
    pub const fn new(field_name: &'static str) -> ConstExtractor {
        ConstExtractor { field_name }
    }
}
impl Extractor for ConstExtractor {
    fn id(&self) -> &str {
        self.field_name
    }
    fn extract(&self, fields: &tracing::field::FieldSet) -> ExtractionResult {
        if fields.iter().any(|f| f.name() == self.field_name) {
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
        // when trying to understand the inner workings of how does the matching work, note that
        // this closure might be called zero times if the span is disabled. normally it is called
        // once per span hierarchy level.
        missing.retain(|extractor| match extractor.extract(md.fields()) {
            ExtractionResult::Present => false,
            ExtractionResult::Absent => true,
        });

        // continue walking up until we've found all missing
        !missing.is_empty()
    });
    if missing.is_empty() {
        Ok(Summary::FoundEverything)
    } else if !tracing_subscriber_configured() {
        Ok(Summary::Unconfigured)
    } else {
        // we can still hit here if a tracing subscriber has been configured but the ErrorLayer is
        // missing, which can be annoying. for this case, we could probably use
        // SpanTrace::status().
        //
        // another way to end up here is with RUST_LOG=pageserver=off while configuring the
        // logging, though I guess in that case the SpanTrace::status() == EMPTY would be valid.
        // this case is covered by test `not_found_if_tracing_error_subscriber_has_wrong_filter`.
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

            match check_fields_present0($extractors) {
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
    let mut noop_configured = false;
    tracing::dispatcher::get_default(|d| {
        // it is possible that this closure will not be invoked, but the current implementation
        // always invokes it
        noop_configured = d.is::<tracing::subscriber::NoSubscriber>();
    });

    !noop_configured
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

    impl MemoryIdentity<'_> {
        fn as_ptr(&self) -> *const () {
            self.0 as *const _ as *const ()
        }
    }
    impl PartialEq for MemoryIdentity<'_> {
        fn eq(&self, other: &Self) -> bool {
            self.as_ptr() == other.as_ptr()
        }
    }
    impl Eq for MemoryIdentity<'_> {}
    impl Hash for MemoryIdentity<'_> {
        fn hash<H: Hasher>(&self, state: &mut H) {
            self.as_ptr().hash(state);
        }
    }
    impl fmt::Debug for MemoryIdentity<'_> {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "{:p}: {}", self.as_ptr(), self.0.id())
        }
    }

    struct Setup {
        _current_thread_subscriber_guard: tracing::subscriber::DefaultGuard,
        tenant_extractor: ConstExtractor,
        timeline_extractor: ConstExtractor,
    }

    fn setup_current_thread() -> Setup {
        let tenant_extractor = ConstExtractor::new("tenant_id");
        let timeline_extractor = ConstExtractor::new("timeline_id");

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
        let res = check_fields_present0([&setup.tenant_extractor, &setup.timeline_extractor]);
        assert!(matches!(res, Ok(Summary::FoundEverything)), "{res:?}");
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

        let res = check_fields_present0([&setup.tenant_extractor, &setup.timeline_extractor]);
        assert!(matches!(res, Ok(Summary::FoundEverything)), "{res:?}");
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
        let res = check_fields_present0([&setup.tenant_extractor]);
        assert!(matches!(res, Ok(Summary::FoundEverything)), "{res:?}");
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

        let res = check_fields_present0([&setup.tenant_extractor]);
        assert!(matches!(res, Ok(Summary::FoundEverything)), "{res:?}");
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

        let extractor = ConstExtractor::new("e");
        let res = check_fields_present0([&extractor]);
        assert!(matches!(res, Ok(Summary::Unconfigured)), "{res:?}");

        // similarly for a not found key
        let extractor = ConstExtractor::new("foobar");
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
        let extractors: [&dyn Extractor; 2] =
            [&ConstExtractor::new("e"), &ConstExtractor::new("f")];

        let res = check_fields_present0(extractors);
        assert!(matches!(res, Ok(Summary::Unconfigured)), "{res:?}");

        // similarly for a not found key
        let extractor = ConstExtractor::new("g");
        let res = check_fields_present0([&extractor]);
        assert!(matches!(res, Ok(Summary::Unconfigured)), "{res:?}");
    }

    #[test]
    fn tracing_subscriber_configured() {
        // this will fail if any utils::logging::init callers appear, but let's hope they do not
        // appear.
        assert!(!super::tracing_subscriber_configured());

        let _g = setup_current_thread();

        assert!(super::tracing_subscriber_configured());
    }

    #[test]
    fn not_found_when_disabled_by_filter() {
        let r = tracing_subscriber::registry().with({
            tracing_error::ErrorLayer::default().with_filter(tracing_subscriber::filter::filter_fn(
                |md| !(md.is_span() && *md.level() == tracing::Level::INFO),
            ))
        });

        let _guard = tracing::subscriber::set_default(r);

        // this test is a rather tricky one, it has a number of possible outcomes depending on the
        // execution order when executed with other tests even if no test sets the global default
        // subscriber.

        let span = tracing::info_span!("foo", e = "some value");
        let _guard = span.enter();

        let extractors: [&dyn Extractor; 1] = [&ConstExtractor::new("e")];

        if span.is_disabled() {
            // the tests are running single threaded, or we got lucky and no other tests subscriber
            // was got to register their per-CALLSITE::META interest between `set_default` and
            // creation of the span, thus the filter got to apply and registered interest of Never,
            // so the span was never created.
            //
            // as the span is disabled, no keys were recorded to it, leading check_fields_present0
            // to find an error.

            let missing = check_fields_present0(extractors).unwrap_err();
            assert_missing(missing, vec![extractors[0]]);
        } else {
            // when the span is enabled, it is because some other test is running at the same time,
            // and that tests registry has filters which are interested in our above span.
            //
            // because the span is now enabled, all keys will be found for it. the
            // tracing_error::SpanTrace does not consider layer filters during the span hierarchy
            // walk (SpanTrace::with_spans), nor is the SpanTrace::status a reliable indicator in
            // this test-induced issue.

            let res = check_fields_present0(extractors);
            assert!(matches!(res, Ok(Summary::FoundEverything)), "{res:?}");
        }
    }
}
