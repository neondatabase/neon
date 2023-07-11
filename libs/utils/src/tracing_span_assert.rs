//! Assert that the current [`tracing::Span`] has a given set of fields.
//!
//! # Usage
//!
//! ```
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
//! match check_fields_present([&extractor]) {
//!    Ok(()) => {},
//!    Err(missing) => {
//!        panic!("Missing fields: {:?}", missing.into_iter().map(|f| f.name() ).collect::<Vec<_>>());
//!    }
//! }
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

/// The extractor names passed as keys to [`new`].
pub fn check_fields_present<const L: usize>(
    must_be_present: [&dyn Extractor; L],
) -> Result<(), Vec<&dyn Extractor>> {
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
        Ok(())
    } else if !tracing_subscriber_configured() {
        Ok(())
    } else {
        Err(missing)
    }
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
        check_fields_present([&setup.tenant_extractor, &setup.timeline_extractor]).unwrap();
    }

    #[test]
    fn negative_one_level() {
        let setup = setup_current_thread();
        let span = tracing::info_span!("root", timeline_id = "timeline-1");
        let _guard = span.enter();
        let missing =
            check_fields_present([&setup.tenant_extractor, &setup.timeline_extractor]).unwrap_err();
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

        check_fields_present([&setup.tenant_extractor, &setup.timeline_extractor]).unwrap();
    }

    #[test]
    fn negative_multiple_levels() {
        let setup = setup_current_thread();

        let span = tracing::info_span!("root");
        let _guard = span.enter();

        let span = tracing::info_span!("child", timeline_id = "timeline-1");
        let _guard = span.enter();

        let missing = check_fields_present([&setup.tenant_extractor]).unwrap_err();
        assert_missing(missing, vec![&setup.tenant_extractor]);
    }

    #[test]
    fn positive_subset_one_level() {
        let setup = setup_current_thread();
        let span = tracing::info_span!("root", tenant_id = "tenant-1", timeline_id = "timeline-1");
        let _guard = span.enter();
        check_fields_present([&setup.tenant_extractor]).unwrap();
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

        check_fields_present([&setup.tenant_extractor]).unwrap();
    }

    #[test]
    fn negative_subset_one_level() {
        let setup = setup_current_thread();
        let span = tracing::info_span!("root", timeline_id = "timeline-1");
        let _guard = span.enter();
        let missing = check_fields_present([&setup.tenant_extractor]).unwrap_err();
        assert_missing(missing, vec![&setup.tenant_extractor]);
    }

    #[test]
    fn negative_subset_multiple_levels() {
        let setup = setup_current_thread();

        let span = tracing::info_span!("root");
        let _guard = span.enter();

        let span = tracing::info_span!("child", timeline_id = "timeline-1");
        let _guard = span.enter();

        let missing = check_fields_present([&setup.tenant_extractor]).unwrap_err();
        assert_missing(missing, vec![&setup.tenant_extractor]);
    }

    #[test]
    fn tracing_error_subscriber_not_set_up() {
        // no setup
        let span = tracing::info_span!("foo", e = "some value");
        let _guard = span.enter();

        let extractor = MultiNameExtractor::new("E", ["e"]);
        check_fields_present([&extractor])
            .expect("without any subscriber, should still return Ok(())");

        // similarly for a not found key
        let extractor = MultiNameExtractor::new("E", ["foobar"]);
        check_fields_present([&extractor])
            .expect("without any subscriber, should still return Ok(())");
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
