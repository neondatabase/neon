#[cfg(debug_assertions)]
use utils::tracing_span_assert::{check_fields_present, Extractor, MultiNameExtractor};

#[cfg(not(debug_assertions))]
pub(crate) fn debug_assert_current_span_has_tenant_and_timeline_id() {}

#[cfg(debug_assertions)]
#[track_caller]
pub(crate) fn debug_assert_current_span_has_tenant_and_timeline_id() {
    static TIMELINE_ID_EXTRACTOR: once_cell::sync::Lazy<MultiNameExtractor<1>> =
        once_cell::sync::Lazy::new(|| MultiNameExtractor::new("TimelineId", ["timeline_id"]));

    let fields: [&dyn Extractor; 2] = [
        &*crate::tenant::span::TENANT_ID_EXTRACTOR,
        &*TIMELINE_ID_EXTRACTOR,
    ];
    if let Err(missing) = check_fields_present!(fields) {
        panic!("missing extractors: {missing:?}")
    }
}
