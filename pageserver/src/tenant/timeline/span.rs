#[cfg(not(debug_assertions))]
#[inline]
pub(crate) fn debug_assert_current_span_has_tenant_and_timeline_id() {}

#[cfg(debug_assertions)]
#[inline]
pub(crate) fn debug_assert_current_span_has_tenant_and_timeline_id() {
    use utils::tracing_span_assert;

    pub static TIMELINE_ID_EXTRACTOR: once_cell::sync::Lazy<
        tracing_span_assert::MultiNameExtractor<2>,
    > = once_cell::sync::Lazy::new(|| {
        tracing_span_assert::MultiNameExtractor::new("TimelineId", ["timeline_id", "timeline"])
    });

    match tracing_span_assert::check_fields_present([
        &*crate::tenant::span::TENANT_ID_EXTRACTOR,
        &*TIMELINE_ID_EXTRACTOR,
    ]) {
        Ok(()) => (),
        Err(missing) => panic!(
            "missing extractors: {:?}",
            missing.into_iter().map(|e| e.name()).collect::<Vec<_>>()
        ),
    }
}
