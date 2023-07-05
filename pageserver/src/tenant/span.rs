#[cfg(not(debug_assertions))]
#[inline]
pub(crate) fn debug_assert_current_span_has_tenant_id() {}

#[cfg(debug_assertions)]
pub static TENANT_ID_EXTRACTOR: once_cell::sync::Lazy<
    utils::tracing_span_assert::MultiNameExtractor<2>,
> = once_cell::sync::Lazy::new(|| {
    utils::tracing_span_assert::MultiNameExtractor::new("TenantId", ["tenant_id", "tenant"])
});

#[cfg(debug_assertions)]
#[inline]
pub(crate) fn debug_assert_current_span_has_tenant_id() {
    use utils::tracing_span_assert;

    match tracing_span_assert::check_fields_present([&*TENANT_ID_EXTRACTOR]) {
        Ok(()) => (),
        Err(missing) => panic!(
            "missing extractors: {:?}",
            missing.into_iter().map(|e| e.name()).collect::<Vec<_>>()
        ),
    }
}
