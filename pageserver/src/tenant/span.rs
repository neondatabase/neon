#[cfg(debug_assertions)]
use utils::tracing_span_assert::{check_fields_present, MultiNameExtractor};

#[cfg(not(debug_assertions))]
pub(crate) fn debug_assert_current_span_has_tenant_id() {}

#[cfg(debug_assertions)]
pub(crate) static TENANT_ID_EXTRACTOR: once_cell::sync::Lazy<MultiNameExtractor<2>> =
    once_cell::sync::Lazy::new(|| MultiNameExtractor::new("TenantId", ["tenant_id", "tenant"]));

#[cfg(debug_assertions)]
#[track_caller]
pub(crate) fn debug_assert_current_span_has_tenant_id() {
    if let Err(missing) = check_fields_present([&*TENANT_ID_EXTRACTOR]) {
        panic!("missing extractors: {missing:?}")
    }
}
