#[cfg(debug_assertions)]
use utils::tracing_span_assert::{check_fields_present, MultiNameExtractor};

#[cfg(debug_assertions)]
static TENANT_ID_EXTRACTOR: once_cell::sync::Lazy<MultiNameExtractor<1>> =
    once_cell::sync::Lazy::new(|| MultiNameExtractor::new("TenantId", ["tenant_id"]));
#[cfg(debug_assertions)]
static SHARD_ID_EXTRACTOR: once_cell::sync::Lazy<MultiNameExtractor<1>> =
    once_cell::sync::Lazy::new(|| MultiNameExtractor::new("ShardId", ["shard_id"]));
#[cfg(debug_assertions)]
static TIMELINE_ID_EXTRACTOR: once_cell::sync::Lazy<MultiNameExtractor<1>> =
    once_cell::sync::Lazy::new(|| MultiNameExtractor::new("TimelineId", ["timeline_id"]));

#[track_caller]
pub(crate) fn debug_assert_current_span_has_tenant_id() {
    if cfg!(debug_assertions) {
        if let Err(missing) = check_fields_present!([&*TENANT_ID_EXTRACTOR, &*SHARD_ID_EXTRACTOR]) {
            panic!("missing extractors: {missing:?}")
        }
    }
}

#[track_caller]
pub(crate) fn debug_assert_current_span_has_tenant_and_timeline_id() {
    if cfg!(debug_assertions) {
        if let Err(missing) = check_fields_present!([
            &*TENANT_ID_EXTRACTOR,
            &*SHARD_ID_EXTRACTOR,
            &*TIMELINE_ID_EXTRACTOR,
        ]) {
            panic!("missing extractors: {missing:?}")
        }
    }
}

#[track_caller]
pub(crate) fn debug_assert_current_span_has_tenant_and_timeline_id_no_shard_id() {
    if cfg!(debug_assertions) {
        if let Err(missing) =
            check_fields_present!([&*TENANT_ID_EXTRACTOR, &*TIMELINE_ID_EXTRACTOR,])
        {
            panic!("missing extractors: {missing:?}")
        }
    }
}
