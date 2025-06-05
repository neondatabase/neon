//! Tracing span helpers.

/// Records the given fields in the current span, as a single call. The fields must already have
/// been declared for the span (typically with empty values).
#[macro_export]
macro_rules! span_record {
    ($($tokens:tt)*) => {$crate::span_record_in!(::tracing::Span::current(), $($tokens)*)};
}

/// Records the given fields in the given span, as a single call. The fields must already have been
/// declared for the span (typically with empty values).
#[macro_export]
macro_rules! span_record_in {
    ($span:expr, $($tokens:tt)*) => {
        if let Some(meta) = $span.metadata() {
            $span.record_all(&tracing::valueset!(meta.fields(), $($tokens)*));
        }
    };
}
