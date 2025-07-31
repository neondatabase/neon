use std::cell::RefCell;
use std::collections::HashMap;
use std::sync::Arc;
use std::{env, io};

use chrono::{DateTime, Utc};
use opentelemetry::trace::TraceContextExt;
use tracing::subscriber::Interest;
use tracing::{Event, Metadata, Span, Subscriber, callsite, span};
use tracing_opentelemetry::OpenTelemetrySpanExt;
use tracing_subscriber::filter::{EnvFilter, LevelFilter};
use tracing_subscriber::fmt::format::{Format, Full};
use tracing_subscriber::fmt::time::SystemTime;
use tracing_subscriber::fmt::{FormatEvent, FormatFields};
use tracing_subscriber::layer::{Context, Layer};
use tracing_subscriber::prelude::*;
use tracing_subscriber::registry::LookupSpan;

use crate::metrics::Metrics;

/// Initialize logging and OpenTelemetry tracing and exporter.
///
/// Logging can be configured using `RUST_LOG` environment variable.
///
/// OpenTelemetry is configured with OTLP/HTTP exporter. It picks up
/// configuration from environment variables. For example, to change the
/// destination, set `OTEL_EXPORTER_OTLP_ENDPOINT=http://jaeger:4318`.
/// See <https://opentelemetry.io/docs/reference/specification/sdk-environment-variables>
pub fn init() -> anyhow::Result<LoggingGuard> {
    let logfmt = LogFormat::from_env()?;

    let env_filter = EnvFilter::builder()
        .with_default_directive(LevelFilter::INFO.into())
        .from_env_lossy()
        .add_directive(
            "aws_config=info"
                .parse()
                .expect("this should be a valid filter directive"),
        )
        .add_directive(
            "azure_core::policies::transport=off"
                .parse()
                .expect("this should be a valid filter directive"),
        );

    let provider = tracing_utils::init_tracing("proxy", tracing_utils::ExportConfig::default());
    let otlp_layer = provider.as_ref().map(tracing_utils::layer);

    let json_log_layer = if logfmt == LogFormat::Json {
        Some(JsonLoggingLayer::new(
            RealClock,
            StderrWriter {
                stderr: std::io::stderr(),
            },
            &["conn_id", "ep", "query_id", "request_id", "session_id"],
        ))
    } else {
        None
    };

    let text_log_layer = if logfmt == LogFormat::Text {
        Some(
            tracing_subscriber::fmt::layer()
                .with_ansi(false)
                .with_writer(std::io::stderr)
                .with_target(false),
        )
    } else {
        None
    };

    tracing_subscriber::registry()
        .with(env_filter)
        .with(otlp_layer)
        .with(json_log_layer)
        .with(text_log_layer)
        .try_init()?;

    Ok(LoggingGuard(provider))
}

/// Initialize logging for local_proxy with log prefix and no opentelemetry.
///
/// Logging can be configured using `RUST_LOG` environment variable.
pub fn init_local_proxy() -> anyhow::Result<LoggingGuard> {
    let env_filter = EnvFilter::builder()
        .with_default_directive(LevelFilter::INFO.into())
        .from_env_lossy();

    let fmt_layer = tracing_subscriber::fmt::layer()
        .with_ansi(false)
        .with_writer(std::io::stderr)
        .event_format(LocalProxyFormatter(Format::default().with_target(false)));

    tracing_subscriber::registry()
        .with(env_filter)
        .with(fmt_layer)
        .try_init()?;

    Ok(LoggingGuard(None))
}

pub struct LocalProxyFormatter(Format<Full, SystemTime>);

impl<S, N> FormatEvent<S, N> for LocalProxyFormatter
where
    S: Subscriber + for<'a> LookupSpan<'a>,
    N: for<'a> FormatFields<'a> + 'static,
{
    fn format_event(
        &self,
        ctx: &tracing_subscriber::fmt::FmtContext<'_, S, N>,
        mut writer: tracing_subscriber::fmt::format::Writer<'_>,
        event: &tracing::Event<'_>,
    ) -> std::fmt::Result {
        writer.write_str("[local_proxy] ")?;
        self.0.format_event(ctx, writer, event)
    }
}

pub struct LoggingGuard(Option<tracing_utils::Provider>);

impl Drop for LoggingGuard {
    fn drop(&mut self) {
        if let Some(p) = &self.0 {
            // Shutdown trace pipeline gracefully, so that it has a chance to send any
            // pending traces before we exit.
            tracing::info!("shutting down the tracing machinery");
            drop(p.shutdown());
        }
    }
}

#[derive(Copy, Clone, PartialEq, Eq, Default, Debug)]
enum LogFormat {
    Text,
    #[default]
    Json,
}

impl LogFormat {
    fn from_env() -> anyhow::Result<Self> {
        let logfmt = env::var("LOGFMT");
        Ok(match logfmt.as_deref() {
            Err(_) => LogFormat::default(),
            Ok("text") => LogFormat::Text,
            Ok("json") => LogFormat::Json,
            Ok(logfmt) => anyhow::bail!("unknown log format: {logfmt}"),
        })
    }
}

trait MakeWriter {
    fn make_writer(&self) -> impl io::Write;
}

struct StderrWriter {
    stderr: io::Stderr,
}

impl MakeWriter for StderrWriter {
    #[inline]
    fn make_writer(&self) -> impl io::Write {
        self.stderr.lock()
    }
}

// TODO: move into separate module or even separate crate.
trait Clock {
    fn now(&self) -> DateTime<Utc>;
}

struct RealClock;

impl Clock for RealClock {
    #[inline]
    fn now(&self) -> DateTime<Utc> {
        Utc::now()
    }
}

/// Name of the field used by tracing crate to store the event message.
const MESSAGE_FIELD: &str = "message";

/// Tracing used to enforce that spans/events have no more than 32 fields.
/// It seems this is no longer the case, but it's still documented in some places.
/// Generally, we shouldn't expect more than 32 fields anyway, so we can try and
/// rely on it for some (minor) performance gains.
const MAX_TRACING_FIELDS: usize = 32;

thread_local! {
    /// Thread-local instance with per-thread buffer for log writing.
    static EVENT_FORMATTER: RefCell<EventFormatter> = const { RefCell::new(EventFormatter::new()) };
    /// Cached OS thread ID.
    static THREAD_ID: u64 = gettid::gettid();
}

/// Map for values fixed at callsite registration.
// We use papaya here because registration rarely happens post-startup.
// papaya is good for read-heavy workloads.
//
// We use rustc_hash here because callsite::Identifier will always be an integer with low-bit entropy,
// since it's always a pointer to static mutable data. rustc_hash was designed for low-bit entropy.
type CallsiteMap<T> =
    papaya::HashMap<callsite::Identifier, T, std::hash::BuildHasherDefault<rustc_hash::FxHasher>>;

/// Implements tracing layer to handle events specific to logging.
struct JsonLoggingLayer<C: Clock, W: MakeWriter> {
    clock: C,
    writer: W,

    /// tracks which fields of each **event** are duplicates
    skipped_field_indices: CallsiteMap<SkippedFieldIndices>,

    /// tracks callsite names to an ID.
    callsite_name_ids: papaya::HashMap<&'static str, u32, ahash::RandomState>,

    span_info: CallsiteMap<CallsiteSpanInfo>,

    /// Fields we want to keep track of in a separate json object.
    extract_fields: &'static [&'static str],
}

impl<C: Clock, W: MakeWriter> JsonLoggingLayer<C, W> {
    fn new(clock: C, writer: W, extract_fields: &'static [&'static str]) -> Self {
        JsonLoggingLayer {
            clock,
            skipped_field_indices: CallsiteMap::default(),
            span_info: CallsiteMap::default(),
            callsite_name_ids: papaya::HashMap::default(),
            writer,
            extract_fields,
        }
    }

    #[inline]
    fn span_info(&self, metadata: &'static Metadata<'static>) -> CallsiteSpanInfo {
        self.span_info
            .pin()
            .get_or_insert_with(metadata.callsite(), || {
                CallsiteSpanInfo::new(&self.callsite_name_ids, metadata, self.extract_fields)
            })
            .clone()
    }
}

impl<S, C: Clock + 'static, W: MakeWriter + 'static> Layer<S> for JsonLoggingLayer<C, W>
where
    S: Subscriber + for<'a> LookupSpan<'a>,
{
    fn on_event(&self, event: &Event<'_>, ctx: Context<'_, S>) {
        use std::io::Write;

        // TODO: consider special tracing subscriber to grab timestamp very
        //       early, before OTel machinery, and add as event extension.
        let now = self.clock.now();

        EVENT_FORMATTER.with(|f| {
            let mut borrow = f.try_borrow_mut();
            let formatter = match borrow.as_deref_mut() {
                Ok(formatter) => formatter,
                // If the thread local formatter is borrowed,
                // then we likely hit an edge case were we panicked during formatting.
                // We allow the logging to proceed with an uncached formatter.
                Err(_) => &mut EventFormatter::new(),
            };

            formatter.format(
                now,
                event,
                &ctx,
                &self.skipped_field_indices,
                self.extract_fields,
            );

            let mut writer = self.writer.make_writer();
            if writer.write_all(formatter.buffer()).is_err() {
                Metrics::get().proxy.logging_errors_count.inc();
            }
        });
    }

    /// Registers a SpanFields instance as span extension.
    fn on_new_span(&self, attrs: &span::Attributes<'_>, id: &span::Id, ctx: Context<'_, S>) {
        let span = ctx.span(id).expect("span must exist");

        let mut fields = SpanFields::new(self.span_info(span.metadata()));
        attrs.record(&mut fields);

        // This is a new span: the extensions should not be locked
        // unless some layer spawned a thread to process this span.
        // I don't think any layers do that.
        span.extensions_mut().insert(fields);
    }

    fn on_record(&self, id: &span::Id, values: &span::Record<'_>, ctx: Context<'_, S>) {
        let span = ctx.span(id).expect("span must exist");

        // assumption: `on_record` is rarely called.
        // assumption: a span being updated by one thread,
        //             and formatted by another thread is even rarer.
        let mut ext = span.extensions_mut();
        if let Some(fields) = ext.get_mut::<SpanFields>() {
            values.record(fields);
        }
    }

    /// Called (lazily) roughly once per event/span instance. We quickly check
    /// for duplicate field names and record duplicates as skippable. Last field wins.
    fn register_callsite(&self, metadata: &'static Metadata<'static>) -> Interest {
        debug_assert!(
            metadata.fields().len() <= MAX_TRACING_FIELDS,
            "callsite {metadata:?} has too many fields."
        );

        if !metadata.is_event() {
            // register the span info.
            self.span_info(metadata);
            // Must not be never because we wouldn't get trace and span data.
            return Interest::always();
        }

        let mut field_indices = SkippedFieldIndices::default();
        let mut seen_fields = HashMap::new();
        for field in metadata.fields() {
            if let Some(old_index) = seen_fields.insert(field.name(), field.index()) {
                field_indices.set(old_index);
            }
        }

        if !field_indices.is_empty() {
            self.skipped_field_indices
                .pin()
                .insert(metadata.callsite(), field_indices);
        }

        Interest::always()
    }
}

/// Any span info that is fixed to a particular callsite. Not variable between span instances.
#[derive(Clone)]
struct CallsiteSpanInfo {
    /// index of each field to extract. usize::MAX if not found.
    extract: Arc<[usize]>,

    /// tracks the fixed "callsite ID" for each span.
    /// note: this is not stable between runs.
    normalized_name: Arc<str>,
}

impl CallsiteSpanInfo {
    fn new(
        callsite_name_ids: &papaya::HashMap<&'static str, u32, ahash::RandomState>,
        metadata: &'static Metadata<'static>,
        extract_fields: &[&'static str],
    ) -> Self {
        let names: Vec<&'static str> = metadata.fields().iter().map(|f| f.name()).collect();

        // get all the indices of span fields we want to focus
        let extract = extract_fields
            .iter()
            // use rposition, since we want last match wins.
            .map(|f1| names.iter().rposition(|f2| f1 == f2).unwrap_or(usize::MAX))
            .collect();

        // normalized_name is unique for each callsite, but it is not
        // unified across separate proxy instances.
        // todo: can we do better here?
        let cid = *callsite_name_ids
            .pin()
            .update_or_insert(metadata.name(), |&cid| cid + 1, 0);

        // we hope that most span names are unique, in which case this will always be 0
        let normalized_name = if cid == 0 {
            metadata.name().into()
        } else {
            // if the span name is not unique, add the numeric ID to span name to distinguish it.
            // sadly this is non-determinstic, across restarts but we should fix it by disambiguating re-used span names instead.
            format!("{}#{cid}", metadata.name()).into()
        };

        Self {
            extract,
            normalized_name,
        }
    }
}

#[derive(Clone)]
struct RawValue(Box<[u8]>);

impl RawValue {
    fn new(v: impl json::ValueEncoder) -> Self {
        Self(json::value_to_vec!(|val| v.encode(val)).into_boxed_slice())
    }
}

impl json::ValueEncoder for &RawValue {
    fn encode(self, v: json::ValueSer<'_>) {
        v.write_raw_json(&self.0);
    }
}

/// Stores span field values recorded during the spans lifetime.
struct SpanFields {
    values: [Option<RawValue>; MAX_TRACING_FIELDS],

    /// cached span info so we can avoid extra hashmap lookups in the hot path.
    span_info: CallsiteSpanInfo,
}

impl SpanFields {
    fn new(span_info: CallsiteSpanInfo) -> Self {
        Self {
            span_info,
            values: [const { None }; MAX_TRACING_FIELDS],
        }
    }
}

impl tracing::field::Visit for SpanFields {
    #[inline]
    fn record_f64(&mut self, field: &tracing::field::Field, value: f64) {
        self.values[field.index()] = Some(RawValue::new(value));
    }

    #[inline]
    fn record_i64(&mut self, field: &tracing::field::Field, value: i64) {
        self.values[field.index()] = Some(RawValue::new(value));
    }

    #[inline]
    fn record_u64(&mut self, field: &tracing::field::Field, value: u64) {
        self.values[field.index()] = Some(RawValue::new(value));
    }

    #[inline]
    fn record_i128(&mut self, field: &tracing::field::Field, value: i128) {
        if let Ok(value) = i64::try_from(value) {
            self.values[field.index()] = Some(RawValue::new(value));
        } else {
            self.values[field.index()] = Some(RawValue::new(format_args!("{value}")));
        }
    }

    #[inline]
    fn record_u128(&mut self, field: &tracing::field::Field, value: u128) {
        if let Ok(value) = u64::try_from(value) {
            self.values[field.index()] = Some(RawValue::new(value));
        } else {
            self.values[field.index()] = Some(RawValue::new(format_args!("{value}")));
        }
    }

    #[inline]
    fn record_bool(&mut self, field: &tracing::field::Field, value: bool) {
        self.values[field.index()] = Some(RawValue::new(value));
    }

    #[inline]
    fn record_bytes(&mut self, field: &tracing::field::Field, value: &[u8]) {
        self.values[field.index()] = Some(RawValue::new(value));
    }

    #[inline]
    fn record_str(&mut self, field: &tracing::field::Field, value: &str) {
        self.values[field.index()] = Some(RawValue::new(value));
    }

    #[inline]
    fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
        self.values[field.index()] = Some(RawValue::new(format_args!("{value:?}")));
    }

    #[inline]
    fn record_error(
        &mut self,
        field: &tracing::field::Field,
        value: &(dyn std::error::Error + 'static),
    ) {
        self.values[field.index()] = Some(RawValue::new(format_args!("{value}")));
    }
}

/// List of field indices skipped during logging. Can list duplicate fields or
/// metafields not meant to be logged.
#[derive(Copy, Clone, Default)]
struct SkippedFieldIndices {
    // 32-bits is large enough for `MAX_TRACING_FIELDS`
    bits: u32,
}

impl SkippedFieldIndices {
    #[inline]
    fn is_empty(self) -> bool {
        self.bits == 0
    }

    #[inline]
    fn set(&mut self, index: usize) {
        debug_assert!(index <= 32, "index out of bounds of 32-bit set");
        self.bits |= 1 << index;
    }

    #[inline]
    fn contains(self, index: usize) -> bool {
        self.bits & (1 << index) != 0
    }
}

/// Formats a tracing event and writes JSON to its internal buffer including a newline.
// TODO: buffer capacity management, truncate if too large
struct EventFormatter {
    logline_buffer: Vec<u8>,
}

impl EventFormatter {
    #[inline]
    const fn new() -> Self {
        EventFormatter {
            logline_buffer: Vec::new(),
        }
    }

    #[inline]
    fn buffer(&self) -> &[u8] {
        &self.logline_buffer
    }

    fn format<S>(
        &mut self,
        now: DateTime<Utc>,
        event: &Event<'_>,
        ctx: &Context<'_, S>,
        skipped_field_indices: &CallsiteMap<SkippedFieldIndices>,
        extract_fields: &'static [&'static str],
    ) where
        S: Subscriber + for<'a> LookupSpan<'a>,
    {
        let timestamp = now.to_rfc3339_opts(chrono::SecondsFormat::Micros, true);

        use tracing_log::NormalizeEvent;
        let normalized_meta = event.normalized_metadata();
        let meta = normalized_meta.as_ref().unwrap_or_else(|| event.metadata());

        let skipped_field_indices = skipped_field_indices
            .pin()
            .get(&meta.callsite())
            .copied()
            .unwrap_or_default();

        self.logline_buffer.clear();
        let serializer = json::ValueSer::new(&mut self.logline_buffer);
        json::value_as_object!(|serializer| {
            // Timestamp comes first, so raw lines can be sorted by timestamp.
            serializer.entry(json::esc!("timestamp"), &*timestamp);

            // Level next.
            serializer.entry(json::esc!("level"), meta.level().as_str());

            // Message next.
            let mut message_extractor = MessageFieldExtractor::new(
                serializer.key(json::esc!("message")),
                skipped_field_indices,
            );
            event.record(&mut message_extractor);
            message_extractor.finish();

            // Direct message fields.
            {
                let mut message_skipper = MessageFieldSkipper::new(
                    serializer.key(json::esc!("fields")).object(),
                    skipped_field_indices,
                );
                event.record(&mut message_skipper);

                // rollback if no fields are present.
                if message_skipper.present {
                    message_skipper.serializer.finish();
                }
            }

            let mut extracted = ExtractedSpanFields::new(extract_fields);

            let spans = serializer.key(json::esc!("spans"));
            json::value_as_object!(|spans| {
                let parent_spans = ctx
                    .event_span(event)
                    .map_or(vec![], |parent| parent.scope().collect());

                for span in parent_spans.iter().rev() {
                    let ext = span.extensions();

                    // all spans should have this extension.
                    let Some(fields) = ext.get() else { continue };

                    extracted.layer_span(fields);

                    let SpanFields { values, span_info } = fields;

                    let span_fields = spans.key(&*span_info.normalized_name);
                    json::value_as_object!(|span_fields| {
                        for (field, value) in std::iter::zip(span.metadata().fields(), values) {
                            if let Some(value) = value {
                                // we don't use entry syntax here, as that's intended for literal keys only.
                                // the field name might need escaping, and entry would panic in that case.
                                span_fields.entry(field.name(), value);
                            }
                        }
                    });
                }
            });

            // TODO: thread-local cache?
            let pid = std::process::id();
            // Skip adding pid 1 to reduce noise for services running in containers.
            if pid != 1 {
                serializer.entry(json::esc!("process_id"), pid);
            }

            THREAD_ID.with(|tid| serializer.entry(json::esc!("thread_id"), tid));

            // TODO: tls cache? name could change
            if let Some(thread_name) = std::thread::current().name()
                && !thread_name.is_empty()
                && thread_name != "tokio-runtime-worker"
            {
                serializer.entry(json::esc!("thread_name"), thread_name);
            }

            if let Some(task_id) = tokio::task::try_id() {
                serializer.entry(json::esc!("task_id"), format_args!("{task_id}"));
            }

            serializer.entry(json::esc!("target"), meta.target());

            // Skip adding module if it's the same as target.
            if let Some(module) = meta.module_path()
                && module != meta.target()
            {
                serializer.entry(json::esc!("module"), module);
            }

            if let Some(file) = meta.file() {
                if let Some(line) = meta.line() {
                    serializer.entry(json::esc!("src"), format_args!("{file}:{line}"));
                } else {
                    serializer.entry(json::esc!("src"), file);
                }
            }

            {
                let otel_context = Span::current().context();
                let otel_spanref = otel_context.span();
                let span_context = otel_spanref.span_context();
                if span_context.is_valid() {
                    serializer.entry(
                        json::esc!("trace_id"),
                        format_args!("{}", span_context.trace_id()),
                    );
                }
            }

            if extracted.has_values() {
                // TODO: add fields from event, too?
                let extract = serializer.key(json::esc!("extract"));
                json::value_as_object!(|extract| {
                    for (key, value) in std::iter::zip(extracted.names, extracted.values) {
                        if let Some(value) = value {
                            extract.entry(*key, &value);
                        }
                    }
                });
            }
        });

        self.logline_buffer.push(b'\n');
    }
}

/// Extracts the message field that's mixed will other fields.
struct MessageFieldExtractor<'buf> {
    serializer: Option<json::ValueSer<'buf>>,
    skipped_field_indices: SkippedFieldIndices,
}

impl<'buf> MessageFieldExtractor<'buf> {
    #[inline]
    fn new(serializer: json::ValueSer<'buf>, skipped_field_indices: SkippedFieldIndices) -> Self {
        Self {
            serializer: Some(serializer),
            skipped_field_indices,
        }
    }

    #[inline]
    fn finish(self) {
        if let Some(ser) = self.serializer {
            ser.value("");
        }
    }

    #[inline]
    fn record_field(&mut self, field: &tracing::field::Field, v: impl json::ValueEncoder) {
        if field.name() == MESSAGE_FIELD
            && !self.skipped_field_indices.contains(field.index())
            && let Some(ser) = self.serializer.take()
        {
            ser.value(v);
        }
    }
}

impl tracing::field::Visit for MessageFieldExtractor<'_> {
    #[inline]
    fn record_f64(&mut self, field: &tracing::field::Field, value: f64) {
        self.record_field(field, value);
    }

    #[inline]
    fn record_i64(&mut self, field: &tracing::field::Field, value: i64) {
        self.record_field(field, value);
    }

    #[inline]
    fn record_u64(&mut self, field: &tracing::field::Field, value: u64) {
        self.record_field(field, value);
    }

    #[inline]
    fn record_i128(&mut self, field: &tracing::field::Field, value: i128) {
        self.record_field(field, value);
    }

    #[inline]
    fn record_u128(&mut self, field: &tracing::field::Field, value: u128) {
        self.record_field(field, value);
    }

    #[inline]
    fn record_bool(&mut self, field: &tracing::field::Field, value: bool) {
        self.record_field(field, value);
    }

    #[inline]
    fn record_bytes(&mut self, field: &tracing::field::Field, value: &[u8]) {
        self.record_field(field, format_args!("{value:x?}"));
    }

    #[inline]
    fn record_str(&mut self, field: &tracing::field::Field, value: &str) {
        self.record_field(field, value);
    }

    #[inline]
    fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
        self.record_field(field, format_args!("{value:?}"));
    }

    #[inline]
    fn record_error(
        &mut self,
        field: &tracing::field::Field,
        value: &(dyn std::error::Error + 'static),
    ) {
        self.record_field(field, format_args!("{value}"));
    }
}

/// A tracing field visitor that skips the message field.
struct MessageFieldSkipper<'buf> {
    serializer: json::ObjectSer<'buf>,
    skipped_field_indices: SkippedFieldIndices,
    present: bool,
}

impl<'buf> MessageFieldSkipper<'buf> {
    #[inline]
    fn new(serializer: json::ObjectSer<'buf>, skipped_field_indices: SkippedFieldIndices) -> Self {
        Self {
            serializer,
            skipped_field_indices,
            present: false,
        }
    }

    #[inline]
    fn record_field(&mut self, field: &tracing::field::Field, v: impl json::ValueEncoder) {
        if field.name() != MESSAGE_FIELD
            && !field.name().starts_with("log.")
            && !self.skipped_field_indices.contains(field.index())
        {
            self.serializer.entry(field.name(), v);
            self.present |= true;
        }
    }
}

impl tracing::field::Visit for MessageFieldSkipper<'_> {
    #[inline]
    fn record_f64(&mut self, field: &tracing::field::Field, value: f64) {
        self.record_field(field, value);
    }

    #[inline]
    fn record_i64(&mut self, field: &tracing::field::Field, value: i64) {
        self.record_field(field, value);
    }

    #[inline]
    fn record_u64(&mut self, field: &tracing::field::Field, value: u64) {
        self.record_field(field, value);
    }

    #[inline]
    fn record_i128(&mut self, field: &tracing::field::Field, value: i128) {
        self.record_field(field, value);
    }

    #[inline]
    fn record_u128(&mut self, field: &tracing::field::Field, value: u128) {
        self.record_field(field, value);
    }

    #[inline]
    fn record_bool(&mut self, field: &tracing::field::Field, value: bool) {
        self.record_field(field, value);
    }

    #[inline]
    fn record_bytes(&mut self, field: &tracing::field::Field, value: &[u8]) {
        self.record_field(field, format_args!("{value:x?}"));
    }

    #[inline]
    fn record_str(&mut self, field: &tracing::field::Field, value: &str) {
        self.record_field(field, value);
    }

    #[inline]
    fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
        self.record_field(field, format_args!("{value:?}"));
    }

    #[inline]
    fn record_error(
        &mut self,
        field: &tracing::field::Field,
        value: &(dyn std::error::Error + 'static),
    ) {
        self.record_field(field, format_args!("{value}"));
    }
}

struct ExtractedSpanFields {
    names: &'static [&'static str],
    values: Vec<Option<RawValue>>,
}

impl ExtractedSpanFields {
    fn new(names: &'static [&'static str]) -> Self {
        ExtractedSpanFields {
            names,
            values: vec![None; names.len()],
        }
    }

    fn layer_span(&mut self, fields: &SpanFields) {
        let SpanFields { values, span_info } = fields;

        // extract the fields
        for (i, &j) in span_info.extract.iter().enumerate() {
            let Some(Some(value)) = values.get(j) else {
                continue;
            };

            // TODO: replace clone with reference, if possible.
            self.values[i] = Some(value.clone());
        }
    }

    #[inline]
    fn has_values(&self) -> bool {
        self.values.iter().any(|v| v.is_some())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex, MutexGuard};

    use assert_json_diff::assert_json_eq;
    use tracing::info_span;

    use super::*;

    struct TestClock {
        current_time: Mutex<DateTime<Utc>>,
    }

    impl Clock for Arc<TestClock> {
        fn now(&self) -> DateTime<Utc> {
            *self.current_time.lock().expect("poisoned")
        }
    }

    struct VecWriter<'a> {
        buffer: MutexGuard<'a, Vec<u8>>,
    }

    impl MakeWriter for Arc<Mutex<Vec<u8>>> {
        fn make_writer(&self) -> impl io::Write {
            VecWriter {
                buffer: self.lock().expect("poisoned"),
            }
        }
    }

    impl io::Write for VecWriter<'_> {
        fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
            self.buffer.write(buf)
        }

        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }

    #[test]
    fn test_field_collection() {
        let clock = Arc::new(TestClock {
            current_time: Mutex::new(Utc::now()),
        });
        let buffer = Arc::new(Mutex::new(Vec::new()));
        let log_layer = JsonLoggingLayer {
            clock: clock.clone(),
            skipped_field_indices: papaya::HashMap::default(),
            span_info: papaya::HashMap::default(),
            callsite_name_ids: papaya::HashMap::default(),
            writer: buffer.clone(),
            extract_fields: &["x"],
        };

        let registry = tracing_subscriber::Registry::default().with(log_layer);

        tracing::subscriber::with_default(registry, || {
            info_span!("some_span", x = 24).in_scope(|| {
                info_span!("some_other_span", y = 30).in_scope(|| {
                    info_span!("some_span", x = 40, x = 41, x = 42).in_scope(|| {
                        tracing::error!(
                            a = 1,
                            a = 2,
                            a = 3,
                            message = "explicit message field",
                            "implicit message field"
                        );
                    });
                });
            });
        });

        let buffer = Arc::try_unwrap(buffer)
            .expect("no other reference")
            .into_inner()
            .expect("poisoned");
        let actual: serde_json::Value = serde_json::from_slice(&buffer).expect("valid JSON");
        let expected: serde_json::Value = serde_json::json!(
            {
                "timestamp": clock.now().to_rfc3339_opts(chrono::SecondsFormat::Micros, true),
                "level": "ERROR",
                "message": "explicit message field",
                "fields": {
                    "a": 3,
                },
                "spans": {
                    "some_span":{
                        "x": 24,
                    },
                    "some_other_span": {
                        "y": 30,
                    },
                    "some_span#1": {
                        "x": 42,
                    },
                },
                "extract": {
                    "x": 42,
                },
                "src": actual.as_object().unwrap().get("src").unwrap().as_str().unwrap(),
                "target": "proxy::logging::tests",
                "process_id": actual.as_object().unwrap().get("process_id").unwrap().as_number().unwrap(),
                "thread_id": actual.as_object().unwrap().get("thread_id").unwrap().as_number().unwrap(),
                "thread_name": "logging::tests::test_field_collection",
            }
        );

        assert_json_eq!(actual, expected);
    }
}
