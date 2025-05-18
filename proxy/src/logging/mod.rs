use std::cell::{Cell, RefCell};
use std::collections::HashMap;
use std::sync::atomic::{AtomicU32, Ordering};
use std::{env, fmt, io};

use chrono::{DateTime, Utc};
use itertools::Itertools;
use json::{Escaped, MapSer, SerializedValue, ValueSer};
use opentelemetry::trace::TraceContextExt;
use scopeguard::defer;
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

mod json;

/// Initialize logging and OpenTelemetry tracing and exporter.
///
/// Logging can be configured using `RUST_LOG` environment variable.
///
/// OpenTelemetry is configured with OTLP/HTTP exporter. It picks up
/// configuration from environment variables. For example, to change the
/// destination, set `OTEL_EXPORTER_OTLP_ENDPOINT=http://jaeger:4318`.
/// See <https://opentelemetry.io/docs/reference/specification/sdk-environment-variables>
pub async fn init() -> anyhow::Result<LoggingGuard> {
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

    let otlp_layer =
        tracing_utils::init_tracing("proxy", tracing_utils::ExportConfig::default()).await;

    let json_log_layer = if logfmt == LogFormat::Json {
        Some(JsonLoggingLayer::new(
            RealClock,
            StderrWriter {
                stderr: std::io::stderr(),
            },
            ["request_id", "session_id", "conn_id"],
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

    Ok(LoggingGuard)
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

    Ok(LoggingGuard)
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

pub struct LoggingGuard;

impl Drop for LoggingGuard {
    fn drop(&mut self) {
        // Shutdown trace pipeline gracefully, so that it has a chance to send any
        // pending traces before we exit.
        tracing::info!("shutting down the tracing machinery");
        tracing_utils::shutdown_tracing();
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

thread_local! {
    /// Protects against deadlocks and double panics during log writing.
    /// The current panic handler will use tracing to log panic information.
    static REENTRANCY_GUARD: Cell<bool> = const { Cell::new(false) };
    /// Thread-local instance with per-thread buffer for log writing.
    static EVENT_FORMATTER: RefCell<EventFormatter> = RefCell::new(EventFormatter::new());
    /// Cached OS thread ID.
    static THREAD_ID: u64 = gettid::gettid();
}

/// Implements tracing layer to handle events specific to logging.
struct JsonLoggingLayer<C: Clock, W: MakeWriter, const F: usize> {
    clock: C,
    writer: W,
    state: JsonLoggingState<F>,
}

/// Map for values fixed at callsite registration.
// We use papaya here because registration rarely happens post-startup.
// papaya is good for read-heavy workloads.
//
// We use rustc_hash here because callsite::Identifier will always be an integer with low-bit entropy,
// since it's always a pointer to static mutable data. rustc_hash was designed for low-bit entropy.
type CallsiteMap<T> =
    papaya::HashMap<callsite::Identifier, T, std::hash::BuildHasherDefault<rustc_hash::FxHasher>>;

struct JsonLoggingState<const F: usize> {
    /// tracks which fields of each **event** are duplicates
    skipped_field_indices: CallsiteMap<SkippedFieldIndices>,
    /// tracks the fixed "callsite ID" for each **span**.
    /// note: this is not stable between runs.
    callsite_ids: CallsiteMap<CallsiteId>,

    /// Fields we want to keep track of in a separate json object.
    // We use a const generic and arrays to bypass one heap allocation.
    extract_fields: [Escaped; F],
}

impl<C: Clock, W: MakeWriter, const F: usize> JsonLoggingLayer<C, W, F> {
    fn new(clock: C, writer: W, extract_fields: [&'static str; F]) -> Self {
        JsonLoggingLayer {
            clock,
            writer,
            state: JsonLoggingState {
                skipped_field_indices: papaya::HashMap::default(),
                callsite_ids: papaya::HashMap::default(),
                extract_fields: extract_fields.map(Escaped::new),
            },
        }
    }
}

impl<S, C: Clock + 'static, W: MakeWriter + 'static, const F: usize> Layer<S>
    for JsonLoggingLayer<C, W, F>
where
    S: Subscriber + for<'a> LookupSpan<'a>,
{
    fn on_event(&self, event: &Event<'_>, ctx: Context<'_, S>) {
        use std::io::Write;

        // TODO: consider special tracing subscriber to grab timestamp very
        //       early, before OTel machinery, and add as event extension.
        let now = self.clock.now();

        let res: io::Result<()> = REENTRANCY_GUARD.with(move |entered| {
            if entered.get() {
                let mut formatter = EventFormatter::new();
                formatter.format(now, event, &ctx, &self.state);
                self.writer.make_writer().write_all(formatter.buffer())
            } else {
                entered.set(true);
                defer!(entered.set(false););

                EVENT_FORMATTER.with_borrow_mut(move |formatter| {
                    formatter.reset();
                    formatter.format(now, event, &ctx, &self.state);
                    self.writer.make_writer().write_all(formatter.buffer())
                })
            }
        });

        // In case logging fails we generate a simpler JSON object.
        if let Err(err) = res {
            if let Ok(mut line) = serde_json::to_vec(&serde_json::json!( {
                "timestamp": now.to_rfc3339_opts(chrono::SecondsFormat::Micros, true),
                "level": "ERROR",
                "message": format_args!("cannot log event: {err:?}"),
                "fields": {
                    "event": format_args!("{event:?}"),
                },
            })) {
                line.push(b'\n');
                self.writer.make_writer().write_all(&line).ok();
            }
        }
    }

    /// Registers a SpanFields instance as span extension.
    fn on_new_span(&self, attrs: &span::Attributes<'_>, id: &span::Id, ctx: Context<'_, S>) {
        let span = ctx.span(id).expect("span must exist");

        let fields = SpanFields::new(attrs);
        span.extensions_mut().insert(fields);
    }

    fn on_record(&self, id: &span::Id, values: &span::Record<'_>, ctx: Context<'_, S>) {
        let span = ctx.span(id).expect("span must exist");

        // assumption: `on_record` is rarely called.
        // assumption: a span being updated by one thread,
        //             and formatted by another thread is even rarer.
        if let Some(data) = span.extensions_mut().get_mut::<SpanFields>() {
            data.record_fields(values);
        }
    }

    /// Called (lazily) whenever a new log call is executed. We quickly check
    /// for duplicate field names and record duplicates as skippable. Last one
    /// wins.
    fn register_callsite(&self, metadata: &'static Metadata<'static>) -> Interest {
        if !metadata.is_event() {
            self.state
                .callsite_ids
                .pin()
                .get_or_insert_with(metadata.callsite(), CallsiteId::next);

            // Must not be never because we wouldn't get trace and span data.
            return Interest::always();
        }

        let mut field_indices = SkippedFieldIndices::default();
        let mut seen_fields = HashMap::new();
        for field in metadata.fields() {
            use std::collections::hash_map::Entry;
            match seen_fields.entry(field.name()) {
                Entry::Vacant(entry) => {
                    // field not seen yet
                    entry.insert(field.index());
                }
                Entry::Occupied(mut entry) => {
                    // replace currently stored index
                    let old_index = entry.insert(field.index());
                    // ... and append it to list of skippable indices
                    field_indices.push(old_index);
                }
            }
        }

        if !field_indices.is_empty() {
            self.state
                .skipped_field_indices
                .pin()
                .insert(metadata.callsite(), field_indices);
        }

        Interest::always()
    }
}

#[derive(Copy, Clone, Debug, Default)]
#[repr(transparent)]
struct CallsiteId(u32);

impl CallsiteId {
    #[inline]
    fn next() -> Self {
        // Start at 1 to reserve 0 for default.
        static COUNTER: AtomicU32 = AtomicU32::new(1);
        CallsiteId(COUNTER.fetch_add(1, Ordering::Relaxed))
    }
}

impl fmt::Display for CallsiteId {
    #[inline]
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

/// Stores span field values recorded during the spans lifetime.
struct SpanFields {
    fields: Vec<(&'static str, Option<SerializedValue>)>,
}

impl SpanFields {
    #[inline]
    fn new(attrs: &span::Attributes<'_>) -> Self {
        let mut fields = attrs
            .fields()
            .iter()
            .map(|f| (f.name(), None))
            .collect_vec();

        attrs.values().record(&mut SpanFieldsRecorder {
            fields: &mut fields,
        });

        Self { fields }
    }

    #[inline]
    fn record_fields<R: tracing_subscriber::field::RecordFields>(&mut self, fields: R) {
        fields.record(&mut SpanFieldsRecorder {
            fields: &mut self.fields,
        });
    }
}

/// Implements a tracing field visitor to convert and store values.
struct SpanFieldsRecorder<'m> {
    fields: &'m mut [(&'static str, Option<SerializedValue>)],
}

impl SpanFieldsRecorder<'_> {
    #[inline]
    fn record_value(&mut self, field: &tracing::field::Field, value: SerializedValue) {
        let f = &mut self.fields[field.index()];
        debug_assert_eq!(f.0, field.name());
        f.1 = Some(value);
    }
}

impl tracing::field::Visit for SpanFieldsRecorder<'_> {
    #[inline]
    fn record_f64(&mut self, field: &tracing::field::Field, value: f64) {
        self.record_value(field, SerializedValue::float(value));
    }

    #[inline]
    fn record_i64(&mut self, field: &tracing::field::Field, value: i64) {
        self.record_value(field, SerializedValue::int(value));
    }

    #[inline]
    fn record_u64(&mut self, field: &tracing::field::Field, value: u64) {
        self.record_value(field, SerializedValue::int(value));
    }

    #[inline]
    fn record_i128(&mut self, field: &tracing::field::Field, value: i128) {
        self.record_value(field, SerializedValue::int(value));
    }

    #[inline]
    fn record_u128(&mut self, field: &tracing::field::Field, value: u128) {
        self.record_value(field, SerializedValue::int(value));
    }

    #[inline]
    fn record_bool(&mut self, field: &tracing::field::Field, value: bool) {
        self.record_value(field, SerializedValue::bool(value));
    }

    #[inline]
    fn record_bytes(&mut self, field: &tracing::field::Field, value: &[u8]) {
        self.record_value(field, SerializedValue::bytes_hex(value));
    }

    #[inline]
    fn record_str(&mut self, field: &tracing::field::Field, value: &str) {
        self.record_value(field, SerializedValue::str(value));
    }

    #[inline]
    fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
        self.record_value(field, SerializedValue::str_args(format_args!("{value:?}")));
    }

    #[inline]
    fn record_error(
        &mut self,
        field: &tracing::field::Field,
        value: &(dyn std::error::Error + 'static),
    ) {
        self.record_value(field, SerializedValue::str_args(format_args!("{value}")));
    }
}

/// List of field indices skipped during logging. Can list duplicate fields or
/// metafields not meant to be logged.
#[derive(Copy, Clone, Default)]
struct SkippedFieldIndices {
    // this assumes that no span/event will have more than 64 fields.
    // this is guaranteed for now as tracing will error if >32 fields are set.
    bits: u64,
}

impl SkippedFieldIndices {
    #[inline]
    fn is_empty(self) -> bool {
        self.bits == 0
    }

    #[inline]
    fn push(&mut self, index: usize) {
        self.bits |= 1u64
            .checked_shl(index as u32)
            .expect("field index too large");
    }

    #[inline]
    fn contains(self, index: usize) -> bool {
        self.bits
            & 1u64
                .checked_shl(index as u32)
                .expect("field index too large")
            != 0
    }
}

/// Formats a tracing event and writes JSON to its internal buffer including a newline.
// TODO: buffer capacity management, truncate if too large
struct EventFormatter {
    logline_buffer: Vec<u8>,
}

impl EventFormatter {
    #[inline]
    fn new() -> Self {
        EventFormatter {
            logline_buffer: Vec::new(),
        }
    }

    #[inline]
    fn buffer(&self) -> &[u8] {
        &self.logline_buffer
    }

    #[inline]
    fn reset(&mut self) {
        self.logline_buffer.clear();
    }

    fn format<S, const F: usize>(
        &mut self,
        now: DateTime<Utc>,
        event: &Event<'_>,
        ctx: &Context<'_, S>,
        state: &JsonLoggingState<F>,
    ) where
        S: Subscriber + for<'a> LookupSpan<'a>,
    {
        let timestamp = now.to_rfc3339_opts(chrono::SecondsFormat::Micros, true);

        use tracing_log::NormalizeEvent;
        let normalized_meta = event.normalized_metadata();
        let meta = normalized_meta.as_ref().unwrap_or_else(|| event.metadata());

        let skipped_field_indices = state
            .skipped_field_indices
            .pin()
            .get(&meta.callsite())
            .copied()
            // default SkippedFieldIndices represents no skipped fields.
            .unwrap_or_default();

        {
            let mut root = ValueSer::new(&mut self.logline_buffer).map();
            macro_rules! entry {
                ($s:literal) => {
                    root.entry(const { Escaped::new($s) })
                };
            }

            // Timestamp comes first, so raw lines can be sorted by timestamp.
            entry!("timestamp").str(&timestamp);

            // Level next.
            entry!("level").str(meta.level().as_str());

            // Message next.
            event.record(&mut MessageFieldExtractor::new(
                entry!("message"),
                skipped_field_indices,
            ));

            // Direct message fields.
            let mut fields_present = FieldsPresent(false, skipped_field_indices);
            event.record(&mut fields_present);
            if fields_present.0 {
                let map = entry!("fields").map();
                event.record(&mut MessageFieldSkipper::new(map, skipped_field_indices));
            }

            let mut extracted_fields = [const { None }; F];
            {
                let mut spans_map = entry!("spans").map();

                let spans = ctx
                    .event_span(event)
                    .map_or(vec![], |parent| parent.scope().collect_vec());

                for span in spans.iter().rev() {
                    // Append a numeric callsite ID to the span name to keep the name unique
                    // in the JSON object.
                    let cid = state
                        .callsite_ids
                        .pin()
                        .get(&span.metadata().callsite())
                        .copied()
                        .unwrap_or_default();

                    let ext = span.extensions();

                    // all spans must have this extension
                    let Some(fields) = ext.get::<SpanFields>() else {
                        continue;
                    };

                    // Loki turns the # into an underscore during field name concatenation.
                    let mut span_map = spans_map
                        .entry_escape_args(format_args!("{}#{}", span.metadata().name(), &cid))
                        .map();

                    for &(name, ref value) in &fields.fields {
                        let Some(value) = value else { continue };

                        span_map.entry_escape(name).serialize(value);

                        // expectation: this list is small, so a linear search is fast.
                        if let Some(index) =
                            state.extract_fields.iter().position(|x| x.as_str() == name)
                        {
                            // TODO: replace clone with reference, if possible.
                            extracted_fields[index] = Some(value.clone());
                        }
                    }
                }
            }

            // TODO: thread-local cache?
            let pid = std::process::id();
            // Skip adding pid 1 to reduce noise for services running in containers.
            if pid != 1 {
                entry!("process_id").int(pid);
            }

            THREAD_ID.with(|tid| entry!("thread_id").int(*tid));

            // TODO: tls cache? name could change
            if let Some(thread_name) = std::thread::current().name() {
                if !thread_name.is_empty() && thread_name != "tokio-runtime-worker" {
                    entry!("thread_name").str(thread_name);
                }
            }

            if let Some(task_id) = tokio::task::try_id() {
                entry!("task_id").str_args(format_args!("{task_id}"));
            }

            entry!("target").str(meta.target());

            // Skip adding module if it's the same as target.
            if let Some(module) = meta.module_path() {
                if module != meta.target() {
                    entry!("module").str(module);
                }
            }

            if let Some(file) = meta.file() {
                if let Some(line) = meta.line() {
                    entry!("src").str_args(format_args!("{file}:{line}"));
                } else {
                    entry!("src").str(file);
                }
            }

            {
                let otel_context = Span::current().context();
                let otel_spanref = otel_context.span();
                let span_context = otel_spanref.span_context();
                if span_context.is_valid() {
                    entry!("trace_id").str_args(format_args!("{}", span_context.trace_id()));
                }
            }

            if extracted_fields.iter().any(|v| !v.is_none()) {
                // TODO: add fields from event, too?
                let mut extract_map = entry!("extract").map();

                for (i, value) in extracted_fields.iter().enumerate() {
                    let Some(value) = value else { continue };
                    extract_map.entry(state.extract_fields[i]).serialize(value);
                }
            }
        }

        self.logline_buffer.push(b'\n');
    }
}

/// Extracts the message field that's mixed will other fields.
struct MessageFieldExtractor<'buf> {
    serializer: Option<ValueSer<'buf>>,
    skipped_field_indices: SkippedFieldIndices,
}

impl Drop for MessageFieldExtractor<'_> {
    fn drop(&mut self) {
        if let Some(s) = self.serializer.take() {
            s.str("");
        }
    }
}

impl<'buf> MessageFieldExtractor<'buf> {
    #[inline]
    fn new(serializer: ValueSer<'buf>, skipped_field_indices: SkippedFieldIndices) -> Self {
        Self {
            serializer: Some(serializer),
            skipped_field_indices,
        }
    }

    #[inline]
    fn accept_field(&mut self, field: &tracing::field::Field) -> Option<ValueSer> {
        if self.serializer.is_some()
            && field.name() == MESSAGE_FIELD
            && !self.skipped_field_indices.contains(field.index())
        {
            self.serializer.take()
        } else {
            None
        }
    }

    #[inline]
    fn visit(&mut self, field: &tracing::field::Field, f: impl FnOnce(ValueSer)) {
        if let Some(s) = self.accept_field(field) {
            f(s);
        }
    }
}

impl tracing::field::Visit for MessageFieldExtractor<'_> {
    #[inline]
    fn record_f64(&mut self, field: &tracing::field::Field, value: f64) {
        self.visit(field, |s| s.float(value));
    }

    #[inline]
    fn record_i64(&mut self, field: &tracing::field::Field, value: i64) {
        self.visit(field, |s| s.int(value));
    }

    #[inline]
    fn record_u64(&mut self, field: &tracing::field::Field, value: u64) {
        self.visit(field, |s| s.int(value));
    }

    #[inline]
    fn record_i128(&mut self, field: &tracing::field::Field, value: i128) {
        self.visit(field, |s| s.int(value));
    }

    #[inline]
    fn record_u128(&mut self, field: &tracing::field::Field, value: u128) {
        self.visit(field, |s| s.int(value));
    }

    #[inline]
    fn record_bool(&mut self, field: &tracing::field::Field, value: bool) {
        self.visit(field, |s| s.bool(value));
    }

    #[inline]
    fn record_bytes(&mut self, field: &tracing::field::Field, value: &[u8]) {
        self.visit(field, |s| s.bytes_hex(value));
    }

    #[inline]
    fn record_str(&mut self, field: &tracing::field::Field, value: &str) {
        self.visit(field, |s| s.str(value));
    }

    #[inline]
    fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
        self.visit(field, |s| s.str_args(format_args!("{value:?}")));
    }

    #[inline]
    fn record_error(
        &mut self,
        field: &tracing::field::Field,
        value: &(dyn std::error::Error + 'static),
    ) {
        self.visit(field, |s| s.str_args(format_args!("{value}")));
    }
}

/// Checks if there's any fields and field values present. If not, the JSON subobject
/// can be skipped.
// This is entirely optional and only cosmetic, though maybe helps a
// bit during log parsing in dashboards when there's no field with empty object.
struct FieldsPresent(pub bool, SkippedFieldIndices);

// Even though some methods have an overhead (error, bytes) it is assumed the
// compiler won't include this since we ignore the value entirely.
impl tracing::field::Visit for FieldsPresent {
    #[inline]
    fn record_debug(&mut self, field: &tracing::field::Field, _: &dyn std::fmt::Debug) {
        if !self.1.contains(field.index())
            && field.name() != MESSAGE_FIELD
            && !field.name().starts_with("log.")
        {
            self.0 |= true;
        }
    }
}

/// A tracing field visitor that skips the message field.
struct MessageFieldSkipper<'buf> {
    serializer: MapSer<'buf>,
    skipped_field_indices: SkippedFieldIndices,
}

impl<'buf> MessageFieldSkipper<'buf> {
    #[inline]
    fn new(serializer: MapSer<'buf>, skipped_field_indices: SkippedFieldIndices) -> Self {
        Self {
            serializer,
            skipped_field_indices,
        }
    }

    #[inline]
    fn accept_field(&self, field: &tracing::field::Field) -> bool {
        field.name() != MESSAGE_FIELD
            && !field.name().starts_with("log.")
            && !self.skipped_field_indices.contains(field.index())
    }
}

impl tracing::field::Visit for MessageFieldSkipper<'_> {
    #[inline]
    fn record_f64(&mut self, field: &tracing::field::Field, value: f64) {
        if self.accept_field(field) {
            self.serializer.entry_escape(field.name()).float(value);
        }
    }

    #[inline]
    fn record_i64(&mut self, field: &tracing::field::Field, value: i64) {
        if self.accept_field(field) {
            self.serializer.entry_escape(field.name()).int(value);
        }
    }

    #[inline]
    fn record_u64(&mut self, field: &tracing::field::Field, value: u64) {
        if self.accept_field(field) {
            self.serializer.entry_escape(field.name()).int(value);
        }
    }

    #[inline]
    fn record_i128(&mut self, field: &tracing::field::Field, value: i128) {
        if self.accept_field(field) {
            self.serializer.entry_escape(field.name()).int(value);
        }
    }

    #[inline]
    fn record_u128(&mut self, field: &tracing::field::Field, value: u128) {
        if self.accept_field(field) {
            self.serializer.entry_escape(field.name()).int(value);
        }
    }

    #[inline]
    fn record_bool(&mut self, field: &tracing::field::Field, value: bool) {
        if self.accept_field(field) {
            self.serializer.entry_escape(field.name()).bool(value);
        }
    }

    #[inline]
    fn record_bytes(&mut self, field: &tracing::field::Field, value: &[u8]) {
        if self.accept_field(field) {
            self.serializer.entry_escape(field.name()).bytes_hex(value);
        }
    }

    #[inline]
    fn record_str(&mut self, field: &tracing::field::Field, value: &str) {
        if self.accept_field(field) {
            self.serializer.entry_escape(field.name()).str(value);
        }
    }

    #[inline]
    fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
        if self.accept_field(field) {
            self.serializer
                .entry_escape(field.name())
                .str_args(format_args!("{value:?}"));
        }
    }

    #[inline]
    fn record_error(
        &mut self,
        field: &tracing::field::Field,
        value: &(dyn std::error::Error + 'static),
    ) {
        if self.accept_field(field) {
            self.serializer
                .entry_escape(field.name())
                .str_args(format_args!("{value}"));
        }
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
            writer: buffer.clone(),
            state: JsonLoggingState {
                skipped_field_indices: CallsiteMap::default(),
                callsite_ids: CallsiteMap::default(),
                extract_fields: [Escaped::new("x")],
            },
        };

        let registry = tracing_subscriber::Registry::default().with(log_layer);

        tracing::subscriber::with_default(registry, || {
            info_span!("some_span", x = 24).in_scope(|| {
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
                    "some_span#1":{
                        "x": 24,
                    },
                    "some_span#2": {
                        "x": 42,
                    }
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
