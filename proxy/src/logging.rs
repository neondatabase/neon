use serde::{Serialize, Serializer};
use tracing::{Level, Subscriber};
use tracing_subscriber::filter::{EnvFilter, LevelFilter};
use tracing_subscriber::fmt::format::{Format, Full};
use tracing_subscriber::fmt::time::SystemTime;
use tracing_subscriber::fmt::{FormatEvent, FormatFields, FormattedFields};
use tracing_subscriber::prelude::*;
use tracing_subscriber::registry::LookupSpan;

/// Initialize logging and OpenTelemetry tracing and exporter.
///
/// Logging can be configured using `RUST_LOG` environment variable.
///
/// OpenTelemetry is configured with OTLP/HTTP exporter. It picks up
/// configuration from environment variables. For example, to change the
/// destination, set `OTEL_EXPORTER_OTLP_ENDPOINT=http://jaeger:4318`.
/// See <https://opentelemetry.io/docs/reference/specification/sdk-environment-variables>
pub async fn init() -> anyhow::Result<LoggingGuard> {
    let env_filter = EnvFilter::builder()
        .with_default_directive(LevelFilter::INFO.into())
        .from_env_lossy()
        .add_directive("aws_config=info".parse().unwrap())
        .add_directive("azure_core::policies::transport=off".parse().unwrap());

    let fmt_layer = tracing_subscriber::fmt::layer()
        .event_format(NeonJsonLogFormat)
        .fmt_fields(JsonFieldAppender)
        .with_writer(std::io::stdout);

    let otlp_layer = tracing_utils::init_tracing(
        "proxy",
        Some(NeonJsonLogFormat::opentelemetry_error_handler),
    )
    .await;

    tracing_subscriber::registry()
        .with(env_filter)
        .with(otlp_layer)
        .with(fmt_layer)
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

struct NeonJsonLogFormat;

impl NeonJsonLogFormat {
    fn opentelemetry_error_handler(error: opentelemetry::global::Error) {
        use std::io::Write;
        let timestamp = chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Micros, true);
        let mut w = std::io::stdout();

        let mut serialize = || {
            use serde::ser::{SerializeMap, Serializer as _};

            // TODO: buffered writer
            let mut serializer = serde_json::Serializer::new(&mut w);
            let mut serializer = serializer.serialize_map(None)?;

            serializer.serialize_entry("t", &timestamp)?;
            serializer.serialize_entry("l", "error")?;
            serializer.serialize_entry("m", &format!("OpenTelemetry error: {error}"))?;
            serializer.serialize_entry("thread", &ThreadInfo)?;
            serializer.end()
        };

        serialize().expect("should be able to write to stdout");
        writeln!(&mut w).expect("should be able to write to stdout");
    }
}

impl<S, N> FormatEvent<S, N> for NeonJsonLogFormat
where
    S: Subscriber + for<'lookup> LookupSpan<'lookup>,
    N: for<'writer> FormatFields<'writer> + 'static,
{
    fn format_event(
        &self,
        ctx: &tracing_subscriber::fmt::FmtContext<'_, S, N>,
        mut writer: tracing_subscriber::fmt::format::Writer<'_>,
        event: &tracing::Event<'_>,
    ) -> std::fmt::Result
    where
        S: Subscriber + for<'a> LookupSpan<'a>,
    {
        use serde::ser::{SerializeMap, Serializer};

        let now = chrono::Utc::now();
        let timestamp = now.to_rfc3339_opts(chrono::SecondsFormat::Micros, true);

        // TODO: tracing-log?
        let meta = event.metadata();

        let mut serialize = || {
            let mut serializer = serde_json::Serializer::new(IoWriteFmtWriter(&mut writer));

            let mut serializer = serializer.serialize_map(None)?;

            // Timestamp comes first, so raw lines can be sorted by timestamp.
            serializer.serialize_entry("t", &timestamp)?;

            // Level next.
            serializer.serialize_entry("l", &SerializeLevel(meta.level()))?;

            // Message next.
            serializer.serialize_key("m")?;
            let mut message_extractor = MessageExtractor::new(serializer);
            event.record(&mut message_extractor);
            let mut serializer = message_extractor.into_serializer()?;

            serializer.serialize_entry("fields", &SerializableEventFields(event))?;

            serializer.serialize_entry("thread", &ThreadInfo)?;

            // serializer.serialize_entry("target", meta.target())?;

            let format_field_marker: std::marker::PhantomData<N> = std::marker::PhantomData;
            if let Some(span) = event
                .parent()
                .and_then(|id| ctx.span(id))
                .or_else(|| ctx.lookup_current())
            {
                serializer
                    .serialize_entry("spans", &SerializableSpans(span, format_field_marker))?;
            }

            serializer.end()
        };

        serialize().map_err(|_| std::fmt::Error)?;

        // Just a newline.
        writeln!(writer)
    }
}

struct IoWriteFmtWriter<'a>(&'a mut dyn std::fmt::Write);

impl<'a> std::io::Write for IoWriteFmtWriter<'a> {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        let s = std::str::from_utf8(buf)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;

        self.0
            .write_str(s)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;

        Ok(s.as_bytes().len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

struct SerializeLevel<'a>(&'a Level);

impl<'a> Serialize for SerializeLevel<'a> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        // This order was chosen to match log level probabilities.
        if self.0 == &Level::INFO {
            serializer.serialize_str("info")
        } else if self.0 == &Level::WARN {
            serializer.serialize_str("warn")
        } else if self.0 == &Level::ERROR {
            serializer.serialize_str("error")
        } else if self.0 == &Level::DEBUG {
            serializer.serialize_str("debug")
        } else if self.0 == &Level::TRACE {
            serializer.serialize_str("trace")
        } else {
            unreachable!()
        }
    }
}

const MESSAGE_FIELD: &str = "message";

pub struct MessageExtractor<S: serde::ser::SerializeMap> {
    serializer: S,
    state: Result<(), S::Error>,
}

impl<S: serde::ser::SerializeMap> MessageExtractor<S> {
    fn new(serializer: S) -> Self {
        Self {
            serializer,
            state: Ok(()),
        }
    }

    fn into_serializer(self) -> Result<S, S::Error> {
        // io::Errors most likely.
        self.state?;
        Ok(self.serializer)
    }
}

impl<S: serde::ser::SerializeMap> tracing::field::Visit for MessageExtractor<S> {
    fn record_f64(&mut self, field: &tracing::field::Field, value: f64) {
        if self.state.is_ok() && field.name() == MESSAGE_FIELD {
            self.state = self.serializer.serialize_value(&value);
        }
    }

    fn record_i64(&mut self, field: &tracing::field::Field, value: i64) {
        if self.state.is_ok() && field.name() == MESSAGE_FIELD {
            self.state = self.serializer.serialize_value(&value);
        }
    }

    fn record_u64(&mut self, field: &tracing::field::Field, value: u64) {
        if self.state.is_ok() && field.name() == MESSAGE_FIELD {
            self.state = self.serializer.serialize_value(&value);
        }
    }

    fn record_i128(&mut self, field: &tracing::field::Field, value: i128) {
        if self.state.is_ok() && field.name() == MESSAGE_FIELD {
            self.state = self.serializer.serialize_value(&value);
        }
    }

    fn record_u128(&mut self, field: &tracing::field::Field, value: u128) {
        if self.state.is_ok() && field.name() == MESSAGE_FIELD {
            self.state = self.serializer.serialize_value(&value);
        }
    }

    fn record_bool(&mut self, field: &tracing::field::Field, value: bool) {
        if self.state.is_ok() && field.name() == MESSAGE_FIELD {
            self.state = self.serializer.serialize_value(&value);
        }
    }

    fn record_str(&mut self, field: &tracing::field::Field, value: &str) {
        if self.state.is_ok() && field.name() == MESSAGE_FIELD {
            self.state = self.serializer.serialize_value(&value);
        }
    }

    fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
        if self.state.is_ok() && field.name() == MESSAGE_FIELD {
            self.state = self.serializer.serialize_value(&format!("{value:?}"));
        }
    }
}

struct SerializableEventFields<'a, 'event>(&'a tracing::Event<'event>);

impl<'a, 'event> serde::ser::Serialize for SerializableEventFields<'a, 'event> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        use serde::ser::SerializeMap;
        let serializer = serializer.serialize_map(None)?;
        let mut message_skipper = MessageSkipper::new(serializer);
        self.0.record(&mut message_skipper);
        let serializer = message_skipper.into_serializer()?;
        serializer.end()
    }
}

pub struct MessageSkipper<S: serde::ser::SerializeMap> {
    serializer: S,
    state: Result<(), S::Error>,
}

impl<S: serde::ser::SerializeMap> MessageSkipper<S> {
    fn new(serializer: S) -> Self {
        Self {
            serializer,
            state: Ok(()),
        }
    }

    fn into_serializer(self) -> Result<S, S::Error> {
        // io::Errors most likely.
        self.state?;
        Ok(self.serializer)
    }
}

impl<S: serde::ser::SerializeMap> tracing::field::Visit for MessageSkipper<S> {
    fn record_f64(&mut self, field: &tracing::field::Field, value: f64) {
        if self.state.is_ok() && field.name() != MESSAGE_FIELD {
            self.state = self.serializer.serialize_entry(field.name(), &value);
        }
    }

    fn record_i64(&mut self, field: &tracing::field::Field, value: i64) {
        if self.state.is_ok() && field.name() != MESSAGE_FIELD {
            self.state = self.serializer.serialize_entry(field.name(), &value);
        }
    }

    fn record_u64(&mut self, field: &tracing::field::Field, value: u64) {
        if self.state.is_ok() && field.name() != MESSAGE_FIELD {
            self.state = self.serializer.serialize_entry(field.name(), &value);
        }
    }

    fn record_i128(&mut self, field: &tracing::field::Field, value: i128) {
        if self.state.is_ok() && field.name() != MESSAGE_FIELD {
            self.state = self.serializer.serialize_entry(field.name(), &value);
        }
    }

    fn record_u128(&mut self, field: &tracing::field::Field, value: u128) {
        if self.state.is_ok() && field.name() != MESSAGE_FIELD {
            self.state = self.serializer.serialize_entry(field.name(), &value);
        }
    }

    fn record_bool(&mut self, field: &tracing::field::Field, value: bool) {
        if self.state.is_ok() && field.name() != MESSAGE_FIELD {
            self.state = self.serializer.serialize_entry(field.name(), &value);
        }
    }

    fn record_str(&mut self, field: &tracing::field::Field, value: &str) {
        if self.state.is_ok() && field.name() != MESSAGE_FIELD {
            self.state = self.serializer.serialize_entry(field.name(), &value);
        }
    }

    fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
        if self.state.is_ok() && field.name() != MESSAGE_FIELD {
            self.state = self
                .serializer
                .serialize_entry(field.name(), &format!("{value:?}"));
        }
    }
}

struct SerializableSpans<'a, Span, N>(
    tracing_subscriber::registry::SpanRef<'a, Span>,
    std::marker::PhantomData<N>,
)
where
    Span: Subscriber + for<'lookup> LookupSpan<'lookup>,
    N: for<'writer> FormatFields<'writer> + 'static;

impl<'a, Span, N> serde::ser::Serialize for SerializableSpans<'a, Span, N>
where
    Span: Subscriber + for<'lookup> LookupSpan<'lookup>,
    N: for<'writer> FormatFields<'writer> + 'static,
{
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::ser::Serializer,
    {
        use serde::ser::SerializeSeq;
        let mut serializer = serializer.serialize_seq(None)?;
        for span in self.0.scope().from_root() {
            serializer.serialize_element(&SerializableSpan(span, self.1))?;
        }
        serializer.end()
    }
}

struct SerializableSpan<'a, Span, N>(
    tracing_subscriber::registry::SpanRef<'a, Span>,
    std::marker::PhantomData<N>,
)
where
    Span: for<'lookup> LookupSpan<'lookup>,
    N: for<'writer> FormatFields<'writer> + 'static;

impl<'a, Span, N> serde::ser::Serialize for SerializableSpan<'a, Span, N>
where
    Span: for<'lookup> LookupSpan<'lookup>,
    N: for<'writer> FormatFields<'writer> + 'static,
{
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::ser::Serializer,
    {
        use serde::ser::SerializeMap;
        let mut serializer = serializer.serialize_map(None)?;
        serializer.serialize_entry("name", self.0.metadata().name())?;

        let ext = self.0.extensions();
        let data = ext
            .get::<FormattedFields<N>>()
            .expect("Unable to find FormattedFields in extensions; this is a bug");

        if !data.fields.is_empty() {
            let mut json_fields = String::with_capacity(data.fields.len() + 2);
            json_fields.push('{');
            json_fields.push_str(&data.fields);
            json_fields.push('}');
            debug_assert_eq!(
                json_fields.len(),
                json_fields.capacity(),
                "must have no free capacity to avoid allocation in RawValue"
            );
            serializer.serialize_entry(
                "fields",
                &serde_json::value::RawValue::from_string(json_fields)
                    .expect("formatter should produce valid json"),
            )?;
        }

        serializer.end()
    }
}

struct ThreadInfo;

impl serde::ser::Serialize for ThreadInfo {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::ser::Serializer,
    {
        use serde::ser::SerializeMap;
        let mut serializer = serializer.serialize_map(None)?;
        // TODO: not useful in container
        // serializer.serialize_entry("pid", &std::process::id())?;
        serializer.serialize_entry("id", &gettid::gettid())?; // TODO: tls cache
        if let Some(name) = std::thread::current().name() {
            serializer.serialize_entry("name", name)?;
        } else {
            serializer.serialize_entry("name", &None::<&str>)?;
        }
        serializer.end()
    }
}

struct JsonFieldAppender;

impl<'writer> FormatFields<'writer> for JsonFieldAppender {
    fn format_fields<R: tracing_subscriber::field::RecordFields>(
        &self,
        mut writer: tracing_subscriber::fmt::format::Writer<'writer>,
        fields: R,
    ) -> std::fmt::Result {
        fields.record(&mut JsonFieldVisitor::new(&mut writer));
        Ok(())
    }

    fn add_fields(
        &self,
        current: &'writer mut FormattedFields<Self>,
        fields: &tracing::span::Record<'_>,
    ) -> std::fmt::Result {
        if !current.fields.is_empty() {
            current.fields.push(',');
        }
        self.format_fields(current.as_writer(), fields)
    }
}

struct JsonFieldVisitor<'a, 'writer> {
    writer: &'a mut tracing_subscriber::fmt::format::Writer<'writer>,
    first_seen: bool,
}

impl<'a, 'writer> JsonFieldVisitor<'a, 'writer> {
    fn new(writer: &'a mut tracing_subscriber::fmt::format::Writer<'writer>) -> Self {
        Self {
            writer,
            first_seen: false,
        }
    }

    fn maybe_write_comma(&mut self) -> std::fmt::Result {
        if self.first_seen {
            self.writer.write_char(',')
        } else {
            self.first_seen = true;
            Ok(())
        }
    }

    fn write_field_name(&mut self, name: &str) -> std::fmt::Result {
        let name = name.strip_prefix("r#").unwrap_or(name);
        self.writer
            .write_str(&serde_json::Value::from(name).to_string())
    }

    fn write_field_value<V: Into<serde_json::Value>>(&mut self, value: V) -> std::fmt::Result {
        self.writer.write_str(&value.into().to_string())
    }
}

impl<'a, 'writer> tracing_subscriber::field::Visit for JsonFieldVisitor<'a, 'writer> {
    fn record_f64(&mut self, field: &tracing::field::Field, value: f64) {
        let mut write = || {
            self.maybe_write_comma()?;
            self.write_field_name(field.name())?;
            self.writer.write_char(':')?;
            self.write_field_value(value)
        };
        write().expect("writing to string should not fail");
    }

    fn record_i64(&mut self, field: &tracing::field::Field, value: i64) {
        let mut write = || {
            self.maybe_write_comma()?;
            self.write_field_name(field.name())?;
            self.writer.write_char(':')?;
            self.write_field_value(value)
        };
        write().expect("writing to string should not fail");
    }

    fn record_u64(&mut self, field: &tracing::field::Field, value: u64) {
        let mut write = || {
            self.maybe_write_comma()?;
            self.write_field_name(field.name())?;
            self.writer.write_char(':')?;
            self.write_field_value(value)
        };
        write().expect("writing to string should not fail");
    }

    fn record_bool(&mut self, field: &tracing::field::Field, value: bool) {
        let mut write = || {
            self.maybe_write_comma()?;
            self.write_field_name(field.name())?;
            self.writer.write_char(':')?;
            self.write_field_value(value)
        };
        write().expect("writing to string should not fail");
    }

    fn record_str(&mut self, field: &tracing::field::Field, value: &str) {
        let mut write = || {
            self.maybe_write_comma()?;
            self.write_field_name(field.name())?;
            self.writer.write_char(':')?;
            self.write_field_value(value)
        };
        write().expect("writing to string should not fail");
    }

    fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
        let mut write = || {
            self.maybe_write_comma()?;
            self.write_field_name(field.name())?;
            self.writer.write_char(':')?;
            self.write_field_value(format!("{value:?}"))
        };
        write().expect("writing to string should not fail");
    }
}
