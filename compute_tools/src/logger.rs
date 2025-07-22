use std::collections::HashMap;
use std::sync::{LazyLock, RwLock};
use tracing::Subscriber;
use tracing::info;
use tracing_appender;
use tracing_subscriber::prelude::*;
use tracing_subscriber::{fmt, layer::SubscriberExt, registry::LookupSpan};

/// Initialize logging to stderr, and OpenTelemetry tracing and exporter.
///
/// Logging is configured using either `default_log_level` or
/// `RUST_LOG` environment variable as default log level.
///
/// OpenTelemetry is configured with OTLP/HTTP exporter. It picks up
/// configuration from environment variables. For example, to change the destination,
/// set `OTEL_EXPORTER_OTLP_ENDPOINT=http://jaeger:4318`. See
/// `tracing-utils` package description.
///
pub fn init_tracing_and_logging(
    default_log_level: &str,
    log_dir_opt: &Option<String>,
) -> anyhow::Result<(
    Option<tracing_utils::Provider>,
    Option<tracing_appender::non_blocking::WorkerGuard>,
)> {
    // Initialize Logging
    let env_filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new(default_log_level));

    // Standard output streams
    let fmt_layer = tracing_subscriber::fmt::layer()
        .with_ansi(false)
        .with_target(false)
        .with_writer(std::io::stderr);

    // Logs with file rotation. Files in `$log_dir/pgcctl.yyyy-MM-dd`
    let (json_to_file_layer, _file_logs_guard) = if let Some(log_dir) = log_dir_opt {
        std::fs::create_dir_all(log_dir)?;
        let file_logs_appender = tracing_appender::rolling::RollingFileAppender::builder()
            .rotation(tracing_appender::rolling::Rotation::DAILY)
            .filename_prefix("pgcctl")
            // Lib appends to existing files, so we will keep files for up to 2 days even on restart loops.
            // At minimum, log-daemon will have 1 day to detect and upload a file (if created right before midnight).
            .max_log_files(2)
            .build(log_dir)
            .expect("Initializing rolling file appender should succeed");
        let (file_logs_writer, _file_logs_guard) =
            tracing_appender::non_blocking(file_logs_appender);
        let json_to_file_layer = tracing_subscriber::fmt::layer()
            .with_ansi(false)
            .with_target(false)
            .event_format(PgJsonLogShapeFormatter)
            .with_writer(file_logs_writer);
        (Some(json_to_file_layer), Some(_file_logs_guard))
    } else {
        (None, None)
    };

    // Initialize OpenTelemetry
    let provider =
        tracing_utils::init_tracing("compute_ctl", tracing_utils::ExportConfig::default());
    let otlp_layer = provider.as_ref().map(tracing_utils::layer);

    // Put it all together
    tracing_subscriber::registry()
        .with(env_filter)
        .with(otlp_layer)
        .with(fmt_layer)
        .with(json_to_file_layer)
        .init();
    tracing::info!("logging and tracing started");

    utils::logging::replace_panic_hook_with_tracing_panic_hook().forget();

    Ok((provider, _file_logs_guard))
}

/// Replace all newline characters with a special character to make it
/// easier to grep for log messages.
pub fn inlinify(s: &str) -> String {
    s.replace('\n', "\u{200B}")
}

pub fn startup_context_from_env() -> Option<opentelemetry::Context> {
    // Extract OpenTelemetry context for the startup actions from the
    // TRACEPARENT and TRACESTATE env variables, and attach it to the current
    // tracing context.
    //
    // This is used to propagate the context for the 'start_compute' operation
    // from the neon control plane. This allows linking together the wider
    // 'start_compute' operation that creates the compute container, with the
    // startup actions here within the container.
    //
    // There is no standard for passing context in env variables, but a lot of
    // tools use TRACEPARENT/TRACESTATE, so we use that convention too. See
    // https://github.com/open-telemetry/opentelemetry-specification/issues/740
    //
    // Switch to the startup context here, and exit it once the startup has
    // completed and Postgres is up and running.
    //
    // If this pod is pre-created without binding it to any particular endpoint
    // yet, this isn't the right place to enter the startup context. In that
    // case, the control plane should pass the tracing context as part of the
    // /configure API call.
    //
    // NOTE: This is supposed to only cover the *startup* actions. Once
    // postgres is configured and up-and-running, we exit this span. Any other
    // actions that are performed on incoming HTTP requests, for example, are
    // performed in separate spans.
    //
    // XXX: If the pod is restarted, we perform the startup actions in the same
    // context as the original startup actions, which probably doesn't make
    // sense.
    let mut startup_tracing_carrier: HashMap<String, String> = HashMap::new();
    if let Ok(val) = std::env::var("TRACEPARENT") {
        startup_tracing_carrier.insert("traceparent".to_string(), val);
    }
    if let Ok(val) = std::env::var("TRACESTATE") {
        startup_tracing_carrier.insert("tracestate".to_string(), val);
    }
    if !startup_tracing_carrier.is_empty() {
        use opentelemetry::propagation::TextMapPropagator;
        use opentelemetry_sdk::propagation::TraceContextPropagator;
        info!("got startup tracing context from env variables");
        Some(TraceContextPropagator::new().extract(&startup_tracing_carrier))
    } else {
        None
    }
}

/// Track relevant id's
const UNKNOWN_IDS: &str = r#""pg_instance_id": "", "pg_compute_id": """#;
static IDS: LazyLock<RwLock<String>> = LazyLock::new(|| RwLock::new(UNKNOWN_IDS.to_string()));

pub fn update_ids(instance_id: &Option<String>, compute_id: &Option<String>) -> anyhow::Result<()> {
    let ids = format!(
        r#""pg_instance_id": "{}", "pg_compute_id": "{}""#,
        instance_id.as_ref().map(|s| s.as_str()).unwrap_or_default(),
        compute_id.as_ref().map(|s| s.as_str()).unwrap_or_default()
    );
    let mut guard = IDS
        .write()
        .map_err(|e| anyhow::anyhow!("Log set id's rwlock poisoned: {}", e))?;
    *guard = ids;
    Ok(())
}

/// Massage compute_ctl logs into PG json log shape so we can use the same Lumberjack setup.
struct PgJsonLogShapeFormatter;
impl<S, N> fmt::format::FormatEvent<S, N> for PgJsonLogShapeFormatter
where
    S: Subscriber + for<'a> LookupSpan<'a>,
    N: for<'a> fmt::format::FormatFields<'a> + 'static,
{
    fn format_event(
        &self,
        ctx: &fmt::FmtContext<'_, S, N>,
        mut writer: fmt::format::Writer<'_>,
        event: &tracing::Event<'_>,
    ) -> std::fmt::Result {
        // Format values from the event's metadata, and open message string
        let metadata = event.metadata();
        {
            let ids_guard = IDS.read();
            let ids = ids_guard
                .as_ref()
                .map(|guard| guard.as_str())
                // Surpress so that we don't lose all uploaded/ file logs if something goes super wrong. We would notice the missing id's.
                .unwrap_or(UNKNOWN_IDS);
            write!(
                &mut writer,
                r#"{{"timestamp": "{}", "error_severity": "{}", "file_name": "{}", "backend_type": "compute_ctl_self", {}, "message": "#,
                chrono::Utc::now().format("%Y-%m-%d %H:%M:%S%.3f GMT"),
                metadata.level(),
                metadata.target(),
                ids
            )?;
        }

        let mut message = String::new();
        let message_writer = fmt::format::Writer::new(&mut message);

        // Gather the message
        ctx.field_format().format_fields(message_writer, event)?;

        // TODO: any better options than to copy-paste this OSS span formatter?
        // impl<S, N, T> FormatEvent<S, N> for Format<Full, T>
        // https://docs.rs/tracing-subscriber/latest/tracing_subscriber/fmt/trait.FormatEvent.html#impl-FormatEvent%3CS,+N%3E-for-Format%3CFull,+T%3E

        // write message, close bracket, and new line
        writeln!(writer, "{}}}", serde_json::to_string(&message).unwrap())
    }
}

#[cfg(feature = "testing")]
#[cfg(test)]
mod test {
    use super::*;
    use std::{cell::RefCell, io};

    // Use thread_local! instead of Mutex for test isolation
    thread_local! {
        static WRITER_OUTPUT: RefCell<String> = const { RefCell::new(String::new()) };
    }

    #[derive(Clone, Default)]
    struct StaticStringWriter;

    impl io::Write for StaticStringWriter {
        fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
            let output = String::from_utf8(buf.to_vec()).expect("Invalid UTF-8 in test output");
            WRITER_OUTPUT.with(|s| s.borrow_mut().push_str(&output));
            Ok(buf.len())
        }

        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }

    impl fmt::MakeWriter<'_> for StaticStringWriter {
        type Writer = Self;

        fn make_writer(&self) -> Self::Writer {
            Self
        }
    }

    #[test]
    fn test_log_pg_json_shape_formatter() {
        // Use a scoped subscriber to prevent global state pollution
        let subscriber = tracing_subscriber::registry().with(
            tracing_subscriber::fmt::layer()
                .with_ansi(false)
                .with_target(false)
                .event_format(PgJsonLogShapeFormatter)
                .with_writer(StaticStringWriter),
        );

        let _ = update_ids(&Some("000".to_string()), &Some("111".to_string()));

        // Clear any previous test state
        WRITER_OUTPUT.with(|s| s.borrow_mut().clear());

        let messages = [
            "test message",
            r#"json escape check:  name="BatchSpanProcessor.Flush.ExportError" reason="Other(reqwest::Error { kind: Request, url: \"http://localhost:4318/v1/traces\", source: hyper_
            util::client::legacy::Error(Connect, ConnectError(\"tcp connect error\", Os { code: 111, kind: ConnectionRefused, message: \"Connection refused\" })) })" Failed during the export process"#,
        ];

        tracing::subscriber::with_default(subscriber, || {
            for message in messages {
                tracing::info!(message);
            }
        });
        tracing::info!("not test message");

        // Get captured output
        let output = WRITER_OUTPUT.with(|s| s.borrow().clone());

        let json_strings: Vec<&str> = output.lines().collect();
        assert_eq!(
            json_strings.len(),
            messages.len(),
            "Log didn't have the expected number of json strings."
        );

        let json_string_shape_regex = regex::Regex::new(
            r#"\{"timestamp": "\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d{3} GMT", "error_severity": "INFO", "file_name": ".+", "backend_type": "compute_ctl_self", "pg_instance_id": "000", "pg_compute_id": "111", "message": ".+"\}"#
        ).unwrap();

        for (i, expected_message) in messages.iter().enumerate() {
            let json_string = json_strings[i];
            assert!(
                json_string_shape_regex.is_match(json_string),
                "Json log didn't match expected pattern:\n{}",
                json_string
            );
            let parsed_json: serde_json::Value = serde_json::from_str(json_string).unwrap();
            let actual_message = parsed_json["message"].as_str().unwrap();
            assert_eq!(*expected_message, actual_message);
        }
    }
}
