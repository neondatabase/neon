use std::{
    str::FromStr,
    sync::{Arc, Mutex},
};

use anyhow::Context;
use strum_macros::{EnumString, EnumVariantNames};
use tracing_subscriber::{
    fmt::{format, Layer, TestWriter},
    layer::Layered,
    reload, Registry,
};

pub use tracing::Level;
pub use tracing_subscriber::{filter::Directive, EnvFilter};

pub const DEFAULT_LOG_LEVEL: Level = Level::INFO;

#[derive(EnumString, EnumVariantNames, Eq, PartialEq, Debug, Clone, Copy)]
#[strum(serialize_all = "snake_case")]
pub enum LogFormat {
    Plain,
    Json,
    /// Makes tracing to output logs similar way the std test crate does,
    /// see [`TestWriter`] for the details.
    Test,
}

impl LogFormat {
    pub fn from_config(s: &str) -> anyhow::Result<LogFormat> {
        use strum::VariantNames;
        LogFormat::from_str(s).with_context(|| {
            format!(
                "Unrecognized log format. Please specify one of: {:?}",
                LogFormat::VARIANTS
            )
        })
    }
}

type PlainSubscriber = Layered<Layer<Registry>, Registry>;
type StdoutWriter = fn() -> std::io::Stdout;
type JsonSubscriber = Layered<
    Layer<Registry, format::JsonFields, format::Format<format::Json>, StdoutWriter>,
    Registry,
>;
type TestSubscriber =
    Layered<Layer<Registry, format::DefaultFields, format::Format, TestWriter>, Registry>;

/// A helper enum to ease log reloads.
/// Tracing [0.4](https://github.com/tokio-rs/tracing/milestone/11)
/// includes https://github.com/tokio-rs/tracing/pull/1035
/// that makes this enum and related types obsolete.
#[derive(Clone)]
pub enum LogReloadHandle {
    Plain(reload::Handle<EnvFilter, PlainSubscriber>),
    Test(reload::Handle<EnvFilter, TestSubscriber>),
    Json(reload::Handle<EnvFilter, JsonSubscriber>),
    /// Allows to update and read its [`EnvFilter`] state via the [`reload::Handle`]-like API,
    /// but does not actually affect any logging.
    /// Could be used in any unit tests that wrap [`LogReloadHandle`] around.
    Noop(Arc<Mutex<EnvFilter>>),
}

impl LogReloadHandle {
    pub fn noop() -> Self {
        Self::Noop(Arc::new(Mutex::new(initial_env_filter())))
    }

    pub fn reload(&self, new_value: impl Into<EnvFilter>) -> Result<(), reload::Error> {
        match self {
            Self::Plain(p) => p.reload(new_value),
            Self::Json(j) => j.reload(new_value),
            Self::Test(t) => t.reload(new_value),
            Self::Noop(state) => {
                *state.lock().unwrap() = new_value.into();
                Ok(())
            }
        }
    }

    pub fn modify(&self, f: impl FnOnce(&mut EnvFilter)) -> Result<(), reload::Error> {
        match self {
            Self::Plain(p) => p.modify(f),
            Self::Json(j) => j.modify(f),
            Self::Test(t) => t.modify(f),
            Self::Noop(state) => {
                f(&mut state.lock().unwrap());
                Ok(())
            }
        }
    }

    pub fn with_current<T>(&self, f: impl FnOnce(&EnvFilter) -> T) -> Result<T, reload::Error> {
        match self {
            Self::Plain(p) => p.with_current(f),
            Self::Json(j) => j.with_current(f),
            Self::Test(t) => t.with_current(f),
            Self::Noop(state) => Ok(f(&state.lock().unwrap())),
        }
    }
}

/// We fall back to printing all spans at [`DEFAULT_LOG_LEVEL`] or above if `RUST_LOG` environment variable is not set.
pub fn initial_env_filter() -> EnvFilter {
    EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new(DEFAULT_LOG_LEVEL.to_string()))
}

pub fn init(log_format: LogFormat) -> LogReloadHandle {
    let base_logger = tracing_subscriber::fmt()
        .with_env_filter(initial_env_filter())
        .with_target(false)
        .with_ansi(atty::is(atty::Stream::Stdout))
        .with_writer(std::io::stdout as StdoutWriter);

    match log_format {
        LogFormat::Json => {
            let json = base_logger.json().with_filter_reloading();
            let handle = json.reload_handle();
            json.init();
            LogReloadHandle::Json(handle)
        }
        LogFormat::Plain => {
            let plain = base_logger.with_filter_reloading();
            let handle = plain.reload_handle();
            plain.init();
            LogReloadHandle::Plain(handle)
        }
        LogFormat::Test => {
            let test = base_logger.with_test_writer().with_filter_reloading();
            let handle = test.reload_handle();
            test.init();
            LogReloadHandle::Test(handle)
        }
    }
}
