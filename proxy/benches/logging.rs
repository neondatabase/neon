use std::io;

use criterion::{Criterion, criterion_group, criterion_main};
use proxy::logging::{Clock, JsonLoggingLayer};
use tracing_subscriber::prelude::*;

struct DevNullWriter;

impl proxy::logging::MakeWriter for DevNullWriter {
    fn make_writer(&self) -> impl io::Write {
        DevNullWriter
    }
}

impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for DevNullWriter {
    type Writer = DevNullWriter;
    fn make_writer(&'a self) -> Self::Writer {
        DevNullWriter
    }
}

impl io::Write for DevNullWriter {
    #[inline]
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        Ok(criterion::black_box(buf).len())
    }

    #[inline(always)]
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

struct FixedClock;

impl Clock for FixedClock {
    fn now(&self) -> chrono::DateTime<chrono::Utc> {
        const { chrono::DateTime::from_timestamp_nanos(1747859990_000_000_000).to_utc() }
    }
}

pub fn bench_logging(c: &mut Criterion) {
    c.bench_function("text fmt current", |b| {
        let registry = tracing_subscriber::Registry::default().with(
            tracing_subscriber::fmt::layer()
                .with_ansi(false)
                .with_target(false)
                .with_writer(DevNullWriter),
        );

        tracing::subscriber::with_default(registry, || {
            tracing::info_span!("span1", a = 42, b = true, c = "string").in_scope(|| {
                tracing::info_span!("span2", a = 42, b = true, c = "string").in_scope(|| {
                    b.iter(|| {
                        tracing::error!(a = 42, b = true, c = "string", "message field");
                    })
                });
            });
        });
    });

    c.bench_function("text fmt full", |b| {
        let registry = tracing_subscriber::Registry::default().with(
            tracing_subscriber::fmt::layer()
                .with_level(true)
                .with_file(true)
                .with_line_number(true)
                .with_target(true)
                .with_thread_ids(true)
                .with_writer(DevNullWriter),
        );

        tracing::subscriber::with_default(registry, || {
            tracing::info_span!("span1", a = 42, b = true, c = "string").in_scope(|| {
                tracing::info_span!("span2", a = 42, b = true, c = "string").in_scope(|| {
                    b.iter(|| {
                        tracing::error!(a = 42, b = true, c = "string", "message field");
                    })
                });
            });
        });
    });

    c.bench_function("json fmt", |b| {
        let registry = tracing_subscriber::Registry::default().with(
            tracing_subscriber::fmt::layer()
                .with_level(true)
                .with_file(true)
                .with_line_number(true)
                .with_target(true)
                .with_thread_ids(true)
                .with_writer(DevNullWriter)
                .json(),
        );

        tracing::subscriber::with_default(registry, || {
            tracing::info_span!("span1", a = 42, b = true, c = "string").in_scope(|| {
                tracing::info_span!("span2", a = 42, b = true, c = "string").in_scope(|| {
                    b.iter(|| {
                        tracing::error!(a = 42, b = true, c = "string", "message field");
                    })
                });
            });
        });
    });

    c.bench_function("json custom", |b| {
        let registry = tracing_subscriber::Registry::default().with(JsonLoggingLayer::new(
            FixedClock,
            DevNullWriter,
            &["a"],
        ));

        tracing::subscriber::with_default(registry, || {
            tracing::info_span!("span1", a = 42, b = true, c = "string").in_scope(|| {
                tracing::info_span!("span2", a = 42, b = true, c = "string").in_scope(|| {
                    b.iter(|| {
                        tracing::error!(a = 42, b = true, c = "string", "message field");
                    })
                });
            });
        });
    });
}

criterion_group!(benches, bench_logging);
criterion_main!(benches);
