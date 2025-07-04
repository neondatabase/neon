//! Glue code to hook up Rust logging with the `tracing` crate to the PostgreSQL log
//!
//! In the Rust threads, the log messages are written to a mpsc Channel, and the Postgres
//! process latch is raised. That wakes up the loop in the main thread. It reads the
//! message from the channel and ereport()s it. This ensures that only one thread, the main
//! thread, calls the PostgreSQL logging routines at any time.

use std::sync::mpsc::sync_channel;
use std::sync::mpsc::{Receiver, SyncSender};
use std::sync::mpsc::{TryRecvError, TrySendError};

use tracing::info;
use tracing::{Event, Level, Metadata, Subscriber};
use tracing_subscriber::filter::LevelFilter;
use tracing_subscriber::fmt::FmtContext;
use tracing_subscriber::fmt::FormatEvent;
use tracing_subscriber::fmt::FormatFields;
use tracing_subscriber::fmt::FormattedFields;
use tracing_subscriber::fmt::MakeWriter;
use tracing_subscriber::fmt::format::Writer;
use tracing_subscriber::registry::LookupSpan;

use crate::worker_process::callbacks::callback_set_my_latch;

pub struct LoggingState {
    receiver: Receiver<FormattedEventWithMeta>,
}

/// Called once, at worker process startup. The returned LoggingState is passed back
/// in the subsequent calls to `pump_logging`. It is opaque to the C code.
#[unsafe(no_mangle)]
pub extern "C" fn configure_logging() -> Box<LoggingState> {
    let (sender, receiver) = sync_channel(1000);

    let maker = Maker { channel: sender };

    use tracing_subscriber::prelude::*;
    let r = tracing_subscriber::registry();

    let r = r.with(
        tracing_subscriber::fmt::layer()
            .with_ansi(false)
            .event_format(SimpleFormatter::new())
            .with_writer(maker)
            // TODO: derive this from log_min_messages?
            .with_filter(LevelFilter::from_level(Level::INFO)),
    );
    r.init();

    info!("communicator process logging started");

    let state = LoggingState { receiver };

    Box::new(state)
}

/// Read one message from the logging queue. This is essentially a wrapper to Receiver,
/// with a C-friendly signature.
///
/// The message is copied into *errbuf, which is a caller-supplied buffer of size `errbuf_len`.
/// If the message doesn't fit in the buffer, it is truncated. It is always NULL-terminated.
///
/// The error level is returned *elevel_p. It's one of the PostgreSQL error levels, see elog.h
#[unsafe(no_mangle)]
pub extern "C" fn pump_logging(
    state: &mut LoggingState,
    errbuf: *mut u8,
    errbuf_len: u32,
    elevel_p: &mut i32,
) -> i32 {
    let msg = match state.receiver.try_recv() {
        Err(TryRecvError::Empty) => return 0,
        Err(TryRecvError::Disconnected) => return -1,
        Ok(msg) => msg,
    };

    let src: &[u8] = &msg.message;
    let dst = errbuf;
    let len = std::cmp::min(src.len(), errbuf_len as usize - 1);
    unsafe {
        std::ptr::copy_nonoverlapping(src.as_ptr(), dst, len);
        *(errbuf.add(len)) = b'\0'; // NULL terminator
    }

    // XXX: these levels are copied from PostgreSQL's elog.h. Introduce another enum
    // to hide these?
    *elevel_p = match msg.level {
        Level::TRACE => 10, // DEBUG5
        Level::DEBUG => 14, // DEBUG1
        Level::INFO => 17,  // INFO
        Level::WARN => 19,  // WARNING
        Level::ERROR => 21, // ERROR
    };

    1
}

//---- The following functions can be called from any thread ----

#[derive(Clone)]
struct FormattedEventWithMeta {
    message: Vec<u8>,
    level: tracing::Level,
}

impl Default for FormattedEventWithMeta {
    fn default() -> Self {
        FormattedEventWithMeta {
            message: Vec::new(),
            level: tracing::Level::DEBUG,
        }
    }
}

struct EventBuilder<'a> {
    event: FormattedEventWithMeta,

    maker: &'a Maker,
}

impl std::io::Write for EventBuilder<'_> {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.event.message.write(buf)
    }
    fn flush(&mut self) -> std::io::Result<()> {
        self.maker.send_event(self.event.clone());
        Ok(())
    }
}

impl Drop for EventBuilder<'_> {
    fn drop(&mut self) {
        let maker = self.maker;
        let event = std::mem::take(&mut self.event);

        maker.send_event(event);
    }
}

struct Maker {
    channel: SyncSender<FormattedEventWithMeta>,
}

impl<'a> MakeWriter<'a> for Maker {
    type Writer = EventBuilder<'a>;

    fn make_writer(&'a self) -> Self::Writer {
        panic!("not expected to be called when make_writer_for is implemented");
    }

    fn make_writer_for(&'a self, meta: &Metadata<'_>) -> Self::Writer {
        EventBuilder {
            event: FormattedEventWithMeta {
                message: Vec::new(),
                level: *meta.level(),
            },
            maker: self,
        }
    }
}

impl Maker {
    fn send_event(&self, e: FormattedEventWithMeta) {
        match self.channel.try_send(e) {
            Ok(()) => {
                // notify the main thread
                callback_set_my_latch();
            }
            Err(TrySendError::Disconnected(_)) => {}
            Err(TrySendError::Full(_)) => {
                // TODO: record that some messages were lost
            }
        }
    }
}

/// Simple formatter implementation for tracing_subscriber, which prints the log
/// spans and message part like the default formatter, but no timestamp or error
/// level. The error level is captured separately by `FormattedEventWithMeta',
/// and when the error is printed by the main thread, with PostgreSQL ereport(),
/// it gets a timestamp at that point. (The timestamp printed will therefore lag
/// behind the timestamp on the event here, if the main thread doesn't process
/// the log message promptly)
struct SimpleFormatter;

impl<S, N> FormatEvent<S, N> for SimpleFormatter
where
    S: Subscriber + for<'a> LookupSpan<'a>,
    N: for<'a> FormatFields<'a> + 'static,
{
    fn format_event(
        &self,
        ctx: &FmtContext<'_, S, N>,
        mut writer: Writer<'_>,
        event: &Event<'_>,
    ) -> std::fmt::Result {
        // Format all the spans in the event's span context.
        if let Some(scope) = ctx.event_scope() {
            for span in scope.from_root() {
                write!(writer, "{}", span.name())?;

                // `FormattedFields` is a formatted representation of the span's
                // fields, which is stored in its extensions by the `fmt` layer's
                // `new_span` method. The fields will have been formatted
                // by the same field formatter that's provided to the event
                // formatter in the `FmtContext`.
                let ext = span.extensions();
                let fields = &ext
                    .get::<FormattedFields<N>>()
                    .expect("will never be `None`");

                // Skip formatting the fields if the span had no fields.
                if !fields.is_empty() {
                    write!(writer, "{{{fields}}}")?;
                }
                write!(writer, ": ")?;
            }
        }

        // Write fields on the event
        ctx.field_format().format_fields(writer.by_ref(), event)?;

        writeln!(writer)
    }
}

impl SimpleFormatter {
    fn new() -> Self {
        SimpleFormatter {}
    }
}
