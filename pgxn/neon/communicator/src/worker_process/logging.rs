//! Glue code to hook up Rust logging with the `tracing` crate to the PostgreSQL log
//!
//! In the Rust threads, the log messages are written to a mpsc Channel, and the Postgres
//! process latch is raised. That wakes up the loop in the main thread, see
//! `communicator_new_bgworker_main()`. It reads the message from the channel and
//! ereport()s it. This ensures that only one thread, the main thread, calls the
//! PostgreSQL logging routines at any time.

use std::ffi::c_char;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc::sync_channel;
use std::sync::mpsc::{Receiver, SyncSender};
use std::sync::mpsc::{TryRecvError, TrySendError};

use tracing::info;
use tracing::{Event, Level, Metadata, Subscriber};
use tracing_subscriber::filter::LevelFilter;
use tracing_subscriber::fmt::format::Writer;
use tracing_subscriber::fmt::{FmtContext, FormatEvent, FormatFields, FormattedFields, MakeWriter};
use tracing_subscriber::registry::LookupSpan;

use crate::worker_process::callbacks::callback_set_my_latch;

/// This handle is passed to the C code, and used by [`communicator_worker_poll_logging`]
pub struct LoggingReceiver {
    receiver: Receiver<FormattedEventWithMeta>,
}

/// This is passed to `tracing`
struct LoggingSender {
    sender: SyncSender<FormattedEventWithMeta>,
}

static DROPPED_EVENT_COUNT: AtomicU64 = AtomicU64::new(0);

/// Called once, at worker process startup. The returned LoggingState is passed back
/// in the subsequent calls to `pump_logging`. It is opaque to the C code.
#[unsafe(no_mangle)]
pub extern "C" fn communicator_worker_configure_logging() -> Box<LoggingReceiver> {
    let (sender, receiver) = sync_channel(1000);

    let receiver = LoggingReceiver { receiver };
    let sender = LoggingSender { sender };

    use tracing_subscriber::prelude::*;
    let r = tracing_subscriber::registry();

    let r = r.with(
        tracing_subscriber::fmt::layer()
            .with_ansi(false)
            .event_format(SimpleFormatter)
            .with_writer(sender)
            // TODO: derive this from log_min_messages? Currently the code in
            // communicator_process.c forces log_min_messages='INFO'.
            .with_filter(LevelFilter::from_level(Level::INFO)),
    );
    r.init();

    info!("communicator process logging started");

    Box::new(receiver)
}

/// Read one message from the logging queue. This is essentially a wrapper to Receiver,
/// with a C-friendly signature.
///
/// The message is copied into *errbuf, which is a caller-supplied buffer of size
/// `errbuf_len`.  If the message doesn't fit in the buffer, it is truncated. It is always
/// NULL-terminated.
///
/// The error level is returned *elevel_p. It's one of the PostgreSQL error levels, see
/// elog.h
///
/// If there was a message, *dropped_event_count_p is also updated with a counter of how
/// many log messages in total has been dropped. By comparing that with the value from
/// previous call, you can tell how many were dropped since last call.
///
/// Returns:
///
///   0 if there were no messages
///   1 if there was a message. The message and its level are returned in
///     *errbuf and *elevel_p. *dropped_event_count_p is also updated.
///  -1 on error, i.e the other end of the queue was disconnected
#[unsafe(no_mangle)]
pub extern "C" fn communicator_worker_poll_logging(
    state: &mut LoggingReceiver,
    errbuf: *mut c_char,
    errbuf_len: u32,
    elevel_p: &mut i32,
    dropped_event_count_p: &mut u64,
) -> i32 {
    let msg = match state.receiver.try_recv() {
        Err(TryRecvError::Empty) => return 0,
        Err(TryRecvError::Disconnected) => return -1,
        Ok(msg) => msg,
    };

    let src: &[u8] = &msg.message;
    let dst: *mut u8 = errbuf.cast();
    let len = std::cmp::min(src.len(), errbuf_len as usize - 1);
    unsafe {
        std::ptr::copy_nonoverlapping(src.as_ptr(), dst, len);
        *(dst.add(len)) = b'\0'; // NULL terminator
    }

    // Map the tracing Level to PostgreSQL elevel.
    //
    // XXX: These levels are copied from PostgreSQL's elog.h. Introduce another enum to
    // hide these?
    *elevel_p = match msg.level {
        Level::TRACE => 10, // DEBUG5
        Level::DEBUG => 14, // DEBUG1
        Level::INFO => 17,  // INFO
        Level::WARN => 19,  // WARNING
        Level::ERROR => 21, // ERROR
    };

    *dropped_event_count_p = DROPPED_EVENT_COUNT.load(Ordering::Relaxed);

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

    sender: &'a LoggingSender,
}

impl std::io::Write for EventBuilder<'_> {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.event.message.write(buf)
    }
    fn flush(&mut self) -> std::io::Result<()> {
        self.sender.send_event(self.event.clone());
        Ok(())
    }
}

impl Drop for EventBuilder<'_> {
    fn drop(&mut self) {
        let sender = self.sender;
        let event = std::mem::take(&mut self.event);

        sender.send_event(event);
    }
}

impl<'a> MakeWriter<'a> for LoggingSender {
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
            sender: self,
        }
    }
}

impl LoggingSender {
    fn send_event(&self, e: FormattedEventWithMeta) {
        match self.sender.try_send(e) {
            Ok(()) => {
                // notify the main thread
                callback_set_my_latch();
            }
            Err(TrySendError::Disconnected(_)) => {}
            Err(TrySendError::Full(_)) => {
                // The queue is full, cannot send any more. To avoid blocking the tokio
                // thread, simply drop the message. Better to lose some logs than get
                // stuck if there's a problem with the logging.
                //
                // Record the fact that was a message was dropped by incrementing the
                // counter.
                DROPPED_EVENT_COUNT.fetch_add(1, Ordering::Relaxed);
            }
        }
    }
}

/// Simple formatter implementation for tracing_subscriber, which prints the log spans and
/// message part like the default formatter, but no timestamp or error level. The error
/// level is captured separately by `FormattedEventWithMeta', and when the error is
/// printed by the main thread, with PostgreSQL ereport(), it gets a timestamp at that
/// point. (The timestamp printed will therefore lag behind the timestamp on the event
/// here, if the main thread doesn't process the log message promptly)
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

                // `FormattedFields` is a formatted representation of the span's fields,
                // which is stored in its extensions by the `fmt` layer's `new_span`
                // method. The fields will have been formatted by the same field formatter
                // that's provided to the event formatter in the `FmtContext`.
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

        Ok(())
    }
}
