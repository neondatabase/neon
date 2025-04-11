//!
//! Glue code to get Rust log stream, created with the `tracing` crate, to the PostgreSQL log
//!
//! In the Rust threads, the log messages are written to a mpsc Channel, and the Postgres
//! process latch is raised. That wakes up the loop in the  main thread. It reads the
//! message from the channel and ereport()s it. This ensures that only one thread, the main
//! thread, calls the PostgreSQL logging routines at any time.
//!

use std::sync::mpsc::sync_channel;
use std::sync::mpsc::{Receiver, SyncSender};
use std::sync::mpsc::{TryRecvError, TrySendError};

use tracing::info;
use tracing::Level;
use tracing::Event;
use tracing::Metadata;
use tracing::Subscriber;
use tracing_subscriber::filter::LevelFilter;
use tracing_subscriber::fmt::FormatEvent;
use tracing_subscriber::fmt::FormatFields;
use tracing_subscriber::fmt::FormattedFields;
use tracing_subscriber::fmt::FmtContext;
use tracing_subscriber::fmt::MakeWriter;
use tracing_subscriber::fmt::format::Writer;
use tracing_subscriber::registry::LookupSpan;


use crate::worker_process::callbacks::callback_set_my_latch;

pub struct LoggingState {
    receiver: Receiver<FormattedEventWithMeta>
}

#[unsafe(no_mangle)]
pub extern "C" fn configure_logging() -> *mut LoggingState {
    let (sender, receiver) = sync_channel(1000);

    let maker = Maker {
        channel: sender
    };

    use tracing_subscriber::prelude::*;
    let r = tracing_subscriber::registry();

    let r = r.with(
        tracing_subscriber::fmt::layer()
            .event_format(MyFormatter::new())
            .with_writer(maker)
            .with_filter(LevelFilter::from_level(Level::TRACE)
            )
    );
    r.init();

    info!("communicator process logging started");

    let state = LoggingState {
        receiver,
    };

    Box::leak(Box::new(state))
}

/// This is called from the main thread of the communicator process. That makes it safe to
/// call ereport()
#[unsafe(no_mangle)]
pub extern "C" fn pump_logging(state: *mut LoggingState, errbuf: *mut u8, errbuf_len: u32, elevel_p: *mut i32) -> i32 {
    let receiver = unsafe { &(*state).receiver };

    let msg = match receiver.try_recv() {
        Err(TryRecvError::Empty) => return 0,
        Err(TryRecvError::Disconnected) => return -1,
        Ok(msg) => msg,
    };

    let src: &[u8] = &msg.message;
    let dst = errbuf;
    let len = std::cmp::min(src.len(), errbuf_len as usize - 1);
    unsafe {
        std::ptr::copy_nonoverlapping(src.as_ptr(), dst, len);
        /* NULL terminator */
        *(errbuf.add(len)) = b'\0';

        // TODO: these levels are copied from PostgreSQL's elog.h
        *elevel_p = match msg.level {
            Level::TRACE => 10,		// DEBUG5
            Level::DEBUG => 14,		// DEBUG1
            Level::INFO => 17,		// INFO
            Level::WARN => 19,		// WARNING
            Level::ERROR => 21,		// ERROR
        }
    }
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

impl<'a> std::io::Write for EventBuilder<'a> {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize>  {
        self.event.message.write(buf)
    }
    fn flush(&mut self) -> std::io::Result<()> {
        self.maker.send_event(self.event.clone());
        Ok(())
    }
}

impl<'a> Drop for EventBuilder<'a> {
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
            },
            Err(TrySendError::Disconnected(_)) => {},
            Err(TrySendError::Full(_)) => {
                // TODO: record that some messages were lost
            }
        }
    }
}

struct MyFormatter {

}

impl<S, N> FormatEvent<S, N> for MyFormatter
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
                    write!(writer, "{{{}}}", fields)?;
                }
                write!(writer, ": ")?;
            }
        }

        // Write fields on the event
        ctx.field_format().format_fields(writer.by_ref(), event)?;

        writeln!(writer)
    }
}

impl MyFormatter {
    fn new() -> Self {
        MyFormatter {}
    }
}
