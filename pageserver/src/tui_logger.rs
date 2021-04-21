//
// A TUI Widget that displays log entries
//
// This is heavily inspired by gin66's tui_logger crate at https://github.com/gin66/tui-logger,
// but I wrote this based on the 'slog' module, which simplified things a lot. tui-logger also
// implemented the slog Drain trait, but it had a model of one global buffer for the records.
// With this implementation, each TuiLogger is a separate ring buffer and separate slog Drain.
// Also, I didn't do any of the "hot log" stuff that gin66's implementation had, you can use an
// AsyncDrain to buffer and handle overflow if desired.
//
use chrono::offset::Local;
use chrono::DateTime;
use slog::{Drain, Level, OwnedKVList, Record};
use slog_async::AsyncRecord;
use std::collections::VecDeque;
use std::sync::Mutex;
use std::time::SystemTime;
use tui::buffer::Buffer;
use tui::layout::Rect;
use tui::style::{Modifier, Style};
use tui::text::{Span, Spans};
use tui::widgets::{Block, Paragraph, Widget, Wrap};

// Size of the log ring buffer, in # of records
static BUFFER_SIZE: usize = 1000;

pub struct TuiLogger {
    events: Mutex<VecDeque<(SystemTime, AsyncRecord)>>,
}

impl<'a> Default for TuiLogger {
    fn default() -> TuiLogger {
        TuiLogger {
            events: Mutex::new(VecDeque::with_capacity(BUFFER_SIZE)),
        }
    }
}

impl Drain for TuiLogger {
    type Ok = ();
    type Err = slog::Error;

    fn log(&self, record: &Record, values: &OwnedKVList) -> Result<Self::Ok, Self::Err> {
        let mut events = self.events.lock().unwrap();

        let now = SystemTime::now();
        let asyncrec = AsyncRecord::from(record, values);
        events.push_front((now, asyncrec));

        if events.len() > BUFFER_SIZE {
            events.pop_back();
        }

        return Ok(());
    }
}

// TuiLoggerWidget renders a TuiLogger ring buffer
pub struct TuiLoggerWidget<'b> {
    block: Option<Block<'b>>,
    /// Base style of the widget
    style: Style,
    /// Level based style
    style_error: Option<Style>,
    style_warn: Option<Style>,
    style_debug: Option<Style>,
    style_trace: Option<Style>,
    style_info: Option<Style>,
    show_module: bool,
    logger: &'b TuiLogger,
}
impl<'b> TuiLoggerWidget<'b> {
    pub fn default(logger: &'b TuiLogger) -> TuiLoggerWidget<'b> {
        TuiLoggerWidget {
            block: None,
            style: Default::default(),
            style_error: None,
            style_warn: None,
            style_debug: None,
            style_trace: None,
            style_info: None,
            show_module: true,
            logger,
        }
    }
}
impl<'b> TuiLoggerWidget<'b> {
    pub fn block(mut self, block: Block<'b>) -> TuiLoggerWidget<'b> {
        self.block = Some(block);
        self
    }
    #[allow(unused)]
    pub fn style(mut self, style: Style) -> TuiLoggerWidget<'b> {
        self.style = style;
        self
    }
    pub fn style_error(mut self, style: Style) -> TuiLoggerWidget<'b> {
        self.style_error = Some(style);
        self
    }
    pub fn style_warn(mut self, style: Style) -> TuiLoggerWidget<'b> {
        self.style_warn = Some(style);
        self
    }
    pub fn style_info(mut self, style: Style) -> TuiLoggerWidget<'b> {
        self.style_info = Some(style);
        self
    }
    #[allow(unused)]
    pub fn style_trace(mut self, style: Style) -> TuiLoggerWidget<'b> {
        self.style_trace = Some(style);
        self
    }
    #[allow(unused)]
    pub fn style_debug(mut self, style: Style) -> TuiLoggerWidget<'b> {
        self.style_debug = Some(style);
        self
    }

    pub fn show_module(mut self, b: bool) -> TuiLoggerWidget<'b> {
        self.show_module = b;
        self
    }
}
impl<'b> Widget for TuiLoggerWidget<'b> {
    fn render(mut self, area: Rect, buf: &mut Buffer) {
        buf.set_style(area, self.style);
        let list_area = match self.block.take() {
            Some(b) => {
                let inner_area = b.inner(area);
                b.render(area, buf);
                inner_area
            }
            None => area,
        };
        if list_area.width == 0 || list_area.height == 0 {
            return;
        }

        let la_height = list_area.height as usize;

        //
        // Iterate through the records in the buffer. The records are
        // pushed to the front, so the newest records come first.
        //
        let mut lines: Vec<Spans> = Vec::new();

        let style_msg = Style::default().add_modifier(Modifier::BOLD);
        {
            let events = self.logger.events.lock().unwrap();

            for evt in events.iter() {
                let (timestamp, rec) = evt;

                rec.as_record_values(|rec, _kwlist| {
                    let mut line: Vec<Span> = Vec::new();

                    let datetime: DateTime<Local> = timestamp.clone().into();
                    let ts = format!("{}", datetime.format("%H:%M:%S%.3f "));
                    line.push(Span::raw(ts));

                    let (lvl_style, txt, with_loc) = match rec.level() {
                        Level::Critical => (self.style_error, "CRIT ", true),
                        Level::Error => (self.style_error, "ERROR", true),
                        Level::Warning => (self.style_warn, "WARN ", true),
                        Level::Info => (self.style_info, "INFO ", false),
                        Level::Debug => (self.style_debug, "DEBUG", true),
                        Level::Trace => (self.style_trace, "TRACE", true),
                    };
                    line.push(Span::styled(txt, lvl_style.unwrap_or_default()));

                    if self.show_module {
                        line.push(Span::raw(" "));
                        line.push(Span::raw(rec.module()));
                    }
                    if with_loc {
                        let loc = format!(" {}:{}", rec.file(), rec.line());
                        line.push(Span::raw(loc));
                    }
                    let msg = format!(" {}", rec.msg());
                    line.push(Span::styled(msg, style_msg));

                    lines.push(Spans::from(line));
                });
                if lines.len() == la_height {
                    break;
                }
            }
        }

        lines.reverse();

        let text = tui::text::Text::from(lines);

        Paragraph::new(text)
            .wrap(Wrap { trim: true })
            .render(list_area, buf);
    }
}
