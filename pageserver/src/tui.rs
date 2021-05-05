use crate::tui_event::{Event, Events};
use crate::tui_logger::TuiLogger;
use crate::tui_logger::TuiLoggerWidget;

use lazy_static::lazy_static;
use std::sync::Arc;
use std::{error::Error, io};
use termion::{event::Key, input::MouseTerminal, raw::IntoRawMode, screen::AlternateScreen};
use tui::backend::TermionBackend;
use tui::buffer::Buffer;
use tui::layout::{Constraint, Direction, Layout, Rect};
use tui::style::{Color, Modifier, Style};
use tui::text::{Span, Spans, Text};
use tui::widgets::{Block, BorderType, Borders, Paragraph, Widget};
use tui::Terminal;

use slog::Drain;

lazy_static! {
    pub static ref PAGESERVICE_DRAIN: Arc<TuiLogger> = Arc::new(TuiLogger::default());
    pub static ref WALRECEIVER_DRAIN: Arc<TuiLogger> = Arc::new(TuiLogger::default());
    pub static ref WALREDO_DRAIN: Arc<TuiLogger> = Arc::new(TuiLogger::default());
    pub static ref CATCHALL_DRAIN: Arc<TuiLogger> = Arc::new(TuiLogger::default());
}

pub fn init_logging() -> slog_scope::GlobalLoggerGuard {
    let pageservice_drain =
        slog::Filter::new(PAGESERVICE_DRAIN.as_ref(), |record: &slog::Record| {
            if record.level().is_at_least(slog::Level::Debug)
                && record.module().starts_with("pageserver::page_service")
            {
                return true;
            }
            false
        })
        .fuse();

    let walredo_drain = slog::Filter::new(WALREDO_DRAIN.as_ref(), |record: &slog::Record| {
        if record.level().is_at_least(slog::Level::Debug)
            && record.module().starts_with("pageserver::walredo")
        {
            return true;
        }
        false
    })
    .fuse();

    let walreceiver_drain =
        slog::Filter::new(WALRECEIVER_DRAIN.as_ref(), |record: &slog::Record| {
            if record.level().is_at_least(slog::Level::Debug)
                && record.module().starts_with("pageserver::walreceiver")
            {
                return true;
            }
            false
        })
        .fuse();

    let catchall_drain = slog::Filter::new(CATCHALL_DRAIN.as_ref(), |record: &slog::Record| {
        if record.level().is_at_least(slog::Level::Info) {
            return true;
        }
        if record.level().is_at_least(slog::Level::Debug)
            && record.module().starts_with("pageserver")
        {
            return true;
        }
        false
    })
    .fuse();

    let drain = pageservice_drain;
    let drain = slog::Duplicate::new(drain, walreceiver_drain).fuse();
    let drain = slog::Duplicate::new(drain, walredo_drain).fuse();
    let drain = slog::Duplicate::new(drain, catchall_drain).fuse();
    let drain = slog_async::Async::new(drain).chan_size(1000).build().fuse();
    let drain = slog::Filter::new(drain, |record: &slog::Record| {
        if record.level().is_at_least(slog::Level::Info) {
            return true;
        }
        if record.level().is_at_least(slog::Level::Debug)
            && record.module().starts_with("pageserver")
        {
            return true;
        }

        false
    })
    .fuse();
    let logger = slog::Logger::root(drain, slog::o!());
    slog_scope::set_global_logger(logger)
}

pub fn ui_main() -> Result<(), Box<dyn Error>> {
    // Terminal initialization
    let stdout = io::stdout().into_raw_mode()?;
    let stdout = MouseTerminal::from(stdout);
    let stdout = AlternateScreen::from(stdout);
    let backend = TermionBackend::new(stdout);
    let mut terminal = Terminal::new(backend)?;

    // Setup event handlers
    let events = Events::new();

    loop {
        terminal.draw(|f| {
            let size = f.size();

            // +----------------+----------------+
            // |                |                |
            // |  top_top_left  | top_top_right  |
            // |                |                |
            // +----------------+----------------|
            // |                |                |
            // |  top_bot_left  | top_left_right |
            // |                |                |
            // +----------------+----------------+
            // |                                 |
            // |             bottom              |
            // |                                 |
            // +---------------------------------+
            let chunks = Layout::default()
                .direction(Direction::Vertical)
                .constraints([Constraint::Percentage(70), Constraint::Percentage(30)].as_ref())
                .split(size);
            let top_chunk = chunks[0];
            let bottom_chunk = chunks[1];

            let top_chunks = Layout::default()
                .direction(Direction::Horizontal)
                .constraints([Constraint::Percentage(50), Constraint::Percentage(50)].as_ref())
                .split(top_chunk);
            let top_left_chunk = top_chunks[0];
            let top_right_chunk = top_chunks[1];

            let c = Layout::default()
                .direction(Direction::Vertical)
                .constraints([Constraint::Percentage(50), Constraint::Percentage(50)].as_ref())
                .split(top_left_chunk);
            let top_top_left_chunk = c[0];
            let top_bot_left_chunk = c[1];

            let c = Layout::default()
                .direction(Direction::Vertical)
                .constraints([Constraint::Percentage(50), Constraint::Percentage(50)].as_ref())
                .split(top_right_chunk);
            let top_top_right_chunk = c[0];
            let top_bot_right_chunk = c[1];

            f.render_widget(
                LogWidget::new(PAGESERVICE_DRAIN.as_ref(), "Page Service"),
                top_top_left_chunk,
            );

            f.render_widget(
                LogWidget::new(WALREDO_DRAIN.as_ref(), "WAL Redo"),
                top_bot_left_chunk,
            );

            f.render_widget(
                LogWidget::new(WALRECEIVER_DRAIN.as_ref(), "WAL Receiver"),
                top_top_right_chunk,
            );

            f.render_widget(MetricsWidget {}, top_bot_right_chunk);

            f.render_widget(
                LogWidget::new(CATCHALL_DRAIN.as_ref(), "All Log").show_module(true),
                bottom_chunk,
            );
        })?;

        // If ther user presses 'q', quit.
        if let Event::Input(key) = events.next()? {
            match key {
                Key::Char('q') => {
                    break;
                }
                _ => (),
            }
        }
    }

    terminal.show_cursor().unwrap();
    terminal.clear().unwrap();

    Ok(())
}

#[allow(dead_code)]
struct LogWidget<'a> {
    logger: &'a TuiLogger,
    title: &'a str,
    show_module: bool,
}

impl<'a> LogWidget<'a> {
    fn new(logger: &'a TuiLogger, title: &'a str) -> LogWidget<'a> {
        LogWidget {
            logger,
            title,
            show_module: false,
        }
    }

    fn show_module(mut self, b: bool) -> LogWidget<'a> {
        self.show_module = b;
        self
    }
}

impl<'a> Widget for LogWidget<'a> {
    fn render(self, area: Rect, buf: &mut Buffer) {
        let w = TuiLoggerWidget::default(self.logger)
            .block(
                Block::default()
                    .borders(Borders::ALL)
                    .title(self.title)
                    .border_type(BorderType::Rounded),
            )
            .show_module(true)
            .style_error(Style::default().fg(Color::Red))
            .style_warn(Style::default().fg(Color::Yellow))
            .style_info(Style::default().fg(Color::Green));
        w.render(area, buf);
    }
}

// Render a widget to show some metrics
struct MetricsWidget {}

fn _get_metric_u64(title: &str, value: u64) -> Spans {
    Spans::from(vec![
        Span::styled(format!("{:<20}", title), Style::default()),
        Span::raw(": "),
        Span::styled(
            value.to_string(),
            Style::default().add_modifier(Modifier::BOLD),
        ),
    ])
}

// This is not used since LSNs were removed from page cache stats.
// Maybe it will be used in the future?
fn _get_metric_str<'a>(title: &str, value: &'a str) -> Spans<'a> {
    Spans::from(vec![
        Span::styled(format!("{:<20}", title), Style::default()),
        Span::raw(": "),
        Span::styled(value, Style::default().add_modifier(Modifier::BOLD)),
    ])
}

impl tui::widgets::Widget for MetricsWidget {
    fn render(self, area: Rect, buf: &mut Buffer) {
        let block = Block::default()
            .borders(Borders::ALL)
            .title("Page Cache Metrics")
            .border_type(BorderType::Rounded);
        let inner_area = block.inner(area);

        block.render(area, buf);

        #[allow(unused_mut)]
        let mut lines: Vec<Spans> = Vec::new();

        // FIXME
        //let page_cache_stats = crate::page_cache::get_stats();

        // This is not used since LSNs were removed from page cache stats.
        // Maybe it will be used in the future?
        /*
        let lsnrange = format!(
            "{} - {}",
            page_cache_stats.first_valid_lsn, page_cache_stats.last_valid_lsn
        );
        let last_valid_recordlsn_str = page_cache_stats.last_record_lsn.to_string();
        lines.push(get_metric_str("Valid LSN range", &lsnrange));
        lines.push(get_metric_str("Last record LSN", &last_valid_recordlsn_str));
        */
/*
        lines.push(get_metric_u64(
            "# of cache entries",
            page_cache_stats.num_entries,
        ));
        lines.push(get_metric_u64(
            "# of page images",
            page_cache_stats.num_page_images,
        ));
        lines.push(get_metric_u64(
            "# of WAL records",
            page_cache_stats.num_wal_records,
        ));
        lines.push(get_metric_u64(
            "# of GetPage@LSN calls",
            page_cache_stats.num_getpage_requests,
        ));
*/
        let text = Text::from(lines);

        Paragraph::new(text).render(inner_area, buf);
    }
}
