use crate::tui_event::{Event, Events};
use crate::tui_logger::TuiLogger;
use crate::tui_logger::TuiLoggerWidget;

use std::{error::Error, io};
use std::sync::Arc;
use termion::{event::Key, input::MouseTerminal, raw::IntoRawMode, screen::AlternateScreen};
use tui::backend::TermionBackend;
use tui::style::{Color, Style};
use tui::Terminal;
use tui::widgets::{Block, Borders, BorderType};
use tui::layout::{Layout, Direction, Constraint};
use lazy_static::lazy_static;

use slog;
use slog::Drain;

lazy_static! {
    pub static ref PAGESERVICE_DRAIN: Arc<TuiLogger> = Arc::new(TuiLogger::default());
    pub static ref WALRECEIVER_DRAIN: Arc<TuiLogger> = Arc::new(TuiLogger::default());
    pub static ref WALREDO_DRAIN: Arc<TuiLogger> = Arc::new(TuiLogger::default());
    pub static ref CATCHALL_DRAIN: Arc<TuiLogger> = Arc::new(TuiLogger::default());
}

pub fn init_logging() -> slog_scope::GlobalLoggerGuard {

    let pageservice_drain = slog::Filter::new(PAGESERVICE_DRAIN.as_ref(),
        |record: &slog::Record| {
            if record.level().is_at_least(slog::Level::Debug) && record.module().starts_with("pageserver::page_service") {
                return true;
            }
            return false;
        }
    ).fuse();

    let walredo_drain = slog::Filter::new(WALREDO_DRAIN.as_ref(),
        |record: &slog::Record| {
            if record.level().is_at_least(slog::Level::Debug) && record.module().starts_with("pageserver::walredo") {
                return true;
            }
            return false;
        }
    ).fuse();

    let walreceiver_drain = slog::Filter::new(WALRECEIVER_DRAIN.as_ref(),
        |record: &slog::Record| {
            if record.level().is_at_least(slog::Level::Debug) && record.module().starts_with("pageserver::walreceiver") {
                return true;
            }
            return false;
        }
    ).fuse();

    let catchall_drain = slog::Filter::new(CATCHALL_DRAIN.as_ref(),
        |record: &slog::Record| {
            if record.level().is_at_least(slog::Level::Info) {
                return true;
            }
            if record.level().is_at_least(slog::Level::Debug) && record.module().starts_with("pageserver") {
                return true;
            }
            return false;
        }
    ).fuse();

    let drain = pageservice_drain;
    let drain = slog::Duplicate::new(drain, walreceiver_drain).fuse();
    let drain = slog::Duplicate::new(drain, walredo_drain).fuse();
    let drain = slog::Duplicate::new(drain, catchall_drain).fuse();
    let drain = slog_async::Async::new(drain).chan_size(1000).build().fuse();
    let drain = slog::Filter::new(drain,
                                  |record: &slog::Record| {

                                      if record.level().is_at_least(slog::Level::Info) {
                                          return true;
                                      }
                                      if record.level().is_at_least(slog::Level::Debug) && record.module().starts_with("pageserver") {
                                          return true;
                                      }

                                      return false;
                                  }
    ).fuse();
    let logger = slog::Logger::root(drain, slog::o!());
    return slog_scope::set_global_logger(logger);
}

pub fn ui_main<'b>() -> Result<(), Box<dyn Error>> {
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

            // +---------------+---------------+
            // |               |               |
            // | top_top_left  |               |
            // |               |               |
            // +---------------+   top_right   |
            // |               |               |
            // | top_bot_left  |               |
            // |               |               |
            // +---------------+---------------+
            // |                               |
            // |            bottom             |
            // |                               |
            // +---------------+---------------+
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

            let w = TuiLoggerWidget::default(PAGESERVICE_DRAIN.as_ref())
                .block(Block::default()
                       .borders(Borders::ALL)
                       .title("Page Service")
                       .border_type(BorderType::Rounded))
                .show_module(false)
                .style_error(Style::default().fg(Color::Red))
                .style_warn(Style::default().fg(Color::Yellow))
                .style_info(Style::default().fg(Color::Green));
            f.render_widget(w, top_top_left_chunk);

            let w = TuiLoggerWidget::default(WALREDO_DRAIN.as_ref())
                .block(Block::default()
                       .borders(Borders::ALL)
                       .title("WAL Redo")
                       .border_type(BorderType::Rounded))
                .show_module(false)
                .style_error(Style::default().fg(Color::Red))
                .style_warn(Style::default().fg(Color::Yellow))
                .style_info(Style::default().fg(Color::Green));
            f.render_widget(w, top_bot_left_chunk);

            let w = TuiLoggerWidget::default(WALRECEIVER_DRAIN.as_ref())
                .block(Block::default()
                       .borders(Borders::ALL)
                       .title("WAL Receiver")
                       .border_type(BorderType::Rounded))
                .show_module(false)
                .style_error(Style::default().fg(Color::Red))
                .style_warn(Style::default().fg(Color::Yellow))
                .style_info(Style::default().fg(Color::Green));
            f.render_widget(w, top_right_chunk);

            let w = TuiLoggerWidget::default(CATCHALL_DRAIN.as_ref())
                .block(Block::default()
                .borders(Borders::ALL)
                .title("Other log")
                .border_type(BorderType::Rounded))
                .show_module(true)
                .style_error(Style::default().fg(Color::Red))
                .style_warn(Style::default().fg(Color::Yellow))
                .style_info(Style::default().fg(Color::Green));
            f.render_widget(w, bottom_chunk);

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
