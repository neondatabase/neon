use std::io::Write;

use anyhow::Result;
use chrono::Utc;
use env_logger::{Builder, Env};

macro_rules! info_println {
    ($($tts:tt)*) => {
        if log_enabled!(Level::Info) {
            println!($($tts)*);
        }
    }
}

macro_rules! info_print {
    ($($tts:tt)*) => {
        if log_enabled!(Level::Info) {
            print!($($tts)*);
        }
    }
}

/// Initialize `env_logger` using either `default_level` or
/// `RUST_LOG` environment variable as default log level.
pub fn init_logger(default_level: &str) -> Result<()> {
    let env = Env::default().filter_or("RUST_LOG", default_level);

    Builder::from_env(env)
        .format(|buf, record| {
            let thread_handle = std::thread::current();
            writeln!(
                buf,
                "{} [{}] {}: {}",
                Utc::now().format("%Y-%m-%d %H:%M:%S%.3f %Z"),
                thread_handle.name().unwrap_or("main"),
                record.level(),
                record.args()
            )
        })
        .init();

    Ok(())
}
