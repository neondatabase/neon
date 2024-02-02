use signal_hook::iterator::Signals;

pub use signal_hook::consts::{signal::*, TERM_SIGNALS};

pub enum Signal {
    Quit,
    Interrupt,
    Terminate,
}

impl Signal {
    pub fn name(&self) -> &'static str {
        match self {
            Signal::Quit => "SIGQUIT",
            Signal::Interrupt => "SIGINT",
            Signal::Terminate => "SIGTERM",
        }
    }
}

pub struct ShutdownSignals;

impl ShutdownSignals {
    pub fn handle(mut handler: impl FnMut(Signal) -> anyhow::Result<()>) -> anyhow::Result<()> {
        for raw_signal in Signals::new(TERM_SIGNALS)?.into_iter() {
            let signal = match raw_signal {
                SIGINT => Signal::Interrupt,
                SIGTERM => Signal::Terminate,
                SIGQUIT => Signal::Quit,
                other => panic!("unknown signal: {}", other),
            };

            handler(signal)?;
        }

        Ok(())
    }
}
