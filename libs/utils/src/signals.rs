use signal_hook::flag;
use signal_hook::iterator::Signals;
use std::sync::atomic::AtomicBool;
use std::sync::Arc;

pub use signal_hook::consts::{signal::*, TERM_SIGNALS};

pub fn install_shutdown_handlers() -> anyhow::Result<ShutdownSignals> {
    let term_now = Arc::new(AtomicBool::new(false));
    for sig in TERM_SIGNALS {
        // When terminated by a second term signal, exit with exit code 1.
        // This will do nothing the first time (because term_now is false).
        flag::register_conditional_shutdown(*sig, 1, Arc::clone(&term_now))?;
        // But this will "arm" the above for the second time, by setting it to true.
        // The order of registering these is important, if you put this one first, it will
        // first arm and then terminate ‒ all in the first round.
        flag::register(*sig, Arc::clone(&term_now))?;
    }

    Ok(ShutdownSignals)
}

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
    pub fn handle(
        self,
        mut handler: impl FnMut(Signal) -> anyhow::Result<()>,
    ) -> anyhow::Result<()> {
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
