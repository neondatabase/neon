//!  Protect a piece of state from reuse after it is left in an inconsistent state.
//!
//!  # Example
//!
//!  ```
//!  # tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap().block_on(async {
//!  use utils::poison::Poison;
//!  use std::time::Duration;
//!
//!  struct State {
//!    clean: bool,
//!  }
//!  let state = tokio::sync::Mutex::new(Poison::new("mystate", State { clean: true }));
//!
//!  let mut mutex_guard = state.lock().await;
//!  let mut poison_guard = mutex_guard.check_and_arm()?;
//!  let state = poison_guard.data_mut();
//!  state.clean = false;
//!  // If we get cancelled at this await point, subsequent check_and_arm() calls will fail.
//!  tokio::time::sleep(Duration::from_secs(10)).await;
//!  state.clean = true;
//!  poison_guard.disarm();
//!  # Ok::<(), utils::poison::Error>(())
//!  # });
//!  ```

use tracing::warn;

pub struct Poison<T> {
    what: &'static str,
    state: State,
    data: T,
}

#[derive(Clone, Copy)]
enum State {
    Clean,
    Armed,
    Poisoned { at: chrono::DateTime<chrono::Utc> },
}

impl<T> Poison<T> {
    /// We log `what` `warning!` level if the [`Guard`] gets dropped without being [`Guard::disarm`]ed.
    pub fn new(what: &'static str, data: T) -> Self {
        Self {
            what,
            state: State::Clean,
            data,
        }
    }

    /// Check for poisoning and return a [`Guard`] that provides access to the wrapped state.
    pub fn check_and_arm(&mut self) -> Result<Guard<T>, Error> {
        match self.state {
            State::Clean => {
                self.state = State::Armed;
                Ok(Guard(self))
            }
            State::Armed => unreachable!("transient state"),
            State::Poisoned { at } => Err(Error::Poisoned {
                what: self.what,
                at,
            }),
        }
    }
}

/// Armed pointer to a [`Poison`].
///
/// Use [`Self::data`] and [`Self::data_mut`] to access the wrapped state.
/// Once modifications are done, use [`Self::disarm`].
/// If [`Guard`] gets dropped instead of calling [`Self::disarm`], the state is poisoned
/// and subsequent calls to [`Poison::check_and_arm`] will fail with an error.
pub struct Guard<'a, T>(&'a mut Poison<T>);

impl<T> Guard<'_, T> {
    pub fn data(&self) -> &T {
        &self.0.data
    }
    pub fn data_mut(&mut self) -> &mut T {
        &mut self.0.data
    }

    pub fn disarm(self) {
        match self.0.state {
            State::Clean => unreachable!("we set it to Armed in check_and_arm()"),
            State::Armed => {
                self.0.state = State::Clean;
            }
            State::Poisoned { at } => {
                unreachable!("we fail check_and_arm() if it's in that state: {at}")
            }
        }
    }
}

impl<T> Drop for Guard<'_, T> {
    fn drop(&mut self) {
        match self.0.state {
            State::Clean => {
                // set by disarm()
            }
            State::Armed => {
                // still armed => poison it
                let at = chrono::Utc::now();
                self.0.state = State::Poisoned { at };
                warn!(at=?at, "poisoning {}", self.0.what);
            }
            State::Poisoned { at } => {
                unreachable!("we fail check_and_arm() if it's in that state: {at}")
            }
        }
    }
}

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("poisoned at {at}: {what}")]
    Poisoned {
        what: &'static str,
        at: chrono::DateTime<chrono::Utc>,
    },
}
