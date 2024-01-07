use futures::Future;
use pq_proto::{framed::Framed, BeMessage};
use tokio::{io::AsyncWrite, task::JoinHandle};
use tracing::{info, warn, Instrument};

pub trait Captures<T> {}
impl<T, U> Captures<T> for U {}

#[must_use]
pub enum StageError {
    Flush(Framed<Box<dyn AsyncWrite + Unpin + Send + 'static>>),
    Done,
}

impl StageError {
    pub async fn finish(self) {
        match self {
            StageError::Flush(mut f) => {
                // ignore result. we can't do anything about it.
                // this is already the error case anyway...
                if let Err(e) = f.flush().await {
                    warn!("could not send message to user: {e:?}")
                }
            }
            StageError::Done => {}
        }
        info!("task finished");
    }
}

pub type DynStage = Box<dyn StageSpawn>;

/// Stage represents a single stage in a state machine.
pub trait Stage: 'static + Send {
    /// The span this stage should be run inside.
    fn span(&self) -> tracing::Span;
    /// Run the current stage, returning a new [`DynStage`], or an error
    ///
    /// Can be implemented as `async fn run(self) -> Result<DynStage, StageError>`
    fn run(self) -> impl 'static + Send + Future<Output = Result<DynStage, StageError>>;
}

pub enum StageResult {
    Finished,
    Run(JoinHandle<Result<DynStage, StageError>>),
}

pub trait StageSpawn: 'static + Send {
    fn run(self: Box<Self>) -> StageResult;
}

/// Stage spawn is a helper trait for the state machine. It spawns the stages as a tokio task
impl<S: Stage> StageSpawn for S {
    fn run(self: Box<Self>) -> StageResult {
        let span = self.span();
        StageResult::Run(tokio::spawn(S::run(*self).instrument(span)))
    }
}

pub struct Finished;

impl StageSpawn for Finished {
    fn run(self: Box<Self>) -> StageResult {
        StageResult::Finished
    }
}

use crate::{
    context::RequestMonitoring,
    error::{ErrorKind, UserFacingError},
    stream::PqStream,
};

pub trait ResultExt<T, E> {
    fn send_error_to_user<S>(
        self,
        ctx: &mut RequestMonitoring,
        stream: PqStream<S>,
    ) -> Result<(T, PqStream<S>), StageError>
    where
        S: AsyncWrite + Unpin + Send + 'static,
        E: UserFacingError;

    fn no_user_error(self, ctx: &mut RequestMonitoring, kind: ErrorKind) -> Result<T, StageError>
    where
        E: std::fmt::Display;
}

impl<T, E> ResultExt<T, E> for Result<T, E> {
    fn send_error_to_user<S>(
        self,
        ctx: &mut RequestMonitoring,
        stream: PqStream<S>,
    ) -> Result<(T, PqStream<S>), StageError>
    where
        S: AsyncWrite + Unpin + Send + 'static,
        E: UserFacingError,
    {
        match self {
            Ok(t) => Ok((t, stream)),
            Err(e) => Err(user_facing_error(e, ctx, stream)),
        }
    }

    fn no_user_error(self, ctx: &mut RequestMonitoring, kind: ErrorKind) -> Result<T, StageError>
    where
        E: std::fmt::Display,
    {
        match self {
            Ok(t) => Ok(t),
            Err(e) => {
                tracing::error!(
                    kind = kind.to_str(),
                    user_msg = "",
                    "task finished with error: {e}"
                );

                ctx.error(kind);
                ctx.log();
                Err(StageError::Done)
            }
        }
    }
}

pub fn user_facing_error<S, E>(
    err: E,
    ctx: &mut RequestMonitoring,
    mut stream: PqStream<S>,
) -> StageError
where
    S: AsyncWrite + Unpin + Send + 'static,
    E: UserFacingError,
{
    let kind = err.get_error_type();
    ctx.error(kind);
    ctx.log();

    let msg = err.to_string_client();
    tracing::error!(
        kind = kind.to_str(),
        user_msg = msg,
        "task finished with error: {err}"
    );
    if let Err(err) = stream.write_message_noflush(&BeMessage::ErrorResponse(&msg, None)) {
        warn!("could not process error message: {err:?}")
    }
    StageError::Flush(stream.framed.map_stream_sync(|f| Box::new(f) as Box<_>))
}
