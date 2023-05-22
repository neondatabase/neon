use std::future::Future;
use std::sync::Arc;

/// Container using which many request handlers can come together and join a single task to
/// completion instead of racing each other and their own cancellation.
///
/// In a picture:
///
/// ```text
/// SharedRetryable::try_restart         Spawned task completes with only one concurrent attempt
///                             \       /
///      request handler 1 ---->|--X
///      request handler 2 ---->|-------|
///      request handler 3 ---->|-------|
///                             |       |
///                             v       |
///       one spawned task      \------>/
///
/// (X = cancelled during await)
/// ```
///
/// Implementation is cancel safe. Implementation and internal structure are hurt by the inability
/// to just spawn the task, but this is needed for `pageserver` usage. Within `pageserver`, the
/// `task_mgr` must be used to spawn the future because it will cause awaiting during shutdown.
///
/// Implementation exposes a fully decomposed [`SharedRetryable::try_restart`] which requires the
/// caller to do the spawning before awaiting for the result. If the caller is dropped while this
/// happens, a new attempt will be required, and all concurrent awaiters will see a
/// [`RetriedTaskPanicked`] error.
///
/// There is another "family of APIs" [`SharedRetryable::attempt_spawn`] for infallible futures. It is
/// just provided for completeness, and it does not have a fully decomposed version like
/// `try_restart`.
///
/// For `try_restart_*` family of APIs, there is a concept of two leveled results. The inner level
/// is returned by the executed future. It needs to be `Clone`. Most errors are not `Clone`, so
/// implementation advice is to log the happened error, and not propagate more than a label as the
/// "inner error" which will be used to build an outer error. The outer error will also have to be
/// convertable from [`RetriedTaskPanicked`] to absorb that case as well.
///
/// ## Example
///
/// A shared service value completes the infallible work once, even if called concurrently by
/// multiple cancellable tasks.
///
/// Example moved as a test `service_example`.
#[derive(Clone)]
pub struct SharedRetryable<V> {
    inner: Arc<tokio::sync::Mutex<Option<MaybeDone<V>>>>,
}

impl<V> Default for SharedRetryable<V> {
    fn default() -> Self {
        Self {
            inner: Arc::new(tokio::sync::Mutex::new(None)),
        }
    }
}

/// Determine if an error is transient or permanent.
pub trait Retryable {
    fn is_permanent(&self) -> bool {
        true
    }
}

pub trait MakeFuture {
    type Future: Future<Output = Self::Output> + Send + 'static;
    type Output: Send + 'static;

    fn make_future(self) -> Self::Future;
}

impl<Fun, Fut, R> MakeFuture for Fun
where
    Fun: FnOnce() -> Fut,
    Fut: Future<Output = R> + Send + 'static,
    R: Send + 'static,
{
    type Future = Fut;
    type Output = R;

    fn make_future(self) -> Self::Future {
        self()
    }
}

/// Retried task panicked, was cancelled, or never spawned (see [`SharedRetryable::try_restart`]).
#[derive(Debug, PartialEq, Eq)]
pub struct RetriedTaskPanicked;

impl<T, E1> SharedRetryable<Result<T, E1>>
where
    T: Clone + std::fmt::Debug + Send + 'static,
    E1: Retryable + Clone + std::fmt::Debug + Send + 'static,
{
    /// Restart a previously failed operation unless it already completed with a terminal result.
    ///
    /// Many futures can call this function and and get the terminal result from an earlier attempt
    /// or start a new attempt, or join an existing one.
    ///
    /// Compared to `Self::try_restart`, this method also spawns the future to run, which would
    /// otherwise have to be done manually.
    #[cfg(test)]
    pub async fn try_restart_spawn<E2>(
        &self,
        retry_with: impl MakeFuture<Output = Result<T, E1>>,
    ) -> Result<T, E2>
    where
        E2: From<E1> + From<RetriedTaskPanicked> + Send + 'static,
    {
        let (recv, maybe_fut) = self.try_restart(retry_with).await;

        if let Some(fut) = maybe_fut {
            // top level function, we must spawn, pageserver cannot use this
            tokio::spawn(fut);
        }

        recv.await
    }

    /// Restart a previously failed operation unless it already completed with a terminal result.
    ///
    /// Many futures can call this function and get the terminal result from an earlier attempt or
    /// start a new attempt, or join an existing one.
    ///
    /// If a task calling this method is cancelled before spawning the returned future, this
    /// attempt is immediatedly deemed as having panicked will happen, but without a panic ever
    /// happening.
    ///
    /// Returns one future for waiting for the result and possibly another which needs to be
    /// spawned when `Some`. Spawning has to happen before waiting is started, otherwise the first
    /// future will never make progress.
    ///
    /// This complication exists because on `pageserver` we cannot use `tokio::spawn` directly
    /// at this time.
    pub async fn try_restart<E2>(
        &self,
        retry_with: impl MakeFuture<Output = Result<T, E1>>,
    ) -> (
        impl Future<Output = Result<T, E2>> + Send + 'static,
        Option<impl Future<Output = ()> + Send + 'static>,
    )
    where
        E2: From<E1> + From<RetriedTaskPanicked> + Send + 'static,
    {
        use futures::future::Either;

        match self.decide_to_retry_or_join(retry_with).await {
            Ok(terminal) => (Either::Left(async move { terminal }), None),
            Err((rx, maybe_fut)) => {
                let recv = Self::make_oneshot_alike_receiver(rx);

                (Either::Right(recv), maybe_fut)
            }
        }
    }

    /// Returns a Ok if the previous attempt had resulted in a terminal result. Err is returned
    /// when an attempt can be joined and possibly needs to be spawned.
    async fn decide_to_retry_or_join<E2>(
        &self,
        retry_with: impl MakeFuture<Output = Result<T, E1>>,
    ) -> Result<
        Result<T, E2>,
        (
            tokio::sync::broadcast::Receiver<Result<T, E1>>,
            Option<impl Future<Output = ()> + Send + 'static>,
        ),
    >
    where
        E2: From<E1> + From<RetriedTaskPanicked>,
    {
        let mut g = self.inner.lock().await;

        let maybe_rx = match g.as_ref() {
            Some(MaybeDone::Done(Ok(t))) => return Ok(Ok(t.to_owned())),
            Some(MaybeDone::Done(Err(e))) if e.is_permanent() => {
                return Ok(Err(E2::from(e.to_owned())))
            }
            Some(MaybeDone::Pending(weak)) => {
                // failure to upgrade can mean only one thing: there was an unexpected
                // panic which we consider as a transient retryable error.
                weak.upgrade()
            }
            Some(MaybeDone::Done(Err(_retryable))) => None,
            None => None,
        };

        let (strong, maybe_fut) = match maybe_rx {
            Some(strong) => (strong, None),
            None => {
                // new attempt
                // panic safety: invoke the factory before configuring the pending value
                let fut = retry_with.make_future();

                let (strong, fut) = self.make_run_and_complete(fut, &mut g);
                (strong, Some(fut))
            }
        };

        // important: the Arc<Receiver> is not held after unlocking
        // important: we resubscribe before lock is released to be sure to get a message which
        // is sent once receiver is dropped
        let rx = strong.resubscribe();
        drop(strong);
        Err((rx, maybe_fut))
    }

    /// Configure a new attempt, but leave spawning it to the caller.
    ///
    /// Returns an `Arc<Receiver<V>>` which is valid until the attempt completes, and the future
    /// which will need to run to completion outside the lifecycle of the caller.
    fn make_run_and_complete(
        &self,
        fut: impl Future<Output = Result<T, E1>> + Send + 'static,
        g: &mut tokio::sync::MutexGuard<'_, Option<MaybeDone<Result<T, E1>>>>,
    ) -> (
        Arc<tokio::sync::broadcast::Receiver<Result<T, E1>>>,
        impl Future<Output = ()> + Send + 'static,
    ) {
        #[cfg(debug_assertions)]
        match &**g {
            Some(MaybeDone::Pending(weak)) => {
                assert!(
                    weak.upgrade().is_none(),
                    "when starting a restart, should no longer have an upgradeable channel"
                );
            }
            Some(MaybeDone::Done(Err(err))) => {
                assert!(
                    !err.is_permanent(),
                    "when restarting, the err must be transient"
                );
            }
            Some(MaybeDone::Done(Ok(_))) => {
                panic!("unexpected restart after a completion on MaybeDone");
            }
            None => {}
        }

        self.make_run_and_complete_any(fut, g)
    }

    /// Oneshot alike as in it's a future which will be consumed by an `await`.
    ///
    /// Otherwise the caller might think it's beneficial or reasonable to poll the channel multiple
    /// times.
    async fn make_oneshot_alike_receiver<E2>(
        mut rx: tokio::sync::broadcast::Receiver<Result<T, E1>>,
    ) -> Result<T, E2>
    where
        E2: From<E1> + From<RetriedTaskPanicked>,
    {
        use tokio::sync::broadcast::error::RecvError;

        match rx.recv().await {
            Ok(Ok(t)) => Ok(t),
            Ok(Err(e)) => Err(E2::from(e)),
            Err(RecvError::Closed | RecvError::Lagged(_)) => {
                // lagged doesn't mean anything with 1 send, but whatever, handle it the same
                // this case should only ever happen if a panick happened in the `fut`.
                Err(E2::from(RetriedTaskPanicked))
            }
        }
    }
}

impl<V> SharedRetryable<V>
where
    V: std::fmt::Debug + Clone + Send + 'static,
{
    /// Attempt to run once a spawned future to completion.
    ///
    /// Any previous attempt which panicked will be retried, but the `RetriedTaskPanicked` will be
    /// returned when the most recent attempt panicked.
    #[cfg(test)]
    pub async fn attempt_spawn(
        &self,
        attempt_with: impl MakeFuture<Output = V>,
    ) -> Result<V, RetriedTaskPanicked> {
        let (rx, maybe_fut) = {
            let mut g = self.inner.lock().await;

            let maybe_rx = match g.as_ref() {
                Some(MaybeDone::Done(v)) => return Ok(v.to_owned()),
                Some(MaybeDone::Pending(weak)) => {
                    // see comment in try_restart
                    weak.upgrade()
                }
                None => None,
            };

            let (strong, maybe_fut) = match maybe_rx {
                Some(strong) => (strong, None),
                None => {
                    let fut = attempt_with.make_future();

                    let (strong, fut) = self.make_run_and_complete_any(fut, &mut g);
                    (strong, Some(fut))
                }
            };

            // see decide_to_retry_or_join for important notes
            let rx = strong.resubscribe();
            drop(strong);
            (rx, maybe_fut)
        };

        if let Some(fut) = maybe_fut {
            // this is a top level function, need to spawn directly
            // from pageserver one wouldn't use this but more piecewise functions
            tokio::spawn(fut);
        }

        let recv = Self::make_oneshot_alike_receiver_any(rx);

        recv.await
    }

    /// Configure a new attempt, but leave spawning it to the caller.
    ///
    /// Forgetting the returned future is outside of scope of any correctness guarantees; all of
    /// the waiters will then be deadlocked, and the MaybeDone will forever be pending. Dropping
    /// and not running the future will then require a new attempt.
    ///
    /// Also returns an `Arc<Receiver<V>>` which is valid until the attempt completes.
    fn make_run_and_complete_any(
        &self,
        fut: impl Future<Output = V> + Send + 'static,
        g: &mut tokio::sync::MutexGuard<'_, Option<MaybeDone<V>>>,
    ) -> (
        Arc<tokio::sync::broadcast::Receiver<V>>,
        impl Future<Output = ()> + Send + 'static,
    ) {
        let (tx, rx) = tokio::sync::broadcast::channel(1);
        let strong = Arc::new(rx);

        **g = Some(MaybeDone::Pending(Arc::downgrade(&strong)));

        let retry = {
            let strong = strong.clone();
            self.clone().run_and_complete(fut, tx, strong)
        };

        #[cfg(debug_assertions)]
        match &**g {
            Some(MaybeDone::Pending(weak)) => {
                let rx = weak.upgrade().expect("holding the weak and strong locally");
                assert!(Arc::ptr_eq(&strong, &rx));
            }
            _ => unreachable!("MaybeDone::pending must be set after spawn_and_run_complete_any"),
        }

        (strong, retry)
    }

    /// Run the actual attempt, and communicate the response via both:
    /// - setting the `MaybeDone::Done`
    /// - the broadcast channel
    async fn run_and_complete(
        self,
        fut: impl Future<Output = V>,
        tx: tokio::sync::broadcast::Sender<V>,
        strong: Arc<tokio::sync::broadcast::Receiver<V>>,
    ) {
        let res = fut.await;

        {
            let mut g = self.inner.lock().await;
            MaybeDone::complete(&mut *g, &strong, res.clone());

            // make the weak un-upgradeable by dropping the final alive
            // reference to it. it is final Arc because the Arc never escapes
            // the critical section in `decide_to_retry_or_join` or `attempt_spawn`.
            Arc::try_unwrap(strong).expect("expected this to be the only Arc<Receiver<V>>");
        }

        // now no one can get the Pending(weak) value to upgrade and they only see
        // the Done(res).
        //
        // send the result value to listeners, if any
        drop(tx.send(res));
    }

    #[cfg(test)]
    async fn make_oneshot_alike_receiver_any(
        mut rx: tokio::sync::broadcast::Receiver<V>,
    ) -> Result<V, RetriedTaskPanicked> {
        use tokio::sync::broadcast::error::RecvError;

        match rx.recv().await {
            Ok(t) => Ok(t),
            Err(RecvError::Closed | RecvError::Lagged(_)) => {
                // lagged doesn't mean anything with 1 send, but whatever, handle it the same
                // this case should only ever happen if a panick happened in the `fut`.
                Err(RetriedTaskPanicked)
            }
        }
    }
}

/// MaybeDone handles synchronization for multiple requests and the single actual task.
///
/// If request handlers witness `Pending` which they are able to upgrade, they are guaranteed a
/// useful `recv().await`, where useful means "value" or "disconnect" arrives. If upgrade fails,
/// this means that "disconnect" has happened in the past.
///
/// On successful execution the one executing task will set this to `Done` variant, with the actual
/// resulting value.
#[derive(Debug)]
pub enum MaybeDone<V> {
    Pending(std::sync::Weak<tokio::sync::broadcast::Receiver<V>>),
    Done(V),
}

impl<V: std::fmt::Debug> MaybeDone<V> {
    fn complete(
        this: &mut Option<MaybeDone<V>>,
        _strong: &Arc<tokio::sync::broadcast::Receiver<V>>,
        outcome: V,
    ) {
        #[cfg(debug_assertions)]
        match this {
            Some(MaybeDone::Pending(weak)) => {
                let same = weak
                    .upgrade()
                    // we don't yet have Receiver::same_channel
                    .map(|rx| Arc::ptr_eq(_strong, &rx))
                    .unwrap_or(false);
                assert!(same, "different channel had been replaced or dropped");
            }
            other => panic!("unexpected MaybeDone: {other:?}"),
        }

        *this = Some(MaybeDone::Done(outcome));
    }
}

#[cfg(test)]
mod tests {
    use super::{RetriedTaskPanicked, Retryable, SharedRetryable};
    use std::sync::Arc;

    #[derive(Debug)]
    enum OuterError {
        AttemptPanicked,
        Unlucky,
    }

    #[derive(Clone, Debug)]
    enum InnerError {
        Unlucky,
    }

    impl Retryable for InnerError {
        fn is_permanent(&self) -> bool {
            false
        }
    }

    impl From<InnerError> for OuterError {
        fn from(_: InnerError) -> Self {
            OuterError::Unlucky
        }
    }

    impl From<RetriedTaskPanicked> for OuterError {
        fn from(_: RetriedTaskPanicked) -> Self {
            OuterError::AttemptPanicked
        }
    }

    #[tokio::test]
    async fn restartable_until_permanent() {
        let shr = SharedRetryable::<Result<u8, InnerError>>::default();

        let res = shr
            .try_restart_spawn(|| async move { panic!("really unlucky") })
            .await;

        assert!(matches!(res, Err(OuterError::AttemptPanicked)));

        let res = shr
            .try_restart_spawn(|| async move { Err(InnerError::Unlucky) })
            .await;

        assert!(matches!(res, Err(OuterError::Unlucky)));

        let res = shr.try_restart_spawn(|| async move { Ok(42) }).await;

        assert!(matches!(res, Ok::<u8, OuterError>(42)));

        let res = shr
            .try_restart_spawn(|| async move { panic!("rerun should clone Ok(42)") })
            .await;

        assert!(matches!(res, Ok::<u8, OuterError>(42)));
    }

    /// Demonstration of the SharedRetryable::attempt
    #[tokio::test]
    async fn attemptable_until_no_panic() {
        let shr = SharedRetryable::<u8>::default();

        let res = shr
            .attempt_spawn(|| async move { panic!("should not interfere") })
            .await;

        assert!(matches!(res, Err(RetriedTaskPanicked)), "{res:?}");

        let res = shr.attempt_spawn(|| async move { 42 }).await;

        assert_eq!(res, Ok(42));

        let res = shr
            .attempt_spawn(|| async move { panic!("should not be called") })
            .await;

        assert_eq!(res, Ok(42));
    }

    #[tokio::test]
    async fn cancelling_spawner_is_fine() {
        let shr = SharedRetryable::<Result<u8, InnerError>>::default();

        let (recv1, maybe_fut) = shr
            .try_restart(|| async move { panic!("should not have been called") })
            .await;
        let should_be_spawned = maybe_fut.unwrap();

        let (recv2, maybe_fut) = shr
            .try_restart(|| async move {
                panic!("should never be called because waiting on should_be_spawned")
            })
            .await;
        assert!(
            matches!(maybe_fut, None),
            "only the first one should had created the future"
        );

        let mut recv1 = std::pin::pin!(recv1);
        let mut recv2 = std::pin::pin!(recv2);

        tokio::select! {
            _ = tokio::time::sleep(std::time::Duration::from_millis(100)) => {},
            _ = &mut recv1 => unreachable!("should not have completed because should_be_spawned not spawned"),
            _ = &mut recv2 => unreachable!("should not have completed because should_be_spawned not spawned"),
        }

        drop(should_be_spawned);

        let res = recv1.await;
        assert!(matches!(res, Err(OuterError::AttemptPanicked)), "{res:?}");

        let res = recv2.await;
        assert!(matches!(res, Err(OuterError::AttemptPanicked)), "{res:?}");

        // but we can still reach a terminal state if the api is not misused or the
        // should_be_spawned winner is not cancelled

        let recv1 = shr.try_restart_spawn::<OuterError>(|| async move { Ok(42) });
        let recv2 = shr.try_restart_spawn::<OuterError>(|| async move { Ok(43) });

        assert_eq!(recv1.await.unwrap(), 42);
        assert_eq!(recv2.await.unwrap(), 42, "43 should never be returned");
    }

    #[tokio::test]
    async fn service_example() {
        #[derive(Debug, Clone, Copy)]
        enum OneLevelError {
            TaskPanicked,
        }

        impl Retryable for OneLevelError {
            fn is_permanent(&self) -> bool {
                // for a single level errors, this wording is weird
                !matches!(self, OneLevelError::TaskPanicked)
            }
        }

        impl From<RetriedTaskPanicked> for OneLevelError {
            fn from(_: RetriedTaskPanicked) -> Self {
                OneLevelError::TaskPanicked
            }
        }

        #[derive(Clone, Default)]
        struct Service(SharedRetryable<Result<u8, OneLevelError>>);

        impl Service {
            async fn work(
                &self,
                completions: Arc<std::sync::atomic::AtomicUsize>,
            ) -> Result<u8, OneLevelError> {
                self.0
                    .try_restart_spawn(|| async move {
                        // give time to cancel some of the tasks
                        tokio::time::sleep(std::time::Duration::from_secs(1)).await;
                        completions.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                        Self::work_once().await
                    })
                    .await
            }

            async fn work_once() -> Result<u8, OneLevelError> {
                Ok(42)
            }
        }

        let svc = Service::default();

        let mut js = tokio::task::JoinSet::new();

        let barrier = Arc::new(tokio::sync::Barrier::new(10 + 1));
        let completions = Arc::new(std::sync::atomic::AtomicUsize::new(0));

        let handles = (0..10)
            .map(|_| {
                js.spawn({
                    let svc = svc.clone();
                    let barrier = barrier.clone();
                    let completions = completions.clone();
                    async move {
                        // make sure all tasks are ready to start at the same time
                        barrier.wait().await;
                        // after successfully starting the work, any of the futures could get cancelled
                        svc.work(completions).await
                    }
                })
            })
            .collect::<Vec<_>>();

        barrier.wait().await;

        tokio::time::sleep(std::time::Duration::from_millis(100)).await;

        handles[5].abort();

        let mut cancellations = 0;

        while let Some(res) = js.join_next().await {
            // all complete with the same result
            match res {
                Ok(res) => assert_eq!(res.unwrap(), 42),
                Err(je) => {
                    // except for the one task we cancelled; it's cancelling
                    // does not interfere with the result
                    assert!(je.is_cancelled());
                    cancellations += 1;
                    assert_eq!(cancellations, 1, "only 6th task was aborted");
                    // however we cannot assert that everytime we get to cancel the 6th task
                }
            }
        }

        // there will be at most one terminal completion
        assert_eq!(completions.load(std::sync::atomic::Ordering::Relaxed), 1);
    }
}
