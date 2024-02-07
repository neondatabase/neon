use std::sync::{
    atomic::{AtomicBool, AtomicUsize, Ordering},
    Arc,
};
use tokio::sync::Semaphore;

/// Custom design like [`tokio::sync::OnceCell`] but using [`OwnedSemaphorePermit`] instead of
/// `SemaphorePermit`, allowing use of `take` which does not require holding an outer mutex guard
/// for the duration of initialization.
///
/// Has no unsafe, builds upon [`tokio::sync::Semaphore`] and [`std::sync::Mutex`].
///
/// [`OwnedSemaphorePermit`]: tokio::sync::OwnedSemaphorePermit
pub struct OnceCell<T> {
    inner: tokio::sync::RwLock<Inner<T>>,
    initializers: AtomicUsize,
    /// Do we have one permit or `u32::MAX` permits?
    ///
    /// Having one permit means the cell is not initialized, and one winning future could
    /// initialize it. The act of initializing the cell adds `u32::MAX` permits and set this to
    /// `false`.
    ///
    /// Deinitializing an initialized cell will first take `u32::MAX` permits handing one of them
    /// out, then set this back to `true`.
    ///
    /// Because we need to see all changes to this variable, always use Acquire to read, AcqRel to
    /// compare_exchange.
    has_one_permit: AtomicBool,
}

impl<T> Default for OnceCell<T> {
    /// Create new uninitialized [`OnceCell`].
    fn default() -> Self {
        Self {
            inner: Default::default(),
            initializers: AtomicUsize::new(0),
            has_one_permit: AtomicBool::new(true),
        }
    }
}

/// Semaphore is the current state:
/// - open semaphore means the value is `None`, not yet initialized
/// - closed semaphore means the value has been initialized
#[derive(Debug)]
struct Inner<T> {
    init_semaphore: Arc<Semaphore>,
    value: Option<T>,
}

impl<T> Default for Inner<T> {
    fn default() -> Self {
        Self {
            init_semaphore: Arc::new(Semaphore::new(1)),
            value: None,
        }
    }
}

impl<T> OnceCell<T> {
    /// Creates an already initialized `OnceCell` with the given value.
    pub fn new(value: T) -> Self {
        let sem = Semaphore::new(u32::MAX as usize);
        Self {
            inner: tokio::sync::RwLock::new(Inner {
                init_semaphore: Arc::new(sem),
                value: Some(value),
            }),
            initializers: AtomicUsize::new(0),
            has_one_permit: AtomicBool::new(false),
        }
    }

    /// Returns a guard to an existing initialized value, or uniquely initializes the value before
    /// returning the guard.
    ///
    /// Initializing might wait on any existing [`GuardMut::take_and_deinit`] deinitialization.
    ///
    /// Initialization is panic-safe and cancellation-safe.
    #[tracing::instrument(level = tracing::Level::DEBUG, skip_all)]
    pub async fn get_mut_or_init<F, Fut, E>(&self, factory: F) -> Result<GuardMut<'_, T>, E>
    where
        F: FnOnce(InitPermit) -> Fut,
        Fut: std::future::Future<Output = Result<(T, InitPermit), E>>,
    {
        loop {
            let sem = {
                let guard = self.inner.write().await;
                if guard.value.is_some() {
                    tracing::debug!("returning GuardMut over existing value");
                    return Ok(GuardMut(guard));
                }
                guard.init_semaphore.clone()
            };

            {
                let permit = {
                    let _guard = CountWaitingInitializers::start(self);
                    sem.acquire().await
                };

                let permit = permit.expect("semaphore is never closed");

                if !self.has_one_permit.load(Ordering::Acquire) {
                    // it is important that the permit is dropped here otherwise there would be a
                    // deadlock with `take_and_deinit` happening at the same time.
                    tracing::trace!("seems initialization happened already, trying again");
                    continue;
                }

                permit.forget();
            }

            tracing::trace!("calling factory");
            let permit = InitPermit::from(sem);
            let (value, permit) = factory(permit).await?;

            let guard = self.inner.write().await;

            return Ok(self.set0(value, guard, permit));
        }
    }

    /// Returns a guard to an existing initialized value, or uniquely initializes the value before
    /// returning the guard.
    ///
    /// Initialization is panic-safe and cancellation-safe.
    #[tracing::instrument(level = tracing::Level::DEBUG, skip_all)]
    pub async fn get_or_init<F, Fut, E>(&self, factory: F) -> Result<GuardRef<'_, T>, E>
    where
        F: FnOnce(InitPermit) -> Fut,
        Fut: std::future::Future<Output = Result<(T, InitPermit), E>>,
    {
        loop {
            let sem = {
                let guard = self.inner.read().await;
                if guard.value.is_some() {
                    tracing::debug!("returning GuardRef over existing value");
                    return Ok(GuardRef(guard));
                }
                guard.init_semaphore.clone()
            };

            {
                let permit = {
                    // increment the count for the duration of queued
                    let _guard = CountWaitingInitializers::start(self);
                    sem.acquire().await
                };

                let permit = permit.expect("semaphore is never closed");

                if !self.has_one_permit.load(Ordering::Acquire) {
                    tracing::trace!("seems initialization happened already, trying again");
                    continue;
                } else {
                    // it is our turn to initialize for sure
                }

                permit.forget();
            }

            tracing::trace!("calling factory");
            let permit = InitPermit::from(sem);
            let (value, permit) = factory(permit).await?;

            let guard = self.inner.write().await;

            return Ok(self.set0(value, guard, permit).downgrade());
        }
    }

    /// Assuming a permit is held after previous call to [`GuardMut::take_and_deinit`], it can be used
    /// to complete initializing the inner value.
    ///
    /// # Panics
    ///
    /// If the inner has already been initialized.
    #[tracing::instrument(level = tracing::Level::DEBUG, skip_all)]
    pub async fn set(&self, value: T, permit: InitPermit) -> GuardMut<'_, T> {
        let guard = self.inner.write().await;

        assert!(
            self.has_one_permit.load(Ordering::Acquire),
            "cannot set when there are multiple permits"
        );

        // cannot assert that this permit is for self.inner.semaphore, but we can assert it cannot
        // give more permits right now.
        if guard.init_semaphore.try_acquire().is_ok() {
            let available = guard.init_semaphore.available_permits();
            drop(guard);
            panic!("permit is of wrong origin: {available}");
        }

        self.set0(value, guard, permit)
    }

    fn set0<'a>(
        &'a self,
        value: T,
        mut guard: tokio::sync::RwLockWriteGuard<'a, Inner<T>>,
        permit: InitPermit,
    ) -> GuardMut<'a, T> {
        if guard.value.is_some() {
            drop(guard);
            unreachable!("we won permit, must not be initialized");
        }
        guard.value = Some(value);
        assert!(
            self.has_one_permit
                .compare_exchange(true, false, Ordering::AcqRel, Ordering::Relaxed)
                .is_ok(),
            "should had only had one permit"
        );
        permit.forget();
        guard.init_semaphore.add_permits(u32::MAX as usize);

        tracing::debug!("value initialized");
        GuardMut(guard)
    }

    /// Returns a guard to an existing initialized value, if any.
    pub async fn get_mut(&self) -> Option<GuardMut<'_, T>> {
        let guard = self.inner.write().await;
        if guard.value.is_some() {
            Some(GuardMut(guard))
        } else {
            None
        }
    }

    /// Returns a guard to an existing initialized value, if any.
    pub async fn get(&self) -> Option<GuardRef<'_, T>> {
        let guard = self.inner.read().await;
        if guard.value.is_some() {
            Some(GuardRef(guard))
        } else {
            None
        }
    }

    /// Return the number of [`Self::get_or_init`] calls waiting for initialization to complete.
    pub fn initializer_count(&self) -> usize {
        self.initializers.load(Ordering::Relaxed)
    }

    /// Take the current value, and a new permit for it's deinitialization.
    ///
    /// The permit will be on a semaphore part of the new internal value, and any following
    /// [`OnceCell::get_or_init`] will wait on it to complete.
    #[tracing::instrument(level = tracing::Level::DEBUG, skip_all)]
    pub async fn take_and_deinit(&self, mut guard: GuardMut<'_, T>) -> (T, InitPermit) {
        // guard exists => we have been initialized
        assert!(
            !self.has_one_permit.load(Ordering::Acquire),
            "has to have all permits after initializing"
        );
        assert!(guard.0.value.is_some(), "guard exists => initialized");

        // we must first drain out all "waiting to initialize" stragglers
        tracing::trace!("draining other initializers");
        let all_permits = guard
            .0
            .init_semaphore
            .acquire_many(u32::MAX)
            .await
            .expect("never closed");
        all_permits.forget();
        tracing::debug!("other initializers drained");

        assert_eq!(guard.0.init_semaphore.available_permits(), 0);

        // now that the permits have been drained, switch the state
        assert!(
            self.has_one_permit
                .compare_exchange(false, true, Ordering::AcqRel, Ordering::Relaxed)
                .is_ok(),
            "there should be only one GuardMut attempting take_and_deinit"
        );

        let value = guard.0.value.take().unwrap();

        // act of creating an init_permit is the same as "adding back one when this is dropped"
        let init_permit = InitPermit::from(guard.0.init_semaphore.clone());

        (value, init_permit)
    }
}

/// DropGuard counter for queued tasks waiting to initialize, mainly accessible for the
/// initializing task for example at the end of initialization.
struct CountWaitingInitializers<'a, T>(&'a OnceCell<T>);

impl<'a, T> CountWaitingInitializers<'a, T> {
    fn start(target: &'a OnceCell<T>) -> Self {
        target.initializers.fetch_add(1, Ordering::Relaxed);
        CountWaitingInitializers(target)
    }
}

impl<'a, T> Drop for CountWaitingInitializers<'a, T> {
    fn drop(&mut self) {
        self.0.initializers.fetch_sub(1, Ordering::Relaxed);
    }
}

/// Uninteresting guard object to allow short-lived access to inspect or clone the held,
/// initialized value.
#[derive(Debug)]
pub struct GuardMut<'a, T>(tokio::sync::RwLockWriteGuard<'a, Inner<T>>);

impl<T> std::ops::Deref for GuardMut<'_, T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        self.0
            .value
            .as_ref()
            .expect("guard is not created unless value has been initialized")
    }
}

impl<T> std::ops::DerefMut for GuardMut<'_, T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.0
            .value
            .as_mut()
            .expect("guard is not created unless value has been initialized")
    }
}

impl<'a, T> GuardMut<'a, T> {
    pub fn downgrade(self) -> GuardRef<'a, T> {
        GuardRef(self.0.downgrade())
    }
}

#[derive(Debug)]
pub struct GuardRef<'a, T>(tokio::sync::RwLockReadGuard<'a, Inner<T>>);

impl<T> std::ops::Deref for GuardRef<'_, T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        self.0
            .value
            .as_ref()
            .expect("guard is not created unless value has been initialized")
    }
}

/// Type held by OnceCell (de)initializing task.
pub struct InitPermit(Option<Arc<tokio::sync::Semaphore>>);

impl From<Arc<tokio::sync::Semaphore>> for InitPermit {
    fn from(value: Arc<tokio::sync::Semaphore>) -> Self {
        InitPermit(Some(value))
    }
}

impl InitPermit {
    fn forget(mut self) {
        self.0
            .take()
            .expect("unable to forget twice, created with None?");
    }
}

impl Drop for InitPermit {
    fn drop(&mut self) {
        if let Some(sem) = self.0.take() {
            debug_assert_eq!(sem.available_permits(), 0);
            sem.add_permits(1);
        }
    }
}

#[cfg(test)]
mod tests {
    use futures::Future;

    use super::*;
    use std::{
        convert::Infallible,
        pin::{pin, Pin},
        sync::atomic::{AtomicUsize, Ordering},
        time::Duration,
    };

    #[tokio::test]
    async fn many_initializers() {
        #[derive(Default, Debug)]
        struct Counters {
            factory_got_to_run: AtomicUsize,
            future_polled: AtomicUsize,
            winners: AtomicUsize,
        }

        let initializers = 100;

        let cell = Arc::new(OnceCell::default());
        let counters = Arc::new(Counters::default());
        let barrier = Arc::new(tokio::sync::Barrier::new(initializers + 1));

        let mut js = tokio::task::JoinSet::new();
        for i in 0..initializers {
            js.spawn({
                let cell = cell.clone();
                let counters = counters.clone();
                let barrier = barrier.clone();

                async move {
                    barrier.wait().await;
                    let won = {
                        let g = cell
                            .get_mut_or_init(|permit| {
                                counters.factory_got_to_run.fetch_add(1, Ordering::Relaxed);
                                async {
                                    counters.future_polled.fetch_add(1, Ordering::Relaxed);
                                    Ok::<_, Infallible>((i, permit))
                                }
                            })
                            .await
                            .unwrap();

                        *g == i
                    };

                    if won {
                        counters.winners.fetch_add(1, Ordering::Relaxed);
                    }
                }
            });
        }

        barrier.wait().await;

        while let Some(next) = js.join_next().await {
            next.expect("no panics expected");
        }

        let mut counters = Arc::try_unwrap(counters).unwrap();

        assert_eq!(*counters.factory_got_to_run.get_mut(), 1);
        assert_eq!(*counters.future_polled.get_mut(), 1);
        assert_eq!(*counters.winners.get_mut(), 1);
    }

    #[tokio::test(start_paused = true)]
    async fn reinit_waits_for_deinit() {
        // with the tokio::time paused, we will "sleep" for 1s while holding the reinitialization
        let sleep_for = Duration::from_secs(1);
        let initial = 42;
        let reinit = 1;
        let cell = Arc::new(OnceCell::new(initial));

        let deinitialization_started = Arc::new(tokio::sync::Barrier::new(2));

        let jh = tokio::spawn({
            let cell = cell.clone();
            let deinitialization_started = deinitialization_started.clone();
            async move {
                let guard = cell.get_mut().await.unwrap();
                let (answer, _permit) = cell.take_and_deinit(guard).await;
                assert_eq!(answer, initial);

                deinitialization_started.wait().await;
                tokio::time::sleep(sleep_for).await;
            }
        });

        deinitialization_started.wait().await;

        let started_at = tokio::time::Instant::now();
        cell.get_mut_or_init(|permit| async { Ok::<_, Infallible>((reinit, permit)) })
            .await
            .unwrap();

        let elapsed = started_at.elapsed();
        assert!(
            elapsed >= sleep_for,
            "initialization should had taken at least the time time slept with permit"
        );

        jh.await.unwrap();

        assert_eq!(*cell.get_mut().await.unwrap(), reinit);
    }

    #[tokio::test]
    async fn reinit_with_deinit_permit() {
        let cell = Arc::new(OnceCell::new(42));
        assert!(!cell.has_one_permit.load(Ordering::Acquire));
        assert_eq!(
            cell.inner.read().await.init_semaphore.available_permits(),
            u32::MAX as usize
        );

        let guard = cell.get_mut().await.unwrap();
        assert!(!cell.has_one_permit.load(Ordering::Acquire));
        assert_eq!(
            guard.0.init_semaphore.available_permits(),
            u32::MAX as usize
        );

        let (mol, permit) = cell.take_and_deinit(guard).await;
        assert!(cell.has_one_permit.load(Ordering::Acquire));
        assert_eq!(
            cell.inner.read().await.init_semaphore.available_permits(),
            0
        );

        cell.set(5, permit).await;
        assert_eq!(*cell.get_mut().await.unwrap(), 5);

        let guard = cell.get_mut().await.unwrap();
        let (five, permit) = cell.take_and_deinit(guard).await;
        assert_eq!(5, five);
        cell.set(mol, permit).await;
        assert_eq!(*cell.get_mut().await.unwrap(), 42);
    }

    #[tokio::test]
    async fn initialization_attemptable_until_ok() {
        let cell = OnceCell::default();

        for _ in 0..10 {
            cell.get_mut_or_init(|_permit| async { Err("whatever error") })
                .await
                .unwrap_err();
        }

        let g = cell
            .get_mut_or_init(|permit| async { Ok::<_, Infallible>(("finally success", permit)) })
            .await
            .unwrap();
        assert_eq!(*g, "finally success");
    }

    #[tokio::test]
    async fn initialization_is_cancellation_safe() {
        let cell = OnceCell::default();

        let barrier = tokio::sync::Barrier::new(2);

        let initializer = cell.get_mut_or_init(|permit| async {
            barrier.wait().await;
            futures::future::pending::<()>().await;

            Ok::<_, Infallible>(("never reached", permit))
        });

        tokio::select! {
            _ = initializer => { unreachable!("cannot complete; stuck in pending().await") },
            _ = barrier.wait() => {}
        };

        // now initializer is dropped

        assert!(cell.get_mut().await.is_none());

        let g = cell
            .get_mut_or_init(|permit| async { Ok::<_, Infallible>(("now initialized", permit)) })
            .await
            .unwrap();
        assert_eq!(*g, "now initialized");
    }

    #[tokio::test(start_paused = true)]
    async fn reproduce_init_take_deinit_race_ref() {
        init_take_deinit_scenario(|cell, factory| {
            Box::pin(async {
                cell.get_or_init(factory).await.unwrap();
            })
        })
        .await;
    }

    #[tokio::test(start_paused = true)]
    async fn reproduce_init_take_deinit_race_mut() {
        init_take_deinit_scenario(|cell, factory| {
            Box::pin(async {
                cell.get_mut_or_init(factory).await.unwrap();
            })
        })
        .await;
    }

    type BoxedInitFuture<T, E> = Pin<Box<dyn Future<Output = Result<(T, InitPermit), E>>>>;
    type BoxedInitFunction<T, E> = Box<dyn Fn(InitPermit) -> BoxedInitFuture<T, E>>;

    /// Reproduce an assertion failure with both initialization methods.
    ///
    /// This has interesting generics to be generic between `get_or_init` and `get_mut_or_init`.
    /// Alternative would be a macro_rules! but that is the last resort.
    async fn init_take_deinit_scenario<F>(init_way: F)
    where
        F: for<'a> Fn(
            &'a OnceCell<&'static str>,
            BoxedInitFunction<&'static str, Infallible>,
        ) -> Pin<Box<dyn Future<Output = ()> + 'a>>,
    {
        use tracing::Instrument;

        let cell = OnceCell::default();

        // acquire the init_semaphore only permit to drive initializing tasks in order to waiting
        // on the same semaphore.
        let permit = cell
            .inner
            .read()
            .await
            .init_semaphore
            .clone()
            .try_acquire_owned()
            .unwrap();

        let mut t1 = pin!(init_way(
            &cell,
            Box::new(|permit| Box::pin(async move { Ok(("t1", permit)) })),
        )
        .instrument(tracing::info_span!("t1")));

        let mut t2 = pin!(init_way(
            &cell,
            Box::new(|permit| Box::pin(async move { Ok(("t2", permit)) })),
        )
        .instrument(tracing::info_span!("t2")));

        // drive t2 first to the init_semaphore
        tokio::select! {
            _ = &mut t2 => unreachable!("it cannot get permit"),
            _ = tokio::time::sleep(Duration::from_secs(3600 * 24 * 7 * 365)) => {}
        }

        // followed by t1 in the init_semaphore
        tokio::select! {
            _ = &mut t1 => unreachable!("it cannot get permit"),
            _ = tokio::time::sleep(Duration::from_secs(3600 * 24 * 7 * 365)) => {}
        }

        // now let t2 proceed and initialize
        drop(permit);
        t2.await;

        // in original implementation which did closing and re-creation of the semaphore, t1 was
        // still stuck on the first semaphore, but now that t1 and deinit are using the same
        // semaphore, deinit will have to wait for t1.
        let mut deinit = pin!(async {
            let guard = cell.get_mut().await.unwrap();
            cell.take_and_deinit(guard).await
        }
        .instrument(tracing::info_span!("deinit")));

        tokio::select! {
            _ = &mut deinit => unreachable!("deinit must not make progress before t1 is complete"),
            _ = tokio::time::sleep(Duration::from_secs(3600 * 24 * 7 * 365)) => {}
        }

        // now originally t1 would see the semaphore it has as closed. it cannot yet get a permit from
        // the new one.
        tokio::select! {
            _ = &mut t1 => unreachable!("it cannot get permit"),
            _ = tokio::time::sleep(Duration::from_secs(3600 * 24 * 7 * 365)) => {}
        }

        let (s, _) = deinit.await;
        assert_eq!("t2", s);

        t1.await;

        assert_eq!("t1", *cell.get().await.unwrap());
    }
}
