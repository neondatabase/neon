use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc, Mutex, MutexGuard,
};
use tokio::sync::Semaphore;

/// Custom design like [`tokio::sync::OnceCell`] but using [`OwnedSemaphorePermit`] instead of
/// `SemaphorePermit`.
///
/// Allows use of `take` which does not require holding an outer mutex guard
/// for the duration of initialization.
///
/// Has no unsafe, builds upon [`tokio::sync::Semaphore`] and [`std::sync::Mutex`].
///
/// [`OwnedSemaphorePermit`]: tokio::sync::OwnedSemaphorePermit
pub struct OnceCell<T> {
    inner: Mutex<Inner<T>>,
    initializers: AtomicUsize,
}

impl<T> Default for OnceCell<T> {
    /// Create new uninitialized [`OnceCell`].
    fn default() -> Self {
        Self {
            inner: Default::default(),
            initializers: AtomicUsize::new(0),
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
        let sem = Semaphore::new(1);
        sem.close();
        Self {
            inner: Mutex::new(Inner {
                init_semaphore: Arc::new(sem),
                value: Some(value),
            }),
            initializers: AtomicUsize::new(0),
        }
    }

    /// Returns a guard to an existing initialized value, or uniquely initializes the value before
    /// returning the guard.
    ///
    /// Initializing might wait on any existing [`Guard::take_and_deinit`] deinitialization.
    ///
    /// Initialization is panic-safe and cancellation-safe.
    pub async fn get_or_init<F, Fut, E>(&self, factory: F) -> Result<Guard<'_, T>, E>
    where
        F: FnOnce(InitPermit) -> Fut,
        Fut: std::future::Future<Output = Result<(T, InitPermit), E>>,
    {
        loop {
            let sem = {
                let guard = self.inner.lock().unwrap();
                if guard.value.is_some() {
                    return Ok(Guard(guard));
                }
                guard.init_semaphore.clone()
            };

            {
                let permit = {
                    // increment the count for the duration of queued
                    let _guard = CountWaitingInitializers::start(self);
                    sem.acquire().await
                };

                let Ok(permit) = permit else {
                    let guard = self.inner.lock().unwrap();
                    if !Arc::ptr_eq(&sem, &guard.init_semaphore) {
                        // there was a take_and_deinit in between
                        continue;
                    }
                    assert!(
                        guard.value.is_some(),
                        "semaphore got closed, must be initialized"
                    );
                    return Ok(Guard(guard));
                };

                permit.forget();
            }

            let permit = InitPermit(sem);
            let (value, _permit) = factory(permit).await?;

            let guard = self.inner.lock().unwrap();

            return Ok(Self::set0(value, guard));
        }
    }

    /// Returns a guard to an existing initialized value, or returns an unique initialization
    /// permit which can be used to initialize this `OnceCell` using `OnceCell::set`.
    pub async fn get_or_init_detached(&self) -> Result<Guard<'_, T>, InitPermit> {
        // It looks like OnceCell::get_or_init could be implemented using this method instead of
        // duplication. However, that makes the future be !Send due to possibly holding on to the
        // MutexGuard over an await point.
        loop {
            let sem = {
                let guard = self.inner.lock().unwrap();
                if guard.value.is_some() {
                    return Ok(Guard(guard));
                }
                guard.init_semaphore.clone()
            };

            {
                let permit = {
                    // increment the count for the duration of queued
                    let _guard = CountWaitingInitializers::start(self);
                    sem.acquire().await
                };

                let Ok(permit) = permit else {
                    let guard = self.inner.lock().unwrap();
                    if !Arc::ptr_eq(&sem, &guard.init_semaphore) {
                        // there was a take_and_deinit in between
                        continue;
                    }
                    assert!(
                        guard.value.is_some(),
                        "semaphore got closed, must be initialized"
                    );
                    return Ok(Guard(guard));
                };

                permit.forget();
            }

            let permit = InitPermit(sem);
            return Err(permit);
        }
    }

    /// Assuming a permit is held after previous call to [`Guard::take_and_deinit`], it can be used
    /// to complete initializing the inner value.
    ///
    /// # Panics
    ///
    /// If the inner has already been initialized.
    pub fn set(&self, value: T, _permit: InitPermit) -> Guard<'_, T> {
        let guard = self.inner.lock().unwrap();

        // cannot assert that this permit is for self.inner.semaphore, but we can assert it cannot
        // give more permits right now.
        if guard.init_semaphore.try_acquire().is_ok() {
            drop(guard);
            panic!("permit is of wrong origin");
        }

        Self::set0(value, guard)
    }

    fn set0(value: T, mut guard: std::sync::MutexGuard<'_, Inner<T>>) -> Guard<'_, T> {
        if guard.value.is_some() {
            drop(guard);
            unreachable!("we won permit, must not be initialized");
        }
        guard.value = Some(value);
        guard.init_semaphore.close();
        Guard(guard)
    }

    /// Returns a guard to an existing initialized value, if any.
    pub fn get(&self) -> Option<Guard<'_, T>> {
        let guard = self.inner.lock().unwrap();
        if guard.value.is_some() {
            Some(Guard(guard))
        } else {
            None
        }
    }

    /// Like [`Guard::take_and_deinit`], but will return `None` if this OnceCell was never
    /// initialized.
    pub fn take_and_deinit(&mut self) -> Option<(T, InitPermit)> {
        let inner = self.inner.get_mut().unwrap();

        inner.take_and_deinit()
    }

    /// Return the number of [`Self::get_or_init`] calls waiting for initialization to complete.
    pub fn initializer_count(&self) -> usize {
        self.initializers.load(Ordering::Relaxed)
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

impl<T> Drop for CountWaitingInitializers<'_, T> {
    fn drop(&mut self) {
        self.0.initializers.fetch_sub(1, Ordering::Relaxed);
    }
}

/// Uninteresting guard object to allow short-lived access to inspect or clone the held,
/// initialized value.
#[derive(Debug)]
pub struct Guard<'a, T>(MutexGuard<'a, Inner<T>>);

impl<T> std::ops::Deref for Guard<'_, T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        self.0
            .value
            .as_ref()
            .expect("guard is not created unless value has been initialized")
    }
}

impl<T> std::ops::DerefMut for Guard<'_, T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.0
            .value
            .as_mut()
            .expect("guard is not created unless value has been initialized")
    }
}

impl<T> Guard<'_, T> {
    /// Take the current value, and a new permit for it's deinitialization.
    ///
    /// The permit will be on a semaphore part of the new internal value, and any following
    /// [`OnceCell::get_or_init`] will wait on it to complete.
    pub fn take_and_deinit(mut self) -> (T, InitPermit) {
        self.0
            .take_and_deinit()
            .expect("guard is not created unless value has been initialized")
    }
}

impl<T> Inner<T> {
    pub fn take_and_deinit(&mut self) -> Option<(T, InitPermit)> {
        let value = self.value.take()?;

        let mut swapped = Inner::default();
        let sem = swapped.init_semaphore.clone();
        // acquire and forget right away, moving the control over to InitPermit
        sem.try_acquire().expect("we just created this").forget();
        let permit = InitPermit(sem);
        std::mem::swap(self, &mut swapped);
        Some((value, permit))
    }
}

/// Type held by OnceCell (de)initializing task.
///
/// On drop, this type will return the permit.
pub struct InitPermit(Arc<tokio::sync::Semaphore>);

impl std::fmt::Debug for InitPermit {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let ptr = Arc::as_ptr(&self.0) as *const ();
        f.debug_tuple("InitPermit").field(&ptr).finish()
    }
}

impl Drop for InitPermit {
    fn drop(&mut self) {
        assert_eq!(
            self.0.available_permits(),
            0,
            "InitPermit should only exist as the unique permit"
        );
        self.0.add_permits(1);
    }
}

#[cfg(test)]
mod tests {
    use futures::Future;

    use super::*;
    use std::{
        convert::Infallible,
        pin::{pin, Pin},
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
                            .get_or_init(|permit| {
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
                let (answer, _permit) = cell.get().expect("initialized to value").take_and_deinit();
                assert_eq!(answer, initial);

                deinitialization_started.wait().await;
                tokio::time::sleep(sleep_for).await;
            }
        });

        deinitialization_started.wait().await;

        let started_at = tokio::time::Instant::now();
        cell.get_or_init(|permit| async { Ok::<_, Infallible>((reinit, permit)) })
            .await
            .unwrap();

        let elapsed = started_at.elapsed();
        assert!(
            elapsed >= sleep_for,
            "initialization should had taken at least the time time slept with permit"
        );

        jh.await.unwrap();

        assert_eq!(*cell.get().unwrap(), reinit);
    }

    #[test]
    fn reinit_with_deinit_permit() {
        let cell = Arc::new(OnceCell::new(42));

        let (mol, permit) = cell.get().unwrap().take_and_deinit();
        cell.set(5, permit);
        assert_eq!(*cell.get().unwrap(), 5);

        let (five, permit) = cell.get().unwrap().take_and_deinit();
        assert_eq!(5, five);
        cell.set(mol, permit);
        assert_eq!(*cell.get().unwrap(), 42);
    }

    #[tokio::test]
    async fn initialization_attemptable_until_ok() {
        let cell = OnceCell::default();

        for _ in 0..10 {
            cell.get_or_init(|_permit| async { Err("whatever error") })
                .await
                .unwrap_err();
        }

        let g = cell
            .get_or_init(|permit| async { Ok::<_, Infallible>(("finally success", permit)) })
            .await
            .unwrap();
        assert_eq!(*g, "finally success");
    }

    #[tokio::test]
    async fn initialization_is_cancellation_safe() {
        let cell = OnceCell::default();

        let barrier = tokio::sync::Barrier::new(2);

        let initializer = cell.get_or_init(|permit| async {
            barrier.wait().await;
            futures::future::pending::<()>().await;

            Ok::<_, Infallible>(("never reached", permit))
        });

        tokio::select! {
            _ = initializer => { unreachable!("cannot complete; stuck in pending().await") },
            _ = barrier.wait() => {}
        };

        // now initializer is dropped

        assert!(cell.get().is_none());

        let g = cell
            .get_or_init(|permit| async { Ok::<_, Infallible>(("now initialized", permit)) })
            .await
            .unwrap();
        assert_eq!(*g, "now initialized");
    }

    #[tokio::test(start_paused = true)]
    async fn reproduce_init_take_deinit_race() {
        init_take_deinit_scenario(|cell, factory| {
            Box::pin(async {
                cell.get_or_init(factory).await.unwrap();
            })
        })
        .await;
    }

    type BoxedInitFuture<T, E> = Pin<Box<dyn Future<Output = Result<(T, InitPermit), E>>>>;
    type BoxedInitFunction<T, E> = Box<dyn Fn(InitPermit) -> BoxedInitFuture<T, E>>;

    /// Reproduce an assertion failure.
    ///
    /// This has interesting generics to be generic between `get_or_init` and `get_mut_or_init`.
    /// We currently only have one, but the structure is kept.
    async fn init_take_deinit_scenario<F>(init_way: F)
    where
        F: for<'a> Fn(
            &'a OnceCell<&'static str>,
            BoxedInitFunction<&'static str, Infallible>,
        ) -> Pin<Box<dyn Future<Output = ()> + 'a>>,
    {
        let cell = OnceCell::default();

        // acquire the init_semaphore only permit to drive initializing tasks in order to waiting
        // on the same semaphore.
        let permit = cell
            .inner
            .lock()
            .unwrap()
            .init_semaphore
            .clone()
            .try_acquire_owned()
            .unwrap();

        let mut t1 = pin!(init_way(
            &cell,
            Box::new(|permit| Box::pin(async move { Ok(("t1", permit)) })),
        ));

        let mut t2 = pin!(init_way(
            &cell,
            Box::new(|permit| Box::pin(async move { Ok(("t2", permit)) })),
        ));

        // drive t2 first to the init_semaphore -- the timeout will be hit once t2 future can
        // no longer make progress
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

        let (s, permit) = { cell.get().unwrap().take_and_deinit() };
        assert_eq!("t2", s);

        // now originally t1 would see the semaphore it has as closed. it cannot yet get a permit from
        // the new one.
        tokio::select! {
            _ = &mut t1 => unreachable!("it cannot get permit"),
            _ = tokio::time::sleep(Duration::from_secs(3600 * 24 * 7 * 365)) => {}
        }

        // only now we get to initialize it
        drop(permit);
        t1.await;

        assert_eq!("t1", *cell.get().unwrap());
    }

    #[tokio::test(start_paused = true)]
    async fn detached_init_smoke() {
        let target = OnceCell::default();

        let Err(permit) = target.get_or_init_detached().await else {
            unreachable!("it is not initialized")
        };

        tokio::time::timeout(
            std::time::Duration::from_secs(3600 * 24 * 7 * 365),
            target.get_or_init(|permit2| async { Ok::<_, Infallible>((11, permit2)) }),
        )
        .await
        .expect_err("should timeout since we are already holding the permit");

        target.set(42, permit);

        let (_answer, permit) = {
            let guard = target
                .get_or_init(|permit| async { Ok::<_, Infallible>((11, permit)) })
                .await
                .unwrap();

            assert_eq!(*guard, 42);

            guard.take_and_deinit()
        };

        assert!(target.get().is_none());

        target.set(11, permit);

        assert_eq!(*target.get().unwrap(), 11);
    }

    #[tokio::test]
    async fn take_and_deinit_on_mut() {
        use std::convert::Infallible;

        let mut target = OnceCell::<u32>::default();
        assert!(target.take_and_deinit().is_none());

        target
            .get_or_init(|permit| async move { Ok::<_, Infallible>((42, permit)) })
            .await
            .unwrap();

        let again = target.take_and_deinit();
        assert!(matches!(again, Some((42, _))), "{again:?}");

        assert!(target.take_and_deinit().is_none());
    }
}
