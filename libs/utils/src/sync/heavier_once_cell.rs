use std::sync::{
    atomic::{AtomicUsize, Ordering},
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
            inner: tokio::sync::RwLock::new(Inner {
                init_semaphore: Arc::new(sem),
                value: Some(value),
            }),
            initializers: AtomicUsize::new(0),
        }
    }

    /// Returns a guard to an existing initialized value, or uniquely initializes the value before
    /// returning the guard.
    ///
    /// Initializing might wait on any existing [`GuardMut::take_and_deinit`] deinitialization.
    ///
    /// Initialization is panic-safe and cancellation-safe.
    pub async fn get_mut_or_init<F, Fut, E>(&self, factory: F) -> Result<GuardMut<'_, T>, E>
    where
        F: FnOnce(InitPermit) -> Fut,
        Fut: std::future::Future<Output = Result<(T, InitPermit), E>>,
    {
        loop {
            let sem = {
                let guard = self.inner.write().await;
                if guard.value.is_some() {
                    return Ok(GuardMut(guard));
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
                    let guard = self.inner.write().await;
                    if !Arc::ptr_eq(&sem, &guard.init_semaphore) {
                        // there was a take_and_deinit in between
                        continue;
                    }
                    assert!(
                        guard.value.is_some(),
                        "semaphore got closed, must be initialized"
                    );
                    return Ok(GuardMut(guard));
                };

                permit.forget();
            }

            let permit = InitPermit(sem);
            let (value, _permit) = factory(permit).await?;

            let guard = self.inner.write().await;

            return Ok(Self::set0(value, guard));
        }
    }

    /// Returns a guard to an existing initialized value, or uniquely initializes the value before
    /// returning the guard.
    ///
    /// Initialization is panic-safe and cancellation-safe.
    pub async fn get_or_init<F, Fut, E>(&self, factory: F) -> Result<GuardRef<'_, T>, E>
    where
        F: FnOnce(InitPermit) -> Fut,
        Fut: std::future::Future<Output = Result<(T, InitPermit), E>>,
    {
        loop {
            let sem = {
                let guard = self.inner.read().await;
                if guard.value.is_some() {
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

                let Ok(permit) = permit else {
                    let guard = self.inner.read().await;
                    if !Arc::ptr_eq(&sem, &guard.init_semaphore) {
                        // there was a take_and_deinit in between
                        continue;
                    }
                    assert!(
                        guard.value.is_some(),
                        "semaphore got closed, must be initialized"
                    );
                    return Ok(GuardRef(guard));
                };

                permit.forget();
            }

            let permit = InitPermit(sem);
            let (value, _permit) = factory(permit).await?;

            let guard = self.inner.write().await;

            return Ok(Self::set0(value, guard).downgrade());
        }
    }

    /// Assuming a permit is held after previous call to [`GuardMut::take_and_deinit`], it can be used
    /// to complete initializing the inner value.
    ///
    /// # Panics
    ///
    /// If the inner has already been initialized.
    pub async fn set(&self, value: T, _permit: InitPermit) -> GuardMut<'_, T> {
        let guard = self.inner.write().await;

        // cannot assert that this permit is for self.inner.semaphore, but we can assert it cannot
        // give more permits right now.
        if guard.init_semaphore.try_acquire().is_ok() {
            drop(guard);
            panic!("permit is of wrong origin");
        }

        Self::set0(value, guard)
    }

    fn set0(value: T, mut guard: tokio::sync::RwLockWriteGuard<'_, Inner<T>>) -> GuardMut<'_, T> {
        if guard.value.is_some() {
            drop(guard);
            unreachable!("we won permit, must not be initialized");
        }
        guard.value = Some(value);
        guard.init_semaphore.close();
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
    /// Take the current value, and a new permit for it's deinitialization.
    ///
    /// The permit will be on a semaphore part of the new internal value, and any following
    /// [`OnceCell::get_or_init`] will wait on it to complete.
    pub fn take_and_deinit(&mut self) -> (T, InitPermit) {
        let mut swapped = Inner::default();
        let sem = swapped.init_semaphore.clone();
        sem.try_acquire().expect("we just created this").forget();
        std::mem::swap(&mut *self.0, &mut swapped);
        swapped
            .value
            .map(|v| (v, InitPermit(sem)))
            .expect("guard is not created unless value has been initialized")
    }

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
pub struct InitPermit(Arc<tokio::sync::Semaphore>);

impl Drop for InitPermit {
    fn drop(&mut self) {
        self.0.add_permits(1);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{
        convert::Infallible,
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
                let (answer, _permit) = cell
                    .get_mut()
                    .await
                    .expect("initialized to value")
                    .take_and_deinit();
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

        let (mol, permit) = cell.get_mut().await.unwrap().take_and_deinit();
        cell.set(5, permit).await;
        assert_eq!(*cell.get_mut().await.unwrap(), 5);

        let (five, permit) = cell.get_mut().await.unwrap().take_and_deinit();
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

    macro_rules! init_race_reproduction {
        ($method:ident) => {{
            let cell = OnceCell::default();

            // use this permit to force two instances to clone out the semaphore and move to force
            // two tasks t1 and t2 to be awaiting for a permit.
            let permit = cell
                .inner
                .read()
                .await
                .init_semaphore
                .clone()
                .try_acquire_owned()
                .unwrap();

            let t1 = async {
                cell.$method(|init| async { Ok::<_, Infallible>(("t1", init)) })
                    .await
            };
            let mut t1 = std::pin::pin!(t1);

            let t2 = async {
                cell.$method(|init| async { Ok::<_, Infallible>(("t2", init)) })
                    .await
            };
            let mut t2 = std::pin::pin!(t2);

            // drive t2 first to the queue
            tokio::select! {
                _ = &mut t2 => unreachable!("it cannot get permit"),
                _ = tokio::time::sleep(Duration::from_secs(3600 * 24 * 7 * 365)) => {}
            }

            // followed by t1 in the queue
            tokio::select! {
                _ = &mut t1 => unreachable!("it cannot get permit"),
                _ = tokio::time::sleep(Duration::from_secs(3600 * 24 * 7 * 365)) => {}
            }

            drop(permit);

            // now let "the other" proceed and initialize
            drop(t2.await);

            let (s, permit) = { cell.get_mut().await.unwrap().take_and_deinit() };
            assert_eq!("t2", s);

            // now t1 will see the original semaphore as closed, and assert that the option is set
            // instead it should notice the Arc is not the same, and loop around

            tokio::select! {
                _ = &mut t1 => unreachable!("it cannot get permit"),
                _ = tokio::time::sleep(Duration::from_secs(3600 * 24 * 7 * 365)) => {}
            }

            drop(permit);

            // only now we get to initialize it
            drop(t1.await);

            assert_eq!("t1", *cell.get().await.unwrap());
        }};
    }

    #[tokio::test(start_paused = true)]
    async fn reproduce_init_take_deinit_race() {
        init_race_reproduction!(get_or_init);
    }

    #[tokio::test(start_paused = true)]
    async fn reproduce_init_take_deinit_race_mut() {
        init_race_reproduction!(get_mut_or_init);
    }
}
