use std::sync::{Arc, Mutex, MutexGuard};
use tokio::sync::Semaphore;

/// Custom design like [`tokio::sync::OnceCell`] but using [`OwnedSemaphorePermit`] instead of
/// `SemaphorePermit`, allowing use of `take` which does not require holding an outer mutex guard
/// for the duration of initialization.
///
/// Has no unsafe, builds upon [`tokio::sync::Semaphore`] and [`std::sync::Mutex`].
///
/// [`OwnedSemaphorePermit`]: tokio::sync::OwnedSemaphorePermit
pub struct OnceCell<T> {
    inner: Mutex<Inner<T>>,
}

impl<T> Default for OnceCell<T> {
    /// Create new uninitialized [`OnceCell`].
    fn default() -> Self {
        Self {
            inner: Default::default(),
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
        F: FnOnce() -> Fut,
        Fut: std::future::Future<Output = Result<T, E>>,
    {
        let sem = {
            let guard = self.inner.lock().unwrap();
            if guard.value.is_some() {
                return Ok(Guard(guard));
            }
            guard.init_semaphore.clone()
        };

        let permit = sem.acquire_owned().await;
        if permit.is_err() {
            let guard = self.inner.lock().unwrap();
            assert!(
                guard.value.is_some(),
                "semaphore got closed, must be initialized"
            );
            return Ok(Guard(guard));
        } else {
            // now we try
            let value = factory().await?;

            let mut guard = self.inner.lock().unwrap();
            assert!(
                guard.value.is_none(),
                "we won permit, must not be initialized"
            );
            guard.value = Some(value);
            guard.init_semaphore.close();
            Ok(Guard(guard))
        }
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

impl<'a, T> Guard<'a, T> {
    /// Take the current value, and a new permit for it's deinitialization.
    ///
    /// The permit will be on a semaphore part of the new internal value, and any following
    /// [`OnceCell::get_or_init`] will wait on it to complete.
    pub fn take_and_deinit(&mut self) -> (T, tokio::sync::OwnedSemaphorePermit) {
        let mut swapped = Inner::default();
        let permit = swapped
            .init_semaphore
            .clone()
            .try_acquire_owned()
            .expect("we just created this");
        std::mem::swap(&mut *self.0, &mut swapped);
        swapped
            .value
            .map(|v| (v, permit))
            .expect("guard is not created unless value has been initialized")
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
                            .get_or_init(|| {
                                counters.factory_got_to_run.fetch_add(1, Ordering::Relaxed);
                                async {
                                    counters.future_polled.fetch_add(1, Ordering::Relaxed);
                                    Ok::<_, Infallible>(i)
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
        cell.get_or_init(|| async { Ok::<_, Infallible>(reinit) })
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

    #[tokio::test]
    async fn initialization_attemptable_until_ok() {
        let cell = OnceCell::default();

        for _ in 0..10 {
            cell.get_or_init(|| async { Err("whatever error") })
                .await
                .unwrap_err();
        }

        let g = cell
            .get_or_init(|| async { Ok::<_, Infallible>("finally success") })
            .await
            .unwrap();
        assert_eq!(*g, "finally success");
    }

    #[tokio::test]
    async fn initialization_is_cancellation_safe() {
        let cell = OnceCell::default();

        let barrier = tokio::sync::Barrier::new(2);

        let initializer = cell.get_or_init(|| async {
            barrier.wait().await;
            futures::future::pending::<()>().await;

            Ok::<_, Infallible>("never reached")
        });

        tokio::select! {
            _ = initializer => { unreachable!("cannot complete; stuck in pending().await") },
            _ = barrier.wait() => {}
        };

        // now initializer is dropped

        assert!(cell.get().is_none());

        let g = cell
            .get_or_init(|| async { Ok::<_, Infallible>("now initialized") })
            .await
            .unwrap();
        assert_eq!(*g, "now initialized");
    }
}
