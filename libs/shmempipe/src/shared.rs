//! Simple shared pthread mutex.
//!
//! Monkeyd after rust std lib pthread mutex implementation, with the distinction that we will only
//! place mutexes on mmap'd memory, so they can only be accessed via Pin to avoid moving.
//!
//! Also, the pthread mutexes needed are shared between processes. We cannot use plain futexes,
//! because we want to support unixes (mac os), even though we run production on linux. Raw futexes
//! could be prototyped, since on linux pthreads work on top of them.
//!
//! Apparently mac os x doesn't support robust mutexes.
//!
//! Poisoning is not interesting for us either, because we cannot run code while child process is
//! being killed. We do however want robust pthread mutexes which know when their owner has died
//! while holding the lock.
//!
//! ## API
//!
//! There is an in-place initialization support which returns a `&mut T` as a symbolic gesture that
//! `MaybeUninit::assume_init_mut` has been invoked.

use std::{cell::UnsafeCell, mem::MaybeUninit, os::unix::prelude::FromRawFd, pin::Pin};

/// Return value for the robust pthread_mutex [`Mutex::lock`] and [`Mutex::try_lock`].
#[cfg_attr(test, derive(Debug))]
enum Locked<T> {
    /// Mutex is now locked.
    Ok(T),

    /// The process which had previously locked the mutex has died while holding the lock.
    ///
    /// The lock has been made consistent with `pthread_mutex_consistent`, but any
    /// protected data structure has not been fixed.
    PreviousOwnerDied(T),
}

#[cfg(not(miri))]
mod pthread {
    use std::{mem::MaybeUninit, pin::Pin};

    use super::Locked;

    // this was quite tricky to understand first, but in shared memory app these same mutexes
    // appear in different addresses. in order to do the cond variable mutex check, we must instead
    // compare identities.
    static MUTEX_ID_COUNTER: std::sync::atomic::AtomicU32 = std::sync::atomic::AtomicU32::new(0);

    #[repr(C)]
    pub struct Mutex(libc::pthread_mutex_t, u32, std::marker::PhantomPinned);

    impl Mutex {
        // Using MaybeUninit::assume_init might move which is why there are the other variants
        pub(super) fn initialize_at(
            place: &mut MaybeUninit<Mutex>,
        ) -> std::io::Result<Pin<&mut Mutex>> {
            // assumption is that pthread mutexes must not be moved after initialization
            // so pin will be needed somewhere

            // note: this does not escape, and is properly destroyed by Attr
            let mut attr = MaybeUninit::<libc::pthread_mutexattr_t>::uninit();

            unsafe {
                cvt_nz(libc::pthread_mutexattr_init(attr.as_mut_ptr()))?;
            }

            let mut attr = Attr(&mut attr);
            attr.set_type_normal()?;
            attr.set_process_shared()?;
            attr.set_robust()?;

            // Safety: pointer is not null
            let mutex = unsafe { std::ptr::addr_of_mut!((*place.as_mut_ptr()).0) };

            // Safety: ffi
            unsafe { cvt_nz(libc::pthread_mutex_init(mutex, attr.0.as_ptr())) }?;

            let id = MUTEX_ID_COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed);

            let id_slot = unsafe { std::ptr::addr_of_mut!((*place.as_mut_ptr()).1) };

            unsafe { id_slot.write(id) };

            unsafe {
                // Safety: it is now initialized, and we hope it doesn't move
                // Pin here is more "symbolic"
                //
                // TODO: does a ZST field need initialization? probably not because it's okay in
                // repr(transparent).
                Ok(Pin::new_unchecked(place.assume_init_mut()))
            }
        }

        pub(super) fn lock(self: std::pin::Pin<&Self>) -> Locked<()> {
            let ptr = self.inner();
            let res = unsafe { libc::pthread_mutex_lock(ptr) };

            match res {
                0 => Locked::Ok(()),
                libc::EOWNERDEAD => {
                    // unsure if this is the right strategy, but it's not the worst?
                    self.make_consistent().expect("failed to repair after lock");
                    Locked::PreviousOwnerDied(())
                }
                other => Err(std::io::Error::from_raw_os_error(other)).expect("lock failed"),
            }
        }

        pub(super) fn try_lock(self: std::pin::Pin<&Self>) -> Option<Locked<()>> {
            let ptr = self.inner();
            let res = unsafe { libc::pthread_mutex_trylock(ptr) };
            match res {
                0 => Some(Locked::Ok(())),
                libc::EBUSY => None,
                libc::EOWNERDEAD => {
                    // see Mutex::lock
                    self.make_consistent()
                        .expect("failed to repair after try_lock");
                    Some(Locked::PreviousOwnerDied(()))
                }
                other => Err(std::io::Error::from_raw_os_error(other))
                    .expect("unexpected error from trylock"),
            }
        }

        /// Robust pthread mutex requires that we repair it with `pthread_mutex_consistent` before
        /// the unlock. Failure to do so will result in forever poisoned mutex.
        fn make_consistent(self: std::pin::Pin<&Self>) -> std::io::Result<()> {
            let ptr = self.inner();
            let res = unsafe { libc::pthread_mutex_consistent(ptr) };
            cvt_nz(res)
        }

        pub fn unlock(self: std::pin::Pin<&Self>) {
            let ptr = self.inner();
            let res = unsafe { libc::pthread_mutex_unlock(ptr) };
            cvt_nz(res).expect("unlock failed")
        }

        fn inner(self: std::pin::Pin<&Self>) -> *mut libc::pthread_mutex_t {
            &self.0 as *const _ as *mut _
        }
    }

    impl Drop for Mutex {
        fn drop(&mut self) {
            // FIXME: it's forbidden to destroy locked mutex, so there should be a check? or
            // lock+unlock?
            {
                let me = unsafe { Pin::new_unchecked(&*self) };
                match me.try_lock() {
                    Some(Locked::Ok(()) | Locked::PreviousOwnerDied(())) => {
                        me.unlock();
                    }
                    None => {
                        let lock = &self.0 as *const _;

                        #[allow(unreachable_code)]
                        {
                            debug_assert!(
                                false,
                                "a lock at {:?} is locked in Drop -- it shouldn't be",
                                lock
                            );

                            let here = file!();
                            let line = line!();

                            eprintln!(
                                "{here}:{line}: a lock at {:?} is locked In Drop -- it shouldn't be",
                                lock
                            );
                        }
                    }
                }
            }

            let res = unsafe { libc::pthread_mutex_destroy(&mut self.0 as *mut _) };
            debug_assert_eq!(res, 0);
        }
    }

    struct Attr<'a>(&'a mut MaybeUninit<libc::pthread_mutexattr_t>);

    impl Attr<'_> {
        /// Configures the same NORMAL mode as rust std mutex used to be in.
        fn set_type_normal(&mut self) -> std::io::Result<()> {
            let res = unsafe {
                libc::pthread_mutexattr_settype(self.0.as_mut_ptr(), libc::PTHREAD_MUTEX_NORMAL)
            };

            cvt_nz(res)
        }

        /// Configures the mutex shareable between processes.
        fn set_process_shared(&mut self) -> std::io::Result<()> {
            let res = unsafe {
                libc::pthread_mutexattr_setpshared(
                    self.0.as_mut_ptr(),
                    libc::PTHREAD_PROCESS_SHARED,
                )
            };

            cvt_nz(res)
        }

        /// Configures the mutex to be poisoned when a process dies holding it.
        ///
        /// Background: https://www.kernel.org/doc/Documentation/robust-futexes.txt
        fn set_robust(&mut self) -> std::io::Result<()> {
            let res = unsafe {
                libc::pthread_mutexattr_setrobust(self.0.as_mut_ptr(), libc::PTHREAD_MUTEX_ROBUST)
            };

            cvt_nz(res)
        }
    }

    impl Drop for Attr<'_> {
        fn drop(&mut self) {
            let res = unsafe { libc::pthread_mutexattr_destroy(self.0.as_mut_ptr()) };
            debug_assert_eq!(res, 0);
        }
    }

    /// Returns an error using [`std::io::Error::from_raw_os_error`] if the value is non-zero.
    ///
    /// From rust std lib.
    fn cvt_nz(res: libc::c_int) -> std::io::Result<()> {
        if res == 0 {
            Ok(())
        } else {
            Err(std::io::Error::from_raw_os_error(res))
        }
    }

    /// Following std implementation, but in-place.
    // repr(c) as this is one of the structures used to communicate, we don't want fields to be
    // reordered differently in two users.
    #[repr(C)]
    pub struct Condvar {
        /// Used to check against "using different mutexes is UB".
        mutex: std::sync::atomic::AtomicU32,
        inner: libc::pthread_cond_t,
        _marker: std::marker::PhantomPinned,
    }

    impl Condvar {
        pub(super) fn initialize_at(
            place: &mut MaybeUninit<Self>,
        ) -> std::io::Result<Pin<&mut Self>> {
            let mut attr = MaybeUninit::<libc::pthread_condattr_t>::uninit();
            unsafe { libc::pthread_condattr_init(attr.as_mut_ptr()) };
            let mut attr = CondAttr(&mut attr);

            // there are others in rust std
            #[cfg(not(target_os = "macos"))]
            attr.set_monotonic_clock();

            attr.set_pshared();

            // maybe this does not have to go through maybeuninit, since the aggregate is already
            // maybeuninit?
            let target = unsafe { std::ptr::addr_of_mut!((*place.as_mut_ptr()).inner) };

            let res = unsafe { libc::pthread_cond_init(target, attr.0.as_mut_ptr()) };
            assert_eq!(res, 0);

            drop(attr);

            let mutex = unsafe { std::ptr::addr_of_mut!((*place.as_mut_ptr()).mutex) };
            unsafe { mutex.write(std::sync::atomic::AtomicU32::default()) };

            // Safety: all fields are initialized
            unsafe { Ok(Pin::new_unchecked(place.assume_init_mut())) }
        }

        #[track_caller]
        fn verify(self: Pin<&Self>, mutex: Pin<&Mutex>) {
            use std::sync::atomic::Ordering::Relaxed;
            // Relaxed is enough for this check which can be raced by other threads, no additional
            // effects are depended on

            let id = mutex.1;

            match self.mutex.compare_exchange(0, id, Relaxed, Relaxed) {
                Ok(_) => {}
                Err(same) if same == id => {}
                Err(_other) => panic!(
                    "misusing same condvar with different mutexes, {:?} vs. {:?}",
                    id, _other
                ),
            }
        }

        fn inner(self: Pin<&Self>) -> *mut libc::pthread_cond_t {
            // Safety: libc fns expect *mut
            &self.inner as *const _ as *mut _
        }

        pub(super) fn notify_one(self: Pin<&Self>) {
            let inner = self.inner();
            let res = unsafe { libc::pthread_cond_signal(inner) };
            cvt_nz(res).expect("notify_one failed");
        }

        pub(super) fn notify_all(self: Pin<&Self>) {
            let inner = self.inner();
            let res = unsafe { libc::pthread_cond_broadcast(inner) };
            cvt_nz(res).expect("notify_all failed");
        }

        #[track_caller]
        pub(super) fn wait(self: Pin<&Self>, mutex: Pin<&Mutex>) -> Locked<()> {
            self.verify(mutex);
            let mutex = mutex.inner();
            let inner = self.inner();
            let res = unsafe { libc::pthread_cond_wait(inner, mutex) };
            match res {
                0 => Locked::Ok(()),
                x if x == libc::EOWNERDEAD => Locked::PreviousOwnerDied(()),
                other => loop {
                    cvt_nz(other).expect("wait failed")
                },
            }
        }

        #[track_caller]
        pub(super) fn wait_timeout(
            self: Pin<&Self>,
            dur: std::time::Duration,
            mutex: Pin<&Mutex>,
        ) -> Locked<bool> {
            self.verify(mutex);
            let mutex = mutex.inner();

            #[cfg(target_pointer_width = "32")]
            compile_error!("check all the abi complications with x64_32 or other 32-bit");

            #[cfg(target_os = "macos")]
            compile_error!("at least mac os requires special handling for max 1y etc.");

            // FIXME: this is not y2038 safe! rust std uses clock_gettime64 through a weak symbol
            // which requires some ABI tricks, but libc does not expose it.
            let mut now = libc::timespec {
                tv_sec: 0,
                // range 0..999_999_999
                tv_nsec: 0,
            };

            // it is a shame one cannot reuse std's implementations for these.
            // std uses clock_gettime64, but nix doesn't expose it either as of writing this
            let res = unsafe { libc::clock_gettime(libc::CLOCK_MONOTONIC, &mut now as *mut _) };
            assert_eq!(res, 0);

            let until = libc::timespec::from(
                Timespec::from(now)
                    .checked_add_duration(dur)
                    .unwrap_or(Timespec::MAX),
            );

            let inner = self.inner();
            let res = unsafe { libc::pthread_cond_timedwait(inner, mutex, &until as *const _) };

            match res {
                0 => Locked::Ok(true),
                x if x == libc::ETIMEDOUT => Locked::Ok(false),
                x if x == libc::EOWNERDEAD => Locked::Ok(true),
                other => loop {
                    cvt_nz(other).expect("wait_timeout failed")
                },
            }
        }
    }

    // This and all of it's functionality is after rust std: library/std/src/sys/unix/time.rs
    #[derive(Clone, Copy)]
    struct Timespec {
        tv_sec: i64,
        tv_nsec: u32,
    }

    impl From<libc::timespec> for Timespec {
        fn from(ts: libc::timespec) -> Self {
            let ns = if ts.tv_nsec < 1_000_000_000 {
                ts.tv_nsec as u32
            } else {
                panic!("libc::timespec.tv_nsec out of range: {}", ts.tv_nsec);
            };
            Timespec {
                tv_sec: ts.tv_sec,
                tv_nsec: ns,
            }
        }
    }

    impl From<Timespec> for libc::timespec {
        fn from(ts: Timespec) -> Self {
            libc::timespec {
                tv_sec: ts.tv_sec,
                tv_nsec: ts.tv_nsec.try_into().unwrap(),
            }
        }
    }

    impl Timespec {
        const MAX: Timespec = Timespec {
            tv_sec: libc::time_t::MAX,
            tv_nsec: 999_999_999,
        };

        fn checked_add_duration(&self, dur: std::time::Duration) -> Option<Self> {
            let mut secs = dur
                .as_secs()
                .try_into()
                .ok()
                .and_then(|secs: i64| secs.checked_add(self.tv_sec))?;

            let mut nsecs = self.tv_nsec + dur.subsec_nanos();

            if nsecs >= 1_000_000_000 {
                nsecs -= 1_000_000_000;
                secs = secs.checked_add(1)?;
            }

            Some(Timespec {
                tv_sec: secs,
                tv_nsec: nsecs,
            })
        }
    }

    struct CondAttr<'a>(&'a mut MaybeUninit<libc::pthread_condattr_t>);

    impl CondAttr<'_> {
        /// Rust std sets this on linux, perhaps others. Does not set it on mac.
        #[cfg(not(target_os = "macos"))]
        fn set_monotonic_clock(&mut self) {
            let res = unsafe {
                libc::pthread_condattr_setclock(self.0.as_mut_ptr(), libc::CLOCK_MONOTONIC)
            };
            assert_eq!(res, 0);
        }

        fn set_pshared(&mut self) {
            let res = unsafe {
                libc::pthread_condattr_setpshared(self.0.as_mut_ptr(), libc::PTHREAD_PROCESS_SHARED)
            };
            assert_eq!(res, 0);
        }
    }

    impl Drop for CondAttr<'_> {
        fn drop(&mut self) {
            let res = unsafe { libc::pthread_condattr_destroy(self.0.as_mut_ptr()) };
            assert_eq!(res, 0);
        }
    }

    #[test]
    fn smoke_local() {
        let mut m = MaybeUninit::uninit();
        let m = Mutex::initialize_at(&mut m).unwrap();
        let m = &*m;
        let m = unsafe { Pin::new_unchecked(m) };
        assert!(matches!(m.lock(), Locked::Ok(())));
        m.unlock();
        assert!(matches!(m.try_lock(), Some(Locked::Ok(()))));
        m.unlock();

        // now we cannot send the same mutex to another thread, nor does our rust version have
        // scoped threads...

        let mut cv = MaybeUninit::uninit();
        let cv = Condvar::initialize_at(&mut cv).unwrap();
        let cv = &*cv;
        let cv = unsafe { Pin::new_unchecked(cv) };

        m.lock();
        let started_at = std::time::Instant::now();
        let woken_up = match cv.wait_timeout(std::time::Duration::from_millis(1), m) {
            Locked::Ok(woken_up) | Locked::PreviousOwnerDied(woken_up) => woken_up,
        };
        let elapsed = started_at.elapsed();
        m.unlock();

        // 50% is close enough
        assert!((elapsed.as_secs_f64() - 0.001).abs() < 0.0005);
        assert!(!woken_up);
    }

    #[test]
    fn smoke_threaded() {
        use std::sync::atomic::{AtomicUsize, Ordering::SeqCst};
        use std::sync::Arc;

        // we cannot do this through Box because Arc allocates and moves the contents
        let mut pair: Arc<MaybeUninit<(Mutex, Condvar)>> = Arc::new(MaybeUninit::uninit());
        // condition value updated only when lock is held
        let fake_condition = Arc::new(AtomicUsize::new(0));

        {
            let only = Arc::get_mut(&mut pair).unwrap();

            let mutex = unsafe {
                std::ptr::addr_of_mut!((*only.as_mut_ptr()).0)
                    .cast::<MaybeUninit<Mutex>>()
                    .as_mut()
                    .unwrap()
            };
            Mutex::initialize_at(mutex).unwrap();

            let cv = unsafe {
                std::ptr::addr_of_mut!((*only.as_mut_ptr()).1)
                    .cast::<MaybeUninit<Condvar>>()
                    .as_mut()
                    .unwrap()
            };
            Condvar::initialize_at(cv).unwrap();
        }

        // sadly also the Arc::assume_init is unstable

        let spawned = {
            let pair = pair.clone();
            let fake_condition = fake_condition.clone();
            move || {
                let pair = &*pair;
                // Safety: it was initialized
                let pair = unsafe { pair.assume_init_ref() };
                // Safety: Arc is StableDeref, nor can mutable refs be created right now at least
                let m = unsafe { std::pin::Pin::new_unchecked(&pair.0) };
                let cv = unsafe { std::pin::Pin::new_unchecked(&pair.1) };

                assert!(matches!(m.lock(), Locked::Ok(())));
                // using print is actually unsafe without a dropguard
                println!("hello, from other side");

                cv.notify_all();

                fake_condition.store(1, SeqCst);

                while fake_condition.load(SeqCst) < 1 {
                    println!("checking condition");
                    cv.wait(m);
                }

                m.unlock();
            }
        };

        let pair = &*pair;
        // Safety: see above
        let pair = unsafe { pair.assume_init_ref() };
        // Safety: see above
        let m = unsafe { std::pin::Pin::new_unchecked(&pair.0) };
        let cv = unsafe { std::pin::Pin::new_unchecked(&pair.1) };

        assert!(matches!(m.lock(), Locked::Ok(())));

        let jh = std::thread::spawn(spawned);

        println!("spawned while holding the lock, better sleep");

        std::thread::sleep(std::time::Duration::from_millis(100));

        while fake_condition.load(SeqCst) == 0 {
            cv.wait(m);
        }

        fake_condition.store(2, SeqCst);

        cv.notify_all();

        m.unlock();

        jh.join().unwrap();
    }
}

/// Used for miri
#[cfg(miri)]
mod parking_lot_shim {
    use std::{mem::MaybeUninit, pin::Pin};

    use super::Locked;

    use parking_lot::lock_api::RawMutex as _;
    use parking_lot::{Condvar as PLCondvar, RawMutex};

    pub struct Mutex(RawMutex, std::marker::PhantomPinned);

    impl Mutex {
        pub(super) fn initialize_at(
            place: &mut MaybeUninit<Mutex>,
        ) -> std::io::Result<Pin<&mut Mutex>> {
            let mutex = unsafe { std::ptr::addr_of_mut!((*place.as_mut_ptr()).0) };

            unsafe { mutex.write(RawMutex::INIT) };

            unsafe { Ok(Pin::new_unchecked(place.assume_init_mut())) }
        }

        pub(super) fn lock(self: Pin<&Self>) -> Locked {
            self.0.lock();
            Locked::Ok
        }

        pub(super) fn try_lock(self: Pin<&Self>) -> Option<Locked> {
            if self.0.try_lock() {
                Some(Locked::Ok)
            } else {
                None
            }
        }

        pub(super) fn unlock(self: Pin<&Self>) {
            unsafe { self.0.unlock() }
        }
    }

    impl Drop for Mutex {
        fn drop(&mut self) {
            // dunno how to make this not optimized out
        }
    }

    struct Condvar(PLCondvar, std::marker::PhantomPinned);

    impl Condvar {
        pub(super) fn initialize_at(
            place: &mut MaybeUninit<Condvar>,
        ) -> std::io::Result<Pin<&mut Condvar>> {
            let condvar = unsafe { std::ptr::addr_of_mut!((*place.as_mut_ptr()).0) };

            unsafe { condvar.write(PLCondvar::new()) };

            unsafe { Ok(Pin::new_unchecked(condvar.assume_init_mut())) }
        }

        pub(super) fn notify_one(self: Pin<&Self>) {
            self.0.notify_one();
        }

        pub(super) fn notify_all(self: Pin<&Self>) {
            self.0.notify_all();
        }

        pub(super) fn wait(self: Pin<&Self>, mutex: Pin<&Mutex>) {
            todo!("this cannot be implemented by shimming, need full reimpl of condvar because MutexGuard is expected")
        }

        pub(super) fn wait_timeout(self: Pin<&Self>, mutex: Pin<&Mutex>) -> bool {
            todo!("this cannot be implemented by shimming, need full reimpl of condvar because MutexGuard is expected")
        }
    }
}

#[cfg(miri)]
use parking_lot_shim as imp;

#[cfg(not(miri))]
use pthread as imp;

/// Pthread mutex which cannot be moved once initialized.
///
/// repr(c): this is shared between compilations.
#[repr(C)]
pub struct PinnedMutex<T> {
    mutex: imp::Mutex,
    inner: UnsafeCell<T>,
}

/// Because this is a shared mutex between processes, we cannot have the `get_mut` value.
impl<T> PinnedMutex<T> {
    pub fn initialize_at(
        place: &mut MaybeUninit<PinnedMutex<T>>,
        initial: T,
    ) -> std::io::Result<Pin<&mut PinnedMutex<T>>> {
        unsafe {
            // Safety: place is not null
            let inner = std::ptr::addr_of_mut!((*place.as_mut_ptr()).inner);
            inner.write(UnsafeCell::new(initial));
        }

        {
            let mutex = unsafe {
                // Safety: place is not null
                std::ptr::addr_of_mut!((*place.as_mut_ptr()).mutex)
            };

            let mutex: *mut MaybeUninit<imp::Mutex> = mutex.cast();

            // Safety:
            // - assume that addr_of_mut! gives aligned pointers
            // - dereferenceable???
            // - initialized MaybeUninit<_>
            // - &mut does not escape
            let mutex: &mut MaybeUninit<_> = unsafe { mutex.as_mut() }.expect("not null");
            imp::Mutex::initialize_at(mutex)?;
        }

        // again, symbolic pinning
        unsafe { Ok(Pin::new_unchecked(place.assume_init_mut())) }
    }

    // does this actually need to be pinned if only ever accessed through `&`?
    // no BUT we cannot really assume the "not accessed through &mut" so we leave the Pin
    // requirement on the methods.

    pub fn lock<'a>(
        self: Pin<&'a Self>,
    ) -> Result<MutexGuard<'a, T>, PreviousOwnerDied<MutexGuard<'a, T>>> {
        // Safety: pinning is structural
        let mutex = unsafe { Pin::new_unchecked(&self.mutex) };
        let res = mutex.lock();
        // Safety: have now the lock
        let guard = unsafe { MutexGuard::new(self) };
        match res {
            Locked::Ok(()) => Ok(guard),
            Locked::PreviousOwnerDied(()) => Err(PreviousOwnerDied(guard)),
        }
    }

    pub fn try_lock<'a>(
        self: Pin<&'a Self>,
    ) -> Result<MutexGuard<'a, T>, TryLockError<MutexGuard<'a, T>>> {
        // Safety: pinning is structural
        let mutex = unsafe { Pin::new_unchecked(&self.mutex) };

        let res = match mutex.try_lock() {
            Some(res) => res,
            None => return Err(TryLockError::WouldBlock),
        };

        // Safety: have now the lock
        let guard = unsafe { MutexGuard::new(self) };

        match res {
            Locked::Ok(()) => Ok(guard),
            Locked::PreviousOwnerDied(()) => Err(TryLockError::PreviousOwnerDied(guard)),
        }
    }
}

/// Helper trait to help working with especially `Result<_, TryLockError>`.
pub trait IntoGuard {
    type GuardType;
    fn into_guard(self) -> Self::GuardType;

    fn previous_owner_died(&self) -> bool;
}

impl<'a, T> IntoGuard for Result<MutexGuard<'a, T>, PreviousOwnerDied<MutexGuard<'a, T>>> {
    type GuardType = MutexGuard<'a, T>;

    fn into_guard(self) -> Self::GuardType {
        self.unwrap_or_else(|e| e.into_inner())
    }

    fn previous_owner_died(&self) -> bool {
        matches!(self, Err(_))
    }
}

impl<'a, T> IntoGuard for Result<MutexGuard<'a, T>, TryLockError<MutexGuard<'a, T>>> {
    type GuardType = Option<MutexGuard<'a, T>>;

    fn into_guard(self) -> Self::GuardType {
        match self {
            Ok(g) | Err(TryLockError::PreviousOwnerDied(g)) => Some(g),
            Err(TryLockError::WouldBlock) => None,
        }
    }

    fn previous_owner_died(&self) -> bool {
        matches!(self, Err(TryLockError::PreviousOwnerDied(_)))
    }
}

/// Signals that previous owner of the lock had died, similar to [`std::sync::PoisonError`].
///
/// Lock has now been repaired pthread-wise, but the protected data structure might be
/// inconsistent.
pub struct PreviousOwnerDied<T>(T);

impl<T> std::fmt::Debug for PreviousOwnerDied<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("PreviousOwnerDied")
    }
}

impl<T> PreviousOwnerDied<T> {
    pub fn into_inner(self) -> T {
        self.0
    }
}

/// Similar to [`PreviousOwnerDied`] but also covers the failure to lock the [`PinnedMutex`] with
/// `try_lock` operation.
pub enum TryLockError<T> {
    /// See [`PreviousOwnerDied`]
    PreviousOwnerDied(T),

    /// Try lock could not succeed immediately.
    WouldBlock,
}

impl<T> std::fmt::Debug for TryLockError<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::PreviousOwnerDied(_) => write!(f, "PreviousOwnerDied"),
            Self::WouldBlock => write!(f, "WouldBlock"),
        }
    }
}

pub struct MutexGuard<'a, T> {
    mutex: Pin<&'a PinnedMutex<T>>,
}

impl<'a, T> MutexGuard<'a, T> {
    unsafe fn new(mutex: Pin<&'a PinnedMutex<T>>) -> Self {
        MutexGuard { mutex }
    }
}

impl<T> Drop for MutexGuard<'_, T> {
    fn drop(&mut self) {
        // Safety: pinning is structural
        let proj = unsafe { Pin::new_unchecked(&self.mutex.mutex) };
        proj.unlock();
    }
}

impl<T> std::ops::Deref for MutexGuard<'_, T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        // Safety: mutex is locked
        unsafe { &*self.mutex.inner.get() }
    }
}

impl<T: Unpin> std::ops::DerefMut for MutexGuard<'_, T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        // Safety: this is a & -> &mut conversion but it's only available for Unpin types.
        unsafe { &mut *self.mutex.inner.get() }
    }
}

pub struct PinnedCondvar {
    inner: imp::Condvar,
}

impl PinnedCondvar {
    pub fn initialize_at(
        place: &mut MaybeUninit<PinnedCondvar>,
    ) -> std::io::Result<Pin<&mut PinnedCondvar>> {
        let inner = unsafe {
            std::ptr::addr_of_mut!((*place.as_mut_ptr()).inner)
                .cast::<MaybeUninit<imp::Condvar>>()
                .as_mut()
                .unwrap()
        };

        imp::Condvar::initialize_at(inner)?;

        unsafe { Ok(Pin::new_unchecked(place.assume_init_mut())) }
    }

    pub fn notify_one(self: Pin<&Self>) {
        // Safety: structural pinning
        let inner = unsafe { Pin::new_unchecked(&self.inner) };
        inner.notify_one();
    }

    pub fn notify_all(self: Pin<&Self>) {
        // Safety: structural pinning
        let inner = unsafe { Pin::new_unchecked(&self.inner) };
        inner.notify_all();
    }

    #[track_caller]
    pub fn wait<'a, T>(
        self: Pin<&Self>,
        guard: MutexGuard<'a, T>,
    ) -> Result<MutexGuard<'a, T>, PreviousOwnerDied<MutexGuard<'a, T>>> {
        // Safety: structural pinning
        let inner = unsafe { Pin::new_unchecked(&self.inner) };
        let mutex = unsafe { guard.mutex.map_unchecked(|x| &x.mutex) };
        match inner.wait(mutex) {
            Locked::Ok(()) => Ok(guard),
            Locked::PreviousOwnerDied(()) => Err(PreviousOwnerDied(guard)),
        }
    }

    #[track_caller]
    pub fn wait_timeout<'a, T>(
        self: Pin<&Self>,
        guard: MutexGuard<'a, T>,
        timeout: std::time::Duration,
    ) -> Result<(MutexGuard<'a, T>, bool), PreviousOwnerDied<(MutexGuard<'a, T>, bool)>> {
        // Safety: structural pinning
        let inner = unsafe { Pin::new_unchecked(&self.inner) };
        let mutex = unsafe { guard.mutex.map_unchecked(|x| &x.mutex) };
        match inner.wait_timeout(timeout, mutex) {
            Locked::Ok(woken_up) => Ok((guard, woken_up)),
            Locked::PreviousOwnerDied(woken_up) => Err(PreviousOwnerDied((guard, woken_up))),
        }
    }

    #[track_caller]
    pub fn wait_while<'a, F, T>(
        self: Pin<&Self>,
        mut guard: MutexGuard<'a, T>,
        mut condition: F,
    ) -> Result<MutexGuard<'a, T>, PreviousOwnerDied<MutexGuard<'a, T>>>
    where
        F: FnMut(&mut T) -> bool,
        T: Unpin,
    {
        let inner = unsafe { Pin::new_unchecked(&self.inner) };
        let mutex = unsafe { guard.mutex.map_unchecked(|x| &x.mutex) };

        let mut ret = Ok(());

        while condition(&mut *guard) {
            match inner.wait(mutex) {
                Locked::Ok(()) => {}
                Locked::PreviousOwnerDied(()) => ret = Err(PreviousOwnerDied(())),
            }
        }

        match ret {
            Ok(()) => Ok(guard),
            Err(PreviousOwnerDied(())) => Err(PreviousOwnerDied(guard)),
        }
    }

    #[track_caller]
    pub fn wait_timeout_while<'a, F, T>(
        self: Pin<&Self>,
        mut guard: MutexGuard<'a, T>,
        mut condition: F,
        dur: std::time::Duration,
    ) -> Result<(MutexGuard<'a, T>, bool), PreviousOwnerDied<(MutexGuard<'a, T>, bool)>>
    where
        F: FnMut(&mut T) -> bool,
        T: Unpin,
    {
        let inner = unsafe { Pin::new_unchecked(&self.inner) };
        let mutex = unsafe { guard.mutex.map_unchecked(|x| &x.mutex) };

        let mut prev_died = false;
        let start = std::time::Instant::now();

        let check_prev = |prev_died: bool, g: _, timeout: bool| match prev_died {
            false => Ok((g, timeout)),
            true => Err(PreviousOwnerDied((g, timeout))),
        };

        loop {
            if condition(&mut *guard) {
                return check_prev(prev_died, guard, false);
            }

            let timeout = match dur.checked_sub(start.elapsed()) {
                Some(timeout) => timeout,
                None => return check_prev(prev_died, guard, true),
            };

            match inner.wait_timeout(timeout, mutex) {
                Locked::Ok(_timeout) => {}
                Locked::PreviousOwnerDied(_timeout) => prev_died = true,
            }
        }
    }
}

pub struct EventfdSemaphore(std::os::unix::prelude::RawFd);

impl EventfdSemaphore {
    pub fn wait(&self) {
        let mut out = [0u8; 8];
        nix::unistd::read(self.0, &mut out).expect("reading from eventfd semaphore failed");
    }

    pub fn post(&self) {
        let one = 1u64.to_ne_bytes();
        nix::unistd::write(self.0, &one).expect("writing to eventfd semaphore failed");
    }
}

// FIXME: no drop impl for EventfdSemaphore, though we should have close when the drop actually happens.

impl FromRawFd for EventfdSemaphore {
    unsafe fn from_raw_fd(fd: std::os::unix::prelude::RawFd) -> Self {
        EventfdSemaphore(fd)
    }
}
