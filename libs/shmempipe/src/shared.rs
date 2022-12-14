//! Simple shared pthread mutex.
//!
//! Monkeyd after rust std lib pthread mutex implementation, with the distinction that we will only
//! place mutexes on mmap'd memory, so they can only be accessed via Pin to avoid moving.
//!
//! Also, the pthread mutexes needed are shared between processes. We cannot use plain futexes,
//! because we want to support unixes (mac os), even though we run production on linux. Raw futexes
//! could be prototyped, since on linux pthreads work on top of them.
//!
//! Poisoning is not interesting for us either, because we cannot run code while child process is
//! being killed. We do however want robust pthread mutexes which know when their owner has died
//! while holding the lock.
//!
//! ## API
//!
//! There is an in-place initialization support which returns a `&mut T` as a symbolic gesture that
//! `MaybeUninit::assume_init_mut` has been invoked.

use std::{cell::UnsafeCell, mem::MaybeUninit, pin::Pin};

/// Return value for the robust pthread_mutex [`Mutex::lock`] and [`Mutex::try_lock`].
#[cfg_attr(test, derive(Debug))]
enum Locked {
    /// Mutex is now locked.
    Ok,

    /// The process which had previously locked the mutex has died while holding the lock.
    ///
    /// The lock has been made consistent with `pthread_mutex_consistent`, but any
    /// protected data structure has not been fixed.
    PreviousOwnerDied,
}

#[cfg(not(miri))]
mod pthread {
    use std::{mem::MaybeUninit, pin::Pin};

    use super::Locked;

    #[repr(transparent)]
    pub struct Mutex(libc::pthread_mutex_t, std::marker::PhantomPinned);

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

            unsafe {
                // Safety: it is now initialized, and we hope it doesn't move
                // Pin here is more "symbolic"
                //
                // TODO: does a ZST field need initialization? probably not because it's okay in
                // repr(transparent).
                Ok(Pin::new_unchecked(place.assume_init_mut()))
            }
        }

        pub(super) fn lock(self: std::pin::Pin<&Self>) -> Locked {
            let ptr = self.inner();
            let res = unsafe { libc::pthread_mutex_lock(ptr) };

            match res {
                0 => Locked::Ok,
                libc::EOWNERDEAD => {
                    // unsure if this is the right strategy, but it's not the worst?
                    self.make_consistent().expect("failed to repair after lock");
                    Locked::PreviousOwnerDied
                }
                other => Err(std::io::Error::from_raw_os_error(other)).expect("lock failed"),
            }
        }

        pub(super) fn try_lock(self: std::pin::Pin<&Self>) -> Option<Locked> {
            let ptr = self.inner();
            let res = unsafe { libc::pthread_mutex_trylock(ptr) };
            match res {
                0 => Some(Locked::Ok),
                libc::EBUSY => None,
                libc::EOWNERDEAD => {
                    // see Mutex::lock
                    self.make_consistent()
                        .expect("failed to repair after try_lock");
                    Some(Locked::PreviousOwnerDied)
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
                    Some(Locked::Ok | Locked::PreviousOwnerDied) => {
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

    #[test]
    fn smoke_local() {
        let mut m = MaybeUninit::uninit();
        let m = Mutex::initialize_at(&mut m).unwrap();
        let m = &*m;
        let m = unsafe { std::pin::Pin::new_unchecked(m) };
        assert!(matches!(m.lock(), Locked::Ok));
        m.unlock();
        assert!(matches!(m.try_lock(), Some(Locked::Ok)));
        m.unlock();

        // now we cannot send the same mutex to another thread, nor does our rust version have
        // scoped threads...
    }

    #[test]
    fn smoke_threaded() {
        use std::sync::Arc;

        // we cannot do this through Box because Arc allocates and moves the contents
        let mut m = Arc::new(MaybeUninit::uninit());

        {
            let only = Arc::get_mut(&mut m).unwrap();
            Mutex::initialize_at(only).unwrap();
        }

        // sadly also the Arc::assume_init is unstable

        let spawned = {
            let m = m.clone();
            move || {
                let inner = &*m;
                // Safety: it was initialized
                let m = unsafe { inner.assume_init_ref() };
                // Safety: Arc is StableDeref, nor can mutable refs be created right now at least
                let m = unsafe { std::pin::Pin::new_unchecked(m) };
                assert!(matches!(m.lock(), Locked::Ok));
                // using print is actually unsafe without a dropguard
                println!("hello, from other side");
                m.unlock();
            }
        };

        let inner = &*m;
        // Safety: see above
        let m = unsafe { inner.assume_init_ref() };
        // Safety: see above
        let m = unsafe { std::pin::Pin::new_unchecked(m) };

        assert!(matches!(m.lock(), Locked::Ok));

        let jh = std::thread::spawn(spawned);

        println!("spawned while holding the lock, better sleep");

        std::thread::sleep(std::time::Duration::from_millis(100));

        m.unlock();
        jh.join().unwrap();

        println!("joined");
    }
}

/// Used for miri
#[cfg(miri)]
mod parking_lot_shim {
    use std::{mem::MaybeUninit, pin::Pin};

    use super::Locked;

    use parking_lot::lock_api::RawMutex as _;
    use parking_lot::RawMutex;

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
            Locked::Ok => Ok(guard),
            Locked::PreviousOwnerDied => Err(PreviousOwnerDied(guard)),
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
            Locked::Ok => Ok(guard),
            Locked::PreviousOwnerDied => Err(TryLockError::PreviousOwnerDied(guard)),
        }
    }
}

/// Signals that previous owner of the lock had died, similar to [`std::sync::PoisonError`].
///
/// Lock has now been repaired pthread-wise, but the protected data structure might be
/// inconsistent.
pub struct PreviousOwnerDied<T>(T);

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

impl<T> PreviousOwnerDied<T> {
    pub fn into_inner(self) -> T {
        self.0
    }
}

pub struct MutexGuard<'a, T> {
    // unsure if we really need all two lifetimes, seems we do at MG::new(mutex)
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
