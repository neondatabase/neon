//! Simple utilities akin to what's in [`std::sync`] but designed to work with shared memory.

use std::mem::MaybeUninit;
use std::ptr::NonNull;

use nix::errno::Errno;

pub type RwLock<T> = lock_api::RwLock<PthreadRwLock, T>;
pub type RwLockReadGuard<'a, T> = lock_api::RwLockReadGuard<'a, PthreadRwLock, T>;
pub type RwLockWriteGuard<'a, T> = lock_api::RwLockWriteGuard<'a, PthreadRwLock, T>;
pub type ValueReadGuard<'a, T> = lock_api::MappedRwLockReadGuard<'a, PthreadRwLock, T>;
pub type ValueWriteGuard<'a, T> = lock_api::MappedRwLockWriteGuard<'a, PthreadRwLock, T>;

/// Shared memory read-write lock.
pub struct PthreadRwLock(Option<NonNull<libc::pthread_rwlock_t>>);

/// Simple macro that calls a function in the libc namespace and panics if return value is nonzero.
macro_rules! libc_checked {
    ($fn_name:ident ( $($arg:expr),* )) => {{
        let res = libc::$fn_name($($arg),*);
        if res != 0 {
            panic!("{} failed with {}", stringify!($fn_name), Errno::from_raw(res));
        }
    }};
}

impl PthreadRwLock {
    /// Creates a new `PthreadRwLock` on top of a pointer to a pthread rwlock.
    ///
    /// # Safety
    /// `lock` must be non-null. Every unsafe operation will panic in the event of an error.
    pub unsafe fn new(lock: *mut libc::pthread_rwlock_t) -> Self {
        unsafe {
            let mut attrs = MaybeUninit::uninit();
            libc_checked!(pthread_rwlockattr_init(attrs.as_mut_ptr()));
            libc_checked!(pthread_rwlockattr_setpshared(
                attrs.as_mut_ptr(),
                libc::PTHREAD_PROCESS_SHARED
            ));
            libc_checked!(pthread_rwlock_init(lock, attrs.as_mut_ptr()));
            // Safety: POSIX specifies that "any function affecting the attributes
            // object (including destruction) shall not affect any previously
            // initialized read-write locks".
            libc_checked!(pthread_rwlockattr_destroy(attrs.as_mut_ptr()));
            Self(Some(NonNull::new_unchecked(lock)))
        }
    }

    fn inner(&self) -> NonNull<libc::pthread_rwlock_t> {
        match self.0 {
            None => {
                panic!("PthreadRwLock constructed badly - something likely used RawRwLock::INIT")
            }
            Some(x) => x,
        }
    }
}

unsafe impl lock_api::RawRwLock for PthreadRwLock {
    type GuardMarker = lock_api::GuardSend;
    const INIT: Self = Self(None);

    fn try_lock_shared(&self) -> bool {
        unsafe {
            let res = libc::pthread_rwlock_tryrdlock(self.inner().as_ptr());
            match res {
                0 => true,
                libc::EAGAIN => false,
                _ => panic!(
                    "pthread_rwlock_tryrdlock failed with {}",
                    Errno::from_raw(res)
                ),
            }
        }
    }

    fn try_lock_exclusive(&self) -> bool {
        unsafe {
            let res = libc::pthread_rwlock_trywrlock(self.inner().as_ptr());
            match res {
                0 => true,
                libc::EAGAIN => false,
                _ => panic!("try_wrlock failed with {}", Errno::from_raw(res)),
            }
        }
    }

    fn lock_shared(&self) {
        unsafe {
            libc_checked!(pthread_rwlock_rdlock(self.inner().as_ptr()));
        }
    }

    fn lock_exclusive(&self) {
        unsafe {
            libc_checked!(pthread_rwlock_wrlock(self.inner().as_ptr()));
        }
    }

    unsafe fn unlock_exclusive(&self) {
        unsafe {
            libc_checked!(pthread_rwlock_unlock(self.inner().as_ptr()));
        }
    }

    unsafe fn unlock_shared(&self) {
        unsafe {
            libc_checked!(pthread_rwlock_unlock(self.inner().as_ptr()));
        }
    }
}
