//! Simple utilities akin to what's in [`std::sync`] but designed to work with shared memory.

use std::mem::MaybeUninit;
use std::ptr::NonNull;

use nix::errno::Errno;

pub type RwLock<T> = lock_api::RwLock<PthreadRwLock, T>;
pub(crate) type RwLockReadGuard<'a, T> = lock_api::RwLockReadGuard<'a, PthreadRwLock, T>;
pub type RwLockWriteGuard<'a, T> = lock_api::RwLockWriteGuard<'a, PthreadRwLock, T>;
pub type ValueReadGuard<'a, T> = lock_api::MappedRwLockReadGuard<'a, PthreadRwLock, T>;
pub type ValueWriteGuard<'a, T> = lock_api::MappedRwLockWriteGuard<'a, PthreadRwLock, T>;

/// Wrapper around a pointer to a [`libc::pthread_rwlock_t`].
///
/// `PthreadRwLock(None)` is an invalid state for this type. It only exists because the
/// [`lock_api::RawRwLock`] trait has a mandatory `INIT` const member to allow for static
/// initialization of the lock. Unfortunately, pthread seemingly does not support any way
/// to statically initialize a `pthread_rwlock_t` with `PTHREAD_PROCESS_SHARED` set. However,
/// `lock_api` allows manual construction and seemingly doesn't use `INIT` itself so for
/// now it's set to this invalid value to satisfy the trait constraints.
pub struct PthreadRwLock(Option<NonNull<libc::pthread_rwlock_t>>);

impl PthreadRwLock {
	pub fn new(lock: NonNull<libc::pthread_rwlock_t>) -> Self {
		unsafe {
			let mut attrs = MaybeUninit::uninit();
			// Ignoring return value here - only possible error is OOM.
			libc::pthread_rwlockattr_init(attrs.as_mut_ptr());
			libc::pthread_rwlockattr_setpshared(
				attrs.as_mut_ptr(),
				libc::PTHREAD_PROCESS_SHARED
			);
			// TODO(quantumish): worth making this function fallible?
			libc::pthread_rwlock_init(lock.as_ptr(), attrs.as_mut_ptr());
			// Safety: POSIX specifies that "any function affecting the attributes
			// object (including destruction) shall not affect any previously
			// initialized read-write locks". 
			libc::pthread_rwlockattr_destroy(attrs.as_mut_ptr());
			Self(Some(lock))
		}
	}
	
	fn inner(&self) -> NonNull<libc::pthread_rwlock_t> {
		self.0.unwrap_or_else(
			|| panic!("PthreadRwLock constructed badly - something likely used RawMutex::INIT")
		)
	}

	fn unlock(&self) {
		unsafe {
			let res = libc::pthread_rwlock_unlock(self.inner().as_ptr());
			assert!(res == 0, "unlock failed with {}", Errno::from_raw(res));
		}
	}
}

unsafe impl lock_api::RawRwLock for PthreadRwLock {
	type GuardMarker = lock_api::GuardSend;

	/// *DO NOT USE THIS.* See [`PthreadRwLock`] for the full explanation.
	const INIT: Self = Self(None);	
	
	fn lock_shared(&self) {
		unsafe {
			let res = libc::pthread_rwlock_rdlock(self.inner().as_ptr());
			assert!(res == 0, "rdlock failed with {}", Errno::from_raw(res));
		}
	}

	fn try_lock_shared(&self) -> bool {
		unsafe {
			let res = libc::pthread_rwlock_tryrdlock(self.inner().as_ptr());
			match res {
				0 => true,
				libc::EAGAIN => false,
				o => panic!("try_rdlock failed with {}", Errno::from_raw(o)),
			}
		}
	}

	fn lock_exclusive(&self) {
		unsafe {
			let res = libc::pthread_rwlock_wrlock(self.inner().as_ptr());
			assert!(res == 0, "wrlock failed with {}", Errno::from_raw(res));
		}
	}

	fn try_lock_exclusive(&self) -> bool {
		unsafe {
			let res = libc::pthread_rwlock_trywrlock(self.inner().as_ptr());
			match res {
				0 => true,
				libc::EAGAIN => false,
				o => panic!("try_wrlock failed with {}", Errno::from_raw(o)),
			}
		}
	}

	unsafe fn unlock_exclusive(&self) {
		self.unlock();
	}

	unsafe fn unlock_shared(&self) {
		self.unlock();
	}
}
