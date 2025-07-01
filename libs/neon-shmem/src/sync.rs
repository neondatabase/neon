//! Simple utilities akin to what's in [`std::sync`] but designed to work with shared memory.

use std::mem::MaybeUninit;
use std::ptr::NonNull;
use std::cell::UnsafeCell;
use std::ops::{Deref, DerefMut};
use thiserror::Error;

/// Shared memory read-write lock.
struct RwLock<'a, T: ?Sized> {
	inner: &'a mut libc::pthread_rwlock_t,
	data: UnsafeCell<T>,
}

/// RAII guard for a read lock.
struct RwLockReadGuard<'a, 'b, T: ?Sized> {
	data: NonNull<T>,
	lock: &'a RwLock<'b, T>,
}

/// RAII guard for a write lock.
struct RwLockWriteGuard<'a, 'b, T: ?Sized> {
	lock: &'a RwLock<'b, T>,
}

// TODO(quantumish): Support poisoning errors?
#[derive(Error, Debug)]
enum RwLockError {
	#[error("deadlock detected")]
	Deadlock,
	#[error("max number of read locks exceeded")]
	MaxReadLocks,
	#[error("nonblocking operation would block")]
	WouldBlock,
}

unsafe impl<T: ?Sized + Send> Send for RwLock<'_, T> {}
unsafe impl<T: ?Sized + Send + Sync> Sync for RwLock<'_, T> {}

impl<'a, T> RwLock<'a, T> {
	fn new(lock: &'a mut MaybeUninit<libc::pthread_rwlock_t>, data: T) -> Self {
		unsafe {
			let mut attrs = MaybeUninit::uninit();
			// Ignoring return value here - only possible error is OOM.
			libc::pthread_rwlockattr_init(attrs.as_mut_ptr());
			libc::pthread_rwlockattr_setpshared(
				attrs.as_mut_ptr(),
				libc::PTHREAD_PROCESS_SHARED
			);
			// TODO(quantumish): worth making this function return Result?
			libc::pthread_rwlock_init(lock.as_mut_ptr(), attrs.as_mut_ptr());
			// Safety: POSIX specifies that "any function affecting the attributes
			// object (including destruction) shall not affect any previously
			// initialized read-write locks". 
			libc::pthread_rwlockattr_destroy(attrs.as_mut_ptr());
			Self {
				inner: lock.assume_init_mut(),
				data: data.into(),
			}
		}
	}

	fn read(&self) -> Result<RwLockReadGuard<'_, '_, T>, RwLockError> {		
		unsafe {
			let res = libc::pthread_rwlock_rdlock(self.inner as *const _ as *mut _);
			match res {
				0 => (),
				libc::EINVAL => panic!("failed to properly initialize lock"),
				libc::EDEADLK => return Err(RwLockError::Deadlock),
				libc::EAGAIN => return Err(RwLockError::MaxReadLocks),
				e => panic!("unknown error code returned: {e}")
			}
			Ok(RwLockReadGuard {
				data: NonNull::new_unchecked(self.data.get()),
				lock: self
			})
		}
	}

	fn try_read(&self) -> Result<RwLockReadGuard<'_, '_, T>, RwLockError> {
		unsafe {
			let res = libc::pthread_rwlock_tryrdlock(self.inner as *const _ as *mut _);
			match res {
				0 => (),
				libc::EINVAL => panic!("failed to properly initialize lock"),
				libc::EDEADLK => return Err(RwLockError::Deadlock),
				libc::EAGAIN => return Err(RwLockError::MaxReadLocks),
				libc::EBUSY => return Err(RwLockError::WouldBlock),
				e => panic!("unknown error code returned: {e}")
			}
			Ok(RwLockReadGuard {
				data: NonNull::new_unchecked(self.data.get()),
				lock: self
			})
		}
	}
	
	fn write(&self) -> Result<RwLockWriteGuard<'_, '_, T>, RwLockError> {
		unsafe {
			let res = libc::pthread_rwlock_wrlock(self.inner as *const _ as *mut _);
			match res {
				0 => (),
				libc::EINVAL => panic!("failed to properly initialize lock"),
				libc::EDEADLK => return Err(RwLockError::Deadlock),
				e => panic!("unknown error code returned: {e}")
			}
		}
		Ok(RwLockWriteGuard { lock: self })
	}

	fn try_write(&self) -> Result<RwLockWriteGuard<'_, '_, T>, RwLockError> {
		unsafe {
			let res = libc::pthread_rwlock_trywrlock(self.inner as *const _ as *mut _);
			match res {
				0 => (),
				libc::EINVAL => panic!("failed to properly initialize lock"),
				libc::EDEADLK => return Err(RwLockError::Deadlock),
				libc::EBUSY => return Err(RwLockError::WouldBlock),
				e => panic!("unknown error code returned: {e}")
			}
		}
		Ok(RwLockWriteGuard { lock: self })
	}
}

unsafe impl<T: ?Sized + Sync> Sync for RwLockReadGuard<'_, '_, T> {}
unsafe impl<T: ?Sized + Sync> Sync for RwLockWriteGuard<'_, '_, T> {}

impl<T: ?Sized> Deref for RwLockReadGuard<'_, '_, T> {
	type Target = T;
	
	fn deref(&self) -> &T {
		unsafe { self.data.as_ref() }
	}
}

impl<T: ?Sized> Deref for RwLockWriteGuard<'_, '_, T> {
	type Target = T;
	
	fn deref(&self) -> &T {
		unsafe { &*self.lock.data.get() }
	}
}

impl<T: ?Sized> DerefMut for RwLockWriteGuard<'_, '_, T> {
	fn deref_mut(&mut self) -> &mut T {
		unsafe { &mut *self.lock.data.get() }
	}
}

impl<T: ?Sized> Drop for RwLockReadGuard<'_, '_, T> {
	fn drop(&mut self) -> () {
		let res = unsafe { libc::pthread_rwlock_unlock(
			self.lock.inner as *const _ as *mut _
		) };
		debug_assert!(res == 0);
	}
}

impl<T: ?Sized> Drop for RwLockWriteGuard<'_, '_, T> {
	fn drop(&mut self) -> () {
		let res = unsafe { libc::pthread_rwlock_unlock(
			self.lock.inner as *const _ as *mut _
		) };
		debug_assert!(res == 0);
	}
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
use RwLockError::*;

	#[test]
	fn test_single_process() {
		let mut lock = MaybeUninit::uninit();
		let wrapper = RwLock::new(&mut lock, 0);
		let mut writer = wrapper.write().unwrap();
		assert!(matches!(wrapper.try_write(), Err(Deadlock | WouldBlock)));
		assert!(matches!(wrapper.try_read(), Err(Deadlock | WouldBlock)));
		*writer = 5;
		drop(writer);
		let reader = wrapper.read().unwrap();
		assert!(matches!(wrapper.try_write(), Err(Deadlock | WouldBlock)));
		assert!(matches!(wrapper.read(), Ok(_)));
		assert_eq!(*reader, 5);
		drop(reader);
		assert!(matches!(wrapper.try_write(), Ok(_)));
	}

	#[test]
	fn test_multi_thread() {
		let lock = Box::new(MaybeUninit::uninit());
		let wrapper = Arc::new(RwLock::new(Box::leak(lock), 0));
		let mut writer = wrapper.write().unwrap();
		let t1 = {
			let wrapper = wrapper.clone();
			std::thread::spawn(move || {
				let mut writer = wrapper.write().unwrap();
				*writer = 20;
			})
		};
		assert_eq!(*writer, 0);
		*writer = 10;
		assert_eq!(*writer, 10);
		drop(writer);
		t1.join().unwrap();
		let mut writer = wrapper.write().unwrap();
		assert_eq!(*writer, 20);
		drop(writer);
		let mut handles = vec![];
		for _ in 0..5 {
			handles.push({
				let wrapper = wrapper.clone();
				std::thread::spawn(move || {
					let reader = wrapper.read().unwrap();
					assert_eq!(*reader, 20);
				})
			});
		}
		for h in handles {
			h.join().unwrap();
		}
		let writer = wrapper.write().unwrap();
		assert_eq!(*writer, 20);
	}

	// // TODO(quantumish): Terrible time-based synchronization, fix me.
	// #[test]
	// fn test_multi_process() {
	// 	let max_size = 100;
    //     let init_struct = crate::shmem::ShmemHandle::new("test_multi_process", 0, max_size).unwrap();
	// 	let ptr = init_struct.data_ptr.as_ptr();
	// 	let lock: &mut _ = unsafe { ptr.add(
	// 		ptr.align_offset(std::mem::align_of::<MaybeUninit<libc::pthread_rwlock_t>>())
	// 	).cast::<MaybeUninit<libc::pthread_rwlock_t>>().as_mut().unwrap() } ;
	// 	let wrapper = RwLock::new(lock, 0);

    //     let fork_result = unsafe { nix::unistd::fork().unwrap() };

    //     if !fork_result.is_parent() {
	// 		let mut writer = wrapper.write().unwrap();
	// 		std::thread::sleep(std::time::Duration::from_secs(5));
	// 		*writer = 2;
    //     } else {
	// 		std::thread::sleep(std::time::Duration::from_secs(1));
	// 		assert!(matches!(wrapper.try_write(), Err(WouldBlock)));
	// 		std::thread::sleep(std::time::Duration::from_secs(10));
	// 		let writer = wrapper.try_write().unwrap();
	// 		assert_eq!(*writer, 2);
    //     }		
	// }
}
