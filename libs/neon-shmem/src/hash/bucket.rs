
use std::cell::UnsafeCell;
use std::mem::MaybeUninit;
use std::sync::atomic::{AtomicUsize, Ordering};

use atomic::Atomic;

#[derive(bytemuck::NoUninit, Clone, Copy, PartialEq, Eq)]
#[repr(transparent)]
pub(crate) struct BucketIdx(pub(super) u32);

const _: () = assert!(Atomic::<BucketIdx>::is_lock_free());

impl BucketIdx {
	/// Tag for next pointers in free entries.
	const NEXT_TAG: u32 = 0b00 << 30;
	/// Tag for marked next pointers in free entries.
	const MARK_TAG: u32 = 0b01 << 30;
	/// Tag for full entries.
	const FULL_TAG: u32 = 0b10 << 30;
	/// Reserved. Don't use me.
	const RSVD_TAG: u32 = 0b11 << 30;
	
	pub const INVALID: Self = Self(0x3FFFFFFF);
	pub const MAX: usize = Self::INVALID.0 as usize - 1;

	pub(super) fn is_marked(&self) -> bool {
		self.0 & Self::RSVD_TAG == Self::MARK_TAG
	}

	pub(super) fn as_marked(self) -> Self {
		Self((self.0 & Self::INVALID.0) | Self::MARK_TAG)
	}

	pub(super) fn get_unmarked(self) -> Self {
		Self(self.0 & Self::INVALID.0)
	}
	
	pub fn new(val: usize) -> Self {
		debug_assert!(val < Self::MAX);
		Self(val as u32)
	}

	pub fn new_full(val: usize) -> Self {
		debug_assert!(val < Self::MAX);
		Self(val as u32 | Self::FULL_TAG)
	}
	
	pub fn next_checked(&self) -> Option<usize> {
		if *self == Self::INVALID || self.is_marked() {
			None
		} else {
			Some(self.0 as usize)
		}
	}

	pub fn full_checked(&self) -> Option<usize> {
		if self.0 & Self::RSVD_TAG == Self::FULL_TAG {
			Some((self.0 & Self::INVALID.0) as usize) 
		} else {
			None
		}
	}
}

impl std::fmt::Debug for BucketIdx {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> Result<(), std::fmt::Error> {
		let idx = self.get_unmarked().0;
		write!(
			f, "BucketIdx(marked={}, idx={})",
			self.is_marked(),
			match *self {
				Self::INVALID => "INVALID".to_string(),
				_ => format!("{idx}")
			}
		)
	}
}

/// format storage unit within the hash table. Either empty or contains a key-value pair.
/// Always part of a chain of some kind (either a freelist if empty or a hash chain if full).
pub(crate) struct Bucket<V> {
    pub val: MaybeUninit<V>,
	pub next: Atomic<BucketIdx>,
}

impl<V> Bucket<V> {
	pub fn empty(next: BucketIdx) -> Self {		
		Self {
			val: MaybeUninit::uninit(),
			next: Atomic::new(next)
		}
	}

	pub fn full(val: V) -> Self {
		Self {
			val: MaybeUninit::new(val),
			next: Atomic::new(BucketIdx::INVALID)
		}
	}
	
	pub fn as_ref(&self) -> &V {
		unsafe { self.val.assume_init_ref() }
	}

	pub fn as_mut(&mut self) -> &mut V {
		unsafe { self.val.assume_init_mut() }
	}

	pub fn replace(&mut self, new_val: V) -> V {
		unsafe { std::mem::replace(self.val.assume_init_mut(), new_val) }
	}
}

pub(crate) struct BucketArray<'a, V> {
	/// Buckets containing values.
    pub(crate) buckets: &'a UnsafeCell<[Bucket<V>]>,
    /// Head of the freelist.
    pub(crate) free_head: Atomic<BucketIdx>,
    /// Maximum index of a bucket allowed to be allocated.
    pub(crate) alloc_limit: Atomic<BucketIdx>,
    /// The number of currently occupied buckets.
    pub(crate) buckets_in_use: AtomicUsize,
    // Unclear what the purpose of this is.
    pub(crate) _user_list_head: Atomic<BucketIdx>,
}

impl <'a, V> std::ops::Index<usize> for BucketArray<'a, V> {
	type Output = Bucket<V>;
		
	fn index(&self, index: usize) -> &Self::Output {
		let buckets: &[_] = unsafe { &*(self.buckets.get() as *mut _) };
		&buckets[index]
	}
}

impl <'a, V> std::ops::IndexMut<usize> for BucketArray<'a, V> {
	fn index_mut(&mut self, index: usize) -> &mut Self::Output {
		let buckets: &mut [_] = unsafe { &mut *(self.buckets.get() as *mut _) };
		&mut buckets[index]
	}
}

impl<'a, V> BucketArray<'a, V> {
	pub fn new(buckets: &'a UnsafeCell<[Bucket<V>]>) -> Self {		
		Self {
			buckets,
			free_head: Atomic::new(BucketIdx(0)),
			_user_list_head: Atomic::new(BucketIdx(0)),
			alloc_limit: Atomic::new(BucketIdx::INVALID),
			buckets_in_use: 0.into(),
		}
	}

	pub fn as_mut_ptr(&self) -> *mut Bucket<V> {
		unsafe { (&mut *self.buckets.get()).as_mut_ptr() }
	}

	pub fn get_mut(&self, index: usize) -> &mut Bucket<V> {
		let buckets: &mut [_] = unsafe { &mut *(self.buckets.get() as *mut _) };
		&mut buckets[index]
	}
	
	pub fn len(&self) -> usize {
		unsafe { (&*self.buckets.get()).len() }
	}
	
	pub fn dealloc_bucket(&self, pos: usize) -> V {
		loop {
			let free = self.free_head.load(Ordering::Relaxed);
			self[pos].next.store(free, Ordering::Relaxed);
			if self.free_head.compare_exchange_weak(
				free, BucketIdx::new(pos), Ordering::Relaxed, Ordering::Relaxed
			).is_ok() {
				self.buckets_in_use.fetch_sub(1, Ordering::Relaxed);
				return unsafe { self[pos].val.assume_init_read() };
			}
		}
	}

	#[allow(unused_assignments)]
	fn find_bucket(&self) -> (BucketIdx, BucketIdx) {
		let mut left_node = BucketIdx::INVALID;
		let mut right_node = BucketIdx::INVALID;
		let mut left_node_next = BucketIdx::INVALID;
		
		loop { 
			let mut t = BucketIdx::INVALID;
			let mut t_next = self.free_head.load(Ordering::Relaxed);
			let alloc_limit = self.alloc_limit.load(Ordering::Relaxed).next_checked();
			while t_next.is_marked() || t.next_checked()
				.map_or(true, |v| alloc_limit.map_or(false, |l| v > l))
			{
				if !t_next.is_marked() {
					left_node = t;
					left_node_next = t_next;
				}
				t = t_next.get_unmarked();
				if t == BucketIdx::INVALID { break }
				t_next = self[t.0 as usize].next.load(Ordering::Relaxed);
			}
			right_node = t;

			if left_node_next == right_node {
				if right_node != BucketIdx::INVALID && self[right_node.0 as usize]
					.next.load(Ordering::Relaxed).is_marked()
				{					
					continue;
				} else {
					return (left_node, right_node);
				}
			}

			let left_ref = if left_node != BucketIdx::INVALID {
				&self[left_node.0 as usize].next					
			} else { &self.free_head };
			
			if left_ref.compare_exchange_weak(
				left_node_next, right_node, Ordering::Relaxed, Ordering::Relaxed
			).is_ok() {
				if right_node != BucketIdx::INVALID && self[right_node.0 as usize]
					.next.load(Ordering::Relaxed).is_marked()
				{
					continue;
				} else {
					return (left_node, right_node);
				}
			}			
		}
	}

	#[allow(unused_assignments)]
    pub(crate) fn alloc_bucket(&self, value: V, key_pos: usize) -> Option<BucketIdx> {
		// println!("alloc()");
		let mut right_node_next = BucketIdx::INVALID;
		let mut left_idx = BucketIdx::INVALID;
		let mut right_idx = BucketIdx::INVALID;
		
		loop {
			(left_idx, right_idx) = self.find_bucket();
			if right_idx == BucketIdx::INVALID {
				return None;
			}
			
			let right = &self[right_idx.0 as usize];
			right_node_next = right.next.load(Ordering::Relaxed);
			if !right_node_next.is_marked() {
				if right.next.compare_exchange_weak(
					right_node_next, right_node_next.as_marked(),
					Ordering::Relaxed, Ordering::Relaxed
				).is_ok() {
					break;
				}
			}
		}

		let left_ref = if left_idx != BucketIdx::INVALID {
			&self[left_idx.0 as usize].next
		} else {
			&self.free_head
		};
		
		if left_ref.compare_exchange_weak(
			right_idx, right_node_next,
			Ordering::Relaxed, Ordering::Relaxed
		).is_err() {
			todo!()
		}

        self.buckets_in_use.fetch_add(1, Ordering::Relaxed);
		self[right_idx.0 as usize].next.store(
			BucketIdx::new_full(key_pos), Ordering::Relaxed
		);
		self.get_mut(right_idx.0 as usize).val.write(value);
		Some(right_idx)
    }

	pub fn clear(&mut self) {
		for i in 0..self.len() {
			self[i] = Bucket::empty(
				if i < self.len() - 1 {
					BucketIdx::new(i + 1)
				} else {
					BucketIdx::INVALID
				}				
			);
        }

		self.free_head.store(BucketIdx(0), Ordering::Relaxed);
        self.buckets_in_use.store(0, Ordering::Relaxed);
	}
}

