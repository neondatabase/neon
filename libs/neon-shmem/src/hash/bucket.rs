
use std::{mem::MaybeUninit, sync::atomic::{AtomicUsize, Ordering}};

use atomic::Atomic;

#[derive(bytemuck::NoUninit, Clone, Copy, PartialEq, Eq)]
#[repr(transparent)]
pub(crate) struct BucketIdx(pub(super) u32);

impl BucketIdx {
	const MARK_TAG: u32 = 0x80000000;
	pub const INVALID: Self = Self(0x7FFFFFFF);
	pub const RESERVED: Self = Self(0x7FFFFFFE);
	pub const MAX: usize = Self::RESERVED.0 as usize - 1;

	pub(super) fn is_marked(&self) -> bool {
		self.0 & Self::MARK_TAG != 0
	}

	pub(super) fn as_marked(self) -> Self {
		Self(self.0 | Self::MARK_TAG)
	}

	pub(super) fn get_unmarked(self) -> Self {
		Self(self.0 & !Self::MARK_TAG)
	}
	
	pub fn new(val: usize) -> Self {
		Self(val as u32)
	}
	
	pub fn pos_checked(&self) -> Option<usize> {
		if *self == Self::INVALID || self.is_marked() {
			None
		} else {
			Some(self.0 as usize)
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
				Self::RESERVED => "RESERVED".to_string(),
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

	// pub is_full
	
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
    pub(crate) buckets: &'a mut [Bucket<V>],
    /// Head of the freelist.
    pub(crate) free_head: Atomic<BucketIdx>,
    /// Maximum index of a bucket allowed to be allocated.
    pub(crate) alloc_limit: Atomic<BucketIdx>,
    /// The number of currently occupied buckets.
    pub(crate) buckets_in_use: AtomicUsize,
    // Unclear what the purpose of this is.
    pub(crate) _user_list_head: Atomic<BucketIdx>,
}

impl<'a, V> BucketArray<'a, V> {
	pub fn new(buckets: &'a mut [Bucket<V>]) -> Self {
		debug_assert!(Atomic::<BucketIdx>::is_lock_free());
		Self {
			buckets,
			free_head: Atomic::new(BucketIdx(0)),
			_user_list_head: Atomic::new(BucketIdx(0)),
			alloc_limit: Atomic::new(BucketIdx::INVALID),
			buckets_in_use: 0.into(),
		}
	}
	
	pub fn dealloc_bucket(&mut self, pos: usize) -> V {
		let bucket = &mut self.buckets[pos];
		let pos = BucketIdx::new(pos);
		loop {
			let free = self.free_head.load(Ordering::Relaxed);
			bucket.next.store(free, Ordering::Relaxed);
			if self.free_head.compare_exchange_weak(
				free, pos, Ordering::Relaxed, Ordering::Relaxed
			).is_ok() {
				self.buckets_in_use.fetch_sub(1, Ordering::Relaxed);
				return unsafe { bucket.val.assume_init_read() };
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
			let alloc_limit = self.alloc_limit.load(Ordering::Relaxed).pos_checked();
			while t_next.is_marked() || t.pos_checked()
				.map_or(true, |v| alloc_limit.map_or(false, |l| v > l))
			{
				if !t_next.is_marked() {
					left_node = t;
					left_node_next = t_next;
				}
				t = t_next.get_unmarked();
				if t == BucketIdx::INVALID { break }
				t_next = self.buckets[t.0 as usize].next.load(Ordering::Relaxed);
			}
			right_node = t;

			if left_node_next == right_node {
				if right_node != BucketIdx::INVALID && self.buckets[right_node.0 as usize]
					.next.load(Ordering::Relaxed).is_marked()
				{					
					continue;
				} else {
					return (left_node, right_node);
				}
			}

			let left_ref = if left_node != BucketIdx::INVALID {
				&self.buckets[left_node.0 as usize].next					
			} else { &self.free_head };
			
			if left_ref.compare_exchange_weak(
				left_node_next, right_node, Ordering::Relaxed, Ordering::Relaxed
			).is_ok() {
				if right_node != BucketIdx::INVALID && self.buckets[right_node.0 as usize]
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
    pub(crate) fn alloc_bucket(&mut self, value: V) -> Option<BucketIdx> {
		// println!("alloc()");
		let mut right_node_next = BucketIdx::INVALID;
		let mut left_idx = BucketIdx::INVALID;
		let mut right_idx = BucketIdx::INVALID;
		
		loop {
			(left_idx, right_idx) = self.find_bucket();
			if right_idx == BucketIdx::INVALID {
				return None;
			}
			
			let right = &self.buckets[right_idx.0 as usize];
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
			&self.buckets[left_idx.0 as usize].next
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
		self.buckets[right_idx.0 as usize].val.write(value);
		self.buckets[right_idx.0 as usize].next.store(
			BucketIdx::RESERVED, Ordering::Relaxed
		);
		Some(right_idx)
    }

	pub fn clear(&mut self) {
		for i in 0..self.buckets.len() {
			self.buckets[i] = Bucket::empty(
				if i < self.buckets.len() - 1 {
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

