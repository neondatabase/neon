//! Glue code to allow using the Rust shmem hash map implementation from C code
//!
//! For convience of adapting existing code, the interface provided somewhat resembles the dynahash
//! interface.
//!
//! NOTE: The caller is responsible for locking! The caller is expected to hold the PostgreSQL
//! LWLock, 'lfc_lock', while accessing the hash table, in shared or exclusive mode as appropriate.

use std::ffi::c_void;
use std::marker::PhantomData;

use neon_shmem::hash::entry::Entry;
use neon_shmem::hash::{HashMapAccess, HashMapInit};
use neon_shmem::shmem::ShmemHandle;

/// NB: This must match the definition of BufferTag in Postgres C headers. We could use bindgen to
/// generate this from the C headers, but prefer to not introduce dependency on bindgen for now.
///
/// Note that there are no padding bytes. If the corresponding C struct has padding bytes, the C C
/// code must clear them.
#[derive(Clone, Debug, Hash, Eq, PartialEq)]
#[repr(C)]
pub struct FileCacheKey {
    pub _spc_id: u32,
    pub _db_id: u32,
    pub _rel_number: u32,
    pub _fork_num: u32,
    pub _block_num: u32,
}

/// Like with FileCacheKey, this must match the definition of FileCacheEntry in file_cache.c.  We
/// don't look at the contents here though, it's sufficent that the size and alignment matches.
#[derive(Clone, Debug, Default)]
#[repr(C)]
pub struct FileCacheEntry {
    pub _offset: u32,
    pub _access_count: u32,
    pub _prev: *mut FileCacheEntry,
    pub _next: *mut FileCacheEntry,
    pub _state: [u32; 8],
}

/// XXX: This could be just:
///
/// ```ignore
/// type FileCacheHashMapHandle = HashMapInit<'a, FileCacheKey, FileCacheEntry>
/// ```
///
/// but with that, cbindgen generates a broken typedef in the C header file which doesn't
/// compile. It apparently gets confused by the generics.
#[repr(transparent)]
pub struct FileCacheHashMapHandle<'a>(
    pub *mut c_void,
    PhantomData<HashMapInit<'a, FileCacheKey, FileCacheEntry>>,
);
impl<'a> From<Box<HashMapInit<'a, FileCacheKey, FileCacheEntry>>> for FileCacheHashMapHandle<'a> {
    fn from(x: Box<HashMapInit<'a, FileCacheKey, FileCacheEntry>>) -> Self {
        FileCacheHashMapHandle(Box::into_raw(x) as *mut c_void, PhantomData::default())
    }
}
impl<'a> From<FileCacheHashMapHandle<'a>> for Box<HashMapInit<'a, FileCacheKey, FileCacheEntry>> {
    fn from(x: FileCacheHashMapHandle) -> Self {
        unsafe { Box::from_raw(x.0.cast()) }
    }
}

/// XXX: same for this
#[repr(transparent)]
pub struct FileCacheHashMapAccess<'a>(
    pub *mut c_void,
    PhantomData<HashMapAccess<'a, FileCacheKey, FileCacheEntry>>,
);
impl<'a> From<Box<HashMapAccess<'a, FileCacheKey, FileCacheEntry>>> for FileCacheHashMapAccess<'a> {
    fn from(x: Box<HashMapAccess<'a, FileCacheKey, FileCacheEntry>>) -> Self {
        // Convert the Box into a raw mutable pointer to the HashMapAccess itself.
        // This transfers ownership of the HashMapAccess (and its contained ShmemHandle)
        // to the raw pointer. The C caller is now responsible for managing this memory.
        FileCacheHashMapAccess(Box::into_raw(x) as *mut c_void, PhantomData::default())
    }
}
impl<'a> FileCacheHashMapAccess<'a> {
    fn as_ref(self) -> &'a HashMapAccess<'a, FileCacheKey, FileCacheEntry> {
        let ptr: *mut HashMapAccess<'_, FileCacheKey, FileCacheEntry> = self.0.cast();
        unsafe { ptr.as_ref().unwrap() }
    }
    fn as_mut(self) -> &'a mut HashMapAccess<'a, FileCacheKey, FileCacheEntry> {
        let ptr: *mut HashMapAccess<'_, FileCacheKey, FileCacheEntry> = self.0.cast();
        unsafe { ptr.as_mut().unwrap() }
    }
}

/// Initialize the shared memory area at postmaster startup. The returned handle is inherited
/// by all the backend processes across fork()
#[unsafe(no_mangle)]
pub extern "C" fn bcomm_file_cache_shmem_init<'a>(
    initial_num_buckets: u32,
    max_num_buckets: u32,
) -> FileCacheHashMapHandle<'a> {
    let max_bytes = HashMapInit::<FileCacheKey, FileCacheEntry>::estimate_size(max_num_buckets);
    let shmem_handle =
        ShmemHandle::new("lfc mapping", 0, max_bytes).expect("shmem initialization failed");

    let handle = HashMapInit::<FileCacheKey, FileCacheEntry>::init_in_shmem(
        initial_num_buckets,
        shmem_handle,
    );

    Box::new(handle).into()
}

/// Initialize the access to the shared memory area in a backend process.
///
/// XXX: I'm not sure if this actually gets called in each process, or if the returned struct
/// is also inherited across fork(). It currently works either way but if this did more
/// initialization that needed to be done after fork(), then it would matter.
#[unsafe(no_mangle)]
pub extern "C" fn bcomm_file_cache_shmem_access<'a>(
    handle: FileCacheHashMapHandle<'a>,
) -> FileCacheHashMapAccess<'a> {
    let handle: Box<HashMapInit<'_, FileCacheKey, FileCacheEntry>> = handle.into();
    Box::new(handle.attach_writer()).into()
}

/// Return the current number of buckets in the hash table
#[unsafe(no_mangle)]
pub extern "C" fn bcomm_file_cache_get_num_buckets<'a>(
    map: FileCacheHashMapAccess<'static>,
) -> u32 {
    let map = map.as_ref();
    map.get_num_buckets().try_into().unwrap()
}

/// Look up the entry with given key and hash.
///
/// This is similar to dynahash's hash_search(... , HASH_FIND)
#[unsafe(no_mangle)]
pub extern "C" fn bcomm_file_cache_hash_find<'a>(
    map: FileCacheHashMapAccess<'static>,
    key: &FileCacheKey,
    hash: u64,
) -> Option<&'static FileCacheEntry> {
    let map = map.as_ref();
    map.get_with_hash(key, hash)
}

/// Look up the entry at given bucket position
///
/// This has no direct equivalent in the dynahash interface, but can be used to
/// iterate through all entries in the hash table.
#[unsafe(no_mangle)]
pub extern "C" fn bcomm_file_cache_hash_get_at_pos<'a>(
    map: FileCacheHashMapAccess<'static>,
    pos: u32,
) -> Option<&'static FileCacheEntry> {
    let map = map.as_ref();
    map.get_at_bucket(pos as usize).map(|(_k, v)| v)
}

/// Remove entry, given a pointer to the value.
///
/// This is equivalent to dynahash hash_search(entry->key, HASH_REMOVE), where 'entry'
/// is an entry you have previously looked up
#[unsafe(no_mangle)]
pub extern "C" fn bcomm_file_cache_hash_remove_entry<'a, 'b>(
    map: FileCacheHashMapAccess,
    entry: *mut FileCacheEntry,
) {
    let map = map.as_mut();
    let pos = map.get_bucket_for_value(entry);
    match map.entry_at_bucket(pos) {
        Some(e) => {
            e.remove();
        }
        None => {
            // todo: shouldn't happen, panic?
        }
    }
}

/// Compute the hash for given key
///
/// This is equivalent to dynahash get_hash_value() function. We use Rust's default hasher
/// for calculating the hash though.
#[unsafe(no_mangle)]
pub extern "C" fn bcomm_file_cache_get_hash_value<'a, 'b>(
    map: FileCacheHashMapAccess<'static>,
    key: &FileCacheKey,
) -> u64 {
    map.as_ref().get_hash_value(key)
}

/// Insert a new entry to the hash table
///
/// This is equivalent to dynahash hash_search(..., HASH_ENTER).
#[unsafe(no_mangle)]
pub extern "C" fn bcomm_file_cache_hash_enter<'a, 'b>(
    map: FileCacheHashMapAccess,
    key: &FileCacheKey,
    hash: u64,
    found: &mut bool,
) -> *mut FileCacheEntry {
    match map.as_mut().entry_with_hash(key.clone(), hash) {
        Entry::Occupied(mut e) => {
            *found = true;
            e.get_mut()
        }
        Entry::Vacant(e) => {
            *found = false;
            let initial_value = FileCacheEntry::default();
            e.insert(initial_value).expect("TODO: hash table full")
        }
    }
}

/// Get the key for a given entry, which must be present in the hash table.
///
/// Dynahash requires the key to be part of the "value" struct, so you can always
/// access the key with something like `entry->key`. The Rust implementation however
/// stores the key separately. This function extracts the separately stored key.
#[unsafe(no_mangle)]
pub extern "C" fn bcomm_file_cache_hash_get_key_for_entry<'a, 'b>(
    map: FileCacheHashMapAccess,
    entry: *const FileCacheEntry,
) -> Option<&FileCacheKey> {
    let map = map.as_ref();
    let pos = map.get_bucket_for_value(entry);
    map.get_at_bucket(pos as usize).map(|(k, _v)| k)
}

/// Remove all entries from the hash table
#[unsafe(no_mangle)]
pub extern "C" fn bcomm_file_cache_hash_reset<'a, 'b>(map: FileCacheHashMapAccess) {
    let map = map.as_mut();
    let num_buckets = map.get_num_buckets();
    for i in 0..num_buckets {
        if let Some(e) = map.entry_at_bucket(i) {
            e.remove();
        }
    }
}
