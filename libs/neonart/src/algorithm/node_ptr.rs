//! This file contains the implementations of all the different node variants.
//! These implementations use pointers, see node_ref.rs for slightly safer
//! wrappers that deal with references instead.
use std::marker::PhantomData;
use std::ptr::NonNull;

use super::lock_and_version::AtomicLockAndVersion;

use crate::Value;
use crate::allocator::ArtAllocator;
use crate::allocator::OutOfMemoryError;

pub(crate) const MAX_PREFIX_LEN: usize = 8;

enum NodeTag {
    Internal4,
    Internal16,
    Internal48,
    Internal256,
    Leaf,
}

#[repr(C)]
struct NodeBase {
    tag: NodeTag,
}

pub(crate) struct NodePtr<V> {
    ptr: *mut NodeBase,

    phantom_value: PhantomData<V>,
}

impl<V> PartialEq for NodePtr<V> {
    fn eq(&self, other: &NodePtr<V>) -> bool {
        self.ptr == other.ptr
    }
}

impl<V> std::fmt::Debug for NodePtr<V> {
    fn fmt(&self, fmt: &mut std::fmt::Formatter<'_>) -> Result<(), std::fmt::Error> {
        write!(fmt, "0x{}", self.ptr.addr())
    }
}

impl<V> Copy for NodePtr<V> {}
impl<V> Clone for NodePtr<V> {
    #[allow(clippy::non_canonical_clone_impl)]
    fn clone(&self) -> NodePtr<V> {
        NodePtr {
            ptr: self.ptr,
            phantom_value: PhantomData,
        }
    }
}

enum NodeVariant<'a, V> {
    Internal4(&'a NodeInternal4<V>),
    Internal16(&'a NodeInternal16<V>),
    Internal48(&'a NodeInternal48<V>),
    Internal256(&'a NodeInternal256<V>),
    Leaf(&'a NodeLeaf<V>),
}

enum NodeVariantMut<'a, V> {
    Internal4(&'a mut NodeInternal4<V>),
    Internal16(&'a mut NodeInternal16<V>),
    Internal48(&'a mut NodeInternal48<V>),
    Internal256(&'a mut NodeInternal256<V>),
    Leaf(&'a mut NodeLeaf<V>),
}

#[repr(C)]
pub struct NodeInternal4<V> {
    tag: NodeTag,
    prefix_len: u8,
    num_children: u8,

    child_keys: [u8; 4],

    lock_and_version: AtomicLockAndVersion,
    prefix: [u8; MAX_PREFIX_LEN],

    child_ptrs: [NodePtr<V>; 4],
}

#[repr(C)]
pub struct NodeInternal16<V> {
    tag: NodeTag,
    prefix_len: u8,
    num_children: u8,

    lock_and_version: AtomicLockAndVersion,
    prefix: [u8; MAX_PREFIX_LEN],
    child_keys: [u8; 16],
    child_ptrs: [NodePtr<V>; 16],
}

#[repr(C)]
pub struct NodeInternal48<V> {
    tag: NodeTag,
    prefix_len: u8,
    num_children: u8,

    lock_and_version: AtomicLockAndVersion,
    prefix: [u8; MAX_PREFIX_LEN],
    child_indexes: [u8; 256],
    child_ptrs: [NodePtr<V>; 48],
}
const INVALID_CHILD_INDEX: u8 = u8::MAX;

#[repr(C)]
pub struct NodeInternal256<V> {
    tag: NodeTag,
    prefix_len: u8,
    num_children: u16,

    lock_and_version: AtomicLockAndVersion,
    prefix: [u8; MAX_PREFIX_LEN],

    child_ptrs: [NodePtr<V>; 256],
}

#[repr(C)]
pub struct NodeLeaf<V> {
    tag: NodeTag,
    prefix_len: u8,

    // TODO: It's not clear if we need a full version on leaf nodes. I think a single bit
    // to indicate if the node is obsolete would be sufficient.
    lock_and_version: AtomicLockAndVersion,
    prefix: [u8; MAX_PREFIX_LEN],

    value: V,
}

impl<V> NodePtr<V> {
    pub(crate) fn is_leaf(&self) -> bool {
        match self.variant() {
            NodeVariant::Internal4(_) => false,
            NodeVariant::Internal16(_) => false,
            NodeVariant::Internal48(_) => false,
            NodeVariant::Internal256(_) => false,
            NodeVariant::Leaf(_) => true,
        }
    }

    pub(crate) fn lockword(&self) -> &AtomicLockAndVersion {
        match self.variant() {
            NodeVariant::Internal4(n) => &n.lock_and_version,
            NodeVariant::Internal16(n) => &n.lock_and_version,
            NodeVariant::Internal48(n) => &n.lock_and_version,
            NodeVariant::Internal256(n) => &n.lock_and_version,
            NodeVariant::Leaf(n) => &n.lock_and_version,
        }
    }

    pub(crate) fn is_null(&self) -> bool {
        self.ptr.is_null()
    }

    pub(crate) const fn null() -> NodePtr<V> {
        NodePtr {
            ptr: std::ptr::null_mut(),
            phantom_value: PhantomData,
        }
    }

    fn variant(&self) -> NodeVariant<V> {
        unsafe {
            match (*self.ptr).tag {
                NodeTag::Internal4 => NodeVariant::Internal4(
                    NonNull::new_unchecked(self.ptr.cast::<NodeInternal4<V>>()).as_ref(),
                ),
                NodeTag::Internal16 => NodeVariant::Internal16(
                    NonNull::new_unchecked(self.ptr.cast::<NodeInternal16<V>>()).as_ref(),
                ),
                NodeTag::Internal48 => NodeVariant::Internal48(
                    NonNull::new_unchecked(self.ptr.cast::<NodeInternal48<V>>()).as_ref(),
                ),
                NodeTag::Internal256 => NodeVariant::Internal256(
                    NonNull::new_unchecked(self.ptr.cast::<NodeInternal256<V>>()).as_ref(),
                ),
                NodeTag::Leaf => NodeVariant::Leaf(
                    NonNull::new_unchecked(self.ptr.cast::<NodeLeaf<V>>()).as_ref(),
                ),
            }
        }
    }

    fn variant_mut(&mut self) -> NodeVariantMut<V> {
        unsafe {
            match (*self.ptr).tag {
                NodeTag::Internal4 => NodeVariantMut::Internal4(
                    NonNull::new_unchecked(self.ptr.cast::<NodeInternal4<V>>()).as_mut(),
                ),
                NodeTag::Internal16 => NodeVariantMut::Internal16(
                    NonNull::new_unchecked(self.ptr.cast::<NodeInternal16<V>>()).as_mut(),
                ),
                NodeTag::Internal48 => NodeVariantMut::Internal48(
                    NonNull::new_unchecked(self.ptr.cast::<NodeInternal48<V>>()).as_mut(),
                ),
                NodeTag::Internal256 => NodeVariantMut::Internal256(
                    NonNull::new_unchecked(self.ptr.cast::<NodeInternal256<V>>()).as_mut(),
                ),
                NodeTag::Leaf => NodeVariantMut::Leaf(
                    NonNull::new_unchecked(self.ptr.cast::<NodeLeaf<V>>()).as_mut(),
                ),
            }
        }
    }
}

impl<V: Value> NodePtr<V> {
    pub(crate) fn prefix_matches(&self, key: &[u8]) -> Option<usize> {
        let node_prefix = self.get_prefix();
        assert!(node_prefix.len() <= key.len()); // because we only use fixed-size keys
        if &key[0..node_prefix.len()] != node_prefix {
            None
        } else {
            Some(node_prefix.len())
        }
    }

    pub(crate) fn get_prefix(&self) -> &[u8] {
        match self.variant() {
            NodeVariant::Internal4(n) => n.get_prefix(),
            NodeVariant::Internal16(n) => n.get_prefix(),
            NodeVariant::Internal48(n) => n.get_prefix(),
            NodeVariant::Internal256(n) => n.get_prefix(),
            NodeVariant::Leaf(n) => n.get_prefix(),
        }
    }

    pub(crate) fn is_full(&self) -> bool {
        match self.variant() {
            NodeVariant::Internal4(n) => n.is_full(),
            NodeVariant::Internal16(n) => n.is_full(),
            NodeVariant::Internal48(n) => n.is_full(),
            NodeVariant::Internal256(n) => n.is_full(),
            NodeVariant::Leaf(_) => panic!("is_full() called on leaf node"),
        }
    }

    pub(crate) fn num_children(&self) -> usize {
        match self.variant() {
            NodeVariant::Internal4(n) => n.num_children as usize,
            NodeVariant::Internal16(n) => n.num_children as usize,
            NodeVariant::Internal48(n) => n.num_children as usize,
            NodeVariant::Internal256(n) => n.num_children as usize,
            NodeVariant::Leaf(_) => panic!("is_full() called on leaf node"),
        }
    }

    pub(crate) fn can_shrink(&self) -> bool {
        match self.variant() {
            NodeVariant::Internal4(n) => n.can_shrink(),
            NodeVariant::Internal16(n) => n.can_shrink(),
            NodeVariant::Internal48(n) => n.can_shrink(),
            NodeVariant::Internal256(n) => n.can_shrink(),
            NodeVariant::Leaf(_) => panic!("can_shrink() called on leaf node"),
        }
    }

    pub(crate) fn find_child(&self, key_byte: u8) -> Option<NodePtr<V>> {
        match self.variant() {
            NodeVariant::Internal4(n) => n.find_child(key_byte),
            NodeVariant::Internal16(n) => n.find_child(key_byte),
            NodeVariant::Internal48(n) => n.find_child(key_byte),
            NodeVariant::Internal256(n) => n.find_child(key_byte),
            NodeVariant::Leaf(_) => panic!("find_child called on leaf node"),
        }
    }

    pub(crate) fn find_next_child(&self, key_byte: u8) -> Option<(u8, NodePtr<V>)> {
        match self.variant() {
            NodeVariant::Internal4(n) => n.find_next_child(key_byte),
            NodeVariant::Internal16(n) => n.find_next_child(key_byte),
            NodeVariant::Internal48(n) => n.find_next_child(key_byte),
            NodeVariant::Internal256(n) => n.find_next_child(key_byte),
            NodeVariant::Leaf(_) => panic!("find_next_child called on leaf node"),
        }
    }

    pub(crate) fn truncate_prefix(&mut self, new_prefix_len: usize) {
        match self.variant_mut() {
            NodeVariantMut::Internal4(n) => n.truncate_prefix(new_prefix_len),
            NodeVariantMut::Internal16(n) => n.truncate_prefix(new_prefix_len),
            NodeVariantMut::Internal48(n) => n.truncate_prefix(new_prefix_len),
            NodeVariantMut::Internal256(n) => n.truncate_prefix(new_prefix_len),
            NodeVariantMut::Leaf(n) => n.truncate_prefix(new_prefix_len),
        }
    }

    pub(crate) fn prepend_prefix(&mut self, prefix: &[u8], prefix_byte: u8) {
        match self.variant_mut() {
            NodeVariantMut::Internal4(n) => n.prepend_prefix(prefix, prefix_byte),
            NodeVariantMut::Internal16(n) => n.prepend_prefix(prefix, prefix_byte),
            NodeVariantMut::Internal48(n) => n.prepend_prefix(prefix, prefix_byte),
            NodeVariantMut::Internal256(n) => n.prepend_prefix(prefix, prefix_byte),
            NodeVariantMut::Leaf(n) => n.prepend_prefix(prefix, prefix_byte),
        }
    }

    pub(crate) fn grow(
        &self,
        allocator: &impl ArtAllocator<V>,
    ) -> Result<NodePtr<V>, OutOfMemoryError> {
        match self.variant() {
            NodeVariant::Internal4(n) => n.grow(allocator),
            NodeVariant::Internal16(n) => n.grow(allocator),
            NodeVariant::Internal48(n) => n.grow(allocator),
            NodeVariant::Internal256(_) => panic!("cannot grow Internal256 node"),
            NodeVariant::Leaf(_) => panic!("cannot grow Leaf node"),
        }
    }

    pub(crate) fn insert_child(&mut self, key_byte: u8, child: NodePtr<V>) {
        match self.variant_mut() {
            NodeVariantMut::Internal4(n) => n.insert_child(key_byte, child),
            NodeVariantMut::Internal16(n) => n.insert_child(key_byte, child),
            NodeVariantMut::Internal48(n) => n.insert_child(key_byte, child),
            NodeVariantMut::Internal256(n) => n.insert_child(key_byte, child),
            NodeVariantMut::Leaf(_) => panic!("insert_child called on leaf node"),
        }
    }

    pub(crate) fn replace_child(&mut self, key_byte: u8, replacement: NodePtr<V>) {
        match self.variant_mut() {
            NodeVariantMut::Internal4(n) => n.replace_child(key_byte, replacement),
            NodeVariantMut::Internal16(n) => n.replace_child(key_byte, replacement),
            NodeVariantMut::Internal48(n) => n.replace_child(key_byte, replacement),
            NodeVariantMut::Internal256(n) => n.replace_child(key_byte, replacement),
            NodeVariantMut::Leaf(_) => panic!("replace_child called on leaf node"),
        }
    }

    pub(crate) fn delete_child(&mut self, key_byte: u8) {
        match self.variant_mut() {
            NodeVariantMut::Internal4(n) => n.delete_child(key_byte),
            NodeVariantMut::Internal16(n) => n.delete_child(key_byte),
            NodeVariantMut::Internal48(n) => n.delete_child(key_byte),
            NodeVariantMut::Internal256(n) => n.delete_child(key_byte),
            NodeVariantMut::Leaf(_) => panic!("delete_child called on leaf node"),
        }
    }

    pub(crate) fn shrink(
        &self,
        allocator: &impl ArtAllocator<V>,
    ) -> Result<NodePtr<V>, OutOfMemoryError> {
        match self.variant() {
            NodeVariant::Internal4(_) => panic!("shrink called on internal4 node"),
            NodeVariant::Internal16(n) => n.shrink(allocator),
            NodeVariant::Internal48(n) => n.shrink(allocator),
            NodeVariant::Internal256(n) => n.shrink(allocator),
            NodeVariant::Leaf(_) => panic!("shrink called on leaf node"),
        }
    }

    pub(crate) fn get_leaf_value(&self) -> &V {
        match self.variant() {
            NodeVariant::Internal4(_)
            | NodeVariant::Internal16(_)
            | NodeVariant::Internal48(_)
            | NodeVariant::Internal256(_) => panic!("get_leaf_value called on internal node"),
            NodeVariant::Leaf(n) => n.get_leaf_value(),
        }
    }

    pub(crate) fn get_leaf_value_mut(&mut self) -> &mut V {
        match self.variant_mut() {
            NodeVariantMut::Internal4(_)
            | NodeVariantMut::Internal16(_)
            | NodeVariantMut::Internal48(_)
            | NodeVariantMut::Internal256(_) => panic!("get_leaf_value called on internal node"),
            NodeVariantMut::Leaf(n) => n.get_leaf_value_mut(),
        }
    }

    pub(crate) fn deallocate(self, allocator: &impl ArtAllocator<V>) {
        match self.variant() {
            NodeVariant::Internal4(_) => allocator.dealloc_node_internal4(self.ptr.cast()),
            NodeVariant::Internal16(_) => allocator.dealloc_node_internal16(self.ptr.cast()),
            NodeVariant::Internal48(_) => allocator.dealloc_node_internal48(self.ptr.cast()),
            NodeVariant::Internal256(_) => allocator.dealloc_node_internal256(self.ptr.cast()),
            NodeVariant::Leaf(_) => allocator.dealloc_node_leaf(self.ptr.cast()),
        }
    }
}

pub fn new_root<V: Value>(
    allocator: &impl ArtAllocator<V>,
) -> Result<NodePtr<V>, OutOfMemoryError> {
    let ptr: *mut NodeInternal256<V> = allocator.alloc_node_internal256().cast();
    if ptr.is_null() {
        return Err(OutOfMemoryError());
    }

    unsafe {
        *ptr = NodeInternal256::<V>::new();
    }

    Ok(ptr.into())
}

pub fn new_internal<V: Value>(
    prefix: &[u8],
    allocator: &impl ArtAllocator<V>,
) -> Result<NodePtr<V>, OutOfMemoryError> {
    let ptr: *mut NodeInternal4<V> = allocator.alloc_node_internal4().cast();
    if ptr.is_null() {
        return Err(OutOfMemoryError());
    }
    let mut init = NodeInternal4 {
        tag: NodeTag::Internal4,
        lock_and_version: AtomicLockAndVersion::new(),

        prefix: [8; MAX_PREFIX_LEN],
        prefix_len: prefix.len() as u8,
        num_children: 0,

        child_keys: [0; 4],
        child_ptrs: [const { NodePtr::null() }; 4],
    };
    init.prefix[0..prefix.len()].copy_from_slice(prefix);
    unsafe { ptr.write(init) };

    Ok(ptr.into())
}

pub fn new_leaf<V: Value>(
    prefix: &[u8],
    value: V,
    allocator: &impl ArtAllocator<V>,
) -> Result<NodePtr<V>, OutOfMemoryError> {
    let ptr: *mut NodeLeaf<V> = allocator.alloc_node_leaf().cast();
    if ptr.is_null() {
        return Err(OutOfMemoryError());
    }
    let mut init = NodeLeaf {
        tag: NodeTag::Leaf,
        lock_and_version: AtomicLockAndVersion::new(),

        prefix: [8; MAX_PREFIX_LEN],
        prefix_len: prefix.len() as u8,

        value,
    };
    init.prefix[0..prefix.len()].copy_from_slice(prefix);
    unsafe { ptr.write(init) };

    Ok(ptr.into())
}

impl<V: Value> NodeInternal4<V> {
    fn get_prefix(&self) -> &[u8] {
        &self.prefix[0..self.prefix_len as usize]
    }

    fn prepend_prefix(&mut self, prefix: &[u8], prefix_byte: u8) {
        assert!(1 + prefix.len() + self.prefix_len as usize <= MAX_PREFIX_LEN);
        let mut new = Vec::with_capacity(MAX_PREFIX_LEN);
        new.extend_from_slice(prefix);
        new.push(prefix_byte);
        new.extend_from_slice(&self.prefix[0..self.prefix_len as usize]);
        self.prefix[0..new.len()].copy_from_slice(&new);
        self.prefix_len = new.len() as u8;
    }

    fn truncate_prefix(&mut self, new_prefix_len: usize) {
        assert!(new_prefix_len < self.prefix_len as usize);
        let prefix = &mut self.prefix;
        let offset = self.prefix_len as usize - new_prefix_len;
        for i in 0..new_prefix_len {
            prefix[i] = prefix[i + offset];
        }
        self.prefix_len = new_prefix_len as u8;
    }

    fn find_child(&self, key: u8) -> Option<NodePtr<V>> {
        for i in 0..self.num_children as usize {
            if self.child_keys[i] == key {
                return Some(self.child_ptrs[i]);
            }
        }
        None
    }

    fn find_next_child(&self, min_key: u8) -> Option<(u8, NodePtr<V>)> {
        let mut found: Option<(usize, u8)> = None;
        for i in 0..self.num_children as usize {
            let this_key = self.child_keys[i];
            if this_key >= min_key {
                if let Some((_, found_key)) = found {
                    if this_key < found_key {
                        found = Some((i, this_key));
                    }
                } else {
                    found = Some((i, this_key));
                }
            }
        }
        if let Some((found_idx, found_key)) = found {
            Some((found_key, self.child_ptrs[found_idx]))
        } else {
            None
        }
    }

    fn replace_child(&mut self, key_byte: u8, replacement: NodePtr<V>) {
        for i in 0..self.num_children as usize {
            if self.child_keys[i] == key_byte {
                self.child_ptrs[i] = replacement;
                return;
            }
        }
        panic!("could not re-find parent with key {key_byte}");
    }

    fn delete_child(&mut self, key_byte: u8) {
        for i in 0..self.num_children as usize {
            if self.child_keys[i] == key_byte {
                self.num_children -= 1;
                for j in i..self.num_children as usize {
                    self.child_keys[j] = self.child_keys[j + 1];
                    self.child_ptrs[j] = self.child_ptrs[j + 1];
                }
                return;
            }
        }
        panic!("could not re-find parent with key {key_byte}");
    }

    fn is_full(&self) -> bool {
        self.num_children == 4
    }

    fn can_shrink(&self) -> bool {
        false
    }

    fn insert_child(&mut self, key_byte: u8, child: NodePtr<V>) {
        assert!(self.num_children < 4);

        let idx = self.num_children as usize;
        self.child_keys[idx] = key_byte;
        self.child_ptrs[idx] = child;
        self.num_children += 1;
    }

    fn grow(&self, allocator: &impl ArtAllocator<V>) -> Result<NodePtr<V>, OutOfMemoryError> {
        let ptr: *mut NodeInternal16<V> = allocator.alloc_node_internal16().cast();
        if ptr.is_null() {
            return Err(OutOfMemoryError());
        }
        let mut init = NodeInternal16 {
            tag: NodeTag::Internal16,
            lock_and_version: AtomicLockAndVersion::new(),

            prefix: self.prefix,
            prefix_len: self.prefix_len,
            num_children: self.num_children,

            child_keys: [0; 16],
            child_ptrs: [const { NodePtr::null() }; 16],
        };
        for i in 0..self.num_children as usize {
            init.child_keys[i] = self.child_keys[i];
            init.child_ptrs[i] = self.child_ptrs[i];
        }
        unsafe { ptr.write(init) };
        Ok(ptr.into())
    }
}

impl<V: Value> NodeInternal16<V> {
    fn get_prefix(&self) -> &[u8] {
        &self.prefix[0..self.prefix_len as usize]
    }

    fn prepend_prefix(&mut self, prefix: &[u8], prefix_byte: u8) {
        assert!(1 + prefix.len() + self.prefix_len as usize <= MAX_PREFIX_LEN);
        let mut new = Vec::with_capacity(MAX_PREFIX_LEN);
        new.extend_from_slice(prefix);
        new.push(prefix_byte);
        new.extend_from_slice(&self.prefix[0..self.prefix_len as usize]);
        self.prefix[0..new.len()].copy_from_slice(&new);
        self.prefix_len = new.len() as u8;
    }

    fn truncate_prefix(&mut self, new_prefix_len: usize) {
        assert!(new_prefix_len < self.prefix_len as usize);
        let prefix = &mut self.prefix;
        let offset = self.prefix_len as usize - new_prefix_len;
        for i in 0..new_prefix_len {
            prefix[i] = prefix[i + offset];
        }
        self.prefix_len = new_prefix_len as u8;
    }

    fn find_child(&self, key_byte: u8) -> Option<NodePtr<V>> {
        for i in 0..self.num_children as usize {
            if self.child_keys[i] == key_byte {
                return Some(self.child_ptrs[i]);
            }
        }
        None
    }

    fn find_next_child(&self, min_key: u8) -> Option<(u8, NodePtr<V>)> {
        let mut found: Option<(usize, u8)> = None;
        for i in 0..self.num_children as usize {
            let this_key = self.child_keys[i];
            if this_key >= min_key {
                if let Some((_, found_key)) = found {
                    if this_key < found_key {
                        found = Some((i, this_key));
                    }
                } else {
                    found = Some((i, this_key));
                }
            }
        }
        if let Some((found_idx, found_key)) = found {
            Some((found_key, self.child_ptrs[found_idx]))
        } else {
            None
        }
    }

    fn replace_child(&mut self, key_byte: u8, replacement: NodePtr<V>) {
        for i in 0..self.num_children as usize {
            if self.child_keys[i] == key_byte {
                self.child_ptrs[i] = replacement;
                return;
            }
        }
        panic!("could not re-find parent with key {key_byte}");
    }

    fn delete_child(&mut self, key_byte: u8) {
        for i in 0..self.num_children as usize {
            if self.child_keys[i] == key_byte {
                self.num_children -= 1;
                for j in i..self.num_children as usize {
                    self.child_keys[j] = self.child_keys[j + 1];
                    self.child_ptrs[j] = self.child_ptrs[j + 1];
                }
                return;
            }
        }
        panic!("could not re-find parent with key {key_byte}");
    }

    fn is_full(&self) -> bool {
        self.num_children == 16
    }

    fn can_shrink(&self) -> bool {
        self.num_children <= 4
    }

    fn insert_child(&mut self, key_byte: u8, child: NodePtr<V>) {
        assert!(self.num_children < 16);

        let idx = self.num_children as usize;
        self.child_keys[idx] = key_byte;
        self.child_ptrs[idx] = child;
        self.num_children += 1;
    }

    fn grow(&self, allocator: &impl ArtAllocator<V>) -> Result<NodePtr<V>, OutOfMemoryError> {
        let ptr: *mut NodeInternal48<V> = allocator.alloc_node_internal48().cast();
        if ptr.is_null() {
            return Err(OutOfMemoryError());
        }
        let mut init = NodeInternal48 {
            tag: NodeTag::Internal48,
            lock_and_version: AtomicLockAndVersion::new(),

            prefix: self.prefix,
            prefix_len: self.prefix_len,
            num_children: self.num_children,

            child_indexes: [INVALID_CHILD_INDEX; 256],
            child_ptrs: [const { NodePtr::null() }; 48],
        };
        for i in 0..self.num_children as usize {
            let idx = self.child_keys[i] as usize;
            init.child_indexes[idx] = i as u8;
            init.child_ptrs[i] = self.child_ptrs[i];
        }
        init.validate();
        unsafe { ptr.write(init) };
        Ok(ptr.into())
    }

    fn shrink(&self, allocator: &impl ArtAllocator<V>) -> Result<NodePtr<V>, OutOfMemoryError> {
        assert!(self.num_children <= 4);
        let ptr: *mut NodeInternal4<V> = allocator.alloc_node_internal4().cast();
        if ptr.is_null() {
            return Err(OutOfMemoryError());
        }
        let mut init = NodeInternal4 {
            tag: NodeTag::Internal4,
            lock_and_version: AtomicLockAndVersion::new(),

            prefix: self.prefix,
            prefix_len: self.prefix_len,
            num_children: self.num_children,

            child_keys: [0; 4],
            child_ptrs: [const { NodePtr::null() }; 4],
        };
        for i in 0..self.num_children as usize {
            init.child_keys[i] = self.child_keys[i];
            init.child_ptrs[i] = self.child_ptrs[i];
        }
        unsafe { ptr.write(init) };
        Ok(ptr.into())
    }
}

impl<V: Value> NodeInternal48<V> {
    fn validate(&self) {
        let mut shadow_indexes = std::collections::HashSet::new();
        let mut count = 0;
        for i in 0..256 {
            let idx = self.child_indexes[i];
            if idx != INVALID_CHILD_INDEX {
                assert!(
                    idx < self.num_children,
                    "i {} idx {}, num_children {}",
                    i,
                    idx,
                    self.num_children
                );
                assert!(!shadow_indexes.contains(&idx));
                shadow_indexes.insert(idx);
                count += 1;
            }
        }
        assert_eq!(count, self.num_children);
    }

    fn prepend_prefix(&mut self, prefix: &[u8], prefix_byte: u8) {
        assert!(1 + prefix.len() + self.prefix_len as usize <= MAX_PREFIX_LEN);
        let mut new = Vec::with_capacity(MAX_PREFIX_LEN);
        new.extend_from_slice(prefix);
        new.push(prefix_byte);
        new.extend_from_slice(&self.prefix[0..self.prefix_len as usize]);
        self.prefix[0..new.len()].copy_from_slice(&new);
        self.prefix_len = new.len() as u8;
    }

    fn get_prefix(&self) -> &[u8] {
        &self.prefix[0..self.prefix_len as usize]
    }

    fn truncate_prefix(&mut self, new_prefix_len: usize) {
        assert!(new_prefix_len < self.prefix_len as usize);
        let prefix = &mut self.prefix;
        let offset = self.prefix_len as usize - new_prefix_len;
        for i in 0..new_prefix_len {
            prefix[i] = prefix[i + offset];
        }
        self.prefix_len = new_prefix_len as u8;
    }

    fn find_child(&self, key_byte: u8) -> Option<NodePtr<V>> {
        let idx = self.child_indexes[key_byte as usize];
        if idx != INVALID_CHILD_INDEX {
            Some(self.child_ptrs[idx as usize])
        } else {
            None
        }
    }

    fn find_next_child(&self, min_key: u8) -> Option<(u8, NodePtr<V>)> {
        for key in min_key..=u8::MAX {
            let idx = self.child_indexes[key as usize];
            if idx != INVALID_CHILD_INDEX {
                return Some((key, self.child_ptrs[idx as usize]));
            }
        }
        None
    }

    fn replace_child(&mut self, key_byte: u8, replacement: NodePtr<V>) {
        let idx = self.child_indexes[key_byte as usize];
        if idx == INVALID_CHILD_INDEX {
            panic!("could not re-find parent with key {key_byte}");
        }
        self.child_ptrs[idx as usize] = replacement;
        self.validate();
    }

    fn delete_child(&mut self, key_byte: u8) {
        let idx = self.child_indexes[key_byte as usize] as usize;
        if idx == INVALID_CHILD_INDEX as usize {
            panic!("could not re-find parent with key {key_byte}");
        }

        // Compact the child_ptrs array
        let removed_idx = (self.num_children - 1) as usize;
        if idx != removed_idx {
            for i in 0..=u8::MAX as usize {
                if self.child_indexes[i] as usize == removed_idx {
                    self.child_indexes[i] = idx as u8;
                    self.child_ptrs[idx] = self.child_ptrs[removed_idx];

                    self.child_indexes[key_byte as usize] = INVALID_CHILD_INDEX;
                    self.num_children -= 1;
                    self.validate();
                    return;
                }
            }
            panic!("could not re-find last index {removed_idx} on Internal48 node");
        } else {
            self.child_indexes[key_byte as usize] = INVALID_CHILD_INDEX;
            self.num_children -= 1;
        }
    }

    fn is_full(&self) -> bool {
        self.num_children == 48
    }

    fn can_shrink(&self) -> bool {
        self.num_children <= 16
    }

    fn insert_child(&mut self, key_byte: u8, child: NodePtr<V>) {
        assert!(self.num_children < 48);
        assert!(self.child_indexes[key_byte as usize] == INVALID_CHILD_INDEX);
        let idx = self.num_children;
        self.child_indexes[key_byte as usize] = idx;
        self.child_ptrs[idx as usize] = child;
        self.num_children += 1;
        self.validate();
    }

    fn grow(&self, allocator: &impl ArtAllocator<V>) -> Result<NodePtr<V>, OutOfMemoryError> {
        let ptr: *mut NodeInternal256<V> = allocator.alloc_node_internal256().cast();
        if ptr.is_null() {
            return Err(OutOfMemoryError());
        }
        let mut init = NodeInternal256 {
            tag: NodeTag::Internal256,
            lock_and_version: AtomicLockAndVersion::new(),

            prefix: self.prefix,
            prefix_len: self.prefix_len,
            num_children: self.num_children as u16,

            child_ptrs: [const { NodePtr::null() }; 256],
        };
        for i in 0..256 {
            let idx = self.child_indexes[i];
            if idx != INVALID_CHILD_INDEX {
                init.child_ptrs[i] = self.child_ptrs[idx as usize];
            }
        }
        unsafe { ptr.write(init) };
        Ok(ptr.into())
    }

    fn shrink(&self, allocator: &impl ArtAllocator<V>) -> Result<NodePtr<V>, OutOfMemoryError> {
        assert!(self.num_children <= 16);
        let ptr: *mut NodeInternal16<V> = allocator.alloc_node_internal16().cast();
        if ptr.is_null() {
            return Err(OutOfMemoryError());
        }
        let mut init = NodeInternal16 {
            tag: NodeTag::Internal16,
            lock_and_version: AtomicLockAndVersion::new(),

            prefix: self.prefix,
            prefix_len: self.prefix_len,
            num_children: self.num_children,

            child_keys: [0; 16],
            child_ptrs: [const { NodePtr::null() }; 16],
        };
        let mut j = 0;
        for i in 0..256 {
            let idx = self.child_indexes[i];
            if idx != INVALID_CHILD_INDEX {
                init.child_keys[j] = i as u8;
                init.child_ptrs[j] = self.child_ptrs[idx as usize];
                j += 1;
            }
        }
        assert_eq!(j, self.num_children as usize);
        unsafe { ptr.write(init) };
        Ok(ptr.into())
    }
}

impl<V: Value> NodeInternal256<V> {
    fn get_prefix(&self) -> &[u8] {
        &self.prefix[0..self.prefix_len as usize]
    }

    fn prepend_prefix(&mut self, prefix: &[u8], prefix_byte: u8) {
        assert!(1 + prefix.len() + self.prefix_len as usize <= MAX_PREFIX_LEN);
        let mut new = Vec::with_capacity(MAX_PREFIX_LEN);
        new.extend_from_slice(prefix);
        new.push(prefix_byte);
        new.extend_from_slice(&self.prefix[0..self.prefix_len as usize]);
        self.prefix[0..new.len()].copy_from_slice(&new);
        self.prefix_len = new.len() as u8;
    }

    fn truncate_prefix(&mut self, new_prefix_len: usize) {
        assert!(new_prefix_len < self.prefix_len as usize);
        let prefix = &mut self.prefix;
        let offset = self.prefix_len as usize - new_prefix_len;
        for i in 0..new_prefix_len {
            prefix[i] = prefix[i + offset];
        }
        self.prefix_len = new_prefix_len as u8;
    }

    fn find_child(&self, key_byte: u8) -> Option<NodePtr<V>> {
        let idx = key_byte as usize;
        if !self.child_ptrs[idx].is_null() {
            Some(self.child_ptrs[idx])
        } else {
            None
        }
    }

    fn find_next_child(&self, min_key: u8) -> Option<(u8, NodePtr<V>)> {
        for key in min_key..=u8::MAX {
            if !self.child_ptrs[key as usize].is_null() {
                return Some((key, self.child_ptrs[key as usize]));
            }
        }
        None
    }

    fn replace_child(&mut self, key_byte: u8, replacement: NodePtr<V>) {
        let idx = key_byte as usize;
        if !self.child_ptrs[idx].is_null() {
            self.child_ptrs[idx] = replacement
        } else {
            panic!("could not re-find parent with key {key_byte}");
        }
    }

    fn delete_child(&mut self, key_byte: u8) {
        let idx = key_byte as usize;
        if self.child_ptrs[idx].is_null() {
            panic!("could not re-find parent with key {key_byte}");
        }
        self.num_children -= 1;
        self.child_ptrs[idx] = NodePtr::null();
    }

    fn is_full(&self) -> bool {
        self.num_children == 256
    }

    fn can_shrink(&self) -> bool {
        self.num_children <= 48
    }

    fn insert_child(&mut self, key_byte: u8, child: NodePtr<V>) {
        assert!(self.num_children < 256);
        assert!(self.child_ptrs[key_byte as usize].is_null());
        self.child_ptrs[key_byte as usize] = child;
        self.num_children += 1;
    }

    fn shrink(&self, allocator: &impl ArtAllocator<V>) -> Result<NodePtr<V>, OutOfMemoryError> {
        assert!(self.num_children <= 48);
        let ptr: *mut NodeInternal48<V> = allocator.alloc_node_internal48().cast();
        if ptr.is_null() {
            return Err(OutOfMemoryError());
        }
        let mut init = NodeInternal48 {
            tag: NodeTag::Internal48,
            lock_and_version: AtomicLockAndVersion::new(),

            prefix: self.prefix,
            prefix_len: self.prefix_len,
            num_children: self.num_children as u8,

            child_indexes: [INVALID_CHILD_INDEX; 256],
            child_ptrs: [const { NodePtr::null() }; 48],
        };
        let mut j = 0;
        for i in 0..256 {
            if !self.child_ptrs[i].is_null() {
                init.child_indexes[i] = j;
                init.child_ptrs[j as usize] = self.child_ptrs[i];
                j += 1;
            }
        }
        assert_eq!(j as u16, self.num_children);
        unsafe { ptr.write(init) };
        Ok(ptr.into())
    }
}

impl<V: Value> NodeLeaf<V> {
    fn get_prefix(&self) -> &[u8] {
        &self.prefix[0..self.prefix_len as usize]
    }

    fn prepend_prefix(&mut self, prefix: &[u8], prefix_byte: u8) {
        assert!(1 + prefix.len() + self.prefix_len as usize <= MAX_PREFIX_LEN);
        let mut new = Vec::with_capacity(MAX_PREFIX_LEN);
        new.extend_from_slice(prefix);
        new.push(prefix_byte);
        new.extend_from_slice(&self.prefix[0..self.prefix_len as usize]);
        self.prefix[0..new.len()].copy_from_slice(&new);
        self.prefix_len = new.len() as u8;
    }

    fn truncate_prefix(&mut self, new_prefix_len: usize) {
        assert!(new_prefix_len < self.prefix_len as usize);
        let prefix = &mut self.prefix;
        let offset = self.prefix_len as usize - new_prefix_len;
        for i in 0..new_prefix_len {
            prefix[i] = prefix[i + offset];
        }
        self.prefix_len = new_prefix_len as u8;
    }

    fn get_leaf_value<'a: 'b, 'b>(&'a self) -> &'b V {
        &self.value
    }

    fn get_leaf_value_mut<'a: 'b, 'b>(&'a mut self) -> &'b mut V {
        &mut self.value
    }
}

impl<V: Value> NodeInternal256<V> {
    pub(crate) fn new() -> NodeInternal256<V> {
        NodeInternal256 {
            tag: NodeTag::Internal256,
            lock_and_version: AtomicLockAndVersion::new(),

            prefix: [0; MAX_PREFIX_LEN],
            prefix_len: 0,
            num_children: 0,

            child_ptrs: [const { NodePtr::null() }; 256],
        }
    }
}

impl<V: Value> From<*mut NodeInternal4<V>> for NodePtr<V> {
    fn from(val: *mut NodeInternal4<V>) -> NodePtr<V> {
        NodePtr {
            ptr: val.cast(),
            phantom_value: PhantomData,
        }
    }
}
impl<V: Value> From<*mut NodeInternal16<V>> for NodePtr<V> {
    fn from(val: *mut NodeInternal16<V>) -> NodePtr<V> {
        NodePtr {
            ptr: val.cast(),
            phantom_value: PhantomData,
        }
    }
}

impl<V: Value> From<*mut NodeInternal48<V>> for NodePtr<V> {
    fn from(val: *mut NodeInternal48<V>) -> NodePtr<V> {
        NodePtr {
            ptr: val.cast(),
            phantom_value: PhantomData,
        }
    }
}

impl<V: Value> From<*mut NodeInternal256<V>> for NodePtr<V> {
    fn from(val: *mut NodeInternal256<V>) -> NodePtr<V> {
        NodePtr {
            ptr: val.cast(),
            phantom_value: PhantomData,
        }
    }
}

impl<V: Value> From<*mut NodeLeaf<V>> for NodePtr<V> {
    fn from(val: *mut NodeLeaf<V>) -> NodePtr<V> {
        NodePtr {
            ptr: val.cast(),
            phantom_value: PhantomData,
        }
    }
}
