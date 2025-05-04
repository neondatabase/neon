use std::marker::PhantomData;
use std::ptr::NonNull;

use super::lock_and_version::AtomicLockAndVersion;

use crate::Value;
use crate::allocator::ArtAllocator;

pub(crate) const MAX_PREFIX_LEN: usize = 8;

enum NodeTag {
    Internal4,
    Internal16,
    Internal48,
    Internal256,
    Leaf4,
    Leaf16,
    Leaf48,
    Leaf256,
}

#[repr(C)]
struct NodeBase {
    tag: NodeTag,
    lock_and_version: AtomicLockAndVersion,
}

pub(crate) struct NodePtr<V> {
    ptr: *mut NodeBase,

    phantom_value: PhantomData<V>,
}

impl<V> std::fmt::Debug for NodePtr<V> {
    fn fmt(&self, fmt: &mut std::fmt::Formatter<'_>) -> Result<(), std::fmt::Error> {
        write!(fmt, "0x{}", self.ptr.addr())
    }
}

impl<V> Copy for NodePtr<V> {}
impl<V> Clone for NodePtr<V> {
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
    Leaf4(&'a NodeLeaf4<V>),
    Leaf16(&'a NodeLeaf16<V>),
    Leaf48(&'a NodeLeaf48<V>),
    Leaf256(&'a NodeLeaf256<V>),
}

enum NodeVariantMut<'a, V> {
    Internal4(&'a mut NodeInternal4<V>),
    Internal16(&'a mut NodeInternal16<V>),
    Internal48(&'a mut NodeInternal48<V>),
    Internal256(&'a mut NodeInternal256<V>),
    Leaf4(&'a mut NodeLeaf4<V>),
    Leaf16(&'a mut NodeLeaf16<V>),
    Leaf48(&'a mut NodeLeaf48<V>),
    Leaf256(&'a mut NodeLeaf256<V>),
}

pub(crate) enum ChildOrValuePtr<V> {
    Child(NodePtr<V>),
    Value(*const V),
}

#[repr(C)]
pub struct NodeInternal4<V> {
    tag: NodeTag,
    lock_and_version: AtomicLockAndVersion,

    prefix: [u8; MAX_PREFIX_LEN],
    prefix_len: u8,
    num_children: u8,

    child_keys: [u8; 4],
    child_ptrs: [NodePtr<V>; 4],
}

#[repr(C)]
pub struct NodeInternal16<V> {
    tag: NodeTag,
    lock_and_version: AtomicLockAndVersion,

    prefix: [u8; MAX_PREFIX_LEN],
    prefix_len: u8,

    num_children: u8,
    child_keys: [u8; 16],
    child_ptrs: [NodePtr<V>; 16],
}

const INVALID_CHILD_INDEX: u8 = u8::MAX;

#[repr(C)]
pub struct NodeInternal48<V> {
    tag: NodeTag,
    lock_and_version: AtomicLockAndVersion,

    prefix: [u8; MAX_PREFIX_LEN],
    prefix_len: u8,

    num_children: u8,
    child_indexes: [u8; 256],
    child_ptrs: [NodePtr<V>; 48],
}

#[repr(C)]
pub struct NodeInternal256<V> {
    tag: NodeTag,
    lock_and_version: AtomicLockAndVersion,

    prefix: [u8; MAX_PREFIX_LEN],
    prefix_len: u8,

    num_children: u16,
    child_ptrs: [NodePtr<V>; 256],
}

#[repr(C)]
pub struct NodeLeaf4<V> {
    tag: NodeTag,
    lock_and_version: AtomicLockAndVersion,

    prefix: [u8; MAX_PREFIX_LEN],
    prefix_len: u8,

    num_values: u8,
    child_keys: [u8; 4],
    child_values: [Option<V>; 4],
}

#[repr(C)]
pub struct NodeLeaf16<V> {
    tag: NodeTag,
    lock_and_version: AtomicLockAndVersion,

    prefix: [u8; MAX_PREFIX_LEN],
    prefix_len: u8,

    num_values: u8,
    child_keys: [u8; 16],
    child_values: [Option<V>; 16],
}

#[repr(C)]
pub struct NodeLeaf48<V> {
    tag: NodeTag,
    lock_and_version: AtomicLockAndVersion,

    prefix: [u8; MAX_PREFIX_LEN],
    prefix_len: u8,

    num_values: u8,
    child_indexes: [u8; 256],
    child_values: [Option<V>; 48],
}

#[repr(C)]
pub struct NodeLeaf256<V> {
    tag: NodeTag,
    lock_and_version: AtomicLockAndVersion,

    prefix: [u8; MAX_PREFIX_LEN],
    prefix_len: u8,

    num_values: u16,
    child_values: [Option<V>; 256],
}

impl<V> NodePtr<V> {
    pub(crate) fn is_leaf(&self) -> bool {
        match self.variant() {
            NodeVariant::Internal4(_) => false,
            NodeVariant::Internal16(_) => false,
            NodeVariant::Internal48(_) => false,
            NodeVariant::Internal256(_) => false,
            NodeVariant::Leaf4(_) => true,
            NodeVariant::Leaf16(_) => true,
            NodeVariant::Leaf48(_) => true,
            NodeVariant::Leaf256(_) => true,
        }
    }

    pub(crate) fn lockword(&self) -> &AtomicLockAndVersion {
        match self.variant() {
            NodeVariant::Internal4(n) => &n.lock_and_version,
            NodeVariant::Internal16(n) => &n.lock_and_version,
            NodeVariant::Internal48(n) => &n.lock_and_version,
            NodeVariant::Internal256(n) => &n.lock_and_version,
            NodeVariant::Leaf4(n) => &n.lock_and_version,
            NodeVariant::Leaf16(n) => &n.lock_and_version,
            NodeVariant::Leaf48(n) => &n.lock_and_version,
            NodeVariant::Leaf256(n) => &n.lock_and_version,
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
                NodeTag::Leaf4 => NodeVariant::Leaf4(
                    NonNull::new_unchecked(self.ptr.cast::<NodeLeaf4<V>>()).as_ref(),
                ),
                NodeTag::Leaf16 => NodeVariant::Leaf16(
                    NonNull::new_unchecked(self.ptr.cast::<NodeLeaf16<V>>()).as_ref(),
                ),
                NodeTag::Leaf48 => NodeVariant::Leaf48(
                    NonNull::new_unchecked(self.ptr.cast::<NodeLeaf48<V>>()).as_ref(),
                ),
                NodeTag::Leaf256 => NodeVariant::Leaf256(
                    NonNull::new_unchecked(self.ptr.cast::<NodeLeaf256<V>>()).as_ref(),
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
                NodeTag::Leaf4 => NodeVariantMut::Leaf4(
                    NonNull::new_unchecked(self.ptr.cast::<NodeLeaf4<V>>()).as_mut(),
                ),
                NodeTag::Leaf16 => NodeVariantMut::Leaf16(
                    NonNull::new_unchecked(self.ptr.cast::<NodeLeaf16<V>>()).as_mut(),
                ),
                NodeTag::Leaf48 => NodeVariantMut::Leaf48(
                    NonNull::new_unchecked(self.ptr.cast::<NodeLeaf48<V>>()).as_mut(),
                ),
                NodeTag::Leaf256 => NodeVariantMut::Leaf256(
                    NonNull::new_unchecked(self.ptr.cast::<NodeLeaf256<V>>()).as_mut(),
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
            NodeVariant::Leaf4(n) => n.get_prefix(),
            NodeVariant::Leaf16(n) => n.get_prefix(),
            NodeVariant::Leaf48(n) => n.get_prefix(),
            NodeVariant::Leaf256(n) => n.get_prefix(),
        }
    }

    pub(crate) fn is_full(&self) -> bool {
        match self.variant() {
            NodeVariant::Internal4(n) => n.is_full(),
            NodeVariant::Internal16(n) => n.is_full(),
            NodeVariant::Internal48(n) => n.is_full(),
            NodeVariant::Internal256(n) => n.is_full(),
            NodeVariant::Leaf4(n) => n.is_full(),
            NodeVariant::Leaf16(n) => n.is_full(),
            NodeVariant::Leaf48(n) => n.is_full(),
            NodeVariant::Leaf256(n) => n.is_full(),
        }
    }

    pub(crate) fn find_child_or_value(&self, key_byte: u8) -> Option<ChildOrValuePtr<V>> {
        match self.variant() {
            NodeVariant::Internal4(n) => n.find_child(key_byte).map(|c| ChildOrValuePtr::Child(c)),
            NodeVariant::Internal16(n) => n.find_child(key_byte).map(|c| ChildOrValuePtr::Child(c)),
            NodeVariant::Internal48(n) => n.find_child(key_byte).map(|c| ChildOrValuePtr::Child(c)),
            NodeVariant::Internal256(n) => {
                n.find_child(key_byte).map(|c| ChildOrValuePtr::Child(c))
            }
            NodeVariant::Leaf4(n) => n
                .get_leaf_value(key_byte)
                .map(|v| ChildOrValuePtr::Value(v)),
            NodeVariant::Leaf16(n) => n
                .get_leaf_value(key_byte)
                .map(|v| ChildOrValuePtr::Value(v)),
            NodeVariant::Leaf48(n) => n
                .get_leaf_value(key_byte)
                .map(|v| ChildOrValuePtr::Value(v)),
            NodeVariant::Leaf256(n) => n
                .get_leaf_value(key_byte)
                .map(|v| ChildOrValuePtr::Value(v)),
        }
    }

    pub(crate) fn truncate_prefix(&mut self, new_prefix_len: usize) {
        match self.variant_mut() {
            NodeVariantMut::Internal4(n) => n.truncate_prefix(new_prefix_len),
            NodeVariantMut::Internal16(n) => n.truncate_prefix(new_prefix_len),
            NodeVariantMut::Internal48(n) => n.truncate_prefix(new_prefix_len),
            NodeVariantMut::Internal256(n) => n.truncate_prefix(new_prefix_len),
            NodeVariantMut::Leaf4(n) => n.truncate_prefix(new_prefix_len),
            NodeVariantMut::Leaf16(n) => n.truncate_prefix(new_prefix_len),
            NodeVariantMut::Leaf48(n) => n.truncate_prefix(new_prefix_len),
            NodeVariantMut::Leaf256(n) => n.truncate_prefix(new_prefix_len),
        }
    }

    pub(crate) fn grow(&self, allocator: &impl ArtAllocator<V>) -> NodePtr<V> {
        match self.variant() {
            NodeVariant::Internal4(n) => n.grow(allocator),
            NodeVariant::Internal16(n) => n.grow(allocator),
            NodeVariant::Internal48(n) => n.grow(allocator),
            NodeVariant::Internal256(_) => panic!("cannot grow Internal256 node"),
            NodeVariant::Leaf4(n) => n.grow(allocator),
            NodeVariant::Leaf16(n) => n.grow(allocator),
            NodeVariant::Leaf48(n) => n.grow(allocator),
            NodeVariant::Leaf256(_) => panic!("cannot grow Leaf256 node"),
        }
    }

    pub(crate) fn insert_child(&mut self, key_byte: u8, child: NodePtr<V>) {
        match self.variant_mut() {
            NodeVariantMut::Internal4(n) => n.insert_child(key_byte, child),
            NodeVariantMut::Internal16(n) => n.insert_child(key_byte, child),
            NodeVariantMut::Internal48(n) => n.insert_child(key_byte, child),
            NodeVariantMut::Internal256(n) => n.insert_child(key_byte, child),
            NodeVariantMut::Leaf4(_)
            | NodeVariantMut::Leaf16(_)
            | NodeVariantMut::Leaf48(_)
            | NodeVariantMut::Leaf256(_) => panic!("insert_child called on leaf node"),
        }
    }

    pub(crate) fn replace_child(&mut self, key_byte: u8, replacement: NodePtr<V>) {
        match self.variant_mut() {
            NodeVariantMut::Internal4(n) => n.replace_child(key_byte, replacement),
            NodeVariantMut::Internal16(n) => n.replace_child(key_byte, replacement),
            NodeVariantMut::Internal48(n) => n.replace_child(key_byte, replacement),
            NodeVariantMut::Internal256(n) => n.replace_child(key_byte, replacement),
            NodeVariantMut::Leaf4(_)
            | NodeVariantMut::Leaf16(_)
            | NodeVariantMut::Leaf48(_)
            | NodeVariantMut::Leaf256(_) => panic!("replace_child called on leaf node"),
        }
    }

    pub(crate) fn insert_value(&mut self, key_byte: u8, value: V) {
        match self.variant_mut() {
            NodeVariantMut::Internal4(_)
            | NodeVariantMut::Internal16(_)
            | NodeVariantMut::Internal48(_)
            | NodeVariantMut::Internal256(_) => panic!("insert_value called on internal node"),
            NodeVariantMut::Leaf4(n) => n.insert_value(key_byte, value),
            NodeVariantMut::Leaf16(n) => n.insert_value(key_byte, value),
            NodeVariantMut::Leaf48(n) => n.insert_value(key_byte, value),
            NodeVariantMut::Leaf256(n) => n.insert_value(key_byte, value),
        }
    }

    pub(crate) fn delete_value(&mut self, key_byte: u8) {
        match self.variant_mut() {
            NodeVariantMut::Internal4(_)
            | NodeVariantMut::Internal16(_)
            | NodeVariantMut::Internal48(_)
            | NodeVariantMut::Internal256(_) => panic!("delete_value called on internal node"),
            NodeVariantMut::Leaf4(n) => n.delete_value(key_byte),
            NodeVariantMut::Leaf16(n) => n.delete_value(key_byte),
            NodeVariantMut::Leaf48(n) => n.delete_value(key_byte),
            NodeVariantMut::Leaf256(n) => n.delete_value(key_byte),
        }
    }

    pub(crate) fn deallocate(self, allocator: &impl ArtAllocator<V>) {
        match self.variant() {
            NodeVariant::Internal4(_) => allocator.dealloc_node_internal4(self.ptr.cast()),
            NodeVariant::Internal16(_) => allocator.dealloc_node_internal16(self.ptr.cast()),
            NodeVariant::Internal48(_) => allocator.dealloc_node_internal48(self.ptr.cast()),
            NodeVariant::Internal256(_) => allocator.dealloc_node_internal256(self.ptr.cast()),
            NodeVariant::Leaf4(_) => allocator.dealloc_node_leaf4(self.ptr.cast()),
            NodeVariant::Leaf16(_) => allocator.dealloc_node_leaf16(self.ptr.cast()),
            NodeVariant::Leaf48(_) => allocator.dealloc_node_leaf48(self.ptr.cast()),
            NodeVariant::Leaf256(_) => allocator.dealloc_node_leaf256(self.ptr.cast()),
        }
    }
}

pub fn new_root<V: Value>(allocator: &impl ArtAllocator<V>) -> NodePtr<V> {
    let ptr: *mut NodeInternal256<V> = allocator.alloc_node_internal256().cast();
    if ptr.is_null() {
        panic!("out of memory");
    }

    unsafe {
        *ptr = NodeInternal256::<V>::new();
    }

    ptr.into()
}

pub fn new_internal<V: Value>(prefix: &[u8], allocator: &impl ArtAllocator<V>) -> NodePtr<V> {
    let ptr: *mut NodeInternal4<V> = allocator.alloc_node_internal4().cast();
    if ptr.is_null() {
        panic!("out of memory");
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

    ptr.into()
}

pub fn new_leaf<V: Value>(prefix: &[u8], allocator: &impl ArtAllocator<V>) -> NodePtr<V> {
    let ptr: *mut NodeLeaf4<V> = allocator.alloc_node_leaf4().cast();
    if ptr.is_null() {
        panic!("out of memory");
    }
    let mut init = NodeLeaf4 {
        tag: NodeTag::Leaf4,
        lock_and_version: AtomicLockAndVersion::new(),

        prefix: [8; MAX_PREFIX_LEN],
        prefix_len: prefix.len() as u8,
        num_values: 0,

        child_keys: [0; 4],
        child_values: [const { None }; 4],
    };
    init.prefix[0..prefix.len()].copy_from_slice(prefix);
    unsafe { ptr.write(init) };

    ptr.into()
}

impl<V: Value> NodeInternal4<V> {
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

    fn find_child(&self, key: u8) -> Option<NodePtr<V>> {
        for i in 0..self.num_children as usize {
            if self.child_keys[i] == key {
                return Some(self.child_ptrs[i]);
            }
        }
        None
    }

    fn replace_child(&mut self, key_byte: u8, replacement: NodePtr<V>) {
        for i in 0..self.num_children as usize {
            if self.child_keys[i] == key_byte {
                self.child_ptrs[i] = replacement;
                return;
            }
        }
        panic!("could not re-find parent with key {}", key_byte);
    }

    fn is_full(&self) -> bool {
        self.num_children == 4
    }

    fn insert_child(&mut self, key_byte: u8, child: NodePtr<V>) {
        assert!(self.num_children < 4);

        let idx = self.num_children as usize;
        self.child_keys[idx] = key_byte;
        self.child_ptrs[idx] = child;
        self.num_children += 1;
    }

    fn grow(&self, allocator: &impl ArtAllocator<V>) -> NodePtr<V> {
        let ptr: *mut NodeInternal16<V> = allocator.alloc_node_internal16().cast();
        if ptr.is_null() {
            panic!("out of memory");
        }
        let mut init = NodeInternal16 {
            tag: NodeTag::Internal16,
            lock_and_version: AtomicLockAndVersion::new(),

            prefix: self.prefix.clone(),
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
        ptr.into()
    }
}

impl<V: Value> NodeInternal16<V> {
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
        for i in 0..self.num_children as usize {
            if self.child_keys[i] == key_byte {
                return Some(self.child_ptrs[i]);
            }
        }
        None
    }

    fn replace_child(&mut self, key_byte: u8, replacement: NodePtr<V>) {
        for i in 0..self.num_children as usize {
            if self.child_keys[i] == key_byte {
                self.child_ptrs[i] = replacement;
                return;
            }
        }
        panic!("could not re-find parent with key {}", key_byte);
    }

    fn is_full(&self) -> bool {
        self.num_children == 16
    }

    fn insert_child(&mut self, key_byte: u8, child: NodePtr<V>) {
        assert!(self.num_children < 16);

        let idx = self.num_children as usize;
        self.child_keys[idx] = key_byte;
        self.child_ptrs[idx] = child;
        self.num_children += 1;
    }

    fn grow(&self, allocator: &impl ArtAllocator<V>) -> NodePtr<V> {
        let ptr: *mut NodeInternal48<V> = allocator.alloc_node_internal48().cast();
        if ptr.is_null() {
            panic!("out of memory");
        }
        let mut init = NodeInternal48 {
            tag: NodeTag::Internal48,
            lock_and_version: AtomicLockAndVersion::new(),

            prefix: self.prefix.clone(),
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
        unsafe { ptr.write(init) };
        ptr.into()
    }
}

impl<V: Value> NodeInternal48<V> {
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

    fn replace_child(&mut self, key_byte: u8, replacement: NodePtr<V>) {
        let idx = self.child_indexes[key_byte as usize];
        if idx != INVALID_CHILD_INDEX {
            self.child_ptrs[idx as usize] = replacement
        } else {
            panic!("could not re-find parent with key {}", key_byte);
        }
    }

    fn is_full(&self) -> bool {
        self.num_children == 48
    }

    fn insert_child(&mut self, key_byte: u8, child: NodePtr<V>) {
        assert!(self.num_children < 48);
        assert!(self.child_indexes[key_byte as usize] == INVALID_CHILD_INDEX);
        let idx = self.num_children;
        self.child_indexes[key_byte as usize] = idx;
        self.child_ptrs[idx as usize] = child;
        self.num_children += 1;
    }

    fn grow(&self, allocator: &impl ArtAllocator<V>) -> NodePtr<V> {
        let ptr: *mut NodeInternal256<V> = allocator.alloc_node_internal256().cast();
        if ptr.is_null() {
            panic!("out of memory");
        }
        let mut init = NodeInternal256 {
            tag: NodeTag::Internal256,
            lock_and_version: AtomicLockAndVersion::new(),

            prefix: self.prefix.clone(),
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
        ptr.into()
    }
}

impl<V: Value> NodeInternal256<V> {
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
        let idx = key_byte as usize;
        if !self.child_ptrs[idx].is_null() {
            Some(self.child_ptrs[idx])
        } else {
            None
        }
    }

    fn replace_child(&mut self, key_byte: u8, replacement: NodePtr<V>) {
        let idx = key_byte as usize;
        if !self.child_ptrs[idx].is_null() {
            self.child_ptrs[idx] = replacement
        } else {
            panic!("could not re-find parent with key {}", key_byte);
        }
    }

    fn is_full(&self) -> bool {
        self.num_children == 256
    }

    fn insert_child(&mut self, key_byte: u8, child: NodePtr<V>) {
        assert!(self.num_children < 256);
        assert!(self.child_ptrs[key_byte as usize].is_null());
        self.child_ptrs[key_byte as usize] = child;
        self.num_children += 1;
    }
}

impl<V: Value> NodeLeaf4<V> {
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

    fn get_leaf_value<'a: 'b, 'b>(&'a self, key: u8) -> Option<&'b V> {
        for i in 0..self.num_values {
            if self.child_keys[i as usize] == key {
                assert!(self.child_values[i as usize].is_some());
                return self.child_values[i as usize].as_ref();
            }
        }
        None
    }
    fn is_full(&self) -> bool {
        self.num_values == 4
    }

    fn insert_value(&mut self, key_byte: u8, value: V) {
        assert!(self.num_values < 4);

        let idx = self.num_values as usize;
        self.child_keys[idx] = key_byte;
        self.child_values[idx] = Some(value);
        self.num_values += 1;
    }

    fn grow(&self, allocator: &impl ArtAllocator<V>) -> NodePtr<V> {
        let ptr: *mut NodeLeaf16<V> = allocator.alloc_node_leaf16();
        if ptr.is_null() {
            panic!("out of memory");
        }
        let mut init = NodeLeaf16 {
            tag: NodeTag::Leaf16,
            lock_and_version: AtomicLockAndVersion::new(),

            prefix: self.prefix.clone(),
            prefix_len: self.prefix_len,
            num_values: self.num_values,

            child_keys: [0; 16],
            child_values: [const { None }; 16],
        };
        for i in 0..self.num_values as usize {
            init.child_keys[i] = self.child_keys[i];
            init.child_values[i] = self.child_values[i].clone();
        }
        unsafe { ptr.write(init) };
        ptr.into()
    }

    fn delete_value(&mut self, key_byte: u8) {
        assert!(self.num_values <= 4);

        for i in 0..self.num_values as usize {
            if self.child_keys[i] == key_byte {
                assert!(self.child_values[i].is_some());
                if i < self.num_values as usize - 1 {
                    self.child_keys[i] = self.child_keys[self.num_values as usize - 1];
                    self.child_values[i] = std::mem::replace(&mut self.child_values[self.num_values as usize - 1], None);
                }
                self.num_values -= 1;
                return;
            }
        }
        panic!("key to delete not found in leaf4 node");
    }
}

impl<V: Value> NodeLeaf16<V> {
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

    fn get_leaf_value(&self, key: u8) -> Option<&V> {
        for i in 0..self.num_values {
            if self.child_keys[i as usize] == key {
                assert!(self.child_values[i as usize].is_some());
                return self.child_values[i as usize].as_ref();
            }
        }
        None
    }
    fn is_full(&self) -> bool {
        self.num_values == 16
    }

    fn insert_value(&mut self, key_byte: u8, value: V) {
        assert!(self.num_values < 16);

        let idx = self.num_values as usize;
        self.child_keys[idx] = key_byte;
        self.child_values[idx] = Some(value);
        self.num_values += 1;
    }
    fn grow(&self, allocator: &impl ArtAllocator<V>) -> NodePtr<V> {
        let ptr: *mut NodeLeaf48<V> = allocator.alloc_node_leaf48().cast();
        if ptr.is_null() {
            panic!("out of memory");
        }
        let mut init = NodeLeaf48 {
            tag: NodeTag::Leaf48,
            lock_and_version: AtomicLockAndVersion::new(),

            prefix: self.prefix.clone(),
            prefix_len: self.prefix_len,
            num_values: self.num_values,

            child_indexes: [INVALID_CHILD_INDEX; 256],
            child_values: [const { None }; 48],
        };
        for i in 0..self.num_values {
            let idx = self.child_keys[i as usize];
            init.child_indexes[idx as usize] = i;
            init.child_values[i as usize] = self.child_values[i as usize].clone();
        }
        unsafe { ptr.write(init) };
        ptr.into()
    }

    fn delete_value(&mut self, key_byte: u8) {
        assert!(self.num_values <= 16);

        for i in 0..self.num_values as usize {
            if self.child_keys[i as usize] == key_byte {
                assert!(self.child_values[i as usize].is_some());
                if i < self.num_values as usize - 1 {
                    self.child_keys[i] = self.child_keys[self.num_values as usize - 1];
                    self.child_values[i] = std::mem::replace(&mut self.child_values[self.num_values as usize - 1], None);
                }
                self.num_values -= 1;
                return;
            }
        }
        panic!("key to delete not found in leaf16 node");
    }
}

impl<V: Value> NodeLeaf48<V> {
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

    fn get_leaf_value(&self, key: u8) -> Option<&V> {
        let idx = self.child_indexes[key as usize];
        if idx != INVALID_CHILD_INDEX {
            assert!(self.child_values[idx as usize].is_some());
            self.child_values[idx as usize].as_ref()
        } else {
            None
        }
    }
    fn is_full(&self) -> bool {
        self.num_values == 48
    }

    fn insert_value(&mut self, key_byte: u8, value: V) {
        assert!(self.num_values < 48);
        assert!(self.child_indexes[key_byte as usize] == INVALID_CHILD_INDEX);
        let idx = self.num_values;
        self.child_indexes[key_byte as usize] = idx;
        self.child_values[idx as usize] = Some(value);
        self.num_values += 1;
    }
    fn grow(&self, allocator: &impl ArtAllocator<V>) -> NodePtr<V> {
        let ptr: *mut NodeLeaf256<V> = allocator.alloc_node_leaf256();
        if ptr.is_null() {
            panic!("out of memory");
        }
        let mut init = NodeLeaf256 {
            tag: NodeTag::Leaf256,
            lock_and_version: AtomicLockAndVersion::new(),

            prefix: self.prefix.clone(),
            prefix_len: self.prefix_len,
            num_values: self.num_values as u16,

            child_values: [const { None }; 256],
        };
        for i in 0..256 {
            let idx = self.child_indexes[i];
            if idx != INVALID_CHILD_INDEX {
                init.child_values[i] = self.child_values[idx as usize].clone();
            }
        }
        unsafe { ptr.write(init) };
        ptr.into()
    }

    fn delete_value(&mut self, key_byte: u8) {
        assert!(self.num_values <= 48);

        let idx = self.child_indexes[key_byte as usize];
        if idx == INVALID_CHILD_INDEX {
            panic!("key to delete not found in leaf48 node");
        }
        self.child_indexes[key_byte as usize] = INVALID_CHILD_INDEX;
        self.num_values -= 1;

        if idx < self.num_values {
            // Move all existing values with higher indexes down one position
            for i in idx as usize ..self.num_values as usize {
                self.child_values[i] = std::mem::replace(&mut self.child_values[i + 1], None);
            }

            // Update all higher indexes
            for i in 0..256 {
                if self.child_indexes[i] != INVALID_CHILD_INDEX {
                    if self.child_indexes[i] > idx {
                        self.child_indexes[i] -= 1;
                    }
                    assert!(self.child_indexes[i] < self.num_values);
                }
            }
        }
    }
}

impl<V: Value> NodeLeaf256<V> {
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

    fn get_leaf_value(&self, key: u8) -> Option<&V> {
        let idx = key as usize;
        self.child_values[idx].as_ref()
    }
    fn is_full(&self) -> bool {
        self.num_values == 256
    }

    fn insert_value(&mut self, key_byte: u8, value: V) {
        assert!(self.num_values < 256);
        assert!(self.child_values[key_byte as usize].is_none());
        self.child_values[key_byte as usize] = Some(value);
        self.num_values += 1;
    }

    fn delete_value(&mut self, key_byte: u8) {
        if self.child_values[key_byte as usize].is_none() {
            panic!("key to delete not found in leaf256 node");
        }
        self.child_values[key_byte as usize] = None;
        self.num_values -= 1;
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

impl<V: Value> From<*mut NodeLeaf4<V>> for NodePtr<V> {
    fn from(val: *mut NodeLeaf4<V>) -> NodePtr<V> {
        NodePtr {
            ptr: val.cast(),
            phantom_value: PhantomData,
        }
    }
}
impl<V: Value> From<*mut NodeLeaf16<V>> for NodePtr<V> {
    fn from(val: *mut NodeLeaf16<V>) -> NodePtr<V> {
        NodePtr {
            ptr: val.cast(),
            phantom_value: PhantomData,
        }
    }
}

impl<V: Value> From<*mut NodeLeaf48<V>> for NodePtr<V> {
    fn from(val: *mut NodeLeaf48<V>) -> NodePtr<V> {
        NodePtr {
            ptr: val.cast(),
            phantom_value: PhantomData,
        }
    }
}

impl<V: Value> From<*mut NodeLeaf256<V>> for NodePtr<V> {
    fn from(val: *mut NodeLeaf256<V>) -> NodePtr<V> {
        NodePtr {
            ptr: val.cast(),
            phantom_value: PhantomData,
        }
    }
}
