use std::fmt::Debug;
use std::marker::PhantomData;

use super::lock_and_version::ResultOrRestart;
use super::node_ptr;
use super::node_ptr::ChildOrValuePtr;
use super::node_ptr::NodePtr;
use crate::EpochPin;
use crate::Value;
use crate::algorithm::lock_and_version::AtomicLockAndVersion;
use crate::allocator::ArtAllocator;

pub struct NodeRef<'e, V> {
    ptr: NodePtr<V>,

    phantom: PhantomData<&'e EpochPin<'e>>,
}

impl<'e, V> Debug for NodeRef<'e, V> {
    fn fmt(&self, fmt: &mut std::fmt::Formatter<'_>) -> Result<(), std::fmt::Error> {
        write!(fmt, "{:?}", self.ptr)
    }
}

impl<'e, V: Value> NodeRef<'e, V> {
    pub(crate) fn from_root_ptr(root_ptr: NodePtr<V>) -> NodeRef<'e, V> {
        NodeRef {
            ptr: root_ptr,
            phantom: PhantomData,
        }
    }

    pub(crate) fn read_lock_or_restart(&self) -> ResultOrRestart<ReadLockedNodeRef<'e, V>> {
        let version = self.lockword().read_lock_or_restart()?;
        Ok(ReadLockedNodeRef {
            ptr: self.ptr,
            version,
            phantom: self.phantom,
        })
    }

    fn lockword(&self) -> &AtomicLockAndVersion {
        self.ptr.lockword()
    }
}

/// A reference to a node that has been optimistically read-locked. The functions re-check
/// the version after each read.
pub struct ReadLockedNodeRef<'e, V> {
    ptr: NodePtr<V>,
    version: u64,

    phantom: PhantomData<&'e EpochPin<'e>>,
}

pub(crate) enum ChildOrValue<'e, V> {
    Child(NodeRef<'e, V>),
    Value(*const V),
}

impl<'e, V: Value> ReadLockedNodeRef<'e, V> {
    pub(crate) fn is_full(&self) -> bool {
        self.ptr.is_full()
    }

    pub(crate) fn get_prefix(&self) -> &[u8] {
        self.ptr.get_prefix()
    }

    /// Note: because we're only holding a read lock, the prefix can change concurrently.
    /// You must be prepared to restart, if read_unlock() returns error later.
    ///
    /// Returns the length of the prefix, or None if it's not a match
    pub(crate) fn prefix_matches(&self, key: &[u8]) -> Option<usize> {
        self.ptr.prefix_matches(key)
    }

    pub(crate) fn find_child_or_value_or_restart(
        &self,
        key_byte: u8,
    ) -> ResultOrRestart<Option<ChildOrValue<'e, V>>> {
        let child_or_value = self.ptr.find_child_or_value(key_byte);
        self.ptr.lockword().check_or_restart(self.version)?;

        match child_or_value {
            None => Ok(None),
            Some(ChildOrValuePtr::Value(vptr)) => Ok(Some(ChildOrValue::Value(vptr))),
            Some(ChildOrValuePtr::Child(child_ptr)) => Ok(Some(ChildOrValue::Child(NodeRef {
                ptr: child_ptr,
                phantom: self.phantom,
            }))),
        }
    }

    pub(crate) fn upgrade_to_write_lock_or_restart(
        self,
    ) -> ResultOrRestart<WriteLockedNodeRef<'e, V>> {
        self.ptr
            .lockword()
            .upgrade_to_write_lock_or_restart(self.version)?;

        Ok(WriteLockedNodeRef {
            ptr: self.ptr,
            phantom: self.phantom,
        })
    }

    pub(crate) fn read_unlock_or_restart(self) -> ResultOrRestart<()> {
        self.ptr.lockword().check_or_restart(self.version)?;
        Ok(())
    }
}

/// A reference to a node that has been optimistically read-locked. The functions re-check
/// the version after each read.
pub struct WriteLockedNodeRef<'e, V> {
    ptr: NodePtr<V>,
    phantom: PhantomData<&'e EpochPin<'e>>,
}

impl<'e, V: Value> WriteLockedNodeRef<'e, V> {
    pub(crate) fn is_leaf(&self) -> bool {
        self.ptr.is_leaf()
    }

    pub(crate) fn write_unlock(mut self) {
        self.ptr.lockword().write_unlock();
        self.ptr = NodePtr::null();
    }

    pub(crate) fn write_unlock_obsolete(mut self) {
        self.ptr.lockword().write_unlock_obsolete();
        self.ptr = NodePtr::null();
    }

    pub(crate) fn get_prefix(&self) -> &[u8] {
        self.ptr.get_prefix()
    }

    pub(crate) fn truncate_prefix(&mut self, new_prefix_len: usize) {
        self.ptr.truncate_prefix(new_prefix_len)
    }

    pub(crate) fn insert_child(&mut self, key_byte: u8, child: NodePtr<V>) {
        self.ptr.insert_child(key_byte, child)
    }

    pub(crate) fn insert_value(&mut self, key_byte: u8, value: V) {
        self.ptr.insert_value(key_byte, value)
    }

    pub(crate) fn grow(&self, allocator: &impl ArtAllocator<V>) -> NewNodeRef<V> {
        let new_node = self.ptr.grow(allocator);
        NewNodeRef { ptr: new_node }
    }

    pub(crate) fn as_ptr(&self) -> NodePtr<V> {
        self.ptr
    }

    pub(crate) fn replace_child(&mut self, key_byte: u8, replacement: NodePtr<V>) {
        self.ptr.replace_child(key_byte, replacement);
    }
}

impl<'e, V> Drop for WriteLockedNodeRef<'e, V> {
    fn drop(&mut self) {
        if !self.ptr.is_null() {
            self.ptr.lockword().write_unlock();
        }
    }
}

pub(crate) struct NewNodeRef<V> {
    ptr: NodePtr<V>,
}

impl<V: Value> NewNodeRef<V> {
    pub(crate) fn insert_child(&mut self, key_byte: u8, child: NodePtr<V>) {
        self.ptr.insert_child(key_byte, child)
    }

    pub(crate) fn insert_value(&mut self, key_byte: u8, value: V) {
        self.ptr.insert_value(key_byte, value)
    }

    pub(crate) fn into_ptr(self) -> NodePtr<V> {
        let ptr = self.ptr;
        ptr
    }
}

pub(crate) fn new_internal<V: Value>(
    prefix: &[u8],
    allocator: &impl ArtAllocator<V>,
) -> NewNodeRef<V> {
    NewNodeRef {
        ptr: node_ptr::new_internal(prefix, allocator),
    }
}

pub(crate) fn new_leaf<V: Value>(prefix: &[u8], allocator: &impl ArtAllocator<V>) -> NewNodeRef<V> {
    NewNodeRef {
        ptr: node_ptr::new_leaf(prefix, allocator),
    }
}
