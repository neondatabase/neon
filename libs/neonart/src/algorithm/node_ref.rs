use std::fmt::Debug;
use std::marker::PhantomData;

use super::node_ptr;
use super::node_ptr::NodePtr;
use crate::EpochPin;
use crate::Value;
use crate::algorithm::lock_and_version::AtomicLockAndVersion;
use crate::algorithm::lock_and_version::ConcurrentUpdateError;
use crate::allocator::ArtAllocator;
use crate::allocator::OutOfMemoryError;

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

    pub(crate) fn read_lock_or_restart(
        &self,
    ) -> Result<ReadLockedNodeRef<'e, V>, ConcurrentUpdateError> {
        let version = self.lockword().read_lock_or_restart()?;
        Ok(ReadLockedNodeRef {
            ptr: self.ptr,
            version,
            phantom: self.phantom,
        })
    }

    pub(crate) fn write_lock_or_restart(
        &self,
    ) -> Result<WriteLockedNodeRef<'e, V>, ConcurrentUpdateError> {
        self.lockword().write_lock_or_restart()?;
        Ok(WriteLockedNodeRef {
            ptr: self.ptr,
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

impl<'e, V: Value> ReadLockedNodeRef<'e, V> {
    pub(crate) fn is_leaf(&self) -> bool {
        self.ptr.is_leaf()
    }

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

    pub(crate) fn find_child_or_restart(
        &self,
        key_byte: u8,
    ) -> Result<Option<NodeRef<'e, V>>, ConcurrentUpdateError> {
        let child_or_value = self.ptr.find_child(key_byte);
        self.ptr.lockword().check_or_restart(self.version)?;

        match child_or_value {
            None => Ok(None),
            Some(child_ptr) => Ok(Some(NodeRef {
                ptr: child_ptr,
                phantom: self.phantom,
            })),
        }
    }

    pub(crate) fn find_next_child_or_restart(
        &self,
        min_key_byte: u8,
    ) -> Result<Option<(u8, NodeRef<'e, V>)>, ConcurrentUpdateError> {
        let child_or_value = self.ptr.find_next_child(min_key_byte);
        self.ptr.lockword().check_or_restart(self.version)?;

        match child_or_value {
            None => Ok(None),
            Some((k, child_ptr)) => Ok(Some((
                k,
                NodeRef {
                    ptr: child_ptr,
                    phantom: self.phantom,
                },
            ))),
        }
    }

    pub(crate) fn get_leaf_value_ptr(&self) -> Result<*const V, ConcurrentUpdateError> {
        let result = self.ptr.get_leaf_value();
        self.ptr.lockword().check_or_restart(self.version)?;

        // Extend the lifetime.
        let result = std::ptr::from_ref(result);

        Ok(result)
    }

    pub(crate) fn upgrade_to_write_lock_or_restart(
        self,
    ) -> Result<WriteLockedNodeRef<'e, V>, ConcurrentUpdateError> {
        self.ptr
            .lockword()
            .upgrade_to_write_lock_or_restart(self.version)?;

        Ok(WriteLockedNodeRef {
            ptr: self.ptr,
            phantom: self.phantom,
        })
    }

    pub(crate) fn read_unlock_or_restart(self) -> Result<(), ConcurrentUpdateError> {
        self.ptr.lockword().check_or_restart(self.version)?;
        Ok(())
    }

    pub(crate) fn check_or_restart(&self) -> Result<(), ConcurrentUpdateError> {
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
    pub(crate) fn can_shrink(&self) -> bool {
        self.ptr.can_shrink()
    }

    pub(crate) fn num_children(&self) -> usize {
        self.ptr.num_children()
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

    pub(crate) fn prepend_prefix(&mut self, prefix: &[u8], prefix_byte: u8) {
        self.ptr.prepend_prefix(prefix, prefix_byte)
    }

    pub(crate) fn insert_child(&mut self, key_byte: u8, child: NodePtr<V>) {
        self.ptr.insert_child(key_byte, child)
    }

    pub(crate) fn get_leaf_value_mut(&mut self) -> &mut V {
        self.ptr.get_leaf_value_mut()
    }

    pub(crate) fn grow<'a, A>(
        &self,
        allocator: &'a A,
    ) -> Result<NewNodeRef<'a, V, A>, OutOfMemoryError>
    where
        A: ArtAllocator<V>,
    {
        let new_node = self.ptr.grow(allocator)?;
        Ok(NewNodeRef {
            ptr: new_node,
            allocator,
            extra_nodes: Vec::new(),
        })
    }

    pub(crate) fn shrink<'a, A>(
        &self,
        allocator: &'a A,
    ) -> Result<NewNodeRef<'a, V, A>, OutOfMemoryError>
    where
        A: ArtAllocator<V>,
    {
        let new_node = self.ptr.shrink(allocator)?;
        Ok(NewNodeRef {
            ptr: new_node,
            allocator,
            extra_nodes: Vec::new(),
        })
    }

    pub(crate) fn as_ptr(&self) -> NodePtr<V> {
        self.ptr
    }

    pub(crate) fn replace_child(&mut self, key_byte: u8, replacement: NodePtr<V>) {
        self.ptr.replace_child(key_byte, replacement);
    }

    pub(crate) fn delete_child(&mut self, key_byte: u8) {
        self.ptr.delete_child(key_byte);
    }

    pub(crate) fn find_remaining_child(&self) -> (u8, NodeRef<'e, V>) {
        assert_eq!(self.num_children(), 1);
        let child_or_value = self.ptr.find_next_child(0);

        match child_or_value {
            None => panic!("could not find only child in node"),
            Some((k, child_ptr)) => (
                k,
                NodeRef {
                    ptr: child_ptr,
                    phantom: self.phantom,
                },
            ),
        }
    }
}

impl<'e, V> Drop for WriteLockedNodeRef<'e, V> {
    fn drop(&mut self) {
        if !self.ptr.is_null() {
            self.ptr.lockword().write_unlock();
        }
    }
}

pub(crate) struct NewNodeRef<'a, V, A>
where
    V: Value,
    A: ArtAllocator<V>,
{
    ptr: NodePtr<V>,
    allocator: &'a A,

    extra_nodes: Vec<NodePtr<V>>,
}

impl<'a, V, A> NewNodeRef<'a, V, A>
where
    V: Value,
    A: ArtAllocator<V>,
{
    pub(crate) fn insert_old_child(&mut self, key_byte: u8, child: &WriteLockedNodeRef<V>) {
        self.ptr.insert_child(key_byte, child.as_ptr())
    }

    pub(crate) fn into_ptr(mut self) -> NodePtr<V> {
        let ptr = self.ptr;
        self.ptr = NodePtr::null();
        ptr
    }

    pub(crate) fn insert_new_child(&mut self, key_byte: u8, child: NewNodeRef<'a, V, A>) {
        let child_ptr = child.into_ptr();
        self.ptr.insert_child(key_byte, child_ptr);
        self.extra_nodes.push(child_ptr);
    }
}

impl<'a, V, A> Drop for NewNodeRef<'a, V, A>
where
    V: Value,
    A: ArtAllocator<V>,
{
    /// This drop implementation deallocates the newly allocated node, if into_ptr() was not called.
    fn drop(&mut self) {
        if !self.ptr.is_null() {
            self.ptr.deallocate(self.allocator);
            for p in self.extra_nodes.iter() {
                p.deallocate(self.allocator);
            }
        }
    }
}

pub(crate) fn new_internal<'a, V, A>(
    prefix: &[u8],
    allocator: &'a A,
) -> Result<NewNodeRef<'a, V, A>, OutOfMemoryError>
where
    V: Value,
    A: ArtAllocator<V>,
{
    Ok(NewNodeRef {
        ptr: node_ptr::new_internal(prefix, allocator)?,
        allocator,
        extra_nodes: Vec::new(),
    })
}

pub(crate) fn new_leaf<'a, V, A>(
    prefix: &[u8],
    value: V,
    allocator: &'a A,
) -> Result<NewNodeRef<'a, V, A>, OutOfMemoryError>
where
    V: Value,
    A: ArtAllocator<V>,
{
    Ok(NewNodeRef {
        ptr: node_ptr::new_leaf(prefix, value, allocator)?,
        allocator,
        extra_nodes: Vec::new(),
    })
}
