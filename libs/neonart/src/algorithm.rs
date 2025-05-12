mod lock_and_version;
pub(crate) mod node_ptr;
mod node_ref;

use std::vec::Vec;

use crate::algorithm::lock_and_version::ConcurrentUpdateError;
use crate::algorithm::node_ptr::MAX_PREFIX_LEN;
use crate::algorithm::node_ref::{NewNodeRef, NodeRef, ReadLockedNodeRef, WriteLockedNodeRef};
use crate::allocator::OutOfMemoryError;

use crate::GarbageQueueFullError;
use crate::TreeWriteGuard;
use crate::UpdateAction;
use crate::allocator::ArtAllocator;
use crate::epoch::EpochPin;
use crate::{Key, Value};

pub(crate) type RootPtr<V> = node_ptr::NodePtr<V>;

pub enum ArtError {
    ConcurrentUpdate, // need to retry
    OutOfMemory,
    GarbageQueueFull,
}

impl From<ConcurrentUpdateError> for ArtError {
    fn from(_: ConcurrentUpdateError) -> ArtError {
        ArtError::ConcurrentUpdate
    }
}

impl From<OutOfMemoryError> for ArtError {
    fn from(_: OutOfMemoryError) -> ArtError {
        ArtError::OutOfMemory
    }
}

impl From<GarbageQueueFullError> for ArtError {
    fn from(_: GarbageQueueFullError) -> ArtError {
        ArtError::GarbageQueueFull
    }
}

pub fn new_root<V: Value>(allocator: &impl ArtAllocator<V>) -> RootPtr<V> {
    node_ptr::new_root(allocator)
}

pub(crate) fn search<'e, K: Key, V: Value>(
    key: &K,
    root: RootPtr<V>,
    epoch_pin: &'e EpochPin,
) -> Option<&'e V> {
    loop {
        let root_ref = NodeRef::from_root_ptr(root);
        if let Ok(result) = lookup_recurse(key.as_bytes(), root_ref, None, epoch_pin) {
            break result;
        }
        // retry
    }
}

pub(crate) fn iter_next<'e, V: Value>(
    key: &[u8],
    root: RootPtr<V>,
    epoch_pin: &'e EpochPin,
) -> Option<(Vec<u8>, &'e V)> {
    loop {
        let mut path = Vec::new();
        let root_ref = NodeRef::from_root_ptr(root);

        match next_recurse(key, &mut path, root_ref, epoch_pin) {
            Ok(Some(v)) => {
                assert_eq!(path.len(), key.len());
                break Some((path, v));
            }
            Ok(None) => break None,
            Err(ConcurrentUpdateError()) => {
                // retry
                continue;
            }
        }
    }
}

pub(crate) fn update_fn<'e, 'g, K: Key, V: Value, A: ArtAllocator<V>, F>(
    key: &K,
    value_fn: F,
    root: RootPtr<V>,
    guard: &'g mut TreeWriteGuard<'e, K, V, A>,
) where
    F: FnOnce(Option<&V>) -> UpdateAction<V>,
{
    let value_fn_cell = std::cell::Cell::new(Some(value_fn));
    loop {
        let root_ref = NodeRef::from_root_ptr(root);
        let this_value_fn = |arg: Option<&V>| value_fn_cell.take().unwrap()(arg);
        let key_bytes = key.as_bytes();

        match update_recurse(
            key_bytes,
            this_value_fn,
            root_ref,
            None,
            None,
            guard,
            0,
            key_bytes,
        ) {
            Ok(()) => break,
            Err(ArtError::ConcurrentUpdate) => {
                continue; // retry
            }
            Err(ArtError::OutOfMemory) => {
                panic!("todo: OOM: try to GC, propagate to caller");
            }
            Err(ArtError::GarbageQueueFull) => {
                // FIXME: This can happen if someone is holding back the epoch. We should
                // wait for the epoch to advance
                panic!("todo: GC queue is full");
            }
        }
    }
}

pub(crate) fn dump_tree<'e, V: Value + std::fmt::Debug>(root: RootPtr<V>, epoch_pin: &'e EpochPin) {
    let root_ref = NodeRef::from_root_ptr(root);

    let _ = dump_recurse(&[], root_ref, &epoch_pin, 0);
}

// Error means you must retry.
//
// This corresponds to the 'lookupOpt' function in the paper
fn lookup_recurse<'e, V: Value>(
    key: &[u8],
    node: NodeRef<'e, V>,
    parent: Option<ReadLockedNodeRef<V>>,
    epoch_pin: &'e EpochPin,
) -> Result<Option<&'e V>, ConcurrentUpdateError> {
    let rnode = node.read_lock_or_restart()?;
    if let Some(parent) = parent {
        parent.read_unlock_or_restart()?;
    }

    // check if prefix matches, may increment level
    let prefix_len = if let Some(prefix_len) = rnode.prefix_matches(key) {
        prefix_len
    } else {
        rnode.read_unlock_or_restart()?;
        return Ok(None);
    };

    if rnode.is_leaf() {
        assert_eq!(key.len(), prefix_len);
        let vptr = rnode.get_leaf_value_ptr()?;
        // safety: It's OK to return a ref of the pointer because we checked the version
        // and the lifetime of 'epoch_pin' enforces that the reference is only accessible
        // as long as the epoch is pinned.
        let v = unsafe { vptr.as_ref().unwrap() };
        return Ok(Some(v));
    }

    let key = &key[prefix_len..];

    // find child (or leaf value)
    let next_node = rnode.find_child_or_restart(key[0])?;

    match next_node {
        None => Ok(None), // key not found
        Some(child) => lookup_recurse(&key[1..], child, Some(rnode), epoch_pin),
    }
}

fn next_recurse<'e, V: Value>(
    min_key: &[u8],
    path: &mut Vec<u8>,
    node: NodeRef<'e, V>,
    epoch_pin: &'e EpochPin,
) -> Result<Option<&'e V>, ConcurrentUpdateError> {
    let rnode = node.read_lock_or_restart()?;
    let prefix = rnode.get_prefix();
    if prefix.len() != 0 {
        path.extend_from_slice(prefix);
    }

    use std::cmp::Ordering;
    let comparison = path.as_slice().cmp(&min_key[0..path.len()]);
    if comparison == Ordering::Less {
        rnode.read_unlock_or_restart()?;
        return Ok(None);
    }

    if rnode.is_leaf() {
        assert_eq!(path.len(), min_key.len());
        let vptr = rnode.get_leaf_value_ptr()?;
        // safety: It's OK to return a ref of the pointer because we checked the version
        // and the lifetime of 'epoch_pin' enforces that the reference is only accessible
        // as long as the epoch is pinned.
        let v = unsafe { vptr.as_ref().unwrap() };
        return Ok(Some(v));
    }

    let mut min_key_byte = match comparison {
        Ordering::Less => unreachable!(), // checked this above already
        Ordering::Equal => min_key[path.len()],
        Ordering::Greater => 0,
    };

    loop {
        match rnode.find_next_child_or_restart(min_key_byte)? {
            None => {
                return Ok(None);
            }
            Some((key_byte, child_ref)) => {
                let path_len = path.len();
                path.push(key_byte);
                let result = next_recurse(min_key, path, child_ref, epoch_pin)?;
                if result.is_some() {
                    return Ok(result);
                }
                if key_byte == u8::MAX {
                    return Ok(None);
                }
                path.truncate(path_len);
                min_key_byte = key_byte + 1;
            }
        }
    }
}

// This corresponds to the 'insertOpt' function in the paper
pub(crate) fn update_recurse<'e, 'g, K: Key, V: Value, A: ArtAllocator<V>, F>(
    key: &[u8],
    value_fn: F,
    node: NodeRef<'e, V>,
    rparent: Option<(ReadLockedNodeRef<V>, u8)>,
    rgrandparent: Option<(ReadLockedNodeRef<V>, u8)>,
    guard: &'g mut TreeWriteGuard<'e, K, V, A>,
    level: usize,
    orig_key: &[u8],
) -> Result<(), ArtError>
where
    F: FnOnce(Option<&V>) -> UpdateAction<V>,
{
    let rnode = node.read_lock_or_restart()?;

    let prefix_match_len = rnode.prefix_matches(key);
    if prefix_match_len.is_none() {
        let (rparent, parent_key) = rparent.expect("direct children of the root have no prefix");
        let mut wparent = rparent.upgrade_to_write_lock_or_restart()?;
        let mut wnode = rnode.upgrade_to_write_lock_or_restart()?;

        match value_fn(None) {
            UpdateAction::Nothing => {}
            UpdateAction::Insert(new_value) => {
                insert_split_prefix(key, new_value, &mut wnode, &mut wparent, parent_key, guard)?;
            }
            UpdateAction::Remove => {
                panic!("unexpected Remove action on insertion");
            }
        }
        wnode.write_unlock();
        wparent.write_unlock();
        return Ok(());
    }
    let prefix_match_len = prefix_match_len.unwrap();
    let key = &key[prefix_match_len as usize..];
    let level = level + prefix_match_len as usize;

    if rnode.is_leaf() {
        assert_eq!(key.len(), 0);
        let (rparent, parent_key) = rparent.expect("root cannot be leaf");
        let mut wparent = rparent.upgrade_to_write_lock_or_restart()?;
        let mut wnode = rnode.upgrade_to_write_lock_or_restart()?;

        // safety: Now that we have acquired the write lock, we have exclusive access to the
        // value. XXX: There might be concurrent reads though?
        let value_mut = wnode.get_leaf_value_mut();

        match value_fn(Some(value_mut)) {
            UpdateAction::Nothing => {
                wparent.write_unlock();
                wnode.write_unlock();
            }
            UpdateAction::Insert(_) => panic!("cannot insert over existing value"),
            UpdateAction::Remove => {
                // TODO: If the parent becomes empty, unlink it from grandparent
                // TODO: If parent has only one child left, merge it with the child, extending its
                // prefix
                if wparent.can_shrink_after_delete() {
                    if let Some((rgrandparent, grandparent_key)) = rgrandparent {
                        let mut wgrandparent = rgrandparent.upgrade_to_write_lock_or_restart()?;
                        wparent.delete_child(parent_key);
                        shrink(&wparent, &mut wgrandparent, grandparent_key, guard)?;
                        wparent.write_unlock_obsolete();
                    } else {
                        wparent.delete_child(parent_key);
                        wparent.write_unlock();
                    }
                } else {
                    wparent.delete_child(parent_key);
                    wparent.write_unlock();
                }
                guard.remember_obsolete_node(wnode.as_ptr());
                wnode.write_unlock_obsolete();
            }
        }

        return Ok(());
    }

    let next_node = rnode.find_child_or_restart(key[0])?;

    if next_node.is_none() {
        if rnode.is_full() {
            let (rparent, parent_key) = rparent.expect("root node cannot become full");
            let mut wparent = rparent.upgrade_to_write_lock_or_restart()?;
            let wnode = rnode.upgrade_to_write_lock_or_restart()?;

            match value_fn(None) {
                UpdateAction::Nothing => {
                    wnode.write_unlock();
                    wparent.write_unlock();
                }
                UpdateAction::Insert(new_value) => {
                    insert_and_grow(key, new_value, &wnode, &mut wparent, parent_key, guard)?;
                    wnode.write_unlock_obsolete();
                    wparent.write_unlock();
                }
                UpdateAction::Remove => {
                    panic!("unexpected Remove action on insertion");
                }
            };
        } else {
            let mut wnode = rnode.upgrade_to_write_lock_or_restart()?;
            if let Some((rparent, _)) = rparent {
                rparent.read_unlock_or_restart()?;
            }
            match value_fn(None) {
                UpdateAction::Nothing => {}
                UpdateAction::Insert(new_value) => {
                    insert_to_node(&mut wnode, key, new_value, guard)?;
                }
                UpdateAction::Remove => {
                    panic!("unexpected Remove action on insertion");
                }
            };
            wnode.write_unlock();
        }
        return Ok(());
    } else {
        let next_child = next_node.unwrap(); // checked above it's not None
        if let Some((ref rparent, _)) = rparent {
            rparent.check_or_restart()?;
        }

        // recurse to next level
        update_recurse(
            &key[1..],
            value_fn,
            next_child,
            Some((rnode, key[0])),
            rparent,
            guard,
            level + 1,
            orig_key,
        )
    }
}

#[derive(Clone)]
enum PathElement {
    Prefix(Vec<u8>),
    KeyByte(u8),
}

impl std::fmt::Debug for PathElement {
    fn fmt(&self, fmt: &mut std::fmt::Formatter<'_>) -> Result<(), std::fmt::Error> {
        match self {
            PathElement::Prefix(prefix) => write!(fmt, "{:?}", prefix),
            PathElement::KeyByte(key_byte) => write!(fmt, "{}", key_byte),
        }
    }
}

fn dump_recurse<'e, V: Value + std::fmt::Debug>(
    path: &[PathElement],
    node: NodeRef<'e, V>,
    epoch_pin: &'e EpochPin,
    level: usize,
) -> Result<(), ConcurrentUpdateError> {
    let indent = str::repeat(" ", level);

    let rnode = node.read_lock_or_restart()?;
    let mut path = Vec::from(path);
    let prefix = rnode.get_prefix();
    if prefix.len() != 0 {
        path.push(PathElement::Prefix(Vec::from(prefix)));
    }

    if rnode.is_leaf() {
        let vptr = rnode.get_leaf_value_ptr()?;
        // safety: It's OK to return a ref of the pointer because we checked the version
        // and the lifetime of 'epoch_pin' enforces that the reference is only accessible
        // as long as the epoch is pinned.
        let val = unsafe { vptr.as_ref().unwrap() };
        eprintln!("{} {:?}: {:?}", indent, path, val);
    }

    for key_byte in 0..u8::MAX {
        match rnode.find_child_or_restart(key_byte)? {
            None => continue,
            Some(child_ref) => {
                let rchild = child_ref.read_lock_or_restart()?;
                eprintln!(
                    "{} {:?}, {}: prefix {:?}",
                    indent,
                    &path,
                    key_byte,
                    rchild.get_prefix()
                );

                let mut child_path = path.clone();
                child_path.push(PathElement::KeyByte(key_byte));

                dump_recurse(&child_path, child_ref, epoch_pin, level + 1)?;
            }
        }
    }

    Ok(())
}

///```text
///        [fooba]r -> value
///
/// [foo]b -> [a]r  -> value
///      e -> [ls]e -> value
///```
fn insert_split_prefix<'e, K: Key, V: Value, A: ArtAllocator<V>>(
    key: &[u8],
    value: V,
    node: &mut WriteLockedNodeRef<V>,
    parent: &mut WriteLockedNodeRef<V>,
    parent_key: u8,
    guard: &'e TreeWriteGuard<K, V, A>,
) -> Result<(), OutOfMemoryError> {
    let old_node = node;
    let old_prefix = old_node.get_prefix();
    let common_prefix_len = common_prefix(key, old_prefix);

    // Allocate a node for the new value.
    let new_value_node = allocate_node_for_value(
        &key[common_prefix_len + 1..],
        value,
        guard.tree_writer.allocator,
    )?;

    // Allocate a new internal node with the common prefix
    // FIXME: deallocate 'new_value_node' on OOM
    let mut prefix_node =
        node_ref::new_internal(&key[..common_prefix_len], guard.tree_writer.allocator)?;

    // Add the old node and the new nodes to the new internal node
    prefix_node.insert_old_child(old_prefix[common_prefix_len], old_node);
    prefix_node.insert_new_child(key[common_prefix_len], new_value_node);

    // Modify the prefix of the old child in place
    old_node.truncate_prefix(old_prefix.len() - common_prefix_len - 1);

    // replace the pointer in the parent
    parent.replace_child(parent_key, prefix_node.into_ptr());

    Ok(())
}

fn insert_to_node<'e, K: Key, V: Value, A: ArtAllocator<V>>(
    wnode: &mut WriteLockedNodeRef<V>,
    key: &[u8],
    value: V,
    guard: &'e TreeWriteGuard<K, V, A>,
) -> Result<(), OutOfMemoryError> {
    let value_child = allocate_node_for_value(&key[1..], value, guard.tree_writer.allocator)?;
    wnode.insert_child(key[0], value_child.into_ptr());
    Ok(())
}

// On entry: 'parent' and 'node' are locked
fn insert_and_grow<'e, 'g, K: Key, V: Value, A: ArtAllocator<V>>(
    key: &[u8],
    value: V,
    wnode: &WriteLockedNodeRef<V>,
    parent: &mut WriteLockedNodeRef<V>,
    parent_key_byte: u8,
    guard: &'g mut TreeWriteGuard<'e, K, V, A>,
) -> Result<(), ArtError> {
    let mut bigger_node = wnode.grow(guard.tree_writer.allocator)?;

    // FIXME: deallocate 'bigger_node' on OOM
    let value_child = allocate_node_for_value(&key[1..], value, guard.tree_writer.allocator)?;
    bigger_node.insert_new_child(key[0], value_child);

    // Replace the pointer in the parent
    parent.replace_child(parent_key_byte, bigger_node.into_ptr());

    guard.remember_obsolete_node(wnode.as_ptr());

    Ok(())
}

// On entry: 'parent' and 'node' are locked
fn shrink<'e, 'g, K: Key, V: Value, A: ArtAllocator<V>>(
    wnode: &WriteLockedNodeRef<V>,
    parent: &mut WriteLockedNodeRef<V>,
    parent_key_byte: u8,
    guard: &'g mut TreeWriteGuard<'e, K, V, A>,
) -> Result<(), ArtError> {
    eprintln!("SHRINK!");
    let smaller_node = wnode.shrink(guard.tree_writer.allocator)?;

    // Replace the pointer in the parent
    parent.replace_child(parent_key_byte, smaller_node.into_ptr());

    guard.remember_obsolete_node(wnode.as_ptr());

    Ok(())
}

// Allocate a new leaf node to hold 'value'. If the key is long, we
// may need to allocate new internal nodes to hold it too
fn allocate_node_for_value<'a, V: Value, A: ArtAllocator<V>>(
    key: &[u8],
    value: V,
    allocator: &'a A,
) -> Result<NewNodeRef<'a, V, A>, OutOfMemoryError> {
    let mut prefix_off = key.len().saturating_sub(MAX_PREFIX_LEN);

    let leaf_node = node_ref::new_leaf(&key[prefix_off..key.len()], value, allocator)?;

    let mut node = leaf_node;
    while prefix_off > 0 {
        // Need another internal node
        let remain_prefix = &key[0..prefix_off];

        prefix_off = remain_prefix.len().saturating_sub(MAX_PREFIX_LEN + 1);
        let mut internal_node = node_ref::new_internal(
            &remain_prefix[prefix_off..remain_prefix.len() - 1],
            allocator,
        )?;
        internal_node.insert_new_child(*remain_prefix.last().unwrap(), node);
        node = internal_node;
    }

    Ok(node)
}

fn common_prefix(a: &[u8], b: &[u8]) -> usize {
    for i in 0..MAX_PREFIX_LEN {
        if a[i] != b[i] {
            return i;
        }
    }
    panic!("prefixes are equal");
}
