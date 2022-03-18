//!
//! Simple on-disk B-tree implementation
//!
//! This is used as the index structure within image and delta layers
//!
//! Features:
//! - Fixed-width keys
//! - Fixed-width values (VALUE_SZ)
//! - The tree is created in a bulk operation. Insert/deletion after creation
//!   is not suppported
//! - page-oriented
//!
//! TODO:
//! - better errors (e.g. with thiserror?)
//! - maybe something like an Adaptive Radix Tree would be more efficient?
//! - the values stored by image and delta layers are offsets into the file,
//!   and they are in monotonically increasing order. Prefix compression would
//!   be very useful for them, too.
//!
use anyhow;
use byteorder::{ReadBytesExt, BE};
use bytes::{BufMut, Bytes, BytesMut};
use hex;
use std::cmp::Ordering;

pub const VALUE_SZ: usize = 5; // FIXME: replace all 5's with this
pub const MAX_VALUE: u64 = 0x007f_ffff_ffff;

#[allow(dead_code)]
pub const PAGE_SZ: usize = 8192;

///
/// The implementation is generic in that it doesn't know about the
/// page cache or other page server infrastructure. That makes it
/// easier to write unit tests.  The search functions use
/// DiskBlockReader to access the underylying storage.  The real
/// implementation used by the image and delta layers is in
/// 'blocky_reader.rs', and it uses the page cache. The unit tests
/// simulate a disk with a simple in-memory vector of pages.
///
pub trait DiskBlockReader {
    type Lease: AsRef<[u8; PAGE_SZ]>;

    ///
    /// Read a block. Returns a "lease" object that can be used to
    /// access to the contents of the page. (For the page cache, the
    /// lease object represents a lock on the buffer.)
    ///
    fn read_blk(&self, blknum: u32) -> Result<Self::Lease, std::io::Error>;
}

///
/// Abstraction for writing a block, when building the tree.
///
pub trait DiskBlockWriter {
    ///
    /// Write 'buf' to a block on disk. 'buf' must be PAGE_SZ bytes long.
    /// Returns the block number that the block was written to. It can be
    /// used to access the page later, when reading the tree.
    ///
    fn write_blk(&mut self, buf: Bytes) -> Result<u32, std::io::Error>;
}

#[derive(Clone, Copy, Debug)]
struct Value([u8; VALUE_SZ]);

impl Value {
    fn from_slice(slice: &[u8]) -> Value {
        let mut b = [0u8; 5];
        b.copy_from_slice(slice);
        Value(b)
    }

    fn from_u64(x: u64) -> Value {
        assert!(x <= 0x007f_ffff_ffff);
        Value([
            (x >> 32) as u8,
            (x >> 24) as u8,
            (x >> 16) as u8,
            (x >> 8) as u8,
            x as u8,
        ])
    }

    fn from_blknum(x: u32) -> Value {
        Value([
            0x80,
            (x >> 24) as u8,
            (x >> 16) as u8,
            (x >> 8) as u8,
            x as u8,
        ])
    }

    #[allow(dead_code)]
    fn is_offset(self) -> bool {
        self.0[0] & 0x80 != 0
    }

    fn to_u64(self) -> u64 {
        let b = &self.0;
        (b[0] as u64) << 32
            | (b[1] as u64) << 24
            | (b[2] as u64) << 16
            | (b[3] as u64) << 8
            | b[4] as u64
    }

    fn to_blknum(self) -> u32 {
        let b = &self.0;
        assert!(b[0] == 0x80);
        (b[1] as u32) << 24 | (b[2] as u32) << 16 | (b[3] as u32) << 8 | b[4] as u32
    }
}

/// This is the on-disk representation.
struct OnDiskNode<'a> {
    // Fixed-width fields
    num_children: u16,
    level: u8,
    prefix_len: u8,
    suffix_len: u8,

    // Variable-length fields. These are stored on-disk after the fixed-width
    // fields, in this order. In the in-memory representation, these point to
    // the right parts in the page buffer.
    prefix: &'a [u8],
    keys: &'a [u8],
    values: &'a [u8],
}

impl<'a> OnDiskNode<'a> {
    ///
    /// Interpret a PAGE_SZ page as a node.
    ///
    fn deparse(buf: &[u8]) -> OnDiskNode {
        let mut cursor = std::io::Cursor::new(buf);
        let num_children = cursor.read_u16::<BE>().unwrap();
        let level = cursor.read_u8().unwrap();
        let prefix_len = cursor.read_u8().unwrap();
        let suffix_len = cursor.read_u8().unwrap();

        let mut off = cursor.position();
        let prefix_off = off as usize;
        off += prefix_len as u64;

        let keys_off = off as usize;
        let keys_len = num_children as usize * suffix_len as usize;
        off += keys_len as u64;

        let values_off = off as usize;
        let values_len = num_children as usize * VALUE_SZ as usize;
        //off += keys_len as u64;

        let prefix = &buf[prefix_off..prefix_off + prefix_len as usize];
        let keys = &buf[keys_off..keys_off + keys_len];
        let values = &buf[values_off..values_off + values_len];

        OnDiskNode {
            num_children,
            level,
            prefix_len,
            suffix_len,
            prefix,
            keys,
            values,
        }
    }

    ///
    /// Read a value at 'idx'
    ///
    fn value(&self, idx: usize) -> Value {
        let value_off = idx * VALUE_SZ;
        let value_slice = &self.values[value_off..value_off + VALUE_SZ];
        Value::from_slice(value_slice)
    }

    fn binary_search(&self, search_key: &[u8], keybuf: &mut [u8]) -> Result<usize, usize> {
        let mut size = self.num_children as usize;
        let mut low = 0;
        let mut high = size;
        while low < high {
            let mid = low + size / 2;

            let key_off = mid as usize * self.suffix_len as usize;
            let suffix = &self.keys[key_off..key_off + self.suffix_len as usize];
            // Does this match?
            keybuf[self.prefix_len as usize..].copy_from_slice(suffix);

            let cmp = keybuf[..].cmp(search_key);

            if cmp == Ordering::Less {
                low = mid + 1;
            } else if cmp == Ordering::Greater {
                high = mid;
            } else {
                return Ok(mid);
            }
            size = high - low;
        }
        Err(low)
    }
}

///
/// Public reader object, to search the tree.
///
pub struct DiskBtreeReader<R>
where
    R: DiskBlockReader,
{
    root_blk: u32,
    reader: R,
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub enum VisitDirection {
    Forwards,
    Backwards,
}

impl<R> DiskBtreeReader<R>
where
    R: DiskBlockReader,
{
    pub fn new(root_blk: u32, reader: R) -> Self {
        DiskBtreeReader { root_blk, reader }
    }

    ///
    /// Read the value for given key. Returns the value, or None if it doesn't exist.
    ///
    pub fn get(&self, search_key: &[u8]) -> anyhow::Result<Option<u64>> {
        let mut result: Option<u64> = None;
        self.visit(search_key, VisitDirection::Forwards, |key, value| {
            if key == search_key {
                result = Some(value);
            }
            false
        })?;
        Ok(result)
    }

    ///
    /// Scan the tree, starting from 'search_key', in the given direction. 'visitor'
    /// will be called for every key >= 'search_key' (or <= 'search_key', if scanning
    /// backwards)
    ///
    pub fn visit<V>(
        &self,
        search_key: &[u8],
        dir: VisitDirection,
        mut visitor: V,
    ) -> anyhow::Result<bool>
    where
        V: FnMut(&[u8], u64) -> bool,
    {
        self.search_recurse(self.root_blk, search_key, dir, &mut visitor)
    }

    fn search_recurse<V>(
        &self,
        node_blknum: u32,
        search_key: &[u8],
        dir: VisitDirection,
        visitor: &mut V,
    ) -> anyhow::Result<bool>
    where
        V: FnMut(&[u8], u64) -> bool,
    {
        // Locate the node.
        let blk = self.reader.read_blk(node_blknum)?;

        // Search all entries on this node
        self.search_node(blk.as_ref(), search_key, dir, visitor)
    }

    fn search_node<V>(
        &self,
        node_buf: &[u8],
        search_key: &[u8],
        dir: VisitDirection,
        visitor: &mut V,
    ) -> anyhow::Result<bool>
    where
        V: FnMut(&[u8], u64) -> bool,
    {
        let node = OnDiskNode::deparse(node_buf);
        let prefix_len = node.prefix_len as usize;
        let suffix_len = node.suffix_len as usize;

        let mut i = 0;
        while i < prefix_len && i < search_key.len() {
            if node.prefix[i] != search_key[i] {
                break;
            }
            i += 1;
        }
        let common_prefix_len = i;

        assert!(node.num_children > 0);

        let mut keybuf = Vec::new();
        keybuf.extend(node.prefix);
        keybuf.resize(prefix_len + suffix_len, 0);

        // TODO: Binary search
        if dir == VisitDirection::Forwards {
            // Locate the first match
            let mut idx = match node.binary_search(search_key, keybuf.as_mut_slice()) {
                Ok(idx) => idx,
                Err(idx) => {
                    if node.level == 0 {
                        // 1  -> ..
                        // HERE
                        // 3  -> ..  *
                        idx
                    } else {
                        // 1  -> ..   *
                        // HERE
                        // 3  -> ..
                        idx.saturating_sub(1)
                    }
                }
            };
            // idx points to the first match now. Keep going from there
            let mut key_off = idx * suffix_len;
            while idx < node.num_children as usize {
                let suffix = &node.keys[key_off..key_off + suffix_len];
                keybuf[prefix_len..].copy_from_slice(suffix);
                let value = node.value(idx as usize);
                #[allow(clippy::collapsible_if)]
                if node.level == 0 {
                    // leaf
                    if !visitor(&keybuf, value.to_u64()) {
                        return Ok(false);
                    }
                } else {
                    #[allow(clippy::collapsible_if)]
                    if !self.search_recurse(value.to_blknum(), search_key, dir, visitor)? {
                        return Ok(false);
                    }
                }
                idx += 1;
                key_off += suffix_len;
            }
        } else {
            let mut idx = match node.binary_search(search_key, keybuf.as_mut_slice()) {
                Ok(idx) => idx + 1,
                // 1  -> ..   *
                // HERE
                // 3  -> ..
                Err(idx) => idx,
            };

            // idx  points to the first match + 1 now. Keep going from there
            let mut key_off = idx * suffix_len;
            while idx > 0 {
                idx -= 1;
                key_off -= suffix_len;
                let suffix = &node.keys[key_off..key_off + suffix_len];
                keybuf[prefix_len..].copy_from_slice(suffix);
                let value = node.value(idx as usize);
                #[allow(clippy::collapsible_if)]
                if node.level == 0 {
                    // leaf
                    if !visitor(&keybuf, value.to_u64()) {
                        return Ok(false);
                    }
                } else {
                    #[allow(clippy::collapsible_if)]
                    if !self.search_recurse(value.to_blknum(), search_key, dir, visitor)? {
                        return Ok(false);
                    }
                }
                if idx == 0 {
                    break;
                }
            }
        }
        Ok(true)
    }

    #[allow(dead_code)]
    pub fn dump(&self) -> anyhow::Result<()> {
        self.dump_recurse(self.root_blk, &[], 0)
    }

    fn dump_recurse(&self, blknum: u32, path: &[u8], depth: usize) -> anyhow::Result<()> {
        let blk = self.reader.read_blk(blknum)?;
        let buf: &[u8] = blk.as_ref();

        let node = OnDiskNode::deparse(buf);

        print!("{:indent$}", "", indent = depth * 2);
        println!(
            "blk #{}: path {}: prefix {}, suffix_len {}",
            blknum,
            hex::encode(path),
            hex::encode(node.prefix),
            node.suffix_len
        );

        let mut idx = 0;
        let mut key_off = 0;
        while idx < node.num_children {
            let key = &node.keys[key_off..key_off + node.suffix_len as usize];
            let val = node.value(idx as usize);
            print!("{:indent$}", "", indent = depth * 2 + 2);
            println!("{}: {}", hex::encode(key), hex::encode(val.0));

            if node.level > 0 {
                let child_path = [path, node.prefix].concat();
                self.dump_recurse(val.to_blknum(), &child_path, depth + 1)?;
            }
            idx += 1;
            key_off += node.suffix_len as usize;
        }
        Ok(())
    }
}

///
/// Public builder object, for creating a new tree.
///
/// Usage: Create a builder object by calling 'new', load all the data into the
/// tree by calling 'append' for each key-value pair, and then call 'finish'
///
pub struct DiskBtreeBuilder<W>
where
    W: DiskBlockWriter,
{
    key_len: u8,
    writer: W,

    ///
    /// stack[0] is the current root page, stack.last() is the leaf.
    ///
    stack: Vec<BuildNode>,

    /// Last key that was appended to the tree. Used to sanity check that append
    /// is called in increasing key order.
    last_key: Vec<u8>,
}

impl<W> DiskBtreeBuilder<W>
where
    W: DiskBlockWriter,
{
    pub fn new(key_len: u8, writer: W) -> Self {
        DiskBtreeBuilder {
            key_len,
            writer,
            last_key: Vec::new(),
            stack: vec![BuildNode::new(0)],
        }
    }

    pub fn append(&mut self, key: &[u8], value: u64) -> Result<(), anyhow::Error> {
        assert!(key.len() == self.key_len as usize);
        assert!(value <= MAX_VALUE);
        if self.last_key.is_empty() {
            self.last_key.extend(key);
        } else {
            assert!(key > self.last_key.as_slice(), "unsorted input");
            self.last_key.copy_from_slice(key);
        }

        Ok(self.append_internal(key, Value::from_u64(value))?)
    }

    fn append_internal(&mut self, key: &[u8], value: Value) -> Result<(), std::io::Error> {
        // Try to append to the current leaf buffer
        let last = self.stack.last_mut().unwrap();
        let level = last.level;
        if last.push(key, value) {
            return Ok(());
        }

        // It did not fit. Try to compress, and it it succeeds to make some room
        // on the node, try appending to it again.
        #[allow(clippy::collapsible_if)]
        if last.compress() {
            if last.push(key, value) {
                return Ok(());
            }
        }

        // Could not append to the current leaf. Flush it and create a new one.
        self.flush_node()?;

        // Replace the node we flushed with an empty one and append the new
        // key to it.
        let mut last = BuildNode::new(level);
        if !last.push(key, value) {
            panic!("could not push to new leaf node");
        }
        self.stack.push(last);

        Ok(())
    }

    fn flush_node(&mut self) -> Result<(), std::io::Error> {
        let last = self.stack.pop().unwrap();
        let buf = last.pack();
        let downlink_key = last.first_key();
        let downlink_ptr = self.writer.write_blk(buf)?;

        // Append the downlink to the parent
        if self.stack.is_empty() {
            self.stack.push(BuildNode::new(last.level + 1));
        }
        self.append_internal(&downlink_key, Value::from_blknum(downlink_ptr))?;

        Ok(())
    }

    ///
    /// Flushes everything to disk, and returns the block number of the root page.
    /// The caller must store the root block number "out-of-band", and pass it
    /// to the DiskBtreeReader::new() when you want to read the tree again.
    /// (In the image and delta layers, it is stored in the beginning of the file,
    /// in the summary header)
    ///
    pub fn finish(mut self) -> Result<(u32, W), std::io::Error> {
        // flush all levels, except the root.
        while self.stack.len() > 1 {
            self.flush_node()?;
        }

        let root = self.stack.first().unwrap();
        let buf = root.pack();
        let root_blknum = self.writer.write_blk(buf)?;

        Ok((root_blknum, self.writer))
    }

    pub fn borrow_writer(&mut self) -> &mut W {
        &mut self.writer
    }
}

///
/// BuildNode represesnts an incomplete page that we are appending to.
///
#[derive(Clone, Debug)]
struct BuildNode {
    num_children: u16,
    level: u8,
    prefix: Vec<u8>,
    suffix_len: usize,

    keys: Vec<u8>,
    values: Vec<u8>,

    size: usize, // physical size of this node, if it was written to disk like this
}

const NODE_SIZE: usize = PAGE_SZ;

const NODE_HDR_SIZE: usize = 2 + 1 + 1 + 1;

impl BuildNode {
    fn new(level: u8) -> Self {
        BuildNode {
            num_children: 0,
            level,
            prefix: Vec::new(),
            suffix_len: 0,
            keys: Vec::new(),
            values: Vec::new(),
            size: NODE_HDR_SIZE,
        }
    }

    /// Try to append a key-value pair to this node. Returns 'true' on
    /// success, 'false' if the page was full or the key was
    /// incompatible with the prefix of the existing keys.
    fn push(&mut self, key: &[u8], value: Value) -> bool {
        // If we have already performed prefix-compression on the page,
        // check that the incoming key has the same prefix.
        if self.num_children > 0 {
            // does the suffix length and prefix allow it?
            if self.prefix.len() + self.suffix_len != key.len() {
                return false;
            }
            if !key.starts_with(&self.prefix) {
                return false;
            }
        } else {
            self.suffix_len = key.len();
        }

        // Is the node too full?
        if self.size + self.suffix_len + VALUE_SZ >= NODE_SIZE {
            return false;
        }

        // All clear
        self.num_children += 1;
        self.keys.extend(&key[self.prefix.len()..]);
        self.values.extend(value.0);

        assert!(self.keys.len() == self.num_children as usize * self.suffix_len as usize);
        assert!(self.values.len() == self.num_children as usize * VALUE_SZ);

        self.size += self.suffix_len + VALUE_SZ;

        true
    }

    ///
    /// Perform prefix-compression.
    ///
    /// Returns 'true' on success, 'false' if no compression was possible.
    ///
    fn compress(&mut self) -> bool {
        let first_suffix = self.first_suffix();
        let last_suffix = self.last_suffix();

        // Find the common prefix among all keys
        let mut prefix_len = 0;
        while prefix_len < self.suffix_len {
            if first_suffix[prefix_len] != last_suffix[prefix_len] {
                break;
            }
            prefix_len += 1;
        }
        if prefix_len == 0 {
            return false;
        }

        // Can compress. Rewrite the keys without the common prefix.
        self.prefix.extend(&self.keys[..prefix_len]);

        let mut new_keys = Vec::new();
        let mut key_off = 0;
        while key_off < self.keys.len() {
            let next_key_off = key_off + self.suffix_len;
            new_keys.extend(&self.keys[key_off + prefix_len..next_key_off]);
            key_off = next_key_off;
        }
        self.keys = new_keys;
        self.suffix_len -= prefix_len;

        self.size -= prefix_len * self.num_children as usize;
        self.size += prefix_len;

        assert!(self.keys.len() == self.num_children as usize * self.suffix_len as usize);
        assert!(self.values.len() == self.num_children as usize * VALUE_SZ);

        true
    }

    ///
    /// Serialize the node to on-disk format.
    ///
    fn pack(&self) -> Bytes {
        assert!(self.keys.len() == self.num_children as usize * self.suffix_len as usize);
        assert!(self.values.len() == self.num_children as usize * VALUE_SZ);
        assert!(self.num_children > 0);

        let mut buf = BytesMut::new();

        buf.put_u16(self.num_children);
        buf.put_u8(self.level);
        buf.put_u8(self.prefix.len() as u8);
        buf.put_u8(self.suffix_len as u8);
        buf.put(&self.prefix[..]);
        buf.put(&self.keys[..]);
        buf.put(&self.values[..]);

        assert!(buf.len() == self.size);

        assert!(buf.len() <= PAGE_SZ);
        buf.resize(PAGE_SZ, 0);
        buf.freeze()
    }

    fn first_suffix(&self) -> &[u8] {
        &self.keys[..self.suffix_len]
    }
    fn last_suffix(&self) -> &[u8] {
        &self.keys[self.keys.len() - self.suffix_len..]
    }

    /// Return the full first key of the page, including the prefix
    fn first_key(&self) -> Vec<u8> {
        [&self.prefix, self.first_suffix()].concat().to_vec()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use hex_literal::hex;
    use rand::Rng;
    use std::collections::BTreeMap;
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[derive(Clone, Default)]
    struct TestDisk {
        blocks: Vec<Bytes>,
    }
    impl TestDisk {
        fn new() -> Self {
            Self::default()
        }
    }
    impl DiskBlockReader for TestDisk {
        type Lease = std::rc::Rc<[u8; PAGE_SZ]>;

        fn read_blk(&self, blknum: u32) -> Result<Self::Lease, std::io::Error> {
            let mut buf = [0u8; PAGE_SZ];
            buf.copy_from_slice(&self.blocks[blknum as usize]);
            Ok(std::rc::Rc::new(buf))
        }
    }
    impl DiskBlockWriter for &mut TestDisk {
        fn write_blk(&mut self, buf: Bytes) -> Result<u32, std::io::Error> {
            let blknum = self.blocks.len();
            self.blocks.push(buf);
            Ok(blknum as u32)
        }
    }

    #[test]
    fn basic() -> anyhow::Result<()> {
        let mut disk = TestDisk::new();
        let mut writer = DiskBtreeBuilder::new(6, &mut disk);

        let all_keys = vec![
            &b"xaaaaa"[..],
            &b"xaaaba"[..],
            &b"xaaaca"[..],
            &b"xabaaa"[..],
            &b"xababa"[..],
            &b"xabaca"[..],
            &b"xabada"[..],
            &b"xabadb"[..],
        ];
        let all_data: Vec<(&[u8], u64)> = all_keys
            .iter()
            .enumerate()
            .map(|(idx, key)| (*key, idx as u64))
            .collect();
        for (key, val) in all_data.iter() {
            writer.append(key, *val)?;
        }

        let (root_offset, _writer) = writer.finish()?;

        let reader = DiskBtreeReader::new(root_offset, disk);

        reader.dump()?;

        // Test the `get` function on all the keys.
        for (key, val) in all_data.iter() {
            assert_eq!(reader.get(key)?, Some(*val));
        }
        // And on some keys that don't exist
        assert_eq!(reader.get(b"aaaaa")?, None);
        assert_eq!(reader.get(b"zzzzz")?, None);
        assert_eq!(reader.get(b"xaaa")?, None);
        assert_eq!(reader.get(b"xabaaa000")?, None);
        assert_eq!(reader.get(b"a")?, None);
        assert_eq!(reader.get(b"z")?, None);

        // Test search with `visit` function
        let search_key = b"xaba";
        let expected: Vec<(Vec<u8>, u64)> = all_data
            .iter()
            .filter(|(key, _value)| key[..] >= search_key[..])
            .map(|(key, value)| (key.to_vec(), *value))
            .collect();

        let mut data = Vec::new();
        reader.visit(b"xaba", VisitDirection::Forwards, |key, value| {
            data.push((key.to_vec(), value));
            true
        })?;
        assert_eq!(data, expected);

        // Test a backwards scan
        let mut expected: Vec<(Vec<u8>, u64)> = all_data
            .iter()
            .filter(|(key, _value)| key[..] <= search_key[..])
            .map(|(key, value)| (key.to_vec(), *value))
            .collect();
        expected.reverse();
        let mut data = Vec::new();
        reader.visit(b"xaba", VisitDirection::Backwards, |key, value| {
            data.push((key.to_vec(), value));
            true
        })?;
        assert_eq!(data, expected);

        // Backward scan where nothing matches
        reader.visit(b"aaaaa", VisitDirection::Backwards, |key, value| {
            panic!("found unexpected key {}: {}", hex::encode(key), value);
        })?;

        // Full scan
        let expected: Vec<(Vec<u8>, u64)> = all_data
            .iter()
            .map(|(key, value)| (key.to_vec(), *value))
            .collect();
        let mut data = Vec::new();
        reader.visit(b"", VisitDirection::Forwards, |key, value| {
            data.push((key.to_vec(), value));
            true
        })?;
        assert_eq!(data, expected);

        Ok(())
    }

    #[test]
    fn lots_of_keys() -> anyhow::Result<()> {
        let mut disk = TestDisk::new();
        let mut writer = DiskBtreeBuilder::new(8, &mut disk);

        const NUM_KEYS: u64 = 1000;

        let mut all_data: BTreeMap<u64, u64> = BTreeMap::new();

        for idx in 0..NUM_KEYS {
            let key_int: u64 = 1 + idx * 2;
            let key = u64::to_be_bytes(key_int);
            writer.append(&key, idx)?;

            all_data.insert(key_int, idx);
        }

        let (root_offset, _writer) = writer.finish()?;

        let reader = DiskBtreeReader::new(root_offset, disk);

        reader.dump()?;

        use std::sync::Mutex;

        let result = Mutex::new(Vec::new());
        let limit: AtomicUsize = AtomicUsize::new(10);
        let take_ten = |key: &[u8], value: u64| {
            let mut keybuf = [0u8; 8];
            keybuf.copy_from_slice(key);
            let key_int = u64::from_be_bytes(keybuf);

            let mut result = result.lock().unwrap();
            result.push((key_int, value));

            // keep going until we have 10 matches
            result.len() < limit.load(Ordering::Relaxed)
        };

        for search_key_int in 0..(NUM_KEYS * 2 + 10) {
            let search_key = u64::to_be_bytes(search_key_int);
            assert_eq!(
                reader.get(&search_key)?,
                all_data.get(&search_key_int).cloned()
            );

            // Test a forward scan starting with this key
            result.lock().unwrap().clear();
            reader.visit(&search_key, VisitDirection::Forwards, take_ten)?;
            let expected = all_data
                .range(search_key_int..)
                .take(10)
                .map(|(&key, &val)| (key, val))
                .collect::<Vec<(u64, u64)>>();
            assert_eq!(*result.lock().unwrap(), expected);

            // And a backwards scan
            result.lock().unwrap().clear();
            reader.visit(&search_key, VisitDirection::Backwards, take_ten)?;
            let expected = all_data
                .range(..=search_key_int)
                .rev()
                .take(10)
                .map(|(&key, &val)| (key, val))
                .collect::<Vec<(u64, u64)>>();
            assert_eq!(*result.lock().unwrap(), expected);
        }

        // full scan
        let search_key = u64::to_be_bytes(0);
        limit.store(usize::MAX, Ordering::Relaxed);
        result.lock().unwrap().clear();
        reader.visit(&search_key, VisitDirection::Forwards, take_ten)?;
        let expected = all_data
            .iter()
            .map(|(&key, &val)| (key, val))
            .collect::<Vec<(u64, u64)>>();
        assert_eq!(*result.lock().unwrap(), expected);

        // full scan
        let search_key = u64::to_be_bytes(u64::MAX);
        limit.store(usize::MAX, Ordering::Relaxed);
        result.lock().unwrap().clear();
        reader.visit(&search_key, VisitDirection::Backwards, take_ten)?;
        let expected = all_data
            .iter()
            .rev()
            .map(|(&key, &val)| (key, val))
            .collect::<Vec<(u64, u64)>>();
        assert_eq!(*result.lock().unwrap(), expected);

        Ok(())
    }

    #[test]
    fn random_data() -> anyhow::Result<()> {
        // Generate random keys with exponential distribution, to
        // exercise the prefix compression
        const NUM_KEYS: usize = 100000;
        let mut all_data: BTreeMap<u128, u64> = BTreeMap::new();
        for idx in 0..NUM_KEYS {
            let u: f64 = rand::thread_rng().gen_range(0.0..1.0);
            let t = -(f64::ln(u));
            let key_int = (t * 1000000.0) as u128;

            all_data.insert(key_int as u128, idx as u64);
        }

        // Build a tree from it
        let mut disk = TestDisk::new();
        let mut writer = DiskBtreeBuilder::new(16, &mut disk);

        for (&key, &val) in all_data.iter() {
            writer.append(&u128::to_be_bytes(key), val)?;
        }
        let (root_offset, _writer) = writer.finish()?;

        let reader = DiskBtreeReader::new(root_offset, disk);

        // Test get() operation on all the keys
        for (&key, &val) in all_data.iter() {
            let search_key = u128::to_be_bytes(key);
            assert_eq!(reader.get(&search_key)?, Some(val));
        }

        // Test get() operations on random keys, most of which will not exist
        for _ in 0..100000 {
            let key_int = rand::thread_rng().gen::<u128>();
            let search_key = u128::to_be_bytes(key_int);
            assert!(reader.get(&search_key)? == all_data.get(&key_int).cloned());
        }

        // Test boundary cases
        assert!(reader.get(&u128::to_be_bytes(u128::MIN))? == all_data.get(&u128::MIN).cloned());
        assert!(reader.get(&u128::to_be_bytes(u128::MAX))? == all_data.get(&u128::MAX).cloned());

        Ok(())
    }

    #[test]
    #[should_panic(expected = "unsorted input")]
    fn unsorted_input() {
        let mut disk = TestDisk::new();
        let mut writer = DiskBtreeBuilder::new(2, &mut disk);

        let _ = writer.append(b"ba", 1);
        let _ = writer.append(b"bb", 2);
        let _ = writer.append(b"aa", 3);
    }

    ///
    /// This test contains a particular data set, representing all the keys
    /// generated by the 'test_random_updates' unit test. I extracted this while
    /// trying to debug a failure in that test. The bug turned out to be
    /// elsewhere, and I'm not sure if this is still useful, but keeping it for
    /// now...  Maybe it's a useful data set to show the typical key-values used
    /// by a delta layer, for evaluating how well the prefix compression works.
    ///
    #[test]
    fn particular_data() -> anyhow::Result<()> {
        #[rustfmt::skip]
        static DATA:  [([u8; 26], u64); 2000] = [
            (hex!("0122222222333333334444444455000000000000000000000010"), 0x4000002000),
            (hex!("0122222222333333334444444455000000000000000000007cb0"), 0x4000002050),
            (hex!("0122222222333333334444444455000000010000000000000020"), 0x40000020a0),
            (hex!("0122222222333333334444444455000000020000000000000030"), 0x40000020f0),
            (hex!("01222222223333333344444444550000000200000000000051a0"), 0x4000002140),
            (hex!("0122222222333333334444444455000000030000000000000040"), 0x4000002190),
            (hex!("0122222222333333334444444455000000030000000000006cf0"), 0x40000021e0),
            (hex!("0122222222333333334444444455000000030000000000007140"), 0x4000002230),
            (hex!("0122222222333333334444444455000000040000000000000050"), 0x4000002280),
            (hex!("01222222223333333344444444550000000400000000000047f0"), 0x40000022d0),
            (hex!("01222222223333333344444444550000000400000000000072b0"), 0x4000002320),
            (hex!("0122222222333333334444444455000000050000000000000060"), 0x4000002370),
            (hex!("0122222222333333334444444455000000050000000000005550"), 0x40000023c0),
            (hex!("0122222222333333334444444455000000060000000000000070"), 0x4000002410),
            (hex!("01222222223333333344444444550000000600000000000044a0"), 0x4000002460),
            (hex!("0122222222333333334444444455000000060000000000006870"), 0x40000024b0),
            (hex!("0122222222333333334444444455000000070000000000000080"), 0x4000002500),
            (hex!("0122222222333333334444444455000000080000000000000090"), 0x4000002550),
            (hex!("0122222222333333334444444455000000080000000000004150"), 0x40000025a0),
            (hex!("01222222223333333344444444550000000900000000000000a0"), 0x40000025f0),
            (hex!("01222222223333333344444444550000000a00000000000000b0"), 0x4000002640),
            (hex!("01222222223333333344444444550000000a0000000000006680"), 0x4000002690),
            (hex!("01222222223333333344444444550000000b00000000000000c0"), 0x40000026e0),
            (hex!("01222222223333333344444444550000000b0000000000006230"), 0x4000002730),
            (hex!("01222222223333333344444444550000000c00000000000000d0"), 0x4000002780),
            (hex!("01222222223333333344444444550000000d00000000000000e0"), 0x40000027d0),
            (hex!("01222222223333333344444444550000000e00000000000000f0"), 0x4000002820),
            (hex!("01222222223333333344444444550000000e0000000000006000"), 0x4000002870),
            (hex!("01222222223333333344444444550000000f0000000000000100"), 0x40000028c0),
            (hex!("01222222223333333344444444550000000f00000000000053c0"), 0x4000002910),
            (hex!("01222222223333333344444444550000000f0000000000006580"), 0x4000002960),
            (hex!("0122222222333333334444444455000000100000000000000110"), 0x40000029b0),
            (hex!("01222222223333333344444444550000001000000000000046c0"), 0x4000002a00),
            (hex!("0122222222333333334444444455000000100000000000004e40"), 0x4000002a50),
            (hex!("0122222222333333334444444455000000110000000000000120"), 0x4000002aa0),
            (hex!("0122222222333333334444444455000000120000000000000130"), 0x4000002af0),
            (hex!("01222222223333333344444444550000001200000000000066d0"), 0x4000002b40),
            (hex!("0122222222333333334444444455000000130000000000000140"), 0x4000002b90),
            (hex!("0122222222333333334444444455000000130000000000007710"), 0x4000002be0),
            (hex!("0122222222333333334444444455000000140000000000000150"), 0x4000002c30),
            (hex!("0122222222333333334444444455000000140000000000006c40"), 0x4000002c80),
            (hex!("0122222222333333334444444455000000150000000000000160"), 0x4000002cd0),
            (hex!("0122222222333333334444444455000000150000000000005990"), 0x4000002d20),
            (hex!("0122222222333333334444444455000000160000000000000170"), 0x4000002d70),
            (hex!("0122222222333333334444444455000000160000000000005530"), 0x4000002dc0),
            (hex!("0122222222333333334444444455000000170000000000000180"), 0x4000002e10),
            (hex!("0122222222333333334444444455000000170000000000004290"), 0x4000002e60),
            (hex!("0122222222333333334444444455000000180000000000000190"), 0x4000002eb0),
            (hex!("01222222223333333344444444550000001800000000000051c0"), 0x4000002f00),
            (hex!("01222222223333333344444444550000001900000000000001a0"), 0x4000002f50),
            (hex!("0122222222333333334444444455000000190000000000005420"), 0x4000002fa0),
            (hex!("0122222222333333334444444455000000190000000000005770"), 0x4000002ff0),
            (hex!("01222222223333333344444444550000001900000000000079d0"), 0x4000003040),
            (hex!("01222222223333333344444444550000001a00000000000001b0"), 0x4000003090),
            (hex!("01222222223333333344444444550000001a0000000000006f70"), 0x40000030e0),
            (hex!("01222222223333333344444444550000001a0000000000007150"), 0x4000003130),
            (hex!("01222222223333333344444444550000001b00000000000001c0"), 0x4000003180),
            (hex!("01222222223333333344444444550000001b0000000000005070"), 0x40000031d0),
            (hex!("01222222223333333344444444550000001c00000000000001d0"), 0x4000003220),
            (hex!("01222222223333333344444444550000001d00000000000001e0"), 0x4000003270),
            (hex!("01222222223333333344444444550000001e00000000000001f0"), 0x40000032c0),
            (hex!("01222222223333333344444444550000001e0000000000005650"), 0x4000003310),
            (hex!("01222222223333333344444444550000001f0000000000000200"), 0x4000003360),
            (hex!("01222222223333333344444444550000001f0000000000006ca0"), 0x40000033b0),
            (hex!("0122222222333333334444444455000000200000000000000210"), 0x4000003400),
            (hex!("0122222222333333334444444455000000200000000000005fc0"), 0x4000003450),
            (hex!("0122222222333333334444444455000000210000000000000220"), 0x40000034a0),
            (hex!("0122222222333333334444444455000000210000000000006430"), 0x40000034f0),
            (hex!("0122222222333333334444444455000000220000000000000230"), 0x4000003540),
            (hex!("01222222223333333344444444550000002200000000000040e0"), 0x4000003590),
            (hex!("0122222222333333334444444455000000230000000000000240"), 0x40000035e0),
            (hex!("01222222223333333344444444550000002300000000000042d0"), 0x4000003630),
            (hex!("0122222222333333334444444455000000240000000000000250"), 0x4000003680),
            (hex!("0122222222333333334444444455000000250000000000000260"), 0x40000036d0),
            (hex!("01222222223333333344444444550000002500000000000058c0"), 0x4000003720),
            (hex!("0122222222333333334444444455000000260000000000000270"), 0x4000003770),
            (hex!("0122222222333333334444444455000000260000000000004020"), 0x40000037c0),
            (hex!("0122222222333333334444444455000000270000000000000280"), 0x4000003810),
            (hex!("0122222222333333334444444455000000280000000000000290"), 0x4000003860),
            (hex!("0122222222333333334444444455000000280000000000007c00"), 0x40000038b0),
            (hex!("01222222223333333344444444550000002900000000000002a0"), 0x4000003900),
            (hex!("01222222223333333344444444550000002a00000000000002b0"), 0x4000003950),
            (hex!("01222222223333333344444444550000002b00000000000002c0"), 0x40000039a0),
            (hex!("01222222223333333344444444550000002c00000000000002d0"), 0x40000039f0),
            (hex!("01222222223333333344444444550000002c00000000000041b0"), 0x4000003a40),
            (hex!("01222222223333333344444444550000002c0000000000004c30"), 0x4000003a90),
            (hex!("01222222223333333344444444550000002d00000000000002e0"), 0x4000003ae0),
            (hex!("01222222223333333344444444550000002d0000000000005e40"), 0x4000003b30),
            (hex!("01222222223333333344444444550000002d0000000000006990"), 0x4000003b80),
            (hex!("01222222223333333344444444550000002e00000000000002f0"), 0x4000003bd0),
            (hex!("01222222223333333344444444550000002f0000000000000300"), 0x4000003c20),
            (hex!("01222222223333333344444444550000002f0000000000004a70"), 0x4000003c70),
            (hex!("01222222223333333344444444550000002f0000000000006b40"), 0x4000003cc0),
            (hex!("0122222222333333334444444455000000300000000000000310"), 0x4000003d10),
            (hex!("0122222222333333334444444455000000310000000000000320"), 0x4000003d60),
            (hex!("0122222222333333334444444455000000320000000000000330"), 0x4000003db0),
            (hex!("01222222223333333344444444550000003200000000000041a0"), 0x4000003e00),
            (hex!("0122222222333333334444444455000000320000000000007340"), 0x4000003e50),
            (hex!("0122222222333333334444444455000000320000000000007730"), 0x4000003ea0),
            (hex!("0122222222333333334444444455000000330000000000000340"), 0x4000003ef0),
            (hex!("01222222223333333344444444550000003300000000000055a0"), 0x4000003f40),
            (hex!("0122222222333333334444444455000000340000000000000350"), 0x4000003f90),
            (hex!("0122222222333333334444444455000000350000000000000360"), 0x4000003fe0),
            (hex!("01222222223333333344444444550000003500000000000077a0"), 0x4000004030),
            (hex!("0122222222333333334444444455000000360000000000000370"), 0x4000004080),
            (hex!("0122222222333333334444444455000000370000000000000380"), 0x40000040d0),
            (hex!("0122222222333333334444444455000000380000000000000390"), 0x4000004120),
            (hex!("01222222223333333344444444550000003900000000000003a0"), 0x4000004170),
            (hex!("01222222223333333344444444550000003a00000000000003b0"), 0x40000041c0),
            (hex!("01222222223333333344444444550000003a00000000000071c0"), 0x4000004210),
            (hex!("01222222223333333344444444550000003b00000000000003c0"), 0x4000004260),
            (hex!("01222222223333333344444444550000003c00000000000003d0"), 0x40000042b0),
            (hex!("01222222223333333344444444550000003d00000000000003e0"), 0x4000004300),
            (hex!("01222222223333333344444444550000003e00000000000003f0"), 0x4000004350),
            (hex!("01222222223333333344444444550000003e00000000000062e0"), 0x40000043a0),
            (hex!("01222222223333333344444444550000003f0000000000000400"), 0x40000043f0),
            (hex!("0122222222333333334444444455000000400000000000000410"), 0x4000004440),
            (hex!("0122222222333333334444444455000000400000000000004460"), 0x4000004490),
            (hex!("0122222222333333334444444455000000400000000000005b90"), 0x40000044e0),
            (hex!("01222222223333333344444444550000004000000000000079b0"), 0x4000004530),
            (hex!("0122222222333333334444444455000000410000000000000420"), 0x4000004580),
            (hex!("0122222222333333334444444455000000420000000000000430"), 0x40000045d0),
            (hex!("0122222222333333334444444455000000420000000000005640"), 0x4000004620),
            (hex!("0122222222333333334444444455000000430000000000000440"), 0x4000004670),
            (hex!("01222222223333333344444444550000004300000000000072a0"), 0x40000046c0),
            (hex!("0122222222333333334444444455000000440000000000000450"), 0x4000004710),
            (hex!("0122222222333333334444444455000000450000000000000460"), 0x4000004760),
            (hex!("0122222222333333334444444455000000450000000000005750"), 0x40000047b0),
            (hex!("01222222223333333344444444550000004500000000000077b0"), 0x4000004800),
            (hex!("0122222222333333334444444455000000460000000000000470"), 0x4000004850),
            (hex!("0122222222333333334444444455000000470000000000000480"), 0x40000048a0),
            (hex!("0122222222333333334444444455000000480000000000000490"), 0x40000048f0),
            (hex!("01222222223333333344444444550000004800000000000069e0"), 0x4000004940),
            (hex!("01222222223333333344444444550000004900000000000004a0"), 0x4000004990),
            (hex!("0122222222333333334444444455000000490000000000007370"), 0x40000049e0),
            (hex!("01222222223333333344444444550000004a00000000000004b0"), 0x4000004a30),
            (hex!("01222222223333333344444444550000004a0000000000005cb0"), 0x4000004a80),
            (hex!("01222222223333333344444444550000004b00000000000004c0"), 0x4000004ad0),
            (hex!("01222222223333333344444444550000004c00000000000004d0"), 0x4000004b20),
            (hex!("01222222223333333344444444550000004c0000000000004880"), 0x4000004b70),
            (hex!("01222222223333333344444444550000004c0000000000007a40"), 0x4000004bc0),
            (hex!("01222222223333333344444444550000004d00000000000004e0"), 0x4000004c10),
            (hex!("01222222223333333344444444550000004d0000000000006390"), 0x4000004c60),
            (hex!("01222222223333333344444444550000004e00000000000004f0"), 0x4000004cb0),
            (hex!("01222222223333333344444444550000004e0000000000004db0"), 0x4000004d00),
            (hex!("01222222223333333344444444550000004f0000000000000500"), 0x4000004d50),
            (hex!("0122222222333333334444444455000000500000000000000510"), 0x4000004da0),
            (hex!("0122222222333333334444444455000000510000000000000520"), 0x4000004df0),
            (hex!("01222222223333333344444444550000005100000000000069c0"), 0x4000004e40),
            (hex!("0122222222333333334444444455000000520000000000000530"), 0x4000004e90),
            (hex!("0122222222333333334444444455000000520000000000006e60"), 0x4000004ee0),
            (hex!("01222222223333333344444444550000005200000000000070c0"), 0x4000004f30),
            (hex!("0122222222333333334444444455000000530000000000000540"), 0x4000004f80),
            (hex!("0122222222333333334444444455000000530000000000005840"), 0x4000004fd0),
            (hex!("0122222222333333334444444455000000540000000000000550"), 0x4000005020),
            (hex!("01222222223333333344444444550000005400000000000043e0"), 0x4000005070),
            (hex!("01222222223333333344444444550000005400000000000074e0"), 0x40000050c0),
            (hex!("0122222222333333334444444455000000550000000000000560"), 0x4000005110),
            (hex!("0122222222333333334444444455000000550000000000003ee0"), 0x4000005160),
            (hex!("0122222222333333334444444455000000560000000000000570"), 0x40000051b0),
            (hex!("0122222222333333334444444455000000570000000000000580"), 0x4000005200),
            (hex!("0122222222333333334444444455000000570000000000007030"), 0x4000005250),
            (hex!("0122222222333333334444444455000000580000000000000590"), 0x40000052a0),
            (hex!("0122222222333333334444444455000000580000000000005340"), 0x40000052f0),
            (hex!("01222222223333333344444444550000005800000000000059f0"), 0x4000005340),
            (hex!("0122222222333333334444444455000000580000000000006930"), 0x4000005390),
            (hex!("01222222223333333344444444550000005900000000000005a0"), 0x40000053e0),
            (hex!("0122222222333333334444444455000000590000000000003f90"), 0x4000005430),
            (hex!("01222222223333333344444444550000005a00000000000005b0"), 0x4000005480),
            (hex!("01222222223333333344444444550000005b00000000000005c0"), 0x40000054d0),
            (hex!("01222222223333333344444444550000005b00000000000062c0"), 0x4000005520),
            (hex!("01222222223333333344444444550000005c00000000000005d0"), 0x4000005570),
            (hex!("01222222223333333344444444550000005c0000000000005a70"), 0x40000055c0),
            (hex!("01222222223333333344444444550000005c0000000000005dd0"), 0x4000005610),
            (hex!("01222222223333333344444444550000005d00000000000005e0"), 0x4000005660),
            (hex!("01222222223333333344444444550000005d0000000000005730"), 0x40000056b0),
            (hex!("01222222223333333344444444550000005e00000000000005f0"), 0x4000005700),
            (hex!("01222222223333333344444444550000005e0000000000004f40"), 0x4000005750),
            (hex!("01222222223333333344444444550000005f0000000000000600"), 0x40000057a0),
            (hex!("0122222222333333334444444455000000600000000000000610"), 0x40000057f0),
            (hex!("0122222222333333334444444455000000600000000000007c40"), 0x4000005840),
            (hex!("0122222222333333334444444455000000610000000000000620"), 0x4000005890),
            (hex!("0122222222333333334444444455000000610000000000007860"), 0x40000058e0),
            (hex!("0122222222333333334444444455000000620000000000000630"), 0x4000005930),
            (hex!("0122222222333333334444444455000000620000000000005050"), 0x4000005980),
            (hex!("0122222222333333334444444455000000630000000000000640"), 0x40000059d0),
            (hex!("0122222222333333334444444455000000640000000000000650"), 0x4000005a20),
            (hex!("0122222222333333334444444455000000650000000000000660"), 0x4000005a70),
            (hex!("0122222222333333334444444455000000650000000000005330"), 0x4000005ac0),
            (hex!("0122222222333333334444444455000000660000000000000670"), 0x4000005b10),
            (hex!("0122222222333333334444444455000000660000000000004e20"), 0x4000005b60),
            (hex!("0122222222333333334444444455000000660000000000005ee0"), 0x4000005bb0),
            (hex!("0122222222333333334444444455000000660000000000006360"), 0x4000005c00),
            (hex!("0122222222333333334444444455000000670000000000000680"), 0x4000005c50),
            (hex!("0122222222333333334444444455000000670000000000004040"), 0x4000005ca0),
            (hex!("0122222222333333334444444455000000680000000000000690"), 0x4000005cf0),
            (hex!("0122222222333333334444444455000000680000000000003f80"), 0x4000005d40),
            (hex!("01222222223333333344444444550000006800000000000041e0"), 0x4000005d90),
            (hex!("01222222223333333344444444550000006900000000000006a0"), 0x4000005de0),
            (hex!("0122222222333333334444444455000000690000000000006080"), 0x4000005e30),
            (hex!("01222222223333333344444444550000006a00000000000006b0"), 0x4000005e80),
            (hex!("01222222223333333344444444550000006a00000000000042f0"), 0x4000005ed0),
            (hex!("01222222223333333344444444550000006b00000000000006c0"), 0x4000005f20),
            (hex!("01222222223333333344444444550000006b00000000000052f0"), 0x4000005f70),
            (hex!("01222222223333333344444444550000006b0000000000005980"), 0x4000005fc0),
            (hex!("01222222223333333344444444550000006b0000000000006170"), 0x4000006010),
            (hex!("01222222223333333344444444550000006c00000000000006d0"), 0x4000006060),
            (hex!("01222222223333333344444444550000006d00000000000006e0"), 0x40000060b0),
            (hex!("01222222223333333344444444550000006d0000000000006fb0"), 0x4000006100),
            (hex!("01222222223333333344444444550000006e00000000000006f0"), 0x4000006150),
            (hex!("01222222223333333344444444550000006e00000000000065b0"), 0x40000061a0),
            (hex!("01222222223333333344444444550000006e0000000000007970"), 0x40000061f0),
            (hex!("01222222223333333344444444550000006f0000000000000700"), 0x4000006240),
            (hex!("01222222223333333344444444550000006f0000000000005900"), 0x4000006290),
            (hex!("01222222223333333344444444550000006f0000000000006d90"), 0x40000062e0),
            (hex!("0122222222333333334444444455000000700000000000000710"), 0x4000006330),
            (hex!("01222222223333333344444444550000007000000000000045c0"), 0x4000006380),
            (hex!("0122222222333333334444444455000000700000000000004d40"), 0x40000063d0),
            (hex!("0122222222333333334444444455000000710000000000000720"), 0x4000006420),
            (hex!("0122222222333333334444444455000000710000000000004dc0"), 0x4000006470),
            (hex!("0122222222333333334444444455000000710000000000007550"), 0x40000064c0),
            (hex!("0122222222333333334444444455000000720000000000000730"), 0x4000006510),
            (hex!("0122222222333333334444444455000000720000000000003ec0"), 0x4000006560),
            (hex!("01222222223333333344444444550000007200000000000045a0"), 0x40000065b0),
            (hex!("0122222222333333334444444455000000720000000000006770"), 0x4000006600),
            (hex!("0122222222333333334444444455000000720000000000006bc0"), 0x4000006650),
            (hex!("0122222222333333334444444455000000730000000000000740"), 0x40000066a0),
            (hex!("0122222222333333334444444455000000730000000000005250"), 0x40000066f0),
            (hex!("01222222223333333344444444550000007300000000000075f0"), 0x4000006740),
            (hex!("0122222222333333334444444455000000740000000000000750"), 0x4000006790),
            (hex!("0122222222333333334444444455000000740000000000003ff0"), 0x40000067e0),
            (hex!("01222222223333333344444444550000007400000000000079e0"), 0x4000006830),
            (hex!("0122222222333333334444444455000000750000000000000760"), 0x4000006880),
            (hex!("0122222222333333334444444455000000750000000000004310"), 0x40000068d0),
            (hex!("0122222222333333334444444455000000760000000000000770"), 0x4000006920),
            (hex!("0122222222333333334444444455000000770000000000000780"), 0x4000006970),
            (hex!("01222222223333333344444444550000007700000000000062f0"), 0x40000069c0),
            (hex!("0122222222333333334444444455000000770000000000006940"), 0x4000006a10),
            (hex!("0122222222333333334444444455000000780000000000000790"), 0x4000006a60),
            (hex!("01222222223333333344444444550000007900000000000007a0"), 0x4000006ab0),
            (hex!("0122222222333333334444444455000000790000000000007af0"), 0x4000006b00),
            (hex!("01222222223333333344444444550000007a00000000000007b0"), 0x4000006b50),
            (hex!("01222222223333333344444444550000007b00000000000007c0"), 0x4000006ba0),
            (hex!("01222222223333333344444444550000007b00000000000067e0"), 0x4000006bf0),
            (hex!("01222222223333333344444444550000007b0000000000007890"), 0x4000006c40),
            (hex!("01222222223333333344444444550000007c00000000000007d0"), 0x4000006c90),
            (hex!("01222222223333333344444444550000007d00000000000007e0"), 0x4000006ce0),
            (hex!("01222222223333333344444444550000007e00000000000007f0"), 0x4000006d30),
            (hex!("01222222223333333344444444550000007f0000000000000800"), 0x4000006d80),
            (hex!("01222222223333333344444444550000007f0000000000005be0"), 0x4000006dd0),
            (hex!("0122222222333333334444444455000000800000000000000810"), 0x4000006e20),
            (hex!("0122222222333333334444444455000000810000000000000820"), 0x4000006e70),
            (hex!("0122222222333333334444444455000000810000000000007190"), 0x4000006ec0),
            (hex!("0122222222333333334444444455000000820000000000000830"), 0x4000006f10),
            (hex!("0122222222333333334444444455000000820000000000004ab0"), 0x4000006f60),
            (hex!("0122222222333333334444444455000000830000000000000840"), 0x4000006fb0),
            (hex!("0122222222333333334444444455000000830000000000006720"), 0x4000007000),
            (hex!("0122222222333333334444444455000000840000000000000850"), 0x4000007050),
            (hex!("0122222222333333334444444455000000850000000000000860"), 0x40000070a0),
            (hex!("01222222223333333344444444550000008500000000000054f0"), 0x40000070f0),
            (hex!("0122222222333333334444444455000000850000000000007920"), 0x4000007140),
            (hex!("0122222222333333334444444455000000860000000000000870"), 0x4000007190),
            (hex!("01222222223333333344444444550000008600000000000060e0"), 0x40000071e0),
            (hex!("0122222222333333334444444455000000860000000000006be0"), 0x4000007230),
            (hex!("0122222222333333334444444455000000870000000000000880"), 0x4000007280),
            (hex!("0122222222333333334444444455000000870000000000006820"), 0x40000072d0),
            (hex!("0122222222333333334444444455000000880000000000000890"), 0x4000007320),
            (hex!("01222222223333333344444444550000008900000000000008a0"), 0x4000007370),
            (hex!("0122222222333333334444444455000000890000000000007c30"), 0x40000073c0),
            (hex!("01222222223333333344444444550000008a00000000000008b0"), 0x4000007410),
            (hex!("01222222223333333344444444550000008b00000000000008c0"), 0x4000007460),
            (hex!("01222222223333333344444444550000008b0000000000005910"), 0x40000074b0),
            (hex!("01222222223333333344444444550000008b0000000000006fe0"), 0x4000007500),
            (hex!("01222222223333333344444444550000008c00000000000008d0"), 0x4000007550),
            (hex!("01222222223333333344444444550000008c0000000000006800"), 0x40000075a0),
            (hex!("01222222223333333344444444550000008d00000000000008e0"), 0x40000075f0),
            (hex!("01222222223333333344444444550000008d0000000000005810"), 0x4000007640),
            (hex!("01222222223333333344444444550000008d0000000000007c90"), 0x4000007690),
            (hex!("01222222223333333344444444550000008e00000000000008f0"), 0x40000076e0),
            (hex!("01222222223333333344444444550000008e00000000000058f0"), 0x4000007730),
            (hex!("01222222223333333344444444550000008f0000000000000900"), 0x4000007780),
            (hex!("01222222223333333344444444550000008f0000000000005a30"), 0x40000077d0),
            (hex!("0122222222333333334444444455000000900000000000000910"), 0x4000007820),
            (hex!("0122222222333333334444444455000000900000000000006130"), 0x4000007870),
            (hex!("0122222222333333334444444455000000900000000000006550"), 0x40000078c0),
            (hex!("0122222222333333334444444455000000910000000000000920"), 0x4000007910),
            (hex!("01222222223333333344444444550000009100000000000079f0"), 0x4000007960),
            (hex!("0122222222333333334444444455000000920000000000000930"), 0x40000079b0),
            (hex!("0122222222333333334444444455000000920000000000005620"), 0x4000007a00),
            (hex!("0122222222333333334444444455000000920000000000005e90"), 0x4000007a50),
            (hex!("01222222223333333344444444550000009200000000000063d0"), 0x4000007aa0),
            (hex!("01222222223333333344444444550000009200000000000076c0"), 0x4000007af0),
            (hex!("0122222222333333334444444455000000930000000000000940"), 0x4000007b40),
            (hex!("01222222223333333344444444550000009300000000000044e0"), 0x4000007b90),
            (hex!("0122222222333333334444444455000000940000000000000950"), 0x4000007be0),
            (hex!("0122222222333333334444444455000000940000000000007a30"), 0x4000007c30),
            (hex!("0122222222333333334444444455000000950000000000000960"), 0x4000007c80),
            (hex!("0122222222333333334444444455000000950000000000007a70"), 0x4000007cd0),
            (hex!("0122222222333333334444444455000000960000000000000970"), 0x4000007d20),
            (hex!("0122222222333333334444444455000000970000000000000980"), 0x4000007d70),
            (hex!("0122222222333333334444444455000000970000000000007330"), 0x4000007dc0),
            (hex!("0122222222333333334444444455000000980000000000000990"), 0x4000007e10),
            (hex!("0122222222333333334444444455000000980000000000005af0"), 0x4000007e60),
            (hex!("0122222222333333334444444455000000980000000000007ae0"), 0x4000007eb0),
            (hex!("01222222223333333344444444550000009900000000000009a0"), 0x4000007f00),
            (hex!("0122222222333333334444444455000000990000000000005160"), 0x4000007f50),
            (hex!("0122222222333333334444444455000000990000000000006850"), 0x4000007fa0),
            (hex!("01222222223333333344444444550000009a00000000000009b0"), 0x4000007ff0),
            (hex!("01222222223333333344444444550000009b00000000000009c0"), 0x4000008040),
            (hex!("01222222223333333344444444550000009b0000000000005010"), 0x4000008090),
            (hex!("01222222223333333344444444550000009c00000000000009d0"), 0x40000080e0),
            (hex!("01222222223333333344444444550000009c00000000000042e0"), 0x4000008130),
            (hex!("01222222223333333344444444550000009d00000000000009e0"), 0x4000008180),
            (hex!("01222222223333333344444444550000009d00000000000057f0"), 0x40000081d0),
            (hex!("01222222223333333344444444550000009e00000000000009f0"), 0x4000008220),
            (hex!("01222222223333333344444444550000009e0000000000004ef0"), 0x4000008270),
            (hex!("01222222223333333344444444550000009f0000000000000a00"), 0x40000082c0),
            (hex!("01222222223333333344444444550000009f0000000000006110"), 0x4000008310),
            (hex!("0122222222333333334444444455000000a00000000000000a10"), 0x4000008360),
            (hex!("0122222222333333334444444455000000a10000000000000a20"), 0x40000083b0),
            (hex!("0122222222333333334444444455000000a100000000000040d0"), 0x4000008400),
            (hex!("0122222222333333334444444455000000a10000000000007670"), 0x4000008450),
            (hex!("0122222222333333334444444455000000a20000000000000a30"), 0x40000084a0),
            (hex!("0122222222333333334444444455000000a200000000000074d0"), 0x40000084f0),
            (hex!("0122222222333333334444444455000000a30000000000000a40"), 0x4000008540),
            (hex!("0122222222333333334444444455000000a30000000000004c90"), 0x4000008590),
            (hex!("0122222222333333334444444455000000a40000000000000a50"), 0x40000085e0),
            (hex!("0122222222333333334444444455000000a50000000000000a60"), 0x4000008630),
            (hex!("0122222222333333334444444455000000a60000000000000a70"), 0x4000008680),
            (hex!("0122222222333333334444444455000000a60000000000006d80"), 0x40000086d0),
            (hex!("0122222222333333334444444455000000a60000000000007830"), 0x4000008720),
            (hex!("0122222222333333334444444455000000a70000000000000a80"), 0x4000008770),
            (hex!("0122222222333333334444444455000000a700000000000064f0"), 0x40000087c0),
            (hex!("0122222222333333334444444455000000a80000000000000a90"), 0x4000008810),
            (hex!("0122222222333333334444444455000000a90000000000000aa0"), 0x4000008860),
            (hex!("0122222222333333334444444455000000a90000000000005e30"), 0x40000088b0),
            (hex!("0122222222333333334444444455000000aa0000000000000ab0"), 0x4000008900),
            (hex!("0122222222333333334444444455000000ab0000000000000ac0"), 0x4000008950),
            (hex!("0122222222333333334444444455000000ac0000000000000ad0"), 0x40000089a0),
            (hex!("0122222222333333334444444455000000ac0000000000006d20"), 0x40000089f0),
            (hex!("0122222222333333334444444455000000ac0000000000007000"), 0x4000008a40),
            (hex!("0122222222333333334444444455000000ad0000000000000ae0"), 0x4000008a90),
            (hex!("0122222222333333334444444455000000ae0000000000000af0"), 0x4000008ae0),
            (hex!("0122222222333333334444444455000000ae0000000000004a10"), 0x4000008b30),
            (hex!("0122222222333333334444444455000000af0000000000000b00"), 0x4000008b80),
            (hex!("0122222222333333334444444455000000af0000000000004e10"), 0x4000008bd0),
            (hex!("0122222222333333334444444455000000b00000000000000b10"), 0x4000008c20),
            (hex!("0122222222333333334444444455000000b00000000000004280"), 0x4000008c70),
            (hex!("0122222222333333334444444455000000b000000000000077e0"), 0x4000008cc0),
            (hex!("0122222222333333334444444455000000b10000000000000b20"), 0x4000008d10),
            (hex!("0122222222333333334444444455000000b20000000000000b30"), 0x4000008d60),
            (hex!("0122222222333333334444444455000000b30000000000000b40"), 0x4000008db0),
            (hex!("0122222222333333334444444455000000b30000000000004bc0"), 0x4000008e00),
            (hex!("0122222222333333334444444455000000b40000000000000b50"), 0x4000008e50),
            (hex!("0122222222333333334444444455000000b50000000000000b60"), 0x4000008ea0),
            (hex!("0122222222333333334444444455000000b50000000000004fa0"), 0x4000008ef0),
            (hex!("0122222222333333334444444455000000b50000000000006a60"), 0x4000008f40),
            (hex!("0122222222333333334444444455000000b60000000000000b70"), 0x4000008f90),
            (hex!("0122222222333333334444444455000000b60000000000005630"), 0x4000008fe0),
            (hex!("0122222222333333334444444455000000b70000000000000b80"), 0x4000009030),
            (hex!("0122222222333333334444444455000000b80000000000000b90"), 0x4000009080),
            (hex!("0122222222333333334444444455000000b80000000000006f80"), 0x40000090d0),
            (hex!("0122222222333333334444444455000000b90000000000000ba0"), 0x4000009120),
            (hex!("0122222222333333334444444455000000ba0000000000000bb0"), 0x4000009170),
            (hex!("0122222222333333334444444455000000bb0000000000000bc0"), 0x40000091c0),
            (hex!("0122222222333333334444444455000000bb00000000000047c0"), 0x4000009210),
            (hex!("0122222222333333334444444455000000bb0000000000006060"), 0x4000009260),
            (hex!("0122222222333333334444444455000000bc0000000000000bd0"), 0x40000092b0),
            (hex!("0122222222333333334444444455000000bd0000000000000be0"), 0x4000009300),
            (hex!("0122222222333333334444444455000000bd0000000000004e80"), 0x4000009350),
            (hex!("0122222222333333334444444455000000be0000000000000bf0"), 0x40000093a0),
            (hex!("0122222222333333334444444455000000bf0000000000000c00"), 0x40000093f0),
            (hex!("0122222222333333334444444455000000bf00000000000047a0"), 0x4000009440),
            (hex!("0122222222333333334444444455000000bf0000000000006da0"), 0x4000009490),
            (hex!("0122222222333333334444444455000000c00000000000000c10"), 0x40000094e0),
            (hex!("0122222222333333334444444455000000c10000000000000c20"), 0x4000009530),
            (hex!("0122222222333333334444444455000000c20000000000000c30"), 0x4000009580),
            (hex!("0122222222333333334444444455000000c20000000000004bd0"), 0x40000095d0),
            (hex!("0122222222333333334444444455000000c20000000000006ac0"), 0x4000009620),
            (hex!("0122222222333333334444444455000000c30000000000000c40"), 0x4000009670),
            (hex!("0122222222333333334444444455000000c30000000000004660"), 0x40000096c0),
            (hex!("0122222222333333334444444455000000c40000000000000c50"), 0x4000009710),
            (hex!("0122222222333333334444444455000000c50000000000000c60"), 0x4000009760),
            (hex!("0122222222333333334444444455000000c60000000000000c70"), 0x40000097b0),
            (hex!("0122222222333333334444444455000000c60000000000005880"), 0x4000009800),
            (hex!("0122222222333333334444444455000000c60000000000006b70"), 0x4000009850),
            (hex!("0122222222333333334444444455000000c70000000000000c80"), 0x40000098a0),
            (hex!("0122222222333333334444444455000000c80000000000000c90"), 0x40000098f0),
            (hex!("0122222222333333334444444455000000c80000000000005310"), 0x4000009940),
            (hex!("0122222222333333334444444455000000c80000000000005db0"), 0x4000009990),
            (hex!("0122222222333333334444444455000000c80000000000007040"), 0x40000099e0),
            (hex!("0122222222333333334444444455000000c80000000000007290"), 0x4000009a30),
            (hex!("0122222222333333334444444455000000c90000000000000ca0"), 0x4000009a80),
            (hex!("0122222222333333334444444455000000c90000000000004fe0"), 0x4000009ad0),
            (hex!("0122222222333333334444444455000000ca0000000000000cb0"), 0x4000009b20),
            (hex!("0122222222333333334444444455000000ca0000000000006140"), 0x4000009b70),
            (hex!("0122222222333333334444444455000000ca0000000000007700"), 0x4000009bc0),
            (hex!("0122222222333333334444444455000000cb0000000000000cc0"), 0x4000009c10),
            (hex!("0122222222333333334444444455000000cc0000000000000cd0"), 0x4000009c60),
            (hex!("0122222222333333334444444455000000cd0000000000000ce0"), 0x4000009cb0),
            (hex!("0122222222333333334444444455000000cd0000000000003f20"), 0x4000009d00),
            (hex!("0122222222333333334444444455000000cd00000000000040f0"), 0x4000009d50),
            (hex!("0122222222333333334444444455000000cd0000000000004ec0"), 0x4000009da0),
            (hex!("0122222222333333334444444455000000ce0000000000000cf0"), 0x4000009df0),
            (hex!("0122222222333333334444444455000000ce0000000000007200"), 0x4000009e40),
            (hex!("0122222222333333334444444455000000cf0000000000000d00"), 0x4000009e90),
            (hex!("0122222222333333334444444455000000cf00000000000046a0"), 0x4000009ee0),
            (hex!("0122222222333333334444444455000000cf0000000000005960"), 0x4000009f30),
            (hex!("0122222222333333334444444455000000d00000000000000d10"), 0x4000009f80),
            (hex!("0122222222333333334444444455000000d00000000000005f30"), 0x4000009fd0),
            (hex!("0122222222333333334444444455000000d10000000000000d20"), 0x400000a020),
            (hex!("0122222222333333334444444455000000d10000000000007a00"), 0x400000a070),
            (hex!("0122222222333333334444444455000000d20000000000000d30"), 0x400000a0c0),
            (hex!("0122222222333333334444444455000000d30000000000000d40"), 0x400000a110),
            (hex!("0122222222333333334444444455000000d40000000000000d50"), 0x400000a160),
            (hex!("0122222222333333334444444455000000d50000000000000d60"), 0x400000a1b0),
            (hex!("0122222222333333334444444455000000d50000000000004960"), 0x400000a200),
            (hex!("0122222222333333334444444455000000d500000000000055d0"), 0x400000a250),
            (hex!("0122222222333333334444444455000000d500000000000067d0"), 0x400000a2a0),
            (hex!("0122222222333333334444444455000000d60000000000000d70"), 0x400000a2f0),
            (hex!("0122222222333333334444444455000000d70000000000000d80"), 0x400000a340),
            (hex!("0122222222333333334444444455000000d80000000000000d90"), 0x400000a390),
            (hex!("0122222222333333334444444455000000d800000000000065f0"), 0x400000a3e0),
            (hex!("0122222222333333334444444455000000d90000000000000da0"), 0x400000a430),
            (hex!("0122222222333333334444444455000000d90000000000004980"), 0x400000a480),
            (hex!("0122222222333333334444444455000000da0000000000000db0"), 0x400000a4d0),
            (hex!("0122222222333333334444444455000000da00000000000048c0"), 0x400000a520),
            (hex!("0122222222333333334444444455000000da00000000000072c0"), 0x400000a570),
            (hex!("0122222222333333334444444455000000da00000000000076b0"), 0x400000a5c0),
            (hex!("0122222222333333334444444455000000db0000000000000dc0"), 0x400000a610),
            (hex!("0122222222333333334444444455000000dc0000000000000dd0"), 0x400000a660),
            (hex!("0122222222333333334444444455000000dc00000000000040a0"), 0x400000a6b0),
            (hex!("0122222222333333334444444455000000dc00000000000074c0"), 0x400000a700),
            (hex!("0122222222333333334444444455000000dd0000000000000de0"), 0x400000a750),
            (hex!("0122222222333333334444444455000000dd0000000000004e50"), 0x400000a7a0),
            (hex!("0122222222333333334444444455000000dd0000000000007270"), 0x400000a7f0),
            (hex!("0122222222333333334444444455000000de0000000000000df0"), 0x400000a840),
            (hex!("0122222222333333334444444455000000de00000000000078d0"), 0x400000a890),
            (hex!("0122222222333333334444444455000000df0000000000000e00"), 0x400000a8e0),
            (hex!("0122222222333333334444444455000000df0000000000004d30"), 0x400000a930),
            (hex!("0122222222333333334444444455000000df0000000000006c30"), 0x400000a980),
            (hex!("0122222222333333334444444455000000e00000000000000e10"), 0x400000a9d0),
            (hex!("0122222222333333334444444455000000e00000000000005d30"), 0x400000aa20),
            (hex!("0122222222333333334444444455000000e10000000000000e20"), 0x400000aa70),
            (hex!("0122222222333333334444444455000000e10000000000004610"), 0x400000aac0),
            (hex!("0122222222333333334444444455000000e100000000000051d0"), 0x400000ab10),
            (hex!("0122222222333333334444444455000000e10000000000005f10"), 0x400000ab60),
            (hex!("0122222222333333334444444455000000e20000000000000e30"), 0x400000abb0),
            (hex!("0122222222333333334444444455000000e20000000000007a90"), 0x400000ac00),
            (hex!("0122222222333333334444444455000000e30000000000000e40"), 0x400000ac50),
            (hex!("0122222222333333334444444455000000e30000000000005ae0"), 0x400000aca0),
            (hex!("0122222222333333334444444455000000e40000000000000e50"), 0x400000acf0),
            (hex!("0122222222333333334444444455000000e50000000000000e60"), 0x400000ad40),
            (hex!("0122222222333333334444444455000000e50000000000004700"), 0x400000ad90),
            (hex!("0122222222333333334444444455000000e500000000000065d0"), 0x400000ade0),
            (hex!("0122222222333333334444444455000000e60000000000000e70"), 0x400000ae30),
            (hex!("0122222222333333334444444455000000e60000000000004fd0"), 0x400000ae80),
            (hex!("0122222222333333334444444455000000e70000000000000e80"), 0x400000aed0),
            (hex!("0122222222333333334444444455000000e70000000000005150"), 0x400000af20),
            (hex!("0122222222333333334444444455000000e70000000000005920"), 0x400000af70),
            (hex!("0122222222333333334444444455000000e80000000000000e90"), 0x400000afc0),
            (hex!("0122222222333333334444444455000000e80000000000004320"), 0x400000b010),
            (hex!("0122222222333333334444444455000000e80000000000005ec0"), 0x400000b060),
            (hex!("0122222222333333334444444455000000e90000000000000ea0"), 0x400000b0b0),
            (hex!("0122222222333333334444444455000000e900000000000043b0"), 0x400000b100),
            (hex!("0122222222333333334444444455000000ea0000000000000eb0"), 0x400000b150),
            (hex!("0122222222333333334444444455000000ea0000000000003ea0"), 0x400000b1a0),
            (hex!("0122222222333333334444444455000000ea0000000000004f50"), 0x400000b1f0),
            (hex!("0122222222333333334444444455000000ea0000000000007520"), 0x400000b240),
            (hex!("0122222222333333334444444455000000eb0000000000000ec0"), 0x400000b290),
            (hex!("0122222222333333334444444455000000ec0000000000000ed0"), 0x400000b2e0),
            (hex!("0122222222333333334444444455000000ec0000000000006670"), 0x400000b330),
            (hex!("0122222222333333334444444455000000ed0000000000000ee0"), 0x400000b380),
            (hex!("0122222222333333334444444455000000ee0000000000000ef0"), 0x400000b3d0),
            (hex!("0122222222333333334444444455000000ee0000000000004d10"), 0x400000b420),
            (hex!("0122222222333333334444444455000000ef0000000000000f00"), 0x400000b470),
            (hex!("0122222222333333334444444455000000f00000000000000f10"), 0x400000b4c0),
            (hex!("0122222222333333334444444455000000f00000000000007220"), 0x400000b510),
            (hex!("0122222222333333334444444455000000f00000000000007540"), 0x400000b560),
            (hex!("0122222222333333334444444455000000f10000000000000f20"), 0x400000b5b0),
            (hex!("0122222222333333334444444455000000f100000000000066f0"), 0x400000b600),
            (hex!("0122222222333333334444444455000000f20000000000000f30"), 0x400000b650),
            (hex!("0122222222333333334444444455000000f20000000000007810"), 0x400000b6a0),
            (hex!("0122222222333333334444444455000000f30000000000000f40"), 0x400000b6f0),
            (hex!("0122222222333333334444444455000000f30000000000007b70"), 0x400000b740),
            (hex!("0122222222333333334444444455000000f40000000000000f50"), 0x400000b790),
            (hex!("0122222222333333334444444455000000f400000000000059c0"), 0x400000b7e0),
            (hex!("0122222222333333334444444455000000f50000000000000f60"), 0x400000b830),
            (hex!("0122222222333333334444444455000000f50000000000003fb0"), 0x400000b880),
            (hex!("0122222222333333334444444455000000f50000000000005740"), 0x400000b8d0),
            (hex!("0122222222333333334444444455000000f500000000000064d0"), 0x400000b920),
            (hex!("0122222222333333334444444455000000f50000000000006960"), 0x400000b970),
            (hex!("0122222222333333334444444455000000f60000000000000f70"), 0x400000b9c0),
            (hex!("0122222222333333334444444455000000f60000000000006d00"), 0x400000ba10),
            (hex!("0122222222333333334444444455000000f70000000000000f80"), 0x400000ba60),
            (hex!("0122222222333333334444444455000000f80000000000000f90"), 0x400000bab0),
            (hex!("0122222222333333334444444455000000f90000000000000fa0"), 0x400000bb00),
            (hex!("0122222222333333334444444455000000fa0000000000000fb0"), 0x400000bb50),
            (hex!("0122222222333333334444444455000000fa00000000000067b0"), 0x400000bba0),
            (hex!("0122222222333333334444444455000000fb0000000000000fc0"), 0x400000bbf0),
            (hex!("0122222222333333334444444455000000fb0000000000004eb0"), 0x400000bc40),
            (hex!("0122222222333333334444444455000000fb0000000000006ef0"), 0x400000bc90),
            (hex!("0122222222333333334444444455000000fc0000000000000fd0"), 0x400000bce0),
            (hex!("0122222222333333334444444455000000fc0000000000004470"), 0x400000bd30),
            (hex!("0122222222333333334444444455000000fc0000000000005940"), 0x400000bd80),
            (hex!("0122222222333333334444444455000000fd0000000000000fe0"), 0x400000bdd0),
            (hex!("0122222222333333334444444455000000fe0000000000000ff0"), 0x400000be20),
            (hex!("0122222222333333334444444455000000ff0000000000001000"), 0x400000be70),
            (hex!("0122222222333333334444444455000000ff0000000000005690"), 0x400000bec0),
            (hex!("0122222222333333334444444455000001000000000000001010"), 0x400000bf10),
            (hex!("0122222222333333334444444455000001000000000000005210"), 0x400000bf60),
            (hex!("01222222223333333344444444550000010000000000000070a0"), 0x400000bfb0),
            (hex!("0122222222333333334444444455000001010000000000001020"), 0x400000c000),
            (hex!("0122222222333333334444444455000001010000000000006b80"), 0x400000c050),
            (hex!("0122222222333333334444444455000001020000000000001030"), 0x400000c0a0),
            (hex!("0122222222333333334444444455000001030000000000001040"), 0x400000c0f0),
            (hex!("0122222222333333334444444455000001030000000000004c80"), 0x400000c140),
            (hex!("0122222222333333334444444455000001040000000000001050"), 0x400000c190),
            (hex!("0122222222333333334444444455000001040000000000004850"), 0x400000c1e0),
            (hex!("01222222223333333344444444550000010400000000000057b0"), 0x400000c230),
            (hex!("0122222222333333334444444455000001050000000000001060"), 0x400000c280),
            (hex!("01222222223333333344444444550000010500000000000048d0"), 0x400000c2d0),
            (hex!("0122222222333333334444444455000001050000000000007870"), 0x400000c320),
            (hex!("0122222222333333334444444455000001060000000000001070"), 0x400000c370),
            (hex!("0122222222333333334444444455000001060000000000004f90"), 0x400000c3c0),
            (hex!("0122222222333333334444444455000001060000000000006270"), 0x400000c410),
            (hex!("0122222222333333334444444455000001070000000000001080"), 0x400000c460),
            (hex!("01222222223333333344444444550000010700000000000063b0"), 0x400000c4b0),
            (hex!("0122222222333333334444444455000001080000000000001090"), 0x400000c500),
            (hex!("01222222223333333344444444550000010900000000000010a0"), 0x400000c550),
            (hex!("0122222222333333334444444455000001090000000000006f40"), 0x400000c5a0),
            (hex!("01222222223333333344444444550000010a00000000000010b0"), 0x400000c5f0),
            (hex!("01222222223333333344444444550000010a0000000000006640"), 0x400000c640),
            (hex!("01222222223333333344444444550000010b00000000000010c0"), 0x400000c690),
            (hex!("01222222223333333344444444550000010c00000000000010d0"), 0x400000c6e0),
            (hex!("01222222223333333344444444550000010d00000000000010e0"), 0x400000c730),
            (hex!("01222222223333333344444444550000010e00000000000010f0"), 0x400000c780),
            (hex!("01222222223333333344444444550000010e0000000000005c40"), 0x400000c7d0),
            (hex!("01222222223333333344444444550000010e0000000000007ba0"), 0x400000c820),
            (hex!("01222222223333333344444444550000010f0000000000001100"), 0x400000c870),
            (hex!("01222222223333333344444444550000010f0000000000005c30"), 0x400000c8c0),
            (hex!("0122222222333333334444444455000001100000000000001110"), 0x400000c910),
            (hex!("0122222222333333334444444455000001100000000000007640"), 0x400000c960),
            (hex!("0122222222333333334444444455000001110000000000001120"), 0x400000c9b0),
            (hex!("01222222223333333344444444550000011100000000000052c0"), 0x400000ca00),
            (hex!("0122222222333333334444444455000001110000000000005710"), 0x400000ca50),
            (hex!("0122222222333333334444444455000001110000000000006a00"), 0x400000caa0),
            (hex!("0122222222333333334444444455000001120000000000001130"), 0x400000caf0),
            (hex!("0122222222333333334444444455000001130000000000001140"), 0x400000cb40),
            (hex!("0122222222333333334444444455000001140000000000001150"), 0x400000cb90),
            (hex!("0122222222333333334444444455000001140000000000003fa0"), 0x400000cbe0),
            (hex!("01222222223333333344444444550000011400000000000054b0"), 0x400000cc30),
            (hex!("0122222222333333334444444455000001140000000000006070"), 0x400000cc80),
            (hex!("0122222222333333334444444455000001150000000000001160"), 0x400000ccd0),
            (hex!("0122222222333333334444444455000001150000000000005320"), 0x400000cd20),
            (hex!("0122222222333333334444444455000001150000000000006600"), 0x400000cd70),
            (hex!("0122222222333333334444444455000001150000000000006df0"), 0x400000cdc0),
            (hex!("01222222223333333344444444550000011500000000000079c0"), 0x400000ce10),
            (hex!("0122222222333333334444444455000001160000000000001170"), 0x400000ce60),
            (hex!("0122222222333333334444444455000001170000000000001180"), 0x400000ceb0),
            (hex!("0122222222333333334444444455000001170000000000004a60"), 0x400000cf00),
            (hex!("01222222223333333344444444550000011700000000000063c0"), 0x400000cf50),
            (hex!("0122222222333333334444444455000001180000000000001190"), 0x400000cfa0),
            (hex!("0122222222333333334444444455000001180000000000004530"), 0x400000cff0),
            (hex!("01222222223333333344444444550000011800000000000077c0"), 0x400000d040),
            (hex!("01222222223333333344444444550000011900000000000011a0"), 0x400000d090),
            (hex!("01222222223333333344444444550000011a00000000000011b0"), 0x400000d0e0),
            (hex!("01222222223333333344444444550000011a00000000000041c0"), 0x400000d130),
            (hex!("01222222223333333344444444550000011a00000000000061e0"), 0x400000d180),
            (hex!("01222222223333333344444444550000011b00000000000011c0"), 0x400000d1d0),
            (hex!("01222222223333333344444444550000011c00000000000011d0"), 0x400000d220),
            (hex!("01222222223333333344444444550000011c0000000000005f90"), 0x400000d270),
            (hex!("01222222223333333344444444550000011d00000000000011e0"), 0x400000d2c0),
            (hex!("01222222223333333344444444550000011d0000000000004160"), 0x400000d310),
            (hex!("01222222223333333344444444550000011e00000000000011f0"), 0x400000d360),
            (hex!("01222222223333333344444444550000011e00000000000056d0"), 0x400000d3b0),
            (hex!("01222222223333333344444444550000011f0000000000001200"), 0x400000d400),
            (hex!("01222222223333333344444444550000011f0000000000004510"), 0x400000d450),
            (hex!("0122222222333333334444444455000001200000000000001210"), 0x400000d4a0),
            (hex!("0122222222333333334444444455000001210000000000001220"), 0x400000d4f0),
            (hex!("0122222222333333334444444455000001210000000000005140"), 0x400000d540),
            (hex!("0122222222333333334444444455000001210000000000006710"), 0x400000d590),
            (hex!("0122222222333333334444444455000001210000000000006f50"), 0x400000d5e0),
            (hex!("0122222222333333334444444455000001220000000000001230"), 0x400000d630),
            (hex!("0122222222333333334444444455000001220000000000005570"), 0x400000d680),
            (hex!("0122222222333333334444444455000001220000000000007ac0"), 0x400000d6d0),
            (hex!("0122222222333333334444444455000001230000000000001240"), 0x400000d720),
            (hex!("0122222222333333334444444455000001240000000000001250"), 0x400000d770),
            (hex!("0122222222333333334444444455000001240000000000006cd0"), 0x400000d7c0),
            (hex!("0122222222333333334444444455000001250000000000001260"), 0x400000d810),
            (hex!("01222222223333333344444444550000012500000000000046b0"), 0x400000d860),
            (hex!("0122222222333333334444444455000001250000000000005eb0"), 0x400000d8b0),
            (hex!("0122222222333333334444444455000001260000000000001270"), 0x400000d900),
            (hex!("0122222222333333334444444455000001260000000000004630"), 0x400000d950),
            (hex!("0122222222333333334444444455000001270000000000001280"), 0x400000d9a0),
            (hex!("0122222222333333334444444455000001270000000000004ff0"), 0x400000d9f0),
            (hex!("0122222222333333334444444455000001270000000000006ec0"), 0x400000da40),
            (hex!("0122222222333333334444444455000001280000000000001290"), 0x400000da90),
            (hex!("01222222223333333344444444550000012900000000000012a0"), 0x400000dae0),
            (hex!("0122222222333333334444444455000001290000000000005f60"), 0x400000db30),
            (hex!("01222222223333333344444444550000012a00000000000012b0"), 0x400000db80),
            (hex!("01222222223333333344444444550000012a0000000000005480"), 0x400000dbd0),
            (hex!("01222222223333333344444444550000012b00000000000012c0"), 0x400000dc20),
            (hex!("01222222223333333344444444550000012b00000000000065a0"), 0x400000dc70),
            (hex!("01222222223333333344444444550000012b00000000000066c0"), 0x400000dcc0),
            (hex!("01222222223333333344444444550000012c00000000000012d0"), 0x400000dd10),
            (hex!("01222222223333333344444444550000012c00000000000064b0"), 0x400000dd60),
            (hex!("01222222223333333344444444550000012d00000000000012e0"), 0x400000ddb0),
            (hex!("01222222223333333344444444550000012d00000000000049c0"), 0x400000de00),
            (hex!("01222222223333333344444444550000012d0000000000004bf0"), 0x400000de50),
            (hex!("01222222223333333344444444550000012e00000000000012f0"), 0x400000dea0),
            (hex!("01222222223333333344444444550000012e0000000000005ed0"), 0x400000def0),
            (hex!("01222222223333333344444444550000012f0000000000001300"), 0x400000df40),
            (hex!("01222222223333333344444444550000012f00000000000049a0"), 0x400000df90),
            (hex!("0122222222333333334444444455000001300000000000001310"), 0x400000dfe0),
            (hex!("0122222222333333334444444455000001300000000000007840"), 0x400000e030),
            (hex!("0122222222333333334444444455000001310000000000001320"), 0x400000e080),
            (hex!("0122222222333333334444444455000001310000000000005f70"), 0x400000e0d0),
            (hex!("0122222222333333334444444455000001320000000000001330"), 0x400000e120),
            (hex!("0122222222333333334444444455000001320000000000005a00"), 0x400000e170),
            (hex!("0122222222333333334444444455000001330000000000001340"), 0x400000e1c0),
            (hex!("0122222222333333334444444455000001330000000000006c70"), 0x400000e210),
            (hex!("0122222222333333334444444455000001340000000000001350"), 0x400000e260),
            (hex!("0122222222333333334444444455000001340000000000005c60"), 0x400000e2b0),
            (hex!("0122222222333333334444444455000001350000000000001360"), 0x400000e300),
            (hex!("0122222222333333334444444455000001350000000000004f10"), 0x400000e350),
            (hex!("0122222222333333334444444455000001360000000000001370"), 0x400000e3a0),
            (hex!("0122222222333333334444444455000001360000000000004c60"), 0x400000e3f0),
            (hex!("0122222222333333334444444455000001370000000000001380"), 0x400000e440),
            (hex!("0122222222333333334444444455000001380000000000001390"), 0x400000e490),
            (hex!("01222222223333333344444444550000013900000000000013a0"), 0x400000e4e0),
            (hex!("0122222222333333334444444455000001390000000000004ea0"), 0x400000e530),
            (hex!("01222222223333333344444444550000013a00000000000013b0"), 0x400000e580),
            (hex!("01222222223333333344444444550000013a0000000000007350"), 0x400000e5d0),
            (hex!("01222222223333333344444444550000013b00000000000013c0"), 0x400000e620),
            (hex!("01222222223333333344444444550000013c00000000000013d0"), 0x400000e670),
            (hex!("01222222223333333344444444550000013c0000000000007050"), 0x400000e6c0),
            (hex!("01222222223333333344444444550000013d00000000000013e0"), 0x400000e710),
            (hex!("01222222223333333344444444550000013d0000000000006bd0"), 0x400000e760),
            (hex!("01222222223333333344444444550000013e00000000000013f0"), 0x400000e7b0),
            (hex!("01222222223333333344444444550000013e00000000000058e0"), 0x400000e800),
            (hex!("01222222223333333344444444550000013f0000000000001400"), 0x400000e850),
            (hex!("01222222223333333344444444550000013f0000000000004740"), 0x400000e8a0),
            (hex!("0122222222333333334444444455000001400000000000001410"), 0x400000e8f0),
            (hex!("0122222222333333334444444455000001400000000000003f10"), 0x400000e940),
            (hex!("0122222222333333334444444455000001400000000000006d40"), 0x400000e990),
            (hex!("01222222223333333344444444550000014000000000000072d0"), 0x400000e9e0),
            (hex!("0122222222333333334444444455000001410000000000001420"), 0x400000ea30),
            (hex!("0122222222333333334444444455000001420000000000001430"), 0x400000ea80),
            (hex!("0122222222333333334444444455000001430000000000001440"), 0x400000ead0),
            (hex!("0122222222333333334444444455000001440000000000001450"), 0x400000eb20),
            (hex!("0122222222333333334444444455000001450000000000001460"), 0x400000eb70),
            (hex!("0122222222333333334444444455000001460000000000001470"), 0x400000ebc0),
            (hex!("01222222223333333344444444550000014600000000000055c0"), 0x400000ec10),
            (hex!("0122222222333333334444444455000001470000000000001480"), 0x400000ec60),
            (hex!("0122222222333333334444444455000001470000000000004570"), 0x400000ecb0),
            (hex!("0122222222333333334444444455000001470000000000004be0"), 0x400000ed00),
            (hex!("0122222222333333334444444455000001480000000000001490"), 0x400000ed50),
            (hex!("0122222222333333334444444455000001480000000000005360"), 0x400000eda0),
            (hex!("01222222223333333344444444550000014900000000000014a0"), 0x400000edf0),
            (hex!("01222222223333333344444444550000014a00000000000014b0"), 0x400000ee40),
            (hex!("01222222223333333344444444550000014a00000000000053d0"), 0x400000ee90),
            (hex!("01222222223333333344444444550000014b00000000000014c0"), 0x400000eee0),
            (hex!("01222222223333333344444444550000014b0000000000005950"), 0x400000ef30),
            (hex!("01222222223333333344444444550000014c00000000000014d0"), 0x400000ef80),
            (hex!("01222222223333333344444444550000014c0000000000004f60"), 0x400000efd0),
            (hex!("01222222223333333344444444550000014d00000000000014e0"), 0x400000f020),
            (hex!("01222222223333333344444444550000014d0000000000004520"), 0x400000f070),
            (hex!("01222222223333333344444444550000014d0000000000005200"), 0x400000f0c0),
            (hex!("01222222223333333344444444550000014e00000000000014f0"), 0x400000f110),
            (hex!("01222222223333333344444444550000014e0000000000005bd0"), 0x400000f160),
            (hex!("01222222223333333344444444550000014f0000000000001500"), 0x400000f1b0),
            (hex!("01222222223333333344444444550000014f00000000000060d0"), 0x400000f200),
            (hex!("0122222222333333334444444455000001500000000000001510"), 0x400000f250),
            (hex!("01222222223333333344444444550000015000000000000075e0"), 0x400000f2a0),
            (hex!("0122222222333333334444444455000001510000000000001520"), 0x400000f2f0),
            (hex!("0122222222333333334444444455000001510000000000005c00"), 0x400000f340),
            (hex!("0122222222333333334444444455000001510000000000006af0"), 0x400000f390),
            (hex!("0122222222333333334444444455000001510000000000007b80"), 0x400000f3e0),
            (hex!("0122222222333333334444444455000001520000000000001530"), 0x400000f430),
            (hex!("0122222222333333334444444455000001520000000000004c70"), 0x400000f480),
            (hex!("0122222222333333334444444455000001530000000000001540"), 0x400000f4d0),
            (hex!("0122222222333333334444444455000001540000000000001550"), 0x400000f520),
            (hex!("0122222222333333334444444455000001540000000000007cd0"), 0x400000f570),
            (hex!("0122222222333333334444444455000001550000000000001560"), 0x400000f5c0),
            (hex!("0122222222333333334444444455000001550000000000004ae0"), 0x400000f610),
            (hex!("01222222223333333344444444550000015500000000000068c0"), 0x400000f660),
            (hex!("0122222222333333334444444455000001560000000000001570"), 0x400000f6b0),
            (hex!("01222222223333333344444444550000015600000000000064a0"), 0x400000f700),
            (hex!("0122222222333333334444444455000001570000000000001580"), 0x400000f750),
            (hex!("0122222222333333334444444455000001580000000000001590"), 0x400000f7a0),
            (hex!("0122222222333333334444444455000001580000000000006d30"), 0x400000f7f0),
            (hex!("01222222223333333344444444550000015800000000000074f0"), 0x400000f840),
            (hex!("01222222223333333344444444550000015900000000000015a0"), 0x400000f890),
            (hex!("01222222223333333344444444550000015900000000000053a0"), 0x400000f8e0),
            (hex!("01222222223333333344444444550000015900000000000055e0"), 0x400000f930),
            (hex!("0122222222333333334444444455000001590000000000006210"), 0x400000f980),
            (hex!("01222222223333333344444444550000015900000000000067c0"), 0x400000f9d0),
            (hex!("01222222223333333344444444550000015a00000000000015b0"), 0x400000fa20),
            (hex!("01222222223333333344444444550000015b00000000000015c0"), 0x400000fa70),
            (hex!("01222222223333333344444444550000015c00000000000015d0"), 0x400000fac0),
            (hex!("01222222223333333344444444550000015c0000000000004d80"), 0x400000fb10),
            (hex!("01222222223333333344444444550000015c00000000000073f0"), 0x400000fb60),
            (hex!("01222222223333333344444444550000015d00000000000015e0"), 0x400000fbb0),
            (hex!("01222222223333333344444444550000015e00000000000015f0"), 0x400000fc00),
            (hex!("01222222223333333344444444550000015e0000000000004120"), 0x400000fc50),
            (hex!("01222222223333333344444444550000015e0000000000004350"), 0x400000fca0),
            (hex!("01222222223333333344444444550000015e0000000000007c50"), 0x400000fcf0),
            (hex!("01222222223333333344444444550000015f0000000000001600"), 0x400000fd40),
            (hex!("0122222222333333334444444455000001600000000000001610"), 0x400000fd90),
            (hex!("0122222222333333334444444455000001600000000000004840"), 0x400000fde0),
            (hex!("0122222222333333334444444455000001600000000000004b10"), 0x400000fe30),
            (hex!("0122222222333333334444444455000001600000000000007060"), 0x400000fe80),
            (hex!("0122222222333333334444444455000001610000000000001620"), 0x400000fed0),
            (hex!("0122222222333333334444444455000001610000000000005300"), 0x400000ff20),
            (hex!("0122222222333333334444444455000001620000000000001630"), 0x400000ff70),
            (hex!("0122222222333333334444444455000001620000000000006530"), 0x400000ffc0),
            (hex!("0122222222333333334444444455000001630000000000001640"), 0x4000010010),
            (hex!("0122222222333333334444444455000001640000000000001650"), 0x4000010060),
            (hex!("0122222222333333334444444455000001650000000000001660"), 0x40000100b0),
            (hex!("0122222222333333334444444455000001660000000000001670"), 0x4000010100),
            (hex!("0122222222333333334444444455000001670000000000001680"), 0x4000010150),
            (hex!("0122222222333333334444444455000001670000000000007310"), 0x40000101a0),
            (hex!("0122222222333333334444444455000001680000000000001690"), 0x40000101f0),
            (hex!("0122222222333333334444444455000001680000000000007b50"), 0x4000010240),
            (hex!("01222222223333333344444444550000016900000000000016a0"), 0x4000010290),
            (hex!("01222222223333333344444444550000016900000000000049d0"), 0x40000102e0),
            (hex!("01222222223333333344444444550000016a00000000000016b0"), 0x4000010330),
            (hex!("01222222223333333344444444550000016a00000000000078b0"), 0x4000010380),
            (hex!("01222222223333333344444444550000016b00000000000016c0"), 0x40000103d0),
            (hex!("01222222223333333344444444550000016b0000000000004100"), 0x4000010420),
            (hex!("01222222223333333344444444550000016c00000000000016d0"), 0x4000010470),
            (hex!("01222222223333333344444444550000016c0000000000006e00"), 0x40000104c0),
            (hex!("01222222223333333344444444550000016d00000000000016e0"), 0x4000010510),
            (hex!("01222222223333333344444444550000016e00000000000016f0"), 0x4000010560),
            (hex!("01222222223333333344444444550000016e0000000000004ac0"), 0x40000105b0),
            (hex!("01222222223333333344444444550000016e0000000000007820"), 0x4000010600),
            (hex!("01222222223333333344444444550000016f0000000000001700"), 0x4000010650),
            (hex!("0122222222333333334444444455000001700000000000001710"), 0x40000106a0),
            (hex!("0122222222333333334444444455000001700000000000005830"), 0x40000106f0),
            (hex!("0122222222333333334444444455000001710000000000001720"), 0x4000010740),
            (hex!("01222222223333333344444444550000017100000000000072f0"), 0x4000010790),
            (hex!("0122222222333333334444444455000001720000000000001730"), 0x40000107e0),
            (hex!("0122222222333333334444444455000001720000000000004870"), 0x4000010830),
            (hex!("01222222223333333344444444550000017200000000000070b0"), 0x4000010880),
            (hex!("0122222222333333334444444455000001730000000000001740"), 0x40000108d0),
            (hex!("0122222222333333334444444455000001740000000000001750"), 0x4000010920),
            (hex!("0122222222333333334444444455000001750000000000001760"), 0x4000010970),
            (hex!("0122222222333333334444444455000001750000000000005670"), 0x40000109c0),
            (hex!("0122222222333333334444444455000001750000000000005870"), 0x4000010a10),
            (hex!("0122222222333333334444444455000001760000000000001770"), 0x4000010a60),
            (hex!("0122222222333333334444444455000001770000000000001780"), 0x4000010ab0),
            (hex!("0122222222333333334444444455000001770000000000005000"), 0x4000010b00),
            (hex!("0122222222333333334444444455000001770000000000007090"), 0x4000010b50),
            (hex!("0122222222333333334444444455000001780000000000001790"), 0x4000010ba0),
            (hex!("01222222223333333344444444550000017800000000000048a0"), 0x4000010bf0),
            (hex!("0122222222333333334444444455000001780000000000006bf0"), 0x4000010c40),
            (hex!("01222222223333333344444444550000017900000000000017a0"), 0x4000010c90),
            (hex!("01222222223333333344444444550000017900000000000057d0"), 0x4000010ce0),
            (hex!("0122222222333333334444444455000001790000000000006660"), 0x4000010d30),
            (hex!("01222222223333333344444444550000017a00000000000017b0"), 0x4000010d80),
            (hex!("01222222223333333344444444550000017a0000000000004970"), 0x4000010dd0),
            (hex!("01222222223333333344444444550000017a0000000000005dc0"), 0x4000010e20),
            (hex!("01222222223333333344444444550000017b00000000000017c0"), 0x4000010e70),
            (hex!("01222222223333333344444444550000017b0000000000004ee0"), 0x4000010ec0),
            (hex!("01222222223333333344444444550000017b00000000000054c0"), 0x4000010f10),
            (hex!("01222222223333333344444444550000017c00000000000017d0"), 0x4000010f60),
            (hex!("01222222223333333344444444550000017c0000000000003fc0"), 0x4000010fb0),
            (hex!("01222222223333333344444444550000017c00000000000063e0"), 0x4000011000),
            (hex!("01222222223333333344444444550000017c0000000000006520"), 0x4000011050),
            (hex!("01222222223333333344444444550000017d00000000000017e0"), 0x40000110a0),
            (hex!("01222222223333333344444444550000017d0000000000006220"), 0x40000110f0),
            (hex!("01222222223333333344444444550000017d0000000000007120"), 0x4000011140),
            (hex!("01222222223333333344444444550000017e00000000000017f0"), 0x4000011190),
            (hex!("01222222223333333344444444550000017f0000000000001800"), 0x40000111e0),
            (hex!("0122222222333333334444444455000001800000000000001810"), 0x4000011230),
            (hex!("0122222222333333334444444455000001810000000000001820"), 0x4000011280),
            (hex!("01222222223333333344444444550000018100000000000041f0"), 0x40000112d0),
            (hex!("0122222222333333334444444455000001810000000000007590"), 0x4000011320),
            (hex!("0122222222333333334444444455000001820000000000001830"), 0x4000011370),
            (hex!("0122222222333333334444444455000001820000000000004ce0"), 0x40000113c0),
            (hex!("0122222222333333334444444455000001830000000000001840"), 0x4000011410),
            (hex!("01222222223333333344444444550000018300000000000042c0"), 0x4000011460),
            (hex!("0122222222333333334444444455000001840000000000001850"), 0x40000114b0),
            (hex!("0122222222333333334444444455000001840000000000004f70"), 0x4000011500),
            (hex!("0122222222333333334444444455000001850000000000001860"), 0x4000011550),
            (hex!("0122222222333333334444444455000001850000000000006470"), 0x40000115a0),
            (hex!("0122222222333333334444444455000001850000000000007500"), 0x40000115f0),
            (hex!("0122222222333333334444444455000001860000000000001870"), 0x4000011640),
            (hex!("0122222222333333334444444455000001860000000000004770"), 0x4000011690),
            (hex!("0122222222333333334444444455000001870000000000001880"), 0x40000116e0),
            (hex!("0122222222333333334444444455000001870000000000006a30"), 0x4000011730),
            (hex!("0122222222333333334444444455000001880000000000001890"), 0x4000011780),
            (hex!("0122222222333333334444444455000001880000000000007410"), 0x40000117d0),
            (hex!("01222222223333333344444444550000018900000000000018a0"), 0x4000011820),
            (hex!("01222222223333333344444444550000018900000000000044d0"), 0x4000011870),
            (hex!("0122222222333333334444444455000001890000000000005ac0"), 0x40000118c0),
            (hex!("01222222223333333344444444550000018a00000000000018b0"), 0x4000011910),
            (hex!("01222222223333333344444444550000018a0000000000006260"), 0x4000011960),
            (hex!("01222222223333333344444444550000018a0000000000006d70"), 0x40000119b0),
            (hex!("01222222223333333344444444550000018b00000000000018c0"), 0x4000011a00),
            (hex!("01222222223333333344444444550000018b0000000000004aa0"), 0x4000011a50),
            (hex!("01222222223333333344444444550000018b0000000000006fd0"), 0x4000011aa0),
            (hex!("01222222223333333344444444550000018c00000000000018d0"), 0x4000011af0),
            (hex!("01222222223333333344444444550000018c00000000000051b0"), 0x4000011b40),
            (hex!("01222222223333333344444444550000018c0000000000006650"), 0x4000011b90),
            (hex!("01222222223333333344444444550000018d00000000000018e0"), 0x4000011be0),
            (hex!("01222222223333333344444444550000018e00000000000018f0"), 0x4000011c30),
            (hex!("01222222223333333344444444550000018e00000000000041d0"), 0x4000011c80),
            (hex!("01222222223333333344444444550000018f0000000000001900"), 0x4000011cd0),
            (hex!("01222222223333333344444444550000018f0000000000007600"), 0x4000011d20),
            (hex!("0122222222333333334444444455000001900000000000001910"), 0x4000011d70),
            (hex!("0122222222333333334444444455000001900000000000005410"), 0x4000011dc0),
            (hex!("0122222222333333334444444455000001900000000000006760"), 0x4000011e10),
            (hex!("0122222222333333334444444455000001910000000000001920"), 0x4000011e60),
            (hex!("0122222222333333334444444455000001920000000000001930"), 0x4000011eb0),
            (hex!("0122222222333333334444444455000001920000000000004ca0"), 0x4000011f00),
            (hex!("0122222222333333334444444455000001920000000000005d80"), 0x4000011f50),
            (hex!("0122222222333333334444444455000001920000000000005fd0"), 0x4000011fa0),
            (hex!("01222222223333333344444444550000019200000000000070d0"), 0x4000011ff0),
            (hex!("0122222222333333334444444455000001930000000000001940"), 0x4000012040),
            (hex!("0122222222333333334444444455000001930000000000004010"), 0x4000012090),
            (hex!("0122222222333333334444444455000001930000000000007ca0"), 0x40000120e0),
            (hex!("0122222222333333334444444455000001940000000000001950"), 0x4000012130),
            (hex!("0122222222333333334444444455000001950000000000001960"), 0x4000012180),
            (hex!("0122222222333333334444444455000001950000000000005380"), 0x40000121d0),
            (hex!("0122222222333333334444444455000001960000000000001970"), 0x4000012220),
            (hex!("0122222222333333334444444455000001960000000000006de0"), 0x4000012270),
            (hex!("0122222222333333334444444455000001970000000000001980"), 0x40000122c0),
            (hex!("01222222223333333344444444550000019700000000000048f0"), 0x4000012310),
            (hex!("0122222222333333334444444455000001980000000000001990"), 0x4000012360),
            (hex!("0122222222333333334444444455000001980000000000006510"), 0x40000123b0),
            (hex!("01222222223333333344444444550000019900000000000019a0"), 0x4000012400),
            (hex!("0122222222333333334444444455000001990000000000007570"), 0x4000012450),
            (hex!("0122222222333333334444444455000001990000000000007580"), 0x40000124a0),
            (hex!("01222222223333333344444444550000019a00000000000019b0"), 0x40000124f0),
            (hex!("01222222223333333344444444550000019a0000000000004050"), 0x4000012540),
            (hex!("01222222223333333344444444550000019a0000000000004ba0"), 0x4000012590),
            (hex!("01222222223333333344444444550000019a0000000000005540"), 0x40000125e0),
            (hex!("01222222223333333344444444550000019a00000000000061c0"), 0x4000012630),
            (hex!("01222222223333333344444444550000019a0000000000007c60"), 0x4000012680),
            (hex!("01222222223333333344444444550000019b00000000000019c0"), 0x40000126d0),
            (hex!("01222222223333333344444444550000019b0000000000006240"), 0x4000012720),
            (hex!("01222222223333333344444444550000019c00000000000019d0"), 0x4000012770),
            (hex!("01222222223333333344444444550000019d00000000000019e0"), 0x40000127c0),
            (hex!("01222222223333333344444444550000019d0000000000004640"), 0x4000012810),
            (hex!("01222222223333333344444444550000019d00000000000052a0"), 0x4000012860),
            (hex!("01222222223333333344444444550000019d00000000000052b0"), 0x40000128b0),
            (hex!("01222222223333333344444444550000019e00000000000019f0"), 0x4000012900),
            (hex!("01222222223333333344444444550000019f0000000000001a00"), 0x4000012950),
            (hex!("01222222223333333344444444550000019f0000000000006b20"), 0x40000129a0),
            (hex!("0122222222333333334444444455000001a00000000000001a10"), 0x40000129f0),
            (hex!("0122222222333333334444444455000001a10000000000001a20"), 0x4000012a40),
            (hex!("0122222222333333334444444455000001a10000000000005460"), 0x4000012a90),
            (hex!("0122222222333333334444444455000001a10000000000005d20"), 0x4000012ae0),
            (hex!("0122222222333333334444444455000001a100000000000068f0"), 0x4000012b30),
            (hex!("0122222222333333334444444455000001a20000000000001a30"), 0x4000012b80),
            (hex!("0122222222333333334444444455000001a20000000000007170"), 0x4000012bd0),
            (hex!("0122222222333333334444444455000001a30000000000001a40"), 0x4000012c20),
            (hex!("0122222222333333334444444455000001a40000000000001a50"), 0x4000012c70),
            (hex!("0122222222333333334444444455000001a50000000000001a60"), 0x4000012cc0),
            (hex!("0122222222333333334444444455000001a60000000000001a70"), 0x4000012d10),
            (hex!("0122222222333333334444444455000001a70000000000001a80"), 0x4000012d60),
            (hex!("0122222222333333334444444455000001a70000000000005a90"), 0x4000012db0),
            (hex!("0122222222333333334444444455000001a70000000000006440"), 0x4000012e00),
            (hex!("0122222222333333334444444455000001a80000000000001a90"), 0x4000012e50),
            (hex!("0122222222333333334444444455000001a80000000000004800"), 0x4000012ea0),
            (hex!("0122222222333333334444444455000001a90000000000001aa0"), 0x4000012ef0),
            (hex!("0122222222333333334444444455000001aa0000000000001ab0"), 0x4000012f40),
            (hex!("0122222222333333334444444455000001aa0000000000005b60"), 0x4000012f90),
            (hex!("0122222222333333334444444455000001ab0000000000001ac0"), 0x4000012fe0),
            (hex!("0122222222333333334444444455000001ab0000000000006700"), 0x4000013030),
            (hex!("0122222222333333334444444455000001ab00000000000071d0"), 0x4000013080),
            (hex!("0122222222333333334444444455000001ac0000000000001ad0"), 0x40000130d0),
            (hex!("0122222222333333334444444455000001ac0000000000007380"), 0x4000013120),
            (hex!("0122222222333333334444444455000001ad0000000000001ae0"), 0x4000013170),
            (hex!("0122222222333333334444444455000001ad0000000000006350"), 0x40000131c0),
            (hex!("0122222222333333334444444455000001ae0000000000001af0"), 0x4000013210),
            (hex!("0122222222333333334444444455000001af0000000000001b00"), 0x4000013260),
            (hex!("0122222222333333334444444455000001af0000000000007390"), 0x40000132b0),
            (hex!("0122222222333333334444444455000001b00000000000001b10"), 0x4000013300),
            (hex!("0122222222333333334444444455000001b10000000000001b20"), 0x4000013350),
            (hex!("0122222222333333334444444455000001b10000000000005cc0"), 0x40000133a0),
            (hex!("0122222222333333334444444455000001b20000000000001b30"), 0x40000133f0),
            (hex!("0122222222333333334444444455000001b20000000000004fb0"), 0x4000013440),
            (hex!("0122222222333333334444444455000001b30000000000001b40"), 0x4000013490),
            (hex!("0122222222333333334444444455000001b40000000000001b50"), 0x40000134e0),
            (hex!("0122222222333333334444444455000001b50000000000001b60"), 0x4000013530),
            (hex!("0122222222333333334444444455000001b60000000000001b70"), 0x4000013580),
            (hex!("0122222222333333334444444455000001b600000000000048e0"), 0x40000135d0),
            (hex!("0122222222333333334444444455000001b70000000000001b80"), 0x4000013620),
            (hex!("0122222222333333334444444455000001b70000000000005ca0"), 0x4000013670),
            (hex!("0122222222333333334444444455000001b70000000000007900"), 0x40000136c0),
            (hex!("0122222222333333334444444455000001b80000000000001b90"), 0x4000013710),
            (hex!("0122222222333333334444444455000001b80000000000004d90"), 0x4000013760),
            (hex!("0122222222333333334444444455000001b90000000000001ba0"), 0x40000137b0),
            (hex!("0122222222333333334444444455000001b90000000000003f40"), 0x4000013800),
            (hex!("0122222222333333334444444455000001ba0000000000001bb0"), 0x4000013850),
            (hex!("0122222222333333334444444455000001ba00000000000042a0"), 0x40000138a0),
            (hex!("0122222222333333334444444455000001ba00000000000067f0"), 0x40000138f0),
            (hex!("0122222222333333334444444455000001ba00000000000073a0"), 0x4000013940),
            (hex!("0122222222333333334444444455000001bb0000000000001bc0"), 0x4000013990),
            (hex!("0122222222333333334444444455000001bb0000000000004a00"), 0x40000139e0),
            (hex!("0122222222333333334444444455000001bb0000000000005e00"), 0x4000013a30),
            (hex!("0122222222333333334444444455000001bc0000000000001bd0"), 0x4000013a80),
            (hex!("0122222222333333334444444455000001bc0000000000004230"), 0x4000013ad0),
            (hex!("0122222222333333334444444455000001bc0000000000005860"), 0x4000013b20),
            (hex!("0122222222333333334444444455000001bd0000000000001be0"), 0x4000013b70),
            (hex!("0122222222333333334444444455000001bd0000000000007c70"), 0x4000013bc0),
            (hex!("0122222222333333334444444455000001be0000000000001bf0"), 0x4000013c10),
            (hex!("0122222222333333334444444455000001be0000000000007770"), 0x4000013c60),
            (hex!("0122222222333333334444444455000001be0000000000007cf0"), 0x4000013cb0),
            (hex!("0122222222333333334444444455000001bf0000000000001c00"), 0x4000013d00),
            (hex!("0122222222333333334444444455000001bf0000000000006490"), 0x4000013d50),
            (hex!("0122222222333333334444444455000001c00000000000001c10"), 0x4000013da0),
            (hex!("0122222222333333334444444455000001c10000000000001c20"), 0x4000013df0),
            (hex!("0122222222333333334444444455000001c10000000000004600"), 0x4000013e40),
            (hex!("0122222222333333334444444455000001c20000000000001c30"), 0x4000013e90),
            (hex!("0122222222333333334444444455000001c20000000000006e30"), 0x4000013ee0),
            (hex!("0122222222333333334444444455000001c30000000000001c40"), 0x4000013f30),
            (hex!("0122222222333333334444444455000001c40000000000001c50"), 0x4000013f80),
            (hex!("0122222222333333334444444455000001c50000000000001c60"), 0x4000013fd0),
            (hex!("0122222222333333334444444455000001c60000000000001c70"), 0x4000014020),
            (hex!("0122222222333333334444444455000001c60000000000004240"), 0x4000014070),
            (hex!("0122222222333333334444444455000001c60000000000005bb0"), 0x40000140c0),
            (hex!("0122222222333333334444444455000001c70000000000001c80"), 0x4000014110),
            (hex!("0122222222333333334444444455000001c80000000000001c90"), 0x4000014160),
            (hex!("0122222222333333334444444455000001c90000000000001ca0"), 0x40000141b0),
            (hex!("0122222222333333334444444455000001c90000000000006730"), 0x4000014200),
            (hex!("0122222222333333334444444455000001ca0000000000001cb0"), 0x4000014250),
            (hex!("0122222222333333334444444455000001ca00000000000070f0"), 0x40000142a0),
            (hex!("0122222222333333334444444455000001cb0000000000001cc0"), 0x40000142f0),
            (hex!("0122222222333333334444444455000001cb00000000000071a0"), 0x4000014340),
            (hex!("0122222222333333334444444455000001cc0000000000001cd0"), 0x4000014390),
            (hex!("0122222222333333334444444455000001cc0000000000005280"), 0x40000143e0),
            (hex!("0122222222333333334444444455000001cc0000000000005d90"), 0x4000014430),
            (hex!("0122222222333333334444444455000001cd0000000000001ce0"), 0x4000014480),
            (hex!("0122222222333333334444444455000001cd00000000000069b0"), 0x40000144d0),
            (hex!("0122222222333333334444444455000001ce0000000000001cf0"), 0x4000014520),
            (hex!("0122222222333333334444444455000001ce0000000000004540"), 0x4000014570),
            (hex!("0122222222333333334444444455000001cf0000000000001d00"), 0x40000145c0),
            (hex!("0122222222333333334444444455000001cf00000000000076a0"), 0x4000014610),
            (hex!("0122222222333333334444444455000001d00000000000001d10"), 0x4000014660),
            (hex!("0122222222333333334444444455000001d000000000000060a0"), 0x40000146b0),
            (hex!("0122222222333333334444444455000001d10000000000001d20"), 0x4000014700),
            (hex!("0122222222333333334444444455000001d20000000000001d30"), 0x4000014750),
            (hex!("0122222222333333334444444455000001d30000000000001d40"), 0x40000147a0),
            (hex!("0122222222333333334444444455000001d30000000000004000"), 0x40000147f0),
            (hex!("0122222222333333334444444455000001d30000000000004140"), 0x4000014840),
            (hex!("0122222222333333334444444455000001d30000000000006790"), 0x4000014890),
            (hex!("0122222222333333334444444455000001d40000000000001d50"), 0x40000148e0),
            (hex!("0122222222333333334444444455000001d50000000000001d60"), 0x4000014930),
            (hex!("0122222222333333334444444455000001d60000000000001d70"), 0x4000014980),
            (hex!("0122222222333333334444444455000001d60000000000004b50"), 0x40000149d0),
            (hex!("0122222222333333334444444455000001d60000000000007430"), 0x4000014a20),
            (hex!("0122222222333333334444444455000001d70000000000001d80"), 0x4000014a70),
            (hex!("0122222222333333334444444455000001d70000000000006920"), 0x4000014ac0),
            (hex!("0122222222333333334444444455000001d80000000000001d90"), 0x4000014b10),
            (hex!("0122222222333333334444444455000001d80000000000005b30"), 0x4000014b60),
            (hex!("0122222222333333334444444455000001d90000000000001da0"), 0x4000014bb0),
            (hex!("0122222222333333334444444455000001da0000000000001db0"), 0x4000014c00),
            (hex!("0122222222333333334444444455000001da0000000000004af0"), 0x4000014c50),
            (hex!("0122222222333333334444444455000001da0000000000007240"), 0x4000014ca0),
            (hex!("0122222222333333334444444455000001da0000000000007470"), 0x4000014cf0),
            (hex!("0122222222333333334444444455000001db0000000000001dc0"), 0x4000014d40),
            (hex!("0122222222333333334444444455000001db00000000000045d0"), 0x4000014d90),
            (hex!("0122222222333333334444444455000001dc0000000000001dd0"), 0x4000014de0),
            (hex!("0122222222333333334444444455000001dd0000000000001de0"), 0x4000014e30),
            (hex!("0122222222333333334444444455000001dd0000000000004bb0"), 0x4000014e80),
            (hex!("0122222222333333334444444455000001dd0000000000004cd0"), 0x4000014ed0),
            (hex!("0122222222333333334444444455000001dd0000000000006100"), 0x4000014f20),
            (hex!("0122222222333333334444444455000001dd0000000000007bb0"), 0x4000014f70),
            (hex!("0122222222333333334444444455000001de0000000000001df0"), 0x4000014fc0),
            (hex!("0122222222333333334444444455000001de0000000000004260"), 0x4000015010),
            (hex!("0122222222333333334444444455000001de0000000000006040"), 0x4000015060),
            (hex!("0122222222333333334444444455000001df0000000000001e00"), 0x40000150b0),
            (hex!("0122222222333333334444444455000001df0000000000005fa0"), 0x4000015100),
            (hex!("0122222222333333334444444455000001df0000000000006a70"), 0x4000015150),
            (hex!("0122222222333333334444444455000001df0000000000006dc0"), 0x40000151a0),
            (hex!("0122222222333333334444444455000001e00000000000001e10"), 0x40000151f0),
            (hex!("0122222222333333334444444455000001e00000000000007010"), 0x4000015240),
            (hex!("0122222222333333334444444455000001e10000000000001e20"), 0x4000015290),
            (hex!("0122222222333333334444444455000001e10000000000005720"), 0x40000152e0),
            (hex!("0122222222333333334444444455000001e10000000000006830"), 0x4000015330),
            (hex!("0122222222333333334444444455000001e20000000000001e30"), 0x4000015380),
            (hex!("0122222222333333334444444455000001e20000000000005100"), 0x40000153d0),
            (hex!("0122222222333333334444444455000001e30000000000001e40"), 0x4000015420),
            (hex!("0122222222333333334444444455000001e40000000000001e50"), 0x4000015470),
            (hex!("0122222222333333334444444455000001e40000000000003f30"), 0x40000154c0),
            (hex!("0122222222333333334444444455000001e40000000000005220"), 0x4000015510),
            (hex!("0122222222333333334444444455000001e50000000000001e60"), 0x4000015560),
            (hex!("0122222222333333334444444455000001e50000000000006f60"), 0x40000155b0),
            (hex!("0122222222333333334444444455000001e60000000000001e70"), 0x4000015600),
            (hex!("0122222222333333334444444455000001e60000000000006c80"), 0x4000015650),
            (hex!("0122222222333333334444444455000001e70000000000001e80"), 0x40000156a0),
            (hex!("0122222222333333334444444455000001e80000000000001e90"), 0x40000156f0),
            (hex!("0122222222333333334444444455000001e80000000000004e30"), 0x4000015740),
            (hex!("0122222222333333334444444455000001e90000000000001ea0"), 0x4000015790),
            (hex!("0122222222333333334444444455000001e90000000000005470"), 0x40000157e0),
            (hex!("0122222222333333334444444455000001ea0000000000001eb0"), 0x4000015830),
            (hex!("0122222222333333334444444455000001ea0000000000007980"), 0x4000015880),
            (hex!("0122222222333333334444444455000001eb0000000000001ec0"), 0x40000158d0),
            (hex!("0122222222333333334444444455000001eb0000000000004390"), 0x4000015920),
            (hex!("0122222222333333334444444455000001eb0000000000005970"), 0x4000015970),
            (hex!("0122222222333333334444444455000001ec0000000000001ed0"), 0x40000159c0),
            (hex!("0122222222333333334444444455000001ec0000000000005d50"), 0x4000015a10),
            (hex!("0122222222333333334444444455000001ec00000000000076e0"), 0x4000015a60),
            (hex!("0122222222333333334444444455000001ed0000000000001ee0"), 0x4000015ab0),
            (hex!("0122222222333333334444444455000001ed0000000000006190"), 0x4000015b00),
            (hex!("0122222222333333334444444455000001ee0000000000001ef0"), 0x4000015b50),
            (hex!("0122222222333333334444444455000001ee0000000000004900"), 0x4000015ba0),
            (hex!("0122222222333333334444444455000001ef0000000000001f00"), 0x4000015bf0),
            (hex!("0122222222333333334444444455000001ef0000000000006c60"), 0x4000015c40),
            (hex!("0122222222333333334444444455000001f00000000000001f10"), 0x4000015c90),
            (hex!("0122222222333333334444444455000001f00000000000006950"), 0x4000015ce0),
            (hex!("0122222222333333334444444455000001f10000000000001f20"), 0x4000015d30),
            (hex!("0122222222333333334444444455000001f10000000000006400"), 0x4000015d80),
            (hex!("0122222222333333334444444455000001f20000000000001f30"), 0x4000015dd0),
            (hex!("0122222222333333334444444455000001f20000000000006f00"), 0x4000015e20),
            (hex!("0122222222333333334444444455000001f20000000000007b10"), 0x4000015e70),
            (hex!("0122222222333333334444444455000001f30000000000001f40"), 0x4000015ec0),
            (hex!("0122222222333333334444444455000001f40000000000001f50"), 0x4000015f10),
            (hex!("0122222222333333334444444455000001f50000000000001f60"), 0x4000015f60),
            (hex!("0122222222333333334444444455000001f500000000000044f0"), 0x4000015fb0),
            (hex!("0122222222333333334444444455000001f60000000000001f70"), 0x4000016000),
            (hex!("0122222222333333334444444455000001f70000000000001f80"), 0x4000016050),
            (hex!("0122222222333333334444444455000001f70000000000004ad0"), 0x40000160a0),
            (hex!("0122222222333333334444444455000001f80000000000001f90"), 0x40000160f0),
            (hex!("0122222222333333334444444455000001f90000000000001fa0"), 0x4000016140),
            (hex!("0122222222333333334444444455000001f90000000000003f60"), 0x4000016190),
            (hex!("0122222222333333334444444455000001f90000000000004a80"), 0x40000161e0),
            (hex!("0122222222333333334444444455000001fa0000000000001fb0"), 0x4000016230),
            (hex!("0122222222333333334444444455000001fa0000000000006f90"), 0x4000016280),
            (hex!("0122222222333333334444444455000001fb0000000000001fc0"), 0x40000162d0),
            (hex!("0122222222333333334444444455000001fc0000000000001fd0"), 0x4000016320),
            (hex!("0122222222333333334444444455000001fc0000000000004a90"), 0x4000016370),
            (hex!("0122222222333333334444444455000001fd0000000000001fe0"), 0x40000163c0),
            (hex!("0122222222333333334444444455000001fd0000000000005f50"), 0x4000016410),
            (hex!("0122222222333333334444444455000001fe0000000000001ff0"), 0x4000016460),
            (hex!("0122222222333333334444444455000001ff0000000000002000"), 0x40000164b0),
            (hex!("0122222222333333334444444455000002000000000000002010"), 0x4000016500),
            (hex!("0122222222333333334444444455000002000000000000005f00"), 0x4000016550),
            (hex!("0122222222333333334444444455000002000000000000006840"), 0x40000165a0),
            (hex!("0122222222333333334444444455000002010000000000002020"), 0x40000165f0),
            (hex!("0122222222333333334444444455000002020000000000002030"), 0x4000016640),
            (hex!("0122222222333333334444444455000002030000000000002040"), 0x4000016690),
            (hex!("0122222222333333334444444455000002040000000000002050"), 0x40000166e0),
            (hex!("01222222223333333344444444550000020400000000000051f0"), 0x4000016730),
            (hex!("0122222222333333334444444455000002050000000000002060"), 0x4000016780),
            (hex!("0122222222333333334444444455000002060000000000002070"), 0x40000167d0),
            (hex!("0122222222333333334444444455000002060000000000005c80"), 0x4000016820),
            (hex!("01222222223333333344444444550000020600000000000061d0"), 0x4000016870),
            (hex!("01222222223333333344444444550000020600000000000078c0"), 0x40000168c0),
            (hex!("0122222222333333334444444455000002070000000000002080"), 0x4000016910),
            (hex!("0122222222333333334444444455000002070000000000006ba0"), 0x4000016960),
            (hex!("0122222222333333334444444455000002080000000000002090"), 0x40000169b0),
            (hex!("01222222223333333344444444550000020900000000000020a0"), 0x4000016a00),
            (hex!("01222222223333333344444444550000020900000000000067a0"), 0x4000016a50),
            (hex!("01222222223333333344444444550000020a00000000000020b0"), 0x4000016aa0),
            (hex!("01222222223333333344444444550000020a0000000000004950"), 0x4000016af0),
            (hex!("01222222223333333344444444550000020a0000000000004de0"), 0x4000016b40),
            (hex!("01222222223333333344444444550000020b00000000000020c0"), 0x4000016b90),
            (hex!("01222222223333333344444444550000020b0000000000004b00"), 0x4000016be0),
            (hex!("01222222223333333344444444550000020c00000000000020d0"), 0x4000016c30),
            (hex!("01222222223333333344444444550000020d00000000000020e0"), 0x4000016c80),
            (hex!("01222222223333333344444444550000020e00000000000020f0"), 0x4000016cd0),
            (hex!("01222222223333333344444444550000020f0000000000002100"), 0x4000016d20),
            (hex!("0122222222333333334444444455000002100000000000002110"), 0x4000016d70),
            (hex!("0122222222333333334444444455000002110000000000002120"), 0x4000016dc0),
            (hex!("0122222222333333334444444455000002110000000000004490"), 0x4000016e10),
            (hex!("0122222222333333334444444455000002120000000000002130"), 0x4000016e60),
            (hex!("0122222222333333334444444455000002130000000000002140"), 0x4000016eb0),
            (hex!("01222222223333333344444444550000021300000000000046d0"), 0x4000016f00),
            (hex!("01222222223333333344444444550000021300000000000046e0"), 0x4000016f50),
            (hex!("0122222222333333334444444455000002130000000000004b70"), 0x4000016fa0),
            (hex!("0122222222333333334444444455000002140000000000002150"), 0x4000016ff0),
            (hex!("0122222222333333334444444455000002140000000000006c50"), 0x4000017040),
            (hex!("0122222222333333334444444455000002150000000000002160"), 0x4000017090),
            (hex!("01222222223333333344444444550000021500000000000043c0"), 0x40000170e0),
            (hex!("0122222222333333334444444455000002160000000000002170"), 0x4000017130),
            (hex!("01222222223333333344444444550000021600000000000055b0"), 0x4000017180),
            (hex!("0122222222333333334444444455000002160000000000006150"), 0x40000171d0),
            (hex!("0122222222333333334444444455000002170000000000002180"), 0x4000017220),
            (hex!("01222222223333333344444444550000021700000000000053b0"), 0x4000017270),
            (hex!("0122222222333333334444444455000002170000000000007460"), 0x40000172c0),
            (hex!("0122222222333333334444444455000002180000000000002190"), 0x4000017310),
            (hex!("01222222223333333344444444550000021900000000000021a0"), 0x4000017360),
            (hex!("01222222223333333344444444550000021a00000000000021b0"), 0x40000173b0),
            (hex!("01222222223333333344444444550000021a0000000000007650"), 0x4000017400),
            (hex!("01222222223333333344444444550000021b00000000000021c0"), 0x4000017450),
            (hex!("01222222223333333344444444550000021b0000000000004b20"), 0x40000174a0),
            (hex!("01222222223333333344444444550000021c00000000000021d0"), 0x40000174f0),
            (hex!("01222222223333333344444444550000021c0000000000007610"), 0x4000017540),
            (hex!("01222222223333333344444444550000021d00000000000021e0"), 0x4000017590),
            (hex!("01222222223333333344444444550000021d0000000000005f40"), 0x40000175e0),
            (hex!("01222222223333333344444444550000021e00000000000021f0"), 0x4000017630),
            (hex!("01222222223333333344444444550000021e0000000000005a50"), 0x4000017680),
            (hex!("01222222223333333344444444550000021e0000000000005ff0"), 0x40000176d0),
            (hex!("01222222223333333344444444550000021f0000000000002200"), 0x4000017720),
            (hex!("01222222223333333344444444550000021f00000000000043a0"), 0x4000017770),
            (hex!("01222222223333333344444444550000021f0000000000004cb0"), 0x40000177c0),
            (hex!("01222222223333333344444444550000021f0000000000004e00"), 0x4000017810),
            (hex!("0122222222333333334444444455000002200000000000002210"), 0x4000017860),
            (hex!("0122222222333333334444444455000002210000000000002220"), 0x40000178b0),
            (hex!("0122222222333333334444444455000002210000000000006290"), 0x4000017900),
            (hex!("0122222222333333334444444455000002210000000000007230"), 0x4000017950),
            (hex!("0122222222333333334444444455000002220000000000002230"), 0x40000179a0),
            (hex!("0122222222333333334444444455000002220000000000006ea0"), 0x40000179f0),
            (hex!("0122222222333333334444444455000002230000000000002240"), 0x4000017a40),
            (hex!("0122222222333333334444444455000002230000000000004710"), 0x4000017a90),
            (hex!("0122222222333333334444444455000002240000000000002250"), 0x4000017ae0),
            (hex!("0122222222333333334444444455000002250000000000002260"), 0x4000017b30),
            (hex!("0122222222333333334444444455000002260000000000002270"), 0x4000017b80),
            (hex!("0122222222333333334444444455000002260000000000005b40"), 0x4000017bd0),
            (hex!("0122222222333333334444444455000002260000000000006300"), 0x4000017c20),
            (hex!("0122222222333333334444444455000002270000000000002280"), 0x4000017c70),
            (hex!("0122222222333333334444444455000002270000000000005b80"), 0x4000017cc0),
            (hex!("0122222222333333334444444455000002280000000000002290"), 0x4000017d10),
            (hex!("0122222222333333334444444455000002280000000000003ed0"), 0x4000017d60),
            (hex!("0122222222333333334444444455000002280000000000004550"), 0x4000017db0),
            (hex!("01222222223333333344444444550000022800000000000077d0"), 0x4000017e00),
            (hex!("01222222223333333344444444550000022900000000000022a0"), 0x4000017e50),
            (hex!("0122222222333333334444444455000002290000000000006480"), 0x4000017ea0),
            (hex!("01222222223333333344444444550000022a00000000000022b0"), 0x4000017ef0),
            (hex!("01222222223333333344444444550000022a0000000000005450"), 0x4000017f40),
            (hex!("01222222223333333344444444550000022b00000000000022c0"), 0x4000017f90),
            (hex!("01222222223333333344444444550000022b0000000000006dd0"), 0x4000017fe0),
            (hex!("01222222223333333344444444550000022c00000000000022d0"), 0x4000018030),
            (hex!("01222222223333333344444444550000022c0000000000006890"), 0x4000018080),
            (hex!("01222222223333333344444444550000022d00000000000022e0"), 0x40000180d0),
            (hex!("01222222223333333344444444550000022e00000000000022f0"), 0x4000018120),
            (hex!("01222222223333333344444444550000022e0000000000004f20"), 0x4000018170),
            (hex!("01222222223333333344444444550000022f0000000000002300"), 0x40000181c0),
            (hex!("01222222223333333344444444550000022f0000000000005260"), 0x4000018210),
            (hex!("01222222223333333344444444550000022f00000000000053f0"), 0x4000018260),
            (hex!("0122222222333333334444444455000002300000000000002310"), 0x40000182b0),
            (hex!("01222222223333333344444444550000023000000000000050e0"), 0x4000018300),
            (hex!("0122222222333333334444444455000002310000000000002320"), 0x4000018350),
            (hex!("0122222222333333334444444455000002310000000000007800"), 0x40000183a0),
            (hex!("0122222222333333334444444455000002320000000000002330"), 0x40000183f0),
            (hex!("0122222222333333334444444455000002330000000000002340"), 0x4000018440),
            (hex!("0122222222333333334444444455000002330000000000004d70"), 0x4000018490),
            (hex!("0122222222333333334444444455000002330000000000005cf0"), 0x40000184e0),
            (hex!("0122222222333333334444444455000002340000000000002350"), 0x4000018530),
            (hex!("0122222222333333334444444455000002350000000000002360"), 0x4000018580),
            (hex!("0122222222333333334444444455000002350000000000006970"), 0x40000185d0),
            (hex!("0122222222333333334444444455000002360000000000002370"), 0x4000018620),
            (hex!("0122222222333333334444444455000002360000000000005270"), 0x4000018670),
            (hex!("0122222222333333334444444455000002370000000000002380"), 0x40000186c0),
            (hex!("0122222222333333334444444455000002370000000000005d70"), 0x4000018710),
            (hex!("0122222222333333334444444455000002380000000000002390"), 0x4000018760),
            (hex!("01222222223333333344444444550000023800000000000069a0"), 0x40000187b0),
            (hex!("01222222223333333344444444550000023900000000000023a0"), 0x4000018800),
            (hex!("01222222223333333344444444550000023900000000000052e0"), 0x4000018850),
            (hex!("0122222222333333334444444455000002390000000000005a10"), 0x40000188a0),
            (hex!("0122222222333333334444444455000002390000000000007440"), 0x40000188f0),
            (hex!("01222222223333333344444444550000023a00000000000023b0"), 0x4000018940),
            (hex!("01222222223333333344444444550000023a0000000000003f00"), 0x4000018990),
            (hex!("01222222223333333344444444550000023a0000000000004430"), 0x40000189e0),
            (hex!("01222222223333333344444444550000023a0000000000007070"), 0x4000018a30),
            (hex!("01222222223333333344444444550000023a00000000000074a0"), 0x4000018a80),
            (hex!("01222222223333333344444444550000023b00000000000023c0"), 0x4000018ad0),
            (hex!("01222222223333333344444444550000023b0000000000004730"), 0x4000018b20),
            (hex!("01222222223333333344444444550000023b00000000000068b0"), 0x4000018b70),
            (hex!("01222222223333333344444444550000023c00000000000023d0"), 0x4000018bc0),
            (hex!("01222222223333333344444444550000023c0000000000004680"), 0x4000018c10),
            (hex!("01222222223333333344444444550000023d00000000000023e0"), 0x4000018c60),
            (hex!("01222222223333333344444444550000023d00000000000059a0"), 0x4000018cb0),
            (hex!("01222222223333333344444444550000023e00000000000023f0"), 0x4000018d00),
            (hex!("01222222223333333344444444550000023f0000000000002400"), 0x4000018d50),
            (hex!("0122222222333333334444444455000002400000000000002410"), 0x4000018da0),
            (hex!("0122222222333333334444444455000002400000000000004920"), 0x4000018df0),
            (hex!("01222222223333333344444444550000024000000000000066e0"), 0x4000018e40),
            (hex!("01222222223333333344444444550000024000000000000076f0"), 0x4000018e90),
            (hex!("01222222223333333344444444550000024000000000000078e0"), 0x4000018ee0),
            (hex!("0122222222333333334444444455000002410000000000002420"), 0x4000018f30),
            (hex!("0122222222333333334444444455000002420000000000002430"), 0x4000018f80),
            (hex!("0122222222333333334444444455000002420000000000006590"), 0x4000018fd0),
            (hex!("0122222222333333334444444455000002430000000000002440"), 0x4000019020),
            (hex!("0122222222333333334444444455000002430000000000004d00"), 0x4000019070),
            (hex!("0122222222333333334444444455000002440000000000002450"), 0x40000190c0),
            (hex!("0122222222333333334444444455000002440000000000005f80"), 0x4000019110),
            (hex!("0122222222333333334444444455000002450000000000002460"), 0x4000019160),
            (hex!("0122222222333333334444444455000002450000000000004940"), 0x40000191b0),
            (hex!("0122222222333333334444444455000002460000000000002470"), 0x4000019200),
            (hex!("0122222222333333334444444455000002470000000000002480"), 0x4000019250),
            (hex!("0122222222333333334444444455000002470000000000004dd0"), 0x40000192a0),
            (hex!("0122222222333333334444444455000002470000000000005930"), 0x40000192f0),
            (hex!("01222222223333333344444444550000024700000000000061b0"), 0x4000019340),
            (hex!("0122222222333333334444444455000002470000000000007740"), 0x4000019390),
            (hex!("0122222222333333334444444455000002480000000000002490"), 0x40000193e0),
            (hex!("0122222222333333334444444455000002480000000000004890"), 0x4000019430),
            (hex!("01222222223333333344444444550000024900000000000024a0"), 0x4000019480),
            (hex!("01222222223333333344444444550000024a00000000000024b0"), 0x40000194d0),
            (hex!("01222222223333333344444444550000024b00000000000024c0"), 0x4000019520),
            (hex!("01222222223333333344444444550000024c00000000000024d0"), 0x4000019570),
            (hex!("01222222223333333344444444550000024d00000000000024e0"), 0x40000195c0),
            (hex!("01222222223333333344444444550000024d0000000000004070"), 0x4000019610),
            (hex!("01222222223333333344444444550000024e00000000000024f0"), 0x4000019660),
            (hex!("01222222223333333344444444550000024e00000000000066a0"), 0x40000196b0),
            (hex!("01222222223333333344444444550000024e0000000000006ab0"), 0x4000019700),
            (hex!("01222222223333333344444444550000024f0000000000002500"), 0x4000019750),
            (hex!("0122222222333333334444444455000002500000000000002510"), 0x40000197a0),
            (hex!("0122222222333333334444444455000002510000000000002520"), 0x40000197f0),
            (hex!("0122222222333333334444444455000002510000000000007320"), 0x4000019840),
            (hex!("0122222222333333334444444455000002520000000000002530"), 0x4000019890),
            (hex!("0122222222333333334444444455000002520000000000006410"), 0x40000198e0),
            (hex!("0122222222333333334444444455000002530000000000002540"), 0x4000019930),
            (hex!("0122222222333333334444444455000002530000000000005110"), 0x4000019980),
            (hex!("0122222222333333334444444455000002540000000000002550"), 0x40000199d0),
            (hex!("01222222223333333344444444550000025400000000000040c0"), 0x4000019a20),
            (hex!("0122222222333333334444444455000002540000000000006a40"), 0x4000019a70),
            (hex!("0122222222333333334444444455000002550000000000002560"), 0x4000019ac0),
            (hex!("0122222222333333334444444455000002550000000000005190"), 0x4000019b10),
            (hex!("0122222222333333334444444455000002560000000000002570"), 0x4000019b60),
            (hex!("01222222223333333344444444550000025600000000000061f0"), 0x4000019bb0),
            (hex!("0122222222333333334444444455000002570000000000002580"), 0x4000019c00),
            (hex!("0122222222333333334444444455000002580000000000002590"), 0x4000019c50),
            (hex!("01222222223333333344444444550000025800000000000043d0"), 0x4000019ca0),
            (hex!("01222222223333333344444444550000025900000000000025a0"), 0x4000019cf0),
            (hex!("0122222222333333334444444455000002590000000000006bb0"), 0x4000019d40),
            (hex!("01222222223333333344444444550000025a00000000000025b0"), 0x4000019d90),
            (hex!("01222222223333333344444444550000025a0000000000005fb0"), 0x4000019de0),
            (hex!("01222222223333333344444444550000025a00000000000064c0"), 0x4000019e30),
            (hex!("01222222223333333344444444550000025b00000000000025c0"), 0x4000019e80),
            (hex!("01222222223333333344444444550000025b0000000000005c10"), 0x4000019ed0),
            (hex!("01222222223333333344444444550000025c00000000000025d0"), 0x4000019f20),
            (hex!("01222222223333333344444444550000025c0000000000007d00"), 0x4000019f70),
            (hex!("01222222223333333344444444550000025d00000000000025e0"), 0x4000019fc0),
            (hex!("01222222223333333344444444550000025e00000000000025f0"), 0x400001a010),
            (hex!("01222222223333333344444444550000025e00000000000045e0"), 0x400001a060),
            (hex!("01222222223333333344444444550000025e0000000000006ee0"), 0x400001a0b0),
            (hex!("01222222223333333344444444550000025f0000000000002600"), 0x400001a100),
            (hex!("01222222223333333344444444550000025f00000000000050b0"), 0x400001a150),
            (hex!("01222222223333333344444444550000025f0000000000007690"), 0x400001a1a0),
            (hex!("0122222222333333334444444455000002600000000000002610"), 0x400001a1f0),
            (hex!("0122222222333333334444444455000002600000000000007b60"), 0x400001a240),
            (hex!("0122222222333333334444444455000002610000000000002620"), 0x400001a290),
            (hex!("0122222222333333334444444455000002620000000000002630"), 0x400001a2e0),
            (hex!("0122222222333333334444444455000002630000000000002640"), 0x400001a330),
            (hex!("0122222222333333334444444455000002640000000000002650"), 0x400001a380),
            (hex!("0122222222333333334444444455000002650000000000002660"), 0x400001a3d0),
            (hex!("0122222222333333334444444455000002650000000000006180"), 0x400001a420),
            (hex!("0122222222333333334444444455000002660000000000002670"), 0x400001a470),
            (hex!("0122222222333333334444444455000002660000000000005430"), 0x400001a4c0),
            (hex!("0122222222333333334444444455000002660000000000007a60"), 0x400001a510),
            (hex!("0122222222333333334444444455000002670000000000002680"), 0x400001a560),
            (hex!("01222222223333333344444444550000026700000000000077f0"), 0x400001a5b0),
            (hex!("0122222222333333334444444455000002680000000000002690"), 0x400001a600),
            (hex!("01222222223333333344444444550000026900000000000026a0"), 0x400001a650),
            (hex!("01222222223333333344444444550000026a00000000000026b0"), 0x400001a6a0),
            (hex!("01222222223333333344444444550000026a0000000000007530"), 0x400001a6f0),
            (hex!("01222222223333333344444444550000026b00000000000026c0"), 0x400001a740),
            (hex!("01222222223333333344444444550000026b00000000000058b0"), 0x400001a790),
            (hex!("01222222223333333344444444550000026b00000000000066b0"), 0x400001a7e0),
            (hex!("01222222223333333344444444550000026b0000000000006b10"), 0x400001a830),
            (hex!("01222222223333333344444444550000026c00000000000026d0"), 0x400001a880),
            (hex!("01222222223333333344444444550000026d00000000000026e0"), 0x400001a8d0),
            (hex!("01222222223333333344444444550000026d0000000000004210"), 0x400001a920),
            (hex!("01222222223333333344444444550000026d0000000000005490"), 0x400001a970),
            (hex!("01222222223333333344444444550000026d0000000000005e60"), 0x400001a9c0),
            (hex!("01222222223333333344444444550000026d00000000000068e0"), 0x400001aa10),
            (hex!("01222222223333333344444444550000026d0000000000007020"), 0x400001aa60),
            (hex!("01222222223333333344444444550000026d0000000000007300"), 0x400001aab0),
            (hex!("01222222223333333344444444550000026e00000000000026f0"), 0x400001ab00),
            (hex!("01222222223333333344444444550000026f0000000000002700"), 0x400001ab50),
            (hex!("01222222223333333344444444550000026f0000000000004910"), 0x400001aba0),
            (hex!("0122222222333333334444444455000002700000000000002710"), 0x400001abf0),
            (hex!("0122222222333333334444444455000002710000000000002720"), 0x400001ac40),
            (hex!("01222222223333333344444444550000027100000000000050c0"), 0x400001ac90),
            (hex!("0122222222333333334444444455000002720000000000002730"), 0x400001ace0),
            (hex!("0122222222333333334444444455000002730000000000002740"), 0x400001ad30),
            (hex!("0122222222333333334444444455000002740000000000002750"), 0x400001ad80),
            (hex!("0122222222333333334444444455000002740000000000007490"), 0x400001add0),
            (hex!("0122222222333333334444444455000002750000000000002760"), 0x400001ae20),
            (hex!("0122222222333333334444444455000002760000000000002770"), 0x400001ae70),
            (hex!("0122222222333333334444444455000002760000000000004790"), 0x400001aec0),
            (hex!("0122222222333333334444444455000002770000000000002780"), 0x400001af10),
            (hex!("01222222223333333344444444550000027700000000000050a0"), 0x400001af60),
            (hex!("0122222222333333334444444455000002780000000000002790"), 0x400001afb0),
            (hex!("0122222222333333334444444455000002780000000000004330"), 0x400001b000),
            (hex!("0122222222333333334444444455000002780000000000006b00"), 0x400001b050),
            (hex!("01222222223333333344444444550000027900000000000027a0"), 0x400001b0a0),
            (hex!("01222222223333333344444444550000027a00000000000027b0"), 0x400001b0f0),
            (hex!("01222222223333333344444444550000027b00000000000027c0"), 0x400001b140),
            (hex!("01222222223333333344444444550000027b0000000000004930"), 0x400001b190),
            (hex!("01222222223333333344444444550000027b0000000000006250"), 0x400001b1e0),
            (hex!("01222222223333333344444444550000027c00000000000027d0"), 0x400001b230),
            (hex!("01222222223333333344444444550000027d00000000000027e0"), 0x400001b280),
            (hex!("01222222223333333344444444550000027d0000000000005ce0"), 0x400001b2d0),
            (hex!("01222222223333333344444444550000027d0000000000005fe0"), 0x400001b320),
            (hex!("01222222223333333344444444550000027e00000000000027f0"), 0x400001b370),
            (hex!("01222222223333333344444444550000027f0000000000002800"), 0x400001b3c0),
            (hex!("01222222223333333344444444550000027f0000000000003e90"), 0x400001b410),
            (hex!("01222222223333333344444444550000027f0000000000007910"), 0x400001b460),
            (hex!("0122222222333333334444444455000002800000000000002810"), 0x400001b4b0),
            (hex!("0122222222333333334444444455000002800000000000004990"), 0x400001b500),
            (hex!("0122222222333333334444444455000002800000000000006160"), 0x400001b550),
            (hex!("0122222222333333334444444455000002800000000000006740"), 0x400001b5a0),
            (hex!("0122222222333333334444444455000002810000000000002820"), 0x400001b5f0),
            (hex!("0122222222333333334444444455000002820000000000002830"), 0x400001b640),
            (hex!("0122222222333333334444444455000002820000000000005170"), 0x400001b690),
            (hex!("0122222222333333334444444455000002830000000000002840"), 0x400001b6e0),
            (hex!("0122222222333333334444444455000002840000000000002850"), 0x400001b730),
            (hex!("0122222222333333334444444455000002840000000000004810"), 0x400001b780),
            (hex!("0122222222333333334444444455000002840000000000006aa0"), 0x400001b7d0),
            (hex!("0122222222333333334444444455000002850000000000002860"), 0x400001b820),
            (hex!("0122222222333333334444444455000002860000000000002870"), 0x400001b870),
            (hex!("0122222222333333334444444455000002860000000000005080"), 0x400001b8c0),
            (hex!("0122222222333333334444444455000002870000000000002880"), 0x400001b910),
            (hex!("0122222222333333334444444455000002870000000000004e60"), 0x400001b960),
            (hex!("0122222222333333334444444455000002880000000000002890"), 0x400001b9b0),
            (hex!("0122222222333333334444444455000002880000000000005060"), 0x400001ba00),
            (hex!("0122222222333333334444444455000002880000000000006f20"), 0x400001ba50),
            (hex!("01222222223333333344444444550000028900000000000028a0"), 0x400001baa0),
            (hex!("01222222223333333344444444550000028900000000000047e0"), 0x400001baf0),
            (hex!("01222222223333333344444444550000028a00000000000028b0"), 0x400001bb40),
            (hex!("01222222223333333344444444550000028a0000000000005ab0"), 0x400001bb90),
            (hex!("01222222223333333344444444550000028a0000000000007130"), 0x400001bbe0),
            (hex!("01222222223333333344444444550000028a0000000000007660"), 0x400001bc30),
            (hex!("01222222223333333344444444550000028b00000000000028c0"), 0x400001bc80),
            (hex!("01222222223333333344444444550000028b00000000000054e0"), 0x400001bcd0),
            (hex!("01222222223333333344444444550000028c00000000000028d0"), 0x400001bd20),
            (hex!("01222222223333333344444444550000028c00000000000046f0"), 0x400001bd70),
            (hex!("01222222223333333344444444550000028c00000000000061a0"), 0x400001bdc0),
            (hex!("01222222223333333344444444550000028d00000000000028e0"), 0x400001be10),
            (hex!("01222222223333333344444444550000028e00000000000028f0"), 0x400001be60),
            (hex!("01222222223333333344444444550000028e0000000000004130"), 0x400001beb0),
            (hex!("01222222223333333344444444550000028f0000000000002900"), 0x400001bf00),
            (hex!("01222222223333333344444444550000028f0000000000007510"), 0x400001bf50),
            (hex!("0122222222333333334444444455000002900000000000002910"), 0x400001bfa0),
            (hex!("0122222222333333334444444455000002900000000000004a40"), 0x400001bff0),
            (hex!("0122222222333333334444444455000002910000000000002920"), 0x400001c040),
            (hex!("0122222222333333334444444455000002920000000000002930"), 0x400001c090),
            (hex!("0122222222333333334444444455000002920000000000004e90"), 0x400001c0e0),
            (hex!("0122222222333333334444444455000002930000000000002940"), 0x400001c130),
            (hex!("0122222222333333334444444455000002930000000000006880"), 0x400001c180),
            (hex!("0122222222333333334444444455000002940000000000002950"), 0x400001c1d0),
            (hex!("0122222222333333334444444455000002940000000000007bc0"), 0x400001c220),
            (hex!("0122222222333333334444444455000002950000000000002960"), 0x400001c270),
            (hex!("0122222222333333334444444455000002960000000000002970"), 0x400001c2c0),
            (hex!("01222222223333333344444444550000029600000000000059d0"), 0x400001c310),
            (hex!("0122222222333333334444444455000002970000000000002980"), 0x400001c360),
            (hex!("0122222222333333334444444455000002970000000000004a50"), 0x400001c3b0),
            (hex!("0122222222333333334444444455000002970000000000005f20"), 0x400001c400),
            (hex!("01222222223333333344444444550000029700000000000068d0"), 0x400001c450),
            (hex!("0122222222333333334444444455000002980000000000002990"), 0x400001c4a0),
            (hex!("0122222222333333334444444455000002980000000000004370"), 0x400001c4f0),
            (hex!("0122222222333333334444444455000002980000000000004420"), 0x400001c540),
            (hex!("01222222223333333344444444550000029900000000000029a0"), 0x400001c590),
            (hex!("01222222223333333344444444550000029a00000000000029b0"), 0x400001c5e0),
            (hex!("01222222223333333344444444550000029a0000000000006010"), 0x400001c630),
            (hex!("01222222223333333344444444550000029a0000000000006980"), 0x400001c680),
            (hex!("01222222223333333344444444550000029b00000000000029c0"), 0x400001c6d0),
            (hex!("01222222223333333344444444550000029c00000000000029d0"), 0x400001c720),
            (hex!("01222222223333333344444444550000029c0000000000007480"), 0x400001c770),
            (hex!("01222222223333333344444444550000029d00000000000029e0"), 0x400001c7c0),
            (hex!("01222222223333333344444444550000029d0000000000005030"), 0x400001c810),
            (hex!("01222222223333333344444444550000029d0000000000007780"), 0x400001c860),
            (hex!("01222222223333333344444444550000029d0000000000007a50"), 0x400001c8b0),
            (hex!("01222222223333333344444444550000029e00000000000029f0"), 0x400001c900),
            (hex!("01222222223333333344444444550000029e00000000000074b0"), 0x400001c950),
            (hex!("01222222223333333344444444550000029f0000000000002a00"), 0x400001c9a0),
            (hex!("0122222222333333334444444455000002a00000000000002a10"), 0x400001c9f0),
            (hex!("0122222222333333334444444455000002a10000000000002a20"), 0x400001ca40),
            (hex!("0122222222333333334444444455000002a20000000000002a30"), 0x400001ca90),
            (hex!("0122222222333333334444444455000002a20000000000004c50"), 0x400001cae0),
            (hex!("0122222222333333334444444455000002a20000000000006f10"), 0x400001cb30),
            (hex!("0122222222333333334444444455000002a30000000000002a40"), 0x400001cb80),
            (hex!("0122222222333333334444444455000002a40000000000002a50"), 0x400001cbd0),
            (hex!("0122222222333333334444444455000002a40000000000005d60"), 0x400001cc20),
            (hex!("0122222222333333334444444455000002a50000000000002a60"), 0x400001cc70),
            (hex!("0122222222333333334444444455000002a50000000000005440"), 0x400001ccc0),
            (hex!("0122222222333333334444444455000002a50000000000005890"), 0x400001cd10),
            (hex!("0122222222333333334444444455000002a60000000000002a70"), 0x400001cd60),
            (hex!("0122222222333333334444444455000002a70000000000002a80"), 0x400001cdb0),
            (hex!("0122222222333333334444444455000002a700000000000054a0"), 0x400001ce00),
            (hex!("0122222222333333334444444455000002a70000000000007280"), 0x400001ce50),
            (hex!("0122222222333333334444444455000002a80000000000002a90"), 0x400001cea0),
            (hex!("0122222222333333334444444455000002a90000000000002aa0"), 0x400001cef0),
            (hex!("0122222222333333334444444455000002aa0000000000002ab0"), 0x400001cf40),
            (hex!("0122222222333333334444444455000002ab0000000000002ac0"), 0x400001cf90),
            (hex!("0122222222333333334444444455000002ab0000000000006c90"), 0x400001cfe0),
            (hex!("0122222222333333334444444455000002ac0000000000002ad0"), 0x400001d030),
            (hex!("0122222222333333334444444455000002ac0000000000006db0"), 0x400001d080),
            (hex!("0122222222333333334444444455000002ad0000000000002ae0"), 0x400001d0d0),
            (hex!("0122222222333333334444444455000002ad00000000000065e0"), 0x400001d120),
            (hex!("0122222222333333334444444455000002ad0000000000007b40"), 0x400001d170),
            (hex!("0122222222333333334444444455000002ae0000000000002af0"), 0x400001d1c0),
            (hex!("0122222222333333334444444455000002ae0000000000004d20"), 0x400001d210),
            (hex!("0122222222333333334444444455000002ae0000000000006f30"), 0x400001d260),
            (hex!("0122222222333333334444444455000002af0000000000002b00"), 0x400001d2b0),
            (hex!("0122222222333333334444444455000002b00000000000002b10"), 0x400001d300),
            (hex!("0122222222333333334444444455000002b00000000000004560"), 0x400001d350),
            (hex!("0122222222333333334444444455000002b00000000000005800"), 0x400001d3a0),
            (hex!("0122222222333333334444444455000002b00000000000005a60"), 0x400001d3f0),
            (hex!("0122222222333333334444444455000002b10000000000002b20"), 0x400001d440),
            (hex!("0122222222333333334444444455000002b10000000000007b30"), 0x400001d490),
            (hex!("0122222222333333334444444455000002b20000000000002b30"), 0x400001d4e0),
            (hex!("0122222222333333334444444455000002b20000000000004440"), 0x400001d530),
            (hex!("0122222222333333334444444455000002b20000000000004f80"), 0x400001d580),
            (hex!("0122222222333333334444444455000002b20000000000005020"), 0x400001d5d0),
            (hex!("0122222222333333334444444455000002b30000000000002b40"), 0x400001d620),
            (hex!("0122222222333333334444444455000002b40000000000002b50"), 0x400001d670),
            (hex!("0122222222333333334444444455000002b50000000000002b60"), 0x400001d6c0),
            (hex!("0122222222333333334444444455000002b500000000000059e0"), 0x400001d710),
            (hex!("0122222222333333334444444455000002b60000000000002b70"), 0x400001d760),
            (hex!("0122222222333333334444444455000002b70000000000002b80"), 0x400001d7b0),
            (hex!("0122222222333333334444444455000002b80000000000002b90"), 0x400001d800),
            (hex!("0122222222333333334444444455000002b80000000000004590"), 0x400001d850),
            (hex!("0122222222333333334444444455000002b800000000000047d0"), 0x400001d8a0),
            (hex!("0122222222333333334444444455000002b80000000000006030"), 0x400001d8f0),
            (hex!("0122222222333333334444444455000002b80000000000006a20"), 0x400001d940),
            (hex!("0122222222333333334444444455000002b80000000000006a90"), 0x400001d990),
            (hex!("0122222222333333334444444455000002b90000000000002ba0"), 0x400001d9e0),
            (hex!("0122222222333333334444444455000002ba0000000000002bb0"), 0x400001da30),
            (hex!("0122222222333333334444444455000002ba0000000000006e80"), 0x400001da80),
            (hex!("0122222222333333334444444455000002bb0000000000002bc0"), 0x400001dad0),
            (hex!("0122222222333333334444444455000002bc0000000000002bd0"), 0x400001db20),
            (hex!("0122222222333333334444444455000002bc0000000000004b30"), 0x400001db70),
            (hex!("0122222222333333334444444455000002bd0000000000002be0"), 0x400001dbc0),
            (hex!("0122222222333333334444444455000002bd0000000000005e10"), 0x400001dc10),
            (hex!("0122222222333333334444444455000002be0000000000002bf0"), 0x400001dc60),
            (hex!("0122222222333333334444444455000002bf0000000000002c00"), 0x400001dcb0),
            (hex!("0122222222333333334444444455000002c00000000000002c10"), 0x400001dd00),
            (hex!("0122222222333333334444444455000002c10000000000002c20"), 0x400001dd50),
            (hex!("0122222222333333334444444455000002c10000000000003ef0"), 0x400001dda0),
            (hex!("0122222222333333334444444455000002c20000000000002c30"), 0x400001ddf0),
            (hex!("0122222222333333334444444455000002c200000000000056e0"), 0x400001de40),
            (hex!("0122222222333333334444444455000002c30000000000002c40"), 0x400001de90),
            (hex!("0122222222333333334444444455000002c30000000000004b60"), 0x400001dee0),
            (hex!("0122222222333333334444444455000002c40000000000002c50"), 0x400001df30),
            (hex!("0122222222333333334444444455000002c400000000000045f0"), 0x400001df80),
            (hex!("0122222222333333334444444455000002c40000000000005290"), 0x400001dfd0),
            (hex!("0122222222333333334444444455000002c50000000000002c60"), 0x400001e020),
            (hex!("0122222222333333334444444455000002c60000000000002c70"), 0x400001e070),
            (hex!("0122222222333333334444444455000002c60000000000006ae0"), 0x400001e0c0),
            (hex!("0122222222333333334444444455000002c70000000000002c80"), 0x400001e110),
            (hex!("0122222222333333334444444455000002c70000000000005680"), 0x400001e160),
            (hex!("0122222222333333334444444455000002c70000000000006e10"), 0x400001e1b0),
            (hex!("0122222222333333334444444455000002c80000000000002c90"), 0x400001e200),
            (hex!("0122222222333333334444444455000002c90000000000002ca0"), 0x400001e250),
            (hex!("0122222222333333334444444455000002ca0000000000002cb0"), 0x400001e2a0),
            (hex!("0122222222333333334444444455000002cb0000000000002cc0"), 0x400001e2f0),
            (hex!("0122222222333333334444444455000002cc0000000000002cd0"), 0x400001e340),
            (hex!("0122222222333333334444444455000002cc0000000000005b50"), 0x400001e390),
            (hex!("0122222222333333334444444455000002cd0000000000002ce0"), 0x400001e3e0),
            (hex!("0122222222333333334444444455000002ce0000000000002cf0"), 0x400001e430),
            (hex!("0122222222333333334444444455000002ce00000000000043f0"), 0x400001e480),
            (hex!("0122222222333333334444444455000002ce0000000000006420"), 0x400001e4d0),
            (hex!("0122222222333333334444444455000002cf0000000000002d00"), 0x400001e520),
            (hex!("0122222222333333334444444455000002d00000000000002d10"), 0x400001e570),
            (hex!("0122222222333333334444444455000002d10000000000002d20"), 0x400001e5c0),
            (hex!("0122222222333333334444444455000002d10000000000005370"), 0x400001e610),
            (hex!("0122222222333333334444444455000002d20000000000002d30"), 0x400001e660),
            (hex!("0122222222333333334444444455000002d20000000000005ef0"), 0x400001e6b0),
            (hex!("0122222222333333334444444455000002d20000000000006570"), 0x400001e700),
            (hex!("0122222222333333334444444455000002d30000000000002d40"), 0x400001e750),
            (hex!("0122222222333333334444444455000002d30000000000007360"), 0x400001e7a0),
            (hex!("0122222222333333334444444455000002d40000000000002d50"), 0x400001e7f0),
            (hex!("0122222222333333334444444455000002d400000000000079a0"), 0x400001e840),
            (hex!("0122222222333333334444444455000002d50000000000002d60"), 0x400001e890),
            (hex!("0122222222333333334444444455000002d50000000000004250"), 0x400001e8e0),
            (hex!("0122222222333333334444444455000002d50000000000006050"), 0x400001e930),
            (hex!("0122222222333333334444444455000002d60000000000002d70"), 0x400001e980),
            (hex!("0122222222333333334444444455000002d60000000000007080"), 0x400001e9d0),
            (hex!("0122222222333333334444444455000002d70000000000002d80"), 0x400001ea20),
            (hex!("0122222222333333334444444455000002d80000000000002d90"), 0x400001ea70),
            (hex!("0122222222333333334444444455000002d80000000000007110"), 0x400001eac0),
            (hex!("0122222222333333334444444455000002d800000000000073c0"), 0x400001eb10),
            (hex!("0122222222333333334444444455000002d800000000000075a0"), 0x400001eb60),
            (hex!("0122222222333333334444444455000002d90000000000002da0"), 0x400001ebb0),
            (hex!("0122222222333333334444444455000002d90000000000004860"), 0x400001ec00),
            (hex!("0122222222333333334444444455000002d90000000000006b60"), 0x400001ec50),
            (hex!("0122222222333333334444444455000002da0000000000002db0"), 0x400001eca0),
            (hex!("0122222222333333334444444455000002da0000000000006630"), 0x400001ecf0),
            (hex!("0122222222333333334444444455000002db0000000000002dc0"), 0x400001ed40),
            (hex!("0122222222333333334444444455000002dc0000000000002dd0"), 0x400001ed90),
            (hex!("0122222222333333334444444455000002dc0000000000004830"), 0x400001ede0),
            (hex!("0122222222333333334444444455000002dd0000000000002de0"), 0x400001ee30),
            (hex!("0122222222333333334444444455000002de0000000000002df0"), 0x400001ee80),
            (hex!("0122222222333333334444444455000002de0000000000004f00"), 0x400001eed0),
            (hex!("0122222222333333334444444455000002df0000000000002e00"), 0x400001ef20),
            (hex!("0122222222333333334444444455000002e00000000000002e10"), 0x400001ef70),
            (hex!("0122222222333333334444444455000002e10000000000002e20"), 0x400001efc0),
            (hex!("0122222222333333334444444455000002e10000000000006e90"), 0x400001f010),
            (hex!("0122222222333333334444444455000002e20000000000002e30"), 0x400001f060),
            (hex!("0122222222333333334444444455000002e200000000000053e0"), 0x400001f0b0),
            (hex!("0122222222333333334444444455000002e30000000000002e40"), 0x400001f100),
            (hex!("0122222222333333334444444455000002e30000000000006020"), 0x400001f150),
            (hex!("0122222222333333334444444455000002e30000000000006540"), 0x400001f1a0),
            (hex!("0122222222333333334444444455000002e40000000000002e50"), 0x400001f1f0),
            (hex!("0122222222333333334444444455000002e50000000000002e60"), 0x400001f240),
            (hex!("0122222222333333334444444455000002e50000000000005180"), 0x400001f290),
            (hex!("0122222222333333334444444455000002e50000000000007bf0"), 0x400001f2e0),
            (hex!("0122222222333333334444444455000002e60000000000002e70"), 0x400001f330),
            (hex!("0122222222333333334444444455000002e60000000000005350"), 0x400001f380),
            (hex!("0122222222333333334444444455000002e60000000000007960"), 0x400001f3d0),
            (hex!("0122222222333333334444444455000002e70000000000002e80"), 0x400001f420),
            (hex!("0122222222333333334444444455000002e80000000000002e90"), 0x400001f470),
            (hex!("0122222222333333334444444455000002e90000000000002ea0"), 0x400001f4c0),
            (hex!("0122222222333333334444444455000002ea0000000000002eb0"), 0x400001f510),
            (hex!("0122222222333333334444444455000002eb0000000000002ec0"), 0x400001f560),
            (hex!("0122222222333333334444444455000002ec0000000000002ed0"), 0x400001f5b0),
            (hex!("0122222222333333334444444455000002ec0000000000006c10"), 0x400001f600),
            (hex!("0122222222333333334444444455000002ed0000000000002ee0"), 0x400001f650),
            (hex!("0122222222333333334444444455000002ed0000000000005590"), 0x400001f6a0),
            (hex!("0122222222333333334444444455000002ed0000000000005cd0"), 0x400001f6f0),
            (hex!("0122222222333333334444444455000002ed0000000000006910"), 0x400001f740),
            (hex!("0122222222333333334444444455000002ee0000000000002ef0"), 0x400001f790),
            (hex!("0122222222333333334444444455000002ef0000000000002f00"), 0x400001f7e0),
            (hex!("0122222222333333334444444455000002ef0000000000004ed0"), 0x400001f830),
            (hex!("0122222222333333334444444455000002f00000000000002f10"), 0x400001f880),
            (hex!("0122222222333333334444444455000002f00000000000004cf0"), 0x400001f8d0),
            (hex!("0122222222333333334444444455000002f00000000000005d10"), 0x400001f920),
            (hex!("0122222222333333334444444455000002f00000000000006860"), 0x400001f970),
            (hex!("0122222222333333334444444455000002f00000000000006b50"), 0x400001f9c0),
            (hex!("0122222222333333334444444455000002f00000000000007100"), 0x400001fa10),
            (hex!("0122222222333333334444444455000002f00000000000007aa0"), 0x400001fa60),
            (hex!("0122222222333333334444444455000002f10000000000002f20"), 0x400001fab0),
            (hex!("0122222222333333334444444455000002f20000000000002f30"), 0x400001fb00),
            (hex!("0122222222333333334444444455000002f200000000000044b0"), 0x400001fb50),
            (hex!("0122222222333333334444444455000002f30000000000002f40"), 0x400001fba0),
            (hex!("0122222222333333334444444455000002f300000000000075b0"), 0x400001fbf0),
            (hex!("0122222222333333334444444455000002f40000000000002f50"), 0x400001fc40),
            (hex!("0122222222333333334444444455000002f400000000000060f0"), 0x400001fc90),
            (hex!("0122222222333333334444444455000002f50000000000002f60"), 0x400001fce0),
            (hex!("0122222222333333334444444455000002f50000000000007210"), 0x400001fd30),
            (hex!("0122222222333333334444444455000002f60000000000002f70"), 0x400001fd80),
            (hex!("0122222222333333334444444455000002f60000000000006610"), 0x400001fdd0),
            (hex!("0122222222333333334444444455000002f70000000000002f80"), 0x400001fe20),
            (hex!("0122222222333333334444444455000002f70000000000007560"), 0x400001fe70),
            (hex!("0122222222333333334444444455000002f80000000000002f90"), 0x400001fec0),
            (hex!("0122222222333333334444444455000002f80000000000006320"), 0x400001ff10),
            (hex!("0122222222333333334444444455000002f90000000000002fa0"), 0x400001ff60),
            (hex!("0122222222333333334444444455000002f90000000000006e50"), 0x400001ffb0),
            (hex!("0122222222333333334444444455000002fa0000000000002fb0"), 0x4000020000),
            (hex!("0122222222333333334444444455000002fb0000000000002fc0"), 0x4000020050),
            (hex!("0122222222333333334444444455000002fb0000000000004780"), 0x40000200a0),
            (hex!("0122222222333333334444444455000002fc0000000000002fd0"), 0x40000200f0),
            (hex!("0122222222333333334444444455000002fd0000000000002fe0"), 0x4000020140),
            (hex!("0122222222333333334444444455000002fd0000000000005600"), 0x4000020190),
            (hex!("0122222222333333334444444455000002fd0000000000006c00"), 0x40000201e0),
            (hex!("0122222222333333334444444455000002fe0000000000002ff0"), 0x4000020230),
            (hex!("0122222222333333334444444455000002ff0000000000003000"), 0x4000020280),
            (hex!("0122222222333333334444444455000003000000000000003010"), 0x40000202d0),
            (hex!("0122222222333333334444444455000003000000000000004080"), 0x4000020320),
            (hex!("0122222222333333334444444455000003010000000000003020"), 0x4000020370),
            (hex!("0122222222333333334444444455000003010000000000006340"), 0x40000203c0),
            (hex!("0122222222333333334444444455000003020000000000003030"), 0x4000020410),
            (hex!("0122222222333333334444444455000003020000000000005b00"), 0x4000020460),
            (hex!("0122222222333333334444444455000003020000000000007b20"), 0x40000204b0),
            (hex!("0122222222333333334444444455000003030000000000003040"), 0x4000020500),
            (hex!("01222222223333333344444444550000030300000000000056b0"), 0x4000020550),
            (hex!("0122222222333333334444444455000003030000000000006280"), 0x40000205a0),
            (hex!("0122222222333333334444444455000003030000000000007ad0"), 0x40000205f0),
            (hex!("0122222222333333334444444455000003040000000000003050"), 0x4000020640),
            (hex!("0122222222333333334444444455000003040000000000005c50"), 0x4000020690),
            (hex!("0122222222333333334444444455000003050000000000003060"), 0x40000206e0),
            (hex!("01222222223333333344444444550000030500000000000072e0"), 0x4000020730),
            (hex!("0122222222333333334444444455000003060000000000003070"), 0x4000020780),
            (hex!("0122222222333333334444444455000003060000000000004360"), 0x40000207d0),
            (hex!("0122222222333333334444444455000003060000000000004380"), 0x4000020820),
            (hex!("0122222222333333334444444455000003060000000000004820"), 0x4000020870),
            (hex!("0122222222333333334444444455000003060000000000006d10"), 0x40000208c0),
            (hex!("0122222222333333334444444455000003070000000000003080"), 0x4000020910),
            (hex!("0122222222333333334444444455000003070000000000004450"), 0x4000020960),
            (hex!("0122222222333333334444444455000003080000000000003090"), 0x40000209b0),
            (hex!("0122222222333333334444444455000003080000000000005ad0"), 0x4000020a00),
            (hex!("01222222223333333344444444550000030900000000000030a0"), 0x4000020a50),
            (hex!("01222222223333333344444444550000030a00000000000030b0"), 0x4000020aa0),
            (hex!("01222222223333333344444444550000030a0000000000007760"), 0x4000020af0),
            (hex!("01222222223333333344444444550000030b00000000000030c0"), 0x4000020b40),
            (hex!("01222222223333333344444444550000030b0000000000007a80"), 0x4000020b90),
            (hex!("01222222223333333344444444550000030c00000000000030d0"), 0x4000020be0),
            (hex!("01222222223333333344444444550000030d00000000000030e0"), 0x4000020c30),
            (hex!("01222222223333333344444444550000030d0000000000003eb0"), 0x4000020c80),
            (hex!("01222222223333333344444444550000030e00000000000030f0"), 0x4000020cd0),
            (hex!("01222222223333333344444444550000030f0000000000003100"), 0x4000020d20),
            (hex!("01222222223333333344444444550000030f0000000000004690"), 0x4000020d70),
            (hex!("01222222223333333344444444550000030f0000000000006900"), 0x4000020dc0),
            (hex!("0122222222333333334444444455000003100000000000003110"), 0x4000020e10),
            (hex!("01222222223333333344444444550000031000000000000058a0"), 0x4000020e60),
            (hex!("0122222222333333334444444455000003110000000000003120"), 0x4000020eb0),
            (hex!("0122222222333333334444444455000003110000000000004200"), 0x4000020f00),
            (hex!("0122222222333333334444444455000003120000000000003130"), 0x4000020f50),
            (hex!("0122222222333333334444444455000003130000000000003140"), 0x4000020fa0),
            (hex!("0122222222333333334444444455000003130000000000004d50"), 0x4000020ff0),
            (hex!("0122222222333333334444444455000003130000000000005400"), 0x4000021040),
            (hex!("0122222222333333334444444455000003130000000000005520"), 0x4000021090),
            (hex!("0122222222333333334444444455000003140000000000003150"), 0x40000210e0),
            (hex!("0122222222333333334444444455000003140000000000006450"), 0x4000021130),
            (hex!("0122222222333333334444444455000003150000000000003160"), 0x4000021180),
            (hex!("01222222223333333344444444550000031500000000000062d0"), 0x40000211d0),
            (hex!("0122222222333333334444444455000003160000000000003170"), 0x4000021220),
            (hex!("0122222222333333334444444455000003160000000000004c40"), 0x4000021270),
            (hex!("0122222222333333334444444455000003160000000000007c80"), 0x40000212c0),
            (hex!("0122222222333333334444444455000003170000000000003180"), 0x4000021310),
            (hex!("0122222222333333334444444455000003170000000000004400"), 0x4000021360),
            (hex!("0122222222333333334444444455000003170000000000005090"), 0x40000213b0),
            (hex!("0122222222333333334444444455000003170000000000006cb0"), 0x4000021400),
            (hex!("0122222222333333334444444455000003180000000000003190"), 0x4000021450),
            (hex!("0122222222333333334444444455000003180000000000006560"), 0x40000214a0),
            (hex!("01222222223333333344444444550000031900000000000031a0"), 0x40000214f0),
            (hex!("01222222223333333344444444550000031900000000000052d0"), 0x4000021540),
            (hex!("01222222223333333344444444550000031900000000000057e0"), 0x4000021590),
            (hex!("01222222223333333344444444550000031a00000000000031b0"), 0x40000215e0),
            (hex!("01222222223333333344444444550000031a00000000000071e0"), 0x4000021630),
            (hex!("01222222223333333344444444550000031b00000000000031c0"), 0x4000021680),
            (hex!("01222222223333333344444444550000031c00000000000031d0"), 0x40000216d0),
            (hex!("01222222223333333344444444550000031c0000000000004480"), 0x4000021720),
            (hex!("01222222223333333344444444550000031c0000000000005790"), 0x4000021770),
            (hex!("01222222223333333344444444550000031c0000000000007be0"), 0x40000217c0),
            (hex!("01222222223333333344444444550000031d00000000000031e0"), 0x4000021810),
            (hex!("01222222223333333344444444550000031d0000000000005560"), 0x4000021860),
            (hex!("01222222223333333344444444550000031e00000000000031f0"), 0x40000218b0),
            (hex!("01222222223333333344444444550000031f0000000000003200"), 0x4000021900),
            (hex!("01222222223333333344444444550000031f0000000000004190"), 0x4000021950),
            (hex!("0122222222333333334444444455000003200000000000003210"), 0x40000219a0),
            (hex!("0122222222333333334444444455000003210000000000003220"), 0x40000219f0),
            (hex!("0122222222333333334444444455000003220000000000003230"), 0x4000021a40),
            (hex!("0122222222333333334444444455000003230000000000003240"), 0x4000021a90),
            (hex!("01222222223333333344444444550000032300000000000069d0"), 0x4000021ae0),
            (hex!("0122222222333333334444444455000003240000000000003250"), 0x4000021b30),
            (hex!("0122222222333333334444444455000003250000000000003260"), 0x4000021b80),
            (hex!("01222222223333333344444444550000032500000000000042b0"), 0x4000021bd0),
            (hex!("01222222223333333344444444550000032500000000000064e0"), 0x4000021c20),
            (hex!("0122222222333333334444444455000003260000000000003270"), 0x4000021c70),
            (hex!("0122222222333333334444444455000003270000000000003280"), 0x4000021cc0),
            (hex!("0122222222333333334444444455000003270000000000005b20"), 0x4000021d10),
            (hex!("0122222222333333334444444455000003270000000000006330"), 0x4000021d60),
            (hex!("0122222222333333334444444455000003270000000000006810"), 0x4000021db0),
            (hex!("0122222222333333334444444455000003280000000000003290"), 0x4000021e00),
            (hex!("01222222223333333344444444550000032900000000000032a0"), 0x4000021e50),
            (hex!("01222222223333333344444444550000032900000000000056f0"), 0x4000021ea0),
            (hex!("0122222222333333334444444455000003290000000000005e20"), 0x4000021ef0),
            (hex!("0122222222333333334444444455000003290000000000005e70"), 0x4000021f40),
            (hex!("01222222223333333344444444550000032a00000000000032b0"), 0x4000021f90),
            (hex!("01222222223333333344444444550000032b00000000000032c0"), 0x4000021fe0),
            (hex!("01222222223333333344444444550000032b0000000000005500"), 0x4000022030),
            (hex!("01222222223333333344444444550000032b0000000000005a20"), 0x4000022080),
            (hex!("01222222223333333344444444550000032c00000000000032d0"), 0x40000220d0),
            (hex!("01222222223333333344444444550000032c0000000000004060"), 0x4000022120),
            (hex!("01222222223333333344444444550000032c0000000000004760"), 0x4000022170),
            (hex!("01222222223333333344444444550000032d00000000000032e0"), 0x40000221c0),
            (hex!("01222222223333333344444444550000032d00000000000068a0"), 0x4000022210),
            (hex!("01222222223333333344444444550000032e00000000000032f0"), 0x4000022260),
            (hex!("01222222223333333344444444550000032f0000000000003300"), 0x40000222b0),
            (hex!("0122222222333333334444444455000003300000000000003310"), 0x4000022300),
            (hex!("0122222222333333334444444455000003300000000000006e40"), 0x4000022350),
            (hex!("0122222222333333334444444455000003310000000000003320"), 0x40000223a0),
            (hex!("0122222222333333334444444455000003310000000000004620"), 0x40000223f0),
            (hex!("0122222222333333334444444455000003320000000000003330"), 0x4000022440),
            (hex!("0122222222333333334444444455000003330000000000003340"), 0x4000022490),
            (hex!("0122222222333333334444444455000003330000000000004b80"), 0x40000224e0),
            (hex!("0122222222333333334444444455000003340000000000003350"), 0x4000022530),
            (hex!("0122222222333333334444444455000003350000000000003360"), 0x4000022580),
            (hex!("0122222222333333334444444455000003360000000000003370"), 0x40000225d0),
            (hex!("0122222222333333334444444455000003370000000000003380"), 0x4000022620),
            (hex!("0122222222333333334444444455000003380000000000003390"), 0x4000022670),
            (hex!("01222222223333333344444444550000033900000000000033a0"), 0x40000226c0),
            (hex!("0122222222333333334444444455000003390000000000006b90"), 0x4000022710),
            (hex!("01222222223333333344444444550000033a00000000000033b0"), 0x4000022760),
            (hex!("01222222223333333344444444550000033a0000000000007420"), 0x40000227b0),
            (hex!("01222222223333333344444444550000033b00000000000033c0"), 0x4000022800),
            (hex!("01222222223333333344444444550000033b0000000000007620"), 0x4000022850),
            (hex!("01222222223333333344444444550000033c00000000000033d0"), 0x40000228a0),
            (hex!("01222222223333333344444444550000033c0000000000006b30"), 0x40000228f0),
            (hex!("01222222223333333344444444550000033d00000000000033e0"), 0x4000022940),
            (hex!("01222222223333333344444444550000033e00000000000033f0"), 0x4000022990),
            (hex!("01222222223333333344444444550000033e00000000000048b0"), 0x40000229e0),
            (hex!("01222222223333333344444444550000033e0000000000004e70"), 0x4000022a30),
            (hex!("01222222223333333344444444550000033f0000000000003400"), 0x4000022a80),
            (hex!("01222222223333333344444444550000033f0000000000006380"), 0x4000022ad0),
            (hex!("0122222222333333334444444455000003400000000000003410"), 0x4000022b20),
            (hex!("0122222222333333334444444455000003410000000000003420"), 0x4000022b70),
            (hex!("0122222222333333334444444455000003410000000000006090"), 0x4000022bc0),
            (hex!("0122222222333333334444444455000003420000000000003430"), 0x4000022c10),
            (hex!("01222222223333333344444444550000034200000000000073d0"), 0x4000022c60),
            (hex!("0122222222333333334444444455000003430000000000003440"), 0x4000022cb0),
            (hex!("0122222222333333334444444455000003430000000000006370"), 0x4000022d00),
            (hex!("01222222223333333344444444550000034300000000000075c0"), 0x4000022d50),
            (hex!("0122222222333333334444444455000003440000000000003450"), 0x4000022da0),
            (hex!("0122222222333333334444444455000003450000000000003460"), 0x4000022df0),
            (hex!("0122222222333333334444444455000003460000000000003470"), 0x4000022e40),
            (hex!("01222222223333333344444444550000034600000000000055f0"), 0x4000022e90),
            (hex!("0122222222333333334444444455000003470000000000003480"), 0x4000022ee0),
            (hex!("0122222222333333334444444455000003470000000000003fe0"), 0x4000022f30),
            (hex!("0122222222333333334444444455000003480000000000003490"), 0x4000022f80),
            (hex!("0122222222333333334444444455000003480000000000007990"), 0x4000022fd0),
            (hex!("01222222223333333344444444550000034900000000000034a0"), 0x4000023020),
            (hex!("0122222222333333334444444455000003490000000000004410"), 0x4000023070),
            (hex!("01222222223333333344444444550000034a00000000000034b0"), 0x40000230c0),
            (hex!("01222222223333333344444444550000034a00000000000062a0"), 0x4000023110),
            (hex!("01222222223333333344444444550000034a0000000000007260"), 0x4000023160),
            (hex!("01222222223333333344444444550000034b00000000000034c0"), 0x40000231b0),
            (hex!("01222222223333333344444444550000034b0000000000005760"), 0x4000023200),
            (hex!("01222222223333333344444444550000034b0000000000006200"), 0x4000023250),
            (hex!("01222222223333333344444444550000034c00000000000034d0"), 0x40000232a0),
            (hex!("01222222223333333344444444550000034d00000000000034e0"), 0x40000232f0),
            (hex!("01222222223333333344444444550000034e00000000000034f0"), 0x4000023340),
            (hex!("01222222223333333344444444550000034e0000000000007790"), 0x4000023390),
            (hex!("01222222223333333344444444550000034f0000000000003500"), 0x40000233e0),
            (hex!("0122222222333333334444444455000003500000000000003510"), 0x4000023430),
            (hex!("0122222222333333334444444455000003510000000000003520"), 0x4000023480),
            (hex!("0122222222333333334444444455000003520000000000003530"), 0x40000234d0),
            (hex!("01222222223333333344444444550000035200000000000056a0"), 0x4000023520),
            (hex!("0122222222333333334444444455000003530000000000003540"), 0x4000023570),
            (hex!("0122222222333333334444444455000003540000000000003550"), 0x40000235c0),
            (hex!("01222222223333333344444444550000035400000000000047b0"), 0x4000023610),
            (hex!("0122222222333333334444444455000003550000000000003560"), 0x4000023660),
            (hex!("0122222222333333334444444455000003550000000000004500"), 0x40000236b0),
            (hex!("0122222222333333334444444455000003560000000000003570"), 0x4000023700),
            (hex!("0122222222333333334444444455000003560000000000004fc0"), 0x4000023750),
            (hex!("0122222222333333334444444455000003560000000000007160"), 0x40000237a0),
            (hex!("0122222222333333334444444455000003560000000000007400"), 0x40000237f0),
            (hex!("0122222222333333334444444455000003570000000000003580"), 0x4000023840),
            (hex!("0122222222333333334444444455000003580000000000003590"), 0x4000023890),
            (hex!("0122222222333333334444444455000003580000000000005a80"), 0x40000238e0),
            (hex!("01222222223333333344444444550000035900000000000035a0"), 0x4000023930),
            (hex!("01222222223333333344444444550000035900000000000073b0"), 0x4000023980),
            (hex!("01222222223333333344444444550000035a00000000000035b0"), 0x40000239d0),
            (hex!("01222222223333333344444444550000035a0000000000004c20"), 0x4000023a20),
            (hex!("01222222223333333344444444550000035b00000000000035c0"), 0x4000023a70),
            (hex!("01222222223333333344444444550000035b0000000000005120"), 0x4000023ac0),
            (hex!("01222222223333333344444444550000035c00000000000035d0"), 0x4000023b10),
            (hex!("01222222223333333344444444550000035c0000000000004300"), 0x4000023b60),
            (hex!("01222222223333333344444444550000035c0000000000005a40"), 0x4000023bb0),
            (hex!("01222222223333333344444444550000035c0000000000006620"), 0x4000023c00),
            (hex!("01222222223333333344444444550000035c0000000000006ed0"), 0x4000023c50),
            (hex!("01222222223333333344444444550000035d00000000000035e0"), 0x4000023ca0),
            (hex!("01222222223333333344444444550000035d0000000000005df0"), 0x4000023cf0),
            (hex!("01222222223333333344444444550000035e00000000000035f0"), 0x4000023d40),
            (hex!("01222222223333333344444444550000035f0000000000003600"), 0x4000023d90),
            (hex!("01222222223333333344444444550000035f00000000000058d0"), 0x4000023de0),
            (hex!("0122222222333333334444444455000003600000000000003610"), 0x4000023e30),
            (hex!("0122222222333333334444444455000003600000000000007b90"), 0x4000023e80),
            (hex!("0122222222333333334444444455000003610000000000003620"), 0x4000023ed0),
            (hex!("0122222222333333334444444455000003610000000000006ad0"), 0x4000023f20),
            (hex!("0122222222333333334444444455000003620000000000003630"), 0x4000023f70),
            (hex!("01222222223333333344444444550000036200000000000063a0"), 0x4000023fc0),
            (hex!("0122222222333333334444444455000003630000000000003640"), 0x4000024010),
            (hex!("0122222222333333334444444455000003630000000000007250"), 0x4000024060),
            (hex!("0122222222333333334444444455000003640000000000003650"), 0x40000240b0),
            (hex!("0122222222333333334444444455000003640000000000005510"), 0x4000024100),
            (hex!("0122222222333333334444444455000003640000000000007850"), 0x4000024150),
            (hex!("0122222222333333334444444455000003650000000000003660"), 0x40000241a0),
            (hex!("0122222222333333334444444455000003660000000000003670"), 0x40000241f0),
            (hex!("0122222222333333334444444455000003660000000000004650"), 0x4000024240),
            (hex!("01222222223333333344444444550000036600000000000050d0"), 0x4000024290),
            (hex!("0122222222333333334444444455000003660000000000006eb0"), 0x40000242e0),
            (hex!("0122222222333333334444444455000003670000000000003680"), 0x4000024330),
            (hex!("01222222223333333344444444550000036700000000000071f0"), 0x4000024380),
            (hex!("0122222222333333334444444455000003680000000000003690"), 0x40000243d0),
            (hex!("01222222223333333344444444550000036900000000000036a0"), 0x4000024420),
            (hex!("0122222222333333334444444455000003690000000000005c70"), 0x4000024470),
            (hex!("01222222223333333344444444550000036a00000000000036b0"), 0x40000244c0),
            (hex!("01222222223333333344444444550000036a00000000000071b0"), 0x4000024510),
            (hex!("01222222223333333344444444550000036b00000000000036c0"), 0x4000024560),
            (hex!("01222222223333333344444444550000036b0000000000004670"), 0x40000245b0),
            (hex!("01222222223333333344444444550000036c00000000000036d0"), 0x4000024600),
            (hex!("01222222223333333344444444550000036c0000000000004750"), 0x4000024650),
            (hex!("01222222223333333344444444550000036c0000000000006fa0"), 0x40000246a0),
            (hex!("01222222223333333344444444550000036d00000000000036e0"), 0x40000246f0),
            (hex!("01222222223333333344444444550000036d0000000000003f70"), 0x4000024740),
            (hex!("01222222223333333344444444550000036d0000000000004b90"), 0x4000024790),
            (hex!("01222222223333333344444444550000036d00000000000057a0"), 0x40000247e0),
            (hex!("01222222223333333344444444550000036e00000000000036f0"), 0x4000024830),
            (hex!("01222222223333333344444444550000036e00000000000075d0"), 0x4000024880),
            (hex!("01222222223333333344444444550000036f0000000000003700"), 0x40000248d0),
            (hex!("0122222222333333334444444455000003700000000000003710"), 0x4000024920),
            (hex!("0122222222333333334444444455000003700000000000005aa0"), 0x4000024970),
            (hex!("0122222222333333334444444455000003710000000000003720"), 0x40000249c0),
            (hex!("0122222222333333334444444455000003710000000000005130"), 0x4000024a10),
            (hex!("0122222222333333334444444455000003710000000000006fc0"), 0x4000024a60),
            (hex!("0122222222333333334444444455000003710000000000007b00"), 0x4000024ab0),
            (hex!("0122222222333333334444444455000003720000000000003730"), 0x4000024b00),
            (hex!("01222222223333333344444444550000037200000000000054d0"), 0x4000024b50),
            (hex!("0122222222333333334444444455000003730000000000003740"), 0x4000024ba0),
            (hex!("0122222222333333334444444455000003730000000000004220"), 0x4000024bf0),
            (hex!("0122222222333333334444444455000003740000000000003750"), 0x4000024c40),
            (hex!("0122222222333333334444444455000003740000000000004720"), 0x4000024c90),
            (hex!("0122222222333333334444444455000003750000000000003760"), 0x4000024ce0),
            (hex!("0122222222333333334444444455000003750000000000004110"), 0x4000024d30),
            (hex!("0122222222333333334444444455000003760000000000003770"), 0x4000024d80),
            (hex!("0122222222333333334444444455000003770000000000003780"), 0x4000024dd0),
            (hex!("0122222222333333334444444455000003780000000000003790"), 0x4000024e20),
            (hex!("0122222222333333334444444455000003780000000000004b40"), 0x4000024e70),
            (hex!("0122222222333333334444444455000003780000000000005660"), 0x4000024ec0),
            (hex!("0122222222333333334444444455000003780000000000005ea0"), 0x4000024f10),
            (hex!("01222222223333333344444444550000037900000000000037a0"), 0x4000024f60),
            (hex!("01222222223333333344444444550000037a00000000000037b0"), 0x4000024fb0),
            (hex!("01222222223333333344444444550000037b00000000000037c0"), 0x4000025000),
            (hex!("01222222223333333344444444550000037c00000000000037d0"), 0x4000025050),
            (hex!("01222222223333333344444444550000037c0000000000004340"), 0x40000250a0),
            (hex!("01222222223333333344444444550000037c0000000000005230"), 0x40000250f0),
            (hex!("01222222223333333344444444550000037d00000000000037e0"), 0x4000025140),
            (hex!("01222222223333333344444444550000037d00000000000051e0"), 0x4000025190),
            (hex!("01222222223333333344444444550000037e00000000000037f0"), 0x40000251e0),
            (hex!("01222222223333333344444444550000037e0000000000004090"), 0x4000025230),
            (hex!("01222222223333333344444444550000037e0000000000005c20"), 0x4000025280),
            (hex!("01222222223333333344444444550000037f0000000000003800"), 0x40000252d0),
            (hex!("0122222222333333334444444455000003800000000000003810"), 0x4000025320),
            (hex!("0122222222333333334444444455000003800000000000007630"), 0x4000025370),
            (hex!("0122222222333333334444444455000003810000000000003820"), 0x40000253c0),
            (hex!("0122222222333333334444444455000003820000000000003830"), 0x4000025410),
            (hex!("0122222222333333334444444455000003820000000000004170"), 0x4000025460),
            (hex!("0122222222333333334444444455000003830000000000003840"), 0x40000254b0),
            (hex!("0122222222333333334444444455000003840000000000003850"), 0x4000025500),
            (hex!("0122222222333333334444444455000003850000000000003860"), 0x4000025550),
            (hex!("0122222222333333334444444455000003850000000000004180"), 0x40000255a0),
            (hex!("0122222222333333334444444455000003850000000000005c90"), 0x40000255f0),
            (hex!("0122222222333333334444444455000003850000000000005da0"), 0x4000025640),
            (hex!("0122222222333333334444444455000003850000000000006ff0"), 0x4000025690),
            (hex!("0122222222333333334444444455000003860000000000003870"), 0x40000256e0),
            (hex!("01222222223333333344444444550000038600000000000065c0"), 0x4000025730),
            (hex!("0122222222333333334444444455000003870000000000003880"), 0x4000025780),
            (hex!("0122222222333333334444444455000003870000000000007cc0"), 0x40000257d0),
            (hex!("0122222222333333334444444455000003880000000000003890"), 0x4000025820),
            (hex!("01222222223333333344444444550000038900000000000038a0"), 0x4000025870),
            (hex!("01222222223333333344444444550000038a00000000000038b0"), 0x40000258c0),
            (hex!("01222222223333333344444444550000038a00000000000073e0"), 0x4000025910),
            (hex!("01222222223333333344444444550000038b00000000000038c0"), 0x4000025960),
            (hex!("01222222223333333344444444550000038c00000000000038d0"), 0x40000259b0),
            (hex!("01222222223333333344444444550000038d00000000000038e0"), 0x4000025a00),
            (hex!("01222222223333333344444444550000038d00000000000069f0"), 0x4000025a50),
            (hex!("01222222223333333344444444550000038d0000000000007680"), 0x4000025aa0),
            (hex!("01222222223333333344444444550000038e00000000000038f0"), 0x4000025af0),
            (hex!("01222222223333333344444444550000038f0000000000003900"), 0x4000025b40),
            (hex!("01222222223333333344444444550000038f00000000000045b0"), 0x4000025b90),
            (hex!("01222222223333333344444444550000038f0000000000007180"), 0x4000025be0),
            (hex!("0122222222333333334444444455000003900000000000003910"), 0x4000025c30),
            (hex!("0122222222333333334444444455000003910000000000003920"), 0x4000025c80),
            (hex!("0122222222333333334444444455000003910000000000004a20"), 0x4000025cd0),
            (hex!("0122222222333333334444444455000003920000000000003930"), 0x4000025d20),
            (hex!("01222222223333333344444444550000039200000000000059b0"), 0x4000025d70),
            (hex!("0122222222333333334444444455000003930000000000003940"), 0x4000025dc0),
            (hex!("0122222222333333334444444455000003930000000000006cc0"), 0x4000025e10),
            (hex!("0122222222333333334444444455000003940000000000003950"), 0x4000025e60),
            (hex!("01222222223333333344444444550000039400000000000056c0"), 0x4000025eb0),
            (hex!("0122222222333333334444444455000003950000000000003960"), 0x4000025f00),
            (hex!("0122222222333333334444444455000003950000000000004cc0"), 0x4000025f50),
            (hex!("0122222222333333334444444455000003950000000000007720"), 0x4000025fa0),
            (hex!("0122222222333333334444444455000003960000000000003970"), 0x4000025ff0),
            (hex!("0122222222333333334444444455000003960000000000004da0"), 0x4000026040),
            (hex!("0122222222333333334444444455000003960000000000004df0"), 0x4000026090),
            (hex!("0122222222333333334444444455000003960000000000004f30"), 0x40000260e0),
            (hex!("01222222223333333344444444550000039600000000000050f0"), 0x4000026130),
            (hex!("0122222222333333334444444455000003960000000000007940"), 0x4000026180),
            (hex!("0122222222333333334444444455000003970000000000003980"), 0x40000261d0),
            (hex!("0122222222333333334444444455000003970000000000005850"), 0x4000026220),
            (hex!("0122222222333333334444444455000003970000000000007bd0"), 0x4000026270),
            (hex!("0122222222333333334444444455000003980000000000003990"), 0x40000262c0),
            (hex!("0122222222333333334444444455000003980000000000004c00"), 0x4000026310),
            (hex!("0122222222333333334444444455000003980000000000005580"), 0x4000026360),
            (hex!("01222222223333333344444444550000039900000000000039a0"), 0x40000263b0),
            (hex!("0122222222333333334444444455000003990000000000005820"), 0x4000026400),
            (hex!("01222222223333333344444444550000039a00000000000039b0"), 0x4000026450),
            (hex!("01222222223333333344444444550000039b00000000000039c0"), 0x40000264a0),
            (hex!("01222222223333333344444444550000039b0000000000004c10"), 0x40000264f0),
            (hex!("01222222223333333344444444550000039b0000000000006460"), 0x4000026540),
            (hex!("01222222223333333344444444550000039c00000000000039d0"), 0x4000026590),
            (hex!("01222222223333333344444444550000039d00000000000039e0"), 0x40000265e0),
            (hex!("01222222223333333344444444550000039d00000000000044c0"), 0x4000026630),
            (hex!("01222222223333333344444444550000039d00000000000049e0"), 0x4000026680),
            (hex!("01222222223333333344444444550000039e00000000000039f0"), 0x40000266d0),
            (hex!("01222222223333333344444444550000039f0000000000003a00"), 0x4000026720),
            (hex!("0122222222333333334444444455000003a00000000000003a10"), 0x4000026770),
            (hex!("0122222222333333334444444455000003a10000000000003a20"), 0x40000267c0),
            (hex!("0122222222333333334444444455000003a10000000000006a80"), 0x4000026810),
            (hex!("0122222222333333334444444455000003a20000000000003a30"), 0x4000026860),
            (hex!("0122222222333333334444444455000003a200000000000062b0"), 0x40000268b0),
            (hex!("0122222222333333334444444455000003a30000000000003a40"), 0x4000026900),
            (hex!("0122222222333333334444444455000003a30000000000006ce0"), 0x4000026950),
            (hex!("0122222222333333334444444455000003a40000000000003a50"), 0x40000269a0),
            (hex!("0122222222333333334444444455000003a50000000000003a60"), 0x40000269f0),
            (hex!("0122222222333333334444444455000003a60000000000003a70"), 0x4000026a40),
            (hex!("0122222222333333334444444455000003a60000000000007750"), 0x4000026a90),
            (hex!("0122222222333333334444444455000003a70000000000003a80"), 0x4000026ae0),
            (hex!("0122222222333333334444444455000003a70000000000005b10"), 0x4000026b30),
            (hex!("0122222222333333334444444455000003a80000000000003a90"), 0x4000026b80),
            (hex!("0122222222333333334444444455000003a80000000000006c20"), 0x4000026bd0),
            (hex!("0122222222333333334444444455000003a90000000000003aa0"), 0x4000026c20),
            (hex!("0122222222333333334444444455000003a90000000000005b70"), 0x4000026c70),
            (hex!("0122222222333333334444444455000003a900000000000070e0"), 0x4000026cc0),
            (hex!("0122222222333333334444444455000003aa0000000000003ab0"), 0x4000026d10),
            (hex!("0122222222333333334444444455000003aa00000000000049f0"), 0x4000026d60),
            (hex!("0122222222333333334444444455000003aa0000000000004d60"), 0x4000026db0),
            (hex!("0122222222333333334444444455000003ab0000000000003ac0"), 0x4000026e00),
            (hex!("0122222222333333334444444455000003ac0000000000003ad0"), 0x4000026e50),
            (hex!("0122222222333333334444444455000003ac0000000000004580"), 0x4000026ea0),
            (hex!("0122222222333333334444444455000003ad0000000000003ae0"), 0x4000026ef0),
            (hex!("0122222222333333334444444455000003ae0000000000003af0"), 0x4000026f40),
            (hex!("0122222222333333334444444455000003af0000000000003b00"), 0x4000026f90),
            (hex!("0122222222333333334444444455000003b00000000000003b10"), 0x4000026fe0),
            (hex!("0122222222333333334444444455000003b10000000000003b20"), 0x4000027030),
            (hex!("0122222222333333334444444455000003b10000000000003fd0"), 0x4000027080),
            (hex!("0122222222333333334444444455000003b20000000000003b30"), 0x40000270d0),
            (hex!("0122222222333333334444444455000003b30000000000003b40"), 0x4000027120),
            (hex!("0122222222333333334444444455000003b40000000000003b50"), 0x4000027170),
            (hex!("0122222222333333334444444455000003b40000000000007450"), 0x40000271c0),
            (hex!("0122222222333333334444444455000003b50000000000003b60"), 0x4000027210),
            (hex!("0122222222333333334444444455000003b60000000000003b70"), 0x4000027260),
            (hex!("0122222222333333334444444455000003b70000000000003b80"), 0x40000272b0),
            (hex!("0122222222333333334444444455000003b70000000000006d50"), 0x4000027300),
            (hex!("0122222222333333334444444455000003b80000000000003b90"), 0x4000027350),
            (hex!("0122222222333333334444444455000003b800000000000057c0"), 0x40000273a0),
            (hex!("0122222222333333334444444455000003b800000000000078a0"), 0x40000273f0),
            (hex!("0122222222333333334444444455000003b90000000000003ba0"), 0x4000027440),
            (hex!("0122222222333333334444444455000003b90000000000006750"), 0x4000027490),
            (hex!("0122222222333333334444444455000003ba0000000000003bb0"), 0x40000274e0),
            (hex!("0122222222333333334444444455000003ba0000000000007a10"), 0x4000027530),
            (hex!("0122222222333333334444444455000003ba0000000000007a20"), 0x4000027580),
            (hex!("0122222222333333334444444455000003bb0000000000003bc0"), 0x40000275d0),
            (hex!("0122222222333333334444444455000003bb0000000000005bc0"), 0x4000027620),
            (hex!("0122222222333333334444444455000003bc0000000000003bd0"), 0x4000027670),
            (hex!("0122222222333333334444444455000003bc0000000000005e80"), 0x40000276c0),
            (hex!("0122222222333333334444444455000003bc0000000000007ab0"), 0x4000027710),
            (hex!("0122222222333333334444444455000003bd0000000000003be0"), 0x4000027760),
            (hex!("0122222222333333334444444455000003bd00000000000049b0"), 0x40000277b0),
            (hex!("0122222222333333334444444455000003be0000000000003bf0"), 0x4000027800),
            (hex!("0122222222333333334444444455000003be0000000000005780"), 0x4000027850),
            (hex!("0122222222333333334444444455000003be0000000000007930"), 0x40000278a0),
            (hex!("0122222222333333334444444455000003bf0000000000003c00"), 0x40000278f0),
            (hex!("0122222222333333334444444455000003bf0000000000005de0"), 0x4000027940),
            (hex!("0122222222333333334444444455000003bf00000000000060b0"), 0x4000027990),
            (hex!("0122222222333333334444444455000003bf00000000000060c0"), 0x40000279e0),
            (hex!("0122222222333333334444444455000003bf0000000000006a50"), 0x4000027a30),
            (hex!("0122222222333333334444444455000003c00000000000003c10"), 0x4000027a80),
            (hex!("0122222222333333334444444455000003c00000000000004030"), 0x4000027ad0),
            (hex!("0122222222333333334444444455000003c10000000000003c20"), 0x4000027b20),
            (hex!("0122222222333333334444444455000003c20000000000003c30"), 0x4000027b70),
            (hex!("0122222222333333334444444455000003c200000000000040b0"), 0x4000027bc0),
            (hex!("0122222222333333334444444455000003c30000000000003c40"), 0x4000027c10),
            (hex!("0122222222333333334444444455000003c40000000000003c50"), 0x4000027c60),
            (hex!("0122222222333333334444444455000003c40000000000005ba0"), 0x4000027cb0),
            (hex!("0122222222333333334444444455000003c50000000000003c60"), 0x4000027d00),
            (hex!("0122222222333333334444444455000003c60000000000003c70"), 0x4000027d50),
            (hex!("0122222222333333334444444455000003c70000000000003c80"), 0x4000027da0),
            (hex!("0122222222333333334444444455000003c70000000000004270"), 0x4000027df0),
            (hex!("0122222222333333334444444455000003c80000000000003c90"), 0x4000027e40),
            (hex!("0122222222333333334444444455000003c80000000000006e70"), 0x4000027e90),
            (hex!("0122222222333333334444444455000003c90000000000003ca0"), 0x4000027ee0),
            (hex!("0122222222333333334444444455000003ca0000000000003cb0"), 0x4000027f30),
            (hex!("0122222222333333334444444455000003ca0000000000006e20"), 0x4000027f80),
            (hex!("0122222222333333334444444455000003ca0000000000007c20"), 0x4000027fd0),
            (hex!("0122222222333333334444444455000003cb0000000000003cc0"), 0x4000028020),
            (hex!("0122222222333333334444444455000003cc0000000000003cd0"), 0x4000028070),
            (hex!("0122222222333333334444444455000003cc0000000000006120"), 0x40000280c0),
            (hex!("0122222222333333334444444455000003cc0000000000007950"), 0x4000028110),
            (hex!("0122222222333333334444444455000003cd0000000000003ce0"), 0x4000028160),
            (hex!("0122222222333333334444444455000003ce0000000000003cf0"), 0x40000281b0),
            (hex!("0122222222333333334444444455000003cf0000000000003d00"), 0x4000028200),
            (hex!("0122222222333333334444444455000003d00000000000003d10"), 0x4000028250),
            (hex!("0122222222333333334444444455000003d10000000000003d20"), 0x40000282a0),
            (hex!("0122222222333333334444444455000003d10000000000005e50"), 0x40000282f0),
            (hex!("0122222222333333334444444455000003d10000000000007880"), 0x4000028340),
            (hex!("0122222222333333334444444455000003d20000000000003d30"), 0x4000028390),
            (hex!("0122222222333333334444444455000003d20000000000005d00"), 0x40000283e0),
            (hex!("0122222222333333334444444455000003d30000000000003d40"), 0x4000028430),
            (hex!("0122222222333333334444444455000003d30000000000005d40"), 0x4000028480),
            (hex!("0122222222333333334444444455000003d300000000000063f0"), 0x40000284d0),
            (hex!("0122222222333333334444444455000003d40000000000003d50"), 0x4000028520),
            (hex!("0122222222333333334444444455000003d40000000000005700"), 0x4000028570),
            (hex!("0122222222333333334444444455000003d400000000000078f0"), 0x40000285c0),
            (hex!("0122222222333333334444444455000003d50000000000003d60"), 0x4000028610),
            (hex!("0122222222333333334444444455000003d60000000000003d70"), 0x4000028660),
            (hex!("0122222222333333334444444455000003d70000000000003d80"), 0x40000286b0),
            (hex!("0122222222333333334444444455000003d80000000000003d90"), 0x4000028700),
            (hex!("0122222222333333334444444455000003d80000000000006690"), 0x4000028750),
            (hex!("0122222222333333334444444455000003d90000000000003da0"), 0x40000287a0),
            (hex!("0122222222333333334444444455000003d900000000000076d0"), 0x40000287f0),
            (hex!("0122222222333333334444444455000003da0000000000003db0"), 0x4000028840),
            (hex!("0122222222333333334444444455000003db0000000000003dc0"), 0x4000028890),
            (hex!("0122222222333333334444444455000003db0000000000004a30"), 0x40000288e0),
            (hex!("0122222222333333334444444455000003db0000000000005390"), 0x4000028930),
            (hex!("0122222222333333334444444455000003dc0000000000003dd0"), 0x4000028980),
            (hex!("0122222222333333334444444455000003dc0000000000006d60"), 0x40000289d0),
            (hex!("0122222222333333334444444455000003dd0000000000003de0"), 0x4000028a20),
            (hex!("0122222222333333334444444455000003de0000000000003df0"), 0x4000028a70),
            (hex!("0122222222333333334444444455000003df0000000000003e00"), 0x4000028ac0),
            (hex!("0122222222333333334444444455000003df0000000000005240"), 0x4000028b10),
            (hex!("0122222222333333334444444455000003df0000000000005610"), 0x4000028b60),
            (hex!("0122222222333333334444444455000003e00000000000003e10"), 0x4000028bb0),
            (hex!("0122222222333333334444444455000003e00000000000006500"), 0x4000028c00),
            (hex!("0122222222333333334444444455000003e10000000000003e20"), 0x4000028c50),
            (hex!("0122222222333333334444444455000003e10000000000006a10"), 0x4000028ca0),
            (hex!("0122222222333333334444444455000003e10000000000007c10"), 0x4000028cf0),
            (hex!("0122222222333333334444444455000003e20000000000003e30"), 0x4000028d40),
            (hex!("0122222222333333334444444455000003e20000000000006310"), 0x4000028d90),
            (hex!("0122222222333333334444444455000003e30000000000003e40"), 0x4000028de0),
            (hex!("0122222222333333334444444455000003e40000000000003e50"), 0x4000028e30),
            (hex!("0122222222333333334444444455000003e40000000000006780"), 0x4000028e80),
            (hex!("0122222222333333334444444455000003e40000000000007ce0"), 0x4000028ed0),
            (hex!("0122222222333333334444444455000003e50000000000003e60"), 0x4000028f20),
            (hex!("0122222222333333334444444455000003e60000000000003e70"), 0x4000028f70),
            (hex!("0122222222333333334444444455000003e60000000000005040"), 0x4000028fc0),
            (hex!("0122222222333333334444444455000003e60000000000005bf0"), 0x4000029010),
            (hex!("0122222222333333334444444455000003e70000000000003e80"), 0x4000029060),
            (hex!("0122222222333333334444444455000003e70000000000003f50"), 0x40000290b0),
        ];
        // Build a tree from it
        let mut disk = TestDisk::new();
        let mut writer = DiskBtreeBuilder::new(18 + 8, &mut disk);

        for (key, val) in DATA {
            writer.append(&key[..], val)?;
        }
        let (root_offset, writer) = writer.finish()?;

        println!("SIZE: {} blocks", writer.blocks.len());

        let reader = DiskBtreeReader::new(root_offset, disk);

        // Test get() operation on all the keys
        for (key, val) in DATA {
            assert_eq!(reader.get(&key[..])?, Some(val));
        }

        // Test full scan
        let mut count = 0;
        reader.visit(&[], VisitDirection::Forwards, |_key, _value| {
            count += 1;
            true
        })?;
        assert_eq!(count, DATA.len());

        reader.dump()?;

        Ok(())
    }
}
