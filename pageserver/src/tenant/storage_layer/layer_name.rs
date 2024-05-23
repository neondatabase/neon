//!
//! Helper functions for dealing with filenames of the image and delta layer files.
//!
use crate::repository::Key;
use std::borrow::Cow;
use std::cmp::Ordering;
use std::fmt;
use std::ops::Range;
use std::str::FromStr;

use regex::Regex;
use utils::lsn::Lsn;

use super::PersistentLayerDesc;

// Note: Timeline::load_layer_map() relies on this sort order
#[derive(PartialEq, Eq, Clone, Hash)]
pub struct DeltaLayerName {
    pub key_range: Range<Key>,
    pub lsn_range: Range<Lsn>,
}

impl std::fmt::Debug for DeltaLayerName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        use super::RangeDisplayDebug;

        f.debug_struct("DeltaLayerName")
            .field("key_range", &RangeDisplayDebug(&self.key_range))
            .field("lsn_range", &self.lsn_range)
            .finish()
    }
}

impl PartialOrd for DeltaLayerName {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for DeltaLayerName {
    fn cmp(&self, other: &Self) -> Ordering {
        let mut cmp = self.key_range.start.cmp(&other.key_range.start);
        if cmp != Ordering::Equal {
            return cmp;
        }
        cmp = self.key_range.end.cmp(&other.key_range.end);
        if cmp != Ordering::Equal {
            return cmp;
        }
        cmp = self.lsn_range.start.cmp(&other.lsn_range.start);
        if cmp != Ordering::Equal {
            return cmp;
        }
        cmp = self.lsn_range.end.cmp(&other.lsn_range.end);

        cmp
    }
}

/// Represents the region of the LSN-Key space covered by a DeltaLayer
///
/// ```text
///    <key start>-<key end>__<LSN start>-<LSN end>
/// ```
impl DeltaLayerName {
    /// Parse the part of a delta layer's file name that represents the LayerName. Returns None
    /// if the filename does not match the expected pattern.
    pub fn parse_str(fname: &str) -> Option<Self> {
        let mut parts = fname.split("__");
        let mut key_parts = parts.next()?.split('-');
        let mut lsn_parts = parts.next()?.split('-');

        let key_start_str = key_parts.next()?;
        let key_end_str = key_parts.next()?;
        let lsn_start_str = lsn_parts.next()?;
        let lsn_end_str = lsn_parts.next()?;

        if parts.next().is_some() || key_parts.next().is_some() || key_parts.next().is_some() {
            return None;
        }

        if key_start_str.len() != 36
            || key_end_str.len() != 36
            || lsn_start_str.len() != 16
            || lsn_end_str.len() != 16
        {
            return None;
        }

        let key_start = Key::from_hex(key_start_str).ok()?;
        let key_end = Key::from_hex(key_end_str).ok()?;

        let start_lsn = Lsn::from_hex(lsn_start_str).ok()?;
        let end_lsn = Lsn::from_hex(lsn_end_str).ok()?;

        if start_lsn >= end_lsn {
            return None;
            // or panic?
        }

        if key_start >= key_end {
            return None;
            // or panic?
        }

        Some(DeltaLayerName {
            key_range: key_start..key_end,
            lsn_range: start_lsn..end_lsn,
        })
    }
}

impl fmt::Display for DeltaLayerName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{}-{}__{:016X}-{:016X}",
            self.key_range.start,
            self.key_range.end,
            u64::from(self.lsn_range.start),
            u64::from(self.lsn_range.end),
        )
    }
}

#[derive(PartialEq, Eq, Clone, Hash)]
pub struct ImageLayerName {
    pub key_range: Range<Key>,
    pub lsn: Lsn,
}

impl std::fmt::Debug for ImageLayerName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        use super::RangeDisplayDebug;

        f.debug_struct("ImageLayerName")
            .field("key_range", &RangeDisplayDebug(&self.key_range))
            .field("lsn", &self.lsn)
            .finish()
    }
}

impl PartialOrd for ImageLayerName {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for ImageLayerName {
    fn cmp(&self, other: &Self) -> Ordering {
        let mut cmp = self.key_range.start.cmp(&other.key_range.start);
        if cmp != Ordering::Equal {
            return cmp;
        }
        cmp = self.key_range.end.cmp(&other.key_range.end);
        if cmp != Ordering::Equal {
            return cmp;
        }
        cmp = self.lsn.cmp(&other.lsn);

        cmp
    }
}

impl ImageLayerName {
    pub fn lsn_as_range(&self) -> Range<Lsn> {
        // Saves from having to copypaste this all over
        PersistentLayerDesc::image_layer_lsn_range(self.lsn)
    }
}

///
/// Represents the part of the Key-LSN space covered by an ImageLayer
///
/// ```text
///    <key start>-<key end>__<LSN>
/// ```
impl ImageLayerName {
    /// Parse a string as then LayerName part of an image layer file name. Returns None if the
    /// filename does not match the expected pattern.
    pub fn parse_str(fname: &str) -> Option<Self> {
        let mut parts = fname.split("__");
        let mut key_parts = parts.next()?.split('-');

        let key_start_str = key_parts.next()?;
        let key_end_str = key_parts.next()?;
        let lsn_str = parts.next()?;
        if parts.next().is_some() || key_parts.next().is_some() {
            return None;
        }

        if key_start_str.len() != 36 || key_end_str.len() != 36 || lsn_str.len() != 16 {
            return None;
        }

        let key_start = Key::from_hex(key_start_str).ok()?;
        let key_end = Key::from_hex(key_end_str).ok()?;

        let lsn = Lsn::from_hex(lsn_str).ok()?;

        Some(ImageLayerName {
            key_range: key_start..key_end,
            lsn,
        })
    }
}

impl fmt::Display for ImageLayerName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{}-{}__{:016X}",
            self.key_range.start,
            self.key_range.end,
            u64::from(self.lsn),
        )
    }
}

/// LayerName is the logical identity of a layer within a LayerMap at a moment in time.  The
/// LayerName is not a unique filename, as the same LayerName may have multiple physical incarnations
/// over time (e.g. across shard splits or compression). The physical filenames of layers in local
/// storage and object names in remote storage consist of the LayerName plus some extra qualifiers
/// that uniquely identify the physical incarnation of a layer (see [crate::tenant::remote_timeline_client::remote_layer_path])
/// and [`crate::tenant::storage_layer::layer::local_layer_path`])
#[derive(Debug, PartialEq, Eq, Hash, Clone)]
pub enum LayerName {
    Image(ImageLayerName),
    Delta(DeltaLayerName),
}

impl LayerName {
    /// Determines if this layer file is considered to be in future meaning we will discard these
    /// layers during timeline initialization from the given disk_consistent_lsn.
    pub(crate) fn is_in_future(&self, disk_consistent_lsn: Lsn) -> bool {
        use LayerName::*;
        match self {
            Image(file_name) if file_name.lsn > disk_consistent_lsn => true,
            Delta(file_name) if file_name.lsn_range.end > disk_consistent_lsn + 1 => true,
            _ => false,
        }
    }

    pub(crate) fn kind(&self) -> &'static str {
        use LayerName::*;
        match self {
            Delta(_) => "delta",
            Image(_) => "image",
        }
    }
}

impl fmt::Display for LayerName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Image(fname) => write!(f, "{fname}"),
            Self::Delta(fname) => write!(f, "{fname}"),
        }
    }
}

impl From<ImageLayerName> for LayerName {
    fn from(fname: ImageLayerName) -> Self {
        Self::Image(fname)
    }
}
impl From<DeltaLayerName> for LayerName {
    fn from(fname: DeltaLayerName) -> Self {
        Self::Delta(fname)
    }
}

impl FromStr for LayerName {
    type Err = String;

    /// Conversion from either a physical layer filename, or the string-ization of
    /// Self. When loading a physical layer filename, we drop any extra information
    /// not needed to build Self.
    fn from_str(value: &str) -> Result<Self, Self::Err> {
        let gen_suffix_regex = Regex::new("^(?<base>.+)(?<gen>-v1-[0-9a-f]{8})$").unwrap();
        let file_name: Cow<str> = match gen_suffix_regex.captures(value) {
            Some(captures) => captures
                .name("base")
                .expect("Non-optional group")
                .as_str()
                .into(),
            None => value.into(),
        };

        let delta = DeltaLayerName::parse_str(&file_name);
        let image = ImageLayerName::parse_str(&file_name);
        let ok = match (delta, image) {
            (None, None) => {
                return Err(format!(
                    "neither delta nor image layer file name: {value:?}"
                ))
            }
            (Some(delta), None) => Self::Delta(delta),
            (None, Some(image)) => Self::Image(image),
            (Some(_), Some(_)) => unreachable!(),
        };
        Ok(ok)
    }
}

impl serde::Serialize for LayerName {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        match self {
            Self::Image(fname) => serializer.collect_str(fname),
            Self::Delta(fname) => serializer.collect_str(fname),
        }
    }
}

impl<'de> serde::Deserialize<'de> for LayerName {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        deserializer.deserialize_string(LayerNameVisitor)
    }
}

struct LayerNameVisitor;

impl<'de> serde::de::Visitor<'de> for LayerNameVisitor {
    type Value = LayerName;

    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        write!(
            formatter,
            "a string that is a valid image or delta layer file name"
        )
    }
    fn visit_str<E>(self, v: &str) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        v.parse().map_err(|e| E::custom(e))
    }
}

#[cfg(test)]
mod test {
    use super::*;
    #[test]
    fn image_layer_parse() {
        let expected = LayerName::Image(ImageLayerName {
            key_range: Key::from_i128(0)
                ..Key::from_hex("000000067F00000001000004DF0000000006").unwrap(),
            lsn: Lsn::from_hex("00000000014FED58").unwrap(),
        });
        let parsed = LayerName::from_str("000000000000000000000000000000000000-000000067F00000001000004DF0000000006__00000000014FED58-v1-00000001").unwrap();
        assert_eq!(parsed, expected,);

        // Omitting generation suffix is valid
        let parsed = LayerName::from_str("000000000000000000000000000000000000-000000067F00000001000004DF0000000006__00000000014FED58").unwrap();
        assert_eq!(parsed, expected,);
    }

    #[test]
    fn delta_layer_parse() {
        let expected = LayerName::Delta(DeltaLayerName {
            key_range: Key::from_i128(0)
                ..Key::from_hex("000000067F00000001000004DF0000000006").unwrap(),
            lsn_range: Lsn::from_hex("00000000014FED58").unwrap()
                ..Lsn::from_hex("000000000154C481").unwrap(),
        });
        let parsed = LayerName::from_str("000000000000000000000000000000000000-000000067F00000001000004DF0000000006__00000000014FED58-000000000154C481-v1-00000001").unwrap();
        assert_eq!(parsed, expected);

        // Omitting generation suffix is valid
        let parsed = LayerName::from_str("000000000000000000000000000000000000-000000067F00000001000004DF0000000006__00000000014FED58-000000000154C481").unwrap();
        assert_eq!(parsed, expected);
    }
}
