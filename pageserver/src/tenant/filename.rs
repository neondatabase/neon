//!
//! Helper functions for dealing with filenames of the image and delta layer files.
//!
use crate::config::PageServerConf;
use crate::repository::Key;
use std::cmp::Ordering;
use std::fmt;
use std::ops::Range;
use std::path::PathBuf;
use std::str::FromStr;

use utils::lsn::Lsn;

// Note: Timeline::load_layer_map() relies on this sort order
#[derive(Debug, PartialEq, Eq, Clone, Hash)]
pub struct DeltaFileName {
    pub key_range: Range<Key>,
    pub lsn_range: Range<Lsn>,
}

impl PartialOrd for DeltaFileName {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for DeltaFileName {
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

/// Represents the filename of a DeltaLayer
///
///    <key start>-<key end>__<LSN start>-<LSN end>
///
impl DeltaFileName {
    ///
    /// Parse a string as a delta file name. Returns None if the filename does not
    /// match the expected pattern.
    ///
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

        Some(DeltaFileName {
            key_range: key_start..key_end,
            lsn_range: start_lsn..end_lsn,
        })
    }
}

impl fmt::Display for DeltaFileName {
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

#[derive(Debug, PartialEq, Eq, Clone, Hash)]
pub struct ImageFileName {
    pub key_range: Range<Key>,
    pub lsn: Lsn,
}

impl PartialOrd for ImageFileName {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for ImageFileName {
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

///
/// Represents the filename of an ImageLayer
///
///    <key start>-<key end>__<LSN>
impl ImageFileName {
    ///
    /// Parse a string as an image file name. Returns None if the filename does not
    /// match the expected pattern.
    ///
    pub fn parse_str(fname: &str) -> Option<Self> {
        let mut parts = fname.split("__");
        let mut key_parts = parts.next()?.split('-');

        let key_start_str = key_parts.next()?;
        let key_end_str = key_parts.next()?;
        let lsn_str = parts.next()?;
        if parts.next().is_some() || key_parts.next().is_some() {
            return None;
        }

        let key_start = Key::from_hex(key_start_str).ok()?;
        let key_end = Key::from_hex(key_end_str).ok()?;

        let lsn = Lsn::from_hex(lsn_str).ok()?;

        Some(ImageFileName {
            key_range: key_start..key_end,
            lsn,
        })
    }
}

impl fmt::Display for ImageFileName {
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
#[derive(Debug, PartialEq, Eq, Hash, Clone)]
pub enum LayerFileName {
    Image(ImageFileName),
    Delta(DeltaFileName),
    #[cfg(test)]
    Test(String),
}

impl LayerFileName {
    pub fn file_name(&self) -> String {
        match self {
            LayerFileName::Image(fname) => format!("{fname}"),
            LayerFileName::Delta(fname) => format!("{fname}"),
            #[cfg(test)]
            LayerFileName::Test(fname) => fname.to_string(),
        }
    }
    #[cfg(test)]
    pub(crate) fn new_test(name: &str) -> LayerFileName {
        LayerFileName::Test(name.to_owned())
    }
}

impl From<ImageFileName> for LayerFileName {
    fn from(fname: ImageFileName) -> Self {
        LayerFileName::Image(fname)
    }
}
impl From<DeltaFileName> for LayerFileName {
    fn from(fname: DeltaFileName) -> Self {
        LayerFileName::Delta(fname)
    }
}

// include a `/` in the name as an additional layer of robustness
// because `/` chars are not allowed in UNIX paths
#[cfg(test)]
const LAYER_FILE_NAME_TEST_PREFIX: &str = "LAYER_FILE_NAME::test/";

impl FromStr for LayerFileName {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        #[cfg(test)]
        if let Some(value) = value.strip_prefix(LAYER_FILE_NAME_TEST_PREFIX) {
            return Ok(LayerFileName::Test(value.to_owned()));
        }
        let delta = DeltaFileName::parse_str(value);
        let image = ImageFileName::parse_str(value);
        let ok = match (delta, image) {
            (None, None) => {
                return Err(format!(
                    "neither delta nor image layer file name: {value:?}"
                ))
            }
            (Some(delta), None) => LayerFileName::Delta(delta),
            (None, Some(image)) => LayerFileName::Image(image),
            (Some(_), Some(_)) => unreachable!(),
        };
        Ok(ok)
    }
}

impl serde::Serialize for LayerFileName {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        match self {
            LayerFileName::Image(fname) => serializer.serialize_str(&format!("{}", fname)),
            LayerFileName::Delta(fname) => serializer.serialize_str(&format!("{}", fname)),
            #[cfg(test)]
            LayerFileName::Test(t) => {
                serializer.serialize_str(&format!("{LAYER_FILE_NAME_TEST_PREFIX}{t}"))
            }
        }
    }
}

struct LayerFileNameVisitor;

impl<'de> serde::de::Visitor<'de> for LayerFileNameVisitor {
    type Value = LayerFileName;

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

/// Helper enum to hold a PageServerConf, or a path
///
/// This is used by DeltaLayer and ImageLayer. Normally, this holds a reference to the
/// global config, and paths to layer files are constructed using the tenant/timeline
/// path from the config. But in the 'pageserver_binutils' binary, we need to construct a Layer
/// struct for a file on disk, without having a page server running, so that we have no
/// config. In that case, we use the Path variant to hold the full path to the file on
/// disk.
pub enum PathOrConf {
    Path(PathBuf),
    Conf(&'static PageServerConf),
}
