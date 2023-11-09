//! If possible, use `::pageserver_api::models` instead.

use utils::lsn::Lsn;

pub struct Partitioning {
    pub keys: crate::keyspace::KeySpace,

    pub at_lsn: Lsn,
}

impl serde::Serialize for Partitioning {
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeMap;
        let mut map = serializer.serialize_map(Some(2))?;
        map.serialize_key("keys")?;
        map.serialize_value(&KeySpace(&self.keys))?;
        map.serialize_key("at_lsn")?;
        map.serialize_value(&WithDisplay(&self.at_lsn))?;
        map.end()
    }
}

pub struct WithDisplay<'a, T>(&'a T);

impl<'a, T: std::fmt::Display> serde::Serialize for WithDisplay<'a, T> {
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.collect_str(&self.0)
    }
}

pub struct KeySpace<'a>(&'a crate::keyspace::KeySpace);

impl<'a> serde::Serialize for KeySpace<'a> {
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeSeq;
        let mut seq = serializer.serialize_seq(Some(self.0.ranges.len()))?;
        for kr in &self.0.ranges {
            seq.serialize_element(&KeyRange(kr))?;
        }
        seq.end()
    }
}

pub struct KeyRange<'a>(&'a std::ops::Range<crate::repository::Key>);

impl<'a> serde::Serialize for KeyRange<'a> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeTuple;
        let mut t = serializer.serialize_tuple(2)?;
        t.serialize_element(&WithDisplay(&self.0.start))?;
        t.serialize_element(&WithDisplay(&self.0.end))?;
        t.end()
    }
}
