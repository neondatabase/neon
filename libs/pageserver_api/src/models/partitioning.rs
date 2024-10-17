use utils::lsn::Lsn;

use crate::keyspace::SparseKeySpace;

#[derive(Debug, PartialEq, Eq)]
pub struct Partitioning {
    pub keys: crate::keyspace::KeySpace,
    pub sparse_keys: crate::keyspace::SparseKeySpace,
    pub at_lsn: Lsn,
}

impl serde::Serialize for Partitioning {
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        pub struct KeySpace<'a>(&'a crate::keyspace::KeySpace);

        impl serde::Serialize for KeySpace<'_> {
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

        use serde::ser::SerializeMap;
        let mut map = serializer.serialize_map(Some(2))?;
        map.serialize_key("keys")?;
        map.serialize_value(&KeySpace(&self.keys))?;
        map.serialize_key("sparse_keys")?;
        map.serialize_value(&KeySpace(&self.sparse_keys.0))?;
        map.serialize_key("at_lsn")?;
        map.serialize_value(&WithDisplay(&self.at_lsn))?;
        map.end()
    }
}

pub struct WithDisplay<'a, T>(&'a T);

impl<T: std::fmt::Display> serde::Serialize for WithDisplay<'_, T> {
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.collect_str(&self.0)
    }
}

pub struct KeyRange<'a>(&'a std::ops::Range<crate::key::Key>);

impl serde::Serialize for KeyRange<'_> {
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

impl<'a> serde::Deserialize<'a> for Partitioning {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'a>,
    {
        pub struct KeySpace(crate::keyspace::KeySpace);

        impl<'de> serde::Deserialize<'de> for KeySpace {
            fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                #[serde_with::serde_as]
                #[derive(serde::Deserialize)]
                #[serde(transparent)]
                struct Key(#[serde_as(as = "serde_with::DisplayFromStr")] crate::key::Key);

                #[serde_with::serde_as]
                #[derive(serde::Deserialize)]
                struct Range(Key, Key);

                let ranges: Vec<Range> = serde::Deserialize::deserialize(deserializer)?;
                Ok(Self(crate::keyspace::KeySpace {
                    ranges: ranges
                        .into_iter()
                        .map(|Range(start, end)| (start.0..end.0))
                        .collect(),
                }))
            }
        }

        #[serde_with::serde_as]
        #[derive(serde::Deserialize)]
        struct De {
            keys: KeySpace,
            sparse_keys: KeySpace,
            #[serde_as(as = "serde_with::DisplayFromStr")]
            at_lsn: Lsn,
        }

        let de: De = serde::Deserialize::deserialize(deserializer)?;
        Ok(Self {
            at_lsn: de.at_lsn,
            keys: de.keys.0,
            sparse_keys: SparseKeySpace(de.sparse_keys.0),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_serialization_roundtrip() {
        let reference = r#"
        {
            "keys": [
              [
                "000000000000000000000000000000000000",
                "000000000000000000000000000000000001"
              ],
              [
                "000000067F00000001000000000000000000",
                "000000067F00000001000000000000000002"
              ],
              [
                "030000000000000000000000000000000000",
                "030000000000000000000000000000000003"
              ]
            ],
            "sparse_keys": [
              [
                "620000000000000000000000000000000000",
                "620000000000000000000000000000000003"
              ]
            ],
            "at_lsn": "0/2240160"
        }
        "#;

        let de: Partitioning = serde_json::from_str(reference).unwrap();

        let ser = serde_json::to_string(&de).unwrap();

        let ser_de: serde_json::Value = serde_json::from_str(&ser).unwrap();

        assert_eq!(
            ser_de,
            serde_json::from_str::<'_, serde_json::Value>(reference).unwrap()
        );
    }
}
