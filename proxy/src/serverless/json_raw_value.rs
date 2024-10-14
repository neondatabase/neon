//! [`serde_json::Value`] but uses RawValue internally
//!
//! This code forks from the serde_json code, but replaces internal Value with RawValue where possible.
//!
//! Taken from <https://github.com/serde-rs/json/blob/faab2e8d2fcf781a3f77f329df836ffb3aaacfba/src/value/de.rs>
//! Licensed from serde-rs under MIT or APACHE-2.0, with modifications by Conrad Ludgate

use core::fmt;
use std::borrow::Cow;

use indexmap::IndexMap;
use serde::{
    de::{MapAccess, SeqAccess, Visitor},
    Deserialize, Serialize,
};
use serde_json::{value::RawValue, Number};

pub enum LazyValue<'de> {
    Null,
    Bool(bool),
    Number(Number),
    String(Cow<'de, str>),
    Array(Vec<&'de RawValue>),
    Object(IndexMap<String, &'de RawValue>),
}

impl<'de> Deserialize<'de> for LazyValue<'de> {
    #[inline]
    fn deserialize<D>(deserializer: D) -> Result<LazyValue<'de>, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        struct ValueVisitor;

        impl<'de> Visitor<'de> for ValueVisitor {
            type Value = LazyValue<'de>;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("any valid JSON value")
            }

            #[inline]
            fn visit_bool<E>(self, value: bool) -> Result<LazyValue<'de>, E> {
                Ok(LazyValue::Bool(value))
            }

            #[inline]
            fn visit_i64<E>(self, value: i64) -> Result<LazyValue<'de>, E> {
                Ok(LazyValue::Number(value.into()))
            }

            #[inline]
            fn visit_u64<E>(self, value: u64) -> Result<LazyValue<'de>, E> {
                Ok(LazyValue::Number(value.into()))
            }

            #[inline]
            fn visit_f64<E>(self, value: f64) -> Result<LazyValue<'de>, E> {
                Ok(Number::from_f64(value).map_or(LazyValue::Null, LazyValue::Number))
            }

            #[inline]
            fn visit_str<E>(self, value: &str) -> Result<LazyValue<'de>, E>
            where
                E: serde::de::Error,
            {
                self.visit_string(String::from(value))
            }

            #[inline]
            fn visit_borrowed_str<E>(self, value: &'de str) -> Result<LazyValue<'de>, E>
            where
                E: serde::de::Error,
            {
                Ok(LazyValue::String(Cow::Borrowed(value)))
            }

            #[inline]
            fn visit_string<E>(self, value: String) -> Result<LazyValue<'de>, E> {
                Ok(LazyValue::String(Cow::Owned(value)))
            }

            #[inline]
            fn visit_none<E>(self) -> Result<LazyValue<'de>, E> {
                Ok(LazyValue::Null)
            }

            #[inline]
            fn visit_some<D>(self, deserializer: D) -> Result<LazyValue<'de>, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                Deserialize::deserialize(deserializer)
            }

            #[inline]
            fn visit_unit<E>(self) -> Result<LazyValue<'de>, E> {
                Ok(LazyValue::Null)
            }

            #[inline]
            fn visit_seq<V>(self, mut visitor: V) -> Result<LazyValue<'de>, V::Error>
            where
                V: SeqAccess<'de>,
            {
                let mut vec = Vec::new();

                while let Some(elem) = visitor.next_element()? {
                    vec.push(elem);
                }

                Ok(LazyValue::Array(vec))
            }

            fn visit_map<V>(self, mut visitor: V) -> Result<LazyValue<'de>, V::Error>
            where
                V: MapAccess<'de>,
            {
                let mut values = IndexMap::new();

                while let Some((key, value)) = visitor.next_entry()? {
                    values.insert(key, value);
                }

                Ok(LazyValue::Object(values))
            }
        }

        deserializer.deserialize_any(ValueVisitor)
    }
}

impl Serialize for LazyValue<'_> {
    #[inline]
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: ::serde::Serializer,
    {
        match self {
            LazyValue::Null => serializer.serialize_unit(),
            LazyValue::Bool(b) => serializer.serialize_bool(*b),
            LazyValue::Number(n) => n.serialize(serializer),
            LazyValue::String(s) => serializer.serialize_str(s),
            LazyValue::Array(v) => v.serialize(serializer),
            LazyValue::Object(m) => {
                use serde::ser::SerializeMap;
                let mut map = serializer.serialize_map(Some(m.len()))?;
                for (k, v) in m {
                    map.serialize_entry(k, v)?;
                }
                map.end()
            }
        }
    }
}

#[allow(clippy::to_string_trait_impl)]
impl ToString for LazyValue<'_> {
    fn to_string(&self) -> String {
        serde_json::to_string(self).expect("json encoding a LazyValue should never error")
    }
}

#[cfg(test)]
mod tests {
    use std::borrow::Cow;

    use typed_json::json;

    use super::LazyValue;

    #[test]
    fn object() {
        let json = json! {{
            "foo": {
                "bar": 1
            },
            "baz": [2, 3],
        }}
        .to_string();

        let lazy: LazyValue = serde_json::from_str(&json).unwrap();

        let LazyValue::Object(object) = lazy else {
            panic!("expected object")
        };
        assert_eq!(object.len(), 2);

        assert_eq!(object["foo"].get(), r#"{"bar":1}"#);
        assert_eq!(object["baz"].get(), r#"[2,3]"#);
    }

    #[test]
    fn array() {
        let json = json! {[
            {
                "bar": 1
            },
            [2, 3],
        ]}
        .to_string();

        let lazy: LazyValue = serde_json::from_str(&json).unwrap();

        let LazyValue::Array(array) = lazy else {
            panic!("expected array")
        };
        assert_eq!(array.len(), 2);

        assert_eq!(array[0].get(), r#"{"bar":1}"#);
        assert_eq!(array[1].get(), r#"[2,3]"#);
    }

    #[test]
    fn string() {
        let json = json! { "hello world" }.to_string();

        let lazy: LazyValue = serde_json::from_str(&json).unwrap();

        let LazyValue::String(Cow::Borrowed(string)) = lazy else {
            panic!("expected borrowed string")
        };
        assert_eq!(string, "hello world");

        let json = json! { "hello \n world" }.to_string();

        let lazy: LazyValue = serde_json::from_str(&json).unwrap();

        let LazyValue::String(Cow::Owned(string)) = lazy else {
            panic!("expected owned string")
        };
        assert_eq!(string, "hello \n world");
    }
}
