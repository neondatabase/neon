//! Conversions to and from Postgres types.
//!
//! This crate is used by the `tokio-postgres` and `postgres` crates. You normally don't need to depend directly on it
//! unless you want to define your own `ToSql` or `FromSql` definitions.
//! ```
#![doc(html_root_url = "https://docs.rs/postgres-types/0.2")]
#![warn(clippy::all, rust_2018_idioms, missing_docs)]

use arrayvec::ArrayVec;
use fallible_iterator::FallibleIterator;
use postgres_protocol2::types::{self, ArrayDimension};
use std::any::type_name;
use std::borrow::Cow;
use std::collections::HashMap;
use std::error::Error;
use std::fmt;
use std::hash::BuildHasher;
use std::net::IpAddr;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use crate::type_gen::{Inner, Other};

#[doc(inline)]
pub use postgres_protocol2::Oid;

// #[doc(inline)]
// pub use pg_lsn::PgLsn;

// pub use crate::special::{Date, Timestamp};
use bytes::BytesMut;

// Number of seconds from 1970-01-01 to 2000-01-01
const TIME_SEC_CONVERSION: u64 = 946_684_800;
const USEC_PER_SEC: u64 = 1_000_000;
const NSEC_PER_USEC: u64 = 1_000;

/// Generates a simple implementation of `ToSql::accepts` which accepts the
/// types passed to it.
#[macro_export]
macro_rules! accepts {
    ($($expected:ident),+) => (
        fn accepts(ty: &$crate::Type) -> bool {
            matches!(*ty, $($crate::Type::$expected)|+)
        }
    )
}

/// Generates an implementation of `ToSql::to_sql_checked`.
///
/// All `ToSql` implementations should use this macro.
#[macro_export]
macro_rules! to_sql_checked {
    () => {
        fn to_sql_checked(
            &self,
            ty: &$crate::Type,
            out: &mut $crate::private::BytesMut,
        ) -> ::std::result::Result<
            $crate::IsNull,
            Box<dyn ::std::error::Error + ::std::marker::Sync + ::std::marker::Send>,
        > {
            $crate::__to_sql_checked(self, ty, out)
        }
    };
}

// WARNING: this function is not considered part of this crate's public API.
// It is subject to change at any time.
#[doc(hidden)]
pub fn __to_sql_checked<T>(
    v: &T,
    ty: &Type,
    out: &mut BytesMut,
) -> Result<IsNull, Box<dyn Error + Sync + Send>>
where
    T: ToSql,
{
    if !T::accepts(ty) {
        return Err(Box::new(WrongType::new::<T>(ty.clone())));
    }
    v.to_sql(ty, out)
}

// mod pg_lsn;
#[doc(hidden)]
pub mod private;
// mod special;
mod type_gen;

/// A Postgres type.
#[derive(PartialEq, Eq, Clone, Hash)]
pub struct Type(Inner);

impl fmt::Debug for Type {
    fn fmt(&self, fmt: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(&self.0, fmt)
    }
}

impl fmt::Display for Type {
    fn fmt(&self, fmt: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.schema() {
            "public" | "pg_catalog" => {}
            schema => write!(fmt, "{}.", schema)?,
        }
        fmt.write_str(self.name())
    }
}

impl Type {
    /// Creates a new `Type`.
    pub fn new(name: String, oid: Oid, kind: Kind, schema: String) -> Type {
        Type(Inner::Other(Arc::new(Other {
            name,
            oid,
            kind,
            schema,
        })))
    }

    /// Returns the `Type` corresponding to the provided `Oid` if it
    /// corresponds to a built-in type.
    pub fn from_oid(oid: Oid) -> Option<Type> {
        Inner::from_oid(oid).map(Type)
    }

    /// Returns the OID of the `Type`.
    pub fn oid(&self) -> Oid {
        self.0.oid()
    }

    /// Returns the kind of this type.
    pub fn kind(&self) -> &Kind {
        self.0.kind()
    }

    /// Returns the schema of this type.
    pub fn schema(&self) -> &str {
        match self.0 {
            Inner::Other(ref u) => &u.schema,
            _ => "pg_catalog",
        }
    }

    /// Returns the name of this type.
    pub fn name(&self) -> &str {
        self.0.name()
    }
}

/// Represents the kind of a Postgres type.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum Kind {
    /// A simple type like `VARCHAR` or `INTEGER`.
    Simple,
    /// An enumerated type along with its variants.
    Enum(Vec<String>),
    /// A pseudo-type.
    Pseudo,
    /// An array type along with the type of its elements.
    Array(Type),
    /// A range type along with the type of its elements.
    Range(Type),
    /// A multirange type along with the type of its elements.
    Multirange(Type),
    /// A domain type along with its underlying type.
    Domain(Type),
    /// A composite type along with information about its fields.
    Composite(Vec<Field>),
}

/// Information about a field of a composite type.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Field {
    name: String,
    type_: Type,
}

impl Field {
    /// Creates a new `Field`.
    pub fn new(name: String, type_: Type) -> Field {
        Field { name, type_ }
    }

    /// Returns the name of the field.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Returns the type of the field.
    pub fn type_(&self) -> &Type {
        &self.type_
    }
}

/// An error indicating that a `NULL` Postgres value was passed to a `FromSql`
/// implementation that does not support `NULL` values.
#[derive(Debug, Clone, Copy)]
pub struct WasNull;

impl fmt::Display for WasNull {
    fn fmt(&self, fmt: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt.write_str("a Postgres value was `NULL`")
    }
}

impl Error for WasNull {}

/// An error indicating that a conversion was attempted between incompatible
/// Rust and Postgres types.
#[derive(Debug)]
pub struct WrongType {
    postgres: Type,
    rust: &'static str,
}

impl fmt::Display for WrongType {
    fn fmt(&self, fmt: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            fmt,
            "cannot convert between the Rust type `{}` and the Postgres type `{}`",
            self.rust, self.postgres,
        )
    }
}

impl Error for WrongType {}

impl WrongType {
    /// Creates a new `WrongType` error.
    pub fn new<T>(ty: Type) -> WrongType {
        WrongType {
            postgres: ty,
            rust: type_name::<T>(),
        }
    }
}

/// An error indicating that a as_text conversion was attempted on a binary
/// result.
#[derive(Debug)]
pub struct WrongFormat {}

impl Error for WrongFormat {}

impl fmt::Display for WrongFormat {
    fn fmt(&self, fmt: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            fmt,
            "cannot read column as text while it is in binary format"
        )
    }
}

/// A trait for types that can be created from a Postgres value.
///
/// # Types
///
/// The following implementations are provided by this crate, along with the
/// corresponding Postgres types:
///
/// | Rust type                         | Postgres type(s)                              |
/// |-----------------------------------|-----------------------------------------------|
/// | `bool`                            | BOOL                                          |
/// | `i8`                              | "char"                                        |
/// | `i16`                             | SMALLINT, SMALLSERIAL                         |
/// | `i32`                             | INT, SERIAL                                   |
/// | `u32`                             | OID                                           |
/// | `i64`                             | BIGINT, BIGSERIAL                             |
/// | `f32`                             | REAL                                          |
/// | `f64`                             | DOUBLE PRECISION                              |
/// | `&str`/`String`                   | VARCHAR, CHAR(n), TEXT, CITEXT, NAME, UNKNOWN |
/// |                                   | LTREE, LQUERY, LTXTQUERY                      |
/// | `&[u8]`/`Vec<u8>`                 | BYTEA                                         |
/// | `HashMap<String, Option<String>>` | HSTORE                                        |
/// | `SystemTime`                      | TIMESTAMP, TIMESTAMP WITH TIME ZONE           |
/// | `IpAddr`                          | INET                                          |
///
/// In addition, some implementations are provided for types in third party
/// crates. These are disabled by default; to opt into one of these
/// implementations, activate the Cargo feature corresponding to the crate's
/// name prefixed by `with-`. For example, the `with-serde_json-1` feature enables
/// the implementation for the `serde_json::Value` type.
///
/// | Rust type                       | Postgres type(s)                    |
/// |---------------------------------|-------------------------------------|
/// | `chrono::NaiveDateTime`         | TIMESTAMP                           |
/// | `chrono::DateTime<Utc>`         | TIMESTAMP WITH TIME ZONE            |
/// | `chrono::DateTime<Local>`       | TIMESTAMP WITH TIME ZONE            |
/// | `chrono::DateTime<FixedOffset>` | TIMESTAMP WITH TIME ZONE            |
/// | `chrono::NaiveDate`             | DATE                                |
/// | `chrono::NaiveTime`             | TIME                                |
/// | `time::PrimitiveDateTime`       | TIMESTAMP                           |
/// | `time::OffsetDateTime`          | TIMESTAMP WITH TIME ZONE            |
/// | `time::Date`                    | DATE                                |
/// | `time::Time`                    | TIME                                |
/// | `eui48::MacAddress`             | MACADDR                             |
/// | `geo_types::Point<f64>`         | POINT                               |
/// | `geo_types::Rect<f64>`          | BOX                                 |
/// | `geo_types::LineString<f64>`    | PATH                                |
/// | `serde_json::Value`             | JSON, JSONB                         |
/// | `uuid::Uuid`                    | UUID                                |
/// | `bit_vec::BitVec`               | BIT, VARBIT                         |
/// | `eui48::MacAddress`             | MACADDR                             |
/// | `cidr::InetCidr`                | CIDR                                |
/// | `cidr::InetAddr`                | INET                                |
/// | `smol_str::SmolStr`             | VARCHAR, CHAR(n), TEXT, CITEXT,     |
/// |                                 | NAME, UNKNOWN, LTREE, LQUERY,       |
/// |                                 | LTXTQUERY                           |
///
/// # Nullability
///
/// In addition to the types listed above, `FromSql` is implemented for
/// `Option<T>` where `T` implements `FromSql`. An `Option<T>` represents a
/// nullable Postgres value.
///
/// # Arrays
///
/// `FromSql` is implemented for `Vec<T>`, `Box<[T]>` and `[T; N]` where `T`
/// implements `FromSql`, and corresponds to one-dimensional Postgres arrays.
///
/// **Note:** the impl for arrays only exist when the Cargo feature `array-impls`
/// is enabled.
pub trait FromSql<'a>: Sized {
    /// Creates a new value of this type from a buffer of data of the specified
    /// Postgres `Type` in its binary format.
    ///
    /// The caller of this method is responsible for ensuring that this type
    /// is compatible with the Postgres `Type`.
    fn from_sql(ty: &Type, raw: &'a [u8]) -> Result<Self, Box<dyn Error + Sync + Send>>;

    /// Creates a new value of this type from a `NULL` SQL value.
    ///
    /// The caller of this method is responsible for ensuring that this type
    /// is compatible with the Postgres `Type`.
    ///
    /// The default implementation returns `Err(Box::new(WasNull))`.
    #[allow(unused_variables)]
    fn from_sql_null(ty: &Type) -> Result<Self, Box<dyn Error + Sync + Send>> {
        Err(Box::new(WasNull))
    }

    /// A convenience function that delegates to `from_sql` and `from_sql_null` depending on the
    /// value of `raw`.
    fn from_sql_nullable(
        ty: &Type,
        raw: Option<&'a [u8]>,
    ) -> Result<Self, Box<dyn Error + Sync + Send>> {
        match raw {
            Some(raw) => Self::from_sql(ty, raw),
            None => Self::from_sql_null(ty),
        }
    }

    /// Determines if a value of this type can be created from the specified
    /// Postgres `Type`.
    fn accepts(ty: &Type) -> bool;
}

/// A trait for types which can be created from a Postgres value without borrowing any data.
///
/// This is primarily useful for trait bounds on functions.
pub trait FromSqlOwned: for<'a> FromSql<'a> {}

impl<T> FromSqlOwned for T where T: for<'a> FromSql<'a> {}

impl<'a, T: FromSql<'a>> FromSql<'a> for Option<T> {
    fn from_sql(ty: &Type, raw: &'a [u8]) -> Result<Option<T>, Box<dyn Error + Sync + Send>> {
        <T as FromSql>::from_sql(ty, raw).map(Some)
    }

    fn from_sql_null(_: &Type) -> Result<Option<T>, Box<dyn Error + Sync + Send>> {
        Ok(None)
    }

    fn accepts(ty: &Type) -> bool {
        <T as FromSql>::accepts(ty)
    }
}

impl<'a, T: FromSql<'a>> FromSql<'a> for Vec<T> {
    fn from_sql(ty: &Type, raw: &'a [u8]) -> Result<Vec<T>, Box<dyn Error + Sync + Send>> {
        let member_type = match *ty.kind() {
            Kind::Array(ref member) => member,
            _ => panic!("expected array type"),
        };

        let array = types::array_from_sql(raw)?;
        if array.dimensions().count()? > 1 {
            return Err("array contains too many dimensions".into());
        }

        array
            .values()
            .map(|v| T::from_sql_nullable(member_type, v))
            .collect()
    }

    fn accepts(ty: &Type) -> bool {
        match *ty.kind() {
            Kind::Array(ref inner) => T::accepts(inner),
            _ => false,
        }
    }
}

impl<'a, T: FromSql<'a>, const N: usize> FromSql<'a> for [T; N] {
    fn from_sql(ty: &Type, raw: &'a [u8]) -> Result<Self, Box<dyn Error + Sync + Send>> {
        let member_type = match *ty.kind() {
            Kind::Array(ref member) => member,
            _ => panic!("expected array type"),
        };

        let array = types::array_from_sql(raw)?;
        if array.dimensions().count()? > 1 {
            return Err("array contains too many dimensions".into());
        }

        let mut values = array.values();
        let mut out = ArrayVec::<T, N>::new();
        loop {
            let Some(v) = values.next()? else { break };
            if out.try_push(T::from_sql_nullable(member_type, v)?).is_err() {
                return Err(format!(
                    "excess elements in array (expected {}, got more than that)",
                    N,
                )
                .into());
            }
        }

        match out.into_inner() {
            Ok(arr) => Ok(arr),
            Err(out) => Err(format!(
                "too few elements in array (expected {}, got {})",
                N,
                out.len()
            )
            .into()),
        }
    }

    fn accepts(ty: &Type) -> bool {
        match *ty.kind() {
            Kind::Array(ref inner) => T::accepts(inner),
            _ => false,
        }
    }
}

impl<'a, T: FromSql<'a>> FromSql<'a> for Box<[T]> {
    fn from_sql(ty: &Type, raw: &'a [u8]) -> Result<Self, Box<dyn Error + Sync + Send>> {
        Vec::<T>::from_sql(ty, raw).map(Vec::into_boxed_slice)
    }

    fn accepts(ty: &Type) -> bool {
        Vec::<T>::accepts(ty)
    }
}

impl<'a> FromSql<'a> for Vec<u8> {
    fn from_sql(_: &Type, raw: &'a [u8]) -> Result<Vec<u8>, Box<dyn Error + Sync + Send>> {
        Ok(types::bytea_from_sql(raw).to_owned())
    }

    accepts!(BYTEA);
}

impl<'a> FromSql<'a> for &'a [u8] {
    fn from_sql(_: &Type, raw: &'a [u8]) -> Result<&'a [u8], Box<dyn Error + Sync + Send>> {
        Ok(types::bytea_from_sql(raw))
    }

    accepts!(BYTEA);
}

impl<'a> FromSql<'a> for String {
    fn from_sql(ty: &Type, raw: &'a [u8]) -> Result<String, Box<dyn Error + Sync + Send>> {
        <&str as FromSql>::from_sql(ty, raw).map(ToString::to_string)
    }

    fn accepts(ty: &Type) -> bool {
        <&str as FromSql>::accepts(ty)
    }
}

impl<'a> FromSql<'a> for Box<str> {
    fn from_sql(ty: &Type, raw: &'a [u8]) -> Result<Box<str>, Box<dyn Error + Sync + Send>> {
        <&str as FromSql>::from_sql(ty, raw)
            .map(ToString::to_string)
            .map(String::into_boxed_str)
    }

    fn accepts(ty: &Type) -> bool {
        <&str as FromSql>::accepts(ty)
    }
}

impl<'a> FromSql<'a> for &'a str {
    fn from_sql(ty: &Type, raw: &'a [u8]) -> Result<&'a str, Box<dyn Error + Sync + Send>> {
        match *ty {
            ref ty if ty.name() == "ltree" => types::ltree_from_sql(raw),
            ref ty if ty.name() == "lquery" => types::lquery_from_sql(raw),
            ref ty if ty.name() == "ltxtquery" => types::ltxtquery_from_sql(raw),
            _ => types::text_from_sql(raw),
        }
    }

    fn accepts(ty: &Type) -> bool {
        match *ty {
            Type::VARCHAR | Type::TEXT | Type::BPCHAR | Type::NAME | Type::UNKNOWN => true,
            ref ty
                if (ty.name() == "citext"
                    || ty.name() == "ltree"
                    || ty.name() == "lquery"
                    || ty.name() == "ltxtquery") =>
            {
                true
            }
            _ => false,
        }
    }
}

macro_rules! simple_from {
    ($t:ty, $f:ident, $($expected:ident),+) => {
        impl<'a> FromSql<'a> for $t {
            fn from_sql(_: &Type, raw: &'a [u8]) -> Result<$t, Box<dyn Error + Sync + Send>> {
                types::$f(raw)
            }

            accepts!($($expected),+);
        }
    }
}

simple_from!(bool, bool_from_sql, BOOL);
simple_from!(i8, char_from_sql, CHAR);
simple_from!(i16, int2_from_sql, INT2);
simple_from!(i32, int4_from_sql, INT4);
simple_from!(u32, oid_from_sql, OID);
simple_from!(i64, int8_from_sql, INT8);
simple_from!(f32, float4_from_sql, FLOAT4);
simple_from!(f64, float8_from_sql, FLOAT8);

impl<'a, S> FromSql<'a> for HashMap<String, Option<String>, S>
where
    S: Default + BuildHasher,
{
    fn from_sql(
        _: &Type,
        raw: &'a [u8],
    ) -> Result<HashMap<String, Option<String>, S>, Box<dyn Error + Sync + Send>> {
        types::hstore_from_sql(raw)?
            .map(|(k, v)| Ok((k.to_owned(), v.map(str::to_owned))))
            .collect()
    }

    fn accepts(ty: &Type) -> bool {
        ty.name() == "hstore"
    }
}

impl<'a> FromSql<'a> for SystemTime {
    fn from_sql(_: &Type, raw: &'a [u8]) -> Result<SystemTime, Box<dyn Error + Sync + Send>> {
        let time = types::timestamp_from_sql(raw)?;
        let epoch = UNIX_EPOCH + Duration::from_secs(TIME_SEC_CONVERSION);

        let negative = time < 0;
        let time = time.unsigned_abs();

        let secs = time / USEC_PER_SEC;
        let nsec = (time % USEC_PER_SEC) * NSEC_PER_USEC;
        let offset = Duration::new(secs, nsec as u32);

        let time = if negative {
            epoch - offset
        } else {
            epoch + offset
        };

        Ok(time)
    }

    accepts!(TIMESTAMP, TIMESTAMPTZ);
}

impl<'a> FromSql<'a> for IpAddr {
    fn from_sql(_: &Type, raw: &'a [u8]) -> Result<IpAddr, Box<dyn Error + Sync + Send>> {
        let inet = types::inet_from_sql(raw)?;
        Ok(inet.addr())
    }

    accepts!(INET);
}

/// An enum representing the nullability of a Postgres value.
pub enum IsNull {
    /// The value is NULL.
    Yes,
    /// The value is not NULL.
    No,
}

/// A trait for types that can be converted into Postgres values.
///
/// # Types
///
/// The following implementations are provided by this crate, along with the
/// corresponding Postgres types:
///
/// | Rust type                         | Postgres type(s)                     |
/// |-----------------------------------|--------------------------------------|
/// | `bool`                            | BOOL                                 |
/// | `i8`                              | "char"                               |
/// | `i16`                             | SMALLINT, SMALLSERIAL                |
/// | `i32`                             | INT, SERIAL                          |
/// | `u32`                             | OID                                  |
/// | `i64`                             | BIGINT, BIGSERIAL                    |
/// | `f32`                             | REAL                                 |
/// | `f64`                             | DOUBLE PRECISION                     |
/// | `&str`/`String`                   | VARCHAR, CHAR(n), TEXT, CITEXT, NAME |
/// |                                   | LTREE, LQUERY, LTXTQUERY             |
/// | `&[u8]`/`Vec<u8>`/`[u8; N]`       | BYTEA                                |
/// | `HashMap<String, Option<String>>` | HSTORE                               |
/// | `SystemTime`                      | TIMESTAMP, TIMESTAMP WITH TIME ZONE  |
/// | `IpAddr`                          | INET                                 |
///
/// In addition, some implementations are provided for types in third party
/// crates. These are disabled by default; to opt into one of these
/// implementations, activate the Cargo feature corresponding to the crate's
/// name prefixed by `with-`. For example, the `with-serde_json-1` feature enables
/// the implementation for the `serde_json::Value` type.
///
/// | Rust type                       | Postgres type(s)                    |
/// |---------------------------------|-------------------------------------|
/// | `chrono::NaiveDateTime`         | TIMESTAMP                           |
/// | `chrono::DateTime<Utc>`         | TIMESTAMP WITH TIME ZONE            |
/// | `chrono::DateTime<Local>`       | TIMESTAMP WITH TIME ZONE            |
/// | `chrono::DateTime<FixedOffset>` | TIMESTAMP WITH TIME ZONE            |
/// | `chrono::NaiveDate`             | DATE                                |
/// | `chrono::NaiveTime`             | TIME                                |
/// | `time::PrimitiveDateTime`       | TIMESTAMP                           |
/// | `time::OffsetDateTime`          | TIMESTAMP WITH TIME ZONE            |
/// | `time::Date`                    | DATE                                |
/// | `time::Time`                    | TIME                                |
/// | `eui48::MacAddress`             | MACADDR                             |
/// | `geo_types::Point<f64>`         | POINT                               |
/// | `geo_types::Rect<f64>`          | BOX                                 |
/// | `geo_types::LineString<f64>`    | PATH                                |
/// | `serde_json::Value`             | JSON, JSONB                         |
/// | `uuid::Uuid`                    | UUID                                |
/// | `bit_vec::BitVec`               | BIT, VARBIT                         |
/// | `eui48::MacAddress`             | MACADDR                             |
///
/// # Nullability
///
/// In addition to the types listed above, `ToSql` is implemented for
/// `Option<T>` where `T` implements `ToSql`. An `Option<T>` represents a
/// nullable Postgres value.
///
/// # Arrays
///
/// `ToSql` is implemented for `[u8; N]`, `Vec<T>`, `&[T]`, `Box<[T]>` and `[T; N]`
/// where `T` implements `ToSql` and `N` is const usize, and corresponds to one-dimensional
/// Postgres arrays with an index offset of 1.
///
/// **Note:** the impl for arrays only exist when the Cargo feature `array-impls`
/// is enabled.
pub trait ToSql: fmt::Debug {
    /// Converts the value of `self` into the binary format of the specified
    /// Postgres `Type`, appending it to `out`.
    ///
    /// The caller of this method is responsible for ensuring that this type
    /// is compatible with the Postgres `Type`.
    ///
    /// The return value indicates if this value should be represented as
    /// `NULL`. If this is the case, implementations **must not** write
    /// anything to `out`.
    fn to_sql(&self, ty: &Type, out: &mut BytesMut) -> Result<IsNull, Box<dyn Error + Sync + Send>>
    where
        Self: Sized;

    /// Determines if a value of this type can be converted to the specified
    /// Postgres `Type`.
    fn accepts(ty: &Type) -> bool
    where
        Self: Sized;

    /// An adaptor method used internally by Rust-Postgres.
    ///
    /// *All* implementations of this method should be generated by the
    /// `to_sql_checked!()` macro.
    fn to_sql_checked(
        &self,
        ty: &Type,
        out: &mut BytesMut,
    ) -> Result<IsNull, Box<dyn Error + Sync + Send>>;

    /// Specify the encode format
    fn encode_format(&self, _ty: &Type) -> Format {
        Format::Binary
    }
}

/// Supported Postgres message format types
///
/// Using Text format in a message assumes a Postgres `SERVER_ENCODING` of `UTF8`
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum Format {
    /// Text format (UTF-8)
    Text,
    /// Compact, typed binary format
    Binary,
}

impl<'a, T> ToSql for &'a T
where
    T: ToSql,
{
    fn to_sql(
        &self,
        ty: &Type,
        out: &mut BytesMut,
    ) -> Result<IsNull, Box<dyn Error + Sync + Send>> {
        (*self).to_sql(ty, out)
    }

    fn accepts(ty: &Type) -> bool {
        T::accepts(ty)
    }

    fn encode_format(&self, ty: &Type) -> Format {
        (*self).encode_format(ty)
    }

    to_sql_checked!();
}

impl<T: ToSql> ToSql for Option<T> {
    fn to_sql(
        &self,
        ty: &Type,
        out: &mut BytesMut,
    ) -> Result<IsNull, Box<dyn Error + Sync + Send>> {
        match *self {
            Some(ref val) => val.to_sql(ty, out),
            None => Ok(IsNull::Yes),
        }
    }

    fn accepts(ty: &Type) -> bool {
        <T as ToSql>::accepts(ty)
    }

    fn encode_format(&self, ty: &Type) -> Format {
        match self {
            Some(ref val) => val.encode_format(ty),
            None => Format::Binary,
        }
    }

    to_sql_checked!();
}

impl<'a, T: ToSql> ToSql for &'a [T] {
    fn to_sql(&self, ty: &Type, w: &mut BytesMut) -> Result<IsNull, Box<dyn Error + Sync + Send>> {
        let member_type = match *ty.kind() {
            Kind::Array(ref member) => member,
            _ => panic!("expected array type"),
        };

        let dimension = ArrayDimension {
            len: downcast(self.len())?,
            lower_bound: 1,
        };

        types::array_to_sql(
            Some(dimension),
            member_type.oid(),
            self.iter(),
            |e, w| match e.to_sql(member_type, w)? {
                IsNull::No => Ok(postgres_protocol2::IsNull::No),
                IsNull::Yes => Ok(postgres_protocol2::IsNull::Yes),
            },
            w,
        )?;
        Ok(IsNull::No)
    }

    fn accepts(ty: &Type) -> bool {
        match *ty.kind() {
            Kind::Array(ref member) => T::accepts(member),
            _ => false,
        }
    }

    to_sql_checked!();
}

impl<'a> ToSql for &'a [u8] {
    fn to_sql(&self, _: &Type, w: &mut BytesMut) -> Result<IsNull, Box<dyn Error + Sync + Send>> {
        types::bytea_to_sql(self, w);
        Ok(IsNull::No)
    }

    accepts!(BYTEA);

    to_sql_checked!();
}

impl<const N: usize> ToSql for [u8; N] {
    fn to_sql(&self, _: &Type, w: &mut BytesMut) -> Result<IsNull, Box<dyn Error + Sync + Send>> {
        types::bytea_to_sql(&self[..], w);
        Ok(IsNull::No)
    }

    accepts!(BYTEA);

    to_sql_checked!();
}

impl<T: ToSql, const N: usize> ToSql for [T; N] {
    fn to_sql(&self, ty: &Type, w: &mut BytesMut) -> Result<IsNull, Box<dyn Error + Sync + Send>> {
        <&[T] as ToSql>::to_sql(&&self[..], ty, w)
    }

    fn accepts(ty: &Type) -> bool {
        <&[T] as ToSql>::accepts(ty)
    }

    to_sql_checked!();
}

impl<T: ToSql> ToSql for Vec<T> {
    fn to_sql(&self, ty: &Type, w: &mut BytesMut) -> Result<IsNull, Box<dyn Error + Sync + Send>> {
        <&[T] as ToSql>::to_sql(&&**self, ty, w)
    }

    fn accepts(ty: &Type) -> bool {
        <&[T] as ToSql>::accepts(ty)
    }

    to_sql_checked!();
}

impl<T: ToSql> ToSql for Box<[T]> {
    fn to_sql(&self, ty: &Type, w: &mut BytesMut) -> Result<IsNull, Box<dyn Error + Sync + Send>> {
        <&[T] as ToSql>::to_sql(&&**self, ty, w)
    }

    fn accepts(ty: &Type) -> bool {
        <&[T] as ToSql>::accepts(ty)
    }

    to_sql_checked!();
}

impl ToSql for Vec<u8> {
    fn to_sql(&self, ty: &Type, w: &mut BytesMut) -> Result<IsNull, Box<dyn Error + Sync + Send>> {
        <&[u8] as ToSql>::to_sql(&&**self, ty, w)
    }

    fn accepts(ty: &Type) -> bool {
        <&[u8] as ToSql>::accepts(ty)
    }

    to_sql_checked!();
}

impl<'a> ToSql for &'a str {
    fn to_sql(&self, ty: &Type, w: &mut BytesMut) -> Result<IsNull, Box<dyn Error + Sync + Send>> {
        match *ty {
            ref ty if ty.name() == "ltree" => types::ltree_to_sql(self, w),
            ref ty if ty.name() == "lquery" => types::lquery_to_sql(self, w),
            ref ty if ty.name() == "ltxtquery" => types::ltxtquery_to_sql(self, w),
            _ => types::text_to_sql(self, w),
        }
        Ok(IsNull::No)
    }

    fn accepts(ty: &Type) -> bool {
        match *ty {
            Type::VARCHAR | Type::TEXT | Type::BPCHAR | Type::NAME | Type::UNKNOWN => true,
            ref ty
                if (ty.name() == "citext"
                    || ty.name() == "ltree"
                    || ty.name() == "lquery"
                    || ty.name() == "ltxtquery") =>
            {
                true
            }
            _ => false,
        }
    }

    to_sql_checked!();
}

impl<'a> ToSql for Cow<'a, str> {
    fn to_sql(&self, ty: &Type, w: &mut BytesMut) -> Result<IsNull, Box<dyn Error + Sync + Send>> {
        <&str as ToSql>::to_sql(&self.as_ref(), ty, w)
    }

    fn accepts(ty: &Type) -> bool {
        <&str as ToSql>::accepts(ty)
    }

    to_sql_checked!();
}

impl ToSql for String {
    fn to_sql(&self, ty: &Type, w: &mut BytesMut) -> Result<IsNull, Box<dyn Error + Sync + Send>> {
        <&str as ToSql>::to_sql(&&**self, ty, w)
    }

    fn accepts(ty: &Type) -> bool {
        <&str as ToSql>::accepts(ty)
    }

    to_sql_checked!();
}

impl ToSql for Box<str> {
    fn to_sql(&self, ty: &Type, w: &mut BytesMut) -> Result<IsNull, Box<dyn Error + Sync + Send>> {
        <&str as ToSql>::to_sql(&&**self, ty, w)
    }

    fn accepts(ty: &Type) -> bool {
        <&str as ToSql>::accepts(ty)
    }

    to_sql_checked!();
}

macro_rules! simple_to {
    ($t:ty, $f:ident, $($expected:ident),+) => {
        impl ToSql for $t {
            fn to_sql(&self,
                      _: &Type,
                      w: &mut BytesMut)
                      -> Result<IsNull, Box<dyn Error + Sync + Send>> {
                types::$f(*self, w);
                Ok(IsNull::No)
            }

            accepts!($($expected),+);

            to_sql_checked!();
        }
    }
}

simple_to!(bool, bool_to_sql, BOOL);
simple_to!(i8, char_to_sql, CHAR);
simple_to!(i16, int2_to_sql, INT2);
simple_to!(i32, int4_to_sql, INT4);
simple_to!(u32, oid_to_sql, OID);
simple_to!(i64, int8_to_sql, INT8);
simple_to!(f32, float4_to_sql, FLOAT4);
simple_to!(f64, float8_to_sql, FLOAT8);

impl<H> ToSql for HashMap<String, Option<String>, H>
where
    H: BuildHasher,
{
    fn to_sql(&self, _: &Type, w: &mut BytesMut) -> Result<IsNull, Box<dyn Error + Sync + Send>> {
        types::hstore_to_sql(
            self.iter().map(|(k, v)| (&**k, v.as_ref().map(|v| &**v))),
            w,
        )?;
        Ok(IsNull::No)
    }

    fn accepts(ty: &Type) -> bool {
        ty.name() == "hstore"
    }

    to_sql_checked!();
}

impl ToSql for SystemTime {
    fn to_sql(&self, _: &Type, w: &mut BytesMut) -> Result<IsNull, Box<dyn Error + Sync + Send>> {
        let epoch = UNIX_EPOCH + Duration::from_secs(TIME_SEC_CONVERSION);

        let to_usec =
            |d: Duration| d.as_secs() * USEC_PER_SEC + u64::from(d.subsec_nanos()) / NSEC_PER_USEC;

        let time = match self.duration_since(epoch) {
            Ok(duration) => to_usec(duration) as i64,
            Err(e) => -(to_usec(e.duration()) as i64),
        };

        types::timestamp_to_sql(time, w);
        Ok(IsNull::No)
    }

    accepts!(TIMESTAMP, TIMESTAMPTZ);

    to_sql_checked!();
}

impl ToSql for IpAddr {
    fn to_sql(&self, _: &Type, w: &mut BytesMut) -> Result<IsNull, Box<dyn Error + Sync + Send>> {
        let netmask = match self {
            IpAddr::V4(_) => 32,
            IpAddr::V6(_) => 128,
        };
        types::inet_to_sql(*self, netmask, w);
        Ok(IsNull::No)
    }

    accepts!(INET);

    to_sql_checked!();
}

fn downcast(len: usize) -> Result<i32, Box<dyn Error + Sync + Send>> {
    if len > i32::MAX as usize {
        Err("value too large to transmit".into())
    } else {
        Ok(len as i32)
    }
}

mod sealed {
    pub trait Sealed {}
}

/// A trait used by clients to abstract over `&dyn ToSql` and `T: ToSql`.
///
/// This cannot be implemented outside of this crate.
pub trait BorrowToSql: sealed::Sealed {
    /// Returns a reference to `self` as a `ToSql` trait object.
    fn borrow_to_sql(&self) -> &dyn ToSql;
}

impl sealed::Sealed for &dyn ToSql {}

impl BorrowToSql for &dyn ToSql {
    #[inline]
    fn borrow_to_sql(&self) -> &dyn ToSql {
        *self
    }
}

impl sealed::Sealed for Box<dyn ToSql + Sync> {}

impl BorrowToSql for Box<dyn ToSql + Sync> {
    #[inline]
    fn borrow_to_sql(&self) -> &dyn ToSql {
        self.as_ref()
    }
}

impl sealed::Sealed for Box<dyn ToSql + Sync + Send> {}
impl BorrowToSql for Box<dyn ToSql + Sync + Send> {
    #[inline]
    fn borrow_to_sql(&self) -> &dyn ToSql {
        self.as_ref()
    }
}

impl sealed::Sealed for &(dyn ToSql + Sync) {}

/// In async contexts it is sometimes necessary to have the additional
/// Sync requirement on parameters for queries since this enables the
/// resulting Futures to be Send, hence usable in, e.g., tokio::spawn.
/// This instance is provided for those cases.
impl BorrowToSql for &(dyn ToSql + Sync) {
    #[inline]
    fn borrow_to_sql(&self) -> &dyn ToSql {
        *self
    }
}

impl<T> sealed::Sealed for T where T: ToSql {}

impl<T> BorrowToSql for T
where
    T: ToSql,
{
    #[inline]
    fn borrow_to_sql(&self) -> &dyn ToSql {
        self
    }
}
