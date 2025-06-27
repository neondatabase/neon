//! Conversions to and from Postgres types.
//!
//! This crate is used by the `tokio-postgres` and `postgres` crates. You normally don't need to depend directly on it
//! unless you want to define your own `ToSql` or `FromSql` definitions.
#![warn(clippy::all, missing_docs)]

use std::any::type_name;
use std::error::Error;
use std::fmt;
use std::sync::Arc;

use fallible_iterator::FallibleIterator;
#[doc(inline)]
pub use postgres_protocol2::Oid;
use postgres_protocol2::types;

use crate::type_gen::{Inner, Other};

/// Generates a simple implementation of `ToSql::accepts` which accepts the
/// types passed to it.
macro_rules! accepts {
    ($($expected:ident),+) => (
        fn accepts(ty: &$crate::Type) -> bool {
            matches!(*ty, $($crate::Type::$expected)|+)
        }
    )
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
            schema => write!(fmt, "{schema}.")?,
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
    /// An enumerated type.
    Enum,
    /// A pseudo-type.
    Pseudo,
    /// An array type along with the type of its elements.
    Array(Type),
    /// A range type along with the type of its elements.
    Range(Oid),
    /// A multirange type along with the type of its elements.
    Multirange(Type),
    /// A domain type along with its underlying type.
    Domain(Oid),
    /// A composite type.
    Composite(Oid),
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

impl<'a> FromSql<'a> for String {
    fn from_sql(ty: &Type, raw: &'a [u8]) -> Result<String, Box<dyn Error + Sync + Send>> {
        <&str as FromSql>::from_sql(ty, raw).map(ToString::to_string)
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

simple_from!(i8, char_from_sql, CHAR);
simple_from!(u32, oid_from_sql, OID);

/// An enum representing the nullability of a Postgres value.
pub enum IsNull {
    /// The value is NULL.
    Yes,
    /// The value is not NULL.
    No,
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
