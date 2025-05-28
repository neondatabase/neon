use std::fmt;
use std::sync::Arc;

use crate::types::Type;
use postgres_protocol2::Oid;
use postgres_protocol2::message::backend::Field;

struct StatementInner {
    name: &'static str,
    columns: Vec<Column>,
}

/// A prepared statement.
///
/// Prepared statements can only be used with the connection that created them.
#[derive(Clone)]
pub struct Statement(Arc<StatementInner>);

impl Statement {
    pub(crate) fn new(name: &'static str, columns: Vec<Column>) -> Statement {
        Statement(Arc::new(StatementInner { name, columns }))
    }

    pub(crate) fn name(&self) -> &str {
        self.0.name
    }

    /// Returns information about the columns returned when the statement is queried.
    pub fn columns(&self) -> &[Column] {
        &self.0.columns
    }
}

/// Information about a column of a query.
pub struct Column {
    name: String,
    pub(crate) type_: Type,

    // raw fields from RowDescription
    table_oid: Oid,
    column_id: i16,
    format: i16,

    // that better be stored in self.type_, but that is more radical refactoring
    type_oid: Oid,
    type_size: i16,
    type_modifier: i32,
}

impl Column {
    pub(crate) fn new(name: String, type_: Type, raw_field: Field<'_>) -> Column {
        Column {
            name,
            type_,
            table_oid: raw_field.table_oid(),
            column_id: raw_field.column_id(),
            format: raw_field.format(),
            type_oid: raw_field.type_oid(),
            type_size: raw_field.type_size(),
            type_modifier: raw_field.type_modifier(),
        }
    }

    /// Returns the name of the column.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Returns the type of the column.
    pub fn type_(&self) -> &Type {
        &self.type_
    }

    /// Returns the table OID of the column.
    pub fn table_oid(&self) -> Oid {
        self.table_oid
    }

    /// Returns the column ID of the column.
    pub fn column_id(&self) -> i16 {
        self.column_id
    }

    /// Returns the format of the column.
    pub fn format(&self) -> i16 {
        self.format
    }

    /// Returns the type OID of the column.
    pub fn type_oid(&self) -> Oid {
        self.type_oid
    }

    /// Returns the type size of the column.
    pub fn type_size(&self) -> i16 {
        self.type_size
    }

    /// Returns the type modifier of the column.
    pub fn type_modifier(&self) -> i32 {
        self.type_modifier
    }
}

impl fmt::Debug for Column {
    fn fmt(&self, fmt: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt.debug_struct("Column")
            .field("name", &self.name)
            .field("type", &self.type_)
            .finish()
    }
}
