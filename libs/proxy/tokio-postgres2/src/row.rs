//! Rows.

use crate::row::sealed::{AsName, Sealed};
use crate::simple_query::SimpleColumn;
use crate::statement::Column;
use crate::types::{FromSql, Type, WrongType};
use crate::{Error, Statement};
use fallible_iterator::FallibleIterator;
use postgres_protocol2::message::backend::DataRowBody;
use postgres_types2::{Format, WrongFormat};
use std::fmt;
use std::ops::Range;
use std::str;
use std::sync::Arc;

mod sealed {
    pub trait Sealed {}

    pub trait AsName {
        fn as_name(&self) -> &str;
    }
}

impl AsName for Column {
    fn as_name(&self) -> &str {
        self.name()
    }
}

impl AsName for String {
    fn as_name(&self) -> &str {
        self
    }
}

/// A trait implemented by types that can index into columns of a row.
///
/// This cannot be implemented outside of this crate.
pub trait RowIndex: Sealed {
    #[doc(hidden)]
    fn __idx<T>(&self, columns: &[T]) -> Option<usize>
    where
        T: AsName;
}

impl Sealed for usize {}

impl RowIndex for usize {
    #[inline]
    fn __idx<T>(&self, columns: &[T]) -> Option<usize>
    where
        T: AsName,
    {
        if *self >= columns.len() {
            None
        } else {
            Some(*self)
        }
    }
}

impl Sealed for str {}

impl RowIndex for str {
    #[inline]
    fn __idx<T>(&self, columns: &[T]) -> Option<usize>
    where
        T: AsName,
    {
        if let Some(idx) = columns.iter().position(|d| d.as_name() == self) {
            return Some(idx);
        };

        // FIXME ASCII-only case insensitivity isn't really the right thing to
        // do. Postgres itself uses a dubious wrapper around tolower and JDBC
        // uses the US locale.
        columns
            .iter()
            .position(|d| d.as_name().eq_ignore_ascii_case(self))
    }
}

impl<T> Sealed for &T where T: ?Sized + Sealed {}

impl<T> RowIndex for &T
where
    T: ?Sized + RowIndex,
{
    #[inline]
    fn __idx<U>(&self, columns: &[U]) -> Option<usize>
    where
        U: AsName,
    {
        T::__idx(*self, columns)
    }
}

/// A row of data returned from the database by a query.
pub struct Row {
    statement: Statement,
    output_format: Format,
    body: DataRowBody,
    ranges: Vec<Option<Range<usize>>>,
}

impl fmt::Debug for Row {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Row")
            .field("columns", &self.columns())
            .finish()
    }
}

impl Row {
    pub(crate) fn new(
        statement: Statement,
        body: DataRowBody,
        output_format: Format,
    ) -> Result<Row, Error> {
        let ranges = body.ranges().collect().map_err(Error::parse)?;
        Ok(Row {
            statement,
            body,
            ranges,
            output_format,
        })
    }

    /// Returns information about the columns of data in the row.
    pub fn columns(&self) -> &[Column] {
        self.statement.columns()
    }

    /// Determines if the row contains no values.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Returns the number of values in the row.
    pub fn len(&self) -> usize {
        self.columns().len()
    }

    /// Deserializes a value from the row.
    ///
    /// The value can be specified either by its numeric index in the row, or by its column name.
    ///
    /// # Panics
    ///
    /// Panics if the index is out of bounds or if the value cannot be converted to the specified type.
    pub fn get<'a, I, T>(&'a self, idx: I) -> T
    where
        I: RowIndex + fmt::Display,
        T: FromSql<'a>,
    {
        match self.get_inner(&idx) {
            Ok(ok) => ok,
            Err(err) => panic!("error retrieving column {}: {}", idx, err),
        }
    }

    /// Like `Row::get`, but returns a `Result` rather than panicking.
    pub fn try_get<'a, I, T>(&'a self, idx: I) -> Result<T, Error>
    where
        I: RowIndex + fmt::Display,
        T: FromSql<'a>,
    {
        self.get_inner(&idx)
    }

    fn get_inner<'a, I, T>(&'a self, idx: &I) -> Result<T, Error>
    where
        I: RowIndex + fmt::Display,
        T: FromSql<'a>,
    {
        let idx = match idx.__idx(self.columns()) {
            Some(idx) => idx,
            None => return Err(Error::column(idx.to_string())),
        };

        let ty = self.columns()[idx].type_();
        if !T::accepts(ty) {
            return Err(Error::from_sql(
                Box::new(WrongType::new::<T>(ty.clone())),
                idx,
            ));
        }

        FromSql::from_sql_nullable(ty, self.col_buffer(idx)).map_err(|e| Error::from_sql(e, idx))
    }

    /// Get the raw bytes for the column at the given index.
    fn col_buffer(&self, idx: usize) -> Option<&[u8]> {
        let range = self.ranges.get(idx)?.to_owned()?;
        Some(&self.body.buffer()[range])
    }

    /// Interpret the column at the given index as text
    ///
    /// Useful when using query_raw_txt() which sets text transfer mode
    pub fn as_text(&self, idx: usize) -> Result<Option<&str>, Error> {
        if self.output_format == Format::Text {
            match self.col_buffer(idx) {
                Some(raw) => {
                    FromSql::from_sql(&Type::TEXT, raw).map_err(|e| Error::from_sql(e, idx))
                }
                None => Ok(None),
            }
        } else {
            Err(Error::from_sql(Box::new(WrongFormat {}), idx))
        }
    }

    /// Row byte size
    pub fn body_len(&self) -> usize {
        self.body.buffer().len()
    }
}

impl AsName for SimpleColumn {
    fn as_name(&self) -> &str {
        self.name()
    }
}

/// A row of data returned from the database by a simple query.
#[derive(Debug)]
pub struct SimpleQueryRow {
    columns: Arc<[SimpleColumn]>,
    body: DataRowBody,
    ranges: Vec<Option<Range<usize>>>,
}

impl SimpleQueryRow {
    #[allow(clippy::new_ret_no_self)]
    pub(crate) fn new(
        columns: Arc<[SimpleColumn]>,
        body: DataRowBody,
    ) -> Result<SimpleQueryRow, Error> {
        let ranges = body.ranges().collect().map_err(Error::parse)?;
        Ok(SimpleQueryRow {
            columns,
            body,
            ranges,
        })
    }

    /// Returns information about the columns of data in the row.
    pub fn columns(&self) -> &[SimpleColumn] {
        &self.columns
    }

    /// Determines if the row contains no values.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Returns the number of values in the row.
    pub fn len(&self) -> usize {
        self.columns.len()
    }

    /// Returns a value from the row.
    ///
    /// The value can be specified either by its numeric index in the row, or by its column name.
    ///
    /// # Panics
    ///
    /// Panics if the index is out of bounds or if the value cannot be converted to the specified type.
    pub fn get<I>(&self, idx: I) -> Option<&str>
    where
        I: RowIndex + fmt::Display,
    {
        match self.get_inner(&idx) {
            Ok(ok) => ok,
            Err(err) => panic!("error retrieving column {}: {}", idx, err),
        }
    }

    /// Like `SimpleQueryRow::get`, but returns a `Result` rather than panicking.
    pub fn try_get<I>(&self, idx: I) -> Result<Option<&str>, Error>
    where
        I: RowIndex + fmt::Display,
    {
        self.get_inner(&idx)
    }

    fn get_inner<I>(&self, idx: &I) -> Result<Option<&str>, Error>
    where
        I: RowIndex + fmt::Display,
    {
        let idx = match idx.__idx(&self.columns) {
            Some(idx) => idx,
            None => return Err(Error::column(idx.to_string())),
        };

        let buf = self.ranges[idx].clone().map(|r| &self.body.buffer()[r]);
        FromSql::from_sql_nullable(&Type::TEXT, buf).map_err(|e| Error::from_sql(e, idx))
    }
}
