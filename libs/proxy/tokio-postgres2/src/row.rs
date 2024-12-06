//! Rows.
use crate::statement::Column;
use crate::types::{FromSql, Type, WrongType};
use crate::{Error, Statement};
use fallible_iterator::FallibleIterator;
use postgres_protocol2::message::backend::DataRowBody;
use postgres_types2::{Format, WrongFormat};
use std::fmt;
use std::ops::Range;
use std::str;

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
    pub fn get<'a, T>(&'a self, idx: usize) -> T
    where
        T: FromSql<'a>,
    {
        match self.get_inner(idx) {
            Ok(ok) => ok,
            Err(err) => panic!("error retrieving column {}: {}", idx, err),
        }
    }

    /// Like `Row::get`, but returns a `Result` rather than panicking.
    pub fn try_get<'a, T>(&'a self, idx: usize) -> Result<T, Error>
    where
        T: FromSql<'a>,
    {
        self.get_inner(idx)
    }

    fn get_inner<'a, T>(&'a self, idx: usize) -> Result<T, Error>
    where
        T: FromSql<'a>,
    {
        let Some(column) = self.columns().get(idx) else {
            return Err(Error::column(idx.to_string()));
        };

        let ty = column.type_();
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
