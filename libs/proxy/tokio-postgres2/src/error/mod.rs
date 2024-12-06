//! Errors.

use fallible_iterator::FallibleIterator;
use postgres_protocol2::message::backend::{ErrorFields, ErrorResponseBody};
use std::error::{self, Error as _Error};
use std::fmt;
use std::io;

pub use self::sqlstate::*;

#[allow(clippy::unreadable_literal)]
mod sqlstate;

/// The severity of a Postgres error or notice.
#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum Severity {
    /// PANIC
    Panic,
    /// FATAL
    Fatal,
    /// ERROR
    Error,
    /// WARNING
    Warning,
    /// NOTICE
    Notice,
    /// DEBUG
    Debug,
    /// INFO
    Info,
    /// LOG
    Log,
}

impl fmt::Display for Severity {
    fn fmt(&self, fmt: &mut fmt::Formatter<'_>) -> fmt::Result {
        let s = match *self {
            Severity::Panic => "PANIC",
            Severity::Fatal => "FATAL",
            Severity::Error => "ERROR",
            Severity::Warning => "WARNING",
            Severity::Notice => "NOTICE",
            Severity::Debug => "DEBUG",
            Severity::Info => "INFO",
            Severity::Log => "LOG",
        };
        fmt.write_str(s)
    }
}

impl Severity {
    fn from_str(s: &str) -> Option<Severity> {
        match s {
            "PANIC" => Some(Severity::Panic),
            "FATAL" => Some(Severity::Fatal),
            "ERROR" => Some(Severity::Error),
            "WARNING" => Some(Severity::Warning),
            "NOTICE" => Some(Severity::Notice),
            "DEBUG" => Some(Severity::Debug),
            "INFO" => Some(Severity::Info),
            "LOG" => Some(Severity::Log),
            _ => None,
        }
    }
}

/// A Postgres error or notice.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DbError {
    severity: String,
    parsed_severity: Option<Severity>,
    code: SqlState,
    message: String,
    detail: Option<String>,
    hint: Option<String>,
    position: Option<ErrorPosition>,
    where_: Option<String>,
    schema: Option<String>,
    table: Option<String>,
    column: Option<String>,
    datatype: Option<String>,
    constraint: Option<String>,
    file: Option<String>,
    line: Option<u32>,
    routine: Option<String>,
}

impl DbError {
    pub(crate) fn parse(fields: &mut ErrorFields<'_>) -> io::Result<DbError> {
        let mut severity = None;
        let mut parsed_severity = None;
        let mut code = None;
        let mut message = None;
        let mut detail = None;
        let mut hint = None;
        let mut normal_position = None;
        let mut internal_position = None;
        let mut internal_query = None;
        let mut where_ = None;
        let mut schema = None;
        let mut table = None;
        let mut column = None;
        let mut datatype = None;
        let mut constraint = None;
        let mut file = None;
        let mut line = None;
        let mut routine = None;

        while let Some(field) = fields.next()? {
            match field.type_() {
                b'S' => severity = Some(field.value().to_owned()),
                b'C' => code = Some(SqlState::from_code(field.value())),
                b'M' => message = Some(field.value().to_owned()),
                b'D' => detail = Some(field.value().to_owned()),
                b'H' => hint = Some(field.value().to_owned()),
                b'P' => {
                    normal_position = Some(field.value().parse::<u32>().map_err(|_| {
                        io::Error::new(
                            io::ErrorKind::InvalidInput,
                            "`P` field did not contain an integer",
                        )
                    })?);
                }
                b'p' => {
                    internal_position = Some(field.value().parse::<u32>().map_err(|_| {
                        io::Error::new(
                            io::ErrorKind::InvalidInput,
                            "`p` field did not contain an integer",
                        )
                    })?);
                }
                b'q' => internal_query = Some(field.value().to_owned()),
                b'W' => where_ = Some(field.value().to_owned()),
                b's' => schema = Some(field.value().to_owned()),
                b't' => table = Some(field.value().to_owned()),
                b'c' => column = Some(field.value().to_owned()),
                b'd' => datatype = Some(field.value().to_owned()),
                b'n' => constraint = Some(field.value().to_owned()),
                b'F' => file = Some(field.value().to_owned()),
                b'L' => {
                    line = Some(field.value().parse::<u32>().map_err(|_| {
                        io::Error::new(
                            io::ErrorKind::InvalidInput,
                            "`L` field did not contain an integer",
                        )
                    })?);
                }
                b'R' => routine = Some(field.value().to_owned()),
                b'V' => {
                    parsed_severity = Some(Severity::from_str(field.value()).ok_or_else(|| {
                        io::Error::new(
                            io::ErrorKind::InvalidInput,
                            "`V` field contained an invalid value",
                        )
                    })?);
                }
                _ => {}
            }
        }

        Ok(DbError {
            severity: severity
                .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "`S` field missing"))?,
            parsed_severity,
            code: code
                .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "`C` field missing"))?,
            message: message
                .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "`M` field missing"))?,
            detail,
            hint,
            position: match normal_position {
                Some(position) => Some(ErrorPosition::Original(position)),
                None => match internal_position {
                    Some(position) => Some(ErrorPosition::Internal {
                        position,
                        query: internal_query.ok_or_else(|| {
                            io::Error::new(
                                io::ErrorKind::InvalidInput,
                                "`q` field missing but `p` field present",
                            )
                        })?,
                    }),
                    None => None,
                },
            },
            where_,
            schema,
            table,
            column,
            datatype,
            constraint,
            file,
            line,
            routine,
        })
    }

    /// The field contents are ERROR, FATAL, or PANIC (in an error message),
    /// or WARNING, NOTICE, DEBUG, INFO, or LOG (in a notice message), or a
    /// localized translation of one of these.
    pub fn severity(&self) -> &str {
        &self.severity
    }

    /// A parsed, nonlocalized version of `severity`. (PostgreSQL 9.6+)
    pub fn parsed_severity(&self) -> Option<Severity> {
        self.parsed_severity
    }

    /// The SQLSTATE code for the error.
    pub fn code(&self) -> &SqlState {
        &self.code
    }

    /// The primary human-readable error message.
    ///
    /// This should be accurate but terse (typically one line).
    pub fn message(&self) -> &str {
        &self.message
    }

    /// An optional secondary error message carrying more detail about the
    /// problem.
    ///
    /// Might run to multiple lines.
    pub fn detail(&self) -> Option<&str> {
        self.detail.as_deref()
    }

    /// An optional suggestion what to do about the problem.
    ///
    /// This is intended to differ from `detail` in that it offers advice
    /// (potentially inappropriate) rather than hard facts. Might run to
    /// multiple lines.
    pub fn hint(&self) -> Option<&str> {
        self.hint.as_deref()
    }

    /// An optional error cursor position into either the original query string
    /// or an internally generated query.
    pub fn position(&self) -> Option<&ErrorPosition> {
        self.position.as_ref()
    }

    /// An indication of the context in which the error occurred.
    ///
    /// Presently this includes a call stack traceback of active procedural
    /// language functions and internally-generated queries. The trace is one
    /// entry per line, most recent first.
    pub fn where_(&self) -> Option<&str> {
        self.where_.as_deref()
    }

    /// If the error was associated with a specific database object, the name
    /// of the schema containing that object, if any. (PostgreSQL 9.3+)
    pub fn schema(&self) -> Option<&str> {
        self.schema.as_deref()
    }

    /// If the error was associated with a specific table, the name of the
    /// table. (Refer to the schema name field for the name of the table's
    /// schema.) (PostgreSQL 9.3+)
    pub fn table(&self) -> Option<&str> {
        self.table.as_deref()
    }

    /// If the error was associated with a specific table column, the name of
    /// the column.
    ///
    /// (Refer to the schema and table name fields to identify the table.)
    /// (PostgreSQL 9.3+)
    pub fn column(&self) -> Option<&str> {
        self.column.as_deref()
    }

    /// If the error was associated with a specific data type, the name of the
    /// data type. (Refer to the schema name field for the name of the data
    /// type's schema.) (PostgreSQL 9.3+)
    pub fn datatype(&self) -> Option<&str> {
        self.datatype.as_deref()
    }

    /// If the error was associated with a specific constraint, the name of the
    /// constraint.
    ///
    /// Refer to fields listed above for the associated table or domain.
    /// (For this purpose, indexes are treated as constraints, even if they
    /// weren't created with constraint syntax.) (PostgreSQL 9.3+)
    pub fn constraint(&self) -> Option<&str> {
        self.constraint.as_deref()
    }

    /// The file name of the source-code location where the error was reported.
    pub fn file(&self) -> Option<&str> {
        self.file.as_deref()
    }

    /// The line number of the source-code location where the error was
    /// reported.
    pub fn line(&self) -> Option<u32> {
        self.line
    }

    /// The name of the source-code routine reporting the error.
    pub fn routine(&self) -> Option<&str> {
        self.routine.as_deref()
    }
}

impl fmt::Display for DbError {
    fn fmt(&self, fmt: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(fmt, "{}: {}", self.severity, self.message)?;
        if let Some(detail) = &self.detail {
            write!(fmt, "\nDETAIL: {}", detail)?;
        }
        if let Some(hint) = &self.hint {
            write!(fmt, "\nHINT: {}", hint)?;
        }
        Ok(())
    }
}

impl error::Error for DbError {}

/// Represents the position of an error in a query.
#[derive(Clone, PartialEq, Eq, Debug)]
pub enum ErrorPosition {
    /// A position in the original query.
    Original(u32),
    /// A position in an internally generated query.
    Internal {
        /// The byte position.
        position: u32,
        /// A query generated by the Postgres server.
        query: String,
    },
}

#[derive(Debug, PartialEq)]
enum Kind {
    Io,
    UnexpectedMessage,
    Tls,
    ToSql(usize),
    FromSql(usize),
    Column(String),
    Closed,
    Db,
    Parse,
    Encode,
    Authentication,
    Config,
    Connect,
    Timeout,
}

struct ErrorInner {
    kind: Kind,
    cause: Option<Box<dyn error::Error + Sync + Send>>,
}

/// An error communicating with the Postgres server.
pub struct Error(Box<ErrorInner>);

impl fmt::Debug for Error {
    fn fmt(&self, fmt: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt.debug_struct("Error")
            .field("kind", &self.0.kind)
            .field("cause", &self.0.cause)
            .finish()
    }
}

impl fmt::Display for Error {
    fn fmt(&self, fmt: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.0.kind {
            Kind::Io => fmt.write_str("error communicating with the server")?,
            Kind::UnexpectedMessage => fmt.write_str("unexpected message from server")?,
            Kind::Tls => fmt.write_str("error performing TLS handshake")?,
            Kind::ToSql(idx) => write!(fmt, "error serializing parameter {}", idx)?,
            Kind::FromSql(idx) => write!(fmt, "error deserializing column {}", idx)?,
            Kind::Column(column) => write!(fmt, "invalid column `{}`", column)?,
            Kind::Closed => fmt.write_str("connection closed")?,
            Kind::Db => fmt.write_str("db error")?,
            Kind::Parse => fmt.write_str("error parsing response from server")?,
            Kind::Encode => fmt.write_str("error encoding message to server")?,
            Kind::Authentication => fmt.write_str("authentication error")?,
            Kind::Config => fmt.write_str("invalid configuration")?,
            Kind::Connect => fmt.write_str("error connecting to server")?,
            Kind::Timeout => fmt.write_str("timeout waiting for server")?,
        };
        if let Some(ref cause) = self.0.cause {
            write!(fmt, ": {}", cause)?;
        }
        Ok(())
    }
}

impl error::Error for Error {
    fn source(&self) -> Option<&(dyn error::Error + 'static)> {
        self.0.cause.as_ref().map(|e| &**e as _)
    }
}

impl Error {
    /// Consumes the error, returning its cause.
    pub fn into_source(self) -> Option<Box<dyn error::Error + Sync + Send>> {
        self.0.cause
    }

    /// Returns the source of this error if it was a `DbError`.
    ///
    /// This is a simple convenience method.
    pub fn as_db_error(&self) -> Option<&DbError> {
        self.source().and_then(|e| e.downcast_ref::<DbError>())
    }

    /// Determines if the error was associated with closed connection.
    pub fn is_closed(&self) -> bool {
        self.0.kind == Kind::Closed
    }

    /// Returns the SQLSTATE error code associated with the error.
    ///
    /// This is a convenience method that downcasts the cause to a `DbError` and returns its code.
    pub fn code(&self) -> Option<&SqlState> {
        self.as_db_error().map(DbError::code)
    }

    fn new(kind: Kind, cause: Option<Box<dyn error::Error + Sync + Send>>) -> Error {
        Error(Box::new(ErrorInner { kind, cause }))
    }

    pub(crate) fn closed() -> Error {
        Error::new(Kind::Closed, None)
    }

    pub(crate) fn unexpected_message() -> Error {
        Error::new(Kind::UnexpectedMessage, None)
    }

    #[allow(clippy::needless_pass_by_value)]
    pub(crate) fn db(error: ErrorResponseBody) -> Error {
        match DbError::parse(&mut error.fields()) {
            Ok(e) => Error::new(Kind::Db, Some(Box::new(e))),
            Err(e) => Error::new(Kind::Parse, Some(Box::new(e))),
        }
    }

    pub(crate) fn parse(e: io::Error) -> Error {
        Error::new(Kind::Parse, Some(Box::new(e)))
    }

    pub(crate) fn encode(e: io::Error) -> Error {
        Error::new(Kind::Encode, Some(Box::new(e)))
    }

    #[allow(clippy::wrong_self_convention)]
    pub(crate) fn to_sql(e: Box<dyn error::Error + Sync + Send>, idx: usize) -> Error {
        Error::new(Kind::ToSql(idx), Some(e))
    }

    pub(crate) fn from_sql(e: Box<dyn error::Error + Sync + Send>, idx: usize) -> Error {
        Error::new(Kind::FromSql(idx), Some(e))
    }

    pub(crate) fn column(column: String) -> Error {
        Error::new(Kind::Column(column), None)
    }

    pub(crate) fn tls(e: Box<dyn error::Error + Sync + Send>) -> Error {
        Error::new(Kind::Tls, Some(e))
    }

    pub(crate) fn io(e: io::Error) -> Error {
        Error::new(Kind::Io, Some(Box::new(e)))
    }

    pub(crate) fn authentication(e: Box<dyn error::Error + Sync + Send>) -> Error {
        Error::new(Kind::Authentication, Some(e))
    }

    pub(crate) fn config(e: Box<dyn error::Error + Sync + Send>) -> Error {
        Error::new(Kind::Config, Some(e))
    }

    pub(crate) fn connect(e: io::Error) -> Error {
        Error::new(Kind::Connect, Some(Box::new(e)))
    }

    #[doc(hidden)]
    pub fn __private_api_timeout() -> Error {
        Error::new(Kind::Timeout, None)
    }
}
