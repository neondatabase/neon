use fallible_iterator::FallibleIterator;
use futures::StreamExt;
use postgres_protocol::message::backend::{DataRowRanges, Message};
use postgres_protocol::message::frontend;
use std::future::Future;
use std::pin::Pin;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio_postgres::types::{Field, Kind, Oid, ToSql, Type};

use super::connection::Connection;
use super::error::Error;

const TYPEINFO_QUERY: &str = "\
SELECT t.typname, t.typtype, t.typelem, r.rngsubtype, t.typbasetype, n.nspname, t.typrelid
FROM pg_catalog.pg_type t
LEFT OUTER JOIN pg_catalog.pg_range r ON r.rngtypid = t.oid
INNER JOIN pg_catalog.pg_namespace n ON t.typnamespace = n.oid
WHERE t.oid = $1
";

const TYPEINFO_ENUM_QUERY: &str = "\
SELECT enumlabel
FROM pg_catalog.pg_enum
WHERE enumtypid = $1
ORDER BY enumsortorder
";

const TYPEINFO_COMPOSITE_QUERY: &str = "\
SELECT attname, atttypid
FROM pg_catalog.pg_attribute
WHERE attrelid = $1
AND NOT attisdropped
AND attnum > 0
ORDER BY attnum
";

#[derive(Clone)]
pub struct TypeinfoPreparedQueries {
    query: String,
    enum_query: String,
    composite_query: String,
}

fn map_is_null(x: tokio_postgres::types::IsNull) -> postgres_protocol::IsNull {
    match x {
        tokio_postgres::types::IsNull::Yes => postgres_protocol::IsNull::Yes,
        tokio_postgres::types::IsNull::No => postgres_protocol::IsNull::No,
    }
}

fn read_column<'a, T: tokio_postgres::types::FromSql<'a>>(
    buffer: &'a [u8],
    type_: &Type,
    ranges: &mut DataRowRanges<'a>,
) -> Result<T, Error> {
    let range = ranges.next()?;
    match range {
        Some(range) => T::from_sql_nullable(type_, range.map(|r| &buffer[r])),
        None => T::from_sql_null(type_),
    }
    .map_err(|e| Error::from_sql(e, 0))
}

impl TypeinfoPreparedQueries {
    pub async fn new<
        S: AsyncRead + AsyncWrite + Unpin + Send,
        T: AsyncRead + AsyncWrite + Unpin + Send,
    >(
        c: &mut Connection<S, T>,
    ) -> Result<Self, Error> {
        if let Some(ti) = &c.typeinfo {
            return Ok(ti.clone());
        }

        let query = c.statement_name();
        let enum_query = c.statement_name();
        let composite_query = c.statement_name();

        frontend::parse(&query, TYPEINFO_QUERY, [Type::OID.oid()], &mut c.raw.buf)?;
        frontend::parse(
            &enum_query,
            TYPEINFO_ENUM_QUERY,
            [Type::OID.oid()],
            &mut c.raw.buf,
        )?;
        c.sync().await?;
        frontend::parse(
            &composite_query,
            TYPEINFO_COMPOSITE_QUERY,
            [Type::OID.oid()],
            &mut c.raw.buf,
        )?;
        c.sync().await?;

        let Message::ParseComplete = c.raw.next_message().await? else { return Err(Error::expecting("parse")) };
        let Message::ParseComplete = c.raw.next_message().await? else { return Err(Error::expecting("parse")) };
        let Message::ParseComplete = c.raw.next_message().await? else { return Err(Error::expecting("parse")) };
        c.wait_for_ready().await?;

        Ok(c.typeinfo
            .insert(TypeinfoPreparedQueries {
                query,
                enum_query,
                composite_query,
            })
            .clone())
    }

    fn get_type_rec<
        S: AsyncRead + AsyncWrite + Unpin + Send,
        T: AsyncRead + AsyncWrite + Unpin + Send,
    >(
        c: &mut Connection<S, T>,
        oid: Oid,
    ) -> Pin<Box<dyn Future<Output = Result<Type, Error>> + Send + '_>> {
        Box::pin(Self::get_type(c, oid))
    }

    pub async fn get_type<
        S: AsyncRead + AsyncWrite + Unpin + Send,
        T: AsyncRead + AsyncWrite + Unpin + Send,
    >(
        c: &mut Connection<S, T>,
        oid: Oid,
    ) -> Result<Type, Error> {
        if let Some(type_) = Type::from_oid(oid) {
            return Ok(type_);
        }

        if let Some(type_) = c.typecache.get(&oid) {
            return Ok(type_.clone());
        }

        let queries = Self::new(c).await?;

        frontend::bind(
            "",
            &queries.query,
            [1], // the only parameter is in binary format
            [oid],
            |param, buf| param.to_sql(&Type::OID, buf).map(map_is_null),
            Some(1), // binary return type
            &mut c.raw.buf,
        )
        .map_err(|e| match e {
            frontend::BindError::Conversion(e) => std::io::Error::new(std::io::ErrorKind::Other, e),
            frontend::BindError::Serialization(io) => io,
        })?;
        frontend::execute("", 0, &mut c.raw.buf)?;

        c.sync().await?;

        let mut stream = c.stream_query_results().await?;

        let Some(row) = stream.next().await.transpose()? else {
            todo!()
        };

        let row = row.map_err(Error::db)?;
        let b = row.buffer();
        let mut ranges = row.ranges();

        let name: String = read_column(b, &Type::NAME, &mut ranges)?;
        let type_: i8 = read_column(b, &Type::CHAR, &mut ranges)?;
        let elem_oid: Oid = read_column(b, &Type::OID, &mut ranges)?;
        let rngsubtype: Option<Oid> = read_column(b, &Type::OID, &mut ranges)?;
        let basetype: Oid = read_column(b, &Type::OID, &mut ranges)?;
        let schema: String = read_column(b, &Type::NAME, &mut ranges)?;
        let relid: Oid = read_column(b, &Type::OID, &mut ranges)?;

        {
            // should be none
            let None = stream.next().await.transpose()? else {
                todo!()
            };
            drop(stream);
        }

        let kind = if type_ == b'e' as i8 {
            let variants = Self::get_enum_variants(c, oid).await?;
            Kind::Enum(variants)
        } else if type_ == b'p' as i8 {
            Kind::Pseudo
        } else if basetype != 0 {
            let type_ = Self::get_type_rec(c, basetype).await?;
            Kind::Domain(type_)
        } else if elem_oid != 0 {
            let type_ = Self::get_type_rec(c, elem_oid).await?;
            Kind::Array(type_)
        } else if relid != 0 {
            let fields = Self::get_composite_fields(c, relid).await?;
            Kind::Composite(fields)
        } else if let Some(rngsubtype) = rngsubtype {
            let type_ = Self::get_type_rec(c, rngsubtype).await?;
            Kind::Range(type_)
        } else {
            Kind::Simple
        };

        let type_ = Type::new(name, oid, kind, schema);
        c.typecache.insert(oid, type_.clone());

        Ok(type_)
    }

    async fn get_enum_variants<
        S: AsyncRead + AsyncWrite + Unpin + Send,
        T: AsyncRead + AsyncWrite + Unpin + Send,
    >(
        c: &mut Connection<S, T>,
        oid: Oid,
    ) -> Result<Vec<String>, Error> {
        let queries = Self::new(c).await?;

        frontend::bind(
            "",
            &queries.enum_query,
            [1], // the only parameter is in binary format
            [oid],
            |param, buf| param.to_sql(&Type::OID, buf).map(map_is_null),
            Some(1), // binary return type
            &mut c.raw.buf,
        )
        .map_err(|e| match e {
            frontend::BindError::Conversion(e) => std::io::Error::new(std::io::ErrorKind::Other, e),
            frontend::BindError::Serialization(io) => io,
        })?;
        frontend::execute("", 0, &mut c.raw.buf)?;

        c.sync().await?;

        let mut stream = c.stream_query_results().await?;
        let mut variants = Vec::new();
        while let Some(row) = stream.next().await.transpose()? {
            let row = row.map_err(Error::db)?;

            let variant: String = read_column(row.buffer(), &Type::NAME, &mut row.ranges())?;
            variants.push(variant);
        }

        c.wait_for_ready().await?;

        Ok(variants)
    }

    async fn get_composite_fields<
        S: AsyncRead + AsyncWrite + Unpin + Send,
        T: AsyncRead + AsyncWrite + Unpin + Send,
    >(
        c: &mut Connection<S, T>,
        oid: Oid,
    ) -> Result<Vec<Field>, Error> {
        let queries = Self::new(c).await?;

        frontend::bind(
            "",
            &queries.composite_query,
            [1], // the only parameter is in binary format
            [oid],
            |param, buf| param.to_sql(&Type::OID, buf).map(map_is_null),
            Some(1), // binary return type
            &mut c.raw.buf,
        )
        .map_err(|e| match e {
            frontend::BindError::Conversion(e) => std::io::Error::new(std::io::ErrorKind::Other, e),
            frontend::BindError::Serialization(io) => io,
        })?;
        frontend::execute("", 0, &mut c.raw.buf)?;

        c.sync().await?;

        let mut stream = c.stream_query_results().await?;
        let mut fields = Vec::new();
        while let Some(row) = stream.next().await.transpose()? {
            let row = row.map_err(Error::db)?;

            let mut ranges = row.ranges();
            let name: String = read_column(row.buffer(), &Type::NAME, &mut ranges)?;
            let oid: Oid = read_column(row.buffer(), &Type::OID, &mut ranges)?;
            fields.push((name, oid));
        }

        c.wait_for_ready().await?;

        let mut output_fields = Vec::with_capacity(fields.len());
        for (name, oid) in fields {
            let type_ = Self::get_type_rec(c, oid).await?;
            output_fields.push(Field::new(name, type_))
        }

        Ok(output_fields)
    }
}
