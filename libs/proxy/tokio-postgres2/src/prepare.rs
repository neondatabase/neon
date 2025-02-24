use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use bytes::Bytes;
use fallible_iterator::FallibleIterator;
use futures_util::{TryStreamExt, pin_mut};
use log::debug;
use postgres_protocol2::message::backend::Message;
use postgres_protocol2::message::frontend;

use crate::client::InnerClient;
use crate::codec::FrontendMessage;
use crate::connection::RequestMessages;
use crate::types::{Field, Kind, Oid, Type};
use crate::{Column, Error, Statement, query, slice_iter};

pub(crate) const TYPEINFO_QUERY: &str = "\
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

pub(crate) const TYPEINFO_COMPOSITE_QUERY: &str = "\
SELECT attname, atttypid
FROM pg_catalog.pg_attribute
WHERE attrelid = $1
AND NOT attisdropped
AND attnum > 0
ORDER BY attnum
";

pub async fn prepare(
    client: &Arc<InnerClient>,
    name: &'static str,
    query: &str,
    types: &[Type],
) -> Result<Statement, Error> {
    let buf = encode(client, name, query, types)?;
    let mut responses = client.send(RequestMessages::Single(FrontendMessage::Raw(buf)))?;

    match responses.next().await? {
        Message::ParseComplete => {}
        _ => return Err(Error::unexpected_message()),
    }

    let parameter_description = match responses.next().await? {
        Message::ParameterDescription(body) => body,
        _ => return Err(Error::unexpected_message()),
    };

    let row_description = match responses.next().await? {
        Message::RowDescription(body) => Some(body),
        Message::NoData => None,
        _ => return Err(Error::unexpected_message()),
    };

    let mut parameters = vec![];
    let mut it = parameter_description.parameters();
    while let Some(oid) = it.next().map_err(Error::parse)? {
        let type_ = get_type(client, oid).await?;
        parameters.push(type_);
    }

    let mut columns = vec![];
    if let Some(row_description) = row_description {
        let mut it = row_description.fields();
        while let Some(field) = it.next().map_err(Error::parse)? {
            let type_ = get_type(client, field.type_oid()).await?;
            let column = Column::new(field.name().to_string(), type_, field);
            columns.push(column);
        }
    }

    Ok(Statement::new(client, name, parameters, columns))
}

fn prepare_rec<'a>(
    client: &'a Arc<InnerClient>,
    name: &'static str,
    query: &'a str,
    types: &'a [Type],
) -> Pin<Box<dyn Future<Output = Result<Statement, Error>> + 'a + Send>> {
    Box::pin(prepare(client, name, query, types))
}

fn encode(client: &InnerClient, name: &str, query: &str, types: &[Type]) -> Result<Bytes, Error> {
    if types.is_empty() {
        debug!("preparing query {}: {}", name, query);
    } else {
        debug!("preparing query {} with types {:?}: {}", name, types, query);
    }

    client.with_buf(|buf| {
        frontend::parse(name, query, types.iter().map(Type::oid), buf).map_err(Error::encode)?;
        frontend::describe(b'S', name, buf).map_err(Error::encode)?;
        frontend::sync(buf);
        Ok(buf.split().freeze())
    })
}

pub async fn get_type(client: &Arc<InnerClient>, oid: Oid) -> Result<Type, Error> {
    if let Some(type_) = Type::from_oid(oid) {
        return Ok(type_);
    }

    if let Some(type_) = client.type_(oid) {
        return Ok(type_);
    }

    let stmt = typeinfo_statement(client).await?;

    let rows = query::query(client, stmt, slice_iter(&[&oid])).await?;
    pin_mut!(rows);

    let row = match rows.try_next().await? {
        Some(row) => row,
        None => return Err(Error::unexpected_message()),
    };

    let name: String = row.try_get(0)?;
    let type_: i8 = row.try_get(1)?;
    let elem_oid: Oid = row.try_get(2)?;
    let rngsubtype: Option<Oid> = row.try_get(3)?;
    let basetype: Oid = row.try_get(4)?;
    let schema: String = row.try_get(5)?;
    let relid: Oid = row.try_get(6)?;

    let kind = if type_ == b'e' as i8 {
        let variants = get_enum_variants(client, oid).await?;
        Kind::Enum(variants)
    } else if type_ == b'p' as i8 {
        Kind::Pseudo
    } else if basetype != 0 {
        let type_ = get_type_rec(client, basetype).await?;
        Kind::Domain(type_)
    } else if elem_oid != 0 {
        let type_ = get_type_rec(client, elem_oid).await?;
        Kind::Array(type_)
    } else if relid != 0 {
        let fields = get_composite_fields(client, relid).await?;
        Kind::Composite(fields)
    } else if let Some(rngsubtype) = rngsubtype {
        let type_ = get_type_rec(client, rngsubtype).await?;
        Kind::Range(type_)
    } else {
        Kind::Simple
    };

    let type_ = Type::new(name, oid, kind, schema);
    client.set_type(oid, &type_);

    Ok(type_)
}

fn get_type_rec<'a>(
    client: &'a Arc<InnerClient>,
    oid: Oid,
) -> Pin<Box<dyn Future<Output = Result<Type, Error>> + Send + 'a>> {
    Box::pin(get_type(client, oid))
}

async fn typeinfo_statement(client: &Arc<InnerClient>) -> Result<Statement, Error> {
    if let Some(stmt) = client.typeinfo() {
        return Ok(stmt);
    }

    let typeinfo = "neon_proxy_typeinfo";
    let stmt = prepare_rec(client, typeinfo, TYPEINFO_QUERY, &[]).await?;

    client.set_typeinfo(&stmt);
    Ok(stmt)
}

async fn get_enum_variants(client: &Arc<InnerClient>, oid: Oid) -> Result<Vec<String>, Error> {
    let stmt = typeinfo_enum_statement(client).await?;

    query::query(client, stmt, slice_iter(&[&oid]))
        .await?
        .and_then(|row| async move { row.try_get(0) })
        .try_collect()
        .await
}

async fn typeinfo_enum_statement(client: &Arc<InnerClient>) -> Result<Statement, Error> {
    if let Some(stmt) = client.typeinfo_enum() {
        return Ok(stmt);
    }

    let typeinfo = "neon_proxy_typeinfo_enum";
    let stmt = prepare_rec(client, typeinfo, TYPEINFO_ENUM_QUERY, &[]).await?;

    client.set_typeinfo_enum(&stmt);
    Ok(stmt)
}

async fn get_composite_fields(client: &Arc<InnerClient>, oid: Oid) -> Result<Vec<Field>, Error> {
    let stmt = typeinfo_composite_statement(client).await?;

    let rows = query::query(client, stmt, slice_iter(&[&oid]))
        .await?
        .try_collect::<Vec<_>>()
        .await?;

    let mut fields = vec![];
    for row in rows {
        let name = row.try_get(0)?;
        let oid = row.try_get(1)?;
        let type_ = get_type_rec(client, oid).await?;
        fields.push(Field::new(name, type_));
    }

    Ok(fields)
}

async fn typeinfo_composite_statement(client: &Arc<InnerClient>) -> Result<Statement, Error> {
    if let Some(stmt) = client.typeinfo_composite() {
        return Ok(stmt);
    }

    let typeinfo = "neon_proxy_typeinfo_composite";
    let stmt = prepare_rec(client, typeinfo, TYPEINFO_COMPOSITE_QUERY, &[]).await?;

    client.set_typeinfo_composite(&stmt);
    Ok(stmt)
}
