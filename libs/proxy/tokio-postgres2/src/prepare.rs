use std::future::Future;
use std::pin::Pin;

use bytes::Bytes;
use fallible_iterator::FallibleIterator;
use futures_util::{TryStreamExt, pin_mut};
use postgres_protocol2::message::backend::Message;
use postgres_protocol2::message::frontend;
use tracing::debug;

use crate::client::{CachedTypeInfo, InnerClient};
use crate::codec::FrontendMessage;
use crate::connection::RequestMessages;
use crate::types::{Kind, Oid, Type};
use crate::{Column, Error, Statement, query, slice_iter};

pub(crate) const TYPEINFO_QUERY: &str = "\
SELECT t.typname, t.typtype, t.typelem, r.rngsubtype, t.typbasetype, n.nspname, t.typrelid
FROM pg_catalog.pg_type t
LEFT OUTER JOIN pg_catalog.pg_range r ON r.rngtypid = t.oid
INNER JOIN pg_catalog.pg_namespace n ON t.typnamespace = n.oid
WHERE t.oid = $1
";

async fn prepare_typecheck(
    client: &mut InnerClient,
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
        let type_ = Type::from_oid(oid).ok_or_else(Error::unexpected_message)?;
        parameters.push(type_);
    }

    let mut columns = vec![];
    if let Some(row_description) = row_description {
        let mut it = row_description.fields();
        while let Some(field) = it.next().map_err(Error::parse)? {
            let type_ = Type::from_oid(field.type_oid()).ok_or_else(Error::unexpected_message)?;
            let column = Column::new(field.name().to_string(), type_, field);
            columns.push(column);
        }
    }

    Ok(Statement::new(name, parameters, columns))
}

fn encode(
    client: &mut InnerClient,
    name: &str,
    query: &str,
    types: &[Type],
) -> Result<Bytes, Error> {
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

pub async fn get_type(
    client: &mut InnerClient,
    typecache: &mut CachedTypeInfo,
    oid: Oid,
) -> Result<Type, Error> {
    if let Some(type_) = Type::from_oid(oid) {
        return Ok(type_);
    }

    if let Some(type_) = typecache.types.get(&oid) {
        return Ok(type_.clone());
    };

    let stmt = typeinfo_statement(client, typecache).await?;

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
        Kind::Enum
    } else if type_ == b'p' as i8 {
        Kind::Pseudo
    } else if basetype != 0 {
        Kind::Domain(basetype)
    } else if elem_oid != 0 {
        let type_ = get_type_rec(client, typecache, elem_oid).await?;
        Kind::Array(type_)
    } else if relid != 0 {
        Kind::Composite(relid)
    } else if let Some(rngsubtype) = rngsubtype {
        let type_ = get_type_rec(client, typecache, rngsubtype).await?;
        Kind::Range(type_)
    } else {
        Kind::Simple
    };

    let type_ = Type::new(name, oid, kind, schema);
    typecache.types.insert(oid, type_.clone());

    Ok(type_)
}

fn get_type_rec<'a>(
    client: &'a mut InnerClient,
    typecache: &'a mut CachedTypeInfo,
    oid: Oid,
) -> Pin<Box<dyn Future<Output = Result<Type, Error>> + Send + 'a>> {
    Box::pin(get_type(client, typecache, oid))
}

async fn typeinfo_statement(
    client: &mut InnerClient,
    typecache: &mut CachedTypeInfo,
) -> Result<Statement, Error> {
    if let Some(stmt) = &typecache.typeinfo {
        return Ok(stmt.clone());
    }

    let typeinfo = "neon_proxy_typeinfo";
    let stmt = prepare_typecheck(client, typeinfo, TYPEINFO_QUERY, &[]).await?;

    typecache.typeinfo = Some(stmt.clone());
    Ok(stmt)
}
