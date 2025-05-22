use bytes::{Bytes, BytesMut};
use fallible_iterator::FallibleIterator;
use postgres_protocol2::IsNull;
use postgres_protocol2::message::backend::{Message, RowDescriptionBody};
use postgres_protocol2::message::frontend;
use postgres_protocol2::types::oid_to_sql;
use postgres_types2::Format;

use crate::client::{CachedTypeInfo, InnerClient};
use crate::codec::FrontendMessage;
use crate::types::{Kind, Oid, Type};
use crate::{Column, Error, Row, Statement};

pub(crate) const TYPEINFO_QUERY: &str = "\
SELECT t.typname, t.typtype, t.typelem, r.rngsubtype, t.typbasetype, n.nspname, t.typrelid
FROM pg_catalog.pg_type t
LEFT OUTER JOIN pg_catalog.pg_range r ON r.rngtypid = t.oid
INNER JOIN pg_catalog.pg_namespace n ON t.typnamespace = n.oid
WHERE t.oid = $1
";

/// we need to send a sync message on error to allow the protocol to reset.
struct CloseStmt<'a> {
    client: &'a mut InnerClient,
    name: &'static str,
}

impl Drop for CloseStmt<'_> {
    fn drop(&mut self) {
        let buf = self.client.with_buf(|buf| {
            frontend::close(b'S', self.name, buf).unwrap();
            buf.split().freeze()
        });
        let _ = self.client.send(FrontendMessage::Raw(buf));
    }
}

async fn prepare_typecheck(
    client: &mut InnerClient,
    name: &'static str,
    query: &str,
) -> Result<Statement, Error> {
    let buf = encode(client, name, query)?;
    let responses = client.send_partial(FrontendMessage::Raw(buf))?;

    match responses.next().await? {
        Message::ParseComplete => {}
        _ => return Err(Error::unexpected_message()),
    }

    match responses.next().await? {
        Message::ParameterDescription(_) => {}
        _ => return Err(Error::unexpected_message()),
    };

    let row_description = match responses.next().await? {
        Message::RowDescription(body) => Some(body),
        Message::NoData => None,
        _ => return Err(Error::unexpected_message()),
    };

    let mut columns = vec![];
    if let Some(row_description) = row_description {
        let mut it = row_description.fields();
        while let Some(field) = it.next().map_err(Error::parse)? {
            let type_ = Type::from_oid(field.type_oid()).ok_or_else(Error::unexpected_message)?;
            let column = Column::new(field.name().to_string(), type_, field);
            columns.push(column);
        }
    }

    Ok(Statement::new(name, columns))
}

fn encode(client: &mut InnerClient, name: &str, query: &str) -> Result<Bytes, Error> {
    client.with_buf(|buf| {
        frontend::parse(name, query, [], buf).map_err(Error::encode)?;
        frontend::describe(b'S', name, buf).map_err(Error::encode)?;
        frontend::flush(buf);
        Ok(buf.split().freeze())
    })
}

enum TypeStack {
    Array,
    Range,
}

fn from_cache(typecache: &CachedTypeInfo, oid: Oid) -> Type {
    if let Some(type_) = Type::from_oid(oid) {
        return type_;
    }

    if let Some(type_) = typecache.types.get(&oid) {
        return type_.clone();
    };

    Type::UNKNOWN
}

pub async fn parse_row_description(
    client: &mut InnerClient,
    typecache: &mut CachedTypeInfo,
    row_description: Option<RowDescriptionBody>,
) -> Result<Vec<Column>, Error> {
    let mut columns = vec![];

    if let Some(row_description) = row_description {
        let mut it = row_description.fields();
        while let Some(field) = it.next().map_err(Error::parse)? {
            let type_ = from_cache(typecache, field.type_oid());
            let column = Column::new(field.name().to_string(), type_, field);
            columns.push(column);
        }
    }

    let all_known = columns.iter().all(|c| c.type_ != Type::UNKNOWN);
    if all_known {
        // all known, return early.
        return Ok(columns);
    }

    let typeinfo = "neon_proxy_typeinfo";

    // make sure to close the typeinfo statement before exiting.
    let guard = CloseStmt {
        name: typeinfo,
        client,
    };

    // get the typeinfo statement.
    let stmt = prepare_typecheck(guard.client, typeinfo, TYPEINFO_QUERY).await?;

    for column in &mut columns {
        if column.type_ != Type::UNKNOWN {
            continue;
        }

        column.type_ = get_type(guard.client, typecache, &stmt, column.type_.oid()).await?;
    }

    // we always close it. this ensures that typeinfo still works in pgbouncer.
    drop(guard);

    Ok(columns)
}

async fn get_type(
    client: &mut InnerClient,
    typecache: &mut CachedTypeInfo,
    stmt: &Statement,
    mut oid: Oid,
) -> Result<Type, Error> {
    let mut stack = vec![];
    let mut type_ = loop {
        if let Some(type_) = Type::from_oid(oid) {
            break type_;
        }

        if let Some(type_) = typecache.types.get(&oid) {
            break type_.clone();
        };

        let row = exec(client, stmt, oid).await?;

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
            stack.push((name, oid, schema, TypeStack::Array));
            oid = elem_oid;
            continue;
        } else if relid != 0 {
            Kind::Composite(relid)
        } else if let Some(rngsubtype) = rngsubtype {
            stack.push((name, oid, schema, TypeStack::Range));
            oid = rngsubtype;
            continue;
        } else {
            Kind::Simple
        };

        let type_ = Type::new(name, oid, kind, schema);
        typecache.types.insert(oid, type_.clone());
        break type_;
    };

    while let Some((name, oid, schema, t)) = stack.pop() {
        let kind = match t {
            TypeStack::Array => Kind::Array(type_),
            TypeStack::Range => Kind::Range(type_),
        };
        type_ = Type::new(name, oid, kind, schema);
        typecache.types.insert(oid, type_.clone());
    }

    Ok(type_)
}

/// exec the typeinfo statement returning one row.
async fn exec(client: &mut InnerClient, statement: &Statement, param: Oid) -> Result<Row, Error> {
    let buf = encode_subquery(client, statement, param)?;
    let responses = client.send_partial(FrontendMessage::Raw(buf))?;

    match responses.next().await? {
        Message::BindComplete => {}
        _ => return Err(Error::unexpected_message()),
    }

    let row = match responses.next().await? {
        Message::DataRow(body) => Row::new(statement.clone(), body, Format::Binary)?,
        _ => return Err(Error::unexpected_message()),
    };

    match responses.next().await? {
        Message::CommandComplete(_) => {}
        _ => return Err(Error::unexpected_message()),
    };

    Ok(row)
}

fn encode_subquery(
    client: &mut InnerClient,
    statement: &Statement,
    param: Oid,
) -> Result<Bytes, Error> {
    client.with_buf(|buf| {
        encode_bind(statement, param, "", buf);
        frontend::execute("", 0, buf).map_err(Error::encode)?;
        frontend::flush(buf);
        Ok(buf.split().freeze())
    })
}

fn encode_bind(statement: &Statement, param: Oid, portal: &str, buf: &mut BytesMut) {
    frontend::bind(
        portal,
        statement.name(),
        [Format::Binary as i16],
        [param],
        |param, buf| {
            oid_to_sql(param, buf);
            Ok(IsNull::No)
        },
        [Format::Binary as i16],
        buf,
    )
    .unwrap();
}
