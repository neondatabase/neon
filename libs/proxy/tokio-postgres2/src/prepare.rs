use bytes::BytesMut;
use fallible_iterator::FallibleIterator;
use postgres_protocol2::IsNull;
use postgres_protocol2::message::backend::{Message, RowDescriptionBody};
use postgres_protocol2::message::frontend;
use postgres_protocol2::types::oid_to_sql;
use postgres_types2::Format;

use crate::client::{CachedTypeInfo, PartialQuery, Responses};
use crate::types::{Kind, Oid, Type};
use crate::{Column, Error, Row, Statement};

pub(crate) const TYPEINFO_QUERY: &str = "\
SELECT t.typname, t.typtype, t.typelem, r.rngsubtype, t.typbasetype, n.nspname, t.typrelid
FROM pg_catalog.pg_type t
LEFT OUTER JOIN pg_catalog.pg_range r ON r.rngtypid = t.oid
INNER JOIN pg_catalog.pg_namespace n ON t.typnamespace = n.oid
WHERE t.oid = $1
";

/// we need to make sure we close this prepared statement.
struct CloseStmt<'a, 'b> {
    client: Option<&'a mut PartialQuery<'b>>,
    name: &'static str,
}

impl<'a> CloseStmt<'a, '_> {
    fn close(mut self) -> Result<&'a mut Responses, Error> {
        let client = self.client.take().unwrap();
        client.send_with_flush(|buf| {
            frontend::close(b'S', self.name, buf).map_err(Error::encode)?;
            Ok(())
        })
    }
}

impl Drop for CloseStmt<'_, '_> {
    fn drop(&mut self) {
        if let Some(client) = self.client.take() {
            let _ = client.send_with_flush(|buf| {
                frontend::close(b'S', self.name, buf).map_err(Error::encode)?;
                Ok(())
            });
        }
    }
}

async fn prepare_typecheck(
    client: &mut PartialQuery<'_>,
    name: &'static str,
    query: &str,
) -> Result<Statement, Error> {
    let responses = client.send_with_flush(|buf| {
        frontend::parse(name, query, [], buf).map_err(Error::encode)?;
        frontend::describe(b'S', name, buf).map_err(Error::encode)?;
        Ok(())
    })?;

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

fn try_from_cache(typecache: &CachedTypeInfo, oid: Oid) -> Option<Type> {
    if let Some(type_) = Type::from_oid(oid) {
        return Some(type_);
    }

    if let Some(type_) = typecache.types.get(&oid) {
        return Some(type_.clone());
    };

    None
}

pub async fn parse_row_description(
    client: &mut PartialQuery<'_>,
    typecache: &mut CachedTypeInfo,
    row_description: Option<RowDescriptionBody>,
) -> Result<Vec<Column>, Error> {
    let mut columns = vec![];

    if let Some(row_description) = row_description {
        let mut it = row_description.fields();
        while let Some(field) = it.next().map_err(Error::parse)? {
            let type_ = try_from_cache(typecache, field.type_oid()).unwrap_or(Type::UNKNOWN);
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
    let mut guard = CloseStmt {
        name: typeinfo,
        client: None,
    };
    let client = guard.client.insert(client);

    // get the typeinfo statement.
    let stmt = prepare_typecheck(client, typeinfo, TYPEINFO_QUERY).await?;

    for column in &mut columns {
        column.type_ = get_type(client, typecache, &stmt, column.type_oid()).await?;
    }

    // cancel the close guard.
    let responses = guard.close()?;

    match responses.next().await? {
        Message::CloseComplete => {}
        _ => return Err(Error::unexpected_message()),
    }

    Ok(columns)
}

async fn get_type(
    client: &mut PartialQuery<'_>,
    typecache: &mut CachedTypeInfo,
    stmt: &Statement,
    mut oid: Oid,
) -> Result<Type, Error> {
    let mut stack = vec![];
    let mut type_ = loop {
        if let Some(type_) = try_from_cache(typecache, oid) {
            break type_;
        }

        let row = exec(client, stmt, oid).await?;
        if stack.len() > 8 {
            return Err(Error::unexpected_message());
        }

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
            stack.push((name, oid, schema));
            oid = elem_oid;
            continue;
        } else if relid != 0 {
            Kind::Composite(relid)
        } else if let Some(rngsubtype) = rngsubtype {
            Kind::Range(rngsubtype)
        } else {
            Kind::Simple
        };

        let type_ = Type::new(name, oid, kind, schema);
        typecache.types.insert(oid, type_.clone());
        break type_;
    };

    while let Some((name, oid, schema)) = stack.pop() {
        type_ = Type::new(name, oid, Kind::Array(type_), schema);
        typecache.types.insert(oid, type_.clone());
    }

    Ok(type_)
}

/// exec the typeinfo statement returning one row.
async fn exec(
    client: &mut PartialQuery<'_>,
    statement: &Statement,
    param: Oid,
) -> Result<Row, Error> {
    let responses = client.send_with_flush(|buf| {
        encode_bind(statement, param, "", buf);
        frontend::execute("", 0, buf).map_err(Error::encode)?;
        Ok(())
    })?;

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
