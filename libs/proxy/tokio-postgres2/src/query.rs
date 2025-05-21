use std::fmt;
use std::pin::Pin;
use std::task::{Context, Poll};

use bytes::{BufMut, Bytes, BytesMut};
use fallible_iterator::FallibleIterator;
use futures_util::{Stream, ready};
use postgres_protocol2::message::backend::Message;
use postgres_protocol2::message::frontend;
use postgres_types2::{Format, ToSql};
use tracing::debug;

use crate::client::{CachedTypeInfo, InnerClient, Responses};
use crate::codec::FrontendMessage;
use crate::types::IsNull;
use crate::{Column, Error, ReadyForQueryStatus, Row, Statement};

struct BorrowToSqlParamsDebug<'a>(&'a [&'a (dyn ToSql + Sync)]);

impl fmt::Debug for BorrowToSqlParamsDebug<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_list().entries(self.0.iter()).finish()
    }
}

/// subquery does not send a sync message.
/// they need to be polled to completion.
pub async fn sub_query<'a, I>(
    client: &mut InnerClient,
    statement: Statement,
    params: I,
) -> Result<RowStream, Error>
where
    I: IntoIterator<Item = &'a (dyn ToSql + Sync)>,
    I::IntoIter: ExactSizeIterator,
{
    let buf = if tracing::enabled!(tracing::Level::DEBUG) {
        let params = params.into_iter().collect::<Vec<_>>();
        debug!(
            "executing statement {} with parameters: {:?}",
            statement.name(),
            BorrowToSqlParamsDebug(params.as_slice()),
        );
        encode_subquery(client, &statement, params)?
    } else {
        encode_subquery(client, &statement, params)?
    };
    let responses = start(client, buf).await?;
    Ok(RowStream {
        responses,
        statement,
        subquery: true,
        command_tag: None,
        status: ReadyForQueryStatus::Unknown,
        output_format: Format::Binary,
    })
}

/// we need to send a sync message on error to allow the protocol to reset.
struct SyncIfNotDone<'a> {
    client: &'a mut InnerClient,
}

impl Drop for SyncIfNotDone<'_> {
    fn drop(&mut self) {
        let buf = self.client.with_buf(|buf| {
            frontend::sync(buf);
            buf.split().freeze()
        });
        let _ = self.client.send_partial(FrontendMessage::Raw(buf));
    }
}

pub async fn query_txt<'a, S, I>(
    client: &'a mut InnerClient,
    typecache: &mut CachedTypeInfo,
    query: &str,
    params: I,
) -> Result<RowStream<'a>, Error>
where
    S: AsRef<str>,
    I: IntoIterator<Item = Option<S>>,
    I::IntoIter: ExactSizeIterator,
{
    let params = params.into_iter();
    let guard = SyncIfNotDone { client };

    // parse the query and get type info
    let buf = guard.client.with_buf(|buf| {
        frontend::parse(
            "",                 // unnamed prepared statement
            query,              // query to parse
            std::iter::empty(), // give no type info
            buf,
        )
        .map_err(Error::encode)?;
        frontend::describe(b'S', "", buf).map_err(Error::encode)?;
        frontend::flush(buf);

        Ok(buf.split().freeze())
    })?;

    // now read the responses
    let responses = guard.client.send(FrontendMessage::Raw(buf))?;

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
        let type_ = crate::prepare::get_type(guard.client, typecache, oid).await?;
        parameters.push(type_);
    }

    let mut columns = vec![];
    if let Some(row_description) = row_description {
        let mut it = row_description.fields();
        while let Some(field) = it.next().map_err(Error::parse)? {
            let type_ = crate::prepare::get_type(guard.client, typecache, field.type_oid()).await?;
            let column = Column::new(field.name().to_string(), type_, field);
            columns.push(column);
        }
    }

    let buf = guard.client.with_buf(|buf| {
        // Bind, pass params as text, retrieve as binary
        match frontend::bind(
            "",                 // empty string selects the unnamed portal
            "",                 // unnamed prepared statement
            std::iter::empty(), // all parameters use the default format (text)
            params,
            |param, buf| match param {
                Some(param) => {
                    buf.put_slice(param.as_ref().as_bytes());
                    Ok(postgres_protocol2::IsNull::No)
                }
                None => Ok(postgres_protocol2::IsNull::Yes),
            },
            Some(0), // all text
            buf,
        ) {
            Ok(()) => Ok(()),
            Err(frontend::BindError::Conversion(e)) => Err(Error::to_sql(e, 0)),
            Err(frontend::BindError::Serialization(e)) => Err(Error::encode(e)),
        }?;

        // Execute
        frontend::execute("", 0, buf).map_err(Error::encode)?;
        // Sync
        frontend::sync(buf);

        Ok(buf.split().freeze())
    })?;

    // we are sending the sync, so let's close our guard.
    std::mem::forget(guard);

    // now read the responses
    let responses = client.send_partial(FrontendMessage::Raw(buf))?;

    match responses.next().await? {
        Message::BindComplete => {}
        _ => return Err(Error::unexpected_message()),
    }

    Ok(RowStream {
        responses,
        statement: Statement::new_anonymous(parameters, columns),
        subquery: false,
        command_tag: None,
        status: ReadyForQueryStatus::Unknown,
        output_format: Format::Text,
    })
}

async fn start(client: &mut InnerClient, buf: Bytes) -> Result<&mut Responses, Error> {
    let responses = client.send_partial(FrontendMessage::Raw(buf))?;

    match responses.next().await? {
        Message::BindComplete => {}
        _ => return Err(Error::unexpected_message()),
    }

    Ok(responses)
}

pub fn encode_subquery<'a, I>(
    client: &mut InnerClient,
    statement: &Statement,
    params: I,
) -> Result<Bytes, Error>
where
    I: IntoIterator<Item = &'a (dyn ToSql + Sync)>,
    I::IntoIter: ExactSizeIterator,
{
    client.with_buf(|buf| {
        encode_bind(statement, params, "", buf)?;
        frontend::execute("", 0, buf).map_err(Error::encode)?;
        frontend::flush(buf);
        Ok(buf.split().freeze())
    })
}

pub fn encode_bind<'a, I>(
    statement: &Statement,
    params: I,
    portal: &str,
    buf: &mut BytesMut,
) -> Result<(), Error>
where
    I: IntoIterator<Item = &'a (dyn ToSql + Sync)>,
    I::IntoIter: ExactSizeIterator,
{
    let param_types = statement.params();
    let params = params.into_iter();

    assert!(
        param_types.len() == params.len(),
        "expected {} parameters but got {}",
        param_types.len(),
        params.len()
    );

    let (param_formats, params): (Vec<_>, Vec<_>) = params
        .zip(param_types.iter())
        .map(|(p, ty)| (p.encode_format(ty) as i16, p))
        .unzip();

    let params = params.into_iter();

    let mut error_idx = 0;
    let r = frontend::bind(
        portal,
        statement.name(),
        param_formats,
        params.zip(param_types).enumerate(),
        |(idx, (param, ty)), buf| match param.to_sql_checked(ty, buf) {
            Ok(IsNull::No) => Ok(postgres_protocol2::IsNull::No),
            Ok(IsNull::Yes) => Ok(postgres_protocol2::IsNull::Yes),
            Err(e) => {
                error_idx = idx;
                Err(e)
            }
        },
        Some(1),
        buf,
    );
    match r {
        Ok(()) => Ok(()),
        Err(frontend::BindError::Conversion(e)) => Err(Error::to_sql(e, error_idx)),
        Err(frontend::BindError::Serialization(e)) => Err(Error::encode(e)),
    }
}

/// A stream of table rows.
pub struct RowStream<'a> {
    responses: &'a mut Responses,
    output_format: Format,
    subquery: bool,
    pub statement: Statement,
    pub command_tag: Option<String>,
    pub status: ReadyForQueryStatus,
}

impl Stream for RowStream<'_> {
    type Item = Result<Row, Error>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        loop {
            match ready!(this.responses.poll_next(cx)?) {
                Message::DataRow(body) => {
                    return Poll::Ready(Some(Ok(Row::new(
                        this.statement.clone(),
                        body,
                        this.output_format,
                    )?)));
                }
                Message::EmptyQueryResponse | Message::PortalSuspended => {
                    if this.subquery {
                        return Poll::Ready(None);
                    }
                }
                Message::CommandComplete(body) => {
                    if let Ok(tag) = body.tag() {
                        this.command_tag = Some(tag.to_string());
                    }
                    if this.subquery {
                        return Poll::Ready(None);
                    }
                }
                Message::ReadyForQuery(status) => {
                    this.status = status.into();
                    return Poll::Ready(None);
                }
                _ => return Poll::Ready(Some(Err(Error::unexpected_message()))),
            }
        }
    }
}
