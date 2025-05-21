use std::iter;
use std::marker::PhantomPinned;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use bytes::{BufMut, Bytes, BytesMut};
use fallible_iterator::FallibleIterator;
use futures_util::{Stream, ready};
use pin_project_lite::pin_project;
use postgres_protocol2::CSafeStr;
use postgres_protocol2::message::backend::Message;
use postgres_protocol2::message::frontend;
use postgres_types2::{Format, ToSql, Type};

use crate::client::{InnerClient, Responses};
use crate::codec::FrontendMessage;
use crate::connection::RequestMessages;
use crate::{Column, Error, ReadyForQueryStatus, Row, Statement};

pub async fn query<'a, I>(
    client: &InnerClient,
    statement: Statement,
    params: I,
) -> Result<RowStream, Error>
where
    I: IntoIterator<Item = &'a (dyn ToSql + Sync)>,
    I::IntoIter: ExactSizeIterator + Clone,
{
    let buf = encode(client, &statement, params)?;
    let responses = start(client, buf).await?;
    Ok(RowStream {
        statement,
        responses,
        command_tag: None,
        status: ReadyForQueryStatus::Unknown,
        output_format: Format::Binary,
        _p: PhantomPinned,
    })
}

pub async fn query_txt<S, I>(
    client: &Arc<InnerClient>,
    query: &str,
    params: I,
) -> Result<RowStream, Error>
where
    S: AsRef<str>,
    I: IntoIterator<Item = Option<S>>,
    I::IntoIter: ExactSizeIterator,
{
    let query = CSafeStr::new(query.as_bytes()).map_err(Error::encode)?;
    let params = params.into_iter();

    let portal = c"".into(); // unnamed portal
    let statement = c"".into(); // unnamed prepared statement

    let buf = client.with_buf(|buf| {
        frontend::parse(
            statement,
            query,         // query to parse
            iter::empty(), // give no type info
            buf,
        );
        frontend::describe(b'S', statement, buf);

        // Bind, pass params as text, retrieve as test
        frontend::bind(
            portal,
            statement,
            iter::empty(), // all parameters use the default format (text)
            params,
            |param, buf| match param {
                Some(param) => {
                    buf.put_slice(param.as_ref().as_bytes());
                    postgres_protocol2::IsNull::No
                }
                None => postgres_protocol2::IsNull::Yes,
            },
            Some(0), // all text
            buf,
        );

        // Execute
        frontend::execute(portal, 0, buf);
        // Sync
        frontend::sync(buf);

        buf.split().freeze()
    });

    // now read the responses
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

    match responses.next().await? {
        Message::BindComplete => {}
        _ => return Err(Error::unexpected_message()),
    }

    let mut parameters = vec![];
    let mut it = parameter_description.parameters();
    while let Some(oid) = it.next().map_err(Error::parse)? {
        let type_ = Type::from_oid(oid).unwrap_or(Type::UNKNOWN);
        parameters.push(type_);
    }

    let mut columns = vec![];
    if let Some(row_description) = row_description {
        let mut it = row_description.fields();
        while let Some(field) = it.next().map_err(Error::parse)? {
            let type_ = Type::from_oid(field.type_oid()).unwrap_or(Type::UNKNOWN);
            let column = Column::new(field.name().to_string(), type_, field);
            columns.push(column);
        }
    }

    Ok(RowStream {
        statement: Statement::new_anonymous(parameters, columns),
        responses,
        command_tag: None,
        status: ReadyForQueryStatus::Unknown,
        output_format: Format::Text,
        _p: PhantomPinned,
    })
}

async fn start(client: &InnerClient, buf: Bytes) -> Result<Responses, Error> {
    let mut responses = client.send(RequestMessages::Single(FrontendMessage::Raw(buf)))?;

    match responses.next().await? {
        Message::BindComplete => {}
        _ => return Err(Error::unexpected_message()),
    }

    Ok(responses)
}

pub fn encode<'a, I>(client: &InnerClient, statement: &Statement, params: I) -> Result<Bytes, Error>
where
    I: IntoIterator<Item = &'a (dyn ToSql + Sync)>,
    I::IntoIter: ExactSizeIterator + Clone,
{
    let portal = c"".into(); // unnamed portal

    client.with_buf(|buf| {
        encode_bind(statement, params, portal, buf)?;
        frontend::execute(portal, 0, buf);
        frontend::sync(buf);
        Ok(buf.split().freeze())
    })
}

pub fn encode_bind<'a, I>(
    statement: &Statement,
    params: I,
    portal: &CSafeStr,
    buf: &mut BytesMut,
) -> Result<(), Error>
where
    I: IntoIterator<Item = &'a (dyn ToSql + Sync)>,
    I::IntoIter: ExactSizeIterator + Clone,
{
    let param_types = statement.params();
    let params = iter::zip(params.into_iter(), param_types);

    assert!(
        param_types.len() == params.len(),
        "expected {} parameters but got {}",
        param_types.len(),
        params.len()
    );

    // check encodings
    for (i, (p, ty)) in params.clone().enumerate() {
        p.check(ty).map_err(|e| Error::to_sql(Box::new(e), i))?
    }

    let param_formats = params.clone().map(|(p, ty)| p.encode_format(ty) as i16);

    frontend::bind(
        portal,
        statement.name(),
        param_formats,
        params,
        |(param, ty), buf| param.to_sql(ty, buf),
        Some(1),
        buf,
    );

    Ok(())
}

pin_project! {
    /// A stream of table rows.
    pub struct RowStream {
        statement: Statement,
        responses: Responses,
        command_tag: Option<String>,
        output_format: Format,
        status: ReadyForQueryStatus,
        #[pin]
        _p: PhantomPinned,
    }
}

impl Stream for RowStream {
    type Item = Result<Row, Error>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.project();
        loop {
            match ready!(this.responses.poll_next(cx)?) {
                Message::DataRow(body) => {
                    return Poll::Ready(Some(Ok(Row::new(
                        this.statement.clone(),
                        body,
                        *this.output_format,
                    )?)));
                }
                Message::EmptyQueryResponse | Message::PortalSuspended => {}
                Message::CommandComplete(body) => {
                    if let Ok(tag) = body.tag() {
                        *this.command_tag = Some(tag.to_string());
                    }
                }
                Message::ReadyForQuery(status) => {
                    *this.status = status.into();
                    return Poll::Ready(None);
                }
                _ => return Poll::Ready(Some(Err(Error::unexpected_message()))),
            }
        }
    }
}

impl RowStream {
    /// Returns information about the columns of data in the row.
    pub fn columns(&self) -> &[Column] {
        self.statement.columns()
    }

    /// Returns the command tag of this query.
    ///
    /// This is only available after the stream has been exhausted.
    pub fn command_tag(&self) -> Option<String> {
        self.command_tag.clone()
    }

    /// Returns if the connection is ready for querying, with the status of the connection.
    ///
    /// This might be available only after the stream has been exhausted.
    pub fn ready_status(&self) -> ReadyForQueryStatus {
        self.status
    }
}
