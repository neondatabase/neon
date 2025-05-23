use std::pin::Pin;
use std::task::{Context, Poll};

use bytes::BufMut;
use futures_util::{Stream, ready};
use postgres_protocol2::message::backend::Message;
use postgres_protocol2::message::frontend;
use postgres_types2::Format;

use crate::client::{CachedTypeInfo, InnerClient, Responses};
use crate::{Error, ReadyForQueryStatus, Row, Statement};

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
    let mut client = client.start()?;

    // Flow:
    // 1. Parse the query
    // 2. Inspect the row description for OIDs
    // 3. If there's any OIDs we don't already know about, perform the typeinfo routine
    // 4. Execute the query
    // 5. Sync.
    //
    // The typeinfo routine:
    // 1. Parse the typeinfo query
    // 2. Execute the query on each OID
    // 3. If the result does not match an OID we know, repeat 2.

    // parse the query and get type info
    let responses = client.send_with_flush(|buf| {
        frontend::parse(
            "",                 // unnamed prepared statement
            query,              // query to parse
            std::iter::empty(), // give no type info
            buf,
        )
        .map_err(Error::encode)?;
        frontend::describe(b'S', "", buf).map_err(Error::encode)?;
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

    let columns =
        crate::prepare::parse_row_description(&mut client, typecache, row_description).await?;

    let responses = client.send_with_sync(|buf| {
        // Bind, pass params as text, retrieve as text
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

        Ok(())
    })?;

    match responses.next().await? {
        Message::BindComplete => {}
        _ => return Err(Error::unexpected_message()),
    }

    Ok(RowStream {
        responses,
        statement: Statement::new("", columns),
        command_tag: None,
        status: ReadyForQueryStatus::Unknown,
        output_format: Format::Text,
    })
}

/// A stream of table rows.
pub struct RowStream<'a> {
    responses: &'a mut Responses,
    output_format: Format,
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
                Message::EmptyQueryResponse | Message::PortalSuspended => {}
                Message::CommandComplete(body) => {
                    if let Ok(tag) = body.tag() {
                        this.command_tag = Some(tag.to_string());
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
