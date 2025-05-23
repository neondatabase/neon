use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use fallible_iterator::FallibleIterator;
use futures_util::{Stream, ready};
use pin_project_lite::pin_project;
use postgres_protocol2::message::backend::Message;
use tracing::debug;

use crate::client::{InnerClient, Responses};
use crate::{Error, ReadyForQueryStatus, SimpleQueryMessage, SimpleQueryRow};

/// Information about a column of a single query row.
#[derive(Debug)]
pub struct SimpleColumn {
    name: String,
}

impl SimpleColumn {
    pub(crate) fn new(name: String) -> SimpleColumn {
        SimpleColumn { name }
    }

    /// Returns the name of the column.
    pub fn name(&self) -> &str {
        &self.name
    }
}

pub async fn simple_query<'a>(
    client: &'a mut InnerClient,
    query: &str,
) -> Result<SimpleQueryStream<'a>, Error> {
    debug!("executing simple query: {}", query);

    let responses = client.send_simple_query(query)?;

    Ok(SimpleQueryStream {
        responses,
        columns: None,
        status: ReadyForQueryStatus::Unknown,
    })
}

pub async fn batch_execute(
    client: &mut InnerClient,
    query: &str,
) -> Result<ReadyForQueryStatus, Error> {
    debug!("executing statement batch: {}", query);

    let responses = client.send_simple_query(query)?;

    loop {
        match responses.next().await? {
            Message::ReadyForQuery(status) => return Ok(status.into()),
            Message::CommandComplete(_)
            | Message::EmptyQueryResponse
            | Message::RowDescription(_)
            | Message::DataRow(_) => {}
            _ => return Err(Error::unexpected_message()),
        }
    }
}

pin_project! {
    /// A stream of simple query results.
    pub struct SimpleQueryStream<'a> {
        responses: &'a mut Responses,
        columns: Option<Arc<[SimpleColumn]>>,
        status: ReadyForQueryStatus,
    }
}

impl SimpleQueryStream<'_> {
    /// Returns if the connection is ready for querying, with the status of the connection.
    ///
    /// This might be available only after the stream has been exhausted.
    pub fn ready_status(&self) -> ReadyForQueryStatus {
        self.status
    }
}

impl Stream for SimpleQueryStream<'_> {
    type Item = Result<SimpleQueryMessage, Error>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.project();
        loop {
            match ready!(this.responses.poll_next(cx)?) {
                Message::CommandComplete(body) => {
                    let rows = body
                        .tag()
                        .map_err(Error::parse)?
                        .rsplit(' ')
                        .next()
                        .unwrap()
                        .parse()
                        .unwrap_or(0);
                    return Poll::Ready(Some(Ok(SimpleQueryMessage::CommandComplete(rows))));
                }
                Message::EmptyQueryResponse => {
                    return Poll::Ready(Some(Ok(SimpleQueryMessage::CommandComplete(0))));
                }
                Message::RowDescription(body) => {
                    let columns = body
                        .fields()
                        .map(|f| Ok(SimpleColumn::new(f.name().to_string())))
                        .collect::<Vec<_>>()
                        .map_err(Error::parse)?
                        .into();

                    *this.columns = Some(columns);
                }
                Message::DataRow(body) => {
                    let row = match &this.columns {
                        Some(columns) => SimpleQueryRow::new(columns.clone(), body)?,
                        None => return Poll::Ready(Some(Err(Error::unexpected_message()))),
                    };
                    return Poll::Ready(Some(Ok(SimpleQueryMessage::Row(row))));
                }
                Message::ReadyForQuery(s) => {
                    *this.status = s.into();
                    return Poll::Ready(None);
                }
                _ => return Poll::Ready(Some(Err(Error::unexpected_message()))),
            }
        }
    }
}
