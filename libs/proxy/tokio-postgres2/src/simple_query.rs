use crate::client::InnerClient;
use crate::codec::FrontendMessage;
use crate::connection::RequestMessages;
use crate::{Error, ReadyForQueryStatus};
use bytes::Bytes;
use log::debug;
use postgres_protocol2::message::backend::Message;
use postgres_protocol2::message::frontend;

pub async fn batch_execute(
    client: &mut InnerClient,
    query: &str,
) -> Result<ReadyForQueryStatus, Error> {
    debug!("executing statement batch: {}", query);

    let buf = encode(client, query)?;
    let mut responses = client.send(RequestMessages::Single(FrontendMessage::Raw(buf)))?;

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

pub(crate) fn encode(client: &mut InnerClient, query: &str) -> Result<Bytes, Error> {
    client.with_buf(|buf| {
        frontend::query(query, buf).map_err(Error::encode)?;
        Ok(buf.split().freeze())
    })
}
