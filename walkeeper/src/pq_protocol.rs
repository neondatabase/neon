use byteorder::{BigEndian, ReadBytesExt};
use bytes::{BufMut, Bytes, BytesMut};
use pageserver::ZTimelineId;
use std::io::{self, Read};
use std::str;
use std::str::FromStr;

pub type Oid = u32;
pub type SystemId = u64;

#[derive(Debug)]
pub enum FeMessage {
    Query(FeQueryMessage),
    Terminate,
    CopyData(FeCopyData),
}

#[derive(Debug)]
pub struct RowDescriptor {
    pub typoid: Oid,
    pub typlen: i16,
    pub name: &'static [u8],
}

#[derive(Debug)]
pub enum BeMessage<'a> {
    AuthenticationOk,
    ReadyForQuery,
    RowDescription(&'a [RowDescriptor]),
    DataRow(&'a [Option<&'a [u8]>]),
    CommandComplete(&'a [u8]),
    Negotiate,
    Copy,
}

#[derive(Debug)]
pub struct FeStartupMessage {
    pub version: u32,
    pub kind: StartupRequestCode,
    pub timelineid: ZTimelineId,
    pub appname: Option<String>,
}

#[derive(Debug)]
pub enum StartupRequestCode {
    Cancel,
    NegotiateSsl,
    NegotiateGss,
    Normal,
}

impl FeStartupMessage {
    pub fn read_from(reader: &mut impl Read) -> io::Result<Self> {
        const MAX_STARTUP_PACKET_LENGTH: usize = 10000;
        const CANCEL_REQUEST_CODE: u32 = (1234 << 16) | 5678;
        const NEGOTIATE_SSL_CODE: u32 = (1234 << 16) | 5679;
        const NEGOTIATE_GSS_CODE: u32 = (1234 << 16) | 5680;

        let len = reader.read_u32::<BigEndian>()? as usize;

        if len < 4 || len > MAX_STARTUP_PACKET_LENGTH {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "FeStartupMessage: invalid message length",
            ));
        }

        let version = reader.read_u32::<BigEndian>()?;

        let kind = match version {
            CANCEL_REQUEST_CODE => StartupRequestCode::Cancel,
            NEGOTIATE_SSL_CODE => StartupRequestCode::NegotiateSsl,
            NEGOTIATE_GSS_CODE => StartupRequestCode::NegotiateGss,
            _ => StartupRequestCode::Normal,
        };

        // FIXME: A buffer pool would be nice, to avoid zeroing the buffer.
        let params_len = len - 8;
        let mut params_bytes = vec![0u8; params_len];
        reader.read_exact(params_bytes.as_mut())?;

        let params_str = str::from_utf8(&params_bytes).unwrap();
        let params = params_str.split('\0');
        let mut options = false;
        let mut timelineid: Option<ZTimelineId> = None;
        let mut appname: Option<String> = None;
        for p in params {
            if p == "options" {
                options = true;
            } else if options {
                for opt in p.split(' ') {
                    if let Some(ztimelineid_str) = opt.strip_prefix("ztimelineid=") {
                        // FIXME: rethrow parsing error, don't unwrap
                        timelineid = Some(ZTimelineId::from_str(ztimelineid_str).unwrap());
                    } else if let Some(val) = opt.strip_prefix("application_name=") {
                        appname = Some(val.to_string());
                    }
                }
                break;
            }
        }
        if timelineid.is_none() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "timelineid is required",
            ));
        }

        Ok(FeStartupMessage {
            version,
            kind,
            appname,
            timelineid: timelineid.unwrap(),
        })
    }
}

#[derive(Debug)]
pub struct FeQueryMessage {
    pub body: Bytes,
}

#[derive(Debug)]
pub struct FeCopyData {
    pub body: Bytes,
}

impl<'a> BeMessage<'a> {
    pub fn write(buf: &mut BytesMut, message: &BeMessage) {
        match message {
            BeMessage::AuthenticationOk => {
                buf.put_u8(b'R');
                buf.put_i32(4 + 4);
                buf.put_i32(0);
            }

            BeMessage::ReadyForQuery => {
                buf.put_u8(b'Z');
                buf.put_i32(4 + 1);
                buf.put_u8(b'I');
            }

            BeMessage::Negotiate => {
                buf.put_u8(b'N');
            }

            BeMessage::Copy => {
                buf.put_u8(b'W');
                buf.put_i32(7);
                buf.put_u8(b'\0');
                buf.put_u8(b'\0');
                buf.put_u8(b'\0');
            }

            BeMessage::RowDescription(rows) => {
                buf.put_u8(b'T');

                let mut body = BytesMut::new();
                body.put_i16(rows.len() as i16); // # of fields
                for row in rows.iter() {
                    body.put_slice(row.name);
                    body.put_i32(0); /* table oid */
                    body.put_i16(0); /* attnum */
                    body.put_u32(row.typoid);
                    body.put_i16(row.typlen);
                    body.put_i32(-1); /* typmod */
                    body.put_i16(0); /* format code */
                }
                buf.put_i32((4 + body.len()) as i32); // # of bytes, including len field itself
                buf.put(body);
            }

            BeMessage::DataRow(vals) => {
                buf.put_u8(b'D');
                let total_len: usize = vals
                    .iter()
                    .fold(0, |acc, row| acc + 4 + row.map_or(0, |s| s.len()));
                buf.put_u32(4 + 2 + total_len as u32);
                buf.put_u16(vals.len() as u16);
                for val_opt in vals.iter() {
                    if let Some(val) = val_opt {
                        buf.put_u32(val.len() as u32);
                        buf.put_slice(val);
                    } else {
                        buf.put_i32(-1);
                    }
                }
            }

            BeMessage::CommandComplete(cmd) => {
                buf.put_u8(b'C');
                buf.put_i32(4 + cmd.len() as i32);
                buf.put_slice(cmd);
            }
        }
    }
}

impl FeMessage {
    pub fn read_from(reader: &mut impl Read) -> io::Result<FeMessage> {
        let tag = reader.read_u8()?;
        let len = reader.read_u32::<BigEndian>()?;

        if len < 4 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "FeMessage: invalid message length",
            ));
        }

        let body_len = (len - 4) as usize;
        let mut body = vec![0u8; body_len];
        reader.read_exact(&mut body)?;

        match tag {
            b'Q' => Ok(FeMessage::Query(FeQueryMessage { body: body.into() })),
            b'd' => Ok(FeMessage::CopyData(FeCopyData { body: body.into() })),
            b'X' => Ok(FeMessage::Terminate),
            tag => Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("unknown message tag: {},'{:?}'", tag, body),
            )),
        }
    }
}
