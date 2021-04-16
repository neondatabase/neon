use byteorder::{BigEndian, ByteOrder};
use bytes::{Buf, BufMut, Bytes, BytesMut};
use std::io;
use std::str;

pub type Oid = u32;
pub type SystemId = u64;
pub type Result<T> = std::result::Result<T, io::Error>;

#[derive(Debug)]
pub enum FeMessage {
    StartupMessage(FeStartupMessage),
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
    pub system_id: SystemId,
}

#[derive(Debug)]
pub enum StartupRequestCode {
    Cancel,
    NegotiateSsl,
    NegotiateGss,
    Normal,
}

impl FeStartupMessage {
    pub fn parse(buf: &mut BytesMut) -> Result<Option<FeMessage>> {
        const MAX_STARTUP_PACKET_LENGTH: usize = 10000;
        const CANCEL_REQUEST_CODE: u32 = (1234 << 16) | 5678;
        const NEGOTIATE_SSL_CODE: u32 = (1234 << 16) | 5679;
        const NEGOTIATE_GSS_CODE: u32 = (1234 << 16) | 5680;

        if buf.len() < 4 {
            return Ok(None);
        }
        let len = BigEndian::read_u32(&buf[0..4]) as usize;

        if len < 4 || len > MAX_STARTUP_PACKET_LENGTH {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "invalid message length",
            ));
        }
        if buf.len() < len {
            return Ok(None);
        }

        let version = BigEndian::read_u32(&buf[4..8]);

        let kind = match version {
            CANCEL_REQUEST_CODE => StartupRequestCode::Cancel,
            NEGOTIATE_SSL_CODE => StartupRequestCode::NegotiateSsl,
            NEGOTIATE_GSS_CODE => StartupRequestCode::NegotiateGss,
            _ => StartupRequestCode::Normal,
        };

        let params_bytes = &buf[8..len];
        let params_str = str::from_utf8(&params_bytes).unwrap();
        let params = params_str.split('\0');
        let mut options = false;
        let mut system_id: u64 = 0;
        for p in params {
            if p == "options" {
                options = true;
            } else if options {
                for opt in p.split(' ') {
                    if opt.starts_with("system.id=") {
                        system_id = opt[10..].parse::<u64>().unwrap();
                        break;
                    }
                }
                break;
            }
        }

        buf.advance(len as usize);
        Ok(Some(FeMessage::StartupMessage(FeStartupMessage {
            version,
            kind,
            system_id,
        })))
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
    pub fn parse(buf: &mut BytesMut) -> Result<Option<FeMessage>> {
        if buf.len() < 5 {
            let to_read = 5 - buf.len();
            buf.reserve(to_read);
            return Ok(None);
        }

        let tag = buf[0];
        let len = BigEndian::read_u32(&buf[1..5]);

        if len < 4 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "invalid message length: parsing u32",
            ));
        }

        let total_len = len as usize + 1;
        if buf.len() < total_len {
            let to_read = total_len - buf.len();
            buf.reserve(to_read);
            return Ok(None);
        }

        let mut body = buf.split_to(total_len);
        body.advance(5);

        match tag {
            b'Q' => Ok(Some(FeMessage::Query(FeQueryMessage {
                body: body.freeze(),
            }))),
            b'd' => Ok(Some(FeMessage::CopyData(FeCopyData {
                body: body.freeze(),
            }))),
            b'X' => Ok(Some(FeMessage::Terminate)),
            tag => Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("unknown message tag: {},'{:?}'", tag, buf),
            )),
        }
    }
}
