use std::io;
use bytes::{Buf, Bytes, BytesMut, BufMut};
use byteorder::{BigEndian, ByteOrder};

pub type Oid = u32;
pub type Result<T> = std::result::Result<T, io::Error>;

#[derive(Debug)]
pub enum FeMessage {
    StartupMessage(FeStartupMessage),
    Query(FeQueryMessage),
    Terminate,
    CopyData(FeCopyData)
}

#[derive(Debug)]
pub struct RowDescriptor {
    pub typoid : Oid,
    pub typlen : i16,
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
	Copy
}

#[derive(Debug)]
pub struct FeStartupMessage {
    pub version: u32,
    pub kind: StartupRequestCode,
}

#[derive(Debug)]
pub enum StartupRequestCode {
    Cancel,
    NegotiateSsl,
    NegotiateGss,
    Normal
}

impl FeStartupMessage {
    pub fn parse(buf: &mut BytesMut) -> Result<Option<FeMessage>> {
        const MAX_STARTUP_PACKET_LENGTH: u32 = 10000;
        const CANCEL_REQUEST_CODE: u32 = (1234 << 16) | 5678;
        const NEGOTIATE_SSL_CODE: u32  = (1234 << 16) | 5679;
        const NEGOTIATE_GSS_CODE: u32  = (1234 << 16) | 5680;

        if buf.len() < 4 {
            return Ok(None);
        }
        let len = BigEndian::read_u32(&buf[0..4]);

        if len < 4 || len as u32 > MAX_STARTUP_PACKET_LENGTH {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "invalid message length",
            ));
        }

        let version = BigEndian::read_u32(&buf[4..8]);

        let kind = match version {
            CANCEL_REQUEST_CODE => StartupRequestCode::Cancel,
            NEGOTIATE_SSL_CODE => StartupRequestCode::NegotiateSsl,
            NEGOTIATE_GSS_CODE => StartupRequestCode::NegotiateGss,
            _ => StartupRequestCode::Normal
        };

        buf.advance(len as usize);
        Ok(Some(FeMessage::StartupMessage(FeStartupMessage{version, kind})))
    }
}

#[derive(Debug)]
pub struct FeQueryMessage {
    pub body: Bytes
}

#[derive(Debug)]
pub struct FeCopyData {
    pub body: Bytes
}

impl<'a> BeMessage<'a> {
    pub fn write(buf : &mut BytesMut, message: &BeMessage) {
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
				let total_len:u32 = rows.iter().fold(0, |acc,row| acc + row.name.len() as u32 + 3*(4 + 2));
				buf.put_u32(4 + 2 + total_len);
				for row in rows.iter() {
					buf.put_i16(row.name.len() as i16);
					buf.put_slice(row.name);
					buf.put_i32(0);    /* table oid */
					buf.put_i16(0);    /* attnum */
					buf.put_u32(row.typoid);
					buf.put_i16(row.typlen);
					buf.put_i32(-1);   /* typmod */
					buf.put_i16(0);    /* format code */
				}
            }

            BeMessage::DataRow(vals) => {
                buf.put_u8(b'D');
				let total_len:usize = vals.iter().fold(0, |acc, row| acc + 4 + row.map_or(0, |s| s.len()));
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
            b'Q' => Ok(Some(FeMessage::Query(FeQueryMessage{body:body.freeze()}))),
            b'd' => Ok(Some(FeMessage::CopyData(FeCopyData{body:body.freeze()}))),
            b'X' => Ok(Some(FeMessage::Terminate)),
            tag => {
                Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    format!("unknown message tag: {},'{:?}'", tag, buf),
                ))
            }
        }
    }
}
