//! Client authentication mechanisms.

pub mod backend;
pub use backend::Backend;

mod password_hack;
use password_hack::PasswordHackPayload;

mod flow;
pub(crate) use flow::*;
use quinn::{RecvStream, SendStream};
use tokio::io::Join;
use tokio_util::codec::Framed;

use crate::PglbCodec;

pub type AuthProxyStream = Framed<Join<RecvStream, SendStream>, PglbCodec>;
