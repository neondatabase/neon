//! Definitions for SASL messages.

use crate::parse::{split_at_const, split_cstr};
use utils::pq_proto::{BeAuthenticationSaslMessage, BeMessage};

/// SASL-specific payload of [`PasswordMessage`](utils::pq_proto::FeMessage::PasswordMessage).
#[derive(Debug)]
pub struct FirstMessage<'a> {
    /// Authentication method, e.g. `"SCRAM-SHA-256"`.
    pub method: &'a str,
    /// Initial client message.
    pub message: &'a str,
}

impl<'a> FirstMessage<'a> {
    // NB: FromStr doesn't work with lifetimes
    pub fn parse(bytes: &'a [u8]) -> Option<Self> {
        let (method_cstr, tail) = split_cstr(bytes)?;
        let method = method_cstr.to_str().ok()?;

        let (len_bytes, bytes) = split_at_const(tail)?;
        let len = u32::from_be_bytes(*len_bytes) as usize;
        if len != bytes.len() {
            return None;
        }

        let message = std::str::from_utf8(bytes).ok()?;
        Some(Self { method, message })
    }
}

/// A single SASL message.
/// This struct is deliberately decoupled from lower-level
/// [`BeAuthenticationSaslMessage`](utils::pq_proto::BeAuthenticationSaslMessage).
#[derive(Debug)]
pub(super) enum ServerMessage<T> {
    /// We expect to see more steps.
    Continue(T),
    /// This is the final step.
    Final(T),
}

impl<'a> ServerMessage<&'a str> {
    pub(super) fn to_reply(&self) -> BeMessage<'a> {
        use BeAuthenticationSaslMessage::*;
        BeMessage::AuthenticationSasl(match self {
            ServerMessage::Continue(s) => Continue(s.as_bytes()),
            ServerMessage::Final(s) => Final(s.as_bytes()),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_sasl_first_message() {
        let proto = "SCRAM-SHA-256";
        let sasl = "n,,n=,r=KHQ2Gjc7NptyB8aov5/TnUy4";
        let sasl_len = (sasl.len() as u32).to_be_bytes();
        let bytes = [proto.as_bytes(), &[0], sasl_len.as_ref(), sasl.as_bytes()].concat();

        let password = FirstMessage::parse(&bytes).unwrap();
        assert_eq!(password.method, proto);
        assert_eq!(password.message, sasl);
    }
}
