//! Definitions for SASL messages.

use crate::parse::split_cstr;

/// SASL-specific payload of [`PasswordMessage`](pq_proto::FeMessage::PasswordMessage).
#[derive(Debug)]
pub(crate) struct FirstMessage<'a> {
    /// Authentication method, e.g. `"SCRAM-SHA-256"`.
    pub(crate) method: &'a str,
    /// Initial client message.
    pub(crate) message: &'a str,
}

impl<'a> FirstMessage<'a> {
    // NB: FromStr doesn't work with lifetimes
    pub(crate) fn parse(bytes: &'a [u8]) -> Option<Self> {
        let (method_cstr, tail) = split_cstr(bytes)?;
        let method = method_cstr.to_str().ok()?;

        let (len_bytes, bytes) = tail.split_first_chunk()?;
        let len = u32::from_be_bytes(*len_bytes) as usize;
        if len != bytes.len() {
            return None;
        }

        let message = std::str::from_utf8(bytes).ok()?;
        Some(Self { method, message })
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
