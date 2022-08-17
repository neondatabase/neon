//! Payload for ad hoc authentication method for clients that don't support SNI.
//! See the `impl` for [`super::backend::BackendType<ClientCredentials>`].
//! Read more: <https://github.com/neondatabase/cloud/issues/1620#issuecomment-1165332290>.
//! UPDATE (Mon Aug  8 13:20:34 UTC 2022): the payload format has been simplified.

use bstr::ByteSlice;

pub struct PasswordHackPayload {
    pub project: String,
    pub password: Vec<u8>,
}

impl PasswordHackPayload {
    pub fn parse(bytes: &[u8]) -> Option<Self> {
        // The format is `project=<utf-8>;<password-bytes>`.
        let mut iter = bytes.strip_prefix(b"project=")?.splitn_str(2, ";");
        let project = iter.next()?.to_str().ok()?.to_owned();
        let password = iter.next()?.to_owned();

        Some(Self { project, password })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_password_hack_payload() {
        let bytes = b"";
        assert!(PasswordHackPayload::parse(bytes).is_none());

        let bytes = b"project=";
        assert!(PasswordHackPayload::parse(bytes).is_none());

        let bytes = b"project=;";
        let payload = PasswordHackPayload::parse(bytes).expect("parsing failed");
        assert_eq!(payload.project, "");
        assert_eq!(payload.password, b"");

        let bytes = b"project=foobar;pass;word";
        let payload = PasswordHackPayload::parse(bytes).expect("parsing failed");
        assert_eq!(payload.project, "foobar");
        assert_eq!(payload.password, b"pass;word");
    }
}
