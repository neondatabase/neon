//! Payload for ad hoc authentication method for clients that don't support SNI.
//! See the `impl` for [`super::backend::Backend<ClientCredentials>`].
//! Read more: <https://github.com/neondatabase/cloud/issues/1620#issuecomment-1165332290>.
//! UPDATE (Mon Aug  8 13:20:34 UTC 2022): the payload format has been simplified.

use bstr::ByteSlice;

use crate::types::EndpointId;

pub(crate) struct PasswordHackPayload {
    pub(crate) endpoint: EndpointId,
    pub(crate) password: Vec<u8>,
}

impl PasswordHackPayload {
    pub(crate) fn parse(bytes: &[u8]) -> Option<Self> {
        // The format is `project=<utf-8>;<password-bytes>` or `project=<utf-8>$<password-bytes>`.
        // The endpoint name is restricted to alphanumeric/hyphen, so it never
        // contains either separator; split on whichever one appears first so
        // we don't truncate the password when it contains the other separator.
        let split = bytes.iter().position(|&b| b == b';' || b == b'$')?;
        let (endpoint, rest) = bytes.split_at(split);
        let password = &rest[1..];

        let endpoint = endpoint.to_str().ok()?;
        Some(Self {
            endpoint: parse_endpoint_param(endpoint)?.into(),
            password: password.to_owned(),
        })
    }
}

pub(crate) fn parse_endpoint_param(bytes: &str) -> Option<&str> {
    bytes
        .strip_prefix("project=")
        .or_else(|| bytes.strip_prefix("endpoint="))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_endpoint_param_fn() {
        let input = "";
        assert!(parse_endpoint_param(input).is_none());

        let input = "project=";
        assert_eq!(parse_endpoint_param(input), Some(""));

        let input = "project=foobar";
        assert_eq!(parse_endpoint_param(input), Some("foobar"));

        let input = "endpoint=";
        assert_eq!(parse_endpoint_param(input), Some(""));

        let input = "endpoint=foobar";
        assert_eq!(parse_endpoint_param(input), Some("foobar"));

        let input = "other_option=foobar";
        assert!(parse_endpoint_param(input).is_none());
    }

    #[test]
    fn parse_password_hack_payload_project() {
        let bytes = b"";
        assert!(PasswordHackPayload::parse(bytes).is_none());

        let bytes = b"project=";
        assert!(PasswordHackPayload::parse(bytes).is_none());

        let bytes = b"project=;";
        let payload: PasswordHackPayload =
            PasswordHackPayload::parse(bytes).expect("parsing failed");
        assert_eq!(payload.endpoint, "");
        assert_eq!(payload.password, b"");

        let bytes = b"project=foobar;pass;word";
        let payload = PasswordHackPayload::parse(bytes).expect("parsing failed");
        assert_eq!(payload.endpoint, "foobar");
        assert_eq!(payload.password, b"pass;word");
    }

    #[test]
    fn parse_password_hack_payload_endpoint() {
        let bytes = b"";
        assert!(PasswordHackPayload::parse(bytes).is_none());

        let bytes = b"endpoint=";
        assert!(PasswordHackPayload::parse(bytes).is_none());

        let bytes = b"endpoint=;";
        let payload = PasswordHackPayload::parse(bytes).expect("parsing failed");
        assert_eq!(payload.endpoint, "");
        assert_eq!(payload.password, b"");

        let bytes = b"endpoint=foobar;pass;word";
        let payload = PasswordHackPayload::parse(bytes).expect("parsing failed");
        assert_eq!(payload.endpoint, "foobar");
        assert_eq!(payload.password, b"pass;word");
    }

    #[test]
    fn parse_password_hack_payload_dollar() {
        let bytes = b"";
        assert!(PasswordHackPayload::parse(bytes).is_none());

        let bytes = b"endpoint=";
        assert!(PasswordHackPayload::parse(bytes).is_none());

        let bytes = b"endpoint=$";
        let payload = PasswordHackPayload::parse(bytes).expect("parsing failed");
        assert_eq!(payload.endpoint, "");
        assert_eq!(payload.password, b"");

        let bytes = b"endpoint=foobar$pass$word";
        let payload = PasswordHackPayload::parse(bytes).expect("parsing failed");
        assert_eq!(payload.endpoint, "foobar");
        assert_eq!(payload.password, b"pass$word");
    }

    // Whichever separator the client used must win, regardless of what
    // characters happen to appear later in the password. Previously the parser
    // always tried `;` before `$`, which truncated the endpoint when a client
    // used `$` as the separator but had `;` in the password (e.g. AWS DMS
    // forbids `: ;+%` only in the password text it builds itself, but other
    // tooling may legitimately pass these through unencoded).
    #[test]
    fn parse_uses_first_separator() {
        let bytes = b"endpoint=foobar$pass;with;semis";
        let payload = PasswordHackPayload::parse(bytes).expect("parsing failed");
        assert_eq!(payload.endpoint, "foobar");
        assert_eq!(payload.password, b"pass;with;semis");

        // And the mirror case: `;` is the separator, password contains `$`.
        let bytes = b"endpoint=foobar;pass$with$dollars";
        let payload = PasswordHackPayload::parse(bytes).expect("parsing failed");
        assert_eq!(payload.endpoint, "foobar");
        assert_eq!(payload.password, b"pass$with$dollars");
    }
}
