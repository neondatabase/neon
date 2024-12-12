//! SASL-based authentication support.

use hmac::{Hmac, Mac};
use rand::{self, Rng};
use sha2::digest::FixedOutput;
use sha2::{Digest, Sha256};
use std::fmt::Write;
use std::io;
use std::iter;
use std::mem;
use std::str;
use tokio::task::yield_now;

const NONCE_LENGTH: usize = 24;

/// The identifier of the SCRAM-SHA-256 SASL authentication mechanism.
pub const SCRAM_SHA_256: &str = "SCRAM-SHA-256";
/// The identifier of the SCRAM-SHA-256-PLUS SASL authentication mechanism.
pub const SCRAM_SHA_256_PLUS: &str = "SCRAM-SHA-256-PLUS";

// since postgres passwords are not required to exclude saslprep-prohibited
// characters or even be valid UTF8, we run saslprep if possible and otherwise
// return the raw password.
fn normalize(pass: &[u8]) -> Vec<u8> {
    let pass = match str::from_utf8(pass) {
        Ok(pass) => pass,
        Err(_) => return pass.to_vec(),
    };

    match stringprep::saslprep(pass) {
        Ok(pass) => pass.into_owned().into_bytes(),
        Err(_) => pass.as_bytes().to_vec(),
    }
}

pub(crate) async fn hi(str: &[u8], salt: &[u8], iterations: u32) -> [u8; 32] {
    let mut hmac =
        Hmac::<Sha256>::new_from_slice(str).expect("HMAC is able to accept all key sizes");
    hmac.update(salt);
    hmac.update(&[0, 0, 0, 1]);
    let mut prev = hmac.finalize().into_bytes();

    let mut hi = prev;

    for i in 1..iterations {
        let mut hmac = Hmac::<Sha256>::new_from_slice(str).expect("already checked above");
        hmac.update(&prev);
        prev = hmac.finalize().into_bytes();

        for (hi, prev) in hi.iter_mut().zip(prev) {
            *hi ^= prev;
        }
        // yield every ~250us
        // hopefully reduces tail latencies
        if i % 1024 == 0 {
            yield_now().await
        }
    }

    hi.into()
}

enum ChannelBindingInner {
    Unrequested,
    Unsupported,
    TlsServerEndPoint(Vec<u8>),
}

/// The channel binding configuration for a SCRAM authentication exchange.
pub struct ChannelBinding(ChannelBindingInner);

impl ChannelBinding {
    /// The server did not request channel binding.
    pub fn unrequested() -> ChannelBinding {
        ChannelBinding(ChannelBindingInner::Unrequested)
    }

    /// The server requested channel binding but the client is unable to provide it.
    pub fn unsupported() -> ChannelBinding {
        ChannelBinding(ChannelBindingInner::Unsupported)
    }

    /// The server requested channel binding and the client will use the `tls-server-end-point`
    /// method.
    pub fn tls_server_end_point(signature: Vec<u8>) -> ChannelBinding {
        ChannelBinding(ChannelBindingInner::TlsServerEndPoint(signature))
    }

    fn gs2_header(&self) -> &'static str {
        match self.0 {
            ChannelBindingInner::Unrequested => "y,,",
            ChannelBindingInner::Unsupported => "n,,",
            ChannelBindingInner::TlsServerEndPoint(_) => "p=tls-server-end-point,,",
        }
    }

    fn cbind_data(&self) -> &[u8] {
        match self.0 {
            ChannelBindingInner::Unrequested | ChannelBindingInner::Unsupported => &[],
            ChannelBindingInner::TlsServerEndPoint(ref buf) => buf,
        }
    }
}

/// A pair of keys for the SCRAM-SHA-256 mechanism.
/// See <https://datatracker.ietf.org/doc/html/rfc5802#section-3> for details.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ScramKeys<const N: usize> {
    /// Used by server to authenticate client.
    pub client_key: [u8; N],
    /// Used by client to verify server's signature.
    pub server_key: [u8; N],
}

/// Password or keys which were derived from it.
enum Credentials<const N: usize> {
    /// A regular password as a vector of bytes.
    Password(Vec<u8>),
    /// A precomputed pair of keys.
    Keys(ScramKeys<N>),
}

enum State {
    Update {
        nonce: String,
        password: Credentials<32>,
        channel_binding: ChannelBinding,
    },
    Finish {
        server_key: [u8; 32],
        auth_message: String,
    },
    Done,
}

/// A type which handles the client side of the SCRAM-SHA-256/SCRAM-SHA-256-PLUS authentication
/// process.
///
/// During the authentication process, if the backend sends an `AuthenticationSASL` message which
/// includes `SCRAM-SHA-256` as an authentication mechanism, this type can be used.
///
/// After a `ScramSha256` is constructed, the buffer returned by the `message()` method should be
/// sent to the backend in a `SASLInitialResponse` message along with the mechanism name.
///
/// The server will reply with an `AuthenticationSASLContinue` message. Its contents should be
/// passed to the `update()` method, after which the buffer returned by the `message()` method
/// should be sent to the backend in a `SASLResponse` message.
///
/// The server will reply with an `AuthenticationSASLFinal` message. Its contents should be passed
/// to the `finish()` method, after which the authentication process is complete.
pub struct ScramSha256 {
    message: String,
    state: State,
}

fn nonce() -> String {
    // rand 0.5's ThreadRng is cryptographically secure
    let mut rng = rand::thread_rng();
    (0..NONCE_LENGTH)
        .map(|_| {
            let mut v = rng.gen_range(0x21u8..0x7e);
            if v == 0x2c {
                v = 0x7e
            }
            v as char
        })
        .collect()
}

impl ScramSha256 {
    /// Constructs a new instance which will use the provided password for authentication.
    pub fn new(password: &[u8], channel_binding: ChannelBinding) -> ScramSha256 {
        let password = Credentials::Password(normalize(password));
        ScramSha256::new_inner(password, channel_binding, nonce())
    }

    /// Constructs a new instance which will use the provided key pair for authentication.
    pub fn new_with_keys(keys: ScramKeys<32>, channel_binding: ChannelBinding) -> ScramSha256 {
        let password = Credentials::Keys(keys);
        ScramSha256::new_inner(password, channel_binding, nonce())
    }

    fn new_inner(
        password: Credentials<32>,
        channel_binding: ChannelBinding,
        nonce: String,
    ) -> ScramSha256 {
        ScramSha256 {
            message: format!("{}n=,r={}", channel_binding.gs2_header(), nonce),
            state: State::Update {
                nonce,
                password,
                channel_binding,
            },
        }
    }

    /// Returns the message which should be sent to the backend in an `SASLResponse` message.
    pub fn message(&self) -> &[u8] {
        if let State::Done = self.state {
            panic!("invalid SCRAM state");
        }
        self.message.as_bytes()
    }

    /// Updates the state machine with the response from the backend.
    ///
    /// This should be called when an `AuthenticationSASLContinue` message is received.
    pub async fn update(&mut self, message: &[u8]) -> io::Result<()> {
        let (client_nonce, password, channel_binding) =
            match mem::replace(&mut self.state, State::Done) {
                State::Update {
                    nonce,
                    password,
                    channel_binding,
                } => (nonce, password, channel_binding),
                _ => return Err(io::Error::new(io::ErrorKind::Other, "invalid SCRAM state")),
            };

        let message =
            str::from_utf8(message).map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e))?;

        let parsed = Parser::new(message).server_first_message()?;

        if !parsed.nonce.starts_with(&client_nonce) {
            return Err(io::Error::new(io::ErrorKind::InvalidInput, "invalid nonce"));
        }

        let (client_key, server_key) = match password {
            Credentials::Password(password) => {
                let salt = match base64::decode(parsed.salt) {
                    Ok(salt) => salt,
                    Err(e) => return Err(io::Error::new(io::ErrorKind::InvalidInput, e)),
                };

                let salted_password = hi(&password, &salt, parsed.iteration_count).await;

                let make_key = |name| {
                    let mut hmac = Hmac::<Sha256>::new_from_slice(&salted_password)
                        .expect("HMAC is able to accept all key sizes");
                    hmac.update(name);

                    let mut key = [0u8; 32];
                    key.copy_from_slice(hmac.finalize().into_bytes().as_slice());
                    key
                };

                (make_key(b"Client Key"), make_key(b"Server Key"))
            }
            Credentials::Keys(keys) => (keys.client_key, keys.server_key),
        };

        let mut hash = Sha256::default();
        hash.update(client_key);
        let stored_key = hash.finalize_fixed();

        let mut cbind_input = vec![];
        cbind_input.extend(channel_binding.gs2_header().as_bytes());
        cbind_input.extend(channel_binding.cbind_data());
        let cbind_input = base64::encode(&cbind_input);

        self.message.clear();
        write!(&mut self.message, "c={},r={}", cbind_input, parsed.nonce).unwrap();

        let auth_message = format!("n=,r={},{},{}", client_nonce, message, self.message);

        let mut hmac = Hmac::<Sha256>::new_from_slice(&stored_key)
            .expect("HMAC is able to accept all key sizes");
        hmac.update(auth_message.as_bytes());
        let client_signature = hmac.finalize().into_bytes();

        let mut client_proof = client_key;
        for (proof, signature) in client_proof.iter_mut().zip(client_signature) {
            *proof ^= signature;
        }

        write!(&mut self.message, ",p={}", base64::encode(client_proof)).unwrap();

        self.state = State::Finish {
            server_key,
            auth_message,
        };
        Ok(())
    }

    /// Finalizes the authentication process.
    ///
    /// This should be called when the backend sends an `AuthenticationSASLFinal` message.
    /// Authentication has only succeeded if this method returns `Ok(())`.
    pub fn finish(&mut self, message: &[u8]) -> io::Result<()> {
        let (server_key, auth_message) = match mem::replace(&mut self.state, State::Done) {
            State::Finish {
                server_key,
                auth_message,
            } => (server_key, auth_message),
            _ => return Err(io::Error::new(io::ErrorKind::Other, "invalid SCRAM state")),
        };

        let message =
            str::from_utf8(message).map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e))?;

        let parsed = Parser::new(message).server_final_message()?;

        let verifier = match parsed {
            ServerFinalMessage::Error(e) => {
                return Err(io::Error::new(
                    io::ErrorKind::Other,
                    format!("SCRAM error: {}", e),
                ));
            }
            ServerFinalMessage::Verifier(verifier) => verifier,
        };

        let verifier = match base64::decode(verifier) {
            Ok(verifier) => verifier,
            Err(e) => return Err(io::Error::new(io::ErrorKind::InvalidInput, e)),
        };

        let mut hmac = Hmac::<Sha256>::new_from_slice(&server_key)
            .expect("HMAC is able to accept all key sizes");
        hmac.update(auth_message.as_bytes());
        hmac.verify_slice(&verifier)
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "SCRAM verification error"))
    }
}

struct Parser<'a> {
    s: &'a str,
    it: iter::Peekable<str::CharIndices<'a>>,
}

impl<'a> Parser<'a> {
    fn new(s: &'a str) -> Parser<'a> {
        Parser {
            s,
            it: s.char_indices().peekable(),
        }
    }

    fn eat(&mut self, target: char) -> io::Result<()> {
        match self.it.next() {
            Some((_, c)) if c == target => Ok(()),
            Some((i, c)) => {
                let m = format!(
                    "unexpected character at byte {}: expected `{}` but got `{}",
                    i, target, c
                );
                Err(io::Error::new(io::ErrorKind::InvalidInput, m))
            }
            None => Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "unexpected EOF",
            )),
        }
    }

    fn take_while<F>(&mut self, f: F) -> io::Result<&'a str>
    where
        F: Fn(char) -> bool,
    {
        let start = match self.it.peek() {
            Some(&(i, _)) => i,
            None => return Ok(""),
        };

        loop {
            match self.it.peek() {
                Some(&(_, c)) if f(c) => {
                    self.it.next();
                }
                Some(&(i, _)) => return Ok(&self.s[start..i]),
                None => return Ok(&self.s[start..]),
            }
        }
    }

    fn printable(&mut self) -> io::Result<&'a str> {
        self.take_while(|c| matches!(c, '\x21'..='\x2b' | '\x2d'..='\x7e'))
    }

    fn nonce(&mut self) -> io::Result<&'a str> {
        self.eat('r')?;
        self.eat('=')?;
        self.printable()
    }

    fn base64(&mut self) -> io::Result<&'a str> {
        self.take_while(|c| matches!(c, 'a'..='z' | 'A'..='Z' | '0'..='9' | '/' | '+' | '='))
    }

    fn salt(&mut self) -> io::Result<&'a str> {
        self.eat('s')?;
        self.eat('=')?;
        self.base64()
    }

    fn posit_number(&mut self) -> io::Result<u32> {
        let n = self.take_while(|c| c.is_ascii_digit())?;
        n.parse()
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e))
    }

    fn iteration_count(&mut self) -> io::Result<u32> {
        self.eat('i')?;
        self.eat('=')?;
        self.posit_number()
    }

    fn eof(&mut self) -> io::Result<()> {
        match self.it.peek() {
            Some(&(i, _)) => Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("unexpected trailing data at byte {}", i),
            )),
            None => Ok(()),
        }
    }

    fn server_first_message(&mut self) -> io::Result<ServerFirstMessage<'a>> {
        let nonce = self.nonce()?;
        self.eat(',')?;
        let salt = self.salt()?;
        self.eat(',')?;
        let iteration_count = self.iteration_count()?;
        self.eof()?;

        Ok(ServerFirstMessage {
            nonce,
            salt,
            iteration_count,
        })
    }

    fn value(&mut self) -> io::Result<&'a str> {
        self.take_while(|c| matches!(c, '\0' | '=' | ','))
    }

    fn server_error(&mut self) -> io::Result<Option<&'a str>> {
        match self.it.peek() {
            Some(&(_, 'e')) => {}
            _ => return Ok(None),
        }

        self.eat('e')?;
        self.eat('=')?;
        self.value().map(Some)
    }

    fn verifier(&mut self) -> io::Result<&'a str> {
        self.eat('v')?;
        self.eat('=')?;
        self.base64()
    }

    fn server_final_message(&mut self) -> io::Result<ServerFinalMessage<'a>> {
        let message = match self.server_error()? {
            Some(error) => ServerFinalMessage::Error(error),
            None => ServerFinalMessage::Verifier(self.verifier()?),
        };
        self.eof()?;
        Ok(message)
    }
}

struct ServerFirstMessage<'a> {
    nonce: &'a str,
    salt: &'a str,
    iteration_count: u32,
}

enum ServerFinalMessage<'a> {
    Error(&'a str),
    Verifier(&'a str),
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn parse_server_first_message() {
        let message = "r=fyko+d2lbbFgONRv9qkxdawL3rfcNHYJY1ZVvWVs7j,s=QSXCR+Q6sek8bf92,i=4096";
        let message = Parser::new(message).server_first_message().unwrap();
        assert_eq!(message.nonce, "fyko+d2lbbFgONRv9qkxdawL3rfcNHYJY1ZVvWVs7j");
        assert_eq!(message.salt, "QSXCR+Q6sek8bf92");
        assert_eq!(message.iteration_count, 4096);
    }

    // recorded auth exchange from psql
    #[tokio::test]
    async fn exchange() {
        let password = "foobar";
        let nonce = "9IZ2O01zb9IgiIZ1WJ/zgpJB";

        let client_first = "n,,n=,r=9IZ2O01zb9IgiIZ1WJ/zgpJB";
        let server_first =
            "r=9IZ2O01zb9IgiIZ1WJ/zgpJBjx/oIRLs02gGSHcw1KEty3eY,s=fs3IXBy7U7+IvVjZ,i\
             =4096";
        let client_final =
            "c=biws,r=9IZ2O01zb9IgiIZ1WJ/zgpJBjx/oIRLs02gGSHcw1KEty3eY,p=AmNKosjJzS3\
             1NTlQYNs5BTeQjdHdk7lOflDo5re2an8=";
        let server_final = "v=U+ppxD5XUKtradnv8e2MkeupiA8FU87Sg8CXzXHDAzw=";

        let mut scram = ScramSha256::new_inner(
            Credentials::Password(normalize(password.as_bytes())),
            ChannelBinding::unsupported(),
            nonce.to_string(),
        );
        assert_eq!(str::from_utf8(scram.message()).unwrap(), client_first);

        scram.update(server_first.as_bytes()).await.unwrap();
        assert_eq!(str::from_utf8(scram.message()).unwrap(), client_final);

        scram.finish(server_final.as_bytes()).unwrap();
    }
}
