//! Salted Challenge Response Authentication Mechanism.
//!
//! RFC: <https://datatracker.ietf.org/doc/html/rfc5802>.
//!
//! Reference implementation:
//! * <https://github.com/postgres/postgres/blob/94226d4506e66d6e7cbf4b391f1e7393c1962841/src/backend/libpq/auth-scram.c>
//! * <https://github.com/postgres/postgres/blob/94226d4506e66d6e7cbf4b391f1e7393c1962841/src/interfaces/libpq/fe-auth-scram.c>

mod exchange;
mod key;
mod messages;
mod secret;
mod signature;

#[cfg(any(test, doc))]
mod password;

pub use exchange::Exchange;
pub use key::ScramKey;
pub use secret::ServerSecret;
pub use secret::*;

use hmac::{Hmac, Mac};
use sha2::{Digest, Sha256};

// TODO: add SCRAM-SHA-256-PLUS
/// A list of supported SCRAM methods.
pub const METHODS: &[&str] = &["SCRAM-SHA-256"];

/// Decode base64 into array without any heap allocations
fn base64_decode_array<const N: usize>(input: impl AsRef<[u8]>) -> Option<[u8; N]> {
    let mut bytes = [0u8; N];

    let size = base64::decode_config_slice(input, base64::STANDARD, &mut bytes).ok()?;
    if size != N {
        return None;
    }

    Some(bytes)
}

/// This function essentially is `Hmac(sha256, key, input)`.
/// Further reading: <https://datatracker.ietf.org/doc/html/rfc2104>.
fn hmac_sha256<'a>(key: &[u8], parts: impl IntoIterator<Item = &'a [u8]>) -> [u8; 32] {
    let mut mac = Hmac::<Sha256>::new_from_slice(key).expect("bad key size");
    parts.into_iter().for_each(|s| mac.update(s));

    mac.finalize().into_bytes().into()
}

fn sha256<'a>(parts: impl IntoIterator<Item = &'a [u8]>) -> [u8; 32] {
    let mut hasher = Sha256::new();
    parts.into_iter().for_each(|s| hasher.update(s));

    hasher.finalize().into()
}

#[cfg(test)]
mod tests {
    use crate::sasl::{Mechanism, Step};

    use super::{password::SaltedPassword, Exchange, ServerSecret};

    #[test]
    fn happy_path() {
        let iterations = 4096;
        let salt_base64 = "QSXCR+Q6sek8bf92";
        let pw = SaltedPassword::new(
            b"pencil",
            base64::decode(salt_base64).unwrap().as_slice(),
            iterations,
        );

        let secret = ServerSecret {
            iterations,
            salt_base64: salt_base64.to_owned(),
            stored_key: pw.client_key().sha256(),
            server_key: pw.server_key(),
            doomed: false,
        };
        const NONCE: [u8; 18] = [
            1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18,
        ];
        let mut exchange = Exchange::new(&secret, || NONCE, None);

        let client_first = "n,,n=user,r=rOprNGfwEbeRWgbNEkqO";
        let client_final = "c=biws,r=rOprNGfwEbeRWgbNEkqOAQIDBAUGBwgJCgsMDQ4PEBES,p=rw1r5Kph5ThxmaUBC2GAQ6MfXbPnNkFiTIvdb/Rear0=";
        let server_first =
            "r=rOprNGfwEbeRWgbNEkqOAQIDBAUGBwgJCgsMDQ4PEBES,s=QSXCR+Q6sek8bf92,i=4096";
        let server_final = "v=qtUDIofVnIhM7tKn93EQUUt5vgMOldcDVu1HC+OH0o0=";

        exchange = match exchange.exchange(client_first).unwrap() {
            Step::Continue(exchange, message) => {
                assert_eq!(message, server_first);
                exchange
            }
            Step::Success(_, _) => panic!("expected continue, got success"),
            Step::Failure(f) => panic!("{f}"),
        };

        let key = match exchange.exchange(client_final).unwrap() {
            Step::Success(key, message) => {
                assert_eq!(message, server_final);
                key
            }
            Step::Continue(_, _) => panic!("expected success, got continue"),
            Step::Failure(f) => panic!("{f}"),
        };

        assert_eq!(
            key.as_bytes(),
            [
                74, 103, 1, 132, 12, 31, 200, 48, 28, 54, 82, 232, 207, 12, 138, 189, 40, 32, 134,
                27, 125, 170, 232, 35, 171, 167, 166, 41, 70, 228, 182, 112,
            ]
        );
    }
}
