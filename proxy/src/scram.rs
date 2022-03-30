//! Salted Challenge Response Authentication Mechanism.
//!
//! RFC: <https://datatracker.ietf.org/doc/html/rfc5802>.
//!
//! Reference implementation:
//! * <https://github.com/postgres/postgres/blob/94226d4506e66d6e7cbf4b391f1e7393c1962841/src/backend/libpq/auth-scram.c>
//! * <https://github.com/postgres/postgres/blob/94226d4506e66d6e7cbf4b391f1e7393c1962841/src/interfaces/libpq/fe-auth-scram.c>

mod channel_binding;
mod exchange;
mod key;
mod messages;
mod password;
mod secret;
mod signature;

pub use channel_binding::*;
pub use secret::*;

pub use exchange::Exchange;
pub use secret::ServerSecret;

use hmac::{Hmac, Mac, NewMac};
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
    let mut mac = Hmac::<Sha256>::new_varkey(key).expect("bad key size");
    parts.into_iter().for_each(|s| mac.update(s));

    // TODO: maybe newer `hmac` et al already migrated to regular arrays?
    let mut result = [0u8; 32];
    result.copy_from_slice(mac.finalize().into_bytes().as_slice());
    result
}

fn sha256<'a>(parts: impl IntoIterator<Item = &'a [u8]>) -> [u8; 32] {
    let mut hasher = Sha256::new();
    parts.into_iter().for_each(|s| hasher.update(s));

    let mut result = [0u8; 32];
    result.copy_from_slice(hasher.finalize().as_slice());
    result
}
