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
mod secret;
mod signature;

pub use channel_binding::*;
pub use secret::*;

pub use exchange::Exchange;
pub use secret::ServerSecret;

/// Decode base64 into array without any heap allocations
fn base64_decode_array<const N: usize>(input: impl AsRef<[u8]>) -> Option<[u8; N]> {
    let mut bytes = [0u8; N];

    let size = base64::decode_config_slice(input, base64::STANDARD, &mut bytes).ok()?;
    if size != N {
        return None;
    }

    Some(bytes)
}
