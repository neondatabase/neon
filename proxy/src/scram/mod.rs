//! Salted Challenge Response Authentication Mechanism.
//!
//! RFC: <https://datatracker.ietf.org/doc/html/rfc5802>.
//!
//! Reference implementation:
//! * <https://github.com/postgres/postgres/blob/94226d4506e66d6e7cbf4b391f1e7393c1962841/src/backend/libpq/auth-scram.c>
//! * <https://github.com/postgres/postgres/blob/94226d4506e66d6e7cbf4b391f1e7393c1962841/src/interfaces/libpq/fe-auth-scram.c>

mod cache;
mod countmin;
mod exchange;
mod key;
mod messages;
mod pbkdf2;
mod secret;
mod signature;
pub mod threadpool;

use base64::Engine as _;
use base64::prelude::BASE64_STANDARD;
pub(crate) use exchange::{Exchange, exchange};
pub(crate) use key::ScramKey;
pub(crate) use secret::ServerSecret;

const SCRAM_SHA_256: &str = "SCRAM-SHA-256";
const SCRAM_SHA_256_PLUS: &str = "SCRAM-SHA-256-PLUS";

/// A list of supported SCRAM methods.
pub(crate) const METHODS: &[&str] = &[SCRAM_SHA_256_PLUS, SCRAM_SHA_256];
pub(crate) const METHODS_WITHOUT_PLUS: &[&str] = &[SCRAM_SHA_256];

/// Decode base64 into array without any heap allocations
fn base64_decode_array<const N: usize>(input: impl AsRef<[u8]>) -> Option<[u8; N]> {
    let mut bytes = [0u8; N];

    let size = BASE64_STANDARD.decode_slice(input, &mut bytes).ok()?;
    if size != N {
        return None;
    }

    Some(bytes)
}

#[cfg(test)]
mod tests {
    use super::threadpool::ThreadPool;
    use super::{Exchange, ServerSecret};
    use crate::intern::{EndpointIdInt, RoleNameInt};
    use crate::sasl::{Mechanism, Step};
    use crate::types::{EndpointId, RoleName};

    #[test]
    fn snapshot() {
        let iterations = 4096;
        let salt = "QSXCR+Q6sek8bf92";
        let stored_key = "FO+9jBb3MUukt6jJnzjPZOWc5ow/Pu6JtPyju0aqaE8=";
        let server_key = "qxJ1SbmSAi5EcS0J5Ck/cKAm/+Ixa+Kwp63f4OHDgzo=";
        let secret = format!("SCRAM-SHA-256${iterations}:{salt}${stored_key}:{server_key}",);
        let secret = ServerSecret::parse(&secret).unwrap();

        const NONCE: [u8; 18] = [
            1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18,
        ];
        let mut exchange =
            Exchange::new(&secret, || NONCE, crate::tls::TlsServerEndPoint::Undefined);

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

    async fn check(
        pool: &ThreadPool,
        scram_secret: &ServerSecret,
        password: &[u8],
    ) -> Result<(), &'static str> {
        let ep = EndpointId::from("foo");
        let ep = EndpointIdInt::from(ep);
        let role = RoleName::from("user");
        let role = RoleNameInt::from(&role);

        let outcome = super::exchange(pool, ep, role, scram_secret, password)
            .await
            .unwrap();

        match outcome {
            crate::sasl::Outcome::Success(_) => Ok(()),
            crate::sasl::Outcome::Failure(r) => Err(r),
        }
    }

    async fn run_round_trip_test(server_password: &str, client_password: &str) {
        let pool = ThreadPool::new(1);
        let scram_secret = ServerSecret::build(server_password).await.unwrap();
        check(&pool, &scram_secret, client_password.as_bytes())
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn round_trip() {
        run_round_trip_test("pencil", "pencil").await;
    }

    #[tokio::test]
    #[should_panic(expected = "password doesn't match")]
    async fn failure() {
        run_round_trip_test("pencil", "eraser").await;
    }

    #[tokio::test]
    #[tracing_test::traced_test]
    async fn password_cache() {
        let pool = ThreadPool::new(1);
        let scram_secret = ServerSecret::build("password").await.unwrap();

        // wrong passwords are not added to cache
        check(&pool, &scram_secret, b"wrong").await.unwrap_err();
        assert!(!logs_contain("storing cached password"));

        // correct passwords get cached
        check(&pool, &scram_secret, b"password").await.unwrap();
        assert!(logs_contain("storing cached password"));

        // wrong passwords do not match the cache
        check(&pool, &scram_secret, b"wrong").await.unwrap_err();
        assert!(!logs_contain("password validated from cache"));

        // correct passwords match the cache
        check(&pool, &scram_secret, b"password").await.unwrap();
        assert!(logs_contain("password validated from cache"));
    }
}
