//! Authentication protocol support.
use md5::{Digest, Md5};

pub mod sasl;

/// Hashes authentication information in a way suitable for use in response
/// to an `AuthenticationMd5Password` message.
///
/// The resulting string should be sent back to the database in a
/// `PasswordMessage` message.
#[inline]
pub fn md5_hash(username: &[u8], password: &[u8], salt: [u8; 4]) -> String {
    let mut md5 = Md5::new();
    md5.update(password);
    md5.update(username);
    let output = md5.finalize_reset();
    md5.update(format!("{:x}", output));
    md5.update(salt);
    format!("md5{:x}", md5.finalize())
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn md5() {
        let username = b"md5_user";
        let password = b"password";
        let salt = [0x2a, 0x3d, 0x8f, 0xe0];

        assert_eq!(
            md5_hash(username, password, salt),
            "md562af4dd09bbb41884907a838a3233294"
        );
    }
}
