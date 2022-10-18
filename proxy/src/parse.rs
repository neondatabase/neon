//! Small parsing helpers.

use std::ffi::CStr;

pub fn split_cstr(bytes: &[u8]) -> Option<(&CStr, &[u8])> {
    let pos = bytes.iter().position(|&x| x == 0)?;
    let (cstr, other) = bytes.split_at(pos + 1);
    // SAFETY: we've already checked that there's a terminator
    Some((unsafe { CStr::from_bytes_with_nul_unchecked(cstr) }, other))
}

/// See <https://doc.rust-lang.org/std/primitive.slice.html#method.split_array_ref>.
pub fn split_at_const<const N: usize>(bytes: &[u8]) -> Option<(&[u8; N], &[u8])> {
    (bytes.len() >= N).then(|| {
        let (head, tail) = bytes.split_at(N);
        (head.try_into().unwrap(), tail)
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_split_cstr() {
        assert!(split_cstr(b"").is_none());
        assert!(split_cstr(b"foo").is_none());

        let (cstr, rest) = split_cstr(b"\0").expect("uh-oh");
        assert_eq!(cstr.to_bytes(), b"");
        assert_eq!(rest, b"");

        let (cstr, rest) = split_cstr(b"foo\0bar").expect("uh-oh");
        assert_eq!(cstr.to_bytes(), b"foo");
        assert_eq!(rest, b"bar");
    }

    #[test]
    fn test_split_at_const() {
        assert!(split_at_const::<0>(b"").is_some());
        assert!(split_at_const::<1>(b"").is_none());
        assert!(matches!(split_at_const::<1>(b"ok"), Some((b"o", b"k"))));
    }
}
