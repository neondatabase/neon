//! Small parsing helpers.

use std::convert::TryInto;
use std::ffi::CStr;

pub fn split_cstr(bytes: &[u8]) -> Option<(&CStr, &[u8])> {
    let pos = bytes.iter().position(|&x| x == 0)?;
    let (cstr, other) = bytes.split_at(pos + 1);
    // SAFETY: we've already checked that there's a terminator
    Some((unsafe { CStr::from_bytes_with_nul_unchecked(cstr) }, other))
}

pub fn split_at_const<const N: usize>(bytes: &[u8]) -> Option<(&[u8; N], &[u8])> {
    (bytes.len() >= N).then(|| {
        let (head, tail) = bytes.split_at(N);
        (head.try_into().unwrap(), tail)
    })
}
