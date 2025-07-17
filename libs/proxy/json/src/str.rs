//! Helpers for serializing escaped strings.
//!
//! ## License
//!
//! <https://github.com/serde-rs/json/blob/c1826ebcccb1a520389c6b78ad3da15db279220d/src/ser.rs#L1514-L1552>
//! <https://github.com/serde-rs/json/blob/c1826ebcccb1a520389c6b78ad3da15db279220d/src/ser.rs#L2081-L2157>
//! Licensed by David Tolnay under MIT or Apache-2.0.
//!
//! With modifications by Conrad Ludgate on behalf of Databricks.

use std::fmt::{self, Write};

use crate::{KeyEncoder, ValueEncoder, ValueSer};

impl KeyEncoder for &str {}
impl ValueEncoder for &str {
    #[inline]
    fn encode(self, v: ValueSer<'_>) {
        format_escaped_str(v.buf, self);
        v.finish();
    }
}

impl KeyEncoder for fmt::Arguments<'_> {}
impl ValueEncoder for fmt::Arguments<'_> {
    #[inline]
    fn encode(self, v: ValueSer<'_>) {
        if let Some(s) = self.as_str() {
            format_escaped_str(v.buf, s);
        } else {
            format_escaped_fmt(v.buf, self);
        }
        v.finish();
    }
}

fn format_escaped_str(writer: &mut Vec<u8>, value: &str) {
    writer.reserve(2 + value.len());

    writer.push(b'"');

    format_escaped_str_contents(writer, value);

    writer.push(b'"');
}

fn format_escaped_fmt(writer: &mut Vec<u8>, args: fmt::Arguments) {
    writer.push(b'"');

    Collect { buf: writer }
        .write_fmt(args)
        .expect("formatting should not error");

    writer.push(b'"');
}

struct Collect<'buf> {
    buf: &'buf mut Vec<u8>,
}

impl fmt::Write for Collect<'_> {
    fn write_str(&mut self, s: &str) -> fmt::Result {
        format_escaped_str_contents(self.buf, s);
        Ok(())
    }
}

// writes any escape sequences, and returns the suffix still needed to be written.
fn format_escaped_str_contents(writer: &mut Vec<u8>, value: &str) {
    let bytes = value.as_bytes();

    let mut start = 0;

    for (i, &byte) in bytes.iter().enumerate() {
        let escape = ESCAPE[byte as usize];
        if escape == 0 {
            continue;
        }

        let next = i + 1;
        write_char_escape(writer, &bytes[start..next]);
        start = next;
    }

    writer.extend_from_slice(&bytes[start..]);
}

const BB: u8 = b'b'; // \x08
const TT: u8 = b't'; // \x09
const NN: u8 = b'n'; // \x0A
const FF: u8 = b'f'; // \x0C
const RR: u8 = b'r'; // \x0D
const QU: u8 = b'"'; // \x22
const BS: u8 = b'\\'; // \x5C
const UU: u8 = b'u'; // \x00...\x1F except the ones above
const __: u8 = 0;

// Lookup table of escape sequences. A value of b'x' at index i means that byte
// i is escaped as "\x" in JSON. A value of 0 means that byte i is not escaped.
static ESCAPE: [u8; 256] = [
    //   1   2   3   4   5   6   7   8   9   A   B   C   D   E   F
    UU, UU, UU, UU, UU, UU, UU, UU, BB, TT, NN, UU, FF, RR, UU, UU, // 0
    UU, UU, UU, UU, UU, UU, UU, UU, UU, UU, UU, UU, UU, UU, UU, UU, // 1
    __, __, QU, __, __, __, __, __, __, __, __, __, __, __, __, __, // 2
    __, __, __, __, __, __, __, __, __, __, __, __, __, __, __, __, // 3
    __, __, __, __, __, __, __, __, __, __, __, __, __, __, __, __, // 4
    __, __, __, __, __, __, __, __, __, __, __, __, BS, __, __, __, // 5
    __, __, __, __, __, __, __, __, __, __, __, __, __, __, __, __, // 6
    __, __, __, __, __, __, __, __, __, __, __, __, __, __, __, __, // 7
    __, __, __, __, __, __, __, __, __, __, __, __, __, __, __, __, // 8
    __, __, __, __, __, __, __, __, __, __, __, __, __, __, __, __, // 9
    __, __, __, __, __, __, __, __, __, __, __, __, __, __, __, __, // A
    __, __, __, __, __, __, __, __, __, __, __, __, __, __, __, __, // B
    __, __, __, __, __, __, __, __, __, __, __, __, __, __, __, __, // C
    __, __, __, __, __, __, __, __, __, __, __, __, __, __, __, __, // D
    __, __, __, __, __, __, __, __, __, __, __, __, __, __, __, __, // E
    __, __, __, __, __, __, __, __, __, __, __, __, __, __, __, __, // F
];

#[cold]
fn write_char_escape(writer: &mut Vec<u8>, bytes: &[u8]) {
    let (&byte, string_run) = bytes.split_last().expect("bytes will not be empty");

    let escape = ESCAPE[byte as usize];
    debug_assert_ne!(escape, 0);

    // the escape char from the escape table is the correct replacement
    // character.
    let mut bytes = [b'\\', escape, b'0', b'0', b'0', b'0'];

    let s = match escape {
        // if the replacement character is 'u', then we need
        // to write the utf16 encoding
        UU => {
            static HEX_DIGITS: [u8; 16] = *b"0123456789abcdef";
            bytes[4] = HEX_DIGITS[(byte >> 4) as usize];
            bytes[5] = HEX_DIGITS[(byte & 0xF) as usize];
            &bytes
        }
        _ => &bytes[0..2],
    };

    writer.extend_from_slice(string_run);
    writer.extend_from_slice(s);
}
