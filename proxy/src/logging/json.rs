//! Vendoring of serde_json's string escaping code.
//!
//! <https://github.com/serde-rs/json/blob/c1826ebcccb1a520389c6b78ad3da15db279220d/src/ser.rs#L1514-L1552>
//! <https://github.com/serde-rs/json/blob/c1826ebcccb1a520389c6b78ad3da15db279220d/src/ser.rs#L2081-L2157>
//! Licensed by David Tolnay under MIT or Apache-2.0.
//!
//! With modifications by Conrad Ludgate on behalf of Neon.

use std::fmt::{self, Write};

use serde_json::ser::CharEscape;

#[must_use]
pub struct ValueSer<'buf> {
    buf: &'buf mut Vec<u8>,
}

impl<'buf> ValueSer<'buf> {
    pub fn new(buf: &'buf mut Vec<u8>) -> Self {
        Self { buf }
    }

    #[inline]
    pub fn serialize(self, value: &SerializedValue) {
        self.buf.extend_from_slice(&value.0);
    }

    #[inline]
    pub fn str(self, s: &str) {
        format_escaped_str(self.buf, s);
    }

    #[inline]
    pub fn str_args(self, s: fmt::Arguments) {
        format_escaped_display(self.buf, s);
    }

    #[inline]
    pub fn bytes_hex(self, s: &[u8]) {
        self.str_args(format_args!("{s:x?}"));
    }

    #[inline]
    pub fn int(self, x: impl itoa::Integer) {
        write_int(x, self.buf);
    }

    #[inline]
    pub fn float(self, x: impl ryu::Float) {
        write_float(x, self.buf);
    }

    #[inline]
    pub fn bool(self, x: bool) {
        let bool = if x { "true" } else { "false" };
        self.buf.extend_from_slice(bool.as_bytes());
    }

    #[inline]
    pub fn map(self) -> MapSer<'buf> {
        MapSer::new(self.buf)
    }

    #[inline]
    #[expect(unused)]
    pub fn list(self) -> ListSer<'buf> {
        ListSer::new(self.buf)
    }
}

pub struct MapSer<'buf> {
    buf: &'buf mut Vec<u8>,
    first: bool,
}

impl<'buf> MapSer<'buf> {
    #[inline]
    fn new(buf: &'buf mut Vec<u8>) -> Self {
        buf.push(b'{');
        Self { buf, first: true }
    }

    #[inline]
    pub fn entry(&mut self, key: Escaped) -> ValueSer {
        self.entry_inner(|b| key.write(b))
    }

    #[inline]
    pub fn entry_escape(&mut self, key: &str) -> ValueSer {
        self.entry_inner(|b| format_escaped_str(b, key))
    }

    #[inline]
    pub fn entry_escape_args(&mut self, key: fmt::Arguments) -> ValueSer {
        self.entry_inner(|b| format_escaped_display(b, key))
    }

    #[inline]
    fn entry_inner(&mut self, f: impl FnOnce(&mut Vec<u8>)) -> ValueSer {
        if !self.first {
            self.buf.push(b',');
        }
        self.first = false;

        f(self.buf);

        self.buf.push(b':');
        ValueSer { buf: self.buf }
    }
}

impl Drop for MapSer<'_> {
    fn drop(&mut self) {
        self.buf.push(b'}');
    }
}

pub struct ListSer<'buf> {
    buf: &'buf mut Vec<u8>,
    first: bool,
}

impl<'buf> ListSer<'buf> {
    #[inline]
    fn new(buf: &'buf mut Vec<u8>) -> Self {
        buf.push(b'[');
        Self { buf, first: true }
    }

    #[expect(unused)]
    #[inline]
    fn entry(&mut self) -> ValueSer {
        if !self.first {
            self.buf.push(b',');
        }
        self.first = false;
        ValueSer { buf: self.buf }
    }
}

impl Drop for ListSer<'_> {
    fn drop(&mut self) {
        self.buf.push(b']');
    }
}

#[derive(Clone)]
pub struct SerializedValue(Box<[u8]>);

impl SerializedValue {
    #[inline]
    pub fn str(s: &str) -> Self {
        let mut v = vec![];
        v.reserve_exact(2 + s.len());
        format_escaped_str(&mut v, s);
        Self(v.into_boxed_slice())
    }

    #[inline]
    pub fn str_args(s: fmt::Arguments) -> Self {
        if let Some(s) = s.as_str() {
            return Self::str(s);
        }

        let mut v = vec![];
        format_escaped_display(&mut v, s);
        Self(v.into_boxed_slice())
    }

    #[inline]
    pub fn bytes_hex(s: &[u8]) -> Self {
        Self::str_args(format_args!("{s:x?}"))
    }

    #[inline]
    pub fn int(x: impl itoa::Integer) -> Self {
        Self(itoa::Buffer::new().format(x).as_bytes().into())
    }

    #[inline]
    pub fn float(x: impl ryu::Float) -> Self {
        Self(ryu::Buffer::new().format(x).as_bytes().into())
    }

    #[inline]
    pub fn bool(x: bool) -> Self {
        let bool = if x { "true" } else { "false" };
        Self(bool.as_bytes().into())
    }
}

/// Represents a string that didn't need escaping because it's already valid json string.
#[derive(Clone, Copy)]
pub struct Escaped(&'static str);

impl Escaped {
    pub const fn new(s: &'static str) -> Self {
        let mut i = 0;
        while i < s.len() {
            let escape = ESCAPE[s.as_bytes()[i] as usize];
            i += 1;
            assert!(escape == 0, "const json string should not need escaping");
        }

        Self(s)
    }

    pub fn as_str(self) -> &'static str {
        self.0
    }

    fn write(self, buf: &mut Vec<u8>) {
        buf.push(b'"');
        buf.extend_from_slice(self.0.as_bytes());
        buf.push(b'"');
    }
}

fn write_int(x: impl itoa::Integer, b: &mut Vec<u8>) {
    b.extend_from_slice(itoa::Buffer::new().format(x).as_bytes());
}

fn write_float(x: impl ryu::Float, b: &mut Vec<u8>) {
    b.extend_from_slice(ryu::Buffer::new().format(x).as_bytes());
}

#[inline]
fn char_escape_from_escape_table(escape: u8, byte: u8) -> CharEscape {
    match escape {
        self::BB => CharEscape::Backspace,
        self::TT => CharEscape::Tab,
        self::NN => CharEscape::LineFeed,
        self::FF => CharEscape::FormFeed,
        self::RR => CharEscape::CarriageReturn,
        self::QU => CharEscape::Quote,
        self::BS => CharEscape::ReverseSolidus,
        self::UU => CharEscape::AsciiControl(byte),
        _ => unreachable!(),
    }
}

fn format_escaped_str(writer: &mut Vec<u8>, value: &str) {
    writer.push(b'"');
    let rest = format_escaped_str_contents(writer, value);
    writer.extend_from_slice(rest);
    writer.push(b'"');
}

fn format_escaped_display(writer: &mut Vec<u8>, args: fmt::Arguments) {
    writer.push(b'"');

    if let Some(s) = args.as_str() {
        let rest = format_escaped_str_contents(writer, s);
        writer.extend_from_slice(rest);
    } else {
        Collect { buf: writer }
            .write_fmt(args)
            .expect("formatting should not error");
    }

    writer.push(b'"');
}

struct Collect<'buf> {
    buf: &'buf mut Vec<u8>,
}

impl fmt::Write for Collect<'_> {
    fn write_str(&mut self, s: &str) -> fmt::Result {
        let last = format_escaped_str_contents(self.buf, s);
        self.buf.extend(last);
        Ok(())
    }
}

// writes any escape sequences, and returns the suffix still needed to be written.
fn format_escaped_str_contents<'a>(writer: &mut Vec<u8>, value: &'a str) -> &'a [u8] {
    let bytes = value.as_bytes();

    let mut start = 0;

    for (i, &byte) in bytes.iter().enumerate() {
        let escape = ESCAPE[byte as usize];
        if escape == 0 {
            continue;
        }

        writer.extend_from_slice(&bytes[start..i]);

        let char_escape = char_escape_from_escape_table(escape, byte);
        write_char_escape(writer, char_escape);

        start = i + 1;
    }

    &bytes[start..]
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

fn write_char_escape(writer: &mut Vec<u8>, char_escape: CharEscape) {
    let s = match char_escape {
        CharEscape::Quote => b"\\\"",
        CharEscape::ReverseSolidus => b"\\\\",
        CharEscape::Solidus => b"\\/",
        CharEscape::Backspace => b"\\b",
        CharEscape::FormFeed => b"\\f",
        CharEscape::LineFeed => b"\\n",
        CharEscape::CarriageReturn => b"\\r",
        CharEscape::Tab => b"\\t",
        CharEscape::AsciiControl(byte) => {
            static HEX_DIGITS: [u8; 16] = *b"0123456789abcdef";
            let bytes = &[
                b'\\',
                b'u',
                b'0',
                b'0',
                HEX_DIGITS[(byte >> 4) as usize],
                HEX_DIGITS[(byte & 0xF) as usize],
            ];
            return writer.extend_from_slice(bytes);
        }
    };

    writer.extend_from_slice(s);
}
