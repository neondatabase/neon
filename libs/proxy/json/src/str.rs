use std::fmt::{self, Write};

/// Represents a character escape code in a type-safe manner.
pub enum CharEscape {
    /// An escaped quote `"`
    Quote,
    /// An escaped reverse solidus `\`
    ReverseSolidus,
    // /// An escaped solidus `/`
    // Solidus,
    /// An escaped backspace character (usually escaped as `\b`)
    Backspace,
    /// An escaped form feed character (usually escaped as `\f`)
    FormFeed,
    /// An escaped line feed character (usually escaped as `\n`)
    LineFeed,
    /// An escaped carriage return character (usually escaped as `\r`)
    CarriageReturn,
    /// An escaped tab character (usually escaped as `\t`)
    Tab,
    /// An escaped ASCII plane control character (usually escaped as
    /// `\u00XX` where `XX` are two hex characters)
    AsciiControl(u8),
}

impl CharEscape {
    #[inline]
    fn from_escape_table(escape: u8, byte: u8) -> CharEscape {
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
}

pub(crate) fn format_escaped_str(writer: &mut Vec<u8>, value: &str) {
    writer.reserve(2 + value.len());

    writer.push(b'"');

    let rest = format_escaped_str_contents(writer, value);
    writer.extend_from_slice(rest);

    writer.push(b'"');
}

pub(crate) fn format_escaped_fmt(writer: &mut Vec<u8>, args: fmt::Arguments) {
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

        let char_escape = CharEscape::from_escape_table(escape, byte);
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
        // CharEscape::Solidus => b"\\/",
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
