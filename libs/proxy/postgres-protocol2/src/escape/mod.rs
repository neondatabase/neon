//! Provides functions for escaping literals and identifiers for use
//! in SQL queries.
//!
//! Prefer parameterized queries where possible. Do not escape
//! parameters in a parameterized query.

#[cfg(test)]
mod test;

/// Escape a literal and surround result with single quotes. Not
/// recommended in most cases.
///
/// If input contains backslashes, result will be of the form `
/// E'...'` so it is safe to use regardless of the setting of
/// standard_conforming_strings.
pub fn escape_literal(input: &str) -> String {
    escape_internal(input, false)
}

/// Escape an identifier and surround result with double quotes.
pub fn escape_identifier(input: &str) -> String {
    escape_internal(input, true)
}

// Translation of PostgreSQL libpq's PQescapeInternal(). Does not
// require a connection because input string is known to be valid
// UTF-8.
//
// Escape arbitrary strings.  If as_ident is true, we escape the
// result as an identifier; if false, as a literal.  The result is
// returned in a newly allocated buffer.  If we fail due to an
// encoding violation or out of memory condition, we return NULL,
// storing an error message into conn.
fn escape_internal(input: &str, as_ident: bool) -> String {
    let mut num_backslashes = 0;
    let mut num_quotes = 0;
    let quote_char = if as_ident { '"' } else { '\'' };

    // Scan the string for characters that must be escaped.
    for ch in input.chars() {
        if ch == quote_char {
            num_quotes += 1;
        } else if ch == '\\' {
            num_backslashes += 1;
        }
    }

    // Allocate output String.
    let mut result_size = input.len() + num_quotes + 3; // two quotes, plus a NUL
    if !as_ident && num_backslashes > 0 {
        result_size += num_backslashes + 2;
    }

    let mut output = String::with_capacity(result_size);

    // If we are escaping a literal that contains backslashes, we use
    // the escape string syntax so that the result is correct under
    // either value of standard_conforming_strings.  We also emit a
    // leading space in this case, to guard against the possibility
    // that the result might be interpolated immediately following an
    // identifier.
    if !as_ident && num_backslashes > 0 {
        output.push(' ');
        output.push('E');
    }

    // Opening quote.
    output.push(quote_char);

    // Use fast path if possible.
    //
    // We've already verified that the input string is well-formed in
    // the current encoding.  If it contains no quotes and, in the
    // case of literal-escaping, no backslashes, then we can just copy
    // it directly to the output buffer, adding the necessary quotes.
    //
    // If not, we must rescan the input and process each character
    // individually.
    if num_quotes == 0 && (num_backslashes == 0 || as_ident) {
        output.push_str(input);
    } else {
        for ch in input.chars() {
            if ch == quote_char || (!as_ident && ch == '\\') {
                output.push(ch);
            }
            output.push(ch);
        }
    }

    output.push(quote_char);

    output
}
