///
/// Module for parsing postgresql.conf file.
///
/// NOTE: This doesn't implement the full, correct postgresql.conf syntax. Just
/// enough to extract a few settings we need in Neon, assuming you don't do
/// funny stuff like include-directives or funny escaping.
use once_cell::sync::Lazy;
use regex::Regex;
use std::collections::HashMap;
use std::fmt;

/// In-memory representation of a postgresql.conf file
#[derive(Default, Debug)]
pub struct PostgresConf {
    lines: Vec<String>,
    hash: HashMap<String, String>,
}

impl PostgresConf {
    pub fn new() -> PostgresConf {
        PostgresConf::default()
    }

    /// Return the current value of 'option'
    pub fn get(&self, option: &str) -> Option<&str> {
        self.hash.get(option).map(|x| x.as_ref())
    }

    ///
    /// Note: if you call this multiple times for the same option, the config
    /// file will a line for each call. It would be nice to have a function
    /// to change an existing line, but that's a TODO.
    ///
    pub fn append(&mut self, option: &str, value: &str) {
        self.lines
            .push(format!("{}={}\n", option, escape_str(value)));
        self.hash.insert(option.to_string(), value.to_string());
    }

    /// Append an arbitrary non-setting line to the config file
    pub fn append_line(&mut self, line: &str) {
        self.lines.push(line.to_string());
    }
}

impl fmt::Display for PostgresConf {
    /// Return the whole configuration file as a string
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        for line in self.lines.iter() {
            f.write_str(line)?;
        }
        Ok(())
    }
}

/// Escape a value for putting in postgresql.conf.
fn escape_str(s: &str) -> String {
    // If the string doesn't contain anything that needs quoting or escaping, return it
    // as it is.
    //
    // The first part of the regex, before the '|', matches the INTEGER rule in the
    // PostgreSQL flex grammar (guc-file.l). It matches plain integers like "123" and
    // "-123", and also accepts units like "10MB". The second part of the regex matches
    // the UNQUOTED_STRING rule, and accepts strings that contain a single word, beginning
    // with a letter. That covers words like "off" or "posix". Everything else is quoted.
    //
    // This regex is a bit more conservative than the rules in guc-file.l, so we quote some
    // strings that PostgreSQL would accept without quoting, but that's OK.

    static UNQUOTED_RE: Lazy<Regex> =
        Lazy::new(|| Regex::new(r"(^[-+]?[0-9]+[a-zA-Z]*$)|(^[a-zA-Z][a-zA-Z0-9]*$)").unwrap());

    if UNQUOTED_RE.is_match(s) {
        s.to_string()
    } else {
        // Otherwise escape and quote it
        let s = s
            .replace('\\', "\\\\")
            .replace('\n', "\\n")
            .replace('\'', "''");

        "\'".to_owned() + &s + "\'"
    }
}

#[test]
fn test_postgresql_conf_escapes() -> anyhow::Result<()> {
    assert_eq!(escape_str("foo bar"), "'foo bar'");
    // these don't need to be quoted
    assert_eq!(escape_str("foo"), "foo");
    assert_eq!(escape_str("123"), "123");
    assert_eq!(escape_str("+123"), "+123");
    assert_eq!(escape_str("-10"), "-10");
    assert_eq!(escape_str("1foo"), "1foo");
    assert_eq!(escape_str("foo1"), "foo1");
    assert_eq!(escape_str("10MB"), "10MB");
    assert_eq!(escape_str("-10kB"), "-10kB");

    // these need quoting and/or escaping
    assert_eq!(escape_str("foo bar"), "'foo bar'");
    assert_eq!(escape_str("fo'o"), "'fo''o'");
    assert_eq!(escape_str("fo\no"), "'fo\\no'");
    assert_eq!(escape_str("fo\\o"), "'fo\\\\o'");
    assert_eq!(escape_str("10 cats"), "'10 cats'");

    Ok(())
}
