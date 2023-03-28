///
/// Module for parsing postgresql.conf file.
///
/// NOTE: This doesn't implement the full, correct postgresql.conf syntax. Just
/// enough to extract a few settings we need in Neon, assuming you don't do
/// funny stuff like include-directives or funny escaping.
use anyhow::{bail, Context, Result};
use once_cell::sync::Lazy;
use regex::Regex;
use std::collections::HashMap;
use std::fmt;
use std::io::BufRead;
use std::str::FromStr;

/// In-memory representation of a postgresql.conf file
#[derive(Default)]
pub struct PostgresConf {
    lines: Vec<String>,
    hash: HashMap<String, String>,
}

#[derive(Debug, PartialEq, Eq, thiserror::Error)]
#[error("PostgresConfParseError")]
pub struct PostgresConfParseError;

static CONF_LINE_RE: Lazy<Regex> = Lazy::new(|| Regex::new(r"^((?:\w|\.)+)\s*=\s*(\S+)$").unwrap());

impl PostgresConf {
    pub fn new() -> PostgresConf {
        PostgresConf::default()
    }

    /// Read file into memory
    pub fn read(read: impl std::io::Read) -> Result<PostgresConf> {
        let mut result = Self::new();

        for line in std::io::BufReader::new(read).lines() {
            let line = line?;

            // Store each line in a vector, in original format
            result.lines.push(line.clone());

            // Also parse each line and insert key=value lines into a hash map.
            //
            // FIXME: This doesn't match exactly the flex/bison grammar in PostgreSQL.
            // But it's close enough for our usage.
            let line = line.trim();
            if line.starts_with('#') {
                // comment, ignore
                continue;
            } else if let Some(caps) = CONF_LINE_RE.captures(line) {
                let name = caps.get(1).unwrap().as_str();
                let raw_val = caps.get(2).unwrap().as_str();

                if let Ok(val) = deescape_str(raw_val) {
                    // Note: if there's already an entry in the hash map for
                    // this key, this will replace it. That's the behavior what
                    // we want; when PostgreSQL reads the file, each line
                    // overrides any previous value for the same setting.
                    result.hash.insert(name.to_string(), val.to_string());
                }
            }
        }
        Ok(result)
    }

    /// Return the current value of 'option'
    pub fn get(&self, option: &str) -> Option<&str> {
        self.hash.get(option).map(|x| x.as_ref())
    }

    /// Return the current value of a field, parsed to the right datatype.
    ///
    /// This calls the FromStr::parse() function on the value of the field. If
    /// the field does not exist, or parsing fails, returns an error.
    ///
    pub fn parse_field<T>(&self, field_name: &str, context: &str) -> Result<T>
    where
        T: FromStr,
        <T as FromStr>::Err: std::error::Error + Send + Sync + 'static,
    {
        self.get(field_name)
            .with_context(|| format!("could not find '{}' option {}", field_name, context))?
            .parse::<T>()
            .with_context(|| format!("could not parse '{}' option {}", field_name, context))
    }

    pub fn parse_field_optional<T>(&self, field_name: &str, context: &str) -> Result<Option<T>>
    where
        T: FromStr,
        <T as FromStr>::Err: std::error::Error + Send + Sync + 'static,
    {
        if let Some(val) = self.get(field_name) {
            let result = val
                .parse::<T>()
                .with_context(|| format!("could not parse '{}' option {}", field_name, context))?;

            Ok(Some(result))
        } else {
            Ok(None)
        }
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

    /// Override the config by user sepefic config
    pub fn override_conf(&mut self, user_config: Option<PostgresConf>) {
        let Some(user_config) = user_config else { return };
        for (name, value) in user_config.hash.iter() {
            let old_value = self.hash.insert(name.to_string(), value.to_string());

            if old_value.is_none() {
                continue;
            }

            let old_line = format!("{}={}\n", name.to_string(), escape_str(&old_value.unwrap()));
            for x in &mut self.lines {
                if *x == old_line {
                    *x = format!("{}={}\n", name.to_string(), escape_str(&value));
                }
            }
        }
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

impl FromStr for PostgresConf {
    type Err = PostgresConfParseError;

    /// Parse an PostgresConf from a string in the form `parameter1=value1;parameter2=value2`
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let mut result = PostgresConf::new();
        let mut splitter = s.trim().split(';');
        while let Some(line) = splitter.next() {
            let line = line.trim();

            if line.starts_with('#') {
                continue;
            } else if let Some(caps) = CONF_LINE_RE.captures(line) {
                let name = caps.get(1).unwrap().as_str();
                let raw_val = caps.get(2).unwrap().as_str();

                if let Ok(val) = deescape_str(raw_val) {
                    // Note: if there's already an entry in the hash map for
                    // this key, this will replace it. That's the behavior what
                    // we want; when PostgreSQL reads the file, each line
                    // overrides any previous value for the same setting.
                    result.hash.insert(name.to_string(), val.to_string());
                }
            } else {
                return Err(PostgresConfParseError);
            }
        }
        Ok(result)
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

/// De-escape a possibly-quoted value.
///
/// See `DeescapeQuotedString` function in PostgreSQL sources for how PostgreSQL
/// does this.
fn deescape_str(s: &str) -> Result<String> {
    // If the string has a quote at the beginning and end, strip them out.
    if s.len() >= 2 && s.starts_with('\'') && s.ends_with('\'') {
        let mut result = String::new();

        let mut iter = s[1..(s.len() - 1)].chars().peekable();
        while let Some(c) = iter.next() {
            let newc = if c == '\\' {
                match iter.next() {
                    Some('b') => '\x08',
                    Some('f') => '\x0c',
                    Some('n') => '\n',
                    Some('r') => '\r',
                    Some('t') => '\t',
                    Some('0'..='7') => {
                        // TODO
                        bail!("octal escapes not supported");
                    }
                    Some(n) => n,
                    None => break,
                }
            } else if c == '\'' && iter.peek() == Some(&'\'') {
                // doubled quote becomes just one quote
                iter.next().unwrap()
            } else {
                c
            };

            result.push(newc);
        }
        Ok(result)
    } else {
        Ok(s.to_string())
    }
}

#[test]
fn test_postgresql_conf_escapes() -> Result<()> {
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

    // Test de-escaping
    assert_eq!(deescape_str(&escape_str("foo"))?, "foo");
    assert_eq!(deescape_str(&escape_str("fo'o\nba\\r"))?, "fo'o\nba\\r");
    assert_eq!(deescape_str("'\\b\\f\\n\\r\\t'")?, "\x08\x0c\n\r\t");

    // octal-escapes are currently not supported
    assert!(deescape_str("'foo\\7\\07\\007'").is_err());

    Ok(())
}

#[test]
fn test_postgresql_conf_override() -> Result<()> {
    let mut conf = PostgresConf::new();
    conf.append("max_wal_senders", "10");
    conf.append("wal_log_hints", "off");
    conf.append("shared_buffer", "20MB");
    conf.append("neon.timeline_id", "'34a15221af534185b74b82b9b34685dc'");

    // multi-parameters
    match PostgresConf::from_str(
        "max_wal_senders=20;wal_log_hints=on;neon.timeline_id='00000000000000000000000'",
    ) {
        Ok(user_config) => {
            conf.override_conf(Some(user_config));

            assert_eq!(conf.get("max_wal_senders"), Some("20"));
            assert_eq!(conf.get("wal_log_hints"), Some("on"));
            assert_eq!(
                conf.get("neon.timeline_id"),
                Some("00000000000000000000000")
            );
            assert_eq!(conf.get("shared_buffer"), Some("20MB"));
        }
        Err(error) => {
            assert!(false, "should not get error{}", error.to_string());
        }
    }

    // single-parameters
    match PostgresConf::from_str("shared_buffer=100MB") {
        Ok(user_config) => {
            conf.override_conf(Some(user_config));

            assert_eq!(conf.get("max_wal_senders"), Some("20"));
            assert_eq!(conf.get("wal_log_hints"), Some("on"));
            assert_eq!(conf.get("shared_buffer"), Some("100MB"));
        }
        Err(error) => {
            assert!(false, "should not get error{}", error.to_string());
        }
    }

    // parse parameter failed
    match PostgresConf::from_str("shared_buffer") {
        Ok(_) => {
            assert!(true, "should not be here");
        }
        Err(error) => {
            assert_eq!(error.to_string(), PostgresConfParseError.to_string());
        }
    }

    Ok(())
}
