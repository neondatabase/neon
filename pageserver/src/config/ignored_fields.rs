//! Check for fields in the on-disk config file that were ignored when
//! deserializing [`pageserver_api::config::ConfigToml`].
//!
//! This could have been part of the [`pageserver_api::config`] module,
//! but the way we identify unused fields in this module
//! is specific to the format (TOML) and the implementation of the
//! deserialization for that format ([`toml_edit`]).

use std::collections::HashSet;

use itertools::Itertools;

/// Pass in the user-specified config and the re-serialized [`pageserver_api::config::ConfigToml`].
/// The returned [`Paths`] contains the paths to the fields that were ignored by deserialization
/// of the [`pageserver_api::config::ConfigToml`].
pub fn find(user_specified: toml_edit::DocumentMut, reserialized: toml_edit::DocumentMut) -> Paths {
    let user_specified = paths(user_specified);
    let reserialized = paths(reserialized);
    fn paths(doc: toml_edit::DocumentMut) -> HashSet<String> {
        let mut paths = Vec::new();
        visit_table_like(doc.as_table(), &mut Vec::new(), &mut paths);
        HashSet::from_iter(paths)
    }

    let mut ignored = HashSet::new();

    // O(n) because of HashSet
    for path in user_specified {
        if !reserialized.contains(&path) {
            ignored.insert(path);
        }
    }

    Paths {
        paths: ignored
            .into_iter()
            // sort lexicographically for deterministic output
            .sorted()
            .collect(),
    }
}

pub struct Paths {
    pub paths: Vec<String>,
}

fn visit_table_like(
    table_like: &dyn toml_edit::TableLike,
    path: &mut Vec<String>,
    paths: &mut Vec<String>,
) {
    for (entry, item) in table_like.iter() {
        path.push(entry.to_string());
        visit_item(item, path, paths);
        path.pop();
    }
}

fn visit_item(item: &toml_edit::Item, path: &mut Vec<String>, paths: &mut Vec<String>) {
    match item {
        toml_edit::Item::None => (),
        toml_edit::Item::Value(value) => visit_value(value, path, paths),
        toml_edit::Item::Table(table) => {
            visit_table_like(table, path, paths);
        }
        toml_edit::Item::ArrayOfTables(array_of_tables) => {
            for (i, table) in array_of_tables.iter().enumerate() {
                path.push(format!("[{i}]"));
                visit_table_like(table, path, paths);
                path.pop();
            }
        }
    }
}

fn visit_value(value: &toml_edit::Value, path: &mut Vec<String>, paths: &mut Vec<String>) {
    match value {
        toml_edit::Value::String(_)
        | toml_edit::Value::Integer(_)
        | toml_edit::Value::Float(_)
        | toml_edit::Value::Boolean(_)
        | toml_edit::Value::Datetime(_) => paths.push(path.join(".")),
        toml_edit::Value::Array(array) => {
            for (i, value) in array.iter().enumerate() {
                path.push(format!("[{i}]"));
                visit_value(value, path, paths);
                path.pop();
            }
        }
        toml_edit::Value::InlineTable(inline_table) => visit_table_like(inline_table, path, paths),
    }
}

#[cfg(test)]
pub(crate) mod tests {

    fn test_impl(original: &str, parsed: &str, expect: [&str; 1]) {
        let original: toml_edit::DocumentMut = original.parse().expect("parse original config");
        let parsed: toml_edit::DocumentMut = parsed.parse().expect("parse re-serialized config");

        let super::Paths { paths: actual } = super::find(original, parsed);
        assert_eq!(actual, &expect);
    }

    #[test]
    fn top_level() {
        test_impl(
            r#"
                [a]
                b = 1
                c = 2
                d = 3
            "#,
            r#"
                [a]
                b = 1
                c = 2
            "#,
            ["a.d"],
        );
    }

    #[test]
    fn nested() {
        test_impl(
            r#"
                [a.b.c]
                d = 23
            "#,
            r#"
                [a]
                e = 42
            "#,
            ["a.b.c.d"],
        );
    }

    #[test]
    fn array_of_tables() {
        test_impl(
            r#"
                [[a]]
                b = 1
                c = 2
                d = 3
            "#,
            r#"
                [[a]]
                b = 1
                c = 2
            "#,
            ["a.[0].d"],
        );
    }

    #[test]
    fn array() {
        test_impl(
            r#"
            foo = [ {bar = 23} ]
            "#,
            r#"
            foo = [ { blup = 42 }]
            "#,
            ["foo.[0].bar"],
        );
    }
}
