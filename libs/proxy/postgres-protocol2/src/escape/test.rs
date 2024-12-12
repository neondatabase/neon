use crate::escape::{escape_identifier, escape_literal};

#[test]
fn test_escape_idenifier() {
    assert_eq!(escape_identifier("foo"), String::from("\"foo\""));
    assert_eq!(escape_identifier("f\\oo"), String::from("\"f\\oo\""));
    assert_eq!(escape_identifier("f'oo"), String::from("\"f'oo\""));
    assert_eq!(escape_identifier("f\"oo"), String::from("\"f\"\"oo\""));
}

#[test]
fn test_escape_literal() {
    assert_eq!(escape_literal("foo"), String::from("'foo'"));
    assert_eq!(escape_literal("f\\oo"), String::from(" E'f\\\\oo'"));
    assert_eq!(escape_literal("f'oo"), String::from("'f''oo'"));
    assert_eq!(escape_literal("f\"oo"), String::from("'f\"oo'"));
}
