use json::Null;
use json::ValueEncoder;
use std::collections::BTreeMap;

macro_rules! treemap {
    () => {
        BTreeMap::new()
    };
    ($($k:expr => $v:expr),+ $(,)?) => {
        {
            let mut m = BTreeMap::new();
            $(
                m.insert($k, $v);
            )+
            m
        }
    };
}

fn test_encode_ok<'a, T>(errors: impl IntoIterator<Item = (T, &'a str)>)
where
    T: ValueEncoder,
{
    for (value, out) in errors {
        let s = json::value_to_string!(|v| value.encode(v));
        assert_eq!(&*s, out);
    }
}

#[test]
fn test_write_null() {
    let tests = [(Null, "null")];
    test_encode_ok(tests);
}

#[test]
fn test_write_u64() {
    let tests = [(3u64, "3"), (u64::MAX, &u64::MAX.to_string())];
    test_encode_ok(tests);
}

#[test]
fn test_write_i64() {
    let tests = [
        (3i64, "3"),
        (-2i64, "-2"),
        (-1234i64, "-1234"),
        (i64::MIN, &i64::MIN.to_string()),
    ];
    test_encode_ok(tests);
}

#[test]
fn test_write_f64() {
    let tests = [
        (3.0, "3.0"),
        (3.1, "3.1"),
        (-1.5, "-1.5"),
        (0.5, "0.5"),
        (f64::MIN, "-1.7976931348623157e308"),
        (f64::MAX, "1.7976931348623157e308"),
        (f64::EPSILON, "2.220446049250313e-16"),
    ];
    test_encode_ok(tests);
}

#[test]
fn test_write_str() {
    let tests = [
        // normal
        ("", "\"\""),
        ("foo", "\"foo\""),
        // ascii escape chars.
        ("\"", "\"\\\"\""),
        ("\x08", "\"\\b\""),
        ("\n", "\"\\n\""),
        ("\r", "\"\\r\""),
        ("\t", "\"\\t\""),
        ("\x07", "\"\\u0007\""),
        // unicode not escaped.
        ("\u{12ab}", "\"\u{12ab}\""),
        ("\u{AB12}", "\"\u{AB12}\""),
        ("\u{1F395}", "\"\u{1F395}\""),
    ];
    test_encode_ok(tests);
}

#[test]
fn test_write_bool() {
    let tests = [(true, "true"), (false, "false")];
    test_encode_ok(tests);
}

#[test]
fn test_write_list() {
    test_encode_ok([
        (vec![], "[]"),
        (vec![true], "[true]"),
        (vec![true, false], "[true,false]"),
    ]);

    test_encode_ok([
        (vec![vec![], vec![], vec![]], "[[],[],[]]"),
        (vec![vec![1, 2, 3], vec![], vec![]], "[[1,2,3],[],[]]"),
        (vec![vec![], vec![1, 2, 3], vec![]], "[[],[1,2,3],[]]"),
        (vec![vec![], vec![], vec![1, 2, 3]], "[[],[],[1,2,3]]"),
    ]);
}

#[test]
fn test_write_object() {
    test_encode_ok([
        (treemap!(), "{}"),
        (treemap!("a".to_owned() => true), "{\"a\":true}"),
        (
            treemap!(
                "a".to_owned() => true,
                "b".to_owned() => false,
            ),
            "{\"a\":true,\"b\":false}",
        ),
    ]);

    test_encode_ok([
        (
            treemap![
                "a".to_owned() => treemap![],
                "b".to_owned() => treemap![],
                "c".to_owned() => treemap![],
            ],
            "{\"a\":{},\"b\":{},\"c\":{}}",
        ),
        (
            treemap![
                "a".to_owned() => treemap![
                    "a".to_owned() => treemap!["a" => vec![1,2,3]],
                    "b".to_owned() => treemap![],
                    "c".to_owned() => treemap![],
                ],
                "b".to_owned() => treemap![],
                "c".to_owned() => treemap![],
            ],
            "{\"a\":{\"a\":{\"a\":[1,2,3]},\"b\":{},\"c\":{}},\"b\":{},\"c\":{}}",
        ),
        (
            treemap![
                "a".to_owned() => treemap![],
                "b".to_owned() => treemap![
                    "a".to_owned() => treemap!["a" => vec![1,2,3]],
                    "b".to_owned() => treemap![],
                    "c".to_owned() => treemap![],
                ],
                "c".to_owned() => treemap![],
            ],
            "{\"a\":{},\"b\":{\"a\":{\"a\":[1,2,3]},\"b\":{},\"c\":{}},\"c\":{}}",
        ),
        (
            treemap![
                "a".to_owned() => treemap![],
                "b".to_owned() => treemap![],
                "c".to_owned() => treemap![
                    "a".to_owned() => treemap!["a" => vec![1,2,3]],
                    "b".to_owned() => treemap![],
                    "c".to_owned() => treemap![],
                ],
            ],
            "{\"a\":{},\"b\":{},\"c\":{\"a\":{\"a\":[1,2,3]},\"b\":{},\"c\":{}}}",
        ),
    ]);
}

#[test]
fn test_write_option() {
    test_encode_ok([(None, "null"), (Some("jodhpurs"), "\"jodhpurs\"")]);

    test_encode_ok([
        (None, "null"),
        (Some(vec!["foo", "bar"]), "[\"foo\",\"bar\"]"),
    ]);
}
