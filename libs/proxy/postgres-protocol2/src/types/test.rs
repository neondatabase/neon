use bytes::{Buf, BytesMut};

use super::*;

#[test]
fn ltree_sql() {
    let mut query = vec![1u8];
    query.extend_from_slice("A.B.C".as_bytes());

    let mut buf = BytesMut::new();

    ltree_to_sql("A.B.C", &mut buf);

    assert_eq!(query.as_slice(), buf.chunk());
}

#[test]
fn ltree_str() {
    let mut query = vec![1u8];
    query.extend_from_slice("A.B.C".as_bytes());

    assert!(ltree_from_sql(query.as_slice()).is_ok())
}

#[test]
fn ltree_wrong_version() {
    let mut query = vec![2u8];
    query.extend_from_slice("A.B.C".as_bytes());

    assert!(ltree_from_sql(query.as_slice()).is_err())
}

#[test]
fn lquery_sql() {
    let mut query = vec![1u8];
    query.extend_from_slice("A.B.C".as_bytes());

    let mut buf = BytesMut::new();

    lquery_to_sql("A.B.C", &mut buf);

    assert_eq!(query.as_slice(), buf.chunk());
}

#[test]
fn lquery_str() {
    let mut query = vec![1u8];
    query.extend_from_slice("A.B.C".as_bytes());

    assert!(lquery_from_sql(query.as_slice()).is_ok())
}

#[test]
fn lquery_wrong_version() {
    let mut query = vec![2u8];
    query.extend_from_slice("A.B.C".as_bytes());

    assert!(lquery_from_sql(query.as_slice()).is_err())
}

#[test]
fn ltxtquery_sql() {
    let mut query = vec![1u8];
    query.extend_from_slice("a & b*".as_bytes());

    let mut buf = BytesMut::new();

    ltree_to_sql("a & b*", &mut buf);

    assert_eq!(query.as_slice(), buf.chunk());
}

#[test]
fn ltxtquery_str() {
    let mut query = vec![1u8];
    query.extend_from_slice("a & b*".as_bytes());

    assert!(ltree_from_sql(query.as_slice()).is_ok())
}

#[test]
fn ltxtquery_wrong_version() {
    let mut query = vec![2u8];
    query.extend_from_slice("a & b*".as_bytes());

    assert!(ltree_from_sql(query.as_slice()).is_err())
}
