//! # Examples
//!
//! ```
//! use futures::{StreamExt, TryStream, TryStreamExt};
//!
//! async fn stream_to_json_list<S, T, E>(mut s: S) -> Result<String, E>
//! where
//!     S: TryStream<Ok = T, Error = E> + Unpin,
//!     T: json::ValueEncoder
//! {
//!     Ok(json::value_to_string!(|val| json::value_as_list!(|val| {
//!         // note how we can use `.await` and `?` in here.
//!         while let Some(value) = s.try_next().await? {
//!             val.push(value);
//!         }
//!     })))
//! }
//!
//! let stream = futures::stream::iter([1, 2, 3]).map(Ok::<i32, ()>);
//! let json_string = futures::executor::block_on(stream_to_json_list(stream)).unwrap();
//! assert_eq!(json_string, "[1,2,3]");
//! ```

/// A helper to create a new JSON vec.
///
/// Implemented as a macro to preserve all control flow.
#[macro_export]
macro_rules! value_to_vec {
    (|$val:ident| $body:expr) => {{
        let mut buf = vec![];
        let $val = $crate::ValueSer::new(&mut buf);
        let _: () = $body;
        buf
    }};
}

/// A helper to create a new JSON string.
///
/// Implemented as a macro to preserve all control flow.
#[macro_export]
macro_rules! value_to_string {
    (|$val:ident| $body:expr) => {{
        ::std::string::String::from_utf8($crate::value_to_vec!(|$val| $body))
            .expect("json should be valid utf8")
    }};
}

/// A helper that ensures the [`ObjectSer::finish`](crate::ObjectSer::finish) method is called on completion.
///
/// Consumes `$val` and assigns it as an [`ObjectSer`](crate::ObjectSer) serializer.
/// The serializer is only 'finished' if the body completes.
/// The serializer is rolled back if `break`/`return` escapes the body.
///
/// Implemented as a macro to preserve all control flow.
#[macro_export]
macro_rules! value_as_object {
    (|$val:ident| $body:expr) => {{
        let mut obj = $crate::ObjectSer::new($val);

        let $val = &mut obj;
        let res = $body;

        obj.finish();
        res
    }};
}

/// A helper that ensures the [`ListSer::finish`](crate::ListSer::finish) method is called on completion.
///
/// Consumes `$val` and assigns it as an [`ListSer`](crate::ListSer) serializer.
/// The serializer is only 'finished' if the body completes.
/// The serializer is rolled back if `break`/`return` escapes the body.
///
/// Implemented as a macro to preserve all control flow.
#[macro_export]
macro_rules! value_as_list {
    (|$val:ident| $body:expr) => {{
        let mut list = $crate::ListSer::new($val);

        let $val = &mut list;
        let res = $body;

        list.finish();
        res
    }};
}
