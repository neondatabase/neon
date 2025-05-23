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

/// A helper that ensures the [`ObjectSer::finish`] method is called on completion.
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

/// A helper that ensures the [`ListSer::finish`] method is called on completion.
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
