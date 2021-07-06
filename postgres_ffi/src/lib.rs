#![allow(non_upper_case_globals)]
#![allow(non_camel_case_types)]
#![allow(non_snake_case)]
// suppress warnings on rust 1.53 due to bindgen unit tests.
// https://github.com/rust-lang/rust-bindgen/issues/1651
#![allow(deref_nullptr)]
include!(concat!(env!("OUT_DIR"), "/bindings.rs"));

pub mod controlfile_utils;
pub mod nonrelfile_utils;
pub mod pg_constants;
pub mod relfile_utils;
pub mod xlog_utils;
