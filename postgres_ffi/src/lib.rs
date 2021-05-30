pub mod non_portable;
pub mod pg_constants;
pub mod relfile_utils;
pub mod xlog_utils;

// Not for public use; this is only for testing that handwritten structs match
// Auto-generated structs.
mod bindgen_bindings {
    #![allow(non_upper_case_globals)]
    #![allow(non_camel_case_types)]
    #![allow(non_snake_case)]
    #![allow(dead_code)]
    include!(concat!(env!("OUT_DIR"), "/bindings.rs"));
}
