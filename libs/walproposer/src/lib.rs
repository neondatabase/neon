pub mod bindings {
    #![allow(non_upper_case_globals)]
    #![allow(non_camel_case_types)]
    #![allow(non_snake_case)]
    // bindgen creates some unsafe code with no doc comments.
    #![allow(clippy::missing_safety_doc)]
    // noted at 1.63 that in many cases there's a u32 -> u32 transmutes in bindgen code.
    #![allow(clippy::useless_transmute)]

    include!(concat!(env!("OUT_DIR"), "/bindings.rs"));
}

pub mod api_bindings;
pub mod walproposer;
