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

pub mod walproposer_api;

#[cfg(test)]
mod walproposer_tests {
    use std::ffi::{CString, c_char};

    use crate::{walproposer_api::{create_api, ApiImpl, ExampleImpl}, bindings::{WalProposerConfig, WalProposerCreate}};

    #[test]
    fn my_call_test() -> anyhow::Result<()> {
        let tenant_id = CString::new("9e4c8f36063c6c6e93bc20d65a820f3d")?.into_raw();
        let timeline_id = CString::new("9e4c8f36063c6c6e93bc20d65a820f3d")?.into_raw();

        let mut safekeepers_list = CString::new("localhost:5000")?.into_bytes_with_nul().into_boxed_slice();

        let my_impl: Box<dyn ApiImpl> = Box::new(ExampleImpl{});
        let boxed_impl = Box::new(my_impl);

        let config = WalProposerConfig {
            neon_tenant: tenant_id,
            neon_timeline: timeline_id,
            safekeepers_list: safekeepers_list.as_mut_ptr() as *mut i8,
            safekeeper_reconnect_timeout: 1000,
            safekeeper_connection_timeout: 10000,
            wal_segment_size: 16 * 1024 * 1024,
            syncSafekeepers: true,
            systemId: 0,
            pgTimeline: 1,
            callback_data: Box::into_raw(boxed_impl) as *mut ::std::os::raw::c_void,
        };
        let config = Box::new(config);

        unsafe {
            let api = create_api();
            let wp = WalProposerCreate(Box::into_raw(config), api);
            // crate::bindings::test_call(api);
        }
        assert_eq!(1, 1);
        assert_eq!(2, 2);

        Ok(())
    }
}
