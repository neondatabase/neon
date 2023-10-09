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

#[cfg(test)]
mod walproposer_tests {
    use std::ffi::{CString, c_char};

    use utils::id::TenantTimelineId;

    use crate::{api_bindings::create_api, bindings::{WalProposerConfig, WalProposerCreate}, walproposer::ApiImpl};

    struct ExampleImpl {}

    impl ApiImpl for ExampleImpl {
        fn strong_random(&self, buf: &mut [u8]) -> bool {
            println!("strong_random");
            true
        }

        fn wal_reader_allocate(&self, sk: &mut crate::bindings::Safekeeper) {
            println!("wal_reader_allocate");
        }

        fn init_event_set(&self, wp: &mut crate::bindings::WalProposer) {
            println!("init_event_set");
        }
    }

    #[test]
    fn my_call_test() -> anyhow::Result<()> {
        let ttid = TenantTimelineId::new(
            "9e4c8f36063c6c6e93bc20d65a820f3d".parse()?,
            "9e4c8f36063c6c6e93bc20d65a820f3d".parse()?,
        );

        let my_impl: Box<dyn ApiImpl> = Box::new(ExampleImpl{});
        let config = crate::walproposer::Config {
            ttid,
            safekeepers_list: vec!["localhost:5000".to_string()],
            safekeeper_reconnect_timeout: 1000,
            safekeeper_connection_timeout: 10000,
            sync_safekeepers: true,
        };

        let wp = crate::walproposer::new(my_impl, config);
        assert_eq!(1, 1);
        assert_eq!(2, 2);

        Ok(())
    }
}
