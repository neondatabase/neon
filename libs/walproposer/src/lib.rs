pub mod bindings {
    include!(concat!(env!("OUT_DIR"), "/bindings.rs"));
}

#[cfg(test)]
mod walproposer_tests {
    use crate::bindings;

    #[test]
    fn my_call_test() {
        unsafe {
            bindings::test_call();
        }
        assert_eq!(1, 1);
        assert_eq!(2, 2);
    }
}
