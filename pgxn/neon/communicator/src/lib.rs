/// dummy function, just to test linking Rust functions into the C
/// extension
#[unsafe(no_mangle)]
pub extern "C" fn communicator_dummy(arg: u32) -> u32 {
    arg + 1
}
