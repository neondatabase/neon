/// Immediately terminate the calling process without calling
/// atexit callbacks, C runtime destructors etc. We mainly use
/// this to protect coverage data from concurrent writes.
pub fn exit_now(code: u8) {
    unsafe { nix::libc::_exit(code as _) };
}
