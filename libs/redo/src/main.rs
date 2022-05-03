

// This will look for `int redo()` in `libredo.a`
#[link(name = "redo")]
extern "C" {
    fn redo() -> i32;
}


// To compile this, run:
// rustc libs/redo/src/main.rs -L /home/bojan/src/neondatabase/neon/tmp_install/lib/
fn main() {
    unsafe {
        dbg!(redo());
    }
}
