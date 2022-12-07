extern crate cc;

fn main() {
    cc::Build::new()
        .file("../../pgxn/neon_walredo/shmpipe.c")
        .compile("libshmpipe.a");
}
