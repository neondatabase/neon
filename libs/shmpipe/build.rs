fn main() {
    println!("cargo:rustc-link-lib=shmpipe");
    println!("cargo:rustc-link-search=libs/shmpipe");
}
