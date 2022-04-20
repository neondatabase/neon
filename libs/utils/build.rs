fn main() {
    println!("cargo:rerun-if-env-changed=GIT_VERSION");
}
