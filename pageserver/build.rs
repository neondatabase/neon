//
//   Triggers postgres build if there is no postgres binary present at
// 'REPO_ROOT/tmp_install/bin/postgres'.
//
//   I can see a lot of disadvantages with such automatization and main
// advantage here is ability to build everything and run integration tests
// in a bare repo by running 'cargo test'.
//
//   We can interceipt whether it is debug or release build and run
// corresponding pg build. But it seems like an overkill for now.
//
// Problem #1 -- language server in my editor likes calling 'cargo build'
// by himself. So if I delete tmp_install directory it would magically reappear
// after some time. During this compilation 'cargo build' may whine about
// "waiting for file lock on build directory".
//
// Problem #2 -- cargo build would run this only if something is changed in
// the crate.
//
//   And generally speaking postgres is not a build dependency for the pageserver,
// just for integration tests. So let's not mix that. I'll leave this file in
// place for some time just in case if anybody would start doing the same.
//

// use std::path::Path;
// use std::process::{Command};

fn main() {
    // // build some postgres if it is not done none yet
    // if !Path::new("../tmp_install/bin/postgres").exists() {
    //     let make_res = Command::new("make")
    //         .arg("postgres")
    //         .env_clear()
    //         .status()
    //         .expect("failed to execute 'make postgres'");

    //     if !make_res.success() {
    //         panic!("postgres build failed");
    //     }
    // }
}
