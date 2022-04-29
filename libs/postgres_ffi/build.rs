extern crate bindgen;

use std::env;
use std::path::PathBuf;

use bindgen::callbacks::ParseCallbacks;

#[derive(Debug)]
struct PostgresFfiCallbacks;

impl ParseCallbacks for PostgresFfiCallbacks {
    fn include_file(&self, filename: &str) {
        // This does the equivalent of passing bindgen::CargoCallbacks
        // to the builder .parse_callbacks() method.
        let cargo_callbacks = bindgen::CargoCallbacks;
        cargo_callbacks.include_file(filename)
    }

    // Add any custom #[derive] attributes to the data structures that bindgen
    // creates.
    fn add_derives(&self, name: &str) -> Vec<String> {
        // This is the list of data structures that we want to serialize/deserialize.
        let serde_list = [
            "XLogRecord",
            "XLogPageHeaderData",
            "XLogLongPageHeaderData",
            "CheckPoint",
            "FullTransactionId",
            "ControlFileData",
        ];

        if serde_list.contains(&name) {
            vec![
                "Default".into(), // Default allows us to easily fill the padding fields with 0.
                "Serialize".into(),
                "Deserialize".into(),
            ]
        } else {
            vec![]
        }
    }
}

fn main() {
    // Tell cargo to invalidate the built crate whenever the wrapper changes
    println!("cargo:rerun-if-changed=pg_control_ffi.h");

    // The bindgen::Builder is the main entry point
    // to bindgen, and lets you build up options for
    // the resulting bindings.
    let bindings = bindgen::Builder::default()
        //
        // All the needed PostgreSQL headers are included from 'pg_control_ffi.h'
        //
        .header("pg_control_ffi.h")
        //
        // Tell cargo to invalidate the built crate whenever any of the
        // included header files changed.
        //
        .parse_callbacks(Box::new(PostgresFfiCallbacks))
        //
        // These are the types and constants that we want to generate bindings for
        //
        .allowlist_type("BlockNumber")
        .allowlist_type("OffsetNumber")
        .allowlist_type("MultiXactId")
        .allowlist_type("MultiXactOffset")
        .allowlist_type("MultiXactStatus")
        .allowlist_type("ControlFileData")
        .allowlist_type("CheckPoint")
        .allowlist_type("FullTransactionId")
        .allowlist_type("XLogRecord")
        .allowlist_type("XLogPageHeaderData")
        .allowlist_type("XLogLongPageHeaderData")
        .allowlist_var("XLOG_PAGE_MAGIC")
        .allowlist_var("PG_CONTROL_FILE_SIZE")
        .allowlist_var("PG_CONTROLFILEDATA_OFFSETOF_CRC")
        .allowlist_type("PageHeaderData")
        .allowlist_type("DBState")
        // Because structs are used for serialization, tell bindgen to emit
        // explicit padding fields.
        .explicit_padding(true)
        //
        // Path the server include dir. It is in tmp_install/include/server, if you did
        // "configure --prefix=<path to tmp_install>". But if you used "configure --prefix=/",
        // and used DESTDIR to move it into tmp_install, then it's in
        // tmp_install/include/postgres/server
        // 'pg_config --includedir-server' would perhaps be the more proper way to find it,
        // but this will do for now.
        //
        .clang_arg("-I../../tmp_install/include/server")
        .clang_arg("-I../../tmp_install/include/postgresql/server")
        //
        // Finish the builder and generate the bindings.
        //
        .generate()
        .expect("Unable to generate bindings");

    // Write the bindings to the $OUT_DIR/bindings.rs file.
    let out_path = PathBuf::from(env::var("OUT_DIR").unwrap());
    bindings
        .write_to_file(out_path.join("bindings.rs"))
        .expect("Couldn't write bindings!");
}
