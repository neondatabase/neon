use cbindgen;

use std::env;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let crate_dir = env::var("CARGO_MANIFEST_DIR").unwrap();

    cbindgen::generate(crate_dir)
        .expect("Unable to generate bindings")
        .write_to_file("communicator_bindings.h");

    tonic_build::configure()
        .build_server(false)
        .build_client(true)
        .compile_protos(
            &["../../../protos/page_service.proto"],
            &["../../../protos/"],
        )?;

    Ok(())
}
