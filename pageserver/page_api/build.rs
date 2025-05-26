use std::env;
use std::path::PathBuf;

/// Generates Rust code from .proto Protobuf schemas, along with a binary file
/// descriptor set for Protobuf schema reflection.
fn main() -> Result<(), Box<dyn std::error::Error>> {
    let out_dir = PathBuf::from(env::var("OUT_DIR")?);
    tonic_build::configure()
        .bytes(["."])
        .file_descriptor_set_path(out_dir.join("page_api_descriptor.bin"))
        .compile_protos(&["proto/page_service.proto"], &["proto"])
        .map_err(|err| err.into())
}
