fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Generates Rust code from .proto Protobuf schemas.
    tonic_build::configure()
        .bytes(["."])
        .compile_protos(&["proto/page_service.proto"], &["proto"])
        .map_err(|err| err.into())
}
