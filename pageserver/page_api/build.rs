fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Generate rust code from .proto protobuf.
    tonic_build::configure()
        .bytes(["."])
        .compile_protos(&["proto/page_service.proto"], &["proto"])
        .map_err(|err| err.into())
}
