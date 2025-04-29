fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Generate rust code from .proto protobuf.
    tonic_build::configure()
        .bytes(["."])
        .compile_protos(&["proto/page_service.proto"], &["proto"])
        .unwrap_or_else(|e| panic!("failed to compile protos {:?}", e));
    Ok(())
}
