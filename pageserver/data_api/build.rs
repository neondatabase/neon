fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Generate rust code from .proto protobuf.
    tonic_build::compile_protos("proto/page_service.proto")
        .unwrap_or_else(|e| panic!("failed to compile protos {:?}", e));
    Ok(())
}
