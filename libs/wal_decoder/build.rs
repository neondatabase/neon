fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Generate rust code from .proto protobuf.
    //
    // Note: we previously tried to use deterministic location at proto/ for
    // easy location, but apparently interference with cachepot sometimes fails
    // the build then. Anyway, per cargo docs build script shouldn't output to
    // anywhere but $OUT_DIR.
    tonic_build::compile_protos("proto/interpreted_wal.proto")
        .unwrap_or_else(|e| panic!("failed to compile protos {:?}", e));
    Ok(())
}
