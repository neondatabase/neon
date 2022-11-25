fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Generate code to deterministic location to make finding it easier.
    tonic_build::configure()
        .out_dir("proto/") // put generated code to proto/
        .compile(&["proto/broker.proto"], &["proto/"])?;
    Ok(())
}
