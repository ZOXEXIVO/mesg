fn main() -> Result<(), Box<dyn std::error::Error>> {
    tonic_build::configure()
        .build_client(true)
        .out_dir("src/")
        .compile(&["../src/server/transport/proto/mesg.proto"], &[".."])?;
    Ok(())
}
