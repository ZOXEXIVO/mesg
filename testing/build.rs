fn main() -> Result<(), Box<dyn std::error::Error>> {
    tonic_build::configure()
        .build_client(true)
        .out_dir("src/")
        .protoc_arg("--experimental_allow_proto3_optional")
        .compile_protos(&["../src/server/transport/proto/mesg.proto"], &[".."])?;
    Ok(())
}
