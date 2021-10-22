fn main() -> Result<(), Box<dyn std::error::Error>> {
    tonic_build::configure()
         .build_server(true)
         .build_client(false)
         .out_dir("src/server/transport")
         .compile(
             &["src/server/transport/proto/mesg.proto"],
             &["src"]
         )?;
    Ok(())
 }