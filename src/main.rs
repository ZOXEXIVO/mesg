mod cluster;
mod metrics;
mod server;
mod storage;
mod utils;

extern crate timer;

use crate::server::server::{MesgServer, MesgServerOptions};
use crate::metrics::MetricsServer;
use env_logger::Env;

#[tokio::main]
async fn main() -> Result<(), std::io::Error> {
    env_logger::Builder::from_env(Env::default().default_filter_or("info")).init();

    let mut server = MesgServer::new(
        MesgServerOptions {
            db_path: ".".to_string(),
            port: 4000,
        }, MetricsServer::start()
    );

    server.run().await?;

    Ok(())
}
