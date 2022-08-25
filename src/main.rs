extern crate timer;
extern crate core;

mod cluster;
mod controller;
mod metrics;
mod server;
mod storage;

use env_logger::Env;

use crate::server::{MesgServer, MesgServerOptions};
use structopt::StructOpt;

#[derive(StructOpt, Debug)]
#[structopt(name = "basic")]
pub struct Opt {
    #[structopt(short, long, default_value = "")]
    pub db_path: String,
    #[structopt(short, long, default_value = "35000")]
    pub port: u16,
    #[structopt(short, long, default_value = "35001")]
    pub metric_port: u16,
}

#[tokio::main]
async fn main() -> Result<(), std::io::Error> {
    color_eyre::install().unwrap();

    init_logger();

    let options = Opt::from_args();
    let server_options = MesgServerOptions {
        db_path: options.db_path,
        port: options.port,
        metric_port: options.metric_port,
    };

    MesgServer::new().run(server_options).await?;

    Ok(())
}

fn init_logger() {
    env_logger::Builder::from_env(Env::default().default_filter_or("info")).init();
}
