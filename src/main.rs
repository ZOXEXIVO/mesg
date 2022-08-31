extern crate core;
extern crate timer;

mod cluster;
mod consumer;
mod controller;
mod metrics;
mod server;
mod storage;

use env_logger::Env;
use log::LevelFilter;

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
    let env = Env::default().default_filter_or("debug");

    let mut builder = env_logger::Builder::from_env(env);

    let builder = builder
        .filter(Some("h2"), LevelFilter::Off)
        .filter(Some("sled"), LevelFilter::Off);

    builder.init();
}
