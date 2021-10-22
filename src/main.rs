extern crate timer;

mod cluster;
mod metrics;
mod server;
mod storage;

use env_logger::Env;

use clap::{App, Arg};
use crate::server::{MesgServerOptions, MesgServer};

#[tokio::main]
async fn main() -> Result<(), std::io::Error> {
    init_logger();

    let options = get_options();
    
    MesgServer::new().run(options).await?;
    
    Ok(())
}

fn get_options() -> MesgServerOptions {
    let matches = App::new("mesg")
        .version(env!("CARGO_PKG_VERSION"))
        .author("Artemov Ivan (@ZOXEXIVO)")
        .about("A simple message broker with GRPC contact")
        .arg(Arg::with_name("port")
            .short("p")
            .long("port")
            .help("listening port")
            .takes_value(true)
        )
        .arg(Arg::with_name("metrics-port")
            .short("sp")
            .long("service-port")
            .help("service port")
            .takes_value(true)
        ).after_help(r#"EXAMPLES:

        ./mesg --port 35000 --service-port 35001
    "#).get_matches();

    MesgServerOptions {
        db_path: String::from(matches.value_of("dbpath").unwrap_or(".")),
        port: matches.value_of("port").unwrap_or("35000").parse::<u16>().unwrap_or(35000),
        metric_port: matches.value_of("port").unwrap_or("35001").parse::<u16>().unwrap_or(35001),
    }
}

fn init_logger() {
    env_logger::Builder::from_env(Env::default().default_filter_or("info")).init();
}
