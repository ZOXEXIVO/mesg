mod transport;

use crate::metrics::{MetricsServer};
use tonic::transport::Server;

use crate::cluster::cluster::Cluster;
use log::info;

use crate::server::transport::service::MesgProtocolService;
use std::thread::JoinHandle;
use tokio::runtime::Runtime;
use crate::server::transport::grpc::mesg_protocol_server::MesgProtocolServer;

pub struct MesgServerOptions {
    pub db_path: String,
    pub port: u16,
    pub metric_port: u16,
}

pub struct MesgServer {
    service: Option<MesgProtocolService>,

    cluster: Cluster,

    metrics: Option<JoinHandle<()>>,
}

impl MesgServer {
    pub fn new() -> Self {
        MesgServer {
            cluster: Cluster::new(),
            service: None,
            metrics: None,
        }
    }

    pub async fn run(&mut self, options: MesgServerOptions) -> std::result::Result<(), std::io::Error> {
        let service_port = options.port;

        self.metrics = Some(MesgServer::start_metrics_server(options.metric_port));

        let service = MesgProtocolServer::new(
            MesgProtocolService::new(options)
        );

        let addr = format!("0.0.0.0:{0}", service_port).parse().unwrap();

        info!("listening: {0}", addr);

        Server::builder()
            .add_service(service)
            .serve(addr)
            .await
            .unwrap();

        Ok(())
    }

    fn start_metrics_server(port: u16) -> JoinHandle<()> {
        std::thread::spawn(move || {
            Runtime::new().unwrap().block_on(MetricsServer::start(port));
        })
    }
}