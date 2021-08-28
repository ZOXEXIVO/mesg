use crate::metrics::{MetricsWriter, MetricsServer};
use tonic::transport::Server;

use crate::cluster::cluster::Cluster;
use log::info;

use crate::server::network::grpc::mesg_service_server::MesgServiceServer;
use crate::server::network::service::MesgInternalService;
use std::thread::JoinHandle;
use futures::executor::block_on;
use tokio::runtime::Runtime;

pub struct MesgServerOptions {
    pub db_path: String,
    pub port: u16,
    pub metric_port: u16,
}

pub struct MesgServer {
    service: Option<MesgInternalService>,

    cluster: Cluster,

    metrics: Option<JoinHandle<()>>
}

impl MesgServer {
    pub fn new() -> Self {
        MesgServer {
            cluster: Cluster::new(),
            service: None,
            metrics: None
        }
    }

    pub async fn run(&mut self, options: MesgServerOptions) -> std::result::Result<(), std::io::Error> {
        let service_port = options.port;

        MesgServer::start_metrics_server(options.metric_port);
        
        let service = MesgServiceServer::new(
            MesgInternalService::new(options)
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
    
    fn start_metrics_server(port: u16) {
        std::thread::spawn(move || {
            Runtime::new().unwrap().block_on(MetricsServer::start(port));
        });
    }
}
