use crate::metrics::MetricsWriter;
use tonic::transport::Server;

use crate::cluster::cluster::Cluster;
use log::info;

use crate::server::network::grpc::mesg_service_server::MesgServiceServer;
use crate::server::network::service::MesgInternalService;

pub struct MesgServerOptions {
    pub db_path: String,
    pub port: u16,
}

pub struct MesgServer {
    service: Option<MesgInternalService>,

    cluster: Cluster,

    metrics: MetricsWriter,
}

impl MesgServer {
    pub fn new(metrics_writer: MetricsWriter) -> Self {
        MesgServer {
            cluster: Cluster::new(),
            service: None,
            metrics: metrics_writer,
        }
    }

    pub async fn run(&mut self, options: MesgServerOptions) -> std::result::Result<(), std::io::Error> {
        let service_port = options.port;

        let service = MesgServiceServer::new(MesgInternalService::new(
            options,
            self.metrics.clone(),
        ));

        let addr = format!("0.0.0.0:{0}", service_port).parse().unwrap();

        info!("listening: {0}", addr);

        Server::builder()
            .add_service(service)
            .serve(addr)
            .await
            .unwrap();

        Ok(())
    }
}
