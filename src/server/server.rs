use crate::metrics::MetricsWriter;
use tonic::transport::Server;

use crate::cluster::cluster::Cluster;
use log::info;

use crate::server::grpc::mesg_service_server::MesgServiceServer;
use crate::server::service::MesgInternalService;

pub struct MesgServerOptions {
    pub db_path: String,
    pub port: u16,
}

pub struct MesgServer {
    options: MesgServerOptions,

    service: Option<MesgInternalService>,

    cluster: Cluster,

    metrics: MetricsWriter,

    port: u16,
}

impl MesgServer {
    pub fn new(options: MesgServerOptions, metrics_writer: MetricsWriter) -> Self {
        let cluster = Cluster::new();

        let port = options.port;

        MesgServer {
            options,
            cluster,
            service: None,
            metrics: metrics_writer,
            port,
        }
    }

    pub async fn run(&mut self) -> std::result::Result<(), std::io::Error> {
        let service = MesgServiceServer::new(MesgInternalService::new(
            &self.options,
            self.metrics.clone(),
        ));

        let addr = format!("0.0.0.0:{0}", self.port).parse().unwrap();

        info!("listening: {0}", addr);

        Server::builder()
            .add_service(service)
            .serve(addr)
            .await
            .unwrap();

        Ok(())
    }
}
